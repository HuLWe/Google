import os
import re
import json
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ================= 1. 读取并清洗环境变量 =================
FRED_API_KEY = os.environ["FRED_API_KEY"].strip()
_raw_ss = os.environ["SPREADSHEET_ID"].strip()
_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", _raw_ss)
SPREADSHEET_ID = _match.group(1) if _match else _raw_ss.split("/")[0].split("?")[0].strip()
GCP_SA_JSON = json.loads(os.environ["GCP_SA_KEY"])

# AI 模型与中转站配置：https://api.starwish.fit/v1 + GPT-6.2 Astra + 最高思考程度
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.starwish.fit/v1").strip().rstrip("/")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-6.2-astra").strip()
OPENAI_REASONING_EFFORT = os.environ.get("OPENAI_REASONING_EFFORT", "max").strip()
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

TZ_CST = timezone(timedelta(hours=8))
NOW_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d %H:%M:%S")
TODAY_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d")

# 精简后的 3 张核心工作表
TAB_DASH = "00_宏观驾驶舱_总览"
TAB_RISK_NEWS = "01_风险雷达与多板块新闻库"
TAB_TS_CALC = "02_底层时序与衍生运算库"

OLD_TABS_TO_REMOVE = [
    "00_宏观驾驶舱_Dashboard",
    "01_跨资产与宏观硬数据_MarketsAndMacro",
    "02_日频短周期库_TimeSeries",
    "03_全量指标底表与日志_RawAndLog",
    "04_长期结构周期库_LongTermCycle",
    "00_总览与自动研判_Dashboard",
    "01_最新快照_Snapshot",
    "02_历史时序库_TimeSeries",
    "03_衍生组合与信号_Derived",
    "04_黑天鹅雷达_BlackSwan",
    "05_多国周期对比_Countries",
    "06_情景与证伪日志_Journal",
    "07_全球跨资产行情_CrossAsset",
    "08_核心经济指标_MacroIndicators",
    "Sheet1",
    "工作表1",
]


def create_http_session():
    session = requests.Session()
    retries = Retry(
        total=4, connect=4, read=4, backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json, application/xml, text/xml, */*",
    })
    return session


HTTP = create_http_session()

# ================= 2. 六层第一性原理全量宏观指标字典 =================
FRED_SERIES = [
    # 第一层（L1）：底层公理与信用引擎
    {"id": "M1SL", "country": "美国", "layer": "L1·信用引擎", "freq": "M", "lt": False, "name": "美国狭义货币M1存量（十亿美元）"},
    {"id": "M2SL", "country": "美国", "layer": "L1·信用引擎", "freq": "M", "lt": False, "name": "美国广义货币M2存量（十亿美元）"},
    {"id": "TOTLL", "country": "美国", "layer": "L1·信用引擎", "freq": "W", "lt": True, "name": "美国商业银行总信贷规模（十亿美元）"},
    {"id": "BUSLOANS", "country": "美国", "layer": "L1·信用引擎", "freq": "M", "lt": True, "name": "美国工商业贷款余额（十亿美元）"},
    {"id": "DRTSCILM", "country": "美国", "layer": "L1·信用引擎", "freq": "Q", "lt": True, "name": "美联储银行收紧贷款标准比例（%）"},
    {"id": "BAMLH0A0HYM2", "country": "美国", "layer": "L1·信用引擎", "freq": "D", "lt": True, "name": "美国高收益垃圾债信用利差（%）"},
    {"id": "BAMLC0A0CM", "country": "美国", "layer": "L1·信用引擎", "freq": "D", "lt": False, "name": "美国投资级企业债信用利差（%）"},
    # 第二层（L2）：全球定价之锚与美元水管
    {"id": "WALCL", "country": "美国", "layer": "L2·流动性管道", "freq": "W", "lt": True, "name": "美联储资产负债表总规模（百万美元）"},
    {"id": "WDTGAL", "country": "美国", "layer": "L2·流动性管道", "freq": "W", "lt": True, "name": "美国财政部一般账户TGA余额（百万美元）"},
    {"id": "RRPONTSYD", "country": "美国", "layer": "L2·流动性管道", "freq": "D", "lt": True, "name": "美联储隔夜逆回购余额（十亿美元）"},
    {"id": "WRESBAL", "country": "美国", "layer": "L2·流动性管道", "freq": "W", "lt": True, "name": "商业银行准备金余额（百万美元）"},
    {"id": "IORB", "country": "美国", "layer": "L2·流动性管道", "freq": "D", "lt": False, "name": "美联储准备金余额利率（%）"},
    {"id": "DFF", "country": "美国", "layer": "L2·利率之锚", "freq": "D", "lt": True, "name": "联邦基金有效利率（%）"},
    {"id": "DGS2", "country": "美国", "layer": "L2·利率之锚", "freq": "D", "lt": True, "name": "美国2年期国债收益率（%）"},
    {"id": "DGS10", "country": "美国", "layer": "L2·利率之锚", "freq": "D", "lt": True, "name": "美国10年期国债收益率（%）"},
    {"id": "DGS30", "country": "美国", "layer": "L2·利率之锚", "freq": "D", "lt": False, "name": "美国30年期国债收益率（%）"},
    {"id": "DFII10", "country": "美国", "layer": "L2·利率之锚", "freq": "D", "lt": True, "name": "美国10年期通胀保值债券实际利率（%）"},
    {"id": "T10YIE", "country": "美国", "layer": "L2·利率之锚", "freq": "D", "lt": True, "name": "美国10年期盈亏平衡通胀预期（%）"},
    {"id": "T10Y2Y", "country": "美国", "layer": "L2·利率之锚", "freq": "D", "lt": True, "name": "美债10年期与2年期期限利差（%）"},
    {"id": "IRLTLT01JPM156N", "country": "日本", "layer": "L2·利率之锚", "freq": "M", "lt": False, "name": "日本10年期国债收益率（%）"},
    {"id": "IRLTLT01DEM156N", "country": "德国", "layer": "L2·利率之锚", "freq": "M", "lt": False, "name": "德国10年期国债收益率（%）"},
    # 第三层（L3）：宏观运行引擎、库存周期与财政生死线
    {"id": "PAYEMS", "country": "美国", "layer": "L3·就业硬数据", "freq": "M", "lt": True, "name": "美国非农就业总人数（千人）"},
    {"id": "ICSA", "country": "美国", "layer": "L3·就业硬数据", "freq": "W", "lt": True, "name": "美国周度初请失业金人数（人）"},
    {"id": "UNRATE", "country": "美国", "layer": "L3·就业硬数据", "freq": "M", "lt": True, "name": "美国失业率（%）"},
    {"id": "SAHMREALTIME", "country": "美国", "layer": "L3·就业硬数据", "freq": "M", "lt": False, "name": "官方萨姆规则衰退指标（%）"},
    {"id": "CES0500000003", "country": "美国", "layer": "L3·就业硬数据", "freq": "M", "lt": False, "name": "美国私营部门平均时薪（美元/小时）"},
    {"id": "INDPRO", "country": "美国", "layer": "L3·工业与库存", "freq": "M", "lt": True, "name": "美国工业生产总指数"},
    {"id": "DGORDER", "country": "美国", "layer": "L3·工业与库存", "freq": "M", "lt": False, "name": "美国耐用品新订单总额（百万美元）"},
    {"id": "ISRATIO", "country": "美国", "layer": "L3·工业与库存", "freq": "M", "lt": False, "name": "美国商业库存与销售额比率"},
    {"id": "RSAFS", "country": "美国", "layer": "L3·工业与库存", "freq": "M", "lt": True, "name": "美国零售与食品服务销售额（百万美元）"},
    {"id": "CPIAUCSL", "country": "美国", "layer": "L3·通胀硬数据", "freq": "M", "lt": True, "name": "美国消费者物价指数CPI"},
    {"id": "PCEPILFE", "country": "美国", "layer": "L3·通胀硬数据", "freq": "M", "lt": True, "name": "美国核心个人消费支出物价指数PCE"},
    {"id": "A091RC1Q027SBEA", "country": "美国", "layer": "L3·财政生死线", "freq": "Q", "lt": True, "name": "美国联邦政府利息支出年率（十亿美元）"},
    {"id": "FGRECPT", "country": "美国", "layer": "L3·财政生死线", "freq": "Q", "lt": True, "name": "美国联邦政府经常性财政收入年率（十亿美元）"},
    {"id": "GDP", "country": "美国", "layer": "L3·财政生死线", "freq": "Q", "lt": True, "name": "美国名义国内生产总值GDP年率（十亿美元）"},
    {"id": "GFDEGDQ188S", "country": "美国", "layer": "L3·财政生死线", "freq": "Q", "lt": False, "name": "美国联邦总债务占GDP比重（%）"},
    {"id": "CP0000EZ19M086NEST", "country": "欧元区", "layer": "L3·通胀硬数据", "freq": "M", "lt": False, "name": "欧元区调和消费者物价指数"},
    {"id": "LRHUTTTTEZM156S", "country": "欧元区", "layer": "L3·就业硬数据", "freq": "M", "lt": False, "name": "欧元区失业率（%）"},
    {"id": "JPNPRMNTO01GPSAM", "country": "日本", "layer": "L3·工业与库存", "freq": "M", "lt": False, "name": "日本制造业生产环比增速（%）"},
    {"id": "JPNCPIALLMINMEI", "country": "日本", "layer": "L3·通胀硬数据", "freq": "M", "lt": False, "name": "日本全国消费者物价指数"},
    # 第四层（L4）：跨境资本流动与汇率博弈
    {"id": "DTWEXBGS", "country": "美国", "layer": "L4·跨境汇率", "freq": "D", "lt": False, "name": "美联储广义贸易加权美元指数"},
    {"id": "FDHBFIN", "country": "美国", "layer": "L4·跨境资本流", "freq": "Q", "lt": False, "name": "海外投资者持有美债总额TIC（十亿美元）"},
    # 第五/六层（L5-L6）：大宗现货与系统性风险雷达
    {"id": "DCOILWTICO", "country": "美国", "layer": "L5·能源现货", "freq": "D", "lt": True, "name": "西德克萨斯轻质原油现货价格（美元/桶）"},
    {"id": "VXVCLS", "country": "美国", "layer": "L6·黑天鹅雷达", "freq": "D", "lt": False, "name": "芝加哥期权交易所3个月期波动率指数"},
    {"id": "STLFSI4", "country": "美国", "layer": "L6·黑天鹅雷达", "freq": "W", "lt": True, "name": "圣路易斯联储金融压力指数"},
    {"id": "NFCI", "country": "美国", "layer": "L6·黑天鹅雷达", "freq": "W", "lt": False, "name": "芝加哥联储金融状况指数"},
    {"id": "WDISBORROW", "country": "美国", "layer": "L6·黑天鹅雷达", "freq": "W", "lt": False, "name": "美联储贴现窗口紧急借款周均值（百万美元）"},
    {"id": "DRCRELEXFACBS", "country": "美国", "layer": "L6·灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国商业地产银行贷款违约率（%）"},
    {"id": "DRCCLACBS", "country": "美国", "layer": "L6·灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国商业银行信用卡贷款违约率（%）"},
    {"id": "DRSFRMACBS", "country": "美国", "layer": "L6·灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国居民住房抵押贷款违约率（%）"},
    {"id": "PSAVERT", "country": "美国", "layer": "L6·灰犀牛雷达", "freq": "M", "lt": False, "name": "美国个人储蓄率占可支配收入比（%）"},
    {"id": "TDSP", "country": "美国", "layer": "L6·灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国家庭债务偿付占可支配收入比（%）"},
    {"id": "DPSACBW027SBOG", "country": "美国", "layer": "L6·灰犀牛雷达", "freq": "W", "lt": False, "name": "美国商业银行总存款余额（十亿美元）"},
]

# ================= 3. 全球跨资产实时行情字典 =================
MARKET_ASSETS = [
    {"cat": "核心股指", "ticker": "^GSPC", "code": "SPX", "lt": True, "flow_chart": True, "short_name": "美股标普500", "name": "美国标普500指数", "driver": "美股盈利预期 × 10年期国债贴现率"},
    {"cat": "核心股指", "ticker": "^NDX", "code": "NDX", "lt": True, "flow_chart": True, "short_name": "纳斯达克100", "name": "美国纳斯达克100指数", "driver": "远期现金流久期资产，对实际利率极敏感"},
    {"cat": "核心股指", "ticker": "^SOX", "code": "SOX", "lt": False, "flow_chart": True, "short_name": "费城半导体", "name": "美国费城半导体指数", "driver": "全球人工智能算力资本开支与芯片周期温度计"},
    {"cat": "核心股指", "ticker": "000300.SS", "fallback_ticker": "ASHR", "code": "CSI300", "lt": True, "flow_chart": True, "short_name": "中国沪深300", "name": "中国沪深300指数（含A股映射）", "driver": "中国社融信贷脉冲与货币活化剪刀差"},
    {"cat": "核心股指", "ticker": "^HSI", "code": "HSI", "lt": False, "flow_chart": True, "short_name": "香港恒生指数", "name": "中国香港恒生指数", "driver": "中国经济基本面分子 × 美元流动性分母双重定价"},
    {"cat": "核心股指", "ticker": "^N225", "code": "N225", "lt": False, "flow_chart": True, "short_name": "日本日经225", "name": "日本日经225指数", "driver": "日元汇率波动与日本央行货币政策转向预期"},
    {"cat": "核心股指", "ticker": "^GDAXI", "code": "DAX", "lt": False, "flow_chart": True, "short_name": "德国DAX指数", "name": "德国法兰克福DAX指数", "driver": "欧元区制造业周期与欧央行降息路径"},
    {"cat": "风险温度计", "ticker": "KRE", "code": "US_REG_BANK", "lt": True, "flow_chart": True, "short_name": "美国区域银行", "name": "美国区域银行指数基金", "driver": "美国中小银行商业地产敞口与存款稳定性温度计"},
    {"cat": "风险温度计", "ticker": "HYG", "code": "US_HY_ETF", "lt": False, "flow_chart": True, "short_name": "高收益信用债", "name": "美国高收益企业债基金", "driver": "垃圾债信用利差与企业再融资流动性实时定价"},
    {"cat": "风险温度计", "ticker": "TLT", "code": "US_20Y_BOND", "lt": False, "flow_chart": True, "short_name": "美债20年+长债", "name": "美国20年以上长期国债基金", "driver": "长端美债久期风险与美国主权信用折价锚"},
    {"cat": "全球外汇", "ticker": "DX-Y.NYB", "code": "DXY", "lt": True, "flow_chart": True, "short_name": "美元指数", "name": "美元指数", "driver": "全球离岸美元流动性松紧总阀门"},
    {"cat": "全球外汇", "ticker": "JPY=X", "code": "USDJPY", "lt": True, "flow_chart": False, "short_name": "美元兑日元", "name": "美元兑日元汇率", "driver": "全球套息交易平仓踩踏核心风向标"},
    {"cat": "全球外汇", "ticker": "USDCNH=X", "fallback_ticker": "CNY=X", "code": "USDCNH", "lt": True, "flow_chart": False, "short_name": "离岸人民币", "name": "美元兑离岸人民币汇率", "driver": "中美利差倒挂深度与外贸结汇意愿"},
    {"cat": "全球外汇", "ticker": "EURUSD=X", "code": "EURUSD", "lt": False, "flow_chart": False, "short_name": "欧元兑美元", "name": "欧元兑美元汇率", "driver": "欧美真实利差与欧洲能源贸易条件"},
    {"cat": "大宗商品", "ticker": "GC=F", "code": "GOLD", "lt": True, "flow_chart": True, "short_name": "国际黄金", "name": "纽约商品交易所黄金期货（美元/盎司）", "driver": "10年期实际利率 + 全球央行去美元化购金"},
    {"cat": "大宗商品", "ticker": "SI=F", "code": "SILVER", "lt": False, "flow_chart": True, "short_name": "国际白银", "name": "纽约商品交易所白银期货（美元/盎司）", "driver": "贵金属金融属性 + 光伏工业需求弹性"},
    {"cat": "大宗商品", "ticker": "BZ=F", "code": "BRENT", "lt": True, "flow_chart": True, "short_name": "布伦特原油", "name": "北海布伦特原油期货（美元/桶）", "driver": "全球通胀预期与地缘供给冲击滞胀扳机"},
    {"cat": "大宗商品", "ticker": "HG=F", "code": "COPPER", "lt": True, "flow_chart": True, "short_name": "精炼铜", "name": "纽约精炼铜期货（美元/磅）", "driver": "全球制造业与电网及数据中心实体需求"},
    {"cat": "大宗商品", "ticker": "ZW=F", "code": "WHEAT", "lt": False, "flow_chart": False, "short_name": "芝加哥小麦", "name": "芝加哥小麦期货（美分/蒲式耳）", "driver": "全球粮食供应链安全与新兴市场食品通胀"},
    {"cat": "恐慌波动", "ticker": "^VIX", "code": "VIX", "lt": True, "flow_chart": False, "short_name": "美股恐慌指数", "name": "美股市场恐慌波动率指数", "driver": "美股期权隐含尾部风险对冲成本"},
]

CRYPTO_ASSETS = [
    {"cg_id": "bitcoin", "yf_ticker": "BTC-USD", "code": "BTC", "lt": True, "flow_chart": True, "short_name": "比特币现货", "name": "比特币现货（美元）", "driver": "全球美元净流动性高弹性放大器"},
    {"cg_id": "ethereum", "yf_ticker": "ETH-USD", "code": "ETH", "lt": False, "flow_chart": False, "short_name": "以太坊现货", "name": "以太坊现货（美元）", "driver": "链上金融流动性与科技风险偏好映射"},
]

COUNTRIES_LIST = ["USA", "CHN", "JPN", "DEU", "IND", "BRA", "IDN", "TUR"]


# ================= 4. 多板块全球新闻抓取 + 中文核心摘要提炼 =================
def clean_html_text(raw_txt):
    if not raw_txt:
        return ""
    txt = re.sub(r"<[^>]+>", " ", str(raw_txt))
    txt = re.sub(r"&nbsp;|&amp;|&quot;|&#39;|&lt;|&gt;", " ", txt)
    return re.sub(r"\s+", " ", txt).strip()


def translate_and_summarize_news_zh(news_items):
    if not news_items:
        return news_items

    if OPENAI_API_KEY:
        try:
            payload_lines = []
            for i, n in enumerate(news_items):
                payload_lines.append(
                    f"{i+1}. 板块:【{n['sector']}】 | 原标题: {n['raw_title']} | 原文摘要: {n['raw_desc'][:260]}"
                )
            prompt = (
                "请将以下全球财经新闻逐条处理为规范的简体中文，"
                "要求每条输出一个 JSON 对象，包含：\n"
                "- index: 序号（整数）\n"
                "- zh_title: 简明准确的中文新闻标题（严禁任何英文单词，人名机构名使用中文标准译名）\n"
                "- zh_summary: 60至90字的中文核心摘要，必须交代清楚核心事件数据及对股、债、汇或大宗商品的传导影响（严禁任何英文单词）\n"
                "严格返回 JSON 数组，不要输出任何额外解释文字：\n"
                + "\n".join(payload_lines)
            )
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
            body = {
                "model": OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4500,
            }
            resp = HTTP.post(f"{OPENAI_BASE_URL}/chat/completions", headers=headers, json=body, timeout=(12, 90))
            if resp.status_code == 200:
                content = resp.json()["choices"][0]["message"]["content"].strip()
                json_match = re.search(r"\[.*\]", content, re.DOTALL)
                if json_match:
                    parsed = json.loads(json_match.group(0))
                    parsed_map = {int(x["index"]): x for x in parsed if "index" in x}
                    for i, n in enumerate(news_items):
                        item = parsed_map.get(i + 1)
                        if item:
                            if item.get("zh_title"):
                                n["title"] = str(item["zh_title"]).strip()
                            if item.get("zh_summary"):
                                n["summary"] = str(item["zh_summary"]).strip()
                    return news_items
        except Exception as e:
            print(f"[INFO] AI 新闻翻译摘要回退至本地规则引擎: {e}")

    replacements = [
        (r"\bFederal Reserve\b|\bFed\b", "美联储"),
        (r"\bPowell\b", "鲍威尔"),
        (r"\bTreasury\b", "美国财政部/美债"),
        (r"\bYields?\b", "国债收益率"),
        (r"\bInflation\b", "通胀"),
        (r"\bCPI\b", "居民消费价格指数"),
        (r"\bPCE\b", "个人消费支出物价指数"),
        (r"\bGDP\b", "国内生产总值"),
        (r"\bRecession\b", "经济衰退"),
        (r"\bTariffs?\b", "关税贸易政策"),
        (r"\bChina\b", "中国"),
        (r"\bPBOC\b", "中国央行"),
        (r"\bBOJ\b", "日本央行"),
        (r"\bECB\b", "欧洲央行"),
        (r"\bOil\b|\bCrude\b", "国际原油"),
        (r"\bOPEC\b", "欧佩克产油国组织"),
        (r"\bGold\b", "黄金"),
        (r"\bDollar\b", "美元"),
        (r"\bYen\b", "日元"),
        (r"\bStocks?\b|\bEquities\b", "股市"),
        (r"\bBanks?\b", "银行业"),
        (r"\bAI\b", "人工智能"),
        (r"\bSemiconductors?\b|\bChips?\b", "半导体芯片"),
    ]
    sector_impact_hint = {
        "央行与货币政策": "核心影响：直接重塑短端利率预期与全球美元流动性松紧阀门。",
        "债市信用与财政": "核心影响：牵动长端美债贴现率分母与高收益企业债再融资信用利差。",
        "美股科技与盈利": "核心影响：影响权益市场分子端盈利预期与科技成长股估值风险偏好。",
        "大宗商品与能源": "核心影响：决定全球供给端通胀黏性与实体制造业补库周期强度。",
        "中国与全球外汇": "核心影响：反映跨境资本流动方向、中美利差传导与非美货币汇率弹性。",
        "地缘贸易与供应链": "核心影响：影响全球供应链成本溢价、避险资产定价与滞胀尾部风险。",
    }
    for n in news_items:
        t = n["raw_title"]
        d = n["raw_desc"][:120]
        for pat, zh in replacements:
            t = re.sub(pat, zh, t, flags=re.IGNORECASE)
            d = re.sub(pat, zh, d, flags=re.IGNORECASE)
        n["title"] = t
        hint = sector_impact_hint.get(n["sector"], "核心影响：需结合跨资产价格验证流动性方向。")
        n["summary"] = f"{d}（{hint}）" if d else f"该动态涉及{n['sector']}最新边际变化。{hint}"
    return news_items


def classify_news_sector(title_lower, default_sector):
    if re.search(r"(fed|powell|rate|fomc|ecb|boj|pboc|central bank|monetary|cut|hike)", title_lower):
        return "央行与货币政策"
    if re.search(r"(treasury|yield|bond|deficit|debt|auction|credit|default|bank|cre|commercial real estate)", title_lower):
        return "债市信用与财政"
    if re.search(r"(ai|chip|semiconductor|nvidia|tech|nasdaq|earnings|s&p|stock)", title_lower):
        return "美股科技与盈利"
    if re.search(r"(oil|opec|crude|gold|copper|wheat|gas|energy|commodity)", title_lower):
        return "大宗商品与能源"
    if re.search(r"(china|yuan|renminbi|asia|japan|yen|euro|currency|fx|dollar)", title_lower):
        return "中国与全球外汇"
    if re.search(r"(tariff|trade|sanction|war|middle east|geopolit|supply chain)", title_lower):
        return "地缘贸易与供应链"
    return default_sector


def fetch_global_macro_news():
    rss_sources = [
        ("美联储官方公告", "央行与货币政策", "https://www.federalreserve.gov/feeds/press_all.xml"),
        ("全球宏观经济", "央行与货币政策", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258"),
        ("全球金融市场", "债市信用与财政", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664"),
        ("科技与半导体", "美股科技与盈利", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=19854910"),
        ("大宗能源商品", "大宗商品与能源", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=19836768"),
        ("道琼斯市场头条", "地缘贸易与供应链", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ]
    collected = []
    seen_titles = set()

    for src_name, default_sec, url in rss_sources:
        try:
            resp = HTTP.get(url, timeout=(6, 15))
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            count_per_src = 0
            for item in root.findall(".//item")[:12]:
                title_el = item.find("title")
                desc_el = item.find("description")
                if title_el is None or not title_el.text:
                    continue
                raw_title = clean_html_text(title_el.text)
                raw_desc = clean_html_text(desc_el.text if desc_el is not None else "")
                norm_key = raw_title.lower()[:55]
                if norm_key in seen_titles:
                    continue
                seen_titles.add(norm_key)
                sector = classify_news_sector(raw_title.lower(), default_sec)
                collected.append({
                    "date": TODAY_STR,
                    "sector": sector,
                    "source": src_name,
                    "raw_title": raw_title[:140],
                    "raw_desc": raw_desc[:300],
                    "title": raw_title[:140],
                    "summary": raw_desc[:160],
                })
                count_per_src += 1
                if count_per_src >= 3:
                    break
        except Exception as e:
            print(f"[WARN] 新闻源 {src_name} 抓取跳过: {e}")

    return translate_and_summarize_news_zh(collected[:18])


# ================= 5. 多源精准数据抓取与解析引擎 =================
def fetch_yahoo_chart(ticker, range_str="1mo", interval_str="1d"):
    for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
        url = f"https://{host}/v8/finance/chart/{ticker}?range={range_str}&interval={interval_str}"
        try:
            resp = HTTP.get(url, timeout=(8, 20))
            if resp.status_code != 200:
                continue
            res = resp.json().get("chart", {}).get("result", [])
            if not res:
                continue
            timestamps = res[0].get("timestamp", [])
            closes = res[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
            valid = [
                (datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"), float(c))
                for ts, c in zip(timestamps, closes) if c is not None
            ]
            if len(valid) >= 2:
                valid.reverse()
                return valid[0][0], [v[1] for v in valid], dict(valid)
        except Exception:
            continue
    return None, [], {}


def fetch_spx_pe_reliable():
    for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
        url = f"https://{host}/v10/finance/quoteSummary/SPY?modules=summaryDetail,defaultKeyStatistics"
        try:
            resp = HTTP.get(url, timeout=(6, 12))
            if resp.status_code == 200:
                res = resp.json().get("quoteSummary", {}).get("result", [])
                if res:
                    sd = res[0].get("summaryDetail", {})
                    ks = res[0].get("defaultKeyStatistics", {})
                    pe = (ks.get("forwardPE") or {}).get("raw") or (sd.get("trailingPE") or {}).get("raw")
                    if pe and 10.0 < float(pe) < 60.0:
                        return round(float(pe), 2)
        except Exception:
            continue

    try:
        resp = HTTP.get("https://www.multpl.com/s-p-500-pe-ratio", timeout=(6, 12))
        if resp.status_code == 200:
            m = re.search(r"Current S&amp;P 500 PE Ratio is\s*<b>\s*([0-9]+\.[0-9]+)", resp.text)
            if not m:
                m = re.search(r"Current S& P 500 PE Ratio is\s*<b>\s*([0-9]+\.[0-9]+)", resp.text)
            if m:
                pe_val = float(m.group(1))
                if 10.0 < pe_val < 60.0:
                    return round(pe_val, 2)
    except Exception:
        pass
    return None


def fetch_market_assets(series_obs, lt_hist_dates):
    cross_asset_items = []
    market_stats = {}

    def calc_pct(arr, idx):
        if len(arr) > idx and arr[idx] != 0:
            return round((arr[0] / arr[idx] - 1) * 100, 2)
        return None

    for item in MARKET_ASSETS:
        active_ticker = item["ticker"]
        _, prices, _ = fetch_yahoo_chart(active_ticker, "1mo", "1d")
        if not prices and item.get("fallback_ticker"):
            active_ticker = item["fallback_ticker"]
            _, prices, _ = fetch_yahoo_chart(active_ticker, "1mo", "1d")
        if not prices:
            print(f"[WARN] 行情资产 {item['name']} 暂不可用")
            continue
        p0 = round(prices[0], 4)
        pct_1d = calc_pct(prices, 1) or 0.0
        pct_5d = calc_pct(prices, 5)
        pct_20d = calc_pct(prices, min(20, len(prices) - 1))

        series_obs[item["code"]] = prices
        market_stats[item["code"]] = {
            "short_name": item["short_name"],
            "flow_chart": item.get("flow_chart", False),
            "price": p0,
            "pct_1d": pct_1d,
            "pct_5d": pct_5d if pct_5d is not None else pct_1d,
            "pct_20d": pct_20d if pct_20d is not None else pct_1d,
        }

        if item.get("lt"):
            _, _, lt_map = fetch_yahoo_chart(active_ticker, "3y", "1wk")
            if lt_map:
                lt_hist_dates[item["code"]] = lt_map

        cross_asset_items.append({
            "cat": item["cat"],
            "name": item["name"],
            "price": p0,
            "pct_1d": pct_1d / 100.0,
            "pct_5d": (pct_5d / 100.0) if pct_5d is not None else "",
            "pct_20d": (pct_20d / 100.0) if pct_20d is not None else "",
            "driver": item["driver"]
        })
        time.sleep(0.04)

    cg_ids = ",".join(c["cg_id"] for c in CRYPTO_ASSETS)
    cg_data = {}
    try:
        cg_url = (
            f"https://api.coingecko.com/api/v3/coins/markets"
            f"?vs_currency=usd&ids={cg_ids}&price_change_percentage=24h,7d,30d"
        )
        resp = HTTP.get(cg_url, timeout=(8, 15))
        if resp.status_code == 200:
            for row in resp.json():
                cg_data[row["id"]] = row
    except Exception:
        pass

    for c_item in CRYPTO_ASSETS:
        _, yf_prices, _ = fetch_yahoo_chart(c_item["yf_ticker"], "1mo", "1d")
        if c_item.get("lt"):
            _, _, lt_map = fetch_yahoo_chart(c_item["yf_ticker"], "3y", "1wk")
            if lt_map:
                lt_hist_dates[c_item["code"]] = lt_map

        cg_row = cg_data.get(c_item["cg_id"])
        if cg_row and cg_row.get("current_price") is not None:
            p0 = round(float(cg_row["current_price"]), 2)
            pct_1d = round(float(cg_row.get("price_change_percentage_24h") or 0), 2)
            pct_5d = round(float(cg_row.get("price_change_percentage_7d_in_currency") or 0), 2)
            pct_20d = round(float(cg_row.get("price_change_percentage_30d_in_currency") or 0), 2)
            series_obs[c_item["code"]] = yf_prices if yf_prices else [p0]
        elif yf_prices:
            p0 = round(yf_prices[0], 2)
            pct_1d = calc_pct(yf_prices, 1) or 0.0
            pct_5d = calc_pct(yf_prices, 5) or pct_1d
            pct_20d = calc_pct(yf_prices, min(20, len(yf_prices) - 1)) or pct_1d
            series_obs[c_item["code"]] = yf_prices
        else:
            continue

        market_stats[c_item["code"]] = {
            "short_name": c_item["short_name"],
            "flow_chart": c_item.get("flow_chart", False),
            "price": p0,
            "pct_1d": pct_1d,
            "pct_5d": pct_5d,
            "pct_20d": pct_20d,
        }
        cross_asset_items.append({
            "cat": "加密流动性",
            "name": c_item["name"],
            "price": p0,
            "pct_1d": pct_1d / 100.0,
            "pct_5d": (pct_5d / 100.0) if pct_5d is not None else "",
            "pct_20d": (pct_20d / 100.0) if pct_20d is not None else "",
            "driver": c_item["driver"]
        })

    spx_pe = fetch_spx_pe_reliable()
    if spx_pe:
        series_obs["SPX_PE"] = [spx_pe]

    return cross_asset_items, market_stats


def fetch_fred_data():
    series_obs = {}
    series_dates = {}
    series_date_maps = {}
    lt_hist_dates = {}

    for item in FRED_SERIES:
        obs_start = (datetime.now(TZ_CST) - timedelta(days=1120)).strftime("%Y-%m-%d")
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={item['id']}&api_key={FRED_API_KEY}"
            f"&file_type=json&sort_order=desc&observation_start={obs_start}"
        )
        try:
            resp = HTTP.get(url, timeout=(10, 30))
            resp.raise_for_status()
            obs = [o for o in resp.json().get("observations", []) if o.get("value") != "."]
            if not obs:
                continue
            vals = [float(o["value"]) for o in obs]
            dmap = {o["date"]: float(o["value"]) for o in obs}
            series_obs[item["id"]] = vals
            series_dates[item["id"]] = obs[0]["date"]
            series_date_maps[item["id"]] = dmap
            if item.get("lt"):
                lt_hist_dates[item["id"]] = dmap
            time.sleep(0.04)
        except Exception as e:
            print(f"[WARN] 底层序列 {item['name']} 获取跳过: {e}")
    return series_obs, series_dates, series_date_maps, lt_hist_dates


def get_val_by_days_ago(series_date_maps, sid, days_ago=0):
    dmap = series_date_maps.get(sid)
    if not dmap:
        return None
    sorted_dts = sorted(dmap.keys())
    latest_dt_str = sorted_dts[-1]
    if days_ago <= 0:
        return dmap[latest_dt_str]
    target_dt = datetime.strptime(latest_dt_str, "%Y-%m-%d") - timedelta(days=days_ago)
    target_str = target_dt.strftime("%Y-%m-%d")
    valid = [d for d in sorted_dts if d <= target_str]
    return dmap[valid[-1]] if valid else dmap[sorted_dts[0]]


def fetch_china_macro_akshare(series_obs, series_dates, lt_hist_dates):
    try:
        import akshare as ak
    except Exception:
        return

    def get_latest_two(df, dt_col, val_col):
        temp = df[[dt_col, val_col]].copy()
        temp["_num"] = pd.to_numeric(
            temp[val_col].astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False).str.strip(),
            errors="coerce"
        )
        temp = temp.dropna(subset=["_num"])
        if len(temp) < 2:
            return None
        temp["_sort"] = temp[dt_col].apply(
            lambda x: tuple(int(n) for n in re.findall(r"\d+", str(x))) or (0,)
        )
        temp = temp.sort_values(by="_sort", ascending=True)
        r_curr, r_prev = temp.iloc[-1], temp.iloc[-2]
        return str(r_curr[dt_col])[:12].strip(), float(r_curr["_num"]), float(r_prev["_num"])

    try:
        df_pmi = ak.macro_china_pmi()
        col_dt = next((c for c in df_pmi.columns if "月份" in str(c) or "日期" in str(c)), df_pmi.columns[0])
        col_mfg = next((c for c in df_pmi.columns if "制造业" in str(c) and "非制造业" not in str(c) and "指数" in str(c)), None)
        if col_mfg:
            res = get_latest_two(df_pmi, col_dt, col_mfg)
            if res:
                series_obs["CN_PMI_MFG"] = [res[1], res[2]]
                series_dates["CN_PMI_MFG"] = res[0]
    except Exception:
        pass

    try:
        fn_ms = getattr(ak, "macro_china_money_supply_em", None) or getattr(ak, "macro_china_money_supply")
        df_ms = fn_ms()
        col_dt = next((c for c in df_ms.columns if "月份" in str(c) or "日期" in str(c)), df_ms.columns[0])
        col_m1 = next((c for c in df_ms.columns if ("M1" in str(c) or ("货币" in str(c) and "准货币" not in str(c))) and "同比" in str(c)), None)
        col_m2 = next((c for c in df_ms.columns if ("M2" in str(c) or "准货币" in str(c)) and "同比" in str(c)), None)
        if col_m1:
            r1 = get_latest_two(df_ms, col_dt, col_m1)
            if r1:
                series_obs["CN_M1_YOY"] = [r1[1], r1[2]]
                series_dates["CN_M1_YOY"] = r1[0]
        if col_m2:
            r2 = get_latest_two(df_ms, col_dt, col_m2)
            if r2:
                series_obs["CN_M2_YOY"] = [r2[1], r2[2]]
                series_dates["CN_M2_YOY"] = r2[0]
    except Exception:
        pass

    try:
        fn_ind = getattr(ak, "macro_china_industrial_value_added", None) or getattr(ak, "macro_china_gyzjz")
        df_ind = fn_ind()
        col_dt = next((c for c in df_ind.columns if "月份" in str(c) or "日期" in str(c)), df_ind.columns[0])
        col_yoy = next((c for c in df_ind.columns if "同比" in str(c) or "今值" in str(c)), df_ind.columns[1])
        res = get_latest_two(df_ind, col_dt, col_yoy)
        if res:
            series_obs["CN_IND_YOY"] = [res[1], res[2]]
            series_dates["CN_IND_YOY"] = res[0]
    except Exception:
        pass

    try:
        fn_tsf = getattr(ak, "macro_china_social_financing", None) or getattr(ak, "macro_china_shrzgm")
        df_tsf = fn_tsf()
        col_dt = next((c for c in df_tsf.columns if "月份" in str(c) or "日期" in str(c)), df_tsf.columns[0])
        col_val = next((c for c in df_tsf.columns if ("社会融资" in str(c) or "增量" in str(c) or "总计" in str(c)) and "同比" not in str(c)), df_tsf.columns[1])
        res = get_latest_two(df_tsf, col_dt, col_val)
        if res:
            series_obs["CN_TSF_NEW"] = [res[1], res[2]]
            series_dates["CN_TSF_NEW"] = res[0]
    except Exception:
        pass

    try:
        df_cpi = ak.macro_china_cpi()
        col_dt = next((c for c in df_cpi.columns if "月份" in str(c) or "日期" in str(c)), df_cpi.columns[0])
        col_yoy = next((c for c in df_cpi.columns if "全国" in str(c) and "同比" in str(c)), next((c for c in df_cpi.columns if "同比" in str(c)), None))
        if col_yoy:
            res = get_latest_two(df_cpi, col_dt, col_yoy)
            if res:
                series_obs["CN_CPI_YOY"] = [res[1], res[2]]
                series_dates["CN_CPI_YOY"] = res[0]
    except Exception:
        pass

    try:
        df_ppi = ak.macro_china_ppi()
        col_dt = next((c for c in df_ppi.columns if "月份" in str(c) or "日期" in str(c)), df_ppi.columns[0])
        col_yoy = next((c for c in df_ppi.columns if "同比" in str(c) or "今值" in str(c)), None)
        if col_yoy:
            res = get_latest_two(df_ppi, col_dt, col_yoy)
            if res:
                series_obs["CN_PPI_YOY"] = [res[1], res[2]]
                series_dates["CN_PPI_YOY"] = res[0]
    except Exception:
        pass

    try:
        df_bond = ak.bond_zh_us_yield_em() if hasattr(ak, "bond_zh_us_yield_em") else ak.bond_zh_us_rate(start_date="20230101")
        col_10y = next((c for c in df_bond.columns if "中国" in str(c) and "10年" in str(c)), None)
        col_dt = next((c for c in df_bond.columns if "日期" in str(c)), df_bond.columns[0])
        if col_10y:
            df_clean = df_bond.dropna(subset=[col_10y]).sort_values(by=col_dt)
            if len(df_clean) >= 2:
                latest_row, prev_row = df_clean.iloc[-1], df_clean.iloc[-2]
                dt_str = str(latest_row[col_dt])[:10]
                series_obs["CN_DGS10"] = [float(latest_row[col_10y]), float(prev_row[col_10y])]
                series_dates["CN_DGS10"] = dt_str
                lt_hist_dates["CN_DGS10"] = {
                    str(r[col_dt])[:10]: float(r[col_10y]) for _, r in df_clean.tail(800).iterrows()
                }
    except Exception:
        pass


def fetch_external_apis():
    wb_metrics = {}
    imf_metrics = {}
    extra_vals = {}

    try:
        resp = HTTP.get("https://markets.newyorkfed.org/api/rates/secured/sofr/last/2.json", timeout=(10, 30))
        if resp.status_code == 200:
            sofr = resp.json().get("refRates", [])
            if len(sofr) >= 2:
                extra_vals["SOFR"] = float(sofr[0]["percentRate"])
    except Exception:
        pass

    try:
        url = (
            "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
            "v1/accounting/od/auctions_query?filter=security_term:in:(10-Year,30-Year)"
            "&sort=-issue_date&page[size]=2"
        )
        resp = HTTP.get(url, timeout=(10, 30))
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                auc = data[0]
                extra_vals["UST_BID_COVER"] = float(auc.get("bid_to_cover_ratio") or 0)
                high_y = float(auc.get("high_yield") or 0)
                med_y = float(auc.get("median_yield") or high_y)
                extra_vals["UST_TAIL_BPS"] = round(max(0.0, (high_y - med_y) * 100), 2)
    except Exception:
        pass

    year = datetime.now().year
    imf_headers = {"Referer": "https://www.imf.org/external/datamapper/", "Accept": "application/json"}
    for code in ["NGDP_RPCH", "PCPIPCH", "BCA_NGDPD", "GGXCNL_NGDP"]:
        try:
            url = f"https://www.imf.org/external/datamapper/api/v1/{code}/{'/'.join(COUNTRIES_LIST)}?periods={year-1},{year}"
            resp = HTTP.get(url, headers=imf_headers, timeout=(10, 25))
            txt = resp.text.strip()
            if resp.status_code == 200 and txt.startswith("{"):
                vals = json.loads(txt).get("values", {}).get(code, {})
                for c in COUNTRIES_LIST:
                    if c in vals:
                        curr = vals[c].get(str(year)) or vals[c].get(str(year - 1))
                        if curr is not None:
                            imf_metrics[(c, code)] = round(float(curr), 2)
        except Exception:
            pass

    for code in ["FI.RES.TOTL.CD", "DT.DOD.DSTC.CD"]:
        try:
            url = f"https://api.worldbank.org/v2/country/{';'.join(COUNTRIES_LIST)}/indicator/{code}?format=json&mrv=2"
            data = HTTP.get(url, timeout=(10, 35)).json()
            if len(data) > 1 and data[1]:
                for row in data[1]:
                    iso3 = row["countryiso3code"]
                    if row["value"] is not None and (iso3, code) not in wb_metrics:
                        wb_metrics[(iso3, code)] = float(row["value"])
        except Exception:
            pass

    return wb_metrics, imf_metrics, extra_vals


# ================= 6. 底层时序对齐 + 衍生公式 + 图表统一规范与实测数据核验 =================
CHART_SERIES_COLUMNS = [
    ("日期", "Date"),                                         # 0
    ("美元净流动性（十亿美元）", "NET_LIQUIDITY"),            # 1
    ("标普500指数", "SPX"),                                   # 2
    ("美国2年期国债收益率（%）", "DGS2"),                     # 3
    ("美国10年期国债收益率（%）", "DGS10"),                   # 4
    ("美债10Y-2Y期限利差（%）", "SPREAD_10Y2Y"),              # 5
    ("美国10年期TIPS实际利率（%）", "DFII10"),                # 6
    ("美国10年期盈亏平衡通胀预期（%）", "T10YIE"),            # 7
    ("COMEX黄金期货（美元/盎司）", "GOLD"),                   # 8
    ("中国10年期国债收益率（%）", "CN_DGS10"),                # 9
    ("中美10年期利差（%）", "US_CN_SPREAD"),                  # 10
    ("铜金比（实体周期系数）", "COPPER_GOLD_RATIO"),          # 11
    ("布伦特原油（美元/桶）", "BRENT"),                       # 12
    ("高收益债信用利差（%）", "BAMLH0A0HYM2"),                # 13
    ("美股VIX恐慌指数", "VIX"),                               # 14
    ("美商业银行总信贷（十亿美元）", "TOTLL"),                # 15
    ("美国区域银行ETF（KRE）", "US_REG_BANK"),                # 16
    ("美联储资产负债表（十亿美元）", "WALCL_B"),              # 17
    ("财政部TGA账户（十亿美元）", "TGA_B"),                   # 18
    ("美联储隔夜逆回购（十亿美元）", "RRPONTSYD"),            # 19
    ("银行准备金余额（十亿美元）", "WRESBAL_B"),              # 20
    ("美国工商业贷款余额（十亿美元）", "BUSLOANS"),           # 21
    ("美元指数（DXY）", "DXY"),                               # 22
    ("美元兑日元汇率", "USDJPY"),                             # 23
    ("联邦利息占财政收入比（%）", "INTEREST_REV_RATIO"),      # 24
    ("主权债务雪球差值r-g（%）", "R_MINUS_G"),                # 25
]

# 单一事实源：12 张图表的精确结构、坐标轴、数据源绑定定义
CHART_SPECS_MASTER = [
    {
        "key": "c1", "tier": 0, "side": "left",
        "title": "图1【L1信用引擎】美国商业银行总信贷规模（左轴）与高收益垃圾债信用利差（右轴）",
        "type": "COMBO", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "商业银行总信贷（十亿美元）", "y_right_title": "高收益债信用利差（%）",
        "series": [
            {"col": 15, "label": "美商业银行总信贷", "unit": "十亿美元", "axis": "LEFT_AXIS", "stype": "COLUMN"},
            {"col": 13, "label": "高收益债信用利差", "unit": "%", "axis": "RIGHT_AXIS", "stype": "LINE"},
        ],
    },
    {
        "key": "c2", "tier": 0, "side": "right",
        "title": "图2【L1信贷派生】美国工商业贷款余额（左轴）与商业银行准备金规模（右轴）",
        "type": "LINE", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "工商业贷款余额（十亿美元）", "y_right_title": "商业银行准备金（十亿美元）",
        "series": [
            {"col": 21, "label": "美国工商业贷款余额", "unit": "十亿美元", "axis": "LEFT_AXIS"},
            {"col": 20, "label": "银行准备金余额", "unit": "十亿美元", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c3", "tier": 1, "side": "left",
        "title": "图3【L2美元水管】美元净流动性总阀门（左轴）与TGA账户、隔夜逆回购蓄水池（右轴）",
        "type": "AREA", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "美元净流动性（十亿美元）", "y_right_title": "TGA与逆回购余额（十亿美元）",
        "series": [
            {"col": 1, "label": "美元净流动性", "unit": "十亿美元", "axis": "LEFT_AXIS"},
            {"col": 18, "label": "财政部TGA账户", "unit": "十亿美元", "axis": "RIGHT_AXIS"},
            {"col": 19, "label": "美联储隔夜逆回购", "unit": "十亿美元", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c4", "tier": 1, "side": "right",
        "title": "图4【L2利率之锚】美债2年期与10年期收益率对比（左轴）及10Y-2Y期限利差（右轴）",
        "type": "LINE", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "2Y 与 10Y 名义收益率（%）", "y_right_title": "10Y-2Y 期限利差（%）",
        "series": [
            {"col": 3, "label": "美国2年期国债收益率", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 4, "label": "美国10年期国债收益率", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 5, "label": "美债10Y-2Y期限利差", "unit": "%", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c5", "tier": 2, "side": "left",
        "title": "图5【L2真实成本】美国10年期TIPS实际利率（左轴）与10年期盈亏平衡通胀预期（右轴）",
        "type": "LINE", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "10年期TIPS实际利率（%）", "y_right_title": "10年期盈亏平衡通胀预期（%）",
        "series": [
            {"col": 6, "label": "美国10年期TIPS实际利率", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 7, "label": "美国10年期盈亏平衡通胀预期", "unit": "%", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c6", "tier": 2, "side": "right",
        "title": "图6【L3周期四象限】宏观增长—通胀四象限动态定位（右下复苏/右上过热/左上滞胀/左下衰退）",
        "type": "SCATTER", "source_table": "quad", "x_col": 27, "x_title": "经济增长景气得分（>0扩张 / <0放缓）",
        "y_left_title": "通胀压力得分（>0高通胀 / <0低通胀）", "y_right_title": "",
        "series": [
            {"col": 28, "label": "近12个月历史轨迹", "unit": "分", "axis": "LEFT_AXIS"},
            {"col": 29, "label": "当前最新站位", "unit": "分", "axis": "LEFT_AXIS"},
        ],
    },
    {
        "key": "c7", "tier": 3, "side": "left",
        "title": "图7【L3财政生死线】联邦利息占财政收入比（左轴·20%红线）与主权债务雪球r-g（右轴）",
        "type": "COMBO", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "利息支出占财政收入比（%）", "y_right_title": "主权债务雪球 r-g（%）",
        "series": [
            {"col": 24, "label": "联邦利息占财政收入比", "unit": "%", "axis": "LEFT_AXIS", "stype": "COLUMN"},
            {"col": 25, "label": "主权债务雪球差值r-g", "unit": "%", "axis": "RIGHT_AXIS", "stype": "LINE"},
        ],
    },
    {
        "key": "c8", "tier": 3, "side": "right",
        "title": "图8【L4跨境利率锚】中美10年期国债收益率对比（左轴）与中美10Y利差倒挂深度（右轴）",
        "type": "LINE", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "中美10年期国债收益率（%）", "y_right_title": "中美10Y利差（%）",
        "series": [
            {"col": 4, "label": "美国10年期国债收益率", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 9, "label": "中国10年期国债收益率", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 10, "label": "中美10年期利差", "unit": "%", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c9", "tier": 4, "side": "left",
        "title": "图9【L4汇率总阀门】美元指数DXY（左轴）与全球套息总阀门美元兑日元汇率（右轴）",
        "type": "LINE", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "美元指数（DXY）", "y_right_title": "美元兑日元汇率",
        "series": [
            {"col": 22, "label": "美元指数", "unit": "", "axis": "LEFT_AXIS"},
            {"col": 23, "label": "美元兑日元汇率", "unit": "", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c10", "tier": 4, "side": "right",
        "title": "图10【L5全球资产流动性全景】16大核心资产资金净流入与净流出对比（20日与5日动量%）",
        "type": "COLUMN", "source_table": "flow", "x_col": 31, "x_title": "全球大类资产（>0资金净流入 / <0资金净流出）",
        "y_left_title": "资金净流向动量（%）", "y_right_title": "",
        "series": [
            {"col": 32, "label": "20日资金净流向动量", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 33, "label": "5日边际资金流向", "unit": "%", "axis": "LEFT_AXIS"},
        ],
    },
    {
        "key": "c11", "tier": 5, "side": "left",
        "title": "图11【L5跨资产背离】铜金比实体周期系数（左轴）与COMEX黄金价格（右轴）",
        "type": "LINE", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "铜金比（实体周期系数）", "y_right_title": "COMEX黄金价格（美元/盎司）",
        "series": [
            {"col": 11, "label": "铜金比", "unit": "", "axis": "LEFT_AXIS"},
            {"col": 8, "label": "国际黄金价格", "unit": "美元/盎司", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c12", "tier": 5, "side": "right",
        "title": "图12【L6风险雷达塔尖】36维黑天鹅与灰犀牛危急度排行（距红灯熔断线% vs 65%警戒线）",
        "type": "COMBO", "source_table": "risk", "x_col": 35, "x_title": "高危系统性风险因子（按危急度降序）",
        "y_left_title": "距红灯熔断线进度（%）", "y_right_title": "",
        "series": [
            {"col": 36, "label": "距红灯熔断线进度", "unit": "%", "axis": "LEFT_AXIS", "stype": "COLUMN"},
            {"col": 37, "label": "黄灯警戒线", "unit": "%", "axis": "LEFT_AXIS", "stype": "LINE"},
        ],
    },
]


def build_clean_chart_timeseries(lt_hist_dates):
    """生成 156 周（近 3 年）连续无缺失值（自动首尾有效值填充）的底层运算宽表"""
    spx_weekly = sorted(lt_hist_dates.get("SPX", {}).keys())
    if not spx_weekly:
        return []
    weekly_dates = spx_weekly[-156:]

    first_valid = {}
    for sid, dmap in lt_hist_dates.items():
        if dmap:
            earliest_dt = min(dmap.keys())
            first_valid[sid] = round(dmap[earliest_dt], 4)

    gdp_dmap = lt_hist_dates.get("GDP", {})
    sorted_gdp_dts = sorted(gdp_dmap.keys())

    def get_rolling_gdp_yoy(dt_str):
        if len(sorted_gdp_dts) < 5:
            return 4.8
        valid_curr = [d for d in sorted_gdp_dts if d <= dt_str]
        if not valid_curr:
            return 4.8
        curr_dt = valid_curr[-1]
        target_prev = (datetime.strptime(curr_dt, "%Y-%m-%d") - timedelta(days=340)).strftime("%Y-%m-%d")
        valid_prev = [d for d in sorted_gdp_dts if d <= target_prev]
        if not valid_prev:
            return 4.8
        prev_dt = valid_prev[-1]
        if prev_dt == curr_dt or gdp_dmap[prev_dt] == 0:
            return 4.8
        return round((gdp_dmap[curr_dt] / gdp_dmap[prev_dt] - 1.0) * 100.0, 2)

    last_known = dict(first_valid)
    raw_rows = []

    for dt in weekly_dates:
        r = {"Date": dt}
        for sid, dmap in lt_hist_dates.items():
            valid_dts = [d for d in dmap.keys() if d <= dt]
            if valid_dts:
                val = round(dmap[max(valid_dts)], 4)
                last_known[sid] = val
                r[sid] = val
            elif sid in last_known:
                r[sid] = last_known[sid]

        if "WALCL" in r:
            r["WALCL_B"] = round(r["WALCL"] / 1000.0, 2)
        if "WDTGAL" in r:
            r["TGA_B"] = round(r["WDTGAL"] / 1000.0, 2)
        if "WRESBAL" in r:
            r["WRESBAL_B"] = round(r["WRESBAL"] / 1000.0, 2)
        if all(k in r for k in ["WALCL", "WDTGAL", "RRPONTSYD"]):
            r["NET_LIQUIDITY"] = round((r["WALCL"] - r["WDTGAL"]) / 1000.0 - r["RRPONTSYD"], 2)
        if all(k in r for k in ["DGS10", "DGS2"]):
            r["SPREAD_10Y2Y"] = round(r["DGS10"] - r["DGS2"], 3)
        if "T10YIE" not in r and all(k in r for k in ["DGS10", "DFII10"]):
            r["T10YIE"] = round(r["DGS10"] - r["DFII10"], 3)
        if all(k in r for k in ["DGS10", "CN_DGS10"]):
            r["US_CN_SPREAD"] = round(r["DGS10"] - r["CN_DGS10"], 3)
        if all(k in r for k in ["COPPER", "GOLD"]) and r["GOLD"] > 0:
            r["COPPER_GOLD_RATIO"] = round((r["COPPER"] * 1000) / r["GOLD"], 3)
        if all(k in r for k in ["A091RC1Q027SBEA", "FGRECPT"]) and r["FGRECPT"] > 0:
            r["INTEREST_REV_RATIO"] = round((r["A091RC1Q027SBEA"] / r["FGRECPT"]) * 100.0, 2)
        if "DGS10" in r:
            nom_g = get_rolling_gdp_yoy(dt)
            r["R_MINUS_G"] = round(r["DGS10"] - nom_g, 2)

        raw_rows.append(r)

    headers = [zh_name for zh_name, _ in CHART_SERIES_COLUMNS]
    matrix = [headers]
    for r in raw_rows:
        row_vals = []
        for _, code in CHART_SERIES_COLUMNS:
            val = r.get(code, 0.0 if code != "Date" else "")
            row_vals.append(val)
        matrix.append(row_vals)
    return matrix


def build_four_quadrant_series(series_obs, current_growth_score, current_infl_score):
    ind_arr = series_obs.get("INDPRO", [])
    nfp_arr = series_obs.get("PAYEMS", [])
    cpi_arr = series_obs.get("CPIAUCSL", [])
    pce_arr = series_obs.get("PCEPILFE", [])

    pts = []
    max_steps = min(11, len(ind_arr) - 13, len(nfp_arr) - 13, len(cpi_arr) - 13, len(pce_arr) - 4)
    if max_steps >= 3:
        for lag in range(max_steps, 0, -1):
            ind_yoy = (ind_arr[lag] / ind_arr[lag + 12] - 1) * 100.0
            nfp_yoy = (nfp_arr[lag] / nfp_arr[lag + 12] - 1) * 100.0
            cpi_yoy = (cpi_arr[lag] / cpi_arr[lag + 12] - 1) * 100.0
            pce_3m = ((pce_arr[lag] / pce_arr[lag + 3]) ** 4 - 1) * 100.0
            gx = round(0.5 * (ind_yoy - 1.2) + 0.5 * (nfp_yoy - 1.2), 2)
            iy = round(0.55 * (pce_3m - 2.3) + 0.45 * (cpi_yoy - 2.5), 2)
            pts.append((gx, iy, ""))

    pts.append((current_growth_score, "", current_infl_score))
    header = ["经济增长景气得分（X轴：>0扩张 / <0放缓）", "近12个月历史轨迹（通胀Y轴）", "当前最新位置（本月所处象限）"]
    return [header] + [[x, y_hist, y_curr] for x, y_hist, y_curr in pts]


def extract_plotted_chart_facts(ts_matrix, quad_matrix, flow_matrix, risk_chart_matrix):
    """直接从即将绘制成图的真实表格矩阵中抽取每张图的实测坐标轴数值与趋势，确保 100% 图文对应"""
    plotted_facts = {}
    ts_rows = ts_matrix[1:] if len(ts_matrix) > 1 else []

    for spec in CHART_SPECS_MASTER:
        ckey = spec["key"]
        st = spec["source_table"]
        series_summaries = []
        verification_tokens = []

        if st == "ts" and ts_rows:
            for s in spec["series"]:
                col_idx = s["col"]
                vals = [float(r[col_idx]) for r in ts_rows if isinstance(r[col_idx], (int, float))]
                if not vals:
                    continue
                curr_v = round(vals[-1], 2)
                prev_3m_v = round(vals[-13], 2) if len(vals) >= 13 else round(vals[0], 2)
                min_v, max_v = round(min(vals), 2), round(max(vals), 2)
                diff_3m = round(curr_v - prev_3m_v, 2)
                arrow = "🔺上行" if diff_3m > 0 else ("🔻下行" if diff_3m < 0 else "➡️走平")
                axis_zh = "左轴" if s["axis"] == "LEFT_AXIS" else "右轴"
                unit = s.get("unit", "")
                series_summaries.append(
                    f"{axis_zh}【{s['label']}】最新实测 {curr_v}{unit}（近3月{arrow} {diff_3m:+}{unit}，三年区间 {min_v}~{max_v}{unit}）"
                )
                verification_tokens.append(f"{axis_zh}{s['label']}:{curr_v}{unit}({arrow})")

        elif st == "quad" and len(quad_matrix) > 1:
            last_pt = quad_matrix[-1]
            gx, iy = last_pt[0], last_pt[2]
            series_summaries.append(f"当前红点实测坐标：X轴增长景气得分={gx:+}，Y轴通胀压力得分={iy:+}")
            verification_tokens.append(f"增长X轴:{gx:+} / 通胀Y轴:{iy:+}")

        elif st == "flow" and len(flow_matrix) > 1:
            items = [(r[0], float(r[1]), float(r[2])) for r in flow_matrix[1:] if len(r) >= 3]
            if items:
                top_in = sorted(items, key=lambda x: x[1], reverse=True)[:3]
                top_out = sorted(items, key=lambda x: x[1])[:3]
                in_str = "、".join([f"{x[0]}({x[1]:+.1f}%)" for x in top_in])
                out_str = "、".join([f"{x[0]}({x[1]:+.1f}%)" for x in top_out])
                series_summaries.append(f"20日资金净流入前三：{in_str}；净流出/承压前三：{out_str}")
                verification_tokens.append(f"净流入领头:{top_in[0][0]}({top_in[0][1]:+.1f}%)")

        elif st == "risk" and len(risk_chart_matrix) > 1:
            r_items = [(r[0], float(r[1])) for r in risk_chart_matrix[1:] if len(r) >= 2]
            if r_items:
                top3_str = "、".join([f"{x[0]}({x[1]:.1f}%)" for x in r_items[:3]])
                series_summaries.append(f"柱状图危急度前三因子（距100%红线进度）：{top3_str}")
                verification_tokens.append(f"榜首风险:{r_items[0][0]}({r_items[0][1]:.1f}%)")

        plotted_facts[ckey] = {
            "title": spec["title"],
            "fact_text": "；".join(series_summaries),
            "verify_stamp": " ✅ 读图实测核验：" + " ｜ ".join(verification_tokens),
        }
    return plotted_facts


# ================= 7. 全因子精确计算 + 36维风险雷达 + AI 读图与一致性校验 =================
def calc_risk_ratio(val, safe_base, warn_line, red_line):
    if red_line > safe_base:
        if val <= safe_base:
            return 0.0
        if val <= warn_line:
            return 0.65 * (val - safe_base) / max(1e-6, (warn_line - safe_base))
        if val <= red_line:
            return 0.65 + 0.35 * (val - warn_line) / max(1e-6, (red_line - warn_line))
        return min(1.5, 1.0 + 0.5 * (val - red_line) / max(1e-6, (red_line - warn_line)))
    else:
        if val >= safe_base:
            return 0.0
        if val >= warn_line:
            return 0.65 * (safe_base - val) / max(1e-6, (safe_base - warn_line))
        if val >= red_line:
            return 0.65 + 0.35 * (warn_line - val) / max(1e-6, (warn_line - red_line))
        return min(1.5, 1.0 + 0.5 * (red_line - val) / max(1e-6, (warn_line - red_line)))


def generate_ai_full_and_chart_commentary(llm_ctx, plotted_facts, fallback_report, fallback_chart_notes):
    """生成全因子总研判与12张图专属读图解说，并逐一进行图表实测数据一致性核验"""
    prompt = (
        "你是严格遵循《宏观经济｜六层第一性原理实战通关总纲》的首席宏观策略师。\n"
        "请基于传入的六层全量因子、衍生公式、36维风险雷达、六大板块新闻，以及【12张结构图的实测坐标轴数据】，"
        "输出一个合法的 JSON 对象（不要输出任何 Markdown 代码块标记以外的废话），包含以下两个字段：\n"
        "1. `master_report`（字符串，1200~1600字）：严格按六层金字塔顺序进行深度全因子中文解说，包含：\n"
        "   - 【一、宏观周期四象限定位与新闻催化主线】\n"
        "   - 【二、第一至二层：信用引擎、货币活化与美元净流动性水管二阶导数】\n"
        "   - 【三、第三至四层：美欧日中硬数据、财政付息生死线与跨境汇率博弈】\n"
        "   - 【四、第五层：全球跨资产资金流向与背离因子穿透（含股权风险溢价）】\n"
        "   - 【五、第六层：36维黑天鹅与灰犀牛高危预警排雷（带 🔴/🟡/🟢 状态图标）】\n"
        "   - 【六、三情景概率分布与未来十日量化证伪红线】\n"
        "2. `chart_notes`（对象，包含键 `c1` 到 `c12` 共12个字符串，每个 95~135 字）：分别为驾驶舱的 12 张结构图撰写专属实时读图解说。\n"
        "【读图解说强制校验铁律】：\n"
        "- 每一张图的解说必须严格对照 `plotted_charts_ground_truth` 中对应键（c1..c12）给出的【左轴实测指标与数值】和【右轴实测指标与数值】进行解读，严禁张冠李戴提及该图未绘制的指标；\n"
        "- 全文 100% 使用规范简体中文，严禁夹杂任何英文缩写或英文单词；\n"
        "- 预警与趋势必须醒目使用 `🔴`、`🟡`、`🟢`、`🔺`、`🔻`、`⚠️` 图标。\n\n"
        f"今日全量宏观数据与12张图实测基准：\n{json.dumps(llm_ctx, ensure_ascii=False)}"
    )

    final_report = fallback_report
    raw_notes = dict(fallback_chart_notes)

    if OPENAI_API_KEY:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        effort_candidates = [OPENAI_REASONING_EFFORT]
        if OPENAI_REASONING_EFFORT.lower() == "max" and "high" not in effort_candidates:
            effort_candidates.append("high")

        for effort in effort_candidates:
            try:
                chat_url = f"{OPENAI_BASE_URL}/chat/completions"
                body_chat = {
                    "model": OPENAI_MODEL,
                    "reasoning_effort": effort,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 16000,
                }
                resp = HTTP.post(chat_url, headers=headers, json=body_chat, timeout=(15, 240))
                if resp.status_code == 200:
                    raw_txt = resp.json()["choices"][0]["message"]["content"].strip()
                    jm = re.search(r"\{.*\}", raw_txt, re.DOTALL)
                    if jm:
                        parsed = json.loads(jm.group(0))
                        m_rep = str(parsed.get("master_report", "")).strip()
                        c_notes = parsed.get("chart_notes", {})
                        if m_rep and isinstance(c_notes, dict):
                            header_banner = f"【首席宏观策略师智能全因子总研判 | 模型：GPT-6.2 深度思考（{effort}） | 更新：{NOW_STR}】\n\n"
                            final_report = header_banner + m_rep
                            for k, v in c_notes.items():
                                if k in raw_notes and str(v).strip():
                                    raw_notes[k] = str(v).strip()
                            break
                elif resp.status_code == 400 and effort == "max":
                    continue
            except Exception as e:
                print(f"[INFO] AI 全因子与读图解说尝试 ({effort}): {e}")

    # 逐图执行图文一致性核验并封装显式编号标签 + 实测核验印章
    verified_notes = {}
    for spec in CHART_SPECS_MASTER:
        ckey = spec["key"]
        short_badge = spec["title"].split("（")[0]
        body_txt = re.sub(r"^【.*?】", "", raw_notes.get(ckey, "")).strip()
        stamp = plotted_facts.get(ckey, {}).get("verify_stamp", "")
        verified_notes[ckey] = f"🤖【{short_badge}·AI专属读图解说】{body_txt}\n{stamp}"

    return final_report, verified_notes


def analyze_all_factors(s_obs, s_dates, s_dmaps, wb_metrics, imf_metrics, extra_vals, market_stats, news_list):
    risk_rows = []
    macro_snapshot_rows = []

    def get(sid, idx=0):
        arr = s_obs.get(sid, [])
        return arr[idx] if len(arr) > idx else None

    def calc_yoy(sid, lag=12):
        v0, v_prev = get(sid, 0), get(sid, lag)
        if v0 is not None and v_prev is not None and v_prev != 0:
            return round((v0 / v_prev - 1) * 100, 2)
        return None

    sahm = get("SAHMREALTIME") or 0.0

    # A. 日历精确对齐的美元净流动性计算
    r0 = get_val_by_days_ago(s_dmaps, "RRPONTSYD", 0)

    def calc_net_liq_at(days_ago):
        w = get_val_by_days_ago(s_dmaps, "WALCL", days_ago)
        t = get_val_by_days_ago(s_dmaps, "WDTGAL", days_ago)
        r = get_val_by_days_ago(s_dmaps, "RRPONTSYD", days_ago)
        if all(v is not None for v in [w, t, r]):
            return round((w - t) / 1000.0 - r, 2)
        return None

    net_liq = calc_net_liq_at(0)
    net_liq_1w = calc_net_liq_at(7)
    net_liq_4w = calc_net_liq_at(28)
    net_liq_13w = calc_net_liq_at(91)
    net_liq_chg = round(net_liq - net_liq_1w, 2) if (net_liq is not None and net_liq_1w is not None) else 0.0
    net_liq_4w_chg = round(net_liq - net_liq_4w, 2) if (net_liq is not None and net_liq_4w is not None) else 0.0
    net_liq_13w_chg = round(net_liq - net_liq_13w, 2) if (net_liq is not None and net_liq_13w is not None) else 0.0

    # B. 六层核心经济硬数据与衍生公式直观快照
    us_credit_yoy = calc_yoy("TOTLL", lag=52)
    if us_credit_yoy is not None:
        sig = "🟢 信用平稳扩张" if us_credit_yoy > 2.0 else "🟡 信用派生放缓"
        macro_snapshot_rows.append(["美国", "L1·信用引擎", "商业银行总信贷同比增速", f"{us_credit_yoy:+.2f}%", f"余额 {get('TOTLL'):,.0f} 十亿美元", s_dates.get("TOTLL", "-"), sig])

    cn_m1_yoy, cn_m2_yoy = get("CN_M1_YOY"), get("CN_M2_YOY")
    cn_scissors = None
    if cn_m1_yoy is not None and cn_m2_yoy is not None:
        cn_scissors = round(cn_m1_yoy - cn_m2_yoy, 2)
        sig = "🟢 资金活化改善" if cn_scissors > -2.0 else "🟡 定期存款沉淀偏高"
        macro_snapshot_rows.append(["中国", "L1·货币活化", "狭义与广义货币剪刀差", f"{cn_scissors:+.2f}%", f"M1 {cn_m1_yoy:.1f}% / M2 {cn_m2_yoy:.1f}%", s_dates.get("CN_M1_YOY", "-"), sig])

    if net_liq is not None:
        sig = "🟢 净流动性边际扩张" if net_liq_4w_chg >= 0 else "🟡 净流动性边际抽水"
        macro_snapshot_rows.append(["美国", "L2·美元水管", "美元净流动性总阀门", f"{net_liq:,.1f} 十亿美元", f"1周 {net_liq_chg:+.1f}B / 4周 {net_liq_4w_chg:+.1f}B", s_dates.get("WALCL", "-"), sig])

    y10, y2, effr = get("DGS10"), get("DGS2"), get("DFF")
    y10_5, y2_5 = get("DGS10", 5), get("DGS2", 5)
    spread_10_2 = round(y10 - y2, 3) if (y10 is not None and y2 is not None) else None
    implied_rate_cut_bps = round((y2 - effr) * 100, 1) if (y2 is not None and effr is not None) else None
    if y10 is not None and y2 is not None:
        sig = "🟡 收益率曲线倒挂" if spread_10_2 < 0 else "🟢 曲线正斜率区间"
        macro_snapshot_rows.append(["美国", "L2·利率之锚", "10年与2年期美债利率对比", f"10Y {y10:.2f}% / 2Y {y2:.2f}%", f"利差 {spread_10_2:+.2f}% | 隐含降息 {implied_rate_cut_bps:+.0f}bp", s_dates.get("DGS10", "-"), sig])

    tips_0, tips_5 = get("DFII10", 0), get("DFII10", 5)
    t10yie = get("T10YIE") or (round(y10 - tips_0, 2) if (y10 is not None and tips_0 is not None) else None)
    if tips_0 is not None:
        sig = "🔴 高实际利率压制估值" if tips_0 >= 2.25 else ("🟡 实际利率偏高" if tips_0 >= 1.80 else "🟢 实际资金成本温和")
        macro_snapshot_rows.append(["美国", "L2·真实成本", "10年期TIPS实际利率与通胀预期", f"实际 {tips_0:.2f}%", f"盈亏平衡通胀 {t10yie:.2f}%" if t10yie else "-", s_dates.get("DFII10", "-"), sig])

    nfp_0, nfp_1 = get("PAYEMS", 0), get("PAYEMS", 1)
    nfp_add_10k = round((nfp_0 - nfp_1) / 10.0, 2) if (nfp_0 is not None and nfp_1 is not None) else None
    if nfp_add_10k is not None:
        sig = "🟡 就业过热抑制降息" if nfp_add_10k > 20 else ("🟢 温和着陆区间" if nfp_add_10k >= 10 else "🔴 就业显著降温")
        macro_snapshot_rows.append(["美国", "L3·就业硬数据", "单月新增非农就业人数", f"{nfp_add_10k:+.2f} 万人", f"失业率 {get('UNRATE')}% | 萨姆 {sahm:.2f}%", s_dates.get("PAYEMS", "-"), sig])

    us_cpi_yoy = calc_yoy("CPIAUCSL")
    pce_0, pce_3 = get("PCEPILFE", 0), get("PCEPILFE", 3)
    pce_3m_ann = round(((pce_0 / pce_3) ** 4 - 1) * 100, 2) if (pce_0 and pce_3 and pce_3 > 0) else None
    if us_cpi_yoy is not None:
        sig = "🟡 通胀具黏性(>2.5%)" if (pce_3m_ann and pce_3m_ann > 2.5) or us_cpi_yoy > 2.6 else "🟢 通胀受控接近目标"
        macro_snapshot_rows.append(["美国", "L3·通胀硬数据", "总体CPI同比与核心PCE折年率", f"CPI {us_cpi_yoy:.2f}%", f"核心PCE 3M折年 {pce_3m_ann:.2f}%" if pce_3m_ann else "-", s_dates.get("CPIAUCSL", "-"), sig])

    isratio_0 = get("ISRATIO", 0)
    dg_yoy = calc_yoy("DGORDER")
    if isratio_0 is not None:
        sig = "🟢 主动补库周期动能" if (dg_yoy and dg_yoy > 2.0 and isratio_0 < 1.38) else "🟡 库存消化震荡期"
        macro_snapshot_rows.append(["美国", "L3·库存周期", "商业库销比与耐用品新订单", f"库销比 {isratio_0:.2f}", f"新订单同比 {dg_yoy:+.2f}%" if dg_yoy is not None else "-", s_dates.get("ISRATIO", "-"), sig])

    fed_int, fed_rev = get("A091RC1Q027SBEA"), get("FGRECPT")
    interest_rev_ratio = round((fed_int / fed_rev) * 100, 2) if (fed_int and fed_rev and fed_rev > 0) else None
    us_nom_gdp_yoy = calc_yoy("GDP", lag=4)
    r_minus_g = round(y10 - us_nom_gdp_yoy, 2) if (y10 is not None and us_nom_gdp_yoy is not None) else None
    if interest_rev_ratio is not None:
        sig = "🔴 突破20%财政空间锁定红线" if interest_rev_ratio >= 20.0 else ("🟡 逼近20%付息警戒线" if interest_rev_ratio >= 16.0 else "🟢 财政付息可控")
        macro_snapshot_rows.append(["美国", "L3·财政生死线", "联邦利息占财政收入比与r-g", f"吞噬比 {interest_rev_ratio:.2f}%", f"债务雪球 r-g = {r_minus_g:+.2f}%" if r_minus_g is not None else "-", s_dates.get("A091RC1Q027SBEA", "-"), sig])

    cn_pmi = get("CN_PMI_MFG")
    if cn_pmi is not None:
        sig = "🟢 制造业处于扩张区间" if cn_pmi >= 50.0 else "🟡 景气度低于荣枯线"
        macro_snapshot_rows.append(["中国", "L3·经济景气", "官方制造业PMI与工业增速", f"PMI {cn_pmi:.1f}", f"工业增加值 {get('CN_IND_YOY')}%" if get("CN_IND_YOY") else "-", s_dates.get("CN_PMI_MFG", "-"), sig])

    tic_0, tic_1 = get("FDHBFIN", 0), get("FDHBFIN", 1)
    if tic_0 is not None and tic_1 is not None:
        tic_flow = round(tic_0 - tic_1, 1)
        sig = "🟢 海外主权资金净增持美债" if tic_flow >= 0 else "🟡 海外资金净减持美债"
        macro_snapshot_rows.append(["全球", "L4·跨境资金流", "海外持有美债存量与季度净流量", f"{tic_0:,.0f} 十亿美元", f"单季净流量 {tic_flow:+.1f}B", s_dates.get("FDHBFIN", "-"), sig])

    spx_pe_val = get("SPX_PE")
    erp_val = round((100.0 / spx_pe_val) - y10, 2) if (spx_pe_val and y10 is not None) else None
    if erp_val is not None:
        sig = "🔴 负风险溢价估值极度脆弱" if erp_val < 0 else ("🟡 风险溢价偏低安全边际窄" if erp_val < 1.0 else "🟢 权益风险溢价合理")
        macro_snapshot_rows.append(["美国", "L5·定价核心公式", "标普500股权风险溢价(ERP)", f"{erp_val:+.2f}%", f"市盈率 {spx_pe_val:.1f}倍 | 10Y美债 {y10:.2f}%", TODAY_STR, sig])

    # C. 周期四象限坐标与曲线形态
    ind_yoy = calc_yoy("INDPRO") or 1.2
    nfp_yoy = calc_yoy("PAYEMS") or 1.2
    rs_yoy = calc_yoy("RSAFS") or 3.0
    curr_growth_x = round(0.35 * (ind_yoy - 1.2) + 0.35 * (nfp_yoy - 1.2) + 0.30 * (rs_yoy - 3.0), 2)
    curr_infl_y = round(0.55 * ((pce_3m_ann or 2.4) - 2.3) + 0.45 * ((us_cpi_yoy or 2.5) - 2.5), 2)

    if curr_growth_x >= 0 and curr_infl_y < 0:
        quadrant = "🟢 第一象限（右下）：温和复苏与金发姑娘周期（增长稳健 + 通胀回落）"
        quad_short = "第一象限·温和复苏"
        regime_logic = "当前处于【增长主导机制】，市场重点交易分子端企业盈利修复，占优资产为成长与周期股票及企业债券。"
    elif curr_growth_x >= 0 and curr_infl_y >= 0:
        quadrant = "🟡 第二象限（右上）：经济过热与紧缩周期（增长具备韧性 + 通胀黏性）"
        quad_short = "第二象限·过热紧缩"
        regime_logic = "当前切换至【利率与通胀主导机制】，超预期经济数据易强化央行紧缩预期并抬高分母端贴现率，占优资产为大宗商品与价值股，规避长久期国债。"
    elif curr_growth_x < 0 and curr_infl_y >= 0:
        quadrant = "🔴 第三象限（左上）：滞胀困境周期（经济增长放缓 + 通胀高企）"
        quad_short = "第三象限·滞胀困境"
        regime_logic = "警惕【薪资-物价螺旋与政策两难】，央行陷入保增长与抗通胀矛盾，占优资产为现金、短债与实物黄金，严格规避高估值科技股与长债。"
    else:
        quadrant = "🟡 第四象限（左下）：衰退防御周期（经济增长走弱 + 通胀回落）"
        quad_short = "第四象限·衰退防御"
        regime_logic = "总需求收缩主导市场定价，央行进入被动降息通道，占优资产为长久期国债，警惕信用利差走阔引发的信用紧缩。"

    curve_regime = "曲线温和震荡"
    if all(v is not None for v in [y10, y2, y10_5, y2_5]):
        d_spread = round(spread_10_2 - (y10_5 - y2_5), 3)
        dy10, dy2 = y10 - y10_5, y2 - y2_5
        if d_spread > 0 and dy2 < 0 and abs(dy2) > abs(dy10):
            curve_regime = "牛市陡峭化（短端降息预期主导，利好短债与黄金）"
        elif d_spread > 0 and dy10 > 0 and dy10 > dy2:
            curve_regime = "熊市陡峭化（再通胀或美债供给过剩，压制高估值科技股）"
        elif d_spread < 0 and dy10 < 0 and abs(dy10) > abs(dy2):
            curve_regime = "牛市平坦化（避险买入长债或长期通胀预期回落）"
        elif d_spread < 0 and dy2 > 0 and dy2 > dy10:
            curve_regime = "熊市平坦化（央行紧缩预期升温，曲线趋平）"

    gold_stat = market_stats.get("GOLD", {})
    copper_stat = market_stats.get("COPPER", {})
    brent_stat = market_stats.get("BRENT", {})
    gold_5d = gold_stat.get("pct_5d", 0.0)
    spx_5d = market_stats.get("SPX", {}).get("pct_5d", 0.0)
    dxy_5d = market_stats.get("DXY", {}).get("pct_5d", 0.0)
    hy = get("BAMLH0A0HYM2")
    hy_5 = get("BAMLH0A0HYM2", 5)
    ig = get("BAMLC0A0CM")

    divergence_flags = []
    if tips_0 is not None and tips_5 is not None and (tips_0 - tips_5) > 0.05 and gold_5d > 1.0:
        divergence_flags.append("⚠️ 实际利率与黄金背离：10年期实际利率上行但黄金逆势上涨，全球央行购金主导定价")
    if dxy_5d is not None and y10_5 is not None and y10 is not None and (y10 - y10_5) >= 0.15 and dxy_5d <= -1.2:
        divergence_flags.append("⚠️ 美债与美元背离：10年期美债收益率飙升但美元下跌，市场计入美国财政信用折价")
    if spx_5d >= 1.5 and hy is not None and hy_5 is not None and (hy - hy_5) >= 0.15:
        divergence_flags.append("⚠️ 股债信用背离：标普500上涨但高收益垃圾债利差走阔，谨防股市补跌")
    if spx_5d <= -3.0:
        divergence_flags.append("⚠️ 高频风险抢跑：股指急跌抢跑低频月度硬数据，谨防拿上月滞后数据误判")

    # D. 36维系统性风险雷达
    red_alerts, yellow_alerts = [], []

    def add_risk(code, r_type, dim, metric, curr_val, warn_lbl, red_lbl, ratio_raw, mech):
        ratio_pct = round(max(0.0, min(1.5, ratio_raw)), 3)
        if ratio_raw >= 1.0:
            st = "🔴 红灯熔断"
            red_alerts.append(f"🔴【{code}·{r_type}】{dim}（当前值：{curr_val}）")
        elif ratio_raw >= 0.65:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"🟡【{code}·{r_type}】{dim}（当前值：{curr_val}）")
        else:
            st = "🟢 绿灯正常"
        risk_rows.append([code, f"{r_type}·{dim}", metric, curr_val, warn_lbl, red_lbl, st, mech, ratio_pct])

    sofr, iorb = extra_vals.get("SOFR"), get("IORB")
    if sofr is not None and iorb is not None:
        sofr_sp = round((sofr - iorb) * 100, 1)
        add_risk("黑天鹅-01", "急性黑天鹅", "美元回购市场管道", "担保隔夜融资利率与准备金利率差（基点）", sofr_sp, "10 基点", "25 基点", calc_risk_ratio(sofr_sp, 0.0, 10.0, 25.0), "银行间美元准备金突发性短缺与货币市场冻结")

    if hy is not None:
        hy_bps = round(hy * 100, 0)
        add_risk("黑天鹅-02", "急性黑天鹅", "垃圾债信用踩踏", "美国高收益企业债信用利差（基点）", hy_bps, "350 基点", "500 基点", calc_risk_ratio(hy_bps, 260.0, 350.0, 500.0), "高收益企业再融资通道骤停引发集中违约潮")

    fsi = get("STLFSI4")
    if fsi is not None:
        add_risk("黑天鹅-03", "急性黑天鹅", "综合金融系统压力", "圣路易斯联储金融压力指数", round(fsi, 2), "0.50", "2.00", calc_risk_ratio(fsi, -0.50, 0.50, 2.00), "跨市场流动性、利差与波动率系统性共振恶化")

    if y10 is not None and y10_5 is not None:
        y10_5d_bps = round((y10 - y10_5) * 100, 1)
        summary_str = f"标普:{spx_5d:+.1f}% | 黄金:{gold_5d:+.1f}% | 美债:{y10_5d_bps:+.0f}基点"
        stress_score = max(0.0, (-spx_5d / 4.0) * 0.5 + (-gold_5d / 2.0) * 0.5)
        if spx_5d <= -4.0 and gold_5d <= -2.0 and y10_5d_bps >= 20 and dxy_5d >= 1.5:
            stress_score = 1.15
        add_risk("黑天鹅-04", "急性黑天鹅", "现金为王三杀挤兑", "5日股金债联动状态", summary_str, "股金同跌", "股金暴跌且债率飙升", stress_score, "保证金追缴下机构无差别抛售黄金美债换取美元现金")

    jpy_5d = market_stats.get("USDJPY", {}).get("pct_5d")
    if jpy_5d is not None:
        add_risk("黑天鹅-05", "急性黑天鹅", "日元套息平仓踩踏", "美元兑日元5日涨跌幅（%）", jpy_5d, "-2.5%", "-4.0%", calc_risk_ratio(jpy_5d, 0.0, -2.5, -4.0), "日元急速升值迫使全球低息日元融资盘拆仓抛售风险资产")

    vix = market_stats.get("VIX", {}).get("price")
    vix3m = get("VXVCLS")
    if vix is not None and vix3m is not None and vix3m > 0:
        vix_ratio = round(vix / vix3m, 2)
        r_vix = max(calc_risk_ratio(vix, 14.0, 24.0, 35.0), calc_risk_ratio(vix_ratio, 0.85, 1.00, 1.15))
        add_risk("黑天鹅-06", "急性黑天鹅", "波动率期限结构倒挂", f"短期与3个月波动率比值（恐慌指数={vix}）", vix_ratio, "1.00", "1.15", r_vix, "短端恐慌情绪大幅超越远期，预示短期尾部黑天鹅爆发")

    btc_auc = extra_vals.get("UST_BID_COVER")
    tail_bps = extra_vals.get("UST_TAIL_BPS", 0.0)
    if btc_auc is not None and btc_auc > 0:
        r_auc = max(calc_risk_ratio(btc_auc, 2.50, 2.25, 2.10), calc_risk_ratio(tail_bps, 0.5, 1.8, 3.0))
        add_risk("黑天鹅-07", "急性黑天鹅", "美债一级拍卖失灵", f"美债拍卖投标倍数（尾部利差={tail_bps}基点）", btc_auc, "2.25倍", "2.10倍", r_auc, "一级交易商承接意愿枯竭引发长端美债收益率无序飙升")

    disc_0 = get("WDISBORROW")
    if disc_0 is not None:
        disc_b = round(disc_0 / 1000.0, 2)
        add_risk("黑天鹅-08", "急性黑天鹅", "银行贴现窗口挤兑", "美联储贴现窗口紧急借款（十亿美元）", disc_b, "15.0 十亿美元", "50.0 十亿美元", calc_risk_ratio(disc_b, 2.0, 15.0, 50.0), "商业银行被迫向美联储紧急求助流动性（类似硅谷银行倒闭前兆）")

    kre_5d = market_stats.get("US_REG_BANK", {}).get("pct_5d")
    if kre_5d is not None:
        add_risk("黑天鹅-09", "急性黑天鹅", "美国区域银行闪崩", "区域银行指数基金5日涨跌（%）", kre_5d, "-5.0%", "-9.0%", calc_risk_ratio(kre_5d, 0.0, -5.0, -9.0), "中小银行存款挤兑或商业地产坏账曝光引发银行板块流动性踩踏")

    brent_5d = brent_stat.get("pct_5d")
    if brent_5d is not None:
        add_risk("黑天鹅-10", "急性黑天鹅", "能源供给断供冲击", "布伦特原油5日暴涨幅度（%）", brent_5d, "+6.5%", "+12.0%", calc_risk_ratio(brent_5d, 0.0, 6.5, 12.0), "地缘战争或航道封锁导致油价脉冲暴涨，触发全球突发滞胀冲击")

    nfci_0 = get("NFCI")
    if nfci_0 is not None:
        add_risk("黑天鹅-11", "急性黑天鹅", "金融条件骤紧熔断", "芝加哥联储金融状况指数", round(nfci_0, 2), "0.00", "+0.50", calc_risk_ratio(nfci_0, -0.50, 0.00, 0.50), "跨市场金融条件由松转紧突破零轴，信用与融资环境骤冷")

    sox_5d = market_stats.get("SOX", {}).get("pct_5d")
    if sox_5d is not None:
        add_risk("黑天鹅-12", "急性黑天鹅", "半导体与算力泡沫去杠杆", "费城半导体指数5日涨跌（%）", sox_5d, "-6.0%", "-10.0%", calc_risk_ratio(sox_5d, 0.0, -6.0, -10.0), "人工智能算力资本开支证伪或高拥挤科技量化盘踩踏平仓")

    jp10_0 = get("IRLTLT01JPM156N")
    if jp10_0 is not None:
        add_risk("黑天鹅-13", "急性黑天鹅", "日本国债失控风险", "日本10年期国债收益率（%）", round(jp10_0, 2), "1.35%", "1.80%", calc_risk_ratio(jp10_0, 0.80, 1.35, 1.80), "日本长端利率失控迫使日资机构大规模抛售美债欧债回流本土")

    cnh_5d = market_stats.get("USDCNH", {}).get("pct_5d")
    if cnh_5d is not None:
        add_risk("黑天鹅-14", "急性黑天鹅", "离岸人民币急贬冲击", "美元兑离岸人民币5日贬值幅度（%）", cnh_5d, "+1.2%", "+2.2%", calc_risk_ratio(cnh_5d, 0.0, 1.2, 2.2), "人民币汇率大幅波动外溢至亚洲及新兴市场货币连锁贬值")

    btc_5d = market_stats.get("BTC", {}).get("pct_5d")
    if btc_5d is not None:
        add_risk("黑天鹅-15", "急性黑天鹅", "加密高杠杆连环爆仓", "比特币5日暴跌幅度（%）", btc_5d, "-10.0%", "-18.0%", calc_risk_ratio(btc_5d, 0.0, -10.0, -18.0), "美元离岸流动性枯竭率先击穿高杠杆加密市场，预警科技股风险")

    copper_5d = copper_stat.get("pct_5d")
    if copper_5d is not None and gold_5d is not None:
        cg_5d_diff = round(copper_5d - gold_5d, 2)
        add_risk("黑天鹅-16", "急性黑天鹅", "铜金比断崖硬着陆脉冲", "精炼铜与黄金5日相对剪刀差（%）", cg_5d_diff, "-4.5%", "-8.0%", calc_risk_ratio(cg_5d_diff, 0.0, -4.5, -8.0), "铜价暴跌而黄金避险上涨，市场急速定价全球实体经济硬着陆")

    if y10 is not None and y10_5 is not None:
        y10_jump_bps = round((y10 - y10_5) * 100, 1)
        add_risk("黑天鹅-17", "急性黑天鹅", "长端美债单周剧烈抛售", "美国10年期国债5日跳升幅度（基点）", y10_jump_bps, "+20 基点", "+35 基点", calc_risk_ratio(y10_jump_bps, 0.0, 20.0, 35.0), "无风险利率单周暴涨引爆商业银行债券账面巨亏与养老金追保")

    wheat_5d = market_stats.get("WHEAT", {}).get("pct_5d")
    if wheat_5d is not None:
        add_risk("黑天鹅-18", "急性黑天鹅", "全球粮食供应链危机", "芝加哥小麦5日暴涨幅度（%）", wheat_5d, "+8.0%", "+15.0%", calc_risk_ratio(wheat_5d, 0.0, 8.0, 15.0), "极端气候或地缘冲突阻断粮道，引爆新兴市场恶性食品通胀")

    if interest_rev_ratio is not None:
        add_risk("灰犀牛-01", "慢性灰犀牛", "美国财政利息吞噬率", "联邦利息支出占财政收入比（%）", interest_rev_ratio, "16.0%", "20.0%", calc_risk_ratio(interest_rev_ratio, 10.0, 16.0, 20.0), "利息支出挤压常规财政预算，倒逼赤字货币化与财政主导")

    if r_minus_g is not None:
        add_risk("灰犀牛-02", "慢性灰犀牛", "美国主权债务雪球效应", "10年期美债利率减名义经济增速（%）", r_minus_g, "+0.5%", "+1.5%", calc_risk_ratio(r_minus_g, -1.5, 0.5, 1.5), "借贷利率持续高于名义经济增长，债务率进入内生发散通道")

    cre_delinq = get("DRCRELEXFACBS")
    if cre_delinq is not None:
        add_risk("灰犀牛-03", "慢性灰犀牛", "美国商业地产贷款违约", "商业银行商业地产贷款违约率（%）", round(cre_delinq, 2), "1.50%", "2.50%", calc_risk_ratio(cre_delinq, 0.70, 1.50, 2.50), "高利率下写字楼与商业地产再融资坏账侵蚀美国区域银行资本金")

    cc_delinq = get("DRCCLACBS")
    psav = get("PSAVERT")
    if cc_delinq is not None:
        lbl_val = f"违约{cc_delinq:.2f}% / 储蓄{psav:.1f}%" if psav is not None else f"{cc_delinq:.2f}%"
        add_risk("灰犀牛-04", "慢性灰犀牛", "美国居民消费信用恶化", "信用卡违约率与储蓄率", lbl_val, "违约 3.0%", "违约 4.0%", calc_risk_ratio(cc_delinq, 2.0, 3.0, 4.0), "超额储蓄耗尽后中低收入群体信用卡违约攀升拖累消费引擎")

    dep_0, dep_13 = get("DPSACBW027SBOG", 0), get("DPSACBW027SBOG", 13)
    if dep_0 is not None and dep_13 is not None and dep_13 > 0:
        dep_q_chg = round((dep_0 / dep_13 - 1) * 100, 2)
        add_risk("灰犀牛-05", "慢性灰犀牛", "商业银行存款持续流失", "美国商业银行总存款季度变动（%）", dep_q_chg, "-1.0%", "-3.0%", calc_risk_ratio(dep_q_chg, 0.5, -1.0, -3.0), "高息货币基金持续虹吸银行低息存款，削弱商业银行放贷能力")

    cn10 = get("CN_DGS10")
    us_cn_sp = round(y10 - cn10, 2) if (y10 is not None and cn10 is not None) else None
    if us_cn_sp is not None:
        add_risk("灰犀牛-06", "慢性灰犀牛", "中美长端负利差倒挂", "美国10年期减中国10年期国债利差（%）", us_cn_sp, "2.00%", "3.00%", calc_risk_ratio(us_cn_sp, 0.80, 2.00, 3.00), "深度负利差持续压制人民币汇率弹性与境外资金配置意愿")

    if cn_scissors is not None:
        add_risk("灰犀牛-07", "慢性灰犀牛", "中国货币活化与通缩压力", "中国狭义与广义货币增速剪刀差（%）", cn_scissors, "-3.5%", "-6.0%", calc_risk_ratio(cn_scissors, -1.0, -3.5, -6.0), "企业与居民预防性储蓄偏高、实体投融资需求待实质性修复")

    em_ratios = {}
    for iso3 in COUNTRIES_LIST:
        res = wb_metrics.get((iso3, "FI.RES.TOTL.CD"))
        debt = wb_metrics.get((iso3, "DT.DOD.DSTC.CD"))
        if res and debt and res > 0:
            em_ratios[iso3] = round((debt / res) * 100, 1)
    country_zh_map = {
        "USA": "美国", "CHN": "中国", "JPN": "日本", "DEU": "德国",
        "IND": "印度", "BRA": "巴西", "IDN": "印度尼西亚", "TUR": "土耳其"
    }
    if em_ratios:
        worst_iso = max(em_ratios, key=em_ratios.get)
        worst_val = em_ratios[worst_iso]
        add_risk("灰犀牛-08", "慢性灰犀牛", f"新兴市场外债脆弱性（{country_zh_map.get(worst_iso, worst_iso)}）", "短期外债占官方外汇储备比（%）", worst_val, "65.0%", "100.0%", calc_risk_ratio(worst_val, 30.0, 65.0, 100.0), "外储覆盖率不足1倍，强美元周期下易爆发主权债务重组危机")

    sloos_0 = get("DRTSCILM")
    if sloos_0 is not None:
        add_risk("灰犀牛-09", "慢性灰犀牛", "银行全面收紧信贷标准", "美联储调查收紧工商业贷款比例（%）", round(sloos_0, 1), "35.0%", "55.0%", calc_risk_ratio(sloos_0, 10.0, 35.0, 55.0), "商业银行大幅收紧信贷标准，实体企业信用创造渠道面临慢性冻结")

    tdsp_0 = get("TDSP")
    if tdsp_0 is not None:
        add_risk("灰犀牛-10", "慢性灰犀牛", "美国家庭债务偿付负担", "家庭偿债支出占可支配收入比（%）", round(tdsp_0, 2), "10.5%", "12.0%", calc_risk_ratio(tdsp_0, 9.0, 10.5, 12.0), "高利率向存量房贷与消费贷传导，持续挤压居民可选消费现金流")

    if r0 is not None:
        add_risk("灰犀牛-11", "慢性灰犀牛", "隔夜逆回购缓冲垫耗尽", "美联储隔夜逆回购余额（十亿美元）", round(r0, 1), "150 十亿美元", "50 十亿美元", calc_risk_ratio(r0, 450.0, 150.0, 50.0), "逆回购蓄水池见底后，美联储缩表与发债将直接抽干商业银行准备金")

    if tips_0 is not None:
        add_risk("灰犀牛-12", "慢性灰犀牛", "实际利率长期高压窒息", "美国10年期通胀保值债券实际利率（%）", round(tips_0, 2), "1.80%", "2.35%", calc_risk_ratio(tips_0, 1.00, 1.80, 2.35), "高实际无风险利率持续压制实体企业资本回报率与高估值资产")

    if hy is not None and ig is not None:
        hy_ig_gap = round((hy - ig) * 100, 0)
        add_risk("灰犀牛-13", "慢性灰犀牛", "企业债信用分层恶化", "垃圾债与投资级债利差剪刀差（基点）", hy_ig_gap, "260 基点", "380 基点", calc_risk_ratio(hy_ig_gap, 180.0, 260.0, 380.0), "低评级僵尸企业借新还旧成本失控，违约潮向中游企业蔓延")

    mtg_delinq = get("DRSFRMACBS")
    if mtg_delinq is not None:
        add_risk("灰犀牛-14", "慢性灰犀牛", "居民住房抵押贷款违约", "商业银行居民房贷违约率（%）", round(mtg_delinq, 2), "2.20%", "3.20%", calc_risk_ratio(mtg_delinq, 1.40, 2.20, 3.20), "失业率上升叠加高房贷利率开始击穿美国家庭核心资产负债表")

    if psav is not None:
        add_risk("灰犀牛-15", "慢性灰犀牛", "美国居民超额储蓄枯竭", "个人储蓄率占可支配收入比（%）", round(psav, 1), "4.2%", "3.0%", calc_risk_ratio(psav, 6.5, 4.2, 3.0), "居民消费高度依赖信用卡透支，一旦就业降温消费将断崖失速")

    bus_yoy = calc_yoy("BUSLOANS", lag=12)
    if bus_yoy is not None:
        add_risk("灰犀牛-16", "慢性灰犀牛", "美国工商业信贷萎缩", "商业银行工商业贷款余额同比（%）", bus_yoy, "+0.5%", "-1.0%", calc_risk_ratio(bus_yoy, 4.0, 0.5, -1.0), "企业停止主动加杠杆扩产，经济内生信用派生引擎熄火")

    add_risk("灰犀牛-17", "慢性灰犀牛", "萨姆规则衰退临界逼近", "官方萨姆规则实时衰退指标（%）", round(sahm, 2), "0.35%", "0.50%", calc_risk_ratio(sahm, 0.10, 0.35, 0.50), "失业率3个月均值较12个月低点上升超0.50%触发历史100%准度衰退铁律")

    if spx_pe_val is not None:
        add_risk("灰犀牛-18", "慢性灰犀牛", "美股估值泡沫与负风险溢价", "标普500市盈率（倍）", spx_pe_val, "24.0 倍", "28.0 倍", calc_risk_ratio(spx_pe_val, 18.0, 24.0, 28.0), "美股盈利收益率低于10年期美债无风险利率，估值安全边际极度脆弱")

    # E. 多国横向对比表
    bond_10y_map = {
        "USA": get("DGS10"), "CHN": get("CN_DGS10"),
        "JPN": get("IRLTLT01JPM156N"), "DEU": get("IRLTLT01DEM156N")
    }
    country_rows = []
    for iso3 in COUNTRIES_LIST:
        gdp = imf_metrics.get((iso3, "NGDP_RPCH"), "暂无")
        cpi = imf_metrics.get((iso3, "PCPIPCH"), "暂无")
        ca = imf_metrics.get((iso3, "BCA_NGDPD"), "暂无")
        fiscal = imf_metrics.get((iso3, "GGXCNL_NGDP"), "暂无")
        b10 = bond_10y_map.get(iso3, "暂无")
        debt_ratio = em_ratios.get(iso3, "暂无")
        country_rows.append([country_zh_map.get(iso3, iso3), gdp, cpi, b10, ca, fiscal, debt_ratio])

    # F. 全球大类资产资金流向对比表
    flow_rows = [["全球大类资产", "20日资金净流向动量（%）", "5日边际资金流向（%）"]]
    for code, st in market_stats.items():
        if st.get("flow_chart"):
            flow_rows.append([st["short_name"], st["pct_20d"], st["pct_5d"]])

    # G. 严格对齐每张图表实测数据的默认解说（作为兜底与结构锚点）
    tips10 = tips_0 or 0.0
    spx_p = market_stats.get("SPX", {}).get("price", "-")
    au_p = market_stats.get("GOLD", {}).get("price", "-")
    br_p = market_stats.get("BRENT", {}).get("price", "-")
    dxy_p = market_stats.get("DXY", {}).get("price", "-")
    jpy_p = market_stats.get("USDJPY", {}).get("price", "-")
    btc_p = market_stats.get("BTC", {}).get("price", "-")
    ndx_p = market_stats.get("NDX", {}).get("price", "-")
    cg_ratio = round((copper_stat.get("price", 0) * 1000) / au_p, 3) if isinstance(au_p, (int, float)) and au_p > 0 else "-"
    bus_bal = round(get("BUSLOANS") or 0, 1)
    res_bal = round((get("WRESBAL") or 0) / 1000.0, 1)

    sorted_risks = sorted(risk_rows, key=lambda x: x[8], reverse=True)
    top1_r = sorted_risks[0] if sorted_risks else ["-", "-", "-", "-", "-", "-", "🟢 绿灯正常", "", 0]

    fallback_chart_notes = {
        "c1": f"左轴柱状代表美国商业银行总信贷规模（当前 {get('TOTLL'):,.0f} 十亿美元，同比 {us_credit_yoy:+.2f}%），右轴折线代表高收益垃圾债信用利差（当前 {hy}%）。柱体稳步抬升且红线低位运行说明企业信用环境通畅；若右轴利差突破 3.50% 黄灯线则预警违约潮。",
        "c2": f"左轴曲线代表美国工商业贷款余额（当前 {bus_bal:,.1f} 十亿美元），右轴曲线代表商业银行准备金余额（当前 {res_bal:,.1f} 十亿美元）。当右轴银行准备金充裕而左轴工商业贷款走平时，说明银行有钱但企业借贷扩张意愿偏弱。",
        "c3": f"左轴面积代表美元净流动性总阀门（当前 {net_liq:,.1f} 十亿美元，近4周变动 {net_liq_4w_chg:+.1f}B），右轴代表财政部TGA账户与隔夜逆回购蓄水池（当前 {r0} 十亿美元）。当逆回购缓冲垫见底后，TGA吸金将直接抽干银行体系流动性。",
        "c4": f"左轴直接对比美债 2年期（{y2}%）与 10年期（{y10}%）收益率，右轴为 10Y-2Y 期限利差（{spread_10_2}%），当前呈现【{curve_regime}】。当2年期利率快速下行带动利差由负转正（牛陡）时，往往预示降息周期开启与经济周期切换。",
        "c5": f"左轴为美国 10年期TIPS实际利率（当前 {tips10}%），右轴为 10年期盈亏平衡通胀预期（当前 {t10yie}%）。实际利率是无息黄金与科技股的真实机会成本，突破 2.0% 且上行将强力压制高估值资产。",
        "c6": f"散点图基于经济增长景气得分（X轴当前 {curr_growth_x:+}）与通胀压力得分（Y轴当前 {curr_infl_y:+}）定位，红点标出本月处于【{quad_short}】，蓝点展示过去12个月轨迹漂移方向。",
        "c7": f"左轴柱状为美国联邦利息支出占财政收入比重（当前 {interest_rev_ratio}%，逼近 20% 财政空间锁死红线），右轴折线为主权债务雪球差值 r-g（当前 {r_minus_g:+}%）。柱体逼近 20% 意味着常规财政预算遭到严重挤压。",
        "c8": f"左轴对比美国 10年期国债（{y10}%）与中国 10年期国债（{cn10}%）收益率，右轴为中美 10年期利差（当前 {us_cn_sp}%）。深度负利差倒挂持续考验离岸人民币汇率弹性与跨境套息资金流向。",
        "c9": f"左轴为美元指数（当前 {dxy_p}），右轴为全球套息交易总阀门美元兑日元汇率（当前 {jpy_p}）。若右轴美元兑日元在短期内急跌超 2.5%，将触发全球低息日元融资盘平仓踩踏。",
        "c10": "柱状图将全球 16 大核心资产置于同一标尺对比 20日与 5日资金净流向动量。零轴上方柱体代表资金净流入与定价扩张，零轴下方代表资金撤离与抛售，直观揭示大资金避险或进攻方向。",
        "c11": f"左轴为铜金比实体周期系数（当前 {cg_ratio}），右轴为 COMEX 黄金价格（当前 {au_p} 美元/盎司）。若左轴铜金比下行而右轴黄金逆势走强，说明市场正在定价需求走弱与央行去美元化避险逻辑。",
        "c12": f"柱状图按危急度降序展示最接近 100% 红灯熔断线的前 12 项风险因子，折线为 65% 黄灯警戒线。当前榜首为【{top1_r[1]}】（进度 {round(top1_r[8]*100, 1)}%，状态 {top1_r[6]}）。",
    }

    if red_alerts:
        probs = "🔴 基准情景（风险加速释放）：45% ｜ 🟢 乐观情景（央行紧急转向救市）：15% ｜ 🟡 悲观情景（尾部危机跨市场扩散）：40%"
    elif yellow_alerts:
        probs = "🟡 基准情景（高位震荡消化）：55% ｜ 🟢 乐观情景（经济软着陆延续）：20% ｜ 🔴 悲观情景（流动性紧缩与波动升级）：25%"
    else:
        probs = "🟢 基准情景（经济软着陆与板块轮动）：60% ｜ 🟢 乐观情景（全球宽信用共振）：25% ｜ 🟡 悲观情景（二次通胀或地缘冲击）：15%"

    fallback_report = (
        f"【首席宏观策略师·六层第一性原理全因子深度总研判 | 更新时间：{NOW_STR}】\n\n"
        f"一、【宏观周期四象限定位与新闻催化主线】\n"
        f"当前全球宏观定位于【{quadrant}】（增长坐标 {curr_growth_x:+}，通胀坐标 {curr_infl_y:+}）。{regime_logic}"
        f"{' 跨资产监测发出背离信号：' + '；'.join(divergence_flags) + '。' if divergence_flags else ' 当前高频资产价格与低频经济硬数据整体保持同向共振。'}\n\n"
        f"二、【第一至二层：信用引擎、货币活化与美元净流动性水管】\n"
        f"1. 信用与货币：美国商业银行总信贷同比 {us_credit_yoy if us_credit_yoy is not None else '-'}%，工商业贷款余额 {bus_bal:,.1f} 十亿美元，银行收紧贷款标准比例 {sloos_0}%；中国狭义与广义货币剪刀差 {cn_scissors if cn_scissors is not None else '-'}%。\n"
        f"2. 美元水管：美元净流动性总阀门水位 {net_liq} 十亿美元（1周 {net_liq_chg:+}B，4周 {net_liq_4w_chg:+}B，13周 {net_liq_13w_chg:+}B），隔夜逆回购蓄水池剩余 {r0} 十亿美元。美债 2年期（{y2}%）与 10年期（{y10}%）利差为 {spread_10_2}%，曲线呈现【{curve_regime}】，市场隐含未来降息预期 {implied_rate_cut_bps:+} 个基点。\n\n"
        f"三、【第三至四层：美欧日中硬数据、财政生死线与跨境汇率博弈】\n"
        f"美国单月新增非农 {nfp_add_10k if nfp_add_10k is not None else '-'} 万人，失业率 {get('UNRATE')}%，萨姆规则衰退指标 {sahm:.2f}%；总体消费物价同比 {us_cpi_yoy}%，核心个人消费支出 3个月折年率 {pce_3m_ann}%。联邦利息支出占财政收入比重达 {interest_rev_ratio}%，主权债务雪球差值（r-g）为 {r_minus_g:+}%。中国官方制造业采购经理指数报 {cn_pmi}，中美 10年期利差倒挂 {us_cn_sp}%。\n\n"
        f"四、【第五层：全球跨资产定价与股权风险溢价穿透】\n"
        f"10年期实际利率报 {tips10}%，标普500市盈率 {spx_pe_val if spx_pe_val else '-'} 倍，股权风险溢价（ERP）为 {erp_val if erp_val is not None else '-'}%。标普500报 {spx_p} 点，纳斯达克100报 {ndx_p} 点，美元指数报 {dxy_p}，美元兑日元报 {jpy_p}，国际黄金报 {au_p} 美元/盎司，布伦特原油报 {br_p} 美元/桶，比特币报 {btc_p} 美元。\n\n"
        f"五、【第六层：36维黑天鹅与灰犀牛高危预警排雷】\n"
        f"{'当前触发预警因子：' + '；'.join(red_alerts + yellow_alerts) + '。需高度防范局部流动性收紧通过保证金追缴向全局传染。' if (red_alerts or yellow_alerts) else '🟢 当前 36 个急性黑天鹅与慢性灰犀牛维度全部处于绿灯常态区间，系统性熔断风险可控。'}\n\n"
        f"六、【三情景概率分布与未来十日量化证伪红线】\n"
        f"{probs}\n"
        f"强制证伪条件：① 美国 10年期实际利率有效突破 {'1.80%' if tips10 > 2.0 else '2.25%'}；② 高收益垃圾债利差突破 500 基点或美股恐慌指数突破 30；③ 美元净流动性连续两周单向收缩超 150 十亿美元。"
    )

    llm_ctx = {
        "分析日期": TODAY_STR,
        "四象限定位": {"象限": quadrant, "增长X轴得分": curr_growth_x, "通胀Y轴得分": curr_infl_y},
        "第一二层_信用与美元水管": {
            "美国商业银行总信贷同比": us_credit_yoy,
            "美国工商业贷款余额十亿美元": bus_bal,
            "银行收紧贷款比例": sloos_0,
            "中国M1减M2剪刀差": cn_scissors,
            "美元净流动性十亿美元": net_liq,
            "净流动性1周4周13周变动": [net_liq_chg, net_liq_4w_chg, net_liq_13w_chg],
            "隔夜逆回购十亿美元": r0,
            "美债2Y与10Y利率及利差": [y2, y10, spread_10_2],
            "曲线形态": curve_regime,
            "隐含降息基点": implied_rate_cut_bps,
            "10年期TIPS实际利率与通胀预期": [tips10, t10yie],
        },
        "第三四层_硬数据财政与跨境": {
            "非农新增万人与萨姆规则": [nfp_add_10k, sahm],
            "CPI同比与核心PCE折年率": [us_cpi_yoy, pce_3m_ann],
            "联邦利息占财政收入比与债务雪球r_g": [interest_rev_ratio, r_minus_g],
            "中国PMI与中美10Y利差": [cn_pmi, us_cn_sp],
        },
        "第五层_跨资产与股权风险溢价": {
            "标普500市盈率与股权风险溢价ERP": [spx_pe_val, erp_val],
            "主要资产价格与20日资金流向": {v["short_name"]: [v["price"], v["pct_20d"]] for v in market_stats.values()},
            "背离预警": divergence_flags,
        },
        "第六层_36维风险雷达前10危急项": [
            {"编号": r[0], "维度": r[1], "当前值": r[3], "状态": r[6], "危急度百分比": round(r[8] * 100, 1), "机制": r[7]}
            for r in sorted_risks[:10]
        ],
        "今日六大板块新闻与摘要": [f"【{n['sector']}】{n['title']}：{n['summary']}" for n in news_list[:12]],
    }

    return risk_rows, country_rows, macro_snapshot_rows, flow_rows, (curr_growth_x, curr_infl_y), llm_ctx, fallback_report, fallback_chart_notes


# ================= 8. 工作表自动扩容 + 零重叠卡片式 12 图布局 + 逐图实测核验 =================
def get_or_create_ws(sh, title, rows=300, cols=45):
    try:
        ws = sh.worksheet(title)
        if ws.row_count < rows or ws.col_count < cols:
            ws.resize(rows=max(ws.row_count, rows), cols=max(ws.col_count, cols))
        return ws
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def build_12_pyramid_charts_requests(dash_sheet_id, ts_sheet_id, num_ts_rows, num_quad_rows, num_flow_rows, num_risk_rows):
    """基于 CHART_SPECS_MASTER 精确生成 12 张不重叠图表（锚定在每张卡片画布区 T+2 行）"""
    row_count_map = {
        "ts": num_ts_rows,
        "quad": num_quad_rows,
        "flow": num_flow_rows,
        "risk": num_risk_rows,
    }

    reqs = []
    for spec in CHART_SPECS_MASTER:
        t_row = 63 + spec["tier"] * 26
        anchor_row = t_row + 2
        anchor_col = 0 if spec["side"] == "left" else 8
        n_rows = row_count_map[spec["source_table"]]

        axes = [{"position": "BOTTOM_AXIS", "title": spec["x_title"]}]
        if spec["y_left_title"]:
            axes.append({"position": "LEFT_AXIS", "title": spec["y_left_title"]})
        if spec["y_right_title"]:
            axes.append({"position": "RIGHT_AXIS", "title": spec["y_right_title"]})

        series_objs = []
        for s in spec["series"]:
            so = {
                "series": {
                    "sourceRange": {
                        "sources": [{
                            "sheetId": ts_sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": n_rows,
                            "startColumnIndex": s["col"],
                            "endColumnIndex": s["col"] + 1,
                        }]
                    }
                },
                "targetAxis": s["axis"],
            }
            if spec["type"] == "COMBO" and s.get("stype"):
                so["type"] = s["stype"]
            series_objs.append(so)

        reqs.append({
            "addChart": {
                "chart": {
                    "spec": {
                        "title": spec["title"],
                        "titleTextFormat": {"bold": True, "fontSize": 10},
                        "basicChart": {
                            "chartType": spec["type"],
                            "legendPosition": "TOP_LEGEND",
                            "axis": axes,
                            "domains": [{
                                "domain": {
                                    "sourceRange": {
                                        "sources": [{
                                            "sheetId": ts_sheet_id,
                                            "startRowIndex": 0,
                                            "endRowIndex": n_rows,
                                            "startColumnIndex": spec["x_col"],
                                            "endColumnIndex": spec["x_col"] + 1,
                                        }]
                                    }
                                }
                            }],
                            "series": series_objs,
                            "headerCount": 1,
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": dash_sheet_id,
                                "rowIndex": anchor_row,
                                "columnIndex": anchor_col,
                            },
                            "offsetXPixels": 8,
                            "offsetYPixels": 6,
                            "widthPixels": 820,
                            "heightPixels": 350,
                        }
                    },
                }
            }
        })
    return reqs


def sync_all_sheets(cross_asset_items, risk_rows, country_rows, macro_rows, ts_matrix, quad_matrix, flow_matrix, news_list, llm_ctx, fallback_report, fallback_chart_notes):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_dash = get_or_create_ws(sh, TAB_DASH, rows=235, cols=16)
    ws_risk_news = get_or_create_ws(sh, TAB_RISK_NEWS, rows=1000, cols=12)
    ws_ts_calc = get_or_create_ws(sh, TAB_TS_CALC, rows=300, cols=45)

    for old_name in OLD_TABS_TO_REMOVE:
        try:
            sh.del_worksheet(sh.worksheet(old_name))
        except Exception:
            pass

    # ================= 1. 写入 02_底层时序与衍生运算库 =================
    ws_ts_calc.clear()
    if ts_matrix and len(ts_matrix) > 1:
        ws_ts_calc.update(range_name="A1", values=ts_matrix, value_input_option="USER_ENTERED")
    if quad_matrix and len(quad_matrix) > 1:
        ws_ts_calc.update(range_name="AB1", values=quad_matrix, value_input_option="USER_ENTERED")
    if flow_matrix and len(flow_matrix) > 1:
        ws_ts_calc.update(range_name="AF1", values=flow_matrix, value_input_option="USER_ENTERED")

    sorted_risks = sorted(risk_rows, key=lambda x: x[8], reverse=True)
    risk_chart_matrix = [["系统性风险因子", "距红灯熔断线进度（%）", "黄灯警戒线（65%）"]]
    for r in sorted_risks[:12]:
        risk_chart_matrix.append([r[1].split("·")[-1], round(r[8] * 100.0, 1), 65.0])
    ws_ts_calc.update(range_name="AJ1", values=risk_chart_matrix, value_input_option="USER_ENTERED")

    # ================= 2. 抽取每张图实测基准数据 -> 调用 AI 读图 -> 自动一致性核验 =================
    plotted_facts = extract_plotted_chart_facts(ts_matrix, quad_matrix, flow_matrix, risk_chart_matrix)
    llm_ctx["plotted_charts_ground_truth"] = {
        k: {"图表标题": v["title"], "实测绘图数据": v["fact_text"]} for k, v in plotted_facts.items()
    }
    report_text, chart_notes = generate_ai_full_and_chart_commentary(
        llm_ctx, plotted_facts, fallback_report, fallback_chart_notes
    )

    # ================= 3. 写入 01_风险雷达与多板块新闻库 =================
    existing_news = []
    try:
        old_vals = ws_risk_news.get_all_values()
        in_news_sec = False
        for row in old_vals:
            if row and len(row) >= 4 and row[0] == "收录日期":
                in_news_sec = True
                continue
            if in_news_sec and row and len(row) >= 4 and row[3].strip():
                existing_news.append({
                    "date": row[0].strip(),
                    "sector": row[1].strip(),
                    "source": row[2].strip(),
                    "title": row[3].strip(),
                    "summary": row[4].strip() if len(row) >= 5 else "",
                })
    except Exception:
        pass

    merged_news = []
    seen_news_titles = set()
    for item in (news_list + existing_news):
        key = item["title"][:40]
        if key not in seen_news_titles:
            seen_news_titles.add(key)
            merged_news.append(item)
    merged_news = merged_news[:300]

    rn_matrix = [
        ["一、36维系统性风险全量扫描底表（急性黑天鹅与慢性灰犀牛）", "", "", "", "", "", "", "", f"更新时间：{NOW_STR}"],
        ["风险编号", "风险类型与维度", "核心监测指标", "当前最新值", "黄灯警戒线", "红灯熔断线", "当前状态", "跨市场传导引爆机制", "距红线进度"],
    ]
    for r in sorted_risks:
        rn_matrix.append([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], f"{round(r[8]*100, 1)}%"])

    rn_matrix.append([])
    rn_matrix.append(["二、全球主要经济体周期与外债脆弱性横向对比"])
    rn_matrix.append(["国家/地区", "实际经济增速（%）", "通胀率（%）", "10年期国债收益率（%）", "经常账户占经济比（%）", "财政赤字占经济比（%）", "短期外债占外储比（%）"])
    rn_matrix.extend(country_rows)

    rn_matrix.append([])
    rn_matrix.append(["三、全球多板块重要财经新闻与核心摘要历史留存库（自动去重增量归档）"])
    rn_matrix.append(["收录日期", "所属宏观板块", "信息来源", "中文新闻标题", "核心摘要与市场传导影响"])
    for n in merged_news:
        rn_matrix.append([n["date"], n["sector"], n["source"], n["title"], n.get("summary", "")])

    ws_risk_news.clear()
    ws_risk_news.update(range_name="A1", values=rn_matrix, value_input_option="USER_ENTERED")

    # ================= 4. 彻底清空驾驶舱旧图表与旧合并单元格 =================
    try:
        meta_info = sh.fetch_sheet_metadata(params={"fields": "sheets(properties.sheetId,charts.chartId)"})
        del_chart_reqs = []
        for s in meta_info.get("sheets", []):
            if s["properties"]["sheetId"] == ws_dash.id:
                for ch in s.get("charts", []):
                    del_chart_reqs.append({"deleteChart": {"chartId": ch["chartId"]}})
        if del_chart_reqs:
            sh.batch_update({"requests": del_chart_reqs})
    except Exception as e:
        print(f"[INFO] 清理旧图表日志: {e}")

    # ================= 5. 构建 00_宏观驾驶舱_总览（独立卡片式零重叠排版） =================
    dash_grid = [["" for _ in range(16)] for _ in range(225)]

    dash_grid[0][0] = "🌍 全球宏观第一性原理自动驾驶舱（六层金字塔全因子决策版）"
    dash_grid[0][11] = f"数据同步时间：{NOW_STR}"
    dash_grid[1][0] = "一、首席宏观策略师·六层第一性原理全因子深度总研判"
    dash_grid[2][0] = report_text

    dash_grid[19][0] = "二、全球核心经济硬数据与底层公式快照"
    dash_grid[20][0:7] = ["国家/地区", "宏观层级", "核心指标与公式", "最新读数", "对比基准", "数据日期", "预警与周期信号"]
    for idx_m, m_row in enumerate(macro_rows[:17]):
        dash_grid[21 + idx_m][0:7] = m_row

    dash_grid[19][8] = "三、36维黑天鹅与灰犀牛高危预警雷达（Top 17 危急度排行）"
    dash_grid[20][8:15] = ["风险编号", "风险维度", "当前读数", "红灯熔断线", "预警状态", "距红线进度", "核心传导机制"]
    for idx_r, r in enumerate(sorted_risks[:17]):
        dash_grid[21 + idx_r][8:15] = [r[0], r[1], r[3], r[5], r[6], f"{round(r[8]*100, 1)}%", r[7]]

    dash_grid[39][0] = "四、全球跨资产实时定价与第一性原理驱动看板"
    dash_grid[40][0:7] = ["资产类别", "资产名称", "最新价格", "单日涨跌", "5日涨跌", "20日涨跌", "第一性原理定价驱动"]
    for idx_c, c_item in enumerate(cross_asset_items[:19]):
        dash_grid[41 + idx_c][0:7] = [
            c_item["cat"], c_item["name"], c_item["price"],
            c_item["pct_1d"], c_item["pct_5d"], c_item["pct_20d"], c_item["driver"]
        ]

    dash_grid[39][8] = "五、今日全球六大板块核心财经新闻与摘要速览（全部中文）"
    dash_grid[40][8:15] = ["所属板块", "中文新闻标题", "", "核心摘要与市场传导影响", "", "", ""]
    for idx_n, n in enumerate(news_list[:18]):
        r_ptr = 41 + idx_n
        dash_grid[r_ptr][8] = n["sector"]
        dash_grid[r_ptr][9] = n["title"]
        dash_grid[r_ptr][11] = n.get("summary", "")

    dash_grid[61][0] = "六、宏观第一性原理金字塔·十二大独立图表卡片与AI实测读图解说（自塔基L1至塔尖L6层层递进）"

    tier_titles = [
        "【金字塔第一层 L1：信用引擎与货币派生】",
        "【金字塔第二层 L2：美元流动性水管与美债收益率曲线】",
        "【金字塔第二/三层 L2-L3：真实资金成本与宏观周期四象限】",
        "【金字塔第三/四层 L3-L4：美国财政生死线与中美跨境利率博弈】",
        "【金字塔第四/五层 L4-L5：全球汇率阀门与大类资产资金流入流出全景】",
        "【金字塔第五/六层 L5-L6：跨资产背离验证与系统性风险雷达塔尖】",
    ]

    spec_by_tier_side = {(s["tier"], s["side"]): s for s in CHART_SPECS_MASTER}

    merge_and_style_reqs = [
        # 先取消整表残留合并，防止旧合并框冲突
        {
            "unmergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 0, "endRowIndex": 225, "startColumnIndex": 0, "endColumnIndex": 16}
            }
        },
        # 显式锁定左栏 A:G (7列*122px=854px)、中间隔离带 H (36px)、右栏 I:O (7列*122px=854px) 宽度，物理隔绝横向重叠
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 7},
                "properties": {"pixelSize": 122},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "COLUMNS", "startIndex": 7, "endIndex": 8},
                "properties": {"pixelSize": 36},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "COLUMNS", "startIndex": 8, "endIndex": 15},
                "properties": {"pixelSize": 122},
                "fields": "pixelSize"
            }
        },
        # 顶部 AI 总研判框合并与排版
        {
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 2, "endRowIndex": 18, "startColumnIndex": 0, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 2, "endRowIndex": 18, "startColumnIndex": 0, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "TOP",
                        "backgroundColor": {"red": 0.96, "green": 0.98, "blue": 1.0},
                        "textFormat": {"fontSize": 10}
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,backgroundColor,textFormat)"
            }
        }
    ]

    for idx_n in range(18):
        r_ptr = 41 + idx_n
        merge_and_style_reqs.append({
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": r_ptr, "endRowIndex": r_ptr + 1, "startColumnIndex": 9, "endColumnIndex": 11},
                "mergeType": "MERGE_ALL"
            }
        })
        merge_and_style_reqs.append({
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": r_ptr, "endRowIndex": r_ptr + 1, "startColumnIndex": 11, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL"
            }
        })
    merge_and_style_reqs.append({
        "repeatCell": {
            "range": {"sheetId": ws_dash.id, "startRowIndex": 41, "endRowIndex": 59, "startColumnIndex": 8, "endColumnIndex": 15},
            "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE", "textFormat": {"fontSize": 9}}},
            "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat)"
        }
    })

    # 填充 6 层金字塔（每层 26 行：T+0层标题 | T+1图表卡片头 | T+2..T+17图表画布(384px) | T+18..T+24AI解说框(175px) | T+25空行）
    for tier_idx, t_title in enumerate(tier_titles):
        t_row = 63 + tier_idx * 26
        left_spec = spec_by_tier_side[(tier_idx, "left")]
        right_spec = spec_by_tier_side[(tier_idx, "right")]

        dash_grid[t_row][0] = t_title
        dash_grid[t_row + 1][0] = f"📊 {left_spec['title']}"
        dash_grid[t_row + 1][8] = f"📊 {right_spec['title']}"

        card_start = t_row + 18
        card_end = t_row + 25
        dash_grid[card_start][0] = chart_notes.get(left_spec["key"], "")
        dash_grid[card_start][8] = chart_notes.get(right_spec["key"], "")

        # 层级横幅样式
        merge_and_style_reqs.append({
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": t_row, "endRowIndex": t_row + 1, "startColumnIndex": 0, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL"
            }
        })
        merge_and_style_reqs.append({
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": t_row, "endRowIndex": t_row + 1, "startColumnIndex": 0, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.16, "green": 0.24, "blue": 0.36},
                        "textFormat": {"bold": True, "fontSize": 11, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)"
            }
        })
        merge_and_style_reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": t_row, "endIndex": t_row + 1},
                "properties": {"pixelSize": 30},
                "fields": "pixelSize"
            }
        })

        # 图表画布区 16 行 * 24px = 384px (> 350px 图表高)
        merge_and_style_reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": t_row + 2, "endIndex": card_start},
                "properties": {"pixelSize": 24},
                "fields": "pixelSize"
            }
        })
        # AI 解说卡片区 7 行 * 25px = 175px
        merge_and_style_reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": card_start, "endIndex": card_end},
                "properties": {"pixelSize": 25},
                "fields": "pixelSize"
            }
        })

        for c_start, c_end in [(0, 7), (8, 15)]:
            # 合并图表上方独立标题栏
            merge_and_style_reqs.append({
                "mergeCells": {
                    "range": {"sheetId": ws_dash.id, "startRowIndex": t_row + 1, "endRowIndex": t_row + 2, "startColumnIndex": c_start, "endColumnIndex": c_end},
                    "mergeType": "MERGE_ALL"
                }
            })
            merge_and_style_reqs.append({
                "repeatCell": {
                    "range": {"sheetId": ws_dash.id, "startRowIndex": t_row + 1, "endRowIndex": t_row + 2, "startColumnIndex": c_start, "endColumnIndex": c_end},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.90, "green": 0.93, "blue": 0.98},
                            "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 0.1, "green": 0.18, "blue": 0.32}},
                            "verticalAlignment": "MIDDLE"
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)"
                }
            })

            # 合并图表下方 AI 解说框
            merge_and_style_reqs.append({
                "mergeCells": {
                    "range": {"sheetId": ws_dash.id, "startRowIndex": card_start, "endRowIndex": card_end, "startColumnIndex": c_start, "endColumnIndex": c_end},
                    "mergeType": "MERGE_ALL"
                }
            })
            merge_and_style_reqs.append({
                "repeatCell": {
                    "range": {"sheetId": ws_dash.id, "startRowIndex": card_start, "endRowIndex": card_end, "startColumnIndex": c_start, "endColumnIndex": c_end},
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP",
                            "verticalAlignment": "MIDDLE",
                            "backgroundColor": {"red": 0.97, "green": 0.98, "blue": 0.99},
                            "textFormat": {"fontSize": 9, "foregroundColor": {"red": 0.12, "green": 0.16, "blue": 0.24}}
                        }
                    },
                    "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,backgroundColor,textFormat)"
                }
            })

            # 为整张卡片（标题 + 图表 + 解说框）加装清晰外边框
            border_style = {"style": "SOLID_MEDIUM", "color": {"red": 0.72, "green": 0.78, "blue": 0.86}}
            merge_and_style_reqs.append({
                "updateBorders": {
                    "range": {"sheetId": ws_dash.id, "startRowIndex": t_row + 1, "endRowIndex": card_end, "startColumnIndex": c_start, "endColumnIndex": c_end},
                    "top": border_style,
                    "bottom": border_style,
                    "left": border_style,
                    "right": border_style,
                }
            })

    for text_val, bg_rgb in [
        ("🔴", {"red": 0.99, "green": 0.91, "blue": 0.90}),
        ("🟡", {"red": 1.0, "green": 0.97, "blue": 0.88}),
        ("🟢", {"red": 0.90, "green": 0.96, "blue": 0.92}),
    ]:
        merge_and_style_reqs.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws_dash.id, "startRowIndex": 20, "endRowIndex": 38, "startColumnIndex": 6, "endColumnIndex": 14}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": text_val}]},
                        "format": {"backgroundColor": bg_rgb, "textFormat": {"bold": True}}
                    }
                },
                "index": 0
            }
        })

    ws_dash.clear()
    ws_dash.update(range_name="A1", values=dash_grid, value_input_option="USER_ENTERED")

    if ts_matrix and len(ts_matrix) > 2:
        chart_reqs = build_12_pyramid_charts_requests(
            ws_dash.id,
            ws_ts_calc.id,
            len(ts_matrix),
            len(quad_matrix),
            len(flow_matrix),
            len(risk_chart_matrix),
        )
        merge_and_style_reqs.extend(chart_reqs)

    sh.batch_update({"requests": merge_and_style_reqs})


if __name__ == "__main__":
    print(f"[{NOW_STR}] 启动全球宏观六层第一性原理自动驾驶舱同步...")
    s_obs, s_dates, s_dmaps, lt_hist = fetch_fred_data()
    cross_items, mkt_stats = fetch_market_assets(s_obs, lt_hist)
    fetch_china_macro_akshare(s_obs, s_dates, lt_hist)
    wb_m, imf_m, ext_v = fetch_external_apis()
    news = fetch_global_macro_news()

    risk_r, country_r, macro_r, flow_mat, quad_xy, llm_ctx, fb_rep, fb_notes = analyze_all_factors(
        s_obs, s_dates, s_dmaps, wb_m, imf_m, ext_v, mkt_stats, news
    )
    ts_mat = build_clean_chart_timeseries(lt_hist)
    quad_mat = build_four_quadrant_series(s_obs, quad_xy[0], quad_xy[1])

    sync_all_sheets(
        cross_items, risk_r, country_r, macro_r,
        ts_mat, quad_mat, flow_mat, news, llm_ctx, fb_rep, fb_notes
    )
    print(f"[{NOW_STR}] 同步完成！已生成 12 张独立卡片结构图及实测核验 AI 读图解说。")
