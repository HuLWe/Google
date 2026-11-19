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

# 精简后的 3 张核心工作表（彻底消除冗余）
TAB_DASH = "00_宏观驾驶舱_总览"
TAB_RISK_NEWS = "01_风险雷达与多板块新闻库"
TAB_TS_CALC = "02_底层时序与衍生运算库"

# 清理所有旧版冗余工作表
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

# ================= 2. 底层宏观指标字典 (全中文命名) =================
FRED_SERIES = [
    # 第一层：信用引擎与货币存量
    {"id": "M1SL", "country": "美国", "layer": "第一层·信用引擎", "freq": "月", "lt": False, "name": "美国狭义货币M1存量（十亿美元）"},
    {"id": "M2SL", "country": "美国", "layer": "第一层·信用引擎", "freq": "月", "lt": False, "name": "美国广义货币M2存量（十亿美元）"},
    {"id": "TOTLL", "country": "美国", "layer": "第一层·信用引擎", "freq": "周", "lt": True, "name": "美国商业银行总信贷规模（十亿美元）"},
    {"id": "BUSLOANS", "country": "美国", "layer": "第一层·信用引擎", "freq": "月", "lt": False, "name": "美国工商业贷款余额（十亿美元）"},
    {"id": "DRTSCILM", "country": "美国", "layer": "第一层·信用引擎", "freq": "季", "lt": False, "name": "美联储银行收紧贷款标准比例（%）"},
    {"id": "BAMLH0A0HYM2", "country": "美国", "layer": "第一层·信用引擎", "freq": "日", "lt": True, "name": "美国高收益垃圾债信用利差（%）"},
    {"id": "BAMLC0A0CM", "country": "美国", "layer": "第一层·信用引擎", "freq": "日", "lt": False, "name": "美国投资级企业债信用利差（%）"},
    # 第二层：美元水管与利率锚
    {"id": "WALCL", "country": "美国", "layer": "第二层·流动性管道", "freq": "周", "lt": True, "name": "美联储资产负债表总规模（百万美元）"},
    {"id": "WDTGAL", "country": "美国", "layer": "第二层·流动性管道", "freq": "周", "lt": True, "name": "美国财政部一般账户TGA余额（百万美元）"},
    {"id": "RRPONTSYD", "country": "美国", "layer": "第二层·流动性管道", "freq": "日", "lt": True, "name": "美联储隔夜逆回购余额（十亿美元）"},
    {"id": "WRESBAL", "country": "美国", "layer": "第二层·流动性管道", "freq": "周", "lt": True, "name": "商业银行准备金余额（百万美元）"},
    {"id": "IORB", "country": "美国", "layer": "第二层·流动性管道", "freq": "日", "lt": False, "name": "美联储准备金余额利率（%）"},
    {"id": "DFF", "country": "美国", "layer": "第二层·利率之锚", "freq": "日", "lt": True, "name": "联邦基金有效利率（%）"},
    {"id": "DGS2", "country": "美国", "layer": "第二层·利率之锚", "freq": "日", "lt": True, "name": "美国2年期国债收益率（%）"},
    {"id": "DGS10", "country": "美国", "layer": "第二层·利率之锚", "freq": "日", "lt": True, "name": "美国10年期国债收益率（%）"},
    {"id": "DGS30", "country": "美国", "layer": "第二层·利率之锚", "freq": "日", "lt": False, "name": "美国30年期国债收益率（%）"},
    {"id": "DFII10", "country": "美国", "layer": "第二层·利率之锚", "freq": "日", "lt": True, "name": "美国10年期通胀保值债券实际利率（%）"},
    {"id": "T10Y2Y", "country": "美国", "layer": "第二层·利率之锚", "freq": "日", "lt": True, "name": "美债10年期与2年期期限利差（%）"},
    {"id": "IRLTLT01JPM156N", "country": "日本", "layer": "第二层·利率之锚", "freq": "月", "lt": False, "name": "日本10年期国债收益率（%）"},
    {"id": "IRLTLT01DEM156N", "country": "德国", "layer": "第二层·利率之锚", "freq": "月", "lt": False, "name": "德国10年期国债收益率（%）"},
    # 第三层：经济硬数据与财政生死线
    {"id": "PAYEMS", "country": "美国", "layer": "第三层·就业硬数据", "freq": "月", "lt": False, "name": "美国非农就业总人数（千人）"},
    {"id": "ICSA", "country": "美国", "layer": "第三层·就业硬数据", "freq": "周", "lt": True, "name": "美国周度初请失业金人数（人）"},
    {"id": "UNRATE", "country": "美国", "layer": "第三层·就业硬数据", "freq": "月", "lt": False, "name": "美国失业率（%）"},
    {"id": "SAHMREALTIME", "country": "美国", "layer": "第三层·就业硬数据", "freq": "月", "lt": False, "name": "官方萨姆规则衰退指标（%）"},
    {"id": "CES0500000003", "country": "美国", "layer": "第三层·就业硬数据", "freq": "月", "lt": False, "name": "美国私营部门平均时薪（美元/小时）"},
    {"id": "INDPRO", "country": "美国", "layer": "第三层·工业与消费", "freq": "月", "lt": False, "name": "美国工业生产总指数"},
    {"id": "RSAFS", "country": "美国", "layer": "第三层·工业与消费", "freq": "月", "lt": False, "name": "美国零售与食品服务销售额（百万美元）"},
    {"id": "CPIAUCSL", "country": "美国", "layer": "第三层·通胀硬数据", "freq": "月", "lt": False, "name": "美国消费者物价指数CPI"},
    {"id": "PCEPILFE", "country": "美国", "layer": "第三层·通胀硬数据", "freq": "月", "lt": False, "name": "美国核心个人消费支出物价指数PCE"},
    {"id": "A091RC1Q027SBEA", "country": "美国", "layer": "第三层·财政生死线", "freq": "季", "lt": False, "name": "美国联邦政府利息支出年率（十亿美元）"},
    {"id": "FGRECPT", "country": "美国", "layer": "第三层·财政生死线", "freq": "季", "lt": False, "name": "美国联邦政府经常性财政收入年率（十亿美元）"},
    {"id": "GDP", "country": "美国", "layer": "第三层·财政生死线", "freq": "季", "lt": False, "name": "美国名义国内生产总值GDP年率（十亿美元）"},
    {"id": "GFDEGDQ188S", "country": "美国", "layer": "第三层·财政生死线", "freq": "季", "lt": False, "name": "美国联邦总债务占GDP比重（%）"},
    {"id": "CP0000EZ19M086NEST", "country": "欧元区", "layer": "第三层·通胀硬数据", "freq": "月", "lt": False, "name": "欧元区调和消费者物价指数"},
    {"id": "LRHUTTTTEZM156S", "country": "欧元区", "layer": "第三层·就业硬数据", "freq": "月", "lt": False, "name": "欧元区失业率（%）"},
    {"id": "JPNPRMNTO01GPSAM", "country": "日本", "layer": "第三层·工业与消费", "freq": "月", "lt": False, "name": "日本制造业生产环比增速（%）"},
    {"id": "JPNCPIALLMINMEI", "country": "日本", "layer": "第三层·通胀硬数据", "freq": "月", "lt": False, "name": "日本全国消费者物价指数"},
    # 第四/五层：跨境与能源现货
    {"id": "DTWEXBGS", "country": "美国", "layer": "第四层·跨境汇率", "freq": "日", "lt": False, "name": "美联储广义贸易加权美元指数"},
    {"id": "DCOILWTICO", "country": "美国", "layer": "第五层·能源现货", "freq": "日", "lt": True, "name": "西德克萨斯轻质原油现货价格（美元/桶）"},
    # 第六层：黑天鹅与灰犀牛专属数据库
    {"id": "VXVCLS", "country": "美国", "layer": "第六层·黑天鹅雷达", "freq": "日", "lt": False, "name": "芝加哥期权交易所3个月期波动率指数"},
    {"id": "STLFSI4", "country": "美国", "layer": "第六层·黑天鹅雷达", "freq": "周", "lt": True, "name": "圣路易斯联储金融压力指数"},
    {"id": "NFCI", "country": "美国", "layer": "第六层·黑天鹅雷达", "freq": "周", "lt": False, "name": "芝加哥联储金融状况指数"},
    {"id": "WDISBORROW", "country": "美国", "layer": "第六层·黑天鹅雷达", "freq": "周", "lt": False, "name": "美联储贴现窗口紧急借款周均值（百万美元）"},
    {"id": "DRCRELEXFACBS", "country": "美国", "layer": "第六层·灰犀牛雷达", "freq": "季", "lt": False, "name": "美国商业地产银行贷款违约率（%）"},
    {"id": "DRCCLACBS", "country": "美国", "layer": "第六层·灰犀牛雷达", "freq": "季", "lt": False, "name": "美国商业银行信用卡贷款违约率（%）"},
    {"id": "DRSFRMACBS", "country": "美国", "layer": "第六层·灰犀牛雷达", "freq": "季", "lt": False, "name": "美国居民住房抵押贷款违约率（%）"},
    {"id": "PSAVERT", "country": "美国", "layer": "第六层·灰犀牛雷达", "freq": "月", "lt": False, "name": "美国个人储蓄率占可支配收入比（%）"},
    {"id": "TDSP", "country": "美国", "layer": "第六层·灰犀牛雷达", "freq": "季", "lt": False, "name": "美国家庭债务偿付占可支配收入比（%）"},
    {"id": "DPSACBW027SBOG", "country": "美国", "layer": "第六层·灰犀牛雷达", "freq": "周", "lt": False, "name": "美国商业银行总存款余额（十亿美元）"},
]

# ================= 3. 全球跨资产实时行情字典 (纯中文展示) =================
MARKET_ASSETS = [
    {"cat": "核心股指", "ticker": "^GSPC", "code": "SPX", "lt": True, "name": "美国标普500指数", "driver": "美股盈利预期 × 10年期国债贴现率"},
    {"cat": "核心股指", "ticker": "^NDX", "code": "NDX", "lt": True, "name": "美国纳斯达克100指数", "driver": "远期现金流久期资产，对实际利率极敏感"},
    {"cat": "核心股指", "ticker": "^SOX", "code": "SOX", "lt": False, "name": "美国费城半导体指数", "driver": "全球人工智能算力资本开支与芯片周期温度计"},
    {"cat": "核心股指", "ticker": "000300.SS", "fallback_ticker": "ASHR", "code": "CSI300", "lt": True, "name": "中国沪深300指数（含A股ETF映射）", "driver": "中国社融信贷脉冲与货币活化剪刀差"},
    {"cat": "核心股指", "ticker": "^HSI", "code": "HSI", "lt": False, "name": "中国香港恒生指数", "driver": "中国经济基本面分子 × 美元流动性分母双重定价"},
    {"cat": "核心股指", "ticker": "^N225", "code": "N225", "lt": False, "name": "日本日经225指数", "driver": "日元汇率波动与日本央行货币政策转向预期"},
    {"cat": "核心股指", "ticker": "^GDAXI", "code": "DAX", "lt": False, "name": "德国法兰克福DAX指数", "driver": "欧元区制造业周期与欧央行降息路径"},
    {"cat": "风险温度计", "ticker": "KRE", "code": "US_REG_BANK", "lt": True, "name": "美国区域银行指数基金", "driver": "美国中小银行商业地产敞口与存款稳定性温度计"},
    {"cat": "风险温度计", "ticker": "HYG", "code": "US_HY_ETF", "lt": False, "name": "美国高收益企业债基金", "driver": "垃圾债信用利差与企业再融资流动性实时定价"},
    {"cat": "风险温度计", "ticker": "TLT", "code": "US_20Y_BOND", "lt": False, "name": "美国20年以上长期国债基金", "driver": "长端美债久期风险与美国主权信用折价锚"},
    {"cat": "全球外汇", "ticker": "DX-Y.NYB", "code": "DXY", "lt": True, "name": "美元指数", "driver": "全球离岸美元流动性松紧总阀门"},
    {"cat": "全球外汇", "ticker": "JPY=X", "code": "USDJPY", "lt": True, "name": "美元兑日元汇率", "driver": "全球套息交易平仓踩踏核心风向标"},
    {"cat": "全球外汇", "ticker": "USDCNH=X", "fallback_ticker": "CNY=X", "code": "USDCNH", "lt": True, "name": "美元兑离岸人民币汇率", "driver": "中美利差倒挂深度与外贸结汇意愿"},
    {"cat": "全球外汇", "ticker": "EURUSD=X", "code": "EURUSD", "lt": False, "name": "欧元兑美元汇率", "driver": "欧美真实利差与欧洲能源贸易条件"},
    {"cat": "大宗商品", "ticker": "GC=F", "code": "GOLD", "lt": True, "name": "纽约商品交易所黄金期货（美元/盎司）", "driver": "10年期实际利率 + 全球央行去美元化购金"},
    {"cat": "大宗商品", "ticker": "SI=F", "code": "SILVER", "lt": False, "name": "纽约商品交易所白银期货（美元/盎司）", "driver": "贵金属金融属性 + 光伏工业需求弹性"},
    {"cat": "大宗商品", "ticker": "BZ=F", "code": "BRENT", "lt": True, "name": "北海布伦特原油期货（美元/桶）", "driver": "全球通胀预期与地缘供给冲击滞胀扳机"},
    {"cat": "大宗商品", "ticker": "HG=F", "code": "COPPER", "lt": True, "name": "纽约精炼铜期货（美元/磅）", "driver": "全球制造业与电网及数据中心实体需求"},
    {"cat": "大宗商品", "ticker": "ZW=F", "code": "WHEAT", "lt": False, "name": "芝加哥小麦期货（美分/蒲式耳）", "driver": "全球粮食供应链安全与新兴市场食品通胀"},
    {"cat": "恐慌波动", "ticker": "^VIX", "code": "VIX", "lt": True, "name": "美股市场恐慌波动率指数", "driver": "美股期权隐含尾部风险对冲成本"},
]

CRYPTO_ASSETS = [
    {"cg_id": "bitcoin", "yf_ticker": "BTC-USD", "code": "BTC", "lt": True, "name": "比特币现货（美元）", "driver": "全球美元净流动性高弹性放大器"},
    {"cg_id": "ethereum", "yf_ticker": "ETH-USD", "code": "ETH", "lt": False, "name": "以太坊现货（美元）", "driver": "链上金融流动性与科技风险偏好映射"},
]

COUNTRIES_LIST = ["USA", "CHN", "JPN", "DEU", "IND", "BRA", "IDN", "TUR"]


# ================= 4. 多板块全球新闻抓取 + 中文自动翻译/归类 + 历史留存 =================
def translate_headlines_to_chinese(news_items):
    """使用配置的中转站 AI 或规则字典将所有英文新闻标题批量翻译为地道中文"""
    if not news_items:
        return news_items

    # 优先使用 AI 批量翻译并提炼市场影响（保证 100% 纯中文且无英文残留）
    if OPENAI_API_KEY:
        try:
            raw_list = [f"{i+1}. [{n['sector']}] {n['title']}" for i, n in enumerate(news_items)]
            prompt = (
                "请将以下财经新闻标题全部翻译为准确、专业、简练的简体中文，"
                "严禁保留任何英文单词（人名/机构名如美联储、鲍威尔、标普、欧央行、日本央行等必须用中文标准译名）。"
                "按 JSON 数组格式返回，每个元素包含 index(序号) 和 zh_title(中文标题)，不要输出任何其他多余文字：\n"
                + "\n".join(raw_list)
            )
            headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
            body = {
                "model": OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 3000,
            }
            resp = HTTP.post(f"{OPENAI_BASE_URL}/chat/completions", headers=headers, json=body, timeout=(10, 60))
            if resp.status_code == 200:
                content = resp.json()["choices"][0]["message"]["content"].strip()
                json_match = re.search(r"\[.*\]", content, re.DOTALL)
                if json_match:
                    parsed = json.loads(json_match.group(0))
                    zh_map = {int(x["index"]): str(x["zh_title"]).strip() for x in parsed if "index" in x and "zh_title" in x}
                    for i, n in enumerate(news_items):
                        if (i + 1) in zh_map and zh_map[i + 1]:
                            n["title"] = zh_map[i + 1]
                    return news_items
        except Exception as e:
            print(f"[INFO] AI headline translation fallback: {e}")

    # 兜底词典替换，确保无生硬英文
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
    for n in news_items:
        t = n["title"]
        for pat, zh in replacements:
            t = re.sub(pat, zh, t, flags=re.IGNORECASE)
        n["title"] = t
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
    """多板块全局新闻抓取（涵盖央行、债市、科技AI、大宗商品、外汇、地缘6大板块）"""
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
                if title_el is None or not title_el.text:
                    continue
                title = re.sub(r"\s+", " ", title_el.text).strip()
                norm_key = title.lower()[:55]
                if norm_key in seen_titles:
                    continue
                seen_titles.add(norm_key)
                sector = classify_news_sector(title.lower(), default_sec)
                collected.append({
                    "date": TODAY_STR,
                    "sector": sector,
                    "source": src_name,
                    "title": title[:140],
                })
                count_per_src += 1
                if count_per_src >= 4:
                    break
        except Exception as e:
            print(f"[WARN] 新闻源 {src_name} 抓取跳过: {e}")

    return translate_headlines_to_chinese(collected[:20])


# ================= 5. 数据抓取模块 =================
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


def fetch_spy_pe_proxy():
    for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
        url = f"https://{host}/v10/finance/quoteSummary/SPY?modules=summaryDetail,defaultKeyStatistics"
        try:
            resp = HTTP.get(url, timeout=(8, 15))
            if resp.status_code != 200:
                continue
            res = resp.json().get("quoteSummary", {}).get("result", [])
            if not res:
                continue
            sd = res[0].get("summaryDetail", {})
            ks = res[0].get("defaultKeyStatistics", {})
            pe = (ks.get("forwardPE") or {}).get("raw") or (sd.get("trailingPE") or {}).get("raw")
            if pe and float(pe) > 0:
                return round(float(pe), 2)
        except Exception:
            continue
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
            "price": p0, "pct_1d": pct_1d, "pct_5d": pct_5d, "pct_20d": pct_20d
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
        time.sleep(0.05)

    cg_ids = ",".join(c["cg_id"] for c in CRYPTO_ASSETS)
    cg_data = {}
    try:
        cg_url = (
            f"https://api.coingecko.com/api/v3/coins/markets"
            f"?vs_currency=usd&ids={cg_ids}&price_change_percentage=24h,7d,30d"
        )
        resp = HTTP.get(cg_url, timeout=(8, 20))
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
            pct_5d = calc_pct(yf_prices, 5)
            pct_20d = calc_pct(yf_prices, min(20, len(yf_prices) - 1))
            series_obs[c_item["code"]] = yf_prices
        else:
            continue

        market_stats[c_item["code"]] = {
            "price": p0, "pct_1d": pct_1d, "pct_5d": pct_5d, "pct_20d": pct_20d
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

    spy_pe = fetch_spy_pe_proxy()
    if spy_pe:
        series_obs["SPX_PE"] = [spy_pe]

    return cross_asset_items, market_stats


def fetch_fred_data():
    series_obs = {}
    series_dates = {}
    lt_hist_dates = {}

    for item in FRED_SERIES:
        obs_start = (datetime.now(TZ_CST) - timedelta(days=1120)).strftime("%Y-%m-%d")
        if item.get("lt"):
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={item['id']}&api_key={FRED_API_KEY}"
                f"&file_type=json&sort_order=desc&observation_start={obs_start}"
            )
        else:
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={item['id']}&api_key={FRED_API_KEY}"
                f"&file_type=json&sort_order=desc&limit=55"
            )
        try:
            resp = HTTP.get(url, timeout=(10, 30))
            resp.raise_for_status()
            obs = [o for o in resp.json().get("observations", []) if o.get("value") != "."]
            if not obs:
                continue
            vals = [float(o["value"]) for o in obs]
            series_obs[item["id"]] = vals
            series_dates[item["id"]] = obs[0]["date"]
            if item.get("lt"):
                lt_hist_dates[item["id"]] = {o["date"]: float(o["value"]) for o in obs}
            time.sleep(0.05)
        except Exception as e:
            print(f"[WARN] 底层序列 {item['name']} 获取跳过: {e}")
    return series_obs, series_dates, lt_hist_dates


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

    # 中国10年期国债收益率（同步写入长期历史库以支撑中美利差结构图）
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


def fetch_external_apis(series_obs):
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


# ================= 6. 底层时序对齐与衍生公式运算引擎 (直接驱动 8 张结构图) =================
CHART_SERIES_COLUMNS = [
    ("日期", "Date"),
    ("美元净流动性（十亿美元）", "NET_LIQUIDITY"),
    ("标普500指数", "SPX"),
    ("美债10Y-2Y期限利差（%）", "SPREAD_10Y2Y"),
    ("联邦基金有效利率（%）", "DFF"),
    ("美国10年期TIPS实际利率（%）", "DFII10"),
    ("COMEX黄金期货（美元/盎司）", "GOLD"),
    ("美国10年期国债收益率（%）", "DGS10"),
    ("中国10年期国债收益率（%）", "CN_DGS10"),
    ("中美10年期利差（%）", "US_CN_SPREAD"),
    ("铜金比（实体周期系数）", "COPPER_GOLD_RATIO"),
    ("布伦特原油（美元/桶）", "BRENT"),
    ("高收益债信用利差（%）", "BAMLH0A0HYM2"),
    ("美股VIX恐慌指数", "VIX"),
    ("美商业银行总信贷（十亿美元）", "TOTLL"),
    ("美国区域银行ETF（KRE）", "US_REG_BANK"),
    ("比特币现货（美元）", "BTC"),
    ("纳斯达克100指数", "NDX"),
    ("美国10年期盈亏平衡通胀预期（%）", "BREAKEVEN_10Y"),
    ("美联储隔夜逆回购（十亿美元）", "RRPONTSYD"),
    ("圣路易斯金融压力指数", "STLFSI4"),
]


def build_clean_chart_timeseries(lt_hist_dates):
    """生成 156 周（近 3 年）连续无缺失值（自动前向填充）的底层运算宽表，100% 保证 Google 图表正常渲染"""
    spx_weekly = sorted(lt_hist_dates.get("SPX", {}).keys())
    if not spx_weekly:
        return []
    weekly_dates = spx_weekly[-156:]

    # 预先获取每条序列的初始基准值，防止开头几周出现空白导致图表断线
    first_valid = {}
    for sid, dmap in lt_hist_dates.items():
        if dmap:
            earliest_dt = min(dmap.keys())
            first_valid[sid] = round(dmap[earliest_dt], 4)

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

        # 底层衍生因子运算
        if all(k in r for k in ["WALCL", "WDTGAL", "RRPONTSYD"]):
            r["NET_LIQUIDITY"] = round((r["WALCL"] - r["WDTGAL"]) / 1000.0 - r["RRPONTSYD"], 2)
        if all(k in r for k in ["DGS10", "DGS2"]):
            r["SPREAD_10Y2Y"] = round(r["DGS10"] - r["DGS2"], 3)
        if all(k in r for k in ["DGS10", "DFII10"]):
            r["BREAKEVEN_10Y"] = round(r["DGS10"] - r["DFII10"], 3)
        if all(k in r for k in ["DGS10", "CN_DGS10"]):
            r["US_CN_SPREAD"] = round(r["DGS10"] - r["CN_DGS10"], 3)
        if all(k in r for k in ["COPPER", "GOLD"]) and r["GOLD"] > 0:
            r["COPPER_GOLD_RATIO"] = round((r["COPPER"] * 1000) / r["GOLD"], 3)

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


# ================= 7. 全因子分析 + 36维风险雷达 + AI 深度中文解说 =================
def generate_ai_full_factor_commentary(llm_ctx, fallback_full_text):
    """调用 https://api.starwish.fit/v1 + GPT-6.2 Astra (思考程度最高) 对所有因子进行纯中文深度解说"""
    prompt = (
        "你是严格遵循《宏观经济六层第一性原理实战通关总纲》的首席宏观策略师。\n"
        "【硬性要求】：\n"
        "1. 必须 100% 使用简体中文撰写，严禁出现任何英文单词或英文缩写（例如：不要写 SPX、DXY、TIPS、CPI、PCE、CRE、ON RRP、HY、VIX、BS、GR 等，一律使用标普500、美元指数、通胀保值债券实际利率、居民消费价格指数、个人消费支出物价指数、商业地产、隔夜逆回购、高收益垃圾债、恐慌指数、黑天鹅、灰犀牛等规范中文名称）；\n"
        "2. 必须对所有核心因子进行全景逐层深度解说，绝不能只总结新闻！必须覆盖：信用引擎（第一层）、美元水管与净流动性冲量（第二层）、美欧日中经济硬数据与美国财政付息生死线（第三层）、跨境汇率与主权外债（第四层）、大宗商品与跨资产背离（第五层）、36维黑天鹅与灰犀牛雷达（第六层）；\n"
        "3. 输出总字数控制在 1200~1600 字之间，逻辑严密、直击因果传导链条。\n\n"
        "请严格按照以下六个部分输出：\n"
        "一、【宏观周期象限与全球新闻催化主线】：结合今日多板块新闻与硬数据，判定当前市场正在交易哪条主线，并指出是否存在高频资产价格抢跑低频数据的背离；\n"
        "二、【第一、二层：信用派生与美元净流动性水管解说】：逐一解说美联储总资产、财政部一般账户、隔夜逆回购、净流动性1周/4周/13周加速度、美债收益率曲线形态（牛陡/熊陡/牛平/熊平）及市场隐含降息预期；\n"
        "三、【第三、四层：美欧日中硬数据与财政债务生死线解说】：逐一解说美国非农就业、失业率与萨姆规则、核心通胀折年率、联邦利息占财政收入比、债务雪球压力，以及中国制造业景气度、社融与货币活化剪刀差、欧日通胀与利率动向；\n"
        "四、【第五层：全球跨资产定价与背离因子解说】：穿透解说实际利率与黄金/纳指/半导体的估值张力、铜金比与原油的实体周期信号、美元与日元/离岸人民币的汇率博弈；\n"
        "五、【第六层：黑天鹅与灰犀牛风险雷达深度排雷】：点名当前距离红灯熔断线最近的三大急性黑天鹅与三大慢性灰犀牛，详细推演其一旦引爆的跨市场传染路径；\n"
        "六、【三情景概率分布与未来十日量化证伪红线】：给出基准、乐观、悲观情景的精确概率及对应大类资产配置建议，并列出三条具体量化证伪条件。\n\n"
        f"今日全量六层宏观因子、衍生背离与多板块新闻数据：\n{json.dumps(llm_ctx, ensure_ascii=False)}"
    )

    if OPENAI_API_KEY:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        effort_candidates = [OPENAI_REASONING_EFFORT]
        if OPENAI_REASONING_EFFORT.lower() == "max" and "high" not in effort_candidates:
            effort_candidates.append("high")

        # 1. 优先尝试标准 Chat Completions 端点（国内聚合中转站最稳定协议）
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
                    data = resp.json()
                    txt = data["choices"][0]["message"]["content"].strip()
                    if txt:
                        return f"【智能深度全因子解说报告 | 模型：GPT-6.2 思考模式（深度：{effort}） | 更新时间：{NOW_STR}】\n\n{txt}"
                elif resp.status_code == 400 and effort == "max":
                    continue
            except Exception as e:
                print(f"[INFO] ChatCompletions 调用尝试 ({effort}): {e}")

        # 2. 备选 Responses API 端点
        for effort in effort_candidates:
            try:
                resp_url = f"{OPENAI_BASE_URL}/responses"
                body_resp = {
                    "model": OPENAI_MODEL,
                    "reasoning": {"effort": effort},
                    "input": prompt,
                    "max_output_tokens": 16000,
                }
                resp = HTTP.post(resp_url, headers=headers, json=body_resp, timeout=(15, 240))
                if resp.status_code == 200:
                    rj = resp.json()
                    txt = rj.get("output_text", "").strip()
                    if not txt:
                        parts = []
                        for item in rj.get("output", []):
                            if item.get("type") == "message":
                                for c in item.get("content", []):
                                    if c.get("text"):
                                        parts.append(c["text"])
                        txt = "\n".join(parts).strip()
                    if txt:
                        return f"【智能深度全因子解说报告 | 模型：GPT-6.2 思考模式（深度：{effort}） | 更新时间：{NOW_STR}】\n\n{txt}"
                elif resp.status_code == 400 and effort == "max":
                    continue
            except Exception as e:
                print(f"[INFO] Responses API 调用尝试 ({effort}): {e}")

    if OPENROUTER_API_KEY:
        try:
            or_model = OPENAI_MODEL if "/" in OPENAI_MODEL else f"openai/{OPENAI_MODEL}"
            resp = HTTP.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
                json={"model": or_model, "reasoning": {"effort": "high"}, "messages": [{"role": "user", "content": prompt}], "max_tokens": 16000},
                timeout=(15, 180)
            )
            if resp.status_code == 200:
                txt = resp.json()["choices"][0]["message"]["content"].strip()
                if txt:
                    return f"【智能深度全因子解说报告 | 备用路由 | 更新时间：{NOW_STR}】\n\n{txt}"
        except Exception:
            pass

    if GEMINI_API_KEY:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
            resp = HTTP.post(url, json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=(10, 45))
            if resp.status_code == 200:
                txt = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                if txt:
                    return f"【智能深度全因子解说报告 | 备用路由 | 更新时间：{NOW_STR}】\n\n{txt}"
        except Exception:
            pass

    return fallback_full_text


def analyze_all_factors(s_obs, s_dates, wb_metrics, imf_metrics, extra_vals, market_stats, news_list):
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

    # ================= A. 直观核心经济硬数据快照（纯中文） =================
    us_credit_yoy = calc_yoy("TOTLL", lag=52)
    if us_credit_yoy is not None:
        macro_snapshot_rows.append(["美国", "第一层·信用", "商业银行总信贷同比增速", f"{us_credit_yoy:+.2f}%", f"余额 {get('TOTLL'):,.0f} 十亿美元", s_dates.get("TOTLL", "-"), "信用平稳扩张" if us_credit_yoy > 2.0 else "信用创造放缓"])

    nfp_0, nfp_1 = get("PAYEMS", 0), get("PAYEMS", 1)
    nfp_add_10k = None
    if nfp_0 is not None and nfp_1 is not None:
        nfp_add_10k = round((nfp_0 - nfp_1) / 10.0, 2)
        nfp_sig = "就业过热（抑制降息空间）" if nfp_add_10k > 20 else ("温和着陆（10至20万人）" if nfp_add_10k >= 10 else "就业显著降温（低于10万人）")
        macro_snapshot_rows.append(["美国", "第三层·就业", "单月新增非农就业人数", f"{nfp_add_10k:+.2f} 万人", f"总就业 {nfp_0/10:.0f} 万人", s_dates.get("PAYEMS", "-"), nfp_sig])

    unrate_0 = get("UNRATE", 0)
    if unrate_0 is not None:
        macro_snapshot_rows.append(["美国", "第三层·就业", "美国官方失业率与萨姆指标", f"{unrate_0:.1f}%", f"萨姆规则 {sahm:.2f}%", s_dates.get("UNRATE", "-"), "逼近衰退红线" if sahm >= 0.35 else "劳动力市场尚稳"])

    us_cpi_yoy = calc_yoy("CPIAUCSL")
    if us_cpi_yoy is not None:
        macro_snapshot_rows.append(["美国", "第三层·通胀", "美国总体消费物价同比增速", f"{us_cpi_yoy:.2f}%", "目标值 2.00%", s_dates.get("CPIAUCSL", "-"), "通胀仍具黏性" if us_cpi_yoy > 2.5 else "接近通胀目标"])

    pce_0, pce_3 = get("PCEPILFE", 0), get("PCEPILFE", 3)
    pce_3m_ann = None
    if pce_0 and pce_3 and pce_3 > 0:
        pce_3m_ann = round(((pce_0 / pce_3) ** 4 - 1) * 100, 2)
        macro_snapshot_rows.append(["美国", "第三层·通胀", "核心个人消费支出3个月折年率", f"{pce_3m_ann:.2f}%", "警戒线 2.50%", s_dates.get("PCEPILFE", "-"), "短期通胀反弹" if pce_3m_ann > 2.5 else "核心通胀温和"])

    fed_int, fed_rev = get("A091RC1Q027SBEA"), get("FGRECPT")
    interest_rev_ratio = None
    if fed_int and fed_rev and fed_rev > 0:
        interest_rev_ratio = round((fed_int / fed_rev) * 100, 2)
        macro_snapshot_rows.append(["美国", "第三层·财政", "联邦利息支出占财政收入比", f"{interest_rev_ratio:.2f}%", f"年利息 {fed_int:.0f} 十亿美元", s_dates.get("A091RC1Q027SBEA", "-"), "财政付息压力高企" if interest_rev_ratio >= 18.0 else "财政付息尚可控"])

    cn_pmi = get("CN_PMI_MFG")
    if cn_pmi is not None:
        macro_snapshot_rows.append(["中国", "第三层·景气", "官方制造业采购经理指数", f"{cn_pmi:.1f}", "荣枯分界线 50.0", s_dates.get("CN_PMI_MFG", "-"), "处于扩张区间" if cn_pmi >= 50.0 else "处于收缩区间"])

    cn_m1_yoy, cn_m2_yoy = get("CN_M1_YOY"), get("CN_M2_YOY")
    cn_scissors = None
    if cn_m1_yoy is not None and cn_m2_yoy is not None:
        cn_scissors = round(cn_m1_yoy - cn_m2_yoy, 2)
        macro_snapshot_rows.append(["中国", "第一层·货币", "狭义与广义货币剪刀差", f"{cn_scissors:+.2f}%", f"M1 {cn_m1_yoy:.1f}% / M2 {cn_m2_yoy:.1f}%", s_dates.get("CN_M1_YOY", "-"), "资金活化改善" if cn_scissors > -2.0 else "定期存款沉淀偏高"])

    cn_cpi, cn_ppi = get("CN_CPI_YOY"), get("CN_PPI_YOY")
    if cn_cpi is not None and cn_ppi is not None:
        macro_snapshot_rows.append(["中国", "第三层·通胀", "消费物价与工业出厂价格同比", f"CPI {cn_cpi:.1f}% / PPI {cn_ppi:.1f}%", f"剪刀差 {cn_ppi - cn_cpi:+.1f}%", s_dates.get("CN_CPI_YOY", "-"), "工业品通缩压力" if cn_ppi < 0 else "价格温和回升"])

    ea_cpi_yoy = calc_yoy("CP0000EZ19M086NEST")
    if ea_cpi_yoy is not None:
        macro_snapshot_rows.append(["欧元区", "第三层·通胀", "欧元区调和消费物价同比", f"{ea_cpi_yoy:.2f}%", "目标值 2.00%", s_dates.get("CP0000EZ19M086NEST", "-"), "欧央行利率决策核心锚"])

    jp_cpi_yoy = calc_yoy("JPNCPIALLMINMEI")
    if jp_cpi_yoy is not None:
        macro_snapshot_rows.append(["日本", "第三层·通胀", "日本全国消费物价同比", f"{jp_cpi_yoy:.2f}%", "日本央行加息锚", s_dates.get("JPNCPIALLMINMEI", "-"), "支撑日银货币正常化"])

    # ================= B. 跨因子与流动性二阶导数计算 =================
    walcl, tga, rrp = get("WALCL"), get("WDTGAL"), get("RRPONTSYD")
    walcl_1, tga_1, rrp_1 = get("WALCL", 1), get("WDTGAL", 1), get("RRPONTSYD", 1)
    walcl_4, tga_4, rrp_4 = get("WALCL", 4), get("WDTGAL", 4), get("RRPONTSYD", 4)
    walcl_13, tga_13, rrp_13 = get("WALCL", 13), get("WDTGAL", 13), get("RRPONTSYD", 13)

    net_liq, net_liq_chg, net_liq_4w, net_liq_13w = None, 0.0, None, None
    if all(v is not None for v in [walcl, tga, rrp]):
        net_liq = round((walcl - tga) / 1000.0 - rrp, 2)
        if all(v is not None for v in [walcl_1, tga_1, rrp_1]):
            net_liq_chg = round(net_liq - ((walcl_1 - tga_1) / 1000.0 - rrp_1), 2)
        if all(v is not None for v in [walcl_4, tga_4, rrp_4]):
            net_liq_4w = round(net_liq - ((walcl_4 - tga_4) / 1000.0 - rrp_4), 2)
        if all(v is not None for v in [walcl_13, tga_13, rrp_13]):
            net_liq_13w = round(net_liq - ((walcl_13 - tga_13) / 1000.0 - rrp_13), 2)

    y10, y2, effr = get("DGS10"), get("DGS2"), get("DFF")
    y10_5, y2_5 = get("DGS10", 5), get("DGS2", 5)
    implied_rate_cut_bps = round((y2 - effr) * 100, 1) if (y2 is not None and effr is not None) else None

    curve_regime = "数据不足"
    spread_10_2 = None
    if all(v is not None for v in [y10, y2, y10_5, y2_5]):
        spread_10_2 = round(y10 - y2, 3)
        d_spread = round(spread_10_2 - (y10_5 - y2_5), 3)
        dy10, dy2 = y10 - y10_5, y2 - y2_5
        if d_spread > 0 and dy2 < 0 and abs(dy2) > abs(dy10):
            curve_regime = "牛市陡峭化（短端降息预期主导，利好短债与黄金）"
        elif d_spread > 0 and dy10 > 0 and dy10 > dy2:
            curve_regime = "熊市陡峭化（再通胀或美债供给过剩，压制科技股高估值）"
        elif d_spread < 0 and dy10 < 0 and abs(dy10) > abs(dy2):
            curve_regime = "牛市平坦化（避险资金买入长债或长期通胀预期回落）"
        elif d_spread < 0 and dy2 > 0 and dy2 > dy10:
            curve_regime = "熊市平坦化（央行紧缩预期升温，曲线趋平）"
        else:
            curve_regime = "倒挂震荡区间" if spread_10_2 < 0 else "曲线温和震荡"

    tips_0, tips_5 = get("DFII10", 0), get("DFII10", 5)
    us_nom_gdp_yoy = calc_yoy("GDP", lag=4)
    gold_stat = market_stats.get("GOLD", {})
    copper_stat = market_stats.get("COPPER", {})
    brent_stat = market_stats.get("BRENT", {})
    gold_5d = gold_stat.get("pct_5d")
    spx_5d = market_stats.get("SPX", {}).get("pct_5d")
    dxy_5d = market_stats.get("DXY", {}).get("pct_5d")
    hy = get("BAMLH0A0HYM2")
    hy_5 = get("BAMLH0A0HYM2", 5)
    ig = get("BAMLC0A0CM")

    divergence_flags = []
    if tips_0 is not None and tips_5 is not None and gold_5d is not None:
        if (tips_0 - tips_5) > 0.05 and gold_5d > 1.0:
            divergence_flags.append("实际利率与黄金背离：10年期实际利率上行但黄金逆势上涨，表明全球央行去美元化购金主导定价")
    if dxy_5d is not None and y10_5 is not None and y10 is not None:
        if (y10 - y10_5) >= 0.15 and dxy_5d <= -1.2:
            divergence_flags.append("美债与美元背离：10年期美债收益率飙升但美元指数下跌，市场对美国财政可持续性计入信用折价")
    if spx_5d is not None and hy is not None and hy_5 is not None:
        if spx_5d >= 1.5 and (hy - hy_5) >= 0.15:
            divergence_flags.append("股债信用背离：标普500上涨但高收益垃圾债利差走阔，谨防股市补跌")
    if spx_5d is not None and spx_5d <= -3.0:
        divergence_flags.append("高频风险抢跑：股指急跌抢跑低频月度硬数据，谨防拿上月滞后数据误判")

    # ================= C. 36维系统性风险数据库（100%纯中文） =================
    red_alerts, yellow_alerts = [], []

    def add_risk(code, r_type, dim, metric, curr_val, warn_lbl, red_lbl, ratio_raw, mech):
        ratio_pct = round(max(0.0, min(1.5, ratio_raw)), 3)
        if ratio_raw >= 1.0:
            st = "红灯熔断"
            red_alerts.append(f"【{code}·{r_type}】{dim}（当前值：{curr_val}）")
        elif ratio_raw >= 0.65:
            st = "黄灯警戒"
            yellow_alerts.append(f"【{code}·{r_type}】{dim}（当前值：{curr_val}）")
        else:
            st = "绿灯正常"
        risk_rows.append([code, f"{r_type}·{dim}", metric, curr_val, warn_lbl, red_lbl, st, mech, ratio_pct])

    # 18 维急性黑天鹅
    sofr, iorb = extra_vals.get("SOFR"), get("IORB")
    if sofr is not None and iorb is not None:
        sofr_sp = round((sofr - iorb) * 100, 1)
        add_risk("黑天鹅-01", "急性黑天鹅", "美元回购市场管道", "担保隔夜融资利率与准备金利率差（基点）", sofr_sp, "10 基点", "25 基点", sofr_sp / 25.0, "银行间美元准备金突发性短缺与货币市场冻结")

    if hy is not None:
        hy_bps = round(hy * 100, 0)
        add_risk("黑天鹅-02", "急性黑天鹅", "垃圾债信用踩踏", "美国高收益企业债信用利差（基点）", hy_bps, "350 基点", "500 基点", hy_bps / 500.0, "高收益企业再融资通道骤停引发集中违约潮")

    fsi = get("STLFSI4")
    if fsi is not None:
        add_risk("黑天鹅-03", "急性黑天鹅", "综合金融系统压力", "圣路易斯联储金融压力指数", round(fsi, 2), "0.50", "2.00", (fsi + 1.0) / 3.0, "跨市场流动性、利差与波动率系统性共振恶化")

    if all(v is not None for v in [spx_5d, gold_5d, dxy_5d, y10, y10_5]):
        y10_5d_bps = round((y10 - y10_5) * 100, 1)
        summary_str = f"标普:{spx_5d:+.1f}% | 黄金:{gold_5d:+.1f}% | 美债:{y10_5d_bps:+.0f}基点"
        stress_score = max(0.0, (-spx_5d / 4.0) * 0.5 + (-gold_5d / 2.0) * 0.5)
        if spx_5d <= -4.0 and gold_5d <= -2.0 and y10_5d_bps >= 20 and dxy_5d >= 1.5:
            stress_score = 1.15
        add_risk("黑天鹅-04", "急性黑天鹅", "现金为王三杀挤兑", "5日股金债联动状态", summary_str, "股金同跌", "股金暴跌且债率飙升", stress_score, "保证金追缴下机构无差别抛售黄金美债换取美元现金")

    jpy_5d = market_stats.get("USDJPY", {}).get("pct_5d")
    if jpy_5d is not None:
        add_risk("黑天鹅-05", "急性黑天鹅", "日元套息平仓踩踏", "美元兑日元5日涨跌幅（%）", jpy_5d, "-2.5%", "-4.0%", -jpy_5d / 4.0, "日元急速升值迫使全球低息日元融资盘拆仓抛售风险资产")

    vix = market_stats.get("VIX", {}).get("price")
    vix3m = get("VXVCLS")
    if vix is not None and vix3m is not None and vix3m > 0:
        vix_ratio = round(vix / vix3m, 2)
        add_risk("黑天鹅-06", "急性黑天鹅", "波动率期限结构倒挂", f"短期与3个月波动率比值（恐慌指数={vix}）", vix_ratio, "1.00", "1.15", max(vix / 35.0, vix_ratio / 1.15), "短端恐慌情绪大幅超越远期，预示短期尾部黑天鹅爆发")

    btc_auc = extra_vals.get("UST_BID_COVER")
    tail_bps = extra_vals.get("UST_TAIL_BPS", 0.0)
    if btc_auc is not None and btc_auc > 0:
        add_risk("黑天鹅-07", "急性黑天鹅", "美债一级拍卖失灵", f"美债拍卖投标倍数（尾部利差={tail_bps}基点）", btc_auc, "2.25倍", "2.10倍", max(2.10 / btc_auc, tail_bps / 3.0), "一级交易商承接意愿枯竭引发长端美债收益率无序飙升")

    disc_0 = get("WDISBORROW")
    if disc_0 is not None:
        disc_b = round(disc_0 / 1000.0, 2)
        add_risk("黑天鹅-08", "急性黑天鹅", "银行贴现窗口挤兑", "美联储贴现窗口紧急借款（十亿美元）", disc_b, "15.0 十亿美元", "50.0 十亿美元", disc_b / 50.0, "商业银行被迫向美联储紧急求助流动性（类似硅谷银行倒闭前兆）")

    kre_5d = market_stats.get("US_REG_BANK", {}).get("pct_5d")
    if kre_5d is not None:
        add_risk("黑天鹅-09", "急性黑天鹅", "美国区域银行闪崩", "区域银行指数基金5日涨跌（%）", kre_5d, "-5.0%", "-9.0%", max(0.0, -kre_5d / 9.0), "中小银行存款挤兑或商业地产坏账曝光引发银行板块流动性踩踏")

    brent_5d = brent_stat.get("pct_5d")
    if brent_5d is not None:
        add_risk("黑天鹅-10", "急性黑天鹅", "能源供给断供冲击", "布伦特原油5日暴涨幅度（%）", brent_5d, "+6.5%", "+12.0%", max(0.0, brent_5d / 12.0), "地缘战争或航道封锁导致油价脉冲暴涨，触发全球突发滞胀冲击")

    nfci_0 = get("NFCI")
    if nfci_0 is not None:
        add_risk("黑天鹅-11", "急性黑天鹅", "金融条件骤紧熔断", "芝加哥联储金融状况指数", round(nfci_0, 2), "0.00", "+0.50", max(0.0, (nfci_0 + 0.5) / 1.0), "跨市场金融条件由松转紧突破零轴，信用与融资环境骤冷")

    sox_5d = market_stats.get("SOX", {}).get("pct_5d")
    if sox_5d is not None:
        add_risk("黑天鹅-12", "急性黑天鹅", "半导体与算力泡沫去杠杆", "费城半导体指数5日涨跌（%）", sox_5d, "-6.0%", "-10.0%", max(0.0, -sox_5d / 10.0), "人工智能算力资本开支证伪或高拥挤科技量化盘踩踏平仓")

    jp10_0 = get("IRLTLT01JPM156N")
    if jp10_0 is not None:
        add_risk("黑天鹅-13", "急性黑天鹅", "日本国债失控风险", "日本10年期国债收益率（%）", round(jp10_0, 2), "1.20%", "1.60%", max(0.0, jp10_0 / 1.60), "日本长端利率失控迫使日资机构大规模抛售美债欧债回流本土")

    cnh_5d = market_stats.get("USDCNH", {}).get("pct_5d")
    if cnh_5d is not None:
        add_risk("黑天鹅-14", "急性黑天鹅", "离岸人民币急贬冲击", "美元兑离岸人民币5日贬值幅度（%）", cnh_5d, "+1.2%", "+2.2%", max(0.0, cnh_5d / 2.2), "人民币汇率大幅波动外溢至亚洲及新兴市场货币连锁贬值")

    btc_5d = market_stats.get("BTC", {}).get("pct_5d")
    if btc_5d is not None:
        add_risk("黑天鹅-15", "急性黑天鹅", "加密高杠杆连环爆仓", "比特币5日暴跌幅度（%）", btc_5d, "-10.0%", "-18.0%", max(0.0, -btc_5d / 18.0), "美元离岸流动性枯竭率先击穿高杠杆加密市场，预警科技股风险")

    copper_5d = copper_stat.get("pct_5d")
    if copper_5d is not None and gold_5d is not None:
        cg_5d_diff = round(copper_5d - gold_5d, 2)
        add_risk("黑天鹅-16", "急性黑天鹅", "铜金比断崖硬着陆脉冲", "精炼铜与黄金5日相对剪刀差（%）", cg_5d_diff, "-4.5%", "-8.0%", max(0.0, -cg_5d_diff / 8.0), "铜价暴跌而黄金避险上涨，市场急速定价全球实体经济硬着陆")

    if y10 is not None and y10_5 is not None:
        y10_jump_bps = round((y10 - y10_5) * 100, 1)
        add_risk("黑天鹅-17", "急性黑天鹅", "长端美债单周剧烈抛售", "美国10年期国债5日跳升幅度（基点）", y10_jump_bps, "+20 基点", "+35 基点", max(0.0, y10_jump_bps / 35.0), "无风险利率单周暴涨引爆商业银行债券账面巨亏与养老金追保")

    wheat_5d = market_stats.get("WHEAT", {}).get("pct_5d")
    if wheat_5d is not None:
        add_risk("黑天鹅-18", "急性黑天鹅", "全球粮食供应链危机", "芝加哥小麦5日暴涨幅度（%）", wheat_5d, "+8.0%", "+15.0%", max(0.0, wheat_5d / 15.0), "极端气候或地缘冲突阻断粮道，引爆新兴市场恶性食品通胀")

    # 18 维慢性灰犀牛
    if interest_rev_ratio is not None:
        add_risk("灰犀牛-01", "慢性灰犀牛", "美国财政利息吞噬率", "联邦利息支出占财政收入比（%）", interest_rev_ratio, "16.0%", "20.0%", interest_rev_ratio / 20.0, "利息支出挤压常规财政预算，倒逼赤字货币化与财政主导")

    if y10 is not None and us_nom_gdp_yoy is not None:
        r_minus_g = round(y10 - us_nom_gdp_yoy, 2)
        add_risk("灰犀牛-02", "慢性灰犀牛", "美国主权债务雪球效应", "10年期美债利率减名义经济增速（%）", r_minus_g, "+0.5%", "+1.5%", (r_minus_g + 1.5) / 3.0, "借贷利率持续高于名义经济增长，债务率进入内生发散通道")

    cre_delinq = get("DRCRELEXFACBS")
    if cre_delinq is not None:
        add_risk("灰犀牛-03", "慢性灰犀牛", "美国商业地产贷款违约", "商业银行商业地产贷款违约率（%）", round(cre_delinq, 2), "1.50%", "2.50%", cre_delinq / 2.50, "高利率下写字楼与商业地产再融资坏账侵蚀美国区域银行资本金")

    cc_delinq = get("DRCCLACBS")
    psav = get("PSAVERT")
    if cc_delinq is not None:
        lbl_val = f"违约{cc_delinq:.2f}% / 储蓄{psav:.1f}%" if psav is not None else f"{cc_delinq:.2f}%"
        add_risk("灰犀牛-04", "慢性灰犀牛", "美国居民消费信用恶化", "信用卡违约率与储蓄率", lbl_val, "违约 3.0%", "违约 4.0%", cc_delinq / 4.0, "超额储蓄耗尽后中低收入群体信用卡违约攀升拖累消费引擎")

    dep_0, dep_13 = get("DPSACBW027SBOG", 0), get("DPSACBW027SBOG", 13)
    if dep_0 is not None and dep_13 is not None and dep_13 > 0:
        dep_q_chg = round((dep_0 / dep_13 - 1) * 100, 2)
        add_risk("灰犀牛-05", "慢性灰犀牛", "商业银行存款持续流失", "美国商业银行总存款季度变动（%）", dep_q_chg, "-1.0%", "-3.0%", max(0.0, -dep_q_chg / 3.0), "高息货币基金持续虹吸银行低息存款，削弱商业银行放贷能力")

    cn10 = get("CN_DGS10")
    if y10 is not None and cn10 is not None:
        us_cn_sp = round(y10 - cn10, 2)
        add_risk("灰犀牛-06", "慢性灰犀牛", "中美长端负利差倒挂", "美国10年期减中国10年期国债利差（%）", us_cn_sp, "2.00%", "3.00%", us_cn_sp / 3.00, "深度负利差持续压制人民币汇率弹性与境外资金配置意愿")

    if cn_scissors is not None:
        add_risk("灰犀牛-07", "慢性灰犀牛", "中国货币活化与通缩压力", "中国狭义与广义货币增速剪刀差（%）", cn_scissors, "-3.5%", "-6.0%", max(0.0, -cn_scissors / 6.0), "企业与居民预防性储蓄偏高、实体投融资需求待实质性修复")

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
        add_risk("灰犀牛-08", "慢性灰犀牛", f"新兴市场外债脆弱性（{country_zh_map.get(worst_iso, worst_iso)}）", "短期外债占官方外汇储备比（%）", worst_val, "65.0%", "100.0%", worst_val / 100.0, "外储覆盖率不足1倍，强美元周期下易爆发主权债务重组危机")

    sloos_0 = get("DRTSCILM")
    if sloos_0 is not None:
        add_risk("灰犀牛-09", "慢性灰犀牛", "银行全面收紧信贷标准", "美联储调查收紧工商业贷款比例（%）", round(sloos_0, 1), "35.0%", "55.0%", max(0.0, sloos_0 / 55.0), "商业银行大幅收紧信贷标准，实体企业信用创造渠道面临慢性冻结")

    tdsp_0 = get("TDSP")
    if tdsp_0 is not None:
        add_risk("灰犀牛-10", "慢性灰犀牛", "美国家庭债务偿付负担", "家庭偿债支出占可支配收入比（%）", round(tdsp_0, 2), "10.5%", "12.0%", max(0.0, (tdsp_0 - 8.0) / 4.0), "高利率向存量房贷与消费贷传导，持续挤压居民可选消费现金流")

    if rrp is not None:
        rrp_depletion_ratio = max(0.0, (400.0 - rrp) / 350.0)
        add_risk("灰犀牛-11", "慢性灰犀牛", "隔夜逆回购缓冲垫耗尽", "美联储隔夜逆回购余额（十亿美元）", round(rrp, 1), "150 十亿美元", "50 十亿美元", rrp_depletion_ratio, "逆回购蓄水池见底后，美联储缩表与发债将直接抽干商业银行准备金")

    if tips_0 is not None:
        add_risk("灰犀牛-12", "慢性灰犀牛", "实际利率长期高压窒息", "美国10年期通胀保值债券实际利率（%）", round(tips_0, 2), "1.80%", "2.35%", max(0.0, tips_0 / 2.35), "高实际无风险利率持续压制实体企业资本回报率与高估值资产")

    if hy is not None and ig is not None:
        hy_ig_gap = round((hy - ig) * 100, 0)
        add_risk("灰犀牛-13", "慢性灰犀牛", "企业债信用分层恶化", "垃圾债与投资级债利差剪刀差（基点）", hy_ig_gap, "260 基点", "380 基点", max(0.0, hy_ig_gap / 380.0), "低评级僵尸企业借新还旧成本失控，违约潮向中游企业蔓延")

    mtg_delinq = get("DRSFRMACBS")
    if mtg_delinq is not None:
        add_risk("灰犀牛-14", "慢性灰犀牛", "居民住房抵押贷款违约", "商业银行居民房贷违约率（%）", round(mtg_delinq, 2), "2.20%", "3.20%", max(0.0, mtg_delinq / 3.20), "失业率上升叠加高房贷利率开始击穿美国家庭核心资产负债表")

    if psav is not None:
        sav_stress = max(0.0, (6.5 - psav) / 3.5)
        add_risk("灰犀牛-15", "慢性灰犀牛", "美国居民超额储蓄枯竭", "个人储蓄率占可支配收入比（%）", round(psav, 1), "4.2%", "3.0%", sav_stress, "居民消费高度依赖信用卡透支，一旦就业降温消费将断崖失速")

    bus_yoy = calc_yoy("BUSLOANS", lag=12)
    if bus_yoy is not None:
        bus_stress = max(0.0, (3.0 - bus_yoy) / 4.0)
        add_risk("灰犀牛-16", "慢性灰犀牛", "美国工商业信贷萎缩", "商业银行工商业贷款余额同比（%）", bus_yoy, "+0.5%", "-1.0%", bus_stress, "企业停止主动加杠杆扩产，经济内生信用派生引擎熄火")

    add_risk("灰犀牛-17", "慢性灰犀牛", "萨姆规则衰退临界逼近", "官方萨姆规则实时衰退指标（%）", round(sahm, 2), "0.35%", "0.50%", max(0.0, sahm / 0.50), "失业率3个月均值较12个月低点上升超0.50%触发历史100%准度衰退铁律")

    spy_pe_val = get("SPX_PE")
    if spy_pe_val is not None:
        add_risk("灰犀牛-18", "慢性灰犀牛", "美股估值泡沫与负风险溢价", "标普500代理市盈率（倍）", spy_pe_val, "24.0 倍", "28.0 倍", max(0.0, (spy_pe_val - 16.0) / 12.0), "美股盈利收益率低于10年期美债无风险利率，估值安全边际极度脆弱")

    # ================= D. 多国横向对比表（100%纯中文） =================
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

    # ================= E. 生成全因子纯中文解说报告 =================
    tips10 = tips_0 or 0.0
    growth_weak = (sahm >= 0.5) or (nfp_add_10k is not None and nfp_add_10k < 8.0)
    infl_high = (pce_3m_ann is not None and pce_3m_ann > 2.5) or (us_cpi_yoy is not None and us_cpi_yoy > 2.8)

    if not growth_weak and not infl_high:
        quadrant = "象限一：温和复苏与金发姑娘周期（经济增长稳健且通胀受控）"
        regime_logic = "当前处于增长主导机制，市场重点交易分子端企业盈利修复，占优资产为成长与周期股票及企业债券。"
    elif not growth_weak and infl_high:
        quadrant = "象限二：经济过热与货币紧缩周期（增长具备韧性但通胀黏性较高）"
        regime_logic = "当前切换至利率与通胀主导机制，超预期经济数据易强化央行紧缩预期并抬高分母端贴现率，占优资产为大宗商品与低估值价值股，需规避长久期国债。"
    elif growth_weak and infl_high:
        quadrant = "象限三：滞胀困境周期（经济增长下滑且通胀居高不下）"
        regime_logic = "警惕薪资与物价螺旋上升引发政策两难，央行陷入保增长与抗通胀矛盾，占优资产为现金、短期国债与实物黄金，严格规避高估值科技股与长债。"
    else:
        quadrant = "象限四：衰退防御周期（经济增长走弱且通胀回落）"
        regime_logic = "总需求收缩主导市场定价，央行进入被动降息通道，占优资产为长久期国债，需高度警惕信用利差走阔引发的信用紧缩。"

    if red_alerts:
        probs = "基准情景（风险加速释放）：45% ｜ 乐观情景（央行紧急转向救市）：15% ｜ 悲观情景（尾部危机跨市场扩散）：40%"
    elif yellow_alerts:
        probs = "基准情景（高位震荡消化）：55% ｜ 乐观情景（经济软着陆延续）：20% ｜ 悲观情景（流动性紧缩与波动升级）：25%"
    else:
        probs = "基准情景（经济软着陆与板块轮动）：60% ｜ 乐观情景（全球宽信用共振）：25% ｜ 悲观情景（二次通胀或地缘冲击）：15%"

    spx_p = market_stats.get("SPX", {}).get("price", "-")
    csi_p = market_stats.get("CSI300", {}).get("price", "-")
    dxy_p = market_stats.get("DXY", {}).get("price", "-")
    jpy_p = market_stats.get("USDJPY", {}).get("price", "-")
    au_p = market_stats.get("GOLD", {}).get("price", "-")
    br_p = market_stats.get("BRENT", {}).get("price", "-")
    btc_p = market_stats.get("BTC", {}).get("price", "-")

    # 内置全因子深度中文兜底解说（确保任何情况下都有完整的六层全因子解说，绝不留白）
    fallback_full_text = (
        f"【全因子宏观第一性原理深度解说报告 | 更新时间：{NOW_STR}】\n\n"
        f"一、【宏观周期象限与新闻催化主线】\n"
        f"当前全球宏观定位于【{quadrant}】。{regime_logic} 结合今日六大板块财经头条，市场核心博弈焦点集中在央行利率路径预期差与财政债务供给压力之间。"
        f"{' 监测到跨资产背离信号：' + '；'.join(divergence_flags) + '。' if divergence_flags else ' 当前高频资产价格与低频经济硬数据整体保持同向共振，暂未出现极端抢跑背离。'}\n\n"
        f"二、【第一、二层：信用派生与美元净流动性水管解说】\n"
        f"1. 美元净流动性总阀门当前水位为 {net_liq} 十亿美元，近1周变动 {net_liq_chg:+} 十亿美元，近4周累计变动 {net_liq_4w} 十亿美元，近13周趋势变动 {net_liq_13w} 十亿美元。隔夜逆回购蓄水池剩余 {rrp} 十亿美元，商业银行总信贷同比增速为 {us_credit_yoy if us_credit_yoy is not None else '暂无'}%。\n"
        f"2. 美债收益率曲线当前处于【{curve_regime}】，10年期与2年期美债利差为 {spread_10_2}%，市场隐含未来降息幅度定价为 {implied_rate_cut_bps} 个基点。\n\n"
        f"三、【第三、四层：美欧日中硬数据与财政债务生死线解说】\n"
        f"1. 美国实体与财政：最新单月新增非农就业 {nfp_add_10k if nfp_add_10k is not None else '暂无'} 万人，失业率 {unrate_0 if unrate_0 is not None else '暂无'}%，萨姆规则衰退指标读数 {sahm}%；总体消费物价同比 {us_cpi_yoy if us_cpi_yoy is not None else '暂无'}%，核心个人消费支出3个月折年率 {pce_3m_ann if pce_3m_ann is not None else '暂无'}%。美国联邦政府年化利息支出占财政收入比重已达 {interest_rev_ratio if interest_rev_ratio is not None else '暂无'}%，持续挤压常规财政空间。\n"
        f"2. 中国与欧日周期：中国官方制造业采购经理指数为 {cn_pmi if cn_pmi is not None else '暂无'}，狭义与广义货币同比剪刀差为 {cn_scissors if cn_scissors is not None else '暂无'}%，居民消费物价与工业出厂价格分别为 {cn_cpi}% 与 {cn_ppi}%；欧元区调和通胀率为 {ea_cpi_yoy if ea_cpi_yoy is not None else '暂无'}%，日本全国通胀率为 {jp_cpi_yoy if jp_cpi_yoy is not None else '暂无'}%。\n\n"
        f"四、【第五层：全球跨资产定价锚点解说】\n"
        f"美国10年期通胀保值债券实际利率报 {tips10}%，标普500指数报 {spx_p} 点，沪深300映射报 {csi_p}，美元指数报 {dxy_p}，美元兑日元报 {jpy_p}，纽约黄金期货报 {au_p} 美元/盎司，布伦特原油报 {br_p} 美元/桶，比特币报 {btc_p} 美元。黄金在当前实际利率水平下的表现直接反映全球央行储备多元化与主权信用对冲需求。\n\n"
        f"五、【第六层：36维黑天鹅与灰犀牛风险雷达排雷】\n"
        f"美国商业地产银行贷款违约率为 {cre_delinq}%，美联储调查银行收紧贷款比例为 {sloos_0}%，贴现窗口紧急借款规模为 {round(disc_0/1000, 2) if disc_0 else '暂无'} 十亿美元。"
        f"{' 当前触发预警因子包括：' + '；'.join(red_alerts + yellow_alerts) + '，需严防局部流动性收紧通过跨市场保证金机制向权益资产传染。' if (red_alerts or yellow_alerts) else ' 当前36个急性黑天鹅与慢性灰犀牛维度全部处于绿灯常态安全区间，系统性熔断概率可控。'}\n\n"
        f"六、【三情景概率分布与未来十日量化证伪红线】\n"
        f"概率分布：{probs}。\n"
        f"强制量化证伪条件：第一，美国10年期实际利率有效突破 {'1.80%' if tips10 > 2.0 else '2.25%'}；第二，美国高收益垃圾债信用利差突破 500 个基点或美股恐慌指数突破 30；第三，美元净流动性连续两周单向收缩超过 150 十亿美元。"
    )

    # 组装纯中文结构化 JSON 喂给 GPT-6.2 Astra
    sorted_risk_summary = sorted(
        [{"编号": r[0], "风险维度": r[1], "当前值": r[3], "状态": r[6], "距红线进度百分比": round(r[8] * 100, 1), "传导机制": r[7]} for r in risk_rows],
        key=lambda x: x["距红线进度百分比"],
        reverse=True
    )

    llm_ctx = {
        "分析日期": TODAY_STR,
        "当前宏观周期象限": quadrant,
        "第一二层_信用与美元净流动性": {
            "美元净流动性十亿美元": net_liq,
            "净流动性1周变动": net_liq_chg,
            "净流动性4周变动": net_liq_4w,
            "净流动性13周变动": net_liq_13w,
            "隔夜逆回购余额十亿美元": rrp,
            "财政部一般账户十亿美元": round(tga / 1000, 1) if tga else None,
            "美国商业银行总信贷同比百分比": us_credit_yoy,
            "美国10年期实际利率百分比": tips10,
            "美债10年减2年利差百分比": spread_10_2,
            "美债收益率曲线形态": curve_regime,
            "市场隐含降息幅度基点": implied_rate_cut_bps,
        },
        "第三四层_全球经济硬数据与财政债务": {
            "美国单月新增非农万人": nfp_add_10k,
            "美国失业率百分比": unrate_0,
            "萨姆规则衰退指标百分比": sahm,
            "美国总体消费物价同比百分比": us_cpi_yoy,
            "美国核心个人消费支出3月折年率百分比": pce_3m_ann,
            "美国联邦利息占财政收入百分比": interest_rev_ratio,
            "美国名义经济同比增速百分比": us_nom_gdp_yoy,
            "中国制造业采购经理指数": cn_pmi,
            "中国狭义广义货币剪刀差百分比": cn_scissors,
            "中国消费物价同比百分比": cn_cpi,
            "中国工业出厂价格同比百分比": cn_ppi,
            "欧元区通胀同比百分比": ea_cpi_yoy,
            "日本通胀同比百分比": jp_cpi_yoy,
        },
        "第五层_跨资产最新定价与背离警告": {
            "标普500": market_stats.get("SPX"),
            "纳斯达克100": market_stats.get("NDX"),
            "费城半导体": market_stats.get("SOX"),
            "沪深300映射": market_stats.get("CSI300"),
            "美国区域银行基金": market_stats.get("US_REG_BANK"),
            "高收益垃圾债基金": market_stats.get("US_HY_ETF"),
            "美元指数": market_stats.get("DXY"),
            "美元兑日元": market_stats.get("USDJPY"),
            "美元兑离岸人民币": market_stats.get("USDCNH"),
            "纽约黄金": market_stats.get("GOLD"),
            "布伦特原油": market_stats.get("BRENT"),
            "纽约精炼铜": market_stats.get("COPPER"),
            "美股恐慌指数": market_stats.get("VIX"),
            "比特币": market_stats.get("BTC"),
            "系统检测到的背离信号": divergence_flags,
        },
        "第六层_36维黑天鹅与灰犀牛最危险前12项": sorted_risk_summary[:12],
        "今日六大板块核心财经新闻": [f"【{n['sector']}】{n['title']}" for n in news_list[:16]],
    }

    report_text = generate_ai_full_factor_commentary(llm_ctx, fallback_full_text)
    return risk_rows, country_rows, macro_snapshot_rows, report_text


# ================= 8. 工作表写入 + 新闻增量留存 + 8张带中文解说高清结构图生成 =================
def get_or_create_ws(sh, title, rows=300, cols=26):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def build_google_sheets_charts_requests(dash_sheet_id, ts_sheet_id, num_ts_rows):
    """生成 8 张带中文核心解说副标题的原生折线图，4行×2列精准嵌入驾驶舱下方（第52行起）"""
    # 列索引严格对应 CHART_SERIES_COLUMNS
    # 0:日期, 1:美元净流动性, 2:标普500, 3:美债10Y-2Y利差, 4:联邦基金利率, 5:10Y TIPS实际利率,
    # 6:COMEX黄金, 7:美债10Y, 8:中债10Y, 9:中美10Y利差, 10:铜金比, 11:布伦特原油,
    # 12:高收益债利差, 13:VIX恐慌指数, 14:美商业银行总信贷, 15:区域银行ETF, 16:比特币, 17:纳斯达克100
    chart_specs = [
        {
            "title": "结构图一：美元净流动性（十亿美元）与标普500走势",
            "subtitle": "【观测逻辑与触发解说】净流动性 = 美联储总资产 - 财政部账户 - 隔夜逆回购。当净流动性曲线拐头向下而标普500仍惯性冲高时，构成典型流动性背离，预示美股分母端承压回调。",
            "left_col": 1, "right_col": 2, "anchor_row": 52, "anchor_col": 0
        },
        {
            "title": "结构图二：美债10年-2年期限利差（%）与联邦基金利率周期",
            "subtitle": "【观测逻辑与触发解说】深度倒挂期间经济往往具备韧性；真正触发历史级衰退与股市主跌浪的时刻，是倒挂解除、利差由负转正且伴随降息周期开启的陡峭化阶段。",
            "left_col": 3, "right_col": 4, "anchor_row": 52, "anchor_col": 8
        },
        {
            "title": "结构图三：美国10年期实际利率（%）与国际黄金价格背离",
            "subtitle": "【观测逻辑与触发解说】传统框架下实际利率（左轴）与黄金（右轴）严格负相关。若出现实际利率走高而黄金同步创新高，说明主权信用折价与全球央行购金已取代实际利率成为第一定价锚。",
            "left_col": 5, "right_col": 6, "anchor_row": 72, "anchor_col": 0
        },
        {
            "title": "结构图四：中美10年期国债收益率与中美利差（%）",
            "subtitle": "【观测逻辑与触发解说】中美10年期国债利差决定跨境套息资金流向。当负利差倒挂突破 2.5% 警戒线时，离岸人民币汇率弹性与外资风险偏好将面临显著压制。",
            "left_col": 7, "right_col": 8, "anchor_row": 72, "anchor_col": 8
        },
        {
            "title": "结构图五：铜金比（实体需求）与布伦特原油（供给通胀）",
            "subtitle": "【观测逻辑与触发解说】铜金比衡量全球实体制造业扩张动能，原油衡量供给端通胀压力。若铜金比持续下行而原油脉冲飙升，即确认进入最恶劣的宏观滞胀象限。",
            "left_col": 10, "right_col": 11, "anchor_row": 92, "anchor_col": 0
        },
        {
            "title": "结构图六：高收益垃圾债信用利差（%）与美股恐慌指数",
            "subtitle": "【观测逻辑与触发解说】信用利差是实体企业违约的领先指标。若恐慌指数处于低位但高收益债利差提前走阔突破 4.0%，说明聪明资金已开始撤离低资质资产。",
            "left_col": 12, "right_col": 13, "anchor_row": 92, "anchor_col": 8
        },
        {
            "title": "结构图七：美国商业银行总信贷与区域银行指数基金",
            "subtitle": "【观测逻辑与触发解说】区域银行深度绑定美国商业地产贷款。若区域银行指数大幅下挫并拖累商业银行总信贷增速转负，表明商业地产灰犀牛正在转化为信贷紧缩。",
            "left_col": 14, "right_col": 15, "anchor_row": 112, "anchor_col": 0
        },
        {
            "title": "结构图八：比特币现货（美元）与纳斯达克100指数",
            "subtitle": "【观测逻辑与触发解说】比特币对离岸美元流动性最为敏感，通常领先纳斯达克100指数 1 至 3 周反映流动性边际拐点，是科技成长股的风险情绪前哨站。",
            "left_col": 16, "right_col": 17, "anchor_row": 112, "anchor_col": 8
        },
    ]

    requests_list = []
    for spec in chart_specs:
        req = {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": spec["title"],
                        "subtitle": spec["subtitle"],
                        "titleTextFormat": {"bold": True, "fontSize": 11},
                        "subtitleTextFormat": {"fontSize": 9, "italic": False},
                        "basicChart": {
                            "chartType": "LINE",
                            "legendPosition": "TOP_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "观测日期（近三年周频）"},
                                {"position": "LEFT_AXIS", "title": CHART_SERIES_COLUMNS[spec["left_col"]][0]},
                                {"position": "RIGHT_AXIS", "title": CHART_SERIES_COLUMNS[spec["right_col"]][0]},
                            ],
                            "domains": [
                                {
                                    "domain": {
                                        "sourceRange": {
                                            "sources": [{
                                                "sheetId": ts_sheet_id,
                                                "startRowIndex": 0,
                                                "endRowIndex": num_ts_rows,
                                                "startColumnIndex": 0,
                                                "endColumnIndex": 1,
                                            }]
                                        }
                                    }
                                }
                            ],
                            "series": [
                                {
                                    "series": {
                                        "sourceRange": {
                                            "sources": [{
                                                "sheetId": ts_sheet_id,
                                                "startRowIndex": 0,
                                                "endRowIndex": num_ts_rows,
                                                "startColumnIndex": spec["left_col"],
                                                "endColumnIndex": spec["left_col"] + 1,
                                            }]
                                        }
                                    },
                                    "targetAxis": "LEFT_AXIS",
                                },
                                {
                                    "series": {
                                        "sourceRange": {
                                            "sources": [{
                                                "sheetId": ts_sheet_id,
                                                "startRowIndex": 0,
                                                "endRowIndex": num_ts_rows,
                                                "startColumnIndex": spec["right_col"],
                                                "endColumnIndex": spec["right_col"] + 1,
                                            }]
                                        }
                                    },
                                    "targetAxis": "RIGHT_AXIS",
                                },
                            ],
                            "headerCount": 1,
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": dash_sheet_id,
                                "rowIndex": spec["anchor_row"],
                                "columnIndex": spec["anchor_col"],
                            },
                            "widthPixels": 620,
                            "heightPixels": 370,
                        }
                    },
                }
            }
        }
        requests_list.append(req)
    return requests_list


def sync_all_sheets(cross_asset_items, risk_rows, country_rows, macro_rows, ts_matrix, news_list, report_text):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_dash = get_or_create_ws(sh, TAB_DASH, rows=160, cols=18)
    ws_risk_news = get_or_create_ws(sh, TAB_RISK_NEWS, rows=1000, cols=12)
    ws_ts_calc = get_or_create_ws(sh, TAB_TS_CALC, rows=300, cols=30)

    # 删除所有旧版多余工作表，保持表格高度精简
    for old_name in OLD_TABS_TO_REMOVE:
        try:
            sh.del_worksheet(sh.worksheet(old_name))
        except Exception:
            pass

    # ================= 1. 写入 02_底层时序与衍生运算库 =================
    if ts_matrix and len(ts_matrix) > 1:
        ws_ts_calc.clear()
        ws_ts_calc.update(range_name="A1", values=ts_matrix, value_input_option="USER_ENTERED")

    # ================= 2. 写入 01_风险雷达与多板块新闻库（支持新闻历史留存去重） =================
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
    merged_news = merged_news[:300]  # 留存最近 300 条跨板块重要新闻

    sorted_risks = sorted(risk_rows, key=lambda x: x[8], reverse=True)
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
    rn_matrix.append(["三、全球多板块重要财经新闻历史留存库（自动去重增量归档）"])
    rn_matrix.append(["收录日期", "所属宏观板块", "信息来源", "中文新闻核心摘要"])
    for n in merged_news:
        rn_matrix.append([n["date"], n["sector"], n["source"], n["title"]])

    ws_risk_news.clear()
    ws_risk_news.update(range_name="A1", values=rn_matrix, value_input_option="USER_ENTERED")

    # ================= 3. 构建 00_宏观驾驶舱_总览（左右双栏网格排版 + 8张原生高清结构图） =================
    # 先清理驾驶舱已有图表，防止重复叠加
    try:
        meta_info = sh.fetch_sheet_metadata()
        del_chart_reqs = []
        for s in meta_info.get("sheets", []):
            if s["properties"]["sheetId"] == ws_dash.id:
                for ch in s.get("charts", []):
                    del_chart_reqs.append({"deleteChart": {"chartId": ch["chartId"]}})
        if del_chart_reqs:
            sh.batch_update({"requests": del_chart_reqs})
    except Exception as e:
        print(f"[INFO] 清理旧图表跳过: {e}")

    dash_grid = [["" for _ in range(16)] for _ in range(52)]

    # 顶部标题与 AI 全因子深度解说区域（第 1~15 行）
    dash_grid[0][0] = "全球宏观第一性原理自动驾驶舱（全中文直观决策版）"
    dash_grid[0][12] = f"数据同步时间：{NOW_STR}"
    dash_grid[1][0] = "【智能深度全因子解说报告（覆盖第一至第六层全量指标与六大板块新闻催化）】"
    dash_grid[2][0] = report_text

    # 左栏（A~G列，第17~49行）：直观核心经济硬数据 + 跨资产实时行情
    dash_grid[16][0] = "一、全球主要经济体核心硬数据直观快照"
    dash_grid[17][0:7] = ["国家/地区", "宏观层级", "核心经济指标", "最新读数", "对比基准", "数据日期", "周期信号"]
    curr_left_r = 18
    for m_row in macro_rows[:12]:
        dash_grid[curr_left_r][0:7] = m_row
        curr_left_r += 1

    curr_left_r += 1
    dash_grid[curr_left_r][0] = "二、全球跨资产实时定价直观看板"
    curr_left_r += 1
    dash_grid[curr_left_r][0:7] = ["资产类别", "资产名称", "最新价格", "单日涨跌", "5日涨跌", "20日涨跌", "第一性原理定价驱动"]
    curr_left_r += 1
    for c_item in cross_asset_items[:18]:
        if curr_left_r >= 50:
            break
        dash_grid[curr_left_r][0:7] = [
            c_item["cat"], c_item["name"], c_item["price"],
            c_item["pct_1d"], c_item["pct_5d"], c_item["pct_20d"], c_item["driver"]
        ]
        curr_left_r += 1

    # 右栏（I~O列，第17~49行）：最危险 Top 15 黑天鹅/灰犀牛预警雷达 + 今日多板块新闻速览
    dash_grid[16][8] = "三、高危黑天鹅与灰犀牛预警雷达（按距红线危急度排序）"
    dash_grid[17][8:15] = ["风险编号", "风险维度", "当前读数", "红灯熔断线", "预警状态", "距红线进度", "核心传导机制"]
    curr_right_r = 18
    for r in sorted_risks[:16]:
        dash_grid[curr_right_r][8:15] = [
            r[0], r[1], r[3], r[5], r[6], f"{round(r[8]*100, 1)}%", r[7]
        ]
        curr_right_r += 1

    curr_right_r += 1
    dash_grid[curr_right_r][8] = "四、今日全球六大板块核心财经新闻速览"
    curr_right_r += 1
    dash_grid[curr_right_r][8:15] = ["所属板块", "信息来源", "中文核心新闻标题", "", "", "", ""]
    curr_right_r += 1
    for n in news_list[:11]:
        if curr_right_r >= 50:
            break
        dash_grid[curr_right_r][8] = n["sector"]
        dash_grid[curr_right_r][9] = n["source"]
        dash_grid[curr_right_r][10] = n["title"]
        curr_right_r += 1

    dash_grid[50][0] = "五、全球宏观第一性原理八大核心结构图（每张图表均附带中文观测逻辑与触发解说）"

    ws_dash.clear()
    ws_dash.update(range_name="A1", values=dash_grid, value_input_option="USER_ENTERED")

    # 合并 AI 解说报告单元格并设置自动换行，提升阅读美感
    format_requests = [
        {
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 2, "endRowIndex": 15, "startColumnIndex": 0, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 2, "endRowIndex": 15, "startColumnIndex": 0, "endColumnIndex": 15},
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

    # 生成 8 张原生高清结构图
    if ts_matrix and len(ts_matrix) > 2:
        chart_reqs = build_google_sheets_charts_requests(ws_dash.id, ws_ts_calc.id, len(ts_matrix))
        format_requests.extend(chart_reqs)

    sh.batch_update({"requests": format_requests})


if __name__ == "__main__":
    print(f"[{NOW_STR}] 启动全球宏观全中文自动驾驶舱同步...")
    s_obs, s_dates, lt_hist = fetch_fred_data()
    cross_items, mkt_stats = fetch_market_assets(s_obs, lt_hist)
    fetch_china_macro_akshare(s_obs, s_dates, lt_hist)
    wb_m, imf_m, ext_v = fetch_external_apis(s_obs)
    news = fetch_global_macro_news()

    risk_r, country_r, macro_r, report = analyze_all_factors(
        s_obs, s_dates, wb_m, imf_m, ext_v, mkt_stats, news
    )
    ts_mat = build_clean_chart_timeseries(lt_hist)

    sync_all_sheets(cross_items, risk_r, country_r, macro_r, ts_mat, news, report)
    print(f"[{NOW_STR}] 同步与图表生成全部完成！共扫描 {len(risk_r)} 维风险，生成 8 张带中文解说结构图。")
