import os
import re
import json
import time
import csv
import io
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

# 三级 AI 引擎配置（主中转站 -> OpenRouter -> Google Gemini）
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.starwish.fit/v1").strip().rstrip("/")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-6.2-astra").strip()
OPENAI_REASONING_EFFORT = os.environ.get("OPENAI_REASONING_EFFORT", "max").strip()
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "google/gemini-2.5-pro").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip()

TZ_CST = timezone(timedelta(hours=8))
NOW_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d %H:%M:%S")
TODAY_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d")

# 精简后的 3 张核心工作表
TAB_DASH = "00_宏观驾驶舱_总览"
TAB_RISK_NEWS = "01_风险雷达与多板块新闻库"
TAB_TS_CALC = "02_底层时序与衍生运算库"

# 彻底清理历史遗留与僵尸表（含 01_原始指标库_Raw）
OLD_TABS_TO_REMOVE = [
    "01_原始指标库_Raw",
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
    {"id": "M1SL", "country": "美国", "layer": "L1·信用引擎", "freq": "M", "lt": True, "name": "美国狭义货币M1存量（十亿美元）"},
    {"id": "M2SL", "country": "美国", "layer": "L1·信用引擎", "freq": "M", "lt": True, "name": "美国广义货币M2存量（十亿美元）"},
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
    {"id": "SAHMREALTIME", "country": "美国", "layer": "L3·就业硬数据", "freq": "M", "lt": True, "name": "官方萨姆规则衰退指标（%）"},
    {"id": "CES0500000003", "country": "美国", "layer": "L3·就业硬数据", "freq": "M", "lt": False, "name": "美国私营部门平均时薪（美元/小时）"},
    {"id": "INDPRO", "country": "美国", "layer": "L3·工业与库存", "freq": "M", "lt": True, "name": "美国工业生产总指数"},
    {"id": "DGORDER", "country": "美国", "layer": "L3·工业与库存", "freq": "M", "lt": True, "name": "美国耐用品新订单总额（百万美元）"},
    {"id": "ISRATIO", "country": "美国", "layer": "L3·工业与库存", "freq": "M", "lt": True, "name": "美国商业库存与销售额比率"},
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
    {"cat": "核心股指", "ticker": "000300.SS", "fallback_ticker": "ASHR", "fallback_label": "（美股ETF映射·ASHR）", "code": "CSI300", "lt": True, "flow_chart": True, "short_name": "中国沪深300", "name": "中国沪深300指数", "driver": "中国社融信贷脉冲与货币活化剪刀差"},
    {"cat": "核心股指", "ticker": "^HSI", "code": "HSI", "lt": False, "flow_chart": True, "short_name": "香港恒生指数", "name": "中国香港恒生指数", "driver": "中国经济基本面分子 × 美元流动性分母双重定价"},
    {"cat": "核心股指", "ticker": "^N225", "code": "N225", "lt": False, "flow_chart": True, "short_name": "日本日经225", "name": "日本日经225指数", "driver": "日元汇率波动与日本央行货币政策转向预期"},
    {"cat": "核心股指", "ticker": "^GDAXI", "code": "DAX", "lt": False, "flow_chart": True, "short_name": "德国DAX指数", "name": "德国法兰克福DAX指数", "driver": "欧元区制造业周期与欧央行降息路径"},
    {"cat": "风险温度计", "ticker": "KRE", "code": "US_REG_BANK", "lt": True, "flow_chart": True, "short_name": "美国区域银行", "name": "美国区域银行指数基金", "driver": "美国中小银行商业地产敞口与存款稳定性温度计"},
    {"cat": "风险温度计", "ticker": "HYG", "code": "US_HY_ETF", "lt": False, "flow_chart": True, "short_name": "高收益信用债", "name": "美国高收益企业债基金", "driver": "垃圾债信用利差与企业再融资流动性实时定价"},
    {"cat": "风险温度计", "ticker": "TLT", "code": "US_20Y_BOND", "lt": False, "flow_chart": True, "short_name": "美债20年+长债", "name": "美国20年以上长期国债基金", "driver": "长端美债久期风险与美国主权信用折价锚"},
    {"cat": "全球外汇", "ticker": "DX-Y.NYB", "code": "DXY", "lt": True, "flow_chart": True, "short_name": "美元指数", "name": "美元指数", "driver": "全球离岸美元流动性松紧总阀门"},
    {"cat": "全球外汇", "ticker": "JPY=X", "code": "USDJPY", "lt": True, "flow_chart": False, "short_name": "美元兑日元", "name": "美元兑日元汇率", "driver": "全球套息交易平仓踩踏核心风向标"},
    {"cat": "全球外汇", "ticker": "USDCNH=X", "fallback_ticker": "CNY=X", "fallback_label": "（在岸映射·CNY）", "code": "USDCNH", "lt": True, "flow_chart": False, "short_name": "离岸人民币", "name": "美元兑离岸人民币汇率", "driver": "中美利差倒挂深度与外贸结汇意愿"},
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


# ================= 4. 安全格式化与三级 AI 引擎（含新闻降噪过滤） =================
def safe_sheet_val(val):
    """防止以 + / - / = / @ 开头的非纯数字字符串被 Google Sheets 误解析为公式导致 #ERROR!"""
    if isinstance(val, str):
        s = val.strip()
        if s and s[0] in ("+", "-", "=", "@"):
            try:
                float(s)
                return val
            except ValueError:
                return "'" + val
    return val


def sanitize_matrix(matrix):
    return [[safe_sheet_val(cell) for cell in row] for row in matrix]


def fmt_num(val, fmt_str="{:,.2f}", suffix="", fallback="待刷新"):
    if val is None or not isinstance(val, (int, float)):
        return fallback
    return f"{fmt_str.format(val)}{suffix}"


def clean_html_text(raw_txt):
    if not raw_txt:
        return ""
    txt = re.sub(r"<[^>]+>", " ", str(raw_txt))
    txt = re.sub(r"&nbsp;|&amp;|&quot;|&#39;|&lt;|&gt;", " ", txt)
    return re.sub(r"\s+", " ", txt).strip()


def call_llm_with_fallback(prompt, max_tokens=4500, reasoning_effort=None, timeout_sec=120):
    """三级 AI 容灾调用链：主中转站 (Starwish) -> OpenRouter -> Google Gemini"""
    # 1. 主通道：OPENAI_BASE_URL
    if OPENAI_API_KEY:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        efforts = [reasoning_effort] if reasoning_effort else [None]
        if reasoning_effort and reasoning_effort.lower() == "max":
            efforts.append("high")
        for eff in efforts:
            try:
                body = {
                    "model": OPENAI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                }
                if eff:
                    body["reasoning_effort"] = eff
                resp = HTTP.post(f"{OPENAI_BASE_URL}/chat/completions", headers=headers, json=body, timeout=(15, timeout_sec))
                if resp.status_code == 200:
                    txt = resp.json()["choices"][0]["message"]["content"].strip()
                    if txt:
                        return txt, f"{OPENAI_MODEL}({eff or 'std'})"
                elif resp.status_code == 400 and eff == "max":
                    continue
            except Exception as e:
                print(f"[INFO] 主 AI 通道尝试失败 ({eff}): {e}")

    # 2. 备用通道一：OpenRouter
    if OPENROUTER_API_KEY:
        try:
            headers = {
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com",
            }
            body = {
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
            }
            resp = HTTP.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=body, timeout=(15, timeout_sec))
            if resp.status_code == 200:
                txt = resp.json()["choices"][0]["message"]["content"].strip()
                if txt:
                    return txt, f"OpenRouter/{OPENROUTER_MODEL}"
        except Exception as e:
            print(f"[INFO] OpenRouter 备用通道尝试失败: {e}")

    # 3. 备用通道二：Google Gemini 原生 REST API
    if GEMINI_API_KEY:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
            body = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.3},
            }
            resp = HTTP.post(url, json=body, timeout=(15, timeout_sec))
            if resp.status_code == 200:
                candidates = resp.json().get("candidates", [])
                if candidates:
                    parts = candidates[0].get("content", {}).get("parts", [])
                    txt = "".join(p.get("text", "") for p in parts).strip()
                    if txt:
                        return txt, f"Gemini/{GEMINI_MODEL}"
        except Exception as e:
            print(f"[INFO] Gemini 备用通道尝试失败: {e}")

    return None, "本地规则兜底"


def is_macro_relevant_news(title, desc):
    """过滤个人理财八卦、情感消费等与宏观交易无关的噪音新闻"""
    combined = f"{title} {desc}".lower()
    noise_patterns = [
        r"\bmy wife\b", r"\bmy husband\b", r"\bretirement savings\b", r"\bannuity\b",
        r"\bi'm terrified\b", r"\bhas me spooked\b", r"\bshould i sell my\b",
        r"\bshould i put \$", r"\bpersonal finance\b", r"\bcredit card rewards\b",
        r"\bmortgage rate shopping\b", r"\bmillionaire\b.*\bhabit\b",
    ]
    for pat in noise_patterns:
        if re.search(pat, combined):
            return False

    macro_keywords = r"(fed|fomc|powell|rate|yield|treasury|inflation|cpi|pce|gdp|jobs|payroll|unemployment|recession|tariff|trade|sanction|china|pboc|boj|ecb|bank|credit|debt|deficit|oil|opec|crude|gold|copper|dollar|yen|yuan|renminbi|euro|s&p|nasdaq|semiconductor|chip|ai|nvidia|earnings|stock|market|economy|central bank|liquidity)"
    return bool(re.search(macro_keywords, combined))


def translate_and_summarize_news_zh(news_items):
    if not news_items:
        return news_items, False

    payload_lines = [
        f"{i+1}. 板块:【{n['sector']}】 | 原标题: {n['raw_title']} | 原文摘要: {n['raw_desc'][:260]}"
        for i, n in enumerate(news_items)
    ]
    prompt = (
        "请将以下全球财经新闻逐条翻译并提炼为规范的简体中文，"
        "要求每条输出一个 JSON 对象，包含：\n"
        "- index: 序号（整数）\n"
        "- zh_title: 简明准确的中文新闻标题（严禁出现任何英文单词，人名机构名使用中文标准译名）\n"
        "- zh_summary: 55至85字的中文核心摘要，必须交代清楚核心事件数据及对股、债、汇或大宗商品的传导影响（严禁任何英文单词）\n"
        "严格返回 JSON 数组，不要输出任何额外解释文字：\n"
        + "\n".join(payload_lines)
    )
    content, used_model = call_llm_with_fallback(prompt, max_tokens=4500, timeout_sec=90)
    if content:
        try:
            json_match = re.search(r"\[.*\]", content, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group(0))
                parsed_map = {int(x["index"]): x for x in parsed if "index" in x}
                translated_cnt = 0
                for i, n in enumerate(news_items):
                    item = parsed_map.get(i + 1)
                    if item:
                        if item.get("zh_title"):
                            n["title"] = str(item["zh_title"]).strip()
                            translated_cnt += 1
                        if item.get("zh_summary"):
                            n["summary"] = str(item["zh_summary"]).strip()
                if translated_cnt > 0:
                    print(f"[INFO] 新闻智能翻译完成（引擎：{used_model}，共 {translated_cnt} 条）")
                    return news_items, True
        except Exception as e:
            print(f"[INFO] AI 新闻 JSON 解析回退: {e}")

    # 本地规则回退（不直接堆砌大段未翻译英文，输出结构化中文宏观映射提示）
    sector_impact_hint = {
        "央行与货币政策": "牵动短端利率定价与全球美元流动性预期，直接影响美债与成长股估值。",
        "债市信用与财政": "影响长端美债贴现率分母及高收益企业债再融资信用利差。",
        "美股科技与盈利": "映射权益市场分子端盈利预期与科技半导体资本开支景气度。",
        "大宗商品与能源": "决定全球供给端通胀黏性与实体制造业补库周期强度。",
        "中国与全球外汇": "反映跨境资本流动方向、中美利差传导与非美货币汇率弹性。",
        "地缘贸易与供应链": "影响全球供应链成本溢价、避险资产定价与滞胀尾部风险。",
    }
    for n in news_items:
        n["title"] = f"【{n['source']}】{n['sector']}最新动态"
        n["summary"] = f"涉及{n['sector']}边际变化，{sector_impact_hint.get(n['sector'], '需结合跨资产价格验证流动性方向。')}"
    return news_items, False


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
            for item in root.findall(".//item")[:15]:
                title_el = item.find("title")
                desc_el = item.find("description")
                if title_el is None or not title_el.text:
                    continue
                raw_title = clean_html_text(title_el.text)
                raw_desc = clean_html_text(desc_el.text if desc_el is not None else "")
                if not is_macro_relevant_news(raw_title, raw_desc):
                    continue
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


# ================= 5. 多源精准数据抓取与解析引擎（含 FRED CSV 兜底） =================
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
        display_name = item["name"]
        _, prices, _ = fetch_yahoo_chart(active_ticker, "1mo", "1d")
        if not prices and item.get("fallback_ticker"):
            active_ticker = item["fallback_ticker"]
            _, prices, _ = fetch_yahoo_chart(active_ticker, "1mo", "1d")
            if prices and item.get("fallback_label"):
                display_name = f"{item['name']}{item['fallback_label']}"
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
            "name": display_name,
            "price": p0,
            "pct_1d": round(pct_1d / 100.0, 5),
            "pct_5d": round(pct_5d / 100.0, 5) if pct_5d is not None else "",
            "pct_20d": round(pct_20d / 100.0, 5) if pct_20d is not None else "",
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
            "pct_1d": round(pct_1d / 100.0, 5),
            "pct_5d": round(pct_5d / 100.0, 5) if pct_5d is not None else "",
            "pct_20d": round(pct_20d / 100.0, 5) if pct_20d is not None else "",
            "driver": c_item["driver"]
        })

    spx_pe = fetch_spx_pe_reliable()
    if spx_pe:
        series_obs["SPX_PE"] = [spx_pe]

    return cross_asset_items, market_stats


def fetch_fred_csv_fallback(sid, obs_start):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd={obs_start}"
    try:
        resp = HTTP.get(url, timeout=(10, 25))
        if resp.status_code != 200:
            return {}
        dmap = {}
        reader = csv.reader(io.StringIO(resp.text))
        next(reader, None)
        for row in reader:
            if len(row) >= 2 and row[1] not in ("", ".", "NaN"):
                try:
                    dmap[row[0].strip()] = float(row[1].strip())
                except ValueError:
                    continue
        return dmap
    except Exception:
        return {}


def fetch_fred_data():
    series_obs = {}
    series_dates = {}
    series_date_maps = {}
    lt_hist_dates = {}
    # 拉取近 4.6 年（1680 天）数据，确保 156 周（3 年）内所有月频同比均有完整基准，避免前一年平铺失真
    obs_start = (datetime.now(TZ_CST) - timedelta(days=1680)).strftime("%Y-%m-%d")

    for item in FRED_SERIES:
        sid = item["id"]
        dmap = {}
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={sid}&api_key={FRED_API_KEY}"
            f"&file_type=json&sort_order=desc&observation_start={obs_start}"
        )
        try:
            resp = HTTP.get(url, timeout=(10, 30))
            if resp.status_code == 200:
                for o in resp.json().get("observations", []):
                    val_str = str(o.get("value", "")).strip()
                    if val_str not in ("", ".", "NaN"):
                        try:
                            dmap[o["date"]] = float(val_str)
                        except ValueError:
                            continue
        except Exception as e:
            print(f"[INFO] JSON 接口获取 {sid} 异常，切换 CSV 直连通道: {e}")

        if len(dmap) < 2:
            csv_dmap = fetch_fred_csv_fallback(sid, obs_start)
            if len(csv_dmap) > len(dmap):
                dmap = csv_dmap

        if not dmap:
            print(f"[WARN] 底层序列 {item['name']} ({sid}) 暂不可用")
            continue

        sorted_desc_dts = sorted(dmap.keys(), reverse=True)
        vals = [dmap[d] for d in sorted_desc_dts]
        series_obs[sid] = vals
        series_dates[sid] = sorted_desc_dts[0]
        series_date_maps[sid] = dmap
        if item.get("lt"):
            lt_hist_dates[sid] = dmap
        time.sleep(0.05)

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
    cn_ok_count = 0
    try:
        import akshare as ak
    except Exception:
        return cn_ok_count

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
                cn_ok_count += 1
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
                cn_ok_count += 1
        if col_m2:
            r2 = get_latest_two(df_ms, col_dt, col_m2)
            if r2:
                series_obs["CN_M2_YOY"] = [r2[1], r2[2]]
                series_dates["CN_M2_YOY"] = r2[0]
                cn_ok_count += 1
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
            cn_ok_count += 1
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
                cn_ok_count += 1
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
                cn_ok_count += 1
    except Exception:
        pass

    return cn_ok_count


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


# ================= 6. 底层时序对齐 + 10张精炼核心金字塔图表规范 =================
CHART_SERIES_COLUMNS = [
    ("日期", "Date"),                                         # 0 (A)
    ("美元净流动性（十亿美元）", "NET_LIQUIDITY"),            # 1 (B)
    ("标普500指数", "SPX"),                                   # 2 (C)
    ("美国2年期国债收益率（%）", "DGS2"),                     # 3 (D)
    ("美国10年期国债收益率（%）", "DGS10"),                   # 4 (E)
    ("美债10Y-2Y期限利差（%）", "SPREAD_10Y2Y"),              # 5 (F)
    ("美国10年期TIPS实际利率（%）", "DFII10"),                # 6 (G)
    ("美国10年期盈亏平衡通胀预期（%）", "T10YIE"),            # 7 (H)
    ("COMEX黄金期货（美元/盎司）", "GOLD"),                   # 8 (I)
    ("中国10年期国债收益率（%）", "CN_DGS10"),                # 9 (J)
    ("中美10年期利差（%）", "US_CN_SPREAD"),                  # 10 (K)
    ("铜金比（实体周期系数）", "COPPER_GOLD_RATIO"),          # 11 (L)
    ("布伦特原油（美元/桶）", "BRENT"),                       # 12 (M)
    ("高收益债信用利差（%）", "BAMLH0A0HYM2"),                # 13 (N)
    ("美股VIX恐慌指数", "VIX"),                               # 14 (O)
    ("美商业银行总信贷（十亿美元）", "TOTLL"),                # 15 (P)
    ("美国区域银行ETF（KRE）", "US_REG_BANK"),                # 16 (Q)
    ("美联储资产负债表（十亿美元）", "WALCL_B"),              # 17 (R)
    ("财政部TGA账户（十亿美元）", "TGA_B"),                   # 18 (S)
    ("美联储隔夜逆回购（十亿美元）", "RRPONTSYD"),            # 19 (T)
    ("银行准备金余额（十亿美元）", "WRESBAL_B"),              # 20 (U)
    ("美国工商业贷款余额（十亿美元）", "BUSLOANS"),           # 21 (V)
    ("美元指数（DXY）", "DXY"),                               # 22 (W)
    ("美元兑日元汇率", "USDJPY"),                             # 23 (X)
    ("联邦利息占财政收入比（%）", "INTEREST_REV_RATIO"),      # 24 (Y)
    ("主权债务雪球差值r-g（%）", "R_MINUS_G"),                # 25 (Z)
    ("美国广义货币M2同比增速（%）", "US_M2_YOY"),             # 26 (AA)
    ("美国M1-M2货币活化剪刀差（%）", "US_M1_M2_SCISSORS"),    # 27 (AB)
    ("美国单月新增非农就业（万人）", "NFP_MONTHLY_10K"),      # 28 (AC)
    ("官方萨姆规则衰退指标（%）", "SAHMREALTIME"),            # 29 (AD)
    ("美国耐用品新订单同比增速（%）", "DGORDER_YOY"),         # 30 (AE)
    ("美国商业库存销售比（倍）", "ISRATIO"),                  # 31 (AF)
]

# 精简为 10 张“左图右解说”核心金字塔图表（自塔基 L1 至塔尖 L6，零冗余、零重叠）
CHART_SPECS_MASTER = [
    {
        "key": "c1", "tier": 0,
        "tier_banner": "【第1阶 L1·信用引擎】商业银行信贷总量与高收益垃圾债风险定价",
        "title": "图1【L1信用引擎·组合图】美国商业银行总信贷规模（左轴柱）与高收益垃圾债利差（右轴线）",
        "type": "COMBO", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "商业银行总信贷（十亿美元）", "y_right_title": "高收益债信用利差（%）",
        "series": [
            {"col": 15, "label": "美商业银行总信贷", "unit": "十亿美元", "axis": "LEFT_AXIS", "stype": "COLUMN"},
            {"col": 13, "label": "高收益债信用利差", "unit": "%", "axis": "RIGHT_AXIS", "stype": "LINE"},
        ],
    },
    {
        "key": "c2", "tier": 1,
        "tier_banner": "【第2阶 L1·货币活化】广义货币供应增速与M1-M2活期资金活化剪刀差",
        "title": "图2【L1货币活化·组合图】美国广义货币M2同比增速（左轴柱）与M1-M2活化剪刀差（右轴线）",
        "type": "COMBO", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "美国M2同比增速（%）", "y_right_title": "M1-M2活化剪刀差（%）",
        "series": [
            {"col": 26, "label": "美国M2同比增速", "unit": "%", "axis": "LEFT_AXIS", "stype": "COLUMN"},
            {"col": 27, "label": "美国M1-M2活化剪刀差", "unit": "%", "axis": "RIGHT_AXIS", "stype": "LINE"},
        ],
    },
    {
        "key": "c3", "tier": 2,
        "tier_banner": "【第3阶 L2·美元水管】全球美元净流动性总阀门与TGA、逆回购三蓄水池",
        "title": "图3【L2美元水管·面积图】美元净流动性总阀门（左轴）与财政部TGA、隔夜逆回购（右轴）",
        "type": "AREA", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "美元净流动性（十亿美元）", "y_right_title": "TGA与逆回购余额（十亿美元）",
        "series": [
            {"col": 1, "label": "美元净流动性", "unit": "十亿美元", "axis": "LEFT_AXIS"},
            {"col": 18, "label": "财政部TGA账户", "unit": "十亿美元", "axis": "RIGHT_AXIS"},
            {"col": 19, "label": "美联储隔夜逆回购", "unit": "十亿美元", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c4", "tier": 3,
        "tier_banner": "【第4阶 L2·利率之锚】名义收益率、10年期TIPS实际利率与期限利差倒挂修复",
        "title": "图4【L2利率之锚·双轴折线】美债2Y、10Y名义与10Y实际利率（左轴）及10Y-2Y期限利差（右轴）",
        "type": "LINE", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "国债与TIPS实际利率（%）", "y_right_title": "10Y-2Y 期限利差（%）",
        "series": [
            {"col": 3, "label": "美国2年期国债收益率", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 4, "label": "美国10年期国债收益率", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 6, "label": "10年期TIPS实际利率", "unit": "%", "axis": "LEFT_AXIS"},
            {"col": 5, "label": "美债10Y-2Y期限利差", "unit": "%", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c5", "tier": 4,
        "tier_banner": "【第5阶 L3·周期四象限】宏观经济增长—通胀动态象限定位与近12月漂移轨迹",
        "title": "图5【L3周期四象限·散点图】宏观增长—通胀四象限定位（右下复苏/右上过热/左上滞胀/左下衰退）",
        "type": "SCATTER", "source_table": "quad", "x_col": 33, "x_title": "经济增长景气得分（>0扩张 / <0放缓）",
        "y_left_title": "通胀压力得分（>0高通胀 / <0低通胀）", "y_right_title": "",
        "series": [
            {"col": 34, "label": "近12个月历史轨迹", "unit": "分", "axis": "LEFT_AXIS"},
            {"col": 35, "label": "当月最新站位", "unit": "分", "axis": "LEFT_AXIS"},
        ],
    },
    {
        "key": "c6", "tier": 5,
        "tier_banner": "【第6阶 L3·就业硬数据】美国单月新增非农就业与萨姆规则 0.50% 衰退红线",
        "title": "图6【L3就业硬数据·组合图】美国单月新增非农就业（右轴柱·万人）与萨姆规则指标（左轴线）",
        "type": "COMBO", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "萨姆规则衰退指标（%·0.50红线）", "y_right_title": "单月新增非农就业（万人）",
        "series": [
            {"col": 28, "label": "美国单月新增非农就业", "unit": "万人", "axis": "RIGHT_AXIS", "stype": "COLUMN"},
            {"col": 29, "label": "萨姆规则衰退指标", "unit": "%", "axis": "LEFT_AXIS", "stype": "LINE"},
        ],
    },
    {
        "key": "c7", "tier": 6,
        "tier_banner": "【第7阶 L3·财政生死线】联邦利息吞噬财政收入比（20%红线）与主权债务雪球 r-g",
        "title": "图7【L3财政生死线·组合图】联邦利息占财政收入比（左轴柱·20%红线）与债务雪球r-g（右轴线）",
        "type": "COMBO", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "利息支出占财政收入比（%）", "y_right_title": "主权债务雪球 r-g（%）",
        "series": [
            {"col": 24, "label": "联邦利息占财政收入比", "unit": "%", "axis": "LEFT_AXIS", "stype": "COLUMN"},
            {"col": 25, "label": "主权债务雪球差值r-g", "unit": "%", "axis": "RIGHT_AXIS", "stype": "LINE"},
        ],
    },
    {
        "key": "c8", "tier": 7,
        "tier_banner": "【第8阶 L4·跨境汇率阀门】美元指数总阀门（DXY）与中美10年期国债利差",
        "title": "图8【L4汇率与利差锚·双轴折线】美元指数DXY（左轴）与中美10年期国债利差（右轴）",
        "type": "LINE", "source_table": "ts", "x_col": 0, "x_title": "近三年周频日期",
        "y_left_title": "美元指数（DXY）", "y_right_title": "中美10年期利差（%）",
        "series": [
            {"col": 22, "label": "美元指数", "unit": "", "axis": "LEFT_AXIS"},
            {"col": 10, "label": "中美10年期利差", "unit": "%", "axis": "RIGHT_AXIS"},
        ],
    },
    {
        "key": "c9", "tier": 8,
        "tier_banner": "【第9阶 L5·全球资产流向】16大核心大类资产20日中期动量与5日边际流向对比",
        "title": "图9【L5全球资产流动性全景·横向条形图】16大核心资产资金净流入与净流出对比（%）",
        "type": "BAR", "source_table": "flow", "x_col": 37, "x_title": "全球大类资产（>0净流入 / <0净流出）",
        "y_left_title": "资金净流向动量（%）", "y_right_title": "",
        "series": [
            {"col": 38, "label": "20日资金净流向动量", "unit": "%", "axis": "BOTTOM_AXIS"},
            {"col": 39, "label": "5日边际资金流向", "unit": "%", "axis": "BOTTOM_AXIS"},
        ],
    },
    {
        "key": "c10", "tier": 9,
        "tier_banner": "【第10阶 L6·风险雷达塔尖】36维黑天鹅与灰犀牛危急度Top 12排行（距100%红灯线进度）",
        "title": "图10【L6风险雷达塔尖·横向条形图】高危系统性风险因子危急度排行（vs 65%警戒线）",
        "type": "BAR", "source_table": "risk", "x_col": 41, "x_title": "高危系统性风险因子（按危急度降序）",
        "y_left_title": "距红灯熔断线进度（%）", "y_right_title": "",
        "series": [
            {"col": 42, "label": "距红灯熔断线进度", "unit": "%", "axis": "BOTTOM_AXIS"},
            {"col": 43, "label": "黄灯警戒线", "unit": "%", "axis": "BOTTOM_AXIS"},
        ],
    },
]


def calc_unified_quadrant_coords(ind_yoy, nfp_yoy, rs_yoy, pce_3m_ann, cpi_yoy):
    """统一四象限坐标系公式（当月红点与近12个月历史轨迹严格使用同一套三因子算法）"""
    gx = round(0.35 * (ind_yoy - 1.2) + 0.35 * (nfp_yoy - 1.2) + 0.30 * (rs_yoy - 3.0), 2)
    iy = round(0.55 * (pce_3m_ann - 2.3) + 0.45 * (cpi_yoy - 2.5), 2)
    return gx, iy


def build_monthly_derived_dmap(dmap, mode="yoy"):
    if not dmap or len(dmap) < 2:
        return {}
    sorted_dts = sorted(dmap.keys())
    out = {}
    for i, dt in enumerate(sorted_dts):
        if mode == "mom_10k" and i >= 1:
            out[dt] = round((dmap[dt] - dmap[sorted_dts[i - 1]]) / 10.0, 2)
        elif mode == "yoy" and i >= 12:
            prev_12 = dmap[sorted_dts[i - 12]]
            if prev_12 != 0:
                out[dt] = round((dmap[dt] / prev_12 - 1.0) * 100.0, 2)
    return out


def build_clean_chart_timeseries(lt_hist_dates):
    """生成 156 周（近 3 年）底层运算宽表；缺失序列保持空字符串而非填 0.0，杜绝假归零断崖"""
    spx_weekly = sorted(lt_hist_dates.get("SPX", {}).keys())
    if not spx_weekly:
        return []
    weekly_dates = spx_weekly[-156:]

    enriched_hist = dict(lt_hist_dates)
    m2_yoy_map = build_monthly_derived_dmap(lt_hist_dates.get("M2SL", {}), mode="yoy")
    m1_yoy_map = build_monthly_derived_dmap(lt_hist_dates.get("M1SL", {}), mode="yoy")
    nfp_10k_map = build_monthly_derived_dmap(lt_hist_dates.get("PAYEMS", {}), mode="mom_10k")
    dg_yoy_map = build_monthly_derived_dmap(lt_hist_dates.get("DGORDER", {}), mode="yoy")

    if m2_yoy_map:
        enriched_hist["US_M2_YOY"] = m2_yoy_map
    if m1_yoy_map and m2_yoy_map:
        enriched_hist["US_M1_M2_SCISSORS"] = {
            dt: round(m1_yoy_map[dt] - m2_yoy_map[dt], 2) for dt in m1_yoy_map if dt in m2_yoy_map
        }
    if nfp_10k_map:
        enriched_hist["NFP_MONTHLY_10K"] = nfp_10k_map
    if dg_yoy_map:
        enriched_hist["DGORDER_YOY"] = dg_yoy_map

    first_valid = {}
    for sid, dmap in enriched_hist.items():
        if dmap:
            earliest_dt = min(dmap.keys())
            first_valid[sid] = round(dmap[earliest_dt], 4)

    gdp_dmap = enriched_hist.get("GDP", {})
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
        for sid, dmap in enriched_hist.items():
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
        row_vals = [r.get(code, "") for _, code in CHART_SERIES_COLUMNS]
        matrix.append(row_vals)
    return matrix


def build_four_quadrant_series(series_obs, current_growth_score, current_infl_score):
    """使用与当月红点完全一致的三因子公式生成近 12 个月四象限历史轨迹"""
    ind_arr = series_obs.get("INDPRO", [])
    nfp_arr = series_obs.get("PAYEMS", [])
    rs_arr = series_obs.get("RSAFS", [])
    cpi_arr = series_obs.get("CPIAUCSL", [])
    pce_arr = series_obs.get("PCEPILFE", [])

    pts = []
    max_steps = min(
        11,
        len(ind_arr) - 13,
        len(nfp_arr) - 13,
        len(rs_arr) - 13,
        len(cpi_arr) - 13,
        len(pce_arr) - 4,
    )
    if max_steps >= 3:
        for lag in range(max_steps, 0, -1):
            ind_yoy = (ind_arr[lag] / ind_arr[lag + 12] - 1) * 100.0
            nfp_yoy = (nfp_arr[lag] / nfp_arr[lag + 12] - 1) * 100.0
            rs_yoy = (rs_arr[lag] / rs_arr[lag + 12] - 1) * 100.0
            cpi_yoy = (cpi_arr[lag] / cpi_arr[lag + 12] - 1) * 100.0
            pce_3m = ((pce_arr[lag] / pce_arr[lag + 3]) ** 4 - 1) * 100.0
            gx, iy = calc_unified_quadrant_coords(ind_yoy, nfp_yoy, rs_yoy, pce_3m, cpi_yoy)
            pts.append((gx, iy, ""))

    pts.append((current_growth_score, current_infl_score, current_infl_score))
    header = ["经济增长景气得分（X轴：>0扩张 / <0放缓）", "近12个月历史轨迹（通胀Y轴）", "当月最新站位坐标（通胀Y轴）"]
    return [header] + [[x, y_hist, y_curr] for x, y_hist, y_curr in pts]


def extract_plotted_chart_facts(ts_matrix, quad_matrix, flow_matrix, risk_chart_matrix):
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
                series_summaries.append(f"条形图危急度前三因子（距100%红线进度）：{top3_str}")
                verification_tokens.append(f"榜首风险:{r_items[0][0]}({r_items[0][1]:.1f}%)")

        plotted_facts[ckey] = {
            "title": spec["title"],
            "fact_text": "；".join(series_summaries),
            "verify_stamp": "✅ 实测核验：" + " ｜ ".join(verification_tokens),
        }
    return plotted_facts


# ================= 7. 全因子精确计算 + 四维关联式 AI 读图解说生成 =================
def calc_risk_ratio(val, safe_base, warn_line, red_line):
    """平滑计算危急度进度（65% 为黄灯警戒，100% 为红灯熔断，超红线按半斜率延伸至 200% 以保留排序区分度）"""
    if red_line > safe_base:
        if val <= safe_base:
            return 0.0
        if val <= warn_line:
            return 0.65 * (val - safe_base) / max(1e-6, (warn_line - safe_base))
        if val <= red_line:
            return 0.65 + 0.35 * (val - warn_line) / max(1e-6, (red_line - warn_line))
        return min(2.0, 1.0 + 0.35 * (val - red_line) / max(1e-6, (red_line - warn_line)))
    else:
        if val >= safe_base:
            return 0.0
        if val >= warn_line:
            return 0.65 * (safe_base - val) / max(1e-6, (safe_base - warn_line))
        if val >= red_line:
            return 0.65 + 0.35 * (warn_line - val) / max(1e-6, (warn_line - red_line))
        return min(2.0, 1.0 + 0.35 * (red_line - val) / max(1e-6, (warn_line - red_line)))


def generate_ai_full_and_chart_commentary(llm_ctx, plotted_facts, fallback_report, fallback_chart_notes):
    prompt = (
        "你是严格遵循《宏观经济｜六层第一性原理实战通关总纲》的首席宏观策略师与交易教练。\n"
        "请基于传入的六层全量因子、衍生公式、36维风险雷达、六大板块新闻以及【10张核心结构图的实测坐标轴数据】，"
        "输出一个合法的 JSON 对象（不要输出任何 Markdown 代码块标记以外的废话），包含以下两个字段：\n"
        "1. `master_report`（字符串，1200~1500字）：严格按六层金字塔顺序进行通俗且深度的全因子中文解说，包含：\n"
        "   - 【一、宏观周期四象限定位与新闻催化主线】\n"
        "   - 【二、第一至二层：信用引擎、货币活化与美元净流动性水管】\n"
        "   - 【三、第三至四层：美欧日中硬数据、财政付息生死线与跨境汇率博弈】\n"
        "   - 【四、第五层：全球跨资产资金流向与背离因子穿透】\n"
        "   - 【五、第六层：36维黑天鹅与灰犀牛高危预警排雷】（带 🔴/🟡/🟢 状态图标）\n"
        "   - 【六、三情景概率分布与未来十日量化证伪红线】\n"
        "2. `chart_notes`（对象，必须完整包含键 `c1` 到 `c10` 共 10 个字符串，一个都不能缺！每个 180~240 字）：分别为驾驶舱的 10 张核心结构图撰写专属深度读图解说。\n"
        "【每张图解说必须严格包含以下 4 个部分（用换行分隔）】：\n"
        "📌【看图结论】用大白话直接报出左轴和右轴最新实测数值及近3月方向，说明这张图揭示的底层真相；\n"
        "🔄【横向对比】对比三年历史区间、警戒红线或另一条曲线的剪刀差，说明当前处于什么水位；\n"
        "🧭【方向推演】给出2条清晰的实战情景分支判断（若指标突破/回落至某位置，对股市、债市、黄金或汇率有何具体影响）；\n"
        "🔗【交叉验证】明确指出需要结合本看板的哪几张图（写明图号和指标名）联动交叉验证。\n\n"
        "【强制校验铁律】：\n"
        "- 10 个键 `c1`..`c10` 必须全部输出且不能为空；\n"
        "- 每一张图必须严格对齐 `plotted_charts_ground_truth` 中对应键的左右轴实测数据；\n"
        "- 全文 100% 使用规范简体中文，严禁夹杂英文缩写或英文单词；\n"
        "- 预警与趋势醒目使用 `🔴`、`🟡`、`🟢`、`🔺`、`🔻`、`⚠️` 图标。\n\n"
        f"今日全量宏观数据与10张图实测基准：\n{json.dumps(llm_ctx, ensure_ascii=False)}"
    )

    final_report = fallback_report
    raw_notes = dict(fallback_chart_notes)

    raw_txt, used_engine = call_llm_with_fallback(
        prompt, max_tokens=14000, reasoning_effort=OPENAI_REASONING_EFFORT, timeout_sec=240
    )
    if raw_txt:
        try:
            jm = re.search(r"\{.*\}", raw_txt, re.DOTALL)
            if jm:
                parsed = json.loads(jm.group(0))
                m_rep = str(parsed.get("master_report", "")).strip()
                c_notes = parsed.get("chart_notes", {})
                if m_rep and len(m_rep) > 200:
                    header_banner = f"【首席宏观策略师智能全因子总研判 | 引擎：{used_engine} | 更新：{NOW_STR}】\n\n"
                    final_report = header_banner + m_rep
                if isinstance(c_notes, dict):
                    for k, v in c_notes.items():
                        v_clean = str(v).strip()
                        if k in raw_notes and len(v_clean) >= 50:
                            raw_notes[k] = v_clean
        except Exception as e:
            print(f"[INFO] AI 深度研判 JSON 解析回退: {e}")

    verified_notes = {}
    for spec in CHART_SPECS_MASTER:
        ckey = spec["key"]
        short_badge = spec["title"].split("（")[0]
        body_txt = raw_notes.get(ckey, "").strip()
        if len(body_txt) < 40:
            body_txt = fallback_chart_notes.get(ckey, "")
        stamp = plotted_facts.get(ckey, {}).get("verify_stamp", "")
        verified_notes[ckey] = f"🤖【{short_badge}·四维关联解说】\n{body_txt}\n{stamp}"

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

    # B. 六层核心经济硬数据与衍生公式快照
    totll_val = get("TOTLL")
    us_credit_yoy = calc_yoy("TOTLL", lag=52)
    if us_credit_yoy is not None:
        sig = "🟢 信用平稳扩张" if us_credit_yoy > 2.0 else "🟡 信用派生放缓"
        macro_snapshot_rows.append(["美国", "L1·信用引擎", "商业银行总信贷同比增速", f"{us_credit_yoy:+.2f}%", f"总余额 {fmt_num(totll_val, '{:,.0f}', '十亿美元')}", s_dates.get("TOTLL", "-"), sig])

    us_m1_yoy = calc_yoy("M1SL", lag=12)
    us_m2_yoy = calc_yoy("M2SL", lag=12)
    us_scissors = round(us_m1_yoy - us_m2_yoy, 2) if (us_m1_yoy is not None and us_m2_yoy is not None) else None
    if us_scissors is not None:
        sig = "🟢 美元活期资金活化" if us_scissors > -1.0 else "🟡 货币定期沉淀偏高"
        macro_snapshot_rows.append(["美国", "L1·货币活化", "美国M1与M2货币活化剪刀差", f"{us_scissors:+.2f}%", f"M1 {us_m1_yoy:+.1f}% / M2 {us_m2_yoy:+.1f}%", s_dates.get("M2SL", "-"), sig])

    cn_m1_yoy, cn_m2_yoy = get("CN_M1_YOY"), get("CN_M2_YOY")
    cn_scissors = None
    if cn_m1_yoy is not None and cn_m2_yoy is not None:
        cn_scissors = round(cn_m1_yoy - cn_m2_yoy, 2)
        sig = "🟢 资金活化改善" if cn_scissors > -2.0 else "🟡 定期存款沉淀偏高"
        macro_snapshot_rows.append(["中国", "L1·货币活化", "中国M1与M2货币活化剪刀差", f"{cn_scissors:+.2f}%", f"M1 {cn_m1_yoy:.1f}% / M2 {cn_m2_yoy:.1f}%", s_dates.get("CN_M1_YOY", "-"), sig])

    if net_liq is not None:
        sig = "🟢 净流动性边际扩张" if net_liq_4w_chg >= 0 else "🟡 净流动性边际抽水"
        macro_snapshot_rows.append(["美国", "L2·美元水管", "美元净流动性总阀门", f"{net_liq:,.1f} 十亿美元", f"1周 {net_liq_chg:+.1f}B / 4周 {net_liq_4w_chg:+.1f}B", s_dates.get("WALCL", "-"), sig])

    y10, y2, effr = get("DGS10"), get("DGS2"), get("DFF")
    y10_5, y2_5 = get("DGS10", 5), get("DGS2", 5)
    spread_10_2 = round(y10 - y2, 3) if (y10 is not None and y2 is not None) else None
    implied_rate_cut_bps = round((y2 - effr) * 100, 1) if (y2 is not None and effr is not None) else None
    if y10 is not None and y2 is not None:
        sig = "🟡 收益率曲线倒挂" if spread_10_2 < 0 else "🟢 曲线正斜率区间"
        macro_snapshot_rows.append(["美国", "L2·利率之锚", "10年与2年期美债利率对比", f"10Y {y10:.2f}% / 2Y {y2:.2f}%", f"利差 {spread_10_2:+.2f}% | 隐含降息 {implied_rate_cut_bps:+.0f}基点", s_dates.get("DGS10", "-"), sig])

    tips_0, tips_5 = get("DFII10", 0), get("DFII10", 5)
    t10yie = get("T10YIE") or (round(y10 - tips_0, 2) if (y10 is not None and tips_0 is not None) else None)
    if tips_0 is not None:
        sig = "🔴 高实际利率压制估值" if tips_0 >= 2.25 else ("🟡 实际利率偏高" if tips_0 >= 1.80 else "🟢 实际资金成本温和")
        macro_snapshot_rows.append(["美国", "L2·真实成本", "10年期TIPS实际利率与通胀预期", f"实际 {tips_0:.2f}%", f"盈亏平衡通胀 {t10yie:.2f}%" if t10yie else "-", s_dates.get("DFII10", "-"), sig])

    nfp_add_10k, nfp_diff_k, prev_nfp_add_10k = None, None, None
    payems_dmap = s_dmaps.get("PAYEMS", {})
    if len(payems_dmap) >= 2:
        p_dts = sorted(payems_dmap.keys())
        nfp_diff_k = round(payems_dmap[p_dts[-1]] - payems_dmap[p_dts[-2]], 1)
        nfp_add_10k = round(nfp_diff_k / 10.0, 2)
        if len(p_dts) >= 3:
            prev_nfp_add_10k = round((payems_dmap[p_dts[-2]] - payems_dmap[p_dts[-3]]) / 10.0, 2)
        sig = "🟡 就业过热抑制降息" if nfp_add_10k > 20 else ("🟢 温和着陆区间" if nfp_add_10k >= 10 else "🔴 就业显著降温")
        prev_str = f"前值 {prev_nfp_add_10k:+.2f}万人 | " if prev_nfp_add_10k is not None else ""
        macro_snapshot_rows.append([
            "美国", "L3·就业硬数据", "单月新增非农就业人数",
            f"{nfp_add_10k:+.2f} 万人（{nfp_diff_k:+.0f}千人）",
            f"{prev_str}失业率 {get('UNRATE')}% | 萨姆 {sahm:.2f}%",
            p_dts[-1], sig
        ])

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

    # C. 统一三因子周期四象限坐标与曲线形态
    ind_yoy = calc_yoy("INDPRO") or 1.2
    nfp_yoy = calc_yoy("PAYEMS") or 1.2
    rs_yoy = calc_yoy("RSAFS") or 3.0
    curr_growth_x, curr_infl_y = calc_unified_quadrant_coords(
        ind_yoy, nfp_yoy, rs_yoy, pce_3m_ann or 2.4, us_cpi_yoy or 2.5
    )

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
        ratio_pct = round(max(0.0, min(2.0, ratio_raw)), 3)
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

    # 严格执行“股、金、债同步恶化”才触发现金为王三杀，避免单一资产正常回调误报 150%
    if y10 is not None and y10_5 is not None:
        y10_5d_bps = round((y10 - y10_5) * 100, 1)
        summary_str = f"标普:{spx_5d:+.1f}% | 黄金:{gold_5d:+.1f}% | 美债:{y10_5d_bps:+.0f}基点"
        spx_drop_score = max(0.0, (-spx_5d - 1.5) / 3.5)
        gold_drop_score = max(0.0, (-gold_5d - 1.5) / 3.5)
        bond_selloff_score = max(0.0, (y10_5d_bps - 8.0) / 18.0)
        if spx_5d < -1.5 and gold_5d < -1.5 and y10_5d_bps > 8.0:
            stress_score = round((spx_drop_score + gold_drop_score + bond_selloff_score) / 2.2, 3)
        else:
            stress_score = round(min(0.55, (spx_drop_score + gold_drop_score + bond_selloff_score) / 4.0), 3)
        add_risk("黑天鹅-04", "急性黑天鹅", "现金为王三杀挤兑", "5日股金债联动状态", summary_str, "股金债同步走弱", "股金暴跌且债率飙升", stress_score, "保证金追缴下机构无差别抛售黄金美债换取美元现金")

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
        b10 = bond_10y_map.get(iso3) if bond_10y_map.get(iso3) is not None else "暂无"
        debt_ratio = em_ratios.get(iso3, "暂无")
        country_rows.append([country_zh_map.get(iso3, iso3), gdp, cpi, b10, ca, fiscal, debt_ratio])

    # F. 全球大类资产资金流向对比表
    flow_rows = [["全球大类资产", "20日资金净流向动量（%）", "5日边际资金流向（%）"]]
    for code, st in market_stats.items():
        if st.get("flow_chart"):
            flow_rows.append([st["short_name"], st["pct_20d"], st["pct_5d"]])

    # G. 10张核心结构图四维关联式兜底深度解说
    tips10 = tips_0 or 0.0
    spx_p = market_stats.get("SPX", {}).get("price")
    au_p = market_stats.get("GOLD", {}).get("price")
    br_p = market_stats.get("BRENT", {}).get("price")
    dxy_p = market_stats.get("DXY", {}).get("price")
    jpy_p = market_stats.get("USDJPY", {}).get("price")
    btc_p = market_stats.get("BTC", {}).get("price")
    ndx_p = market_stats.get("NDX", {}).get("price")
    bus_bal = get("BUSLOANS")

    sorted_risks = sorted(risk_rows, key=lambda x: x[8], reverse=True)
    top1_r = sorted_risks[0] if sorted_risks else ["-", "-", "-", "-", "-", "-", "🟢 绿灯正常", "", 0]

    fallback_chart_notes = {
        "c1": (
            f"📌【看图结论】左轴柱子显示美国商业银行总信贷规模为 {fmt_num(totll_val, '{:,.0f}', '十亿美元')}（同比 {fmt_num(us_credit_yoy, '{:+.2f}', '%')}），右轴红线高收益垃圾债信用利差为 {fmt_num(hy, '{:.2f}', '%')}，说明企业信用渠道仍在平稳运转，市场尚未定价违约恐慌。\n"
            f"🔄【横向对比】对比 3.50% 的黄灯警戒线和 5.00% 的红灯熔断线，当前垃圾债利差处于安全区，企业再融资额外风险补偿可控。\n"
            f"🧭【方向推演】① 若右轴红线向上突破 3.50%，意味着中小企业借新还旧受阻，应果断减持高杠杆股票；② 若左轴信贷柱子持续走高且红线压在 3.50% 下方，则利好美股盈利扩张。\n"
            f"🔗【交叉验证】本图看的是“信贷总量与风险定价”，需结合【图2 货币活化剪刀差】看钱有没有真正流入实体消费与扩产。"
        ),
        "c2": (
            f"📌【看图结论】左轴柱子美国广义货币M2同比增速为 {fmt_num(us_m2_yoy, '{:+.2f}', '%')}，右轴红线M1减M2活化剪刀差为 {fmt_num(us_scissors, '{:+.2f}', '%')}，反映当前货币供应已从收缩恢复温和增长，但部分资金仍沉淀在定期或货币基金里吃利息。\n"
            f"🔄【横向对比】对比中国M1-M2剪刀差（{fmt_num(cn_scissors, '{:+.2f}', '%')}），中美均处于“货币活化待验证”的过渡阶段，剪刀差向上收敛越快代表实体交易越活跃。\n"
            f"🧭【方向推演】① 若右轴剪刀差持续向上收窄甚至转正，说明企业和居民把定期存款转成活期去消费投资，顺周期股与大宗商品将迎来主升浪；② 若M2涨但剪刀差继续深跌，说明资金在金融系统空转。\n"
            f"🔗【交叉验证】需结合【图3 美元净流动性】看央行基础水龙头，再结合【图5 周期四象限】验证活期资金是否转化成了真实经济景气。"
        ),
        "c3": (
            f"📌【看图结论】左轴美元净流动性总阀门为 {fmt_num(net_liq, '{:,.1f}', '十亿美元')}（近4周变动 {net_liq_4w_chg:+.1f}B），右轴隔夜逆回购蓄水池剩余 {fmt_num(r0, '{:,.1f}', '十亿美元')}，揭示当前美元基础水龙头处于微幅平衡拉锯状态。\n"
            f"🔄【横向对比】隔夜逆回购蓄水池已从高峰期大幅消耗至低位，意味着过去两年替美股吸收发债冲击的“免费缓冲垫”所剩无几。\n"
            f"🧭【方向推演】① 若右轴财政部TGA账户大幅吸金而逆回购已无力对冲，左轴净流动性将直接下跌，压制纳斯达克与比特币估值；② 若美联储放缓缩表且财政部加大支出，净流动性走高将推升风险资产。\n"
            f"🔗【交叉验证】净流动性是全球风险资产的分母总开关，必须结合【图9 全球资产流向】观察高弹性资产是否获得真金白银流入。"
        ),
        "c4": (
            f"📌【看图结论】左轴展示美债2年期（{fmt_num(y2, '{:.2f}', '%')}）、10年期（{fmt_num(y10, '{:.2f}', '%')}）与10年期TIPS实际利率（{fmt_num(tips10, '{:.2f}', '%')}），右轴10Y-2Y期限利差为 {fmt_num(spread_10_2, '{:+.2f}', '%')}，当前曲线呈现【{curve_regime}】。\n"
            f"🔄【横向对比】2.0% 是10年期实际利率的分水岭，高于 2.0% 会持续抽血无息黄金与远期现金流科技股；同时期限利差修复标志着货币周期从深度紧缩向正常化过渡。\n"
            f"🧭【方向推演】① 若2年期快速下跌带动牛市陡峭化，利好短债与黄金；② 若10年期与实际利率双双冲高突破 2.35% 警戒线，高市盈率科技股将面临剧烈杀估值。\n"
            f"🔗【交叉验证】要判断利率波动背后的财政供给压力，需立刻对照【图7 联邦利息占财政收入比与债务雪球】。"
        ),
        "c5": (
            f"📌【看图结论】散点图采用统一三因子公式计算经济增长得分（X轴当前 {curr_growth_x:+}）与通胀压力得分（Y轴当前 {curr_infl_y:+}），红点标出本月处于【{quad_short}】，蓝点展示近12个月宏观轨迹漂移方向。\n"
            f"🔄【横向对比】四象限分工明确：右下（增长正、通胀负）是股债双牛的“温和复苏”；右上（双正）是商品占优的“过热”；左上（增长负、通胀正）是央行最怕的“滞胀”；左下（双负）是长债占优的“衰退”。\n"
            f"🧭【方向推演】① 若红点继续向右下象限移动，放心配置成长与周期权益资产；② 若红点向左上角滞胀区漂移，必须削减股票久期并增配实物黄金与短债防御。\n"
            f"🔗【交叉验证】四象限定位依赖月度硬数据，必须结合【图6 新增非农与萨姆规则】预判下个月红点往哪边移动。"
        ),
        "c6": (
            f"📌【看图结论】右轴柱子显示美国单月新增非农就业为 {fmt_num(nfp_add_10k, '{:+.2f}', '万人')}（前值 {fmt_num(prev_nfp_add_10k, '{:+.2f}', '万人')}），左轴红线萨姆规则衰退指标为 {fmt_num(sahm, '{:.2f}', '%')}，表明美国劳动力市场正处于温和降温通道。\n"
            f"🔄【横向对比】萨姆规则 0.50% 是战后历史 100% 应验的衰退铁律，当前距离 0.50% 红线仍有缓冲空间，而单月新增就业 10~18 万人恰好处于“不冷不热”的软着陆区间。\n"
            f"🧭【方向推演】① 若单月非农跌破 8 万人且左轴红线逼近 0.50%，市场将交易“衰退降息”，长债大涨、美股承压；② 若非农反弹超 22 万人，市场会削减降息预期，推升美债收益率与美元。\n"
            f"🔗【交叉验证】就业数据好坏对美股是利好还是利空，取决于当前市场在交易通胀还是衰退，需结合【图5 周期四象限】与【图4 美债利率】联动判断。"
        ),
        "c7": (
            f"📌【看图结论】左轴柱子美国联邦利息支出占财政收入比已达 {fmt_num(interest_rev_ratio, '{:.2f}', '%')}，右轴红线主权债务雪球差值（r-g）为 {fmt_num(r_minus_g, '{:+.2f}', '%')}，揭示美国财政付息压力正处于历史罕见高位。\n"
            f"🔄【横向对比】20% 是主权财政空间的生死警戒线，一旦利息吞噬超过五分之一税收，常规财政政策将被严重锁死；同时 r > g 意味着债务利息增速跑赢经济增速。\n"
            f"🧭【方向推演】① 若柱体继续突破 20% 且 r-g 扩大，市场将长期计入“财政赤字货币化”与主权信用折价，长线利好黄金与抗通胀实物资产；② 唯有美联储降息压低利率 r，才能缓解债务雪球。\n"
            f"🔗【交叉验证】财政供给压力会直接反映在国债定价上，需结合【图4 10年期美债收益率】与【图9 黄金资金流向】观察主权信用溢价。"
        ),
        "c8": (
            f"📌【看图结论】左轴美元指数报 {fmt_num(dxy_p, '{:.2f}')}，右轴中美10年期国债利差为 {fmt_num(us_cn_sp, '{:.2f}', '%')}，显示美元维持相对强势格局，中美长端利差仍处于倒挂区间。\n"
            f"🔄【横向对比】美元指数是全球离岸流动性的总阀门，而中美利差决定跨境资金套利意愿与外贸企业结汇节奏，利差超 2.0% 即进入黄灯警戒区。\n"
            f"🧭【方向推演】① 若美债收益率下行带动右轴利差收窄且美元回落，非美货币与港股、A股核心资产将迎来外资回流窗口；② 若美元冲高且利差走阔，新兴市场资产将继续承压。\n"
            f"🔗【交叉验证】汇率与利差信号需结合【图9 全球资产流向（沪深300/恒生指数）】同步验证跨境资金真实配置方向。"
        ),
        "c9": (
            f"📌【看图结论】横向条形图将全球 16 大核心资产放在同一标尺下对比 20日（深色条）与 5日（浅色条）资金净流向动量，零轴右侧代表资金净流入扩张，零轴左侧代表资金净流出承压。\n"
            f"🔄【横向对比】如果 20日条在右侧（中期强势）但 5日条缩回左侧，说明高位获利盘正在兑现；如果双条同时向右伸长，则是主升浪资金共振。\n"
            f"🧭【方向推演】① 当黄金、长债条形向右伸长而科技股、半导体向左收缩时，说明大资金正在切向防御避险；② 当半导体、标普、精炼铜同步向右扩张时，确认全球风险偏好全面进攻。\n"
            f"🔗【交叉验证】资金流向是市场定价结果，原因要结合【图3 美元净流动性】和【图4 实际利率】验证其是否具备宏观基本面支撑。"
        ),
        "c10": (
            f"📌【看图结论】横向条形图按危急度从高到低展示距离 100% 红灯熔断线最近的 12 项系统性风险因子，竖线为 65% 黄灯警戒线，当前排在首位的是【{top1_r[1]}】（进度 {round(top1_r[8]*100, 1)}%，状态 {top1_r[6]}）。\n"
            f"🔄【横向对比】65% 以下为绿灯安全区，65%~100% 为黄灯预警区（需收紧风险敞口），一旦任何条形冲破 100% 红灯线，就意味着局部脆弱点已演化为系统性危机扳机。\n"
            f"🧭【方向推演】① 重点盯防排名前三的条形是否继续向右逼近 100%，尤其是“垃圾债信用踩踏”与“现金为王三杀挤兑”；② 若多数条形回落至 65% 左侧，则可维持正常权益配置仓位。\n"
            f"🔗【交叉验证】风险雷达是整个金字塔的塔尖警报器，一旦出现红灯，需顺藤摸瓜回到【图1~图9】查找对应的底层传导源头。"
        ),
    }

    if red_alerts:
        probs = "🔴 基准情景（风险加速释放）：45% ｜ 🟢 乐观情景（央行紧急转向救市）：15% ｜ 🟡 悲观情景（尾部危机跨市场扩散）：40%"
        short_prob_kpi = "🔴 防御优先 (45%偏空)"
    elif yellow_alerts:
        probs = "🟡 基准情景（高位震荡消化）：55% ｜ 🟢 乐观情景（经济软着陆延续）：20% ｜ 🔴 悲观情景（流动性紧缩与波动升级）：25%"
        short_prob_kpi = "🟡 震荡分化 (55%中性)"
    else:
        probs = "🟢 基准情景（经济软着陆与板块轮动）：60% ｜ 🟢 乐观情景（全球宽信用共振）：25% ｜ 🟡 悲观情景（二次通胀或地缘冲击）：15%"
        short_prob_kpi = "🟢 软着陆占优 (60%偏多)"

    kpi_strip = {
        "quad_kpi": f"🧭 周期定位：{quad_short}（增长{curr_growth_x:+} / 通胀{curr_infl_y:+}）",
        "liq_kpi": f"💧 美元水管：{fmt_num(net_liq, '{:,.0f}', 'B')}（4周{net_liq_4w_chg:+.1f}B）| 10Y美债：{fmt_num(y10, '{:.2f}', '%')}",
        "risk_kpi": f"🚨 风险雷达：🔴红灯 {len(red_alerts)} 项 ｜ 🟡黄灯 {len(yellow_alerts)} 项 ｜ 🟢绿灯 {len(risk_rows) - len(red_alerts) - len(yellow_alerts)} 项",
        "prob_kpi": f"🎯 情景推演：{short_prob_kpi}",
    }

    fallback_report = (
        f"【首席宏观策略师·六层第一性原理全因子深度总研判 | 更新时间：{NOW_STR}】\n\n"
        f"一、【宏观周期四象限定位与新闻催化主线】\n"
        f"当前全球宏观定位于【{quadrant}】（增长坐标 {curr_growth_x:+}，通胀坐标 {curr_infl_y:+}）。{regime_logic}"
        f"{' 跨资产监测发出背离信号：' + '；'.join(divergence_flags) + '。' if divergence_flags else ' 当前高频资产价格与低频经济硬数据整体保持同向共振。'}\n\n"
        f"二、【第一至二层：信用引擎、货币活化与美元净流动性水管】\n"
        f"1. 信用与货币：美国商业银行总信贷同比 {fmt_num(us_credit_yoy, '{:+.2f}', '%')}，工商业贷款余额 {fmt_num(bus_bal, '{:,.1f}', ' 十亿美元')}，银行收紧贷款标准比例 {fmt_num(sloos_0, '{:.1f}', '%')}；美国M1-M2活化剪刀差 {fmt_num(us_scissors, '{:+.2f}', '%')}，中国M1-M2剪刀差 {fmt_num(cn_scissors, '{:+.2f}', '%')}。\n"
        f"2. 美元水管：美元净流动性总阀门水位 {fmt_num(net_liq, '{:,.1f}', ' 十亿美元')}（1周 {net_liq_chg:+}B，4周 {net_liq_4w_chg:+}B，13周 {net_liq_13w_chg:+}B），隔夜逆回购蓄水池剩余 {fmt_num(r0, '{:,.1f}', ' 十亿美元')}。美债 2年期（{fmt_num(y2, '{:.2f}', '%')}）与 10年期（{fmt_num(y10, '{:.2f}', '%')}）利差为 {fmt_num(spread_10_2, '{:+.2f}', '%')}，曲线呈现【{curve_regime}】，市场隐含未来降息预期 {fmt_num(implied_rate_cut_bps, '{:+.0f}', ' 个基点')}。\n\n"
        f"三、【第三至四层：美欧日中硬数据、财政生死线与跨境汇率博弈】\n"
        f"美国单月新增非农 {fmt_num(nfp_add_10k, '{:+.2f}', ' 万人')}（{fmt_num(nfp_diff_k, '{:+.0f}', ' 千人')}），失业率 {fmt_num(get('UNRATE'), '{:.1f}', '%')}，萨姆规则衰退指标 {sahm:.2f}%；总体消费物价同比 {fmt_num(us_cpi_yoy, '{:.2f}', '%')}，核心个人消费支出 3个月折年率 {fmt_num(pce_3m_ann, '{:+.2f}', '%')}。联邦利息支出占财政收入比重达 {fmt_num(interest_rev_ratio, '{:.2f}', '%')}，主权债务雪球差值（r-g）为 {fmt_num(r_minus_g, '{:+.2f}', '%')}。中国官方制造业采购经理指数报 {fmt_num(cn_pmi, '{:.1f}')}，中美 10年期利差倒挂 {fmt_num(us_cn_sp, '{:.2f}', '%')}。\n\n"
        f"四、【第五层：全球跨资产定价与股权风险溢价穿透】\n"
        f"10年期实际利率报 {tips10:.2f}%，标普500市盈率 {fmt_num(spx_pe_val, '{:.1f}', ' 倍')}，股权风险溢价（ERP）为 {fmt_num(erp_val, '{:+.2f}', '%')}。标普500报 {fmt_num(spx_p, '{:,.1f}', ' 点')}，纳斯达克100报 {fmt_num(ndx_p, '{:,.1f}', ' 点')}，美元指数报 {fmt_num(dxy_p, '{:.2f}')}，美元兑日元报 {fmt_num(jpy_p, '{:.2f}')}，国际黄金报 {fmt_num(au_p, '{:,.1f}', ' 美元/盎司')}，布伦特原油报 {fmt_num(br_p, '{:.2f}', ' 美元/桶')}，比特币报 {fmt_num(btc_p, '{:,.0f}', ' 美元')}。\n\n"
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
            "美国M1减M2剪刀差": us_scissors,
            "中国M1减M2剪刀差": cn_scissors,
            "美元净流动性十亿美元": net_liq,
            "净流动性1周4周13周变动": [net_liq_chg, net_liq_4w_chg, net_liq_13w_chg],
            "隔夜逆回购十亿美元": r0,
            "美债2Y与10Y利率及利差": [y2, y10, spread_10_2],
            "曲线形态": curve_regime,
            "10年期TIPS实际利率与通胀预期": [tips10, t10yie],
        },
        "第三四层_硬数据财政与跨境": {
            "单月新增非农万人与前值": [nfp_add_10k, prev_nfp_add_10k],
            "萨姆规则衰退指标": sahm,
            "CPI同比与核心PCE折年率": [us_cpi_yoy, pce_3m_ann],
            "耐用品新订单同比与库销比": [dg_yoy, isratio_0],
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

    return risk_rows, country_rows, macro_snapshot_rows, flow_rows, (curr_growth_x, curr_infl_y), kpi_strip, llm_ctx, fallback_report, fallback_chart_notes


# ================= 8. 规范化“左图右解说”10阶金字塔布局 + 彻底清理旧图表 =================
def get_or_create_ws(sh, title, rows=260, cols=48):
    try:
        ws = sh.worksheet(title)
        if ws.row_count < rows or ws.col_count < cols:
            ws.resize(rows=max(ws.row_count, rows), cols=max(ws.col_count, cols))
        return ws
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def build_10_pyramid_charts_requests(dash_sheet_id, ts_sheet_id, num_ts_rows, num_quad_rows, num_flow_rows, num_risk_rows):
    """生成 10 张左侧独立容器图表（A:G 宽 805px，图表宽 775px、高 344px，零重叠）"""
    row_count_map = {
        "ts": num_ts_rows,
        "quad": num_quad_rows,
        "flow": num_flow_rows,
        "risk": num_risk_rows,
    }

    reqs = []
    for spec in CHART_SPECS_MASTER:
        t_row = 60 + spec["tier"] * 17
        anchor_row = t_row + 2
        n_rows = row_count_map[spec["source_table"]]
        is_bar = spec["type"] == "BAR"

        if is_bar:
            axes = [
                {"position": "LEFT_AXIS", "title": spec["x_title"]},
                {"position": "BOTTOM_AXIS", "title": spec["y_left_title"]},
            ]
        else:
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
                "targetAxis": "BOTTOM_AXIS" if is_bar else s["axis"],
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
                                "columnIndex": 0,
                            },
                            "offsetXPixels": 12,
                            "offsetYPixels": 8,
                            "widthPixels": 775,
                            "heightPixels": 344,
                        }
                    },
                }
            }
        })
    return reqs


def sync_all_sheets(cross_asset_items, risk_rows, country_rows, macro_rows, ts_matrix, quad_matrix, flow_matrix, news_list, kpi_strip, health_badge, llm_ctx, fallback_report, fallback_chart_notes):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_dash = get_or_create_ws(sh, TAB_DASH, rows=245, cols=16)
    ws_risk_news = get_or_create_ws(sh, TAB_RISK_NEWS, rows=600, cols=10)
    ws_ts_calc = get_or_create_ws(sh, TAB_TS_CALC, rows=220, cols=48)

    for old_name in OLD_TABS_TO_REMOVE:
        try:
            sh.del_worksheet(sh.worksheet(old_name))
            print(f"[INFO] 已清理冗余/僵尸工作表: {old_name}")
        except Exception:
            pass

    # ================= 1. 写入 02_底层时序与衍生运算库 =================
    ws_ts_calc.clear()
    if ts_matrix and len(ts_matrix) > 1:
        ws_ts_calc.update(range_name="A1", values=sanitize_matrix(ts_matrix), value_input_option="USER_ENTERED")
    if quad_matrix and len(quad_matrix) > 1:
        ws_ts_calc.update(range_name="AH1", values=sanitize_matrix(quad_matrix), value_input_option="USER_ENTERED")
    if flow_matrix and len(flow_matrix) > 1:
        ws_ts_calc.update(range_name="AL1", values=sanitize_matrix(flow_matrix), value_input_option="USER_ENTERED")

    sorted_risks = sorted(risk_rows, key=lambda x: x[8], reverse=True)
    risk_chart_matrix = [["系统性风险因子", "距红灯熔断线进度（%）", "黄灯警戒线（65%）"]]
    for r in sorted_risks[:12]:
        risk_chart_matrix.append([r[1].split("·")[-1], round(r[8] * 100.0, 1), 65.0])
    ws_ts_calc.update(range_name="AP1", values=sanitize_matrix(risk_chart_matrix), value_input_option="USER_ENTERED")

    # ================= 2. 抽取10张图实测基准 -> 三级 AI 深度读图 -> 自动核验 =================
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
        rn_matrix.append([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], round(r[8], 4)])

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
    ws_risk_news.update(range_name="A1", values=sanitize_matrix(rn_matrix), value_input_option="USER_ENTERED")

    # ================= 4. 全量拉取元数据并彻底清空驾驶舱旧图表（根治叠罗汉） =================
    try:
        meta_info = sh.fetch_sheet_metadata()
        del_chart_reqs = []
        for s in meta_info.get("sheets", []):
            if s.get("properties", {}).get("sheetId") == ws_dash.id:
                for ch in s.get("charts", []):
                    del_chart_reqs.append({"deleteChart": {"chartId": ch["chartId"]}})
        if del_chart_reqs:
            sh.batch_update({"requests": del_chart_reqs})
            print(f"[INFO] 已彻底清除旧图表 {len(del_chart_reqs)} 张")
    except Exception as e:
        print(f"[INFO] 清理旧图表日志: {e}")

    # ================= 5. 构建规范化 00_宏观驾驶舱_总览 =================
    TOTAL_ROWS = 232
    dash_grid = [["" for _ in range(16)] for _ in range(TOTAL_ROWS)]

    # Row 0: 主标题 + 数据健康状态
    dash_grid[0][0] = "🌍 全球宏观第一性原理自动驾驶舱（六层金字塔决策看板）"
    dash_grid[0][10] = f"{health_badge} ｜ 同步：{NOW_STR}"

    # Row 1: 顶部 4 格核心 KPI 决策速览条
    dash_grid[1][0] = kpi_strip["quad_kpi"]
    dash_grid[1][4] = kpi_strip["liq_kpi"]
    dash_grid[1][8] = kpi_strip["risk_kpi"]
    dash_grid[1][12] = kpi_strip["prob_kpi"]

    # Row 3..16: 首席宏观策略师深度总研判
    dash_grid[3][0] = "一、首席宏观策略师·六层第一性原理全因子深度总研判"
    dash_grid[4][0] = report_text

    # Row 18..35: 左右双栏（左：六层硬数据快照 | 右：36维高危风险雷达 Top 16）
    dash_grid[18][0] = "二、六层核心经济硬数据与底层公式快照"
    dash_grid[19][0:7] = ["国家/地区", "宏观层级", "核心指标与公式", "最新读数", "对比基准", "数据日期", "预警与周期信号"]
    for idx_m, m_row in enumerate(macro_rows[:16]):
        dash_grid[20 + idx_m][0:7] = m_row

    dash_grid[18][8] = "三、36维黑天鹅与灰犀牛高危预警雷达（Top 16 危急度排行）"
    dash_grid[19][8:15] = ["风险编号", "风险维度", "当前读数", "红灯熔断线", "预警状态", "距红线进度", "核心传导机制"]
    for idx_r, r in enumerate(sorted_risks[:16]):
        dash_grid[20 + idx_r][8:15] = [r[0], r[1], r[3], r[5], r[6], round(r[8], 4), r[7]]

    # Row 37..56: 左右双栏（左：跨资产定价驱动 | 右：六大板块中文新闻速览）
    dash_grid[37][0] = "四、全球跨资产实时定价与第一性原理驱动看板"
    dash_grid[38][0:7] = ["资产类别", "资产名称", "最新价格", "单日涨跌", "5日涨跌", "20日涨跌", "第一性原理定价驱动"]
    for idx_c, c_item in enumerate(cross_asset_items[:18]):
        dash_grid[39 + idx_c][0:7] = [
            c_item["cat"], c_item["name"], c_item["price"],
            c_item["pct_1d"], c_item["pct_5d"], c_item["pct_20d"], c_item["driver"]
        ]

    dash_grid[37][8] = "五、今日全球六大板块核心财经新闻与摘要速览（100%中文）"
    dash_grid[38][8:15] = ["所属板块", "中文新闻标题", "", "核心摘要与市场传导影响", "", "", ""]
    for idx_n, n in enumerate(news_list[:18]):
        r_ptr = 39 + idx_n
        dash_grid[r_ptr][8] = n["sector"]
        dash_grid[r_ptr][9] = n["title"]
        dash_grid[r_ptr][11] = n.get("summary", "")

    # Row 58: 金字塔图表区总标题
    dash_grid[58][0] = "六、宏观第一性原理十阶金字塔·左图右解说联动矩阵（自塔基 L1 至塔尖 L6 逐层穿透）"

    merge_and_style_reqs = [
        {
            "unmergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 0, "endRowIndex": TOTAL_ROWS, "startColumnIndex": 0, "endColumnIndex": 16}
            }
        },
        # 锁定列宽：左栏 A:G (7*115=805px) | 隔离带 H (32px) | 右栏 I:O (7*125=875px)
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 7},
                "properties": {"pixelSize": 115},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "COLUMNS", "startIndex": 7, "endIndex": 8},
                "properties": {"pixelSize": 32},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "COLUMNS", "startIndex": 8, "endIndex": 15},
                "properties": {"pixelSize": 125},
                "fields": "pixelSize"
            }
        },
        # 主标题栏样式
        {
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 10},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 10, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.11, "green": 0.18, "blue": 0.29},
                        "textFormat": {"bold": True, "fontSize": 12, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 36},
                "fields": "pixelSize"
            }
        },
    ]

    # 顶部 4 格 KPI 速览条合并与样式
    for k_start, k_end in [(0, 4), (4, 8), (8, 12), (12, 15)]:
        merge_and_style_reqs.append({
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": k_start, "endColumnIndex": k_end},
                "mergeType": "MERGE_ALL"
            }
        })
    merge_and_style_reqs.extend([
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.93, "green": 0.96, "blue": 1.0},
                        "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 0.12, "green": 0.22, "blue": 0.38}},
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment,wrapStrategy)"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 34},
                "fields": "pixelSize"
            }
        },
        # 总研判大卡片 (Row 4..16, 13行 * 28px = 364px)
        {
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 4, "endRowIndex": 17, "startColumnIndex": 0, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 4, "endRowIndex": 17, "startColumnIndex": 0, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "TOP",
                        "backgroundColor": {"red": 0.97, "green": 0.99, "blue": 1.0},
                        "textFormat": {"fontSize": 10, "foregroundColor": {"red": 0.13, "green": 0.17, "blue": 0.25}}
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,backgroundColor,textFormat)"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": 4, "endIndex": 17},
                "properties": {"pixelSize": 28},
                "fields": "pixelSize"
            }
        },
    ])

    # 板块标题条统一样式 (Rows 3, 18, 37, 58)
    for sec_r, c1, c2 in [(3, 0, 15), (18, 0, 7), (18, 8, 15), (37, 0, 7), (37, 8, 15), (58, 0, 15)]:
        merge_and_style_reqs.extend([
            {
                "mergeCells": {
                    "range": {"sheetId": ws_dash.id, "startRowIndex": sec_r, "endRowIndex": sec_r + 1, "startColumnIndex": c1, "endColumnIndex": c2},
                    "mergeType": "MERGE_ALL"
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": ws_dash.id, "startRowIndex": sec_r, "endRowIndex": sec_r + 1, "startColumnIndex": c1, "endColumnIndex": c2},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.18, "green": 0.27, "blue": 0.40},
                            "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
                            "verticalAlignment": "MIDDLE"
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)"
                }
            },
        ])

    # 表头样式 (Row 19 & Row 38)
    for hdr_r, c1, c2 in [(19, 0, 7), (19, 8, 15), (38, 0, 7), (38, 8, 15)]:
        merge_and_style_reqs.append({
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": hdr_r, "endRowIndex": hdr_r + 1, "startColumnIndex": c1, "endColumnIndex": c2},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.89, "green": 0.92, "blue": 0.97},
                        "textFormat": {"bold": True, "fontSize": 9, "foregroundColor": {"red": 0.1, "green": 0.15, "blue": 0.28}},
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)"
            }
        })

    # 第 2、3 板块行高与自动换行（杜绝截断）
    merge_and_style_reqs.extend([
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": 20, "endIndex": 36},
                "properties": {"pixelSize": 34},
                "fields": "pixelSize"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 20, "endRowIndex": 36, "startColumnIndex": 0, "endColumnIndex": 15},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE", "textFormat": {"fontSize": 9}}},
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat)"
            }
        },
        # 风险进度百分比格式
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 20, "endRowIndex": 36, "startColumnIndex": 13, "endColumnIndex": 14},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
    ])

    # 第 4、5 板块（跨资产 + 新闻）行高 42px、自动换行及百分比格式化
    merge_and_style_reqs.extend([
        {
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 38, "endRowIndex": 39, "startColumnIndex": 9, "endColumnIndex": 11},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 38, "endRowIndex": 39, "startColumnIndex": 11, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": 39, "endIndex": 57},
                "properties": {"pixelSize": 42},
                "fields": "pixelSize"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 39, "endRowIndex": 57, "startColumnIndex": 0, "endColumnIndex": 15},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE", "textFormat": {"fontSize": 9}}},
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat)"
            }
        },
        # 跨资产价格与涨跌幅格式
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 39, "endRowIndex": 57, "startColumnIndex": 2, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": 39, "endRowIndex": 57, "startColumnIndex": 3, "endColumnIndex": 6},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "+0.00%;-0.00%;0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
    ])

    for idx_n in range(18):
        r_ptr = 39 + idx_n
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

    # 第 6 板块：10 阶金字塔“左图 (A:G) + 右四维AI解说 (I:O)”横向同排卡片流
    # 每阶 17 行：T+0 阶梯横幅 | T+1 左右卡片标题 | T+2..T+15 (14行*26px=364px) 左图+右解说 | T+16 间隔空行
    for spec in CHART_SPECS_MASTER:
        t_row = 60 + spec["tier"] * 17
        card_start = t_row + 2
        card_end = t_row + 16

        dash_grid[t_row][0] = spec["tier_banner"]
        dash_grid[t_row + 1][0] = f"📊 {spec['title']}"
        dash_grid[t_row + 1][8] = "🤖 AI 四维关联读图解说（看图结论 · 横向对比 · 方向推演 · 交叉验证）"
        dash_grid[card_start][8] = chart_notes.get(spec["key"], "")

        # 阶梯横幅
        merge_and_style_reqs.extend([
            {
                "mergeCells": {
                    "range": {"sheetId": ws_dash.id, "startRowIndex": t_row, "endRowIndex": t_row + 1, "startColumnIndex": 0, "endColumnIndex": 15},
                    "mergeType": "MERGE_ALL"
                }
            },
            {
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
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": t_row, "endIndex": t_row + 1},
                    "properties": {"pixelSize": 30},
                    "fields": "pixelSize"
                }
            },
            # 图表与解说内容区：14 行 * 26px = 364px（高度 > 344px 图表，零溢出）
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": card_start, "endIndex": card_end},
                    "properties": {"pixelSize": 26},
                    "fields": "pixelSize"
                }
            },
            # 阶梯间距行：14px
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": ws_dash.id, "dimension": "ROWS", "startIndex": card_end, "endIndex": card_end + 1},
                    "properties": {"pixelSize": 14},
                    "fields": "pixelSize"
                }
            },
        ])

        border_style = {"style": "SOLID_MEDIUM", "color": {"red": 0.72, "green": 0.78, "blue": 0.86}}
        for c_start, c_end in [(0, 7), (8, 15)]:
            merge_and_style_reqs.extend([
                {
                    "mergeCells": {
                        "range": {"sheetId": ws_dash.id, "startRowIndex": t_row + 1, "endRowIndex": t_row + 2, "startColumnIndex": c_start, "endColumnIndex": c_end},
                        "mergeType": "MERGE_ALL"
                    }
                },
                {
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
                },
                {
                    "mergeCells": {
                        "range": {"sheetId": ws_dash.id, "startRowIndex": card_start, "endRowIndex": card_end, "startColumnIndex": c_start, "endColumnIndex": c_end},
                        "mergeType": "MERGE_ALL"
                    }
                },
                {
                    "updateBorders": {
                        "range": {"sheetId": ws_dash.id, "startRowIndex": t_row + 1, "endRowIndex": card_end, "startColumnIndex": c_start, "endColumnIndex": c_end},
                        "top": border_style,
                        "bottom": border_style,
                        "left": border_style,
                        "right": border_style,
                    }
                },
            ])

        # 右侧 AI 解说卡片专属文本格式
        merge_and_style_reqs.append({
            "repeatCell": {
                "range": {"sheetId": ws_dash.id, "startRowIndex": card_start, "endRowIndex": card_end, "startColumnIndex": 8, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "TOP",
                        "backgroundColor": {"red": 0.97, "green": 0.98, "blue": 0.99},
                        "textFormat": {"fontSize": 9, "foregroundColor": {"red": 0.12, "green": 0.16, "blue": 0.24}}
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,backgroundColor,textFormat)"
            }
        })

    # 红黄绿灯条件格式
    for text_val, bg_rgb in [
        ("🔴", {"red": 0.99, "green": 0.91, "blue": 0.90}),
        ("🟡", {"red": 1.0, "green": 0.97, "blue": 0.88}),
        ("🟢", {"red": 0.90, "green": 0.96, "blue": 0.92}),
    ]:
        merge_and_style_reqs.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws_dash.id, "startRowIndex": 20, "endRowIndex": 36, "startColumnIndex": 6, "endColumnIndex": 14}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": text_val}]},
                        "format": {"backgroundColor": bg_rgb, "textFormat": {"bold": True}}
                    }
                },
                "index": 0
            }
        })

    ws_dash.clear()
    ws_dash.update(range_name="A1", values=sanitize_matrix(dash_grid), value_input_option="USER_ENTERED")

    if ts_matrix and len(ts_matrix) > 2:
        chart_reqs = build_10_pyramid_charts_requests(
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
    cn_ok = fetch_china_macro_akshare(s_obs, s_dates, lt_hist)
    wb_m, imf_m, ext_v = fetch_external_apis()
    news, ai_news_ok = fetch_global_macro_news()

    health_badge = (
        f"📡 数据源：FRED {len(s_obs)}项 | 资产 {len(cross_items)}项 | "
        f"中国接口 {cn_ok}/6 | AI翻译 {'✅' if ai_news_ok else '⚠️回退'}"
    )

    risk_r, country_r, macro_r, flow_mat, quad_xy, kpi_bar, llm_ctx, fb_rep, fb_notes = analyze_all_factors(
        s_obs, s_dates, s_dmaps, wb_m, imf_m, ext_v, mkt_stats, news
    )
    ts_mat = build_clean_chart_timeseries(lt_hist)
    quad_mat = build_four_quadrant_series(s_obs, quad_xy[0], quad_xy[1])

    sync_all_sheets(
        cross_items, risk_r, country_r, macro_r,
        ts_mat, quad_mat, flow_mat, news, kpi_bar, health_badge, llm_ctx, fb_rep, fb_notes
    )
    print(f"[{NOW_STR}] 同步完成！已生成 10 阶金字塔左图右解说驾驶舱。")
