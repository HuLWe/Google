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

# ================= 1. 读取并清洗 GitHub Secrets =================
FRED_API_KEY = os.environ["FRED_API_KEY"].strip()
_raw_ss = os.environ["SPREADSHEET_ID"].strip()
_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", _raw_ss)
SPREADSHEET_ID = _match.group(1) if _match else _raw_ss.split("/")[0].split("?")[0].strip()
GCP_SA_JSON = json.loads(os.environ["GCP_SA_KEY"])

# AI 模型与中转站配置：默认使用 https://api.starwish.fit/v1 + GPT-6.2 Astra + 最高思考程度
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.starwish.fit/v1").strip().rstrip("/")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-6.2-astra").strip()
OPENAI_REASONING_EFFORT = os.environ.get("OPENAI_REASONING_EFFORT", "max").strip()  # 自动兼容 max -> high
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

TZ_CST = timezone(timedelta(hours=8))
NOW_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d %H:%M:%S")
TODAY_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d")

# 5 张核心工作表名称
TAB_DASH = "00_宏观驾驶舱_Dashboard"
TAB_MKT_MAC = "01_跨资产与宏观硬数据_MarketsAndMacro"
TAB_TS = "02_日频短周期库_TimeSeries"
TAB_RAW_LOG = "03_全量指标底表与日志_RawAndLog"
TAB_LT = "04_长期结构周期库_LongTermCycle"

LEGACY_TABS = [
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


def col_idx_to_letter(idx):
    res = ""
    idx += 1
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        res = chr(65 + rem) + res
    return res


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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json, application/xml, text/xml, */*",
    })
    return session


HTTP = create_http_session()

# ================= 2. 宏观基础指标字典 (含扩充版36维黑天鹅+灰犀牛FRED数据库) =================
FRED_SERIES = [
    # L1: 信用引擎与货币存量
    {"id": "M1SL", "country": "US", "layer": "L1-信用引擎", "freq": "M", "lt": False, "name": "美国M1存量 (十亿美元)"},
    {"id": "M2SL", "country": "US", "layer": "L1-信用引擎", "freq": "M", "lt": False, "name": "美国M2存量 (十亿美元)"},
    {"id": "TOTLL", "country": "US", "layer": "L1-信用引擎", "freq": "W", "lt": True, "name": "美国商业银行总信贷-H.8周频 (十亿美元)"},
    {"id": "BUSLOANS", "country": "US", "layer": "L1-信用引擎", "freq": "M", "lt": False, "name": "美国工商业贷款余额-H.8 (十亿美元)"},
    {"id": "DRTSCILM", "country": "US", "layer": "L1-信用引擎", "freq": "Q", "lt": False, "name": "美联储SLOOS银行收紧贷款比例 (%)"},
    {"id": "BAMLH0A0HYM2", "country": "US", "layer": "L1-信用引擎", "freq": "D", "lt": True, "name": "美国高收益债信用利差 (%)"},
    {"id": "BAMLC0A0CM", "country": "US", "layer": "L1-信用引擎", "freq": "D", "lt": False, "name": "美国投资级企业债信用利差 (%)"},
    {"id": "BAMLH0A3HYC", "country": "US", "layer": "L1-信用引擎", "freq": "D", "lt": False, "name": "美国CCC级垃圾债极端信用利差 (%)"},
    # L2: 美元水管与利率锚
    {"id": "WALCL", "country": "US", "layer": "L2-流动性管道", "freq": "W", "lt": True, "name": "美联储总资产 (百万美元)"},
    {"id": "WDTGAL", "country": "US", "layer": "L2-流动性管道", "freq": "W", "lt": True, "name": "财政部TGA账户余额-周三水平 (百万美元)"},
    {"id": "RRPONTSYD", "country": "US", "layer": "L2-流动性管道", "freq": "D", "lt": True, "name": "隔夜逆回购 ON RRP (十亿美元)"},
    {"id": "WRESBAL", "country": "US", "layer": "L2-流动性管道", "freq": "W", "lt": True, "name": "商业银行准备金余额 (百万美元)"},
    {"id": "IORB", "country": "US", "layer": "L2-流动性管道", "freq": "D", "lt": False, "name": "美联储准备金余额利率 IORB (%)"},
    {"id": "DFF", "country": "US", "layer": "L2-利率之锚", "freq": "D", "lt": True, "name": "联邦基金有效利率 EFFR (%)"},
    {"id": "DGS2", "country": "US", "layer": "L2-利率之锚", "freq": "D", "lt": True, "name": "美国2年期国债收益率 (%)"},
    {"id": "DGS10", "country": "US", "layer": "L2-利率之锚", "freq": "D", "lt": True, "name": "美国10年期国债收益率 (%)"},
    {"id": "DGS30", "country": "US", "layer": "L2-利率之锚", "freq": "D", "lt": False, "name": "美国30年期国债收益率 (%)"},
    {"id": "DFII10", "country": "US", "layer": "L2-利率之锚", "freq": "D", "lt": True, "name": "美国10Y TIPS实际利率 (%)"},
    {"id": "T10Y2Y", "country": "US", "layer": "L2-利率之锚", "freq": "D", "lt": True, "name": "美债10Y-2Y期限利差 (%)"},
    {"id": "T10Y3M", "country": "US", "layer": "L2-利率之锚", "freq": "D", "lt": False, "name": "美债10Y-3M经典衰退利差 (%)"},
    {"id": "IRLTLT01JPM156N", "country": "JP", "layer": "L2-利率之锚", "freq": "M", "lt": False, "name": "日本10年期国债收益率 (%)"},
    {"id": "IRLTLT01DEM156N", "country": "DE", "layer": "L2-利率之锚", "freq": "M", "lt": False, "name": "德国(欧元区)10Y国债收益率 (%)"},
    # L3: 美国经济硬数据与财政生死线
    {"id": "PAYEMS", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "lt": False, "name": "美国非农就业总人数 (千人)"},
    {"id": "ICSA", "country": "US", "layer": "L3-就业硬数据", "freq": "W", "lt": True, "name": "美国周度初请失业金人数 (人)"},
    {"id": "CCSA", "country": "US", "layer": "L3-就业硬数据", "freq": "W", "lt": False, "name": "美国续请失业金人数 (人)"},
    {"id": "UNRATE", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "lt": False, "name": "美国失业率 (%)"},
    {"id": "SAHMREALTIME", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "lt": False, "name": "官方萨姆规则衰退指标 (%)"},
    {"id": "CES0500000003", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "lt": False, "name": "美国私营部门平均时薪 (美元/小时)"},
    {"id": "INDPRO", "country": "US", "layer": "L3-工业与消费", "freq": "M", "lt": False, "name": "美国工业生产总指数 (2017=100)"},
    {"id": "RSAFS", "country": "US", "layer": "L3-工业与消费", "freq": "M", "lt": False, "name": "美国零售与食品服务销售额 (百万美元)"},
    {"id": "UMCSENT", "country": "US", "layer": "L3-工业与消费", "freq": "M", "lt": False, "name": "密歇根大学消费者信心指数"},
    {"id": "CPIAUCSL", "country": "US", "layer": "L3-通胀硬数据", "freq": "M", "lt": False, "name": "美国CPI总体物价指数"},
    {"id": "PCEPILFE", "country": "US", "layer": "L3-通胀硬数据", "freq": "M", "lt": False, "name": "美国核心PCE物价指数"},
    {"id": "PPIACO", "country": "US", "layer": "L3-通胀硬数据", "freq": "M", "lt": False, "name": "美国PPI全商品生产者物价指数"},
    {"id": "MTSDS133FMS", "country": "US", "layer": "L3-财政生死线", "freq": "M", "lt": False, "name": "美国联邦月度财政盈余/赤字 (百万美元)"},
    {"id": "A091RC1Q027SBEA", "country": "US", "layer": "L3-财政生死线", "freq": "Q", "lt": False, "name": "美国联邦政府利息支出年率 (十亿美元)"},
    {"id": "FGRECPT", "country": "US", "layer": "L3-财政生死线", "freq": "Q", "lt": False, "name": "美国联邦政府经常性财政收入年率 (十亿美元)"},
    {"id": "GDP", "country": "US", "layer": "L3-财政生死线", "freq": "Q", "lt": False, "name": "美国名义GDP年率 (十亿美元)"},
    {"id": "GFDEGDQ188S", "country": "US", "layer": "L3-财政生死线", "freq": "Q", "lt": False, "name": "美国联邦总债务占GDP比重 (%)"},
    # L3: 欧元区与日本宏观硬数据
    {"id": "CP0000EZ19M086NEST", "country": "EA", "layer": "L3-通胀硬数据", "freq": "M", "lt": False, "name": "欧元区调和CPI物价指数 (HICP)"},
    {"id": "LRHUTTTTEZM156S", "country": "EA", "layer": "L3-就业硬数据", "freq": "M", "lt": False, "name": "欧元区失业率 (%)"},
    {"id": "JPNPRMNTO01GPSAM", "country": "JP", "layer": "L3-工业与消费", "freq": "M", "lt": False, "name": "日本制造业生产环比增速 (%)"},
    {"id": "JPNCPIALLMINMEI", "country": "JP", "layer": "L3-通胀硬数据", "freq": "M", "lt": False, "name": "日本全国CPI物价指数"},
    # L4/L5: 跨境资金流量与实物库存
    {"id": "DTWEXBGS", "country": "US", "layer": "L4-跨境汇率", "freq": "D", "lt": False, "name": "美联储广义贸易加权美元指数"},
    {"id": "FDHBFIN", "country": "US", "layer": "L4-跨境资本流", "freq": "Q", "lt": False, "name": "海外投资者持有美债总额-TIC (十亿美元)"},
    {"id": "WCESTUS1", "country": "US", "layer": "L5-实物库存", "freq": "W", "lt": True, "name": "EIA美国商业原油库存-不含SPR (千桶)"},
    # L6: 黑天鹅 + 灰犀牛专属风险数据库 (扩充版)
    {"id": "VXVCLS", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "D", "lt": False, "name": "CBOE 3个月期VIX3M指数"},
    {"id": "STLFSI4", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "lt": True, "name": "圣路易斯联储金融压力指数"},
    {"id": "NFCI", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "lt": False, "name": "芝加哥联储金融状况指数"},
    {"id": "ANFCI", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "lt": False, "name": "芝加哥联储调整后金融状况指数"},
    {"id": "DISBORROW", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "lt": False, "name": "美联储贴现窗口紧急借款总额 (百万美元)"},
    {"id": "TEDRATE", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "D", "lt": False, "name": "TED离岸美元拆借信用利差 (%)"},
    {"id": "DRCRELEXFACBS", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国商业地产(CRE)银行贷款违约率 (%)"},
    {"id": "DRCCLACBS", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国商业银行信用卡贷款违约率 (%)"},
    {"id": "DRSFRMACBS", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国居民住房抵押贷款违约率 (%)"},
    {"id": "DRALACBS", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国商业银行全部贷款总违约率 (%)"},
    {"id": "PSAVERT", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "M", "lt": False, "name": "美国个人储蓄率占可支配收入比 (%)"},
    {"id": "TDSP", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国家庭债务偿付占可支配收入比 (%)"},
    {"id": "DPSACBW027SBOG", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "W", "lt": False, "name": "美国商业银行总存款余额 (十亿美元)"},
    {"id": "MMMFFAQ027S", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国货币市场基金总资产规模 (百万美元)"},
]

DAILY_WEEKLY_FRED_IDS = {x["id"] for x in FRED_SERIES if x["freq"] in ("D", "W")}
LT_FRED_IDS = {x["id"] for x in FRED_SERIES if x.get("lt")}

# ================= 3. 全球跨资产实时行情字典 (新增费城半导体等风险锚) =================
MARKET_ASSETS = [
    {"cat": "重要股指", "ticker": "^GSPC", "code": "SPX", "country": "US", "lt": True, "name": "标普500指数 (S&P 500)", "driver": "美股盈利预期 × 10Y贴现率"},
    {"cat": "重要股指", "ticker": "^NDX", "code": "NDX", "country": "US", "lt": True, "name": "纳斯达克100指数 (Nasdaq 100)", "driver": "远期现金流久期资产，对10Y TIPS极敏感"},
    {"cat": "重要股指", "ticker": "^SOX", "code": "SOX", "country": "US", "lt": False, "name": "费城半导体指数 (SOX-AI泡沫温度计)", "driver": "全球AI资本开支与半导体周期景气度核心锚"},
    {"cat": "重要股指", "ticker": "000300.SS", "code": "CSI300", "country": "CN", "lt": True, "name": "沪深300指数 (CSI 300)", "driver": "中国社融脉冲与M1-M2活化剪刀差"},
    {"cat": "重要股指", "ticker": "^HSI", "code": "HSI", "country": "HK", "lt": False, "name": "香港恒生指数 (Hang Seng)", "driver": "中国基本面分子 × 美元流动性分母双重定价"},
    {"cat": "重要股指", "ticker": "^N225", "code": "N225", "country": "JP", "lt": False, "name": "日经225指数 (Nikkei 225)", "driver": "日元汇率与日本央行加息路径"},
    {"cat": "重要股指", "ticker": "^GDAXI", "code": "DAX", "country": "DE", "lt": False, "name": "德国DAX指数", "driver": "欧元区制造业周期与欧央行利率路径"},
    {"cat": "重要股指", "ticker": "EEM", "code": "EEM", "country": "EM", "lt": True, "name": "MSCI新兴市场ETF", "driver": "强美元周期与跨境热钱风险偏好"},
    {"cat": "风险晴雨表", "ticker": "KRE", "code": "US_REG_BANK", "country": "US", "lt": False, "name": "美国区域银行ETF (KRE-商业地产灰犀牛)", "driver": "美国中小银行商业地产敞口与存款稳定性实时温度计"},
    {"cat": "风险晴雨表", "ticker": "HYG", "code": "US_HY_ETF", "country": "US", "lt": False, "name": "美国高收益企业债ETF (HYG)", "driver": "企业信用利差与垃圾债再融资流动性实时定价"},
    {"cat": "风险晴雨表", "ticker": "TLT", "code": "US_20Y_BOND", "country": "US", "lt": False, "name": "20年以上美国国债ETF (TLT)", "driver": "长端美债久期风险与主权信用折价温度计"},
    {"cat": "全球工业", "ticker": "EXI", "code": "GLOBAL_IND", "country": "GLOBAL", "lt": False, "name": "iShares全球工业指数ETF (EXI)", "driver": "全球跨国制造业资本开支与贸易景气度"},
    {"cat": "全球工业", "ticker": "XLI", "code": "US_IND_ETF", "country": "US", "lt": False, "name": "美国工业板块指数ETF (XLI)", "driver": "北美基建回流与制造业PMI市场领先指标"},
    {"cat": "全球货币", "ticker": "DX-Y.NYB", "code": "DXY", "country": "US", "lt": True, "name": "ICE美元指数 (DXY)", "driver": "全球离岸美元松紧总阀门"},
    {"cat": "全球货币", "ticker": "JPY=X", "code": "USDJPY", "country": "JP", "lt": True, "name": "美元兑日元 (USD/JPY)", "driver": "全球套息交易(Carry Trade)核心风向标"},
    {"cat": "全球货币", "ticker": "CNH=X", "code": "USDCNH", "country": "CN", "lt": True, "name": "美元兑离岸人民币 (USD/CNH)", "driver": "中美利差与出口企业结汇意愿"},
    {"cat": "全球货币", "ticker": "EURUSD=X", "code": "EURUSD", "country": "EA", "lt": False, "name": "欧元兑美元 (EUR/USD)", "driver": "欧美真实利差与能源贸易条件"},
    {"cat": "全球货币", "ticker": "GBPUSD=X", "code": "GBPUSD", "country": "UK", "lt": False, "name": "英镑兑美元 (GBP/USD)", "driver": "英美利差与英国财政信用溢价"},
    {"cat": "全球货币", "ticker": "AUDUSD=X", "code": "AUDUSD", "country": "AU", "lt": False, "name": "澳元兑美元 (AUD/USD)", "driver": "全球工业大宗商品周期与亚洲需求"},
    {"cat": "黄金贵金属", "ticker": "GC=F", "code": "GOLD", "country": "GLOBAL", "lt": True, "name": "COMEX黄金期货 (美元/盎司)", "driver": "10Y TIPS实际利率 + 央行去美元化购金"},
    {"cat": "黄金贵金属", "ticker": "SI=F", "code": "SILVER", "country": "GLOBAL", "lt": False, "name": "COMEX白银期货 (美元/盎司)", "driver": "金融属性(随黄金) + 光伏/工业需求弹性"},
    {"cat": "大宗商品", "ticker": "CL=F", "code": "WTI", "country": "GLOBAL", "lt": False, "name": "WTI原油期货 (美元/桶)", "driver": "北美现货库存与OPEC+供给约束"},
    {"cat": "大宗商品", "ticker": "BZ=F", "code": "BRENT", "country": "GLOBAL", "lt": True, "name": "布伦特原油期货 (美元/桶)", "driver": "全球通胀预期与供给冲击滞胀扳机"},
    {"cat": "大宗商品", "ticker": "HG=F", "code": "COPPER", "country": "GLOBAL", "lt": True, "name": "COMEX精炼铜 (美元/磅)", "driver": "全球制造业与电网AI基建实体需求"},
    {"cat": "大宗商品", "ticker": "NG=F", "code": "NATGAS", "country": "US", "lt": False, "name": "NYMEX天然气期货", "driver": "欧美能源成本与季节性库存周期"},
    {"cat": "大宗商品", "ticker": "ZW=F", "code": "WHEAT", "country": "GLOBAL", "lt": False, "name": "CBOT小麦期货 (美分/蒲式耳)", "driver": "全球食品通胀与新兴市场社会稳定"},
    {"cat": "波动率", "ticker": "^VIX", "code": "VIX", "country": "US", "lt": True, "name": "CBOE VIX恐慌指数", "driver": "美股期权隐含尾部对冲成本"},
]

CRYPTO_ASSETS = [
    {"cg_id": "bitcoin", "yf_ticker": "BTC-USD", "code": "BTC", "lt": True, "name": "比特币 (Bitcoin)", "driver": "全球美元净流动性高弹性放大器 + 现货ETF资金流"},
    {"cg_id": "ethereum", "yf_ticker": "ETH-USD", "code": "ETH", "lt": False, "name": "以太坊 (Ethereum)", "driver": "链上金融流动性与风险偏好Beta"},
    {"cg_id": "solana", "yf_ticker": "SOL-USD", "code": "SOL", "lt": False, "name": "Solana (SOL)", "driver": "高Beta加密投机情绪与动物精神因子"},
]

COUNTRIES_LIST = ["USA", "CHN", "JPN", "DEU", "IND", "BRA", "IDN", "TUR"]


def fetch_global_macro_news():
    """零成本抓取全球顶级财经与央行 RSS 头条，用于提升 GPT-6.2 Astra 跨因子推演准确率"""
    rss_sources = [
        ("CNBC宏观", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258"),
        ("CNBC金融", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664"),
        ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
        ("美联储官方", "https://www.federalreserve.gov/feeds/press_all.xml"),
    ]
    macro_keywords = re.compile(
        r"(fed|powell|rate|inflation|cpi|pce|treasury|yield|bond|gdp|payroll|jobs|recession|"
        r"china|pboc|tariff|trade|oil|opec|crude|gold|dollar|yen|boj|ecb|bank|credit|debt|deficit|liquidity|default|commercial real estate)",
        re.IGNORECASE
    )
    collected = []
    seen_titles = set()

    for src_name, url in rss_sources:
        try:
            resp = HTTP.get(url, timeout=(6, 15))
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:15]:
                title_el = item.find("title")
                pub_el = item.find("pubDate")
                if title_el is None or not title_el.text:
                    continue
                title = re.sub(r"\s+", " ", title_el.text).strip()
                norm_key = title.lower()[:55]
                if norm_key in seen_titles:
                    continue
                if src_name == "美联储官方" or macro_keywords.search(title):
                    seen_titles.add(norm_key)
                    pub_str = (pub_el.text or "")[:16].strip() if pub_el is not None else TODAY_STR
                    collected.append({
                        "source": src_name,
                        "time": pub_str,
                        "title": title[:130],
                    })
                    if len(collected) >= 15:
                        break
        except Exception as e:
            print(f"[WARN] RSS {src_name} failed: {e}")
        if len(collected) >= 15:
            break

    return collected[:12]


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


def fetch_market_assets(series_obs, series_hist_dates, lt_hist_dates):
    raw_records = []
    cross_asset_items = []
    market_stats = {}

    def calc_pct(arr, idx):
        if len(arr) > idx and arr[idx] != 0:
            return round((arr[0] / arr[idx] - 1) * 100, 2)
        return None

    for item in MARKET_ASSETS:
        dt_str, prices, date_val_map = fetch_yahoo_chart(item["ticker"], "1mo", "1d")
        if not prices:
            print(f"[WARN] Market asset {item['ticker']} unavailable")
            continue
        p0 = round(prices[0], 4)
        p1 = round(prices[1], 4) if len(prices) > 1 else p0
        pct_1d = calc_pct(prices, 1) or 0.0
        pct_5d = calc_pct(prices, 5)
        pct_20d = calc_pct(prices, min(20, len(prices) - 1))

        series_obs[item["code"]] = prices
        series_hist_dates[item["code"]] = date_val_map
        market_stats[item["code"]] = {
            "price": p0, "pct_1d": pct_1d, "pct_5d": pct_5d, "pct_20d": pct_20d
        }

        if item.get("lt"):
            _, _, lt_map = fetch_yahoo_chart(item["ticker"], "3y", "1wk")
            if lt_map:
                lt_hist_dates[item["code"]] = lt_map

        raw_records.append({
            "观测日期": dt_str, "国家/地区": item["country"], "六层归属": f"L5-{item['cat']}",
            "指标代码": item["code"], "指标名称": item["name"],
            "最新值": p0, "前值": p1, "边际变化": round(p0 - p1, 4),
            "数据源": "Yahoo Finance", "同步时间": NOW_STR,
        })
        cross_asset_items.append({
            "cat": item["cat"], "code": item["code"], "name": item["name"],
            "price": p0, "pct_1d": pct_1d / 100.0,
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
    except Exception as e:
        print(f"[WARN] CoinGecko primary failed: {e}")

    for c_item in CRYPTO_ASSETS:
        _, yf_prices, yf_date_map = fetch_yahoo_chart(c_item["yf_ticker"], "1mo", "1d")
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
            p1 = round(p0 / (1 + pct_1d / 100.0), 2) if pct_1d != -100 else p0
            src_name = "CoinGecko API"
            dt_str = TODAY_STR
            yf_date_map[TODAY_STR] = p0
            series_obs[c_item["code"]] = yf_prices if yf_prices else [p0, p1]
            series_hist_dates[c_item["code"]] = yf_date_map
        elif yf_prices:
            dt_str = TODAY_STR
            p0 = round(yf_prices[0], 2)
            p1 = round(yf_prices[1], 2)
            pct_1d = calc_pct(yf_prices, 1) or 0.0
            pct_5d = calc_pct(yf_prices, 5)
            pct_20d = calc_pct(yf_prices, min(20, len(yf_prices) - 1))
            src_name = "Yahoo Finance"
            series_obs[c_item["code"]] = yf_prices
            series_hist_dates[c_item["code"]] = yf_date_map
        else:
            continue

        market_stats[c_item["code"]] = {
            "price": p0, "pct_1d": pct_1d, "pct_5d": pct_5d, "pct_20d": pct_20d
        }
        raw_records.append({
            "观测日期": dt_str, "国家/地区": "GLOBAL", "六层归属": "L5-加密资产",
            "指标代码": c_item["code"], "指标名称": c_item["name"],
            "最新值": p0, "前值": p1, "边际变化": round(p0 - p1, 2),
            "数据源": src_name, "同步时间": NOW_STR,
        })
        cross_asset_items.append({
            "cat": "加密资产", "code": c_item["code"], "name": c_item["name"],
            "price": p0, "pct_1d": pct_1d / 100.0,
            "pct_5d": (pct_5d / 100.0) if pct_5d is not None else "",
            "pct_20d": (pct_20d / 100.0) if pct_20d is not None else "",
            "driver": c_item["driver"]
        })

    spy_pe = fetch_spy_pe_proxy()
    if spy_pe:
        series_obs["SPX_PE"] = [spy_pe]
        raw_records.append({
            "观测日期": TODAY_STR, "国家/地区": "US", "六层归属": "L5-估值锚",
            "指标代码": "SPX_PE", "指标名称": "标普500指数代理市盈率 (SPY PE)",
            "最新值": spy_pe, "前值": spy_pe, "边际变化": 0.0,
            "数据源": "Yahoo Finance", "同步时间": NOW_STR,
        })

    return raw_records, cross_asset_items, market_stats


def fetch_fred_data():
    records = []
    series_obs = {}
    series_dates = {}
    series_hist_dates = {}
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
            if item["id"] in DAILY_WEEKLY_FRED_IDS:
                series_hist_dates[item["id"]] = {o["date"]: float(o["value"]) for o in obs[:30]}
            if item.get("lt"):
                lt_hist_dates[item["id"]] = {o["date"]: float(o["value"]) for o in obs}

            val_curr = vals[0]
            val_prev = vals[1] if len(vals) > 1 else val_curr
            records.append({
                "观测日期": obs[0]["date"], "国家/地区": item["country"], "六层归属": item["layer"],
                "指标代码": item["id"], "指标名称": item["name"],
                "最新值": round(val_curr, 4), "前值": round(val_prev, 4), "边际变化": round(val_curr - val_prev, 4),
                "数据源": "FRED API", "同步时间": NOW_STR,
            })
            time.sleep(0.05)
        except Exception as e:
            print(f"[WARN] FRED {item['id']} failed: {e}")
    return records, series_obs, series_dates, series_hist_dates, lt_hist_dates


def fetch_china_macro_akshare(series_obs, series_dates):
    records = []
    try:
        import akshare as ak
    except Exception as e:
        print(f"[WARN] AkShare import failed: {e}")
        return records

    def add_cn_rec(dt_str, layer, code, name, curr, prev):
        series_obs[code] = [curr, prev]
        series_dates[code] = dt_str
        records.append({
            "观测日期": dt_str, "国家/地区": "CN", "六层归属": layer,
            "指标代码": code, "指标名称": name,
            "最新值": round(curr, 2), "前值": round(prev, 2), "边际变化": round(curr - prev, 2),
            "数据源": "NBS / PBOC (AkShare)", "同步时间": NOW_STR,
        })

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
        col_non = next((c for c in df_pmi.columns if "非制造业" in str(c) and "指数" in str(c)), None)
        if col_mfg:
            res_mfg = get_latest_two(df_pmi, col_dt, col_mfg)
            if res_mfg:
                add_cn_rec(res_mfg[0], "L3-工业与消费", "CN_PMI_MFG", "中国官方制造业PMI (荣枯线=50)", res_mfg[1], res_mfg[2])
        if col_non:
            res_non = get_latest_two(df_pmi, col_dt, col_non)
            if res_non:
                add_cn_rec(res_non[0], "L3-工业与消费", "CN_PMI_NONMFG", "中国官方非制造业PMI (荣枯线=50)", res_non[1], res_non[2])
    except Exception as e:
        print(f"[WARN] CN PMI failed: {e}")

    try:
        df_ms = ak.macro_china_money_supply()
        col_dt = next((c for c in df_ms.columns if "月份" in str(c) or "日期" in str(c)), df_ms.columns[0])
        col_m1_yoy = next((c for c in df_ms.columns if "M1" in str(c) and "同比" in str(c)), None)
        col_m2_yoy = next((c for c in df_ms.columns if "M2" in str(c) and "同比" in str(c)), None)
        if col_m1_yoy:
            res_m1 = get_latest_two(df_ms, col_dt, col_m1_yoy)
            if res_m1:
                add_cn_rec(res_m1[0], "L1-信用引擎", "CN_M1_YOY", "中国M1同比增速-新口径 (%)", res_m1[1], res_m1[2])
        if col_m2_yoy:
            res_m2 = get_latest_two(df_ms, col_dt, col_m2_yoy)
            if res_m2:
                add_cn_rec(res_m2[0], "L1-信用引擎", "CN_M2_YOY", "中国M2同比增速 (%)", res_m2[1], res_m2[2])
    except Exception as e:
        print(f"[WARN] CN Money Supply failed: {e}")

    try:
        fn_ind = getattr(ak, "macro_china_industrial_value_added", None) or getattr(ak, "macro_china_gyzjz")
        df_ind = fn_ind()
        col_dt = next((c for c in df_ind.columns if "月份" in str(c) or "日期" in str(c)), df_ind.columns[0])
        col_yoy = next(
            (c for c in df_ind.columns if "同比" in str(c) or "今值" in str(c) or "最新" in str(c)),
            df_ind.columns[1] if len(df_ind.columns) > 1 else None
        )
        if col_yoy:
            res_ind = get_latest_two(df_ind, col_dt, col_yoy)
            if res_ind:
                add_cn_rec(res_ind[0], "L3-工业与消费", "CN_IND_YOY", "中国规模以上工业增加值同比 (%)", res_ind[1], res_ind[2])
    except Exception as e:
        print(f"[WARN] CN Industrial failed: {e}")

    try:
        fn_tsf = getattr(ak, "macro_china_social_financing", None) or getattr(ak, "macro_china_shrzgm")
        df_tsf = fn_tsf()
        col_dt = next((c for c in df_tsf.columns if "月份" in str(c) or "日期" in str(c)), df_tsf.columns[0])
        col_val = next(
            (c for c in df_tsf.columns if ("社会融资" in str(c) or "增量" in str(c) or "总计" in str(c)) and "同比" not in str(c) and "环比" not in str(c)),
            df_tsf.columns[1] if len(df_tsf.columns) > 1 else None
        )
        if col_val:
            res_tsf = get_latest_two(df_tsf, col_dt, col_val)
            if res_tsf:
                add_cn_rec(res_tsf[0], "L1-信用引擎", "CN_TSF_NEW", "中国社会融资规模增量 (亿元)", res_tsf[1], res_tsf[2])
    except Exception as e:
        print(f"[WARN] CN TSF failed: {e}")

    try:
        df_cpi = ak.macro_china_cpi()
        col_dt = next((c for c in df_cpi.columns if "月份" in str(c) or "日期" in str(c)), df_cpi.columns[0])
        col_yoy = next(
            (c for c in df_cpi.columns if "全国" in str(c) and "同比" in str(c)),
            next((c for c in df_cpi.columns if "同比" in str(c)), None)
        )
        if col_yoy:
            res_cpi = get_latest_two(df_cpi, col_dt, col_yoy)
            if res_cpi:
                add_cn_rec(res_cpi[0], "L3-通胀硬数据", "CN_CPI_YOY", "中国CPI同比增速 (%)", res_cpi[1], res_cpi[2])
    except Exception as e:
        print(f"[WARN] CN CPI failed: {e}")

    try:
        df_ppi = ak.macro_china_ppi()
        col_dt = next((c for c in df_ppi.columns if "月份" in str(c) or "日期" in str(c)), df_ppi.columns[0])
        col_yoy = next((c for c in df_ppi.columns if "同比" in str(c) or "今值" in str(c)), None)
        if col_yoy:
            res_ppi = get_latest_two(df_ppi, col_dt, col_yoy)
            if res_ppi:
                add_cn_rec(res_ppi[0], "L3-通胀硬数据", "CN_PPI_YOY", "中国PPI工业品出厂价格同比 (%)", res_ppi[1], res_ppi[2])
    except Exception as e:
        print(f"[WARN] CN PPI failed: {e}")

    return records


def fetch_external_apis(series_obs, series_dates):
    records = []
    wb_metrics = {}
    imf_metrics = {}
    extra_vals = {}

    try:
        import akshare as ak
        df_bond = ak.bond_zh_us_yield_em() if hasattr(ak, "bond_zh_us_yield_em") else ak.bond_zh_us_rate(start_date="20250101")
        col_10y = next((c for c in df_bond.columns if "中国" in str(c) and "10年" in str(c)), None)
        col_2y = next((c for c in df_bond.columns if "中国" in str(c) and "2年" in str(c)), None)
        col_dt = next((c for c in df_bond.columns if "日期" in str(c)), df_bond.columns[0])
        if col_10y and col_2y:
            df_clean = df_bond.dropna(subset=[col_10y, col_2y]).sort_values(by=col_dt)
            if len(df_clean) >= 2:
                latest_row, prev_row = df_clean.iloc[-1], df_clean.iloc[-2]
                dt_str = str(latest_row[col_dt])[:10]
                cn10_c, cn10_p = float(latest_row[col_10y]), float(prev_row[col_10y])
                cn2_c, cn2_p = float(latest_row[col_2y]), float(prev_row[col_2y])
                series_obs["CN_DGS10"] = [cn10_c, cn10_p]
                series_obs["CN_DGS2"] = [cn2_c, cn2_p]
                series_dates["CN_DGS10"] = dt_str
                records.append({
                    "观测日期": dt_str, "国家/地区": "CN", "六层归属": "L2-利率之锚",
                    "指标代码": "CN_DGS10", "指标名称": "中国10年期国债收益率-日频 (%)",
                    "最新值": round(cn10_c, 4), "前值": round(cn10_p, 4), "边际变化": round(cn10_c - cn10_p, 4),
                    "数据源": "ChinaBond / AkShare", "同步时间": NOW_STR,
                })
                records.append({
                    "观测日期": dt_str, "国家/地区": "CN", "六层归属": "L2-利率之锚",
                    "指标代码": "CN_DGS2", "指标名称": "中国2年期国债收益率-日频 (%)",
                    "最新值": round(cn2_c, 4), "前值": round(cn2_p, 4), "边际变化": round(cn2_c - cn2_p, 4),
                    "数据源": "ChinaBond / AkShare", "同步时间": NOW_STR,
                })
    except Exception as e:
        print(f"[WARN] ChinaBond AkShare failed: {e}")

    try:
        resp = HTTP.get("https://markets.newyorkfed.org/api/rates/secured/sofr/last/2.json", timeout=(10, 30))
        resp.raise_for_status()
        sofr = resp.json().get("refRates", [])
        if len(sofr) >= 2:
            c, p = float(sofr[0]["percentRate"]), float(sofr[1]["percentRate"])
            extra_vals["SOFR"] = c
            series_obs["NYFED_SOFR"] = [c, p]
            records.append({
                "观测日期": sofr[0]["effectiveDate"], "国家/地区": "US", "六层归属": "L2-流动性管道",
                "指标代码": "NYFED_SOFR", "指标名称": "纽约联储 SOFR 隔夜利率 (%)",
                "最新值": c, "前值": p, "边际变化": round(c - p, 4),
                "数据源": "NY Fed API", "同步时间": NOW_STR,
            })
    except Exception as e:
        print(f"[WARN] NYFed failed: {e}")

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
                bid_to_cover = float(auc.get("bid_to_cover_ratio") or 0)
                high_yield = float(auc.get("high_yield") or 0)
                median_yield = float(auc.get("median_yield") or high_yield)
                tail_bps = round(max(0.0, (high_yield - median_yield) * 100), 2)
                extra_vals["UST_BID_COVER"] = bid_to_cover
                extra_vals["UST_TAIL_BPS"] = tail_bps
                series_obs["UST_AUCTION_BTC"] = [bid_to_cover]
                series_obs["UST_TAIL_BPS"] = [tail_bps]
                records.append({
                    "观测日期": auc.get("record_date", TODAY_STR), "国家/地区": "US", "六层归属": "L6-黑天鹅雷达",
                    "指标代码": "UST_AUCTION_BTC", "指标名称": f"美债({auc.get('security_term')})拍卖投标倍数",
                    "最新值": bid_to_cover, "前值": high_yield, "边际变化": 0.0,
                    "数据源": "US Treasury API", "同步时间": NOW_STR,
                })
                records.append({
                    "观测日期": auc.get("record_date", TODAY_STR), "国家/地区": "US", "六层归属": "L3-财政生死线",
                    "指标代码": "UST_TAIL_BPS", "指标名称": f"美债({auc.get('security_term')})拍卖尾部利差 (bps)",
                    "最新值": tail_bps, "前值": 0.0, "边际变化": tail_bps,
                    "数据源": "US Treasury API", "同步时间": NOW_STR,
                })
    except Exception as e:
        print(f"[WARN] Treasury Auction failed: {e}")

    year = datetime.now().year
    imf_codes = {
        "NGDP_RPCH": ("L3-周期象限", "实际GDP年增速 (%)"),
        "PCPIPCH": ("L3-周期象限", "CPI通胀率 (%)"),
        "BCA_NGDPD": ("L4-跨境汇率", "经常账户占GDP比重 (%)"),
        "GGXCNL_NGDP": ("L3-周期象限", "财政净借贷占GDP比重 (%)"),
    }
    for code, (layer, name) in imf_codes.items():
        try:
            url = f"https://www.imf.org/external/datamapper/api/v1/{code}/{'/'.join(COUNTRIES_LIST)}?periods={year-1},{year}"
            vals = HTTP.get(url, timeout=(10, 35)).json().get("values", {}).get(code, {})
            for c in COUNTRIES_LIST:
                if c in vals:
                    curr = vals[c].get(str(year)) or vals[c].get(str(year - 1))
                    prev = vals[c].get(str(year - 1)) or curr
                    if curr is not None:
                        imf_metrics[(c, code)] = round(float(curr), 2)
                        records.append({
                            "观测日期": f"{year}-12-31", "国家/地区": c, "六层归属": layer,
                            "指标代码": f"IMF_{code}", "指标名称": name,
                            "最新值": round(float(curr), 2), "前值": round(float(prev), 2),
                            "边际变化": round(float(curr) - float(prev), 2),
                            "数据源": "IMF DataMapper", "同步时间": NOW_STR,
                        })
        except Exception as e:
            print(f"[WARN] IMF {code} failed: {e}")

    wb_codes = {"FI.RES.TOTL.CD": "官方外汇储备总额 (美元)", "DT.DOD.DSTC.CD": "短期外债余额 (美元)"}
    for code, name in wb_codes.items():
        try:
            url = f"https://api.worldbank.org/v2/country/{';'.join(COUNTRIES_LIST)}/indicator/{code}?format=json&mrv=2"
            data = HTTP.get(url, timeout=(10, 45)).json()
            if len(data) > 1 and data[1]:
                seen = {}
                for row in data[1]:
                    iso3 = row["countryiso3code"]
                    if row["value"] is not None:
                        seen.setdefault(iso3, []).append((row["date"], float(row["value"])))
                for iso3, pts in seen.items():
                    c_date, c_val = pts[0]
                    p_val = pts[1][1] if len(pts) > 1 else c_val
                    wb_metrics[(iso3, code)] = c_val
                    records.append({
                        "观测日期": f"{c_date}-12-31", "国家/地区": iso3, "六层归属": "L4-跨境汇率",
                        "指标代码": f"WB_{code}", "指标名称": name,
                        "最新值": round(c_val, 2), "前值": round(p_val, 2),
                        "边际变化": round(c_val - p_val, 2),
                        "数据源": "World Bank API", "同步时间": NOW_STR,
                    })
        except Exception as e:
            print(f"[WARN] WorldBank {code} failed: {e}")

    return records, wb_metrics, imf_metrics, extra_vals


def enrich_derived_rows(row_dict):
    if all(k in row_dict for k in ["WALCL", "WDTGAL", "RRPONTSYD"]):
        row_dict["NET_LIQUIDITY"] = round((row_dict["WALCL"] - row_dict["WDTGAL"]) / 1000.0 - row_dict["RRPONTSYD"], 2)
    if "WALCL" in row_dict:
        row_dict["WALCL_B"] = round(row_dict["WALCL"] / 1000.0, 2)
    if "WDTGAL" in row_dict:
        row_dict["TGA_B"] = round(row_dict["WDTGAL"] / 1000.0, 2)
    if all(k in row_dict for k in ["DGS10", "DGS2"]):
        row_dict["SPREAD_10Y2Y"] = round(row_dict["DGS10"] - row_dict["DGS2"], 3)
    if all(k in row_dict for k in ["DGS10", "DFII10"]):
        row_dict["BREAKEVEN_10Y"] = round(row_dict["DGS10"] - row_dict["DFII10"], 3)
    if all(k in row_dict for k in ["COPPER", "GOLD"]) and row_dict["GOLD"] > 0:
        row_dict["COPPER_GOLD_RATIO"] = round((row_dict["COPPER"] * 1000) / row_dict["GOLD"], 3)
    if all(k in row_dict for k in ["GOLD", "BRENT"]) and row_dict["BRENT"] > 0:
        row_dict["GOLD_OIL_RATIO"] = round(row_dict["GOLD"] / row_dict["BRENT"], 2)
    if "WCESTUS1" in row_dict:
        row_dict["EIA_CRUDE_MBBL"] = round(row_dict["WCESTUS1"] / 1000.0, 2)


def build_historical_seed_rows(series_hist_dates):
    base_dates = sorted(series_hist_dates.get("SPX", {}).keys())
    if not base_dates:
        return []
    seed_rows = []
    last_known = {}
    for dt in base_dates[-20:]:
        row_dict = {"Date": dt}
        for sid, dmap in series_hist_dates.items():
            valid_dts = [d for d in dmap.keys() if d <= dt]
            if valid_dts:
                val = round(dmap[max(valid_dts)], 4)
                last_known[sid] = val
                row_dict[sid] = val
            elif sid in last_known:
                row_dict[sid] = last_known[sid]
        enrich_derived_rows(row_dict)
        seed_rows.append(row_dict)
    return seed_rows


def build_long_term_cycle_matrix(lt_hist_dates):
    spx_weekly = sorted(lt_hist_dates.get("SPX", {}).keys())
    if not spx_weekly:
        return [], []
    weekly_dates = spx_weekly[-156:]
    lt_rows = []
    last_known = {}
    all_cols = set(["Date"])

    for dt in weekly_dates:
        row_dict = {"Date": dt}
        for sid, dmap in lt_hist_dates.items():
            valid_dts = [d for d in dmap.keys() if d <= dt]
            if valid_dts:
                val = round(dmap[max(valid_dts)], 4)
                last_known[sid] = val
                row_dict[sid] = val
            elif sid in last_known:
                row_dict[sid] = last_known[sid]
        enrich_derived_rows(row_dict)
        all_cols.update(row_dict.keys())
        lt_rows.append(row_dict)

    headers = ["Date"] + sorted(c for c in all_cols if c != "Date")
    matrix = [headers]
    for r in lt_rows:
        matrix.append([r.get(h, "") for h in headers])
    return headers, matrix


def _extract_responses_text(resp_json):
    if isinstance(resp_json.get("output_text"), str) and resp_json["output_text"].strip():
        return resp_json["output_text"].strip()
    texts = []
    for item in resp_json.get("output", []):
        if item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") in ("output_text", "text") and c.get("text"):
                    texts.append(c["text"])
    return "\n".join(texts).strip()


def generate_macro_synthesis(context_payload, fallback_report):
    """通过中转站 https://api.starwish.fit/v1 调用 GPT-6.2 Astra (思考程度最高，自动兼容 max/high 枚举 + 双协议)"""
    prompt = (
        "你是严格遵循《宏观经济六层第一性原理实战通关总纲》的首席宏观策略师。"
        "现在每天执行一次日频全景扫描，请充分发挥深度推理能力，结合【六层硬数据 + 36维黑天鹅/灰犀牛全全景风险数据库 + 预计算背离因子 + 今日全球财经/央行头条】，"
        "输出一份高密度、硬核、具备深度因果链条推演的中文机构级研判内参（控制在 1000~1400 字，不写任何废话套话）。\n"
        "必须严格按以下五步输出：\n"
        "1.【周期定位与新闻催化（主线判定与抢跑背离）】：结合今日重磅新闻头条与硬数据，判断市场正在交易哪条主线（增长分子 vs 利率分母 vs 供给冲击），并严密排查是否存在“低频月度硬数据滞后 vs 高频资产价格抢跑背离”；\n"
        "2.【美元水管与财政生死线（流动性二阶导数）】：深度拆解净流动性 1W/4W/13W 冲量加速度、ON RRP 缓冲垫剩余水位、美债曲线形态（牛陡/熊陡/牛平/熊平）及美国联邦利息吞噬率与 r-g 债务雪球压力；\n"
        "3.【跨资产定价与核心利差博弈】：穿透分析 10Y TIPS 实际利率与黄金/纳指/费城半导体的估值张力、铜金比与原油库存的实体需求信号、市场隐含降息定价(DGS2-EFFR)及中美/美日利差传导；\n"
        "4.【黑天鹅(BS-1~18)与灰犀牛(GR-1~18)36维极端压力扫描】：分别点名当前距离红线最近的【Top3 急性黑天鹅】与【Top3 慢性灰犀牛】，并推演一旦其中某个因子触发红灯熔断，将如何通过跨市场传染链条引爆下一个多米诺骨牌；\n"
        "5.【三情景概率矩阵与强制证伪条件】：给出基准/乐观/悲观精确概率分布及对应占优资产配置，并写出未来 10 个交易日推翻当前基准判断的 3 条具体量化证伪红线。\n\n"
        f"今日实时宏观因子、36维风险库与新闻JSON：\n{json.dumps(context_payload, ensure_ascii=False)}"
    )

    if OPENAI_API_KEY:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        # 自动构造思考强度降级列表：用户指定 max 时，若中转站仅支持 OpenAI 官方标准枚举 high，自动无缝回退
        effort_candidates = [OPENAI_REASONING_EFFORT]
        if OPENAI_REASONING_EFFORT.lower() == "max" and "high" not in effort_candidates:
            effort_candidates.append("high")

        # 1A. 优先尝试 Responses API
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
                    txt = _extract_responses_text(resp.json())
                    if txt:
                        return f"【{OPENAI_MODEL} (思考:{effort} | 中转:{OPENAI_BASE_URL}) 跨因子+36维风险库研判 | {NOW_STR}】\n{txt}"
                elif resp.status_code == 400 and effort == "max":
                    print("[INFO] Relay /responses rejected effort='max', retrying with 'high'...")
                    continue
                else:
                    print(f"[INFO] Relay /responses returned HTTP {resp.status_code}, switching to /chat/completions...")
                    break
            except Exception as e:
                print(f"[INFO] Relay /responses exception ({e}), switching to /chat/completions...")
                break

        # 1B. 自动兼容中转站标准 Chat Completions 端点
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
                    txt = resp.json()["choices"][0]["message"]["content"].strip()
                    if txt:
                        return f"【{OPENAI_MODEL} (思考:{effort} | 中转:{OPENAI_BASE_URL}) 跨因子+36维风险库研判 | {NOW_STR}】\n{txt}"
                elif resp.status_code == 400 and effort == "max":
                    print("[INFO] Relay /chat/completions rejected reasoning_effort='max', retrying with 'high'...")
                    continue
                else:
                    print(f"[WARN] Relay /chat/completions HTTP {resp.status_code}: {resp.text[:220]}")
                    break
            except Exception as e:
                print(f"[WARN] Relay /chat/completions failed: {e}")
                break

    # 2. 次选 OpenRouter 路由
    if OPENROUTER_API_KEY:
        try:
            or_model = OPENAI_MODEL if "/" in OPENAI_MODEL else f"openai/{OPENAI_MODEL}"
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
            body = {
                "model": or_model,
                "reasoning": {"effort": "high"},
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 16000,
            }
            resp = HTTP.post(url, headers=headers, json=body, timeout=(15, 180))
            if resp.status_code == 200:
                txt = resp.json()["choices"][0]["message"]["content"].strip()
                if txt:
                    return f"【{or_model} (思考:high) 跨因子研判 | {NOW_STR}】\n{txt}"
        except Exception as e:
            print(f"[WARN] OpenRouter synthesis failed: {e}")

    # 3. 备选 Gemini
    if GEMINI_API_KEY:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
            resp = HTTP.post(url, json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=(10, 40))
            if resp.status_code == 200:
                txt = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                if txt:
                    return f"【Gemini 跨因子研判 | {NOW_STR}】\n{txt}"
        except Exception as e:
            print(f"[WARN] Gemini synthesis failed: {e}")

    return fallback_report


def analyze_macro_and_blackswan(s_obs, s_dates, wb_metrics, imf_metrics, extra_vals, market_stats, news_list):
    risk_rows = []
    macro_indicator_rows = []
    ts_snapshot_dict = {}

    def get(sid, idx=0):
        arr = s_obs.get(sid, [])
        return arr[idx] if len(arr) > idx else None

    def calc_yoy(sid, lag=12):
        v0, v_prev = get(sid, 0), get(sid, lag)
        if v0 is not None and v_prev is not None and v_prev != 0:
            return round((v0 / v_prev - 1) * 100, 2)
        return None

    for sid, arr in s_obs.items():
        if arr:
            ts_snapshot_dict[sid] = round(arr[0], 4)

    # ================= A. 经济硬数据汇总 =================
    us_credit_yoy = calc_yoy("TOTLL", lag=52)
    if us_credit_yoy is not None:
        ts_snapshot_dict["US_CREDIT_YOY"] = us_credit_yoy
        macro_indicator_rows.append(["美国 (US)", "信用(L1)", "商业银行总信贷同比-H.8 (%)", f"{us_credit_yoy:+.2f}%", f"${get('TOTLL'):,.0f}B", s_dates.get("TOTLL", "-"), "信用扩张" if us_credit_yoy > 2.0 else "信用放缓/紧缩"])

    nfp_0, nfp_1 = get("PAYEMS", 0), get("PAYEMS", 1)
    nfp_add_10k = None
    if nfp_0 is not None and nfp_1 is not None:
        nfp_add_10k = round((nfp_0 - nfp_1) / 10.0, 2)
        ts_snapshot_dict["US_NFP_ADD_10K"] = nfp_add_10k
        nfp_sig = "强劲扩张 (>20万，抑制降息)" if nfp_add_10k > 20 else ("温和着陆 (10~20万)" if nfp_add_10k >= 10 else "⚠️ 就业显著降温 (<10万)")
        macro_indicator_rows.append(["美国 (US)", "就业(L3)", "单月新增非农就业 (万人)", f"{nfp_add_10k:+.2f}", f"总就业 {nfp_0:.0f}千人", s_dates.get("PAYEMS", "-"), nfp_sig])

    icsa_0, icsa_1 = get("ICSA", 0), get("ICSA", 1)
    if icsa_0 is not None:
        icsa_sig = "⚠️ 裁员加速 (>25万)" if icsa_0 > 250000 else "低位稳健 (<25万)"
        macro_indicator_rows.append(["美国 (US)", "就业(L3)", "周度初请失业金人数 (人)", f"{icsa_0:,.0f}", f"{icsa_1:,.0f}" if icsa_1 else "-", s_dates.get("ICSA", "-"), icsa_sig])

    unrate_0, unrate_1 = get("UNRATE", 0), get("UNRATE", 1)
    if unrate_0 is not None:
        macro_indicator_rows.append(["美国 (US)", "就业(L3)", "美国失业率 (%)", f"{unrate_0:.1f}%", f"{unrate_1:.1f}%" if unrate_1 else "-", s_dates.get("UNRATE", "-"), f"萨姆规则={get('SAHMREALTIME') or 0:.2f}%"])

    wage_yoy = calc_yoy("CES0500000003")
    if wage_yoy is not None:
        ts_snapshot_dict["US_WAGE_YOY"] = wage_yoy
        w_sig = "⚠️ 薪资通胀黏性 (>4.0%)" if wage_yoy > 4.0 else "工资增速温和 (<=4.0%)"
        macro_indicator_rows.append(["美国 (US)", "就业/通胀", "私营部门平均时薪同比 (%)", f"{wage_yoy:.2f}%", f"${get('CES0500000003'):.2f}/时", s_dates.get("CES0500000003", "-"), w_sig])

    us_ind_yoy = calc_yoy("INDPRO")
    ind_0 = get("INDPRO", 0)
    if ind_0 is not None:
        if us_ind_yoy is not None:
            ts_snapshot_dict["US_INDPRO_YOY"] = us_ind_yoy
        macro_indicator_rows.append(["美国 (US)", "工业(L3)", "美国工业生产总指数 (2017=100)", f"{ind_0:.2f}", f"同比 {us_ind_yoy:+.2f}%" if us_ind_yoy is not None else "-", s_dates.get("INDPRO", "-"), "美国制造业与采矿产出硬指标"])

    rs_0, rs_1 = get("RSAFS", 0), get("RSAFS", 1)
    if rs_0 is not None and rs_1:
        rs_mom = round((rs_0 / rs_1 - 1) * 100, 2)
        macro_indicator_rows.append(["美国 (US)", "消费(L3)", "美国零售销售环比 (%)", f"{rs_mom:+.2f}%", f"${rs_0:,.0f}M", s_dates.get("RSAFS", "-"), "居民消费韧性" if rs_mom > 0 else "消费支出走弱"])

    us_cpi_yoy = calc_yoy("CPIAUCSL")
    if us_cpi_yoy is not None:
        ts_snapshot_dict["US_CPI_YOY"] = us_cpi_yoy
        macro_indicator_rows.append(["美国 (US)", "通胀(L3)", "美国总体 CPI 同比 (%)", f"{us_cpi_yoy:.2f}%", "-", s_dates.get("CPIAUCSL", "-"), "高于2%目标" if us_cpi_yoy > 2.5 else "接近通胀目标"])

    pce_0, pce_3 = get("PCEPILFE", 0), get("PCEPILFE", 3)
    pce_3m_ann = None
    if pce_0 and pce_3 and pce_3 > 0:
        pce_3m_ann = round(((pce_0 / pce_3) ** 4 - 1) * 100, 2)
        ts_snapshot_dict["US_CORE_PCE_3M_ANN"] = pce_3m_ann
        pce_sig = "通胀黏性偏高 (>2.5%)" if pce_3m_ann > 2.5 else "通胀温和可控 (<=2.5%)"
        macro_indicator_rows.append(["美国 (US)", "通胀(L3)", "核心 PCE 3M环比折年率 (%)", f"{pce_3m_ann:.2f}%", "-", s_dates.get("PCEPILFE", "-"), pce_sig])

    fed_int, fed_rev = get("A091RC1Q027SBEA"), get("FGRECPT")
    interest_rev_ratio = None
    if fed_int and fed_rev and fed_rev > 0:
        interest_rev_ratio = round((fed_int / fed_rev) * 100, 2)
        ts_snapshot_dict["US_INTEREST_REV_RATIO"] = interest_rev_ratio
        macro_indicator_rows.append(["美国 (US)", "财政(L3)", "联邦利息支出 / 财政收入 (%)", f"{interest_rev_ratio:.2f}%", f"利息 ${fed_int:.0f}B", s_dates.get("A091RC1Q027SBEA", "-"), "⚠️ 逼近或突破20%财政空间锁定红线" if interest_rev_ratio >= 18.0 else "财政付息压力可控"])

    us_nom_gdp_yoy = calc_yoy("GDP", lag=4)
    if us_nom_gdp_yoy is not None:
        ts_snapshot_dict["US_NOM_GDP_YOY"] = us_nom_gdp_yoy

    eia_inv_0, eia_inv_1, eia_inv_4 = get("WCESTUS1", 0), get("WCESTUS1", 1), get("WCESTUS1", 4)
    eia_4w_chg = None
    if eia_inv_0 is not None and eia_inv_1 is not None:
        inv_chg = round((eia_inv_0 - eia_inv_1) / 1000.0, 2)
        eia_4w_chg = round((eia_inv_0 - eia_inv_4) / 1000.0, 2) if eia_inv_4 is not None else inv_chg
        ts_snapshot_dict["EIA_CRUDE_MBBL"] = round(eia_inv_0 / 1000.0, 2)
        macro_indicator_rows.append(["美国 (US)", "原油库存(L5)", "EIA商业原油库存 (百万桶)", f"{eia_inv_0/1000.0:.2f}", f"周变动 {inv_chg:+.2f}M桶", s_dates.get("WCESTUS1", "-"), "去库支撑油价" if inv_chg < 0 else "累库压制油价"])

    cn_pmi = get("CN_PMI_MFG")
    if cn_pmi is not None:
        pmi_sig = "🟢 扩张区间 (>50)" if cn_pmi >= 50.0 else "🟡 收缩区间 (<50，待政策发力)"
        macro_indicator_rows.append(["中国 (CN)", "工业/景气", "官方制造业 PMI (荣枯线=50)", f"{cn_pmi:.1f}", f"{get('CN_PMI_MFG', 1):.1f}", s_dates.get("CN_PMI_MFG", "-"), pmi_sig])

    cn_ind = get("CN_IND_YOY")
    if cn_ind is not None:
        macro_indicator_rows.append(["中国 (CN)", "工业(L3)", "规模以上工业增加值同比 (%)", f"{cn_ind:.2f}%", f"{get('CN_IND_YOY', 1):.2f}%", s_dates.get("CN_IND_YOY", "-"), "中国工业生产实际增速"])

    cn_tsf = get("CN_TSF_NEW")
    if cn_tsf is not None:
        macro_indicator_rows.append(["中国 (CN)", "信用(L1)", "社会融资规模增量 (亿元)", f"{cn_tsf:,.0f}", f"{get('CN_TSF_NEW', 1):,.0f}", s_dates.get("CN_TSF_NEW", "-"), "中国宽信用脉冲总量"])

    cn_m1_yoy, cn_m2_yoy = get("CN_M1_YOY"), get("CN_M2_YOY")
    cn_scissors = None
    if cn_m1_yoy is not None and cn_m2_yoy is not None:
        cn_scissors = round(cn_m1_yoy - cn_m2_yoy, 2)
        ts_snapshot_dict["CN_M1_M2_SCISSORS"] = cn_scissors
        macro_indicator_rows.append(["中国 (CN)", "货币活化(L1)", "M1同比 / M2同比 (%)", f"M1 {cn_m1_yoy:.1f}% / M2 {cn_m2_yoy:.1f}%", f"剪刀差 {cn_scissors:+.1f}%", s_dates.get("CN_M1_YOY", "-"), "资金活化改善" if cn_scissors > -2.0 else "定期存款沉淀偏高"])

    cn_cpi, cn_ppi = get("CN_CPI_YOY"), get("CN_PPI_YOY")
    ppi_cpi_gap = None
    if cn_cpi is not None and cn_ppi is not None:
        ppi_cpi_gap = round(cn_ppi - cn_cpi, 2)
        ts_snapshot_dict["CN_PPI_CPI_GAP"] = ppi_cpi_gap
        macro_indicator_rows.append(["中国 (CN)", "通胀(L3)", "CPI同比 / PPI同比 (%)", f"CPI {cn_cpi:.1f}% / PPI {cn_ppi:.1f}%", f"剪刀差 {ppi_cpi_gap:+.1f}%", s_dates.get("CN_CPI_YOY", "-"), "通缩压力观测" if cn_ppi < 0 else "再通胀周期"])

    ea_cpi_yoy = calc_yoy("CP0000EZ19M086NEST")
    if ea_cpi_yoy is not None:
        ts_snapshot_dict["EA_CPI_YOY"] = ea_cpi_yoy
        macro_indicator_rows.append(["欧元区 (EA)", "通胀(L3)", "欧元区调和 CPI 同比 (%)", f"{ea_cpi_yoy:.2f}%", "-", s_dates.get("CP0000EZ19M086NEST", "-"), "欧央行利率路径核心锚"])
    ea_unemp = get("LRHUTTTTEZM156S")
    if ea_unemp is not None:
        macro_indicator_rows.append(["欧元区 (EA)", "就业(L3)", "欧元区失业率 (%)", f"{ea_unemp:.1f}%", f"{get('LRHUTTTTEZM156S', 1):.1f}%", s_dates.get("LRHUTTTTEZM156S", "-"), "欧洲劳动力市场状态"])

    jp_ind_mom = get("JPNPRMNTO01GPSAM")
    if jp_ind_mom is not None:
        macro_indicator_rows.append(["日本 (JP)", "工业(L3)", "日本制造业生产环比 (%)", f"{jp_ind_mom:+.2f}%", f"{get('JPNPRMNTO01GPSAM', 1):+.2f}%", s_dates.get("JPNPRMNTO01GPSAM", "-"), "日本出口与工业产出动能"])
    jp_cpi_yoy = calc_yoy("JPNCPIALLMINMEI")
    if jp_cpi_yoy is not None:
        ts_snapshot_dict["JP_CPI_YOY"] = jp_cpi_yoy
        macro_indicator_rows.append(["日本 (JP)", "通胀(L3)", "日本全国 CPI 同比 (%)", f"{jp_cpi_yoy:.2f}%", "-", s_dates.get("JPNCPIALLMINMEI", "-"), "日本央行加息判定指标"])

    # ================= B. 高浓缩跨因子与二阶导数计算 =================
    walcl, tga, rrp = get("WALCL"), get("WDTGAL"), get("RRPONTSYD")
    walcl_1, tga_1, rrp_1 = get("WALCL", 1), get("WDTGAL", 1), get("RRPONTSYD", 1)
    walcl_4, tga_4, rrp_4 = get("WALCL", 4), get("WDTGAL", 4), get("RRPONTSYD", 4)
    walcl_13, tga_13, rrp_13 = get("WALCL", 13), get("WDTGAL", 13), get("RRPONTSYD", 13)

    net_liq, net_liq_chg, net_liq_4w, net_liq_13w = None, 0.0, None, None
    if all(v is not None for v in [walcl, tga, rrp]):
        net_liq = round((walcl - tga) / 1000.0 - rrp, 2)
        ts_snapshot_dict["NET_LIQUIDITY"] = net_liq
        ts_snapshot_dict["WALCL_B"] = round(walcl / 1000.0, 2)
        ts_snapshot_dict["TGA_B"] = round(tga / 1000.0, 2)
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
    spread_10_2, d_spread = None, 0.0
    if all(v is not None for v in [y10, y2, y10_5, y2_5]):
        spread_10_2 = round(y10 - y2, 3)
        ts_snapshot_dict["SPREAD_10Y2Y"] = spread_10_2
        d_spread = round(spread_10_2 - (y10_5 - y2_5), 3)
        dy10, dy2 = y10 - y10_5, y2 - y2_5
        if d_spread > 0 and dy2 < 0 and abs(dy2) > abs(dy10):
            curve_regime = "牛陡 (短端降息预期主导，利好短债/黄金)"
        elif d_spread > 0 and dy10 > 0 and dy10 > dy2:
            curve_regime = "熊陡 (再通胀或美债供给过剩，压制纳指高估值)"
        elif d_spread < 0 and dy10 < 0 and abs(dy10) > abs(dy2):
            curve_regime = "牛平 (避险买长债或长期通胀预期回落)"
        elif d_spread < 0 and dy2 > 0 and dy2 > dy10:
            curve_regime = "熊平 (央行紧缩预期升温，曲线趋平)"
        else:
            curve_regime = "倒挂震荡区间" if spread_10_2 < 0 else "曲线温和震荡"

    tips_0, tips_5 = get("DFII10", 0), get("DFII10", 5)
    if y10 is not None and tips_0 is not None:
        ts_snapshot_dict["BREAKEVEN_10Y"] = round(y10 - tips_0, 3)

    gold_stat = market_stats.get("GOLD", {})
    copper_stat = market_stats.get("COPPER", {})
    brent_stat = market_stats.get("BRENT", {})
    cg_ratio_curr = None
    if gold_stat.get("price") and copper_stat.get("price"):
        cg_ratio_curr = round((copper_stat["price"] * 1000) / gold_stat["price"], 3)
        ts_snapshot_dict["COPPER_GOLD_RATIO"] = cg_ratio_curr
    if gold_stat.get("price") and brent_stat.get("price"):
        ts_snapshot_dict["GOLD_OIL_RATIO"] = round(gold_stat["price"] / brent_stat["price"], 2)

    gold_5d = gold_stat.get("pct_5d")
    spx_5d = market_stats.get("SPX", {}).get("pct_5d")
    dxy_5d = market_stats.get("DXY", {}).get("pct_5d")
    hy = get("BAMLH0A0HYM2")
    hy_5 = get("BAMLH0A0HYM2", 5)
    ig = get("BAMLC0A0CM")

    divergence_flags = []
    if tips_0 is not None and tips_5 is not None and gold_5d is not None:
        if (tips_0 - tips_5) > 0.05 and gold_5d > 1.0:
            divergence_flags.append("TIPS_GOLD_DECOUPLING: 10Y实际利率上行但黄金逆势上涨 -> 央行去美元化购金主导定价")
    if dxy_5d is not None and y10_5 is not None and y10 is not None:
        if (y10 - y10_5) >= 0.15 and dxy_5d <= -1.2:
            divergence_flags.append("FISCAL_CREDIT_DISCOUNT: 10Y美债收益率飙升但美元指数下跌 -> 市场对美债供给与财政可持续性计入信用折价")
    if spx_5d is not None and hy is not None and hy_5 is not None:
        if spx_5d >= 1.5 and (hy - hy_5) >= 0.15:
            divergence_flags.append("EQUITY_CREDIT_DIVERGENCE: 标普500上涨但高收益债信用利差走阔 -> 股债定价背离，谨防股市补跌")
    if spx_5d is not None and spx_5d <= -3.0:
        divergence_flags.append("HIGH_FREQ_RISK_OFF: 高频股指急跌抢跑低频月度硬数据 -> 谨防拿上月后视镜数据误判")

    # ================= C. 黑天鹅(BS-1~18) + 灰犀牛(GR-1~18) 36维系统性风险数据库 =================
    red_alerts, yellow_alerts = [], []

    def add_risk(code, r_type, dim, metric, curr_val, warn_lbl, red_lbl, ratio_raw, mech):
        ratio_pct = round(max(0.0, min(1.5, ratio_raw)), 3)
        if ratio_raw >= 1.0:
            st = "🔴 红灯熔断"
            red_alerts.append(f"[{code}/{r_type}]{dim}={curr_val}")
        elif ratio_raw >= 0.65:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"[{code}/{r_type}]{dim}={curr_val}")
        else:
            st = "🟢 正常"
        risk_rows.append([code, f"{r_type}·{dim}", metric, curr_val, warn_lbl, red_lbl, st, mech, ratio_pct])

    # --- 第一部分：18 维急性黑天鹅 (BS-1 ~ BS-18) ---
    sofr, iorb = extra_vals.get("SOFR"), get("IORB")
    if sofr is not None and iorb is not None:
        sofr_sp = round((sofr - iorb) * 100, 1)
        add_risk("BS-1", "黑天鹅", "美元回购管道", "SOFR - IORB 利差 (bps)", sofr_sp, "10 bps", "25 bps", sofr_sp / 25.0, "银行间美元准备金突发性短缺与货币市场冻结")

    if hy is not None:
        hy_bps = round(hy * 100, 0)
        add_risk("BS-2", "黑天鹅", "垃圾债信用踩踏", "高收益债信用利差 (bps)", hy_bps, "350 bps", "500 bps", hy_bps / 500.0, "高收益企业再融资通道骤停引发集中违约潮")

    fsi = get("STLFSI4")
    if fsi is not None:
        add_risk("BS-3", "黑天鹅", "综合金融压力", "圣路易斯金融压力 STLFSI4", round(fsi, 2), "0.50", "2.00", (fsi + 1.0) / 3.0, "跨市场流动性、利差与波动率系统性共振恶化")

    if all(v is not None for v in [spx_5d, gold_5d, dxy_5d, y10, y10_5]):
        y10_5d_bps = round((y10 - y10_5) * 100, 1)
        summary_str = f"SPX:{spx_5d:+.1f}% | 金:{gold_5d:+.1f}% | 10Y:{y10_5d_bps:+.0f}bp"
        stress_score = max(0.0, (-spx_5d / 4.0) * 0.5 + (-gold_5d / 2.0) * 0.5)
        if spx_5d <= -4.0 and gold_5d <= -2.0 and y10_5d_bps >= 20 and dxy_5d >= 1.5:
            stress_score = 1.15
        add_risk("BS-4", "黑天鹅", "现金为王三杀挤兑", "5日股/金/债联动状态", summary_str, "股金同跌", "股金暴跌+债率飙升", stress_score, "Margin Call下机构无差别抛售黄金美债换美元")

    jpy_5d = market_stats.get("USDJPY", {}).get("pct_5d")
    if jpy_5d is not None:
        add_risk("BS-5", "黑天鹅", "日元套息平仓踩踏", "USD/JPY 5日涨跌幅 (%)", jpy_5d, "-2.5%", "-4.0%", -jpy_5d / 4.0, "日元急速升值迫使全球低息日元融资盘拆仓抛售风险资产")

    vix = market_stats.get("VIX", {}).get("price")
    vix3m = get("VXVCLS")
    if vix is not None and vix3m is not None and vix3m > 0:
        vix_ratio = round(vix / vix3m, 2)
        add_risk("BS-6", "黑天鹅", "波动率期限倒挂", f"VIX/VIX3M (VIX={vix})", vix_ratio, "1.00", "1.15", max(vix / 35.0, vix_ratio / 1.15), "短端恐慌情绪大幅超越远期，预示短期尾部黑天鹅")

    btc_auc = extra_vals.get("UST_BID_COVER")
    tail_bps = extra_vals.get("UST_TAIL_BPS", 0.0)
    if btc_auc is not None and btc_auc > 0:
        add_risk("BS-7", "黑天鹅", "美债一级拍卖失灵", f"投标倍数(Tail={tail_bps}bp)", btc_auc, "2.25x", "2.10x", max(2.10 / btc_auc, tail_bps / 3.0), "一级交易商承接意愿枯竭引发长端美债收益率无序飙升")

    disc_0 = get("DISBORROW")
    if disc_0 is not None:
        disc_b = round(disc_0 / 1000.0, 2)
        add_risk("BS-8", "黑天鹅", "银行贴现窗口挤兑", "美联储贴现窗口借款 ($B)", disc_b, "$15.0B", "$50.0B", disc_b / 50.0, "商业银行被迫向美联储紧急求助流动性（类似2023 SVB前兆）")

    kre_5d = market_stats.get("US_REG_BANK", {}).get("pct_5d")
    if kre_5d is not None:
        add_risk("BS-9", "黑天鹅", "区域银行股闪崩", "区域银行ETF(KRE) 5日涨跌(%)", kre_5d, "-5.0%", "-9.0%", max(0.0, -kre_5d / 9.0), "中小银行存款挤兑或商业地产坏账曝光引发银行板块流动性踩踏")

    brent_5d = brent_stat.get("pct_5d")
    if brent_5d is not None:
        add_risk("BS-10", "黑天鹅", "能源供给断供冲击", "布伦特原油 5日暴涨 (%)", brent_5d, "+6.5%", "+12.0%", max(0.0, brent_5d / 12.0), "地缘战争或航道封锁导致油价脉冲暴涨，触发全球突发滞胀冲击")

    # 新增 BS-11 ~ BS-18
    nfci_0 = get("NFCI")
    if nfci_0 is not None:
        add_risk("BS-11", "黑天鹅", "金融条件骤紧熔断", "芝加哥联储 NFCI 指数", round(nfci_0, 2), "0.00", "+0.50", max(0.0, (nfci_0 + 0.5) / 1.0), "跨市场金融条件由松转紧突破零轴，信用与融资环境骤冷")

    sox_5d = market_stats.get("SOX", {}).get("pct_5d")
    if sox_5d is not None:
        add_risk("BS-12", "黑天鹅", "AI/芯片泡沫去杠杆", "费城半导体(SOX) 5日涨跌(%)", sox_5d, "-6.0%", "-10.0%", max(0.0, -sox_5d / 10.0), "AI算力资本开支证伪或高拥挤科技量化盘踩踏平仓")

    jp10_0 = get("IRLTLT01JPM156N")
    if jp10_0 is not None:
        add_risk("BS-13", "黑天鹅", "日债失控(JGB债灾)", "日本10Y国债收益率 (%)", round(jp10_0, 2), "1.20%", "1.60%", max(0.0, jp10_0 / 1.60), "日本长端利率失控迫使日资机构大规模抛售美债欧债回流本土")

    cnh_5d = market_stats.get("USDCNH", {}).get("pct_5d")
    if cnh_5d is not None:
        add_risk("BS-14", "黑天鹅", "离岸人民币急贬冲击", "USD/CNH 5日急贬幅度 (%)", cnh_5d, "+1.2%", "+2.2%", max(0.0, cnh_5d / 2.2), "人民币汇率大幅波动外溢至亚洲及新兴市场货币连锁贬值")

    btc_5d = market_stats.get("BTC", {}).get("pct_5d")
    if btc_5d is not None:
        add_risk("BS-15", "黑天鹅", "加密高杠杆连环爆仓", "比特币(BTC) 5日暴跌 (%)", btc_5d, "-10.0%", "-18.0%", max(0.0, -btc_5d / 18.0), "美元离岸流动性枯竭率先击穿高杠杆加密市场，预警纳指风险")

    copper_5d = copper_stat.get("pct_5d")
    if copper_5d is not None and gold_5d is not None:
        cg_5d_diff = round(copper_5d - gold_5d, 2)
        add_risk("BS-16", "黑天鹅", "铜金比断崖(硬着陆)", "铜与黄金 5日相对剪刀差(%)", cg_5d_diff, "-4.5%", "-8.0%", max(0.0, -cg_5d_diff / 8.0), "铜价暴跌而黄金避险上涨，市场急速定价全球实体经济硬着陆")

    if y10 is not None and y10_5 is not None:
        y10_jump_bps = round((y10 - y10_5) * 100, 1)
        add_risk("BS-17", "黑天鹅", "长端美债单周屠杀", "US 10Y 5日跳升幅度 (bps)", y10_jump_bps, "+20 bps", "+35 bps", max(0.0, y10_jump_bps / 35.0), "无风险利率单周暴涨引爆银行AFS债券账面巨亏与养老金Margin Call")

    wheat_5d = market_stats.get("WHEAT", {}).get("pct_5d")
    if wheat_5d is not None:
        add_risk("BS-18", "黑天鹅", "全球粮食断供危机", "CBOT小麦 5日暴涨幅度 (%)", wheat_5d, "+8.0%", "+15.0%", max(0.0, wheat_5d / 15.0), "极端气候或黑海/地缘冲突阻断粮道，引爆新兴市场恶性通胀")

    # --- 第二部分：18 维慢性灰犀牛 (GR-1 ~ GR-18) ---
    if interest_rev_ratio is not None:
        add_risk("GR-1", "灰犀牛", "美财政利息吞噬率", "联邦利息/财政收入 (%)", interest_rev_ratio, "16.0%", "20.0%", interest_rev_ratio / 20.0, "利息支出挤压常规财政预算，倒逼赤字货币化与财政主导")

    if y10 is not None and us_nom_gdp_yoy is not None:
        r_minus_g = round(y10 - us_nom_gdp_yoy, 2)
        add_risk("GR-2", "灰犀牛", "主权债务雪球(r-g)", "US 10Y利率 - 名义GDP (%)", r_minus_g, "+0.5%", "+1.5%", (r_minus_g + 1.5) / 3.0, "借贷利率持续高于名义经济增长，债务率进入内生发散通道")

    cre_delinq = get("DRCRELEXFACBS")
    if cre_delinq is not None:
        add_risk("GR-3", "灰犀牛", "商业地产(CRE)违约", "银行CRE贷款违约率 (%)", round(cre_delinq, 2), "1.50%", "2.50%", cre_delinq / 2.50, "高利率下写字楼与商业地产再融资坏账侵蚀美国区域银行资本金")

    cc_delinq = get("DRCCLACBS")
    psav = get("PSAVERT")
    if cc_delinq is not None:
        lbl_val = f"违约{cc_delinq:.2f}% / 储蓄{psav:.1f}%" if psav is not None else f"{cc_delinq:.2f}%"
        add_risk("GR-4", "灰犀牛", "美居民消费信用恶化", "信用卡违约率与储蓄率", lbl_val, "违约3.0%", "违约4.0%", cc_delinq / 4.0, "超额储蓄耗尽后中低收入群体信用卡违约攀升拖累消费引擎")

    dep_0, dep_13 = get("DPSACBW027SBOG", 0), get("DPSACBW027SBOG", 13)
    if dep_0 is not None and dep_13 is not None and dep_13 > 0:
        dep_q_chg = round((dep_0 / dep_13 - 1) * 100, 2)
        add_risk("GR-5", "灰犀牛", "商业银行存款流失", "商业银行总存款季度变动(%)", dep_q_chg, "-1.0%", "-3.0%", max(0.0, -dep_q_chg / 3.0), "高息货币基金持续虹吸银行低息存款，削弱银行放贷能力")

    cn10 = get("CN_DGS10")
    if y10 is not None and cn10 is not None:
        us_cn_sp = round(y10 - cn10, 2)
        add_risk("GR-6", "灰犀牛", "中美长端负利差倒挂", "US 10Y - CN 10Y (%)", us_cn_sp, "2.00%", "3.00%", us_cn_sp / 3.00, "深度负利差持续压制人民币汇率弹性与外资配置意愿")

    if cn_scissors is not None:
        add_risk("GR-7", "灰犀牛", "中国货币活化与通缩", "CN M1-M2剪刀差 (%)", cn_scissors, "-3.5%", "-6.0%", max(0.0, -cn_scissors / 6.0), "企业与居民预防性储蓄偏高、实体投融资需求待实质性修复")

    em_ratios = {}
    for iso3 in COUNTRIES_LIST:
        res = wb_metrics.get((iso3, "FI.RES.TOTL.CD"))
        debt = wb_metrics.get((iso3, "DT.DOD.DSTC.CD"))
        if res and debt and res > 0:
            em_ratios[iso3] = round((debt / res) * 100, 1)
    if em_ratios:
        worst_iso = max(em_ratios, key=em_ratios.get)
        worst_val = em_ratios[worst_iso]
        add_risk("GR-8", "灰犀牛", f"新兴市场外债({worst_iso})", "短期外债/官方外储 (%)", worst_val, "65.0%", "100.0%", worst_val / 100.0, "外储覆盖率不足1倍，强美元周期下易爆发主权债务重组危机")

    sloos_0 = get("DRTSCILM")
    if sloos_0 is not None:
        add_risk("GR-9", "灰犀牛", "SLOOS银行全面惜贷", "银行收紧工商业贷款比例(%)", round(sloos_0, 1), "35.0%", "55.0%", max(0.0, sloos_0 / 55.0), "商业银行大幅收紧信贷标准，实体企业信用创造渠道面临慢性冻结")

    tdsp_0 = get("TDSP")
    if tdsp_0 is not None:
        add_risk("GR-10", "灰犀牛", "美家庭债务偿付负担", "家庭偿债占可支配收入比(%)", round(tdsp_0, 2), "10.5%", "12.0%", max(0.0, (tdsp_0 - 8.0) / 4.0), "高利率向存量房贷/车贷/消费贷传导，挤压居民可选消费现金流")

    # 新增 GR-11 ~ GR-18
    if rrp is not None:
        rrp_depletion_ratio = max(0.0, (400.0 - rrp) / 350.0)  # 低于50B触发红灯
        add_risk("GR-11", "灰犀牛", "ON RRP流动性缓冲耗尽", "隔夜逆回购余额 ($B)", round(rrp, 1), "$150B", "$50B", rrp_depletion_ratio, "逆回购蓄水池见底后，美联储QT与TGA发债将直接抽干商业银行准备金")

    if tips_0 is not None:
        add_risk("GR-12", "灰犀牛", "实际利率长期高压窒息", "US 10Y TIPS 实际利率 (%)", round(tips_0, 2), "1.80%", "2.35%", max(0.0, tips_0 / 2.35), "高实际无风险利率持续压制实体企业资本回报率(ROIC)与高估值资产")

    if hy is not None and ig is not None:
        hy_ig_gap = round((hy - ig) * 100, 0)
        add_risk("GR-13", "灰犀牛", "企业债信用分层恶化", "HY - IG 信用利差剪刀差(bps)", hy_ig_gap, "260 bps", "380 bps", max(0.0, hy_ig_gap / 380.0), "低评级僵尸企业借新还旧成本失控，违约潮向中游企业蔓延")

    mtg_delinq = get("DRSFRMACBS")
    if mtg_delinq is not None:
        add_risk("GR-14", "灰犀牛", "居民住房抵押贷款违约", "银行居民房贷违约率 (%)", round(mtg_delinq, 2), "2.20%", "3.20%", max(0.0, mtg_delinq / 3.20), "失业率上升叠加高房贷利率开始击穿美国家庭核心资产负债表")

    if psav is not None:
        sav_stress = max(0.0, (6.5 - psav) / 3.5)  # 储蓄率低于3.0%触发红灯
        add_risk("GR-15", "灰犀牛", "美居民超额储蓄枯竭", "个人储蓄率占可支配收入(%)", round(psav, 1), "4.2%", "3.0%", sav_stress, "居民消费高度依赖信用卡透支，一旦就业降温消费将断崖失速")

    bus_yoy = calc_yoy("BUSLOANS", lag=12)
    if bus_yoy is not None:
        bus_stress = max(0.0, (3.0 - bus_yoy) / 4.0)  # 工商业贷款同比低于-1.0%触发红灯
        add_risk("GR-16", "灰犀牛", "美国工商业信贷萎缩", "工商业贷款余额同比 (%)", bus_yoy, "+0.5%", "-1.0%", bus_stress, "企业停止主动加杠杆扩产，经济内生信用派生引擎熄火")

    add_risk("GR-17", "灰犀牛", "萨姆规则衰退临界逼近", "萨姆规则实时指标 (%)", round(sahm, 2), "0.35%", "0.50%", max(0.0, sahm / 0.50), "失业率3个月均值较12个月低点上升超0.50%触发历史100%准度衰退铁律")

    spy_pe_val = get("SPX_PE")
    if spy_pe_val is not None:
        add_risk("GR-18", "灰犀牛", "美股估值泡沫与负ERP", "标普500代理市盈率 (SPY PE)", spy_pe_val, "24.0x", "28.0x", max(0.0, (spy_pe_val - 16.0) / 12.0), "美股盈利收益率低于10Y美债无风险利率，估值安全边际极度脆弱")

    # ================= D. 多国横向对比表 =================
    country_names = {
        "USA": "美国 (US)", "CHN": "中国 (CN)", "JPN": "日本 (JP)", "DEU": "德国/欧元区 (DE)",
        "IND": "印度 (IN)", "BRA": "巴西 (BR)", "IDN": "印度尼西亚 (ID)", "TUR": "土耳其 (TR)"
    }
    bond_10y_map = {
        "USA": get("DGS10"), "CHN": get("CN_DGS10"),
        "JPN": get("IRLTLT01JPM156N"), "DEU": get("IRLTLT01DEM156N")
    }
    country_rows = []
    for iso3 in COUNTRIES_LIST:
        gdp = imf_metrics.get((iso3, "NGDP_RPCH"), "")
        cpi = imf_metrics.get((iso3, "PCPIPCH"), "")
        ca = imf_metrics.get((iso3, "BCA_NGDPD"), "")
        fiscal = imf_metrics.get((iso3, "GGXCNL_NGDP"), "")
        b10 = bond_10y_map.get(iso3, "")
        debt_ratio = em_ratios.get(iso3, "")
        country_rows.append([iso3, country_names.get(iso3, iso3), gdp, cpi, b10, "", ca, fiscal, debt_ratio])

    # ================= E. 生成五步中文研判报告 (GPT-6.2 Astra 最高思考 + 36维风险库 + 新闻) =================
    tips10 = tips_0 or 0.0
    growth_weak = (sahm >= 0.5) or (nfp_add_10k is not None and nfp_add_10k < 8.0)
    infl_high = (pce_3m_ann is not None and pce_3m_ann > 2.5) or (us_cpi_yoy is not None and us_cpi_yoy > 2.8)

    if not growth_weak and not infl_high:
        quadrant = "【象限一：复苏/金发姑娘（增长稳 + 通胀可控）】"
        regime_logic = "当前处于【增长主导机制】（好消息=好消息），市场重点交易分子端企业盈利修复，占优资产为权益（成长与周期股）及企业债。"
    elif not growth_weak and infl_high:
        quadrant = "【象限二：过热/紧缩（增长韧性 + 通胀黏性）】"
        regime_logic = "当前切换至【利率/通胀主导机制】（好消息=坏消息），超预期数据易强化央行紧缩预期并抬高分母端贴现率，占优资产为大宗商品与价值股，规避长久期国债。"
    elif growth_weak and infl_high:
        quadrant = "【象限三：滞胀困境（增长下滑 + 通胀高企）】"
        regime_logic = "警惕【薪资-物价螺旋与政策两难】，央行陷入保增长与抗通胀矛盾，占优资产为现金/短债与黄金，严格规避高估值科技股与长债。"
    else:
        quadrant = "【象限四：衰退防御（增长走弱 + 通胀回落）】"
        regime_logic = "总需求收缩主导市场，央行进入被动降息周期，占优资产为长久期国债，警惕信用利差走阔引发的信用紧缩。"

    if red_alerts:
        probs = "基准情景(风险释放): 45% | 乐观情景(政策急转救市): 15% | 悲观情景(尾部危机扩散): 40%"
    elif yellow_alerts:
        probs = "基准情景(震荡消化): 55% | 乐观情景(软着陆延续): 20% | 悲观情景(紧缩/波动升级): 25%"
    else:
        probs = "基准情景(软着陆/常态轮动): 60% | 乐观情景(宽信用共振): 25% | 悲观情景(二次通胀/突发冲击): 15%"

    spx_p = market_stats.get("SPX", {}).get("price", "-")
    csi_p = market_stats.get("CSI300", {}).get("price", "-")
    dxy_p = market_stats.get("DXY", {}).get("price", "-")
    jpy_p = market_stats.get("USDJPY", {}).get("price", "-")
    au_p = market_stats.get("GOLD", {}).get("price", "-")
    br_p = market_stats.get("BRENT", {}).get("price", "-")
    btc_p = market_stats.get("BTC", {}).get("price", "-")

    fallback_report = (
        f"【宏观第一性原理自动研判简报 | 更新时间: {NOW_STR}】\n"
        f"1. 周期定位与硬数据：{quadrant} 美非农 {nfp_add_10k if nfp_add_10k is not None else '-'} 万、CPI {us_cpi_yoy if us_cpi_yoy is not None else '-'}%、萨姆规则 {sahm}%、美联邦利息占财政收入 {interest_rev_ratio if interest_rev_ratio is not None else '-'}%；中国PMI {cn_pmi if cn_pmi is not None else '-'}、M1-M2剪刀差 {cn_scissors if cn_scissors is not None else '-'}%。\n"
        f"2. 水管与跨资产锚点：净流动性 {net_liq}B（1W {net_liq_chg:+}B | 4W {net_liq_4w}B | 13W {net_liq_13w}B），10Y TIPS {tips10}%，10Y-2Y利差 {spread_10_2}%（{curve_regime}）。市场：SPX={spx_p} | 沪深300={csi_p} | DXY={dxy_p} | USD/JPY={jpy_p} | 黄金={au_p} | 布油={br_p} | BTC={btc_p}。\n"
        f"3. 传导机制与背离监控：{regime_logic} {' '.join(divergence_flags)}\n"
        f"4. 黑天鹅&灰犀牛36维雷达：ON RRP {rrp}B，CRE违约率 {cre_delinq}%，SLOOS收紧 {sloos_0}%，贴现窗口借款 ${round(disc_0/1000,1) if disc_0 else '-'}B。{'⚠️ 预警：' + '；'.join(red_alerts + yellow_alerts) if (red_alerts or yellow_alerts) else '✅ 36大风险维度均处绿灯常态区间。'}\n"
        f"5. 情景概率与强制证伪：{probs} ｜ 证伪点：① 10Y TIPS 突破 {'1.80%' if tips10 > 2.0 else '2.25%'}；② 高收益债利差破 500bps 或 VIX 破 30；③ 净流动性连续两周反向变动超 150B。"
    )

    llm_ctx = {
        "date": TODAY_STR,
        "quadrant": quadrant,
        "liquidity_impulse_B": {"current": net_liq, "chg_1w": net_liq_chg, "chg_4w": net_liq_4w, "chg_13w": net_liq_13w, "on_rrp": rrp, "tga_B": round(tga/1000, 1) if tga else None},
        "rates_and_fiscal": {"tips10": tips10, "spread_10_2": spread_10_2, "curve_regime": curve_regime, "implied_rate_cut_bps": implied_rate_cut_bps, "interest_rev_ratio_pct": interest_rev_ratio, "us_nom_gdp_yoy": us_nom_gdp_yoy},
        "growth_and_inflation": {"pce_3m_ann": pce_3m_ann, "us_cpi_yoy": us_cpi_yoy, "nfp_10k": nfp_add_10k, "sahm": sahm, "eia_crude_4w_chg_mbbl": eia_4w_chg, "cn_pmi": cn_pmi, "cn_m1_m2_scissors": cn_scissors, "cn_ppi_cpi_gap": ppi_cpi_gap},
        "cross_asset_prices": {k: {"price": v["price"], "pct_5d": v["pct_5d"], "pct_20d": v["pct_20d"]} for k, v in market_stats.items() if k in ["SPX", "NDX", "SOX", "CSI300", "US_REG_BANK", "US_HY_ETF", "US_20Y_BOND", "DXY", "USDJPY", "USDCNH", "GOLD", "BRENT", "COPPER", "VIX", "BTC"]},
        "precomputed_divergence_flags": divergence_flags,
        "blackswan_and_grayrhino_36d": sorted(
            [
                {"code": r[0], "dim": r[1], "val": r[3], "status": r[6], "pct_to_redline": round(r[8] * 100, 1), "mech": r[7]}
                for r in risk_rows
            ],
            key=lambda x: x["pct_to_redline"],
            reverse=True
        ),
        "top_global_macro_news": [f"[{n['source']}] {n['title']}" for n in news_list[:12]]
    }
    report_text = generate_macro_synthesis(llm_ctx, fallback_report)

    meta_ctx = {
        "net_liq_chg": net_liq_chg,
        "d_spread": d_spread,
        "curve_regime": curve_regime,
        "cn_scissors": cn_scissors,
        "pce_3m_ann": pce_3m_ann,
        "ppi_cpi_gap": ppi_cpi_gap,
        "us_credit_yoy": us_credit_yoy,
        "interest_rev_ratio": interest_rev_ratio,
        "us_nom_gdp_yoy": us_nom_gdp_yoy,
    }
    return risk_rows, country_rows, macro_indicator_rows, ts_snapshot_dict, report_text, meta_ctx


def get_or_create_ws(sh, title, rows=300, cols=26):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def sync_all_sheets(raw_records, cross_asset_items, risk_rows, country_rows, macro_rows, ts_dict, seed_rows, lt_headers, lt_matrix, news_list, report_text, meta_ctx):
    if not raw_records:
        raise RuntimeError("所有外部接口均未返回有效数据，中止写入以保护表格。")

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_dash = get_or_create_ws(sh, TAB_DASH, rows=150, cols=25)
    ws_mkt_mac = get_or_create_ws(sh, TAB_MKT_MAC, rows=220, cols=15)
    ws_ts = get_or_create_ws(sh, TAB_TS, rows=2000, cols=120)
    ws_raw_log = get_or_create_ws(sh, TAB_RAW_LOG, rows=450, cols=16)
    ws_lt = get_or_create_ws(sh, TAB_LT, rows=300, cols=65)

    for old_name in LEGACY_TABS:
        if old_name not in [TAB_DASH, TAB_MKT_MAC, TAB_TS, TAB_RAW_LOG, TAB_LT]:
            try:
                sh.del_worksheet(sh.worksheet(old_name))
            except Exception:
                pass

    # ================= 1. 更新 04_长期结构周期库_LongTermCycle =================
    if lt_matrix and len(lt_matrix) > 1:
        ws_lt.clear()
        ws_lt.update(range_name="A1", values=lt_matrix, value_input_option="USER_ENTERED")

    # ================= 2. 更新 02_日频短周期库_TimeSeries =================
    existing = [r for r in ws_ts.get_all_values() if r and len(r) > 0 and str(r[0]).strip()]
    all_keys = ["Date"] + sorted(ts_dict.keys())

    if len(existing) <= 2 and seed_rows:
        merged_by_dt = {r["Date"]: r for r in seed_rows}
        today_entry = {"Date": TODAY_STR}
        today_entry.update(ts_dict)
        merged_by_dt[TODAY_STR] = today_entry
        sorted_dts = sorted(merged_by_dt.keys())
        matrix = [all_keys]
        for dt in sorted_dts:
            r_obj = merged_by_dt[dt]
            matrix.append([dt] + [r_obj.get(k, "") for k in all_keys[1:]])
        ws_ts.clear()
        ws_ts.update(range_name="A1", values=matrix, value_input_option="USER_ENTERED")
    else:
        headers = existing[0]
        new_cols = [k for k in all_keys if k not in headers]
        if new_cols:
            headers = headers + new_cols
            ws_ts.update(range_name="A1", values=[headers], value_input_option="USER_ENTERED")
        new_row = [TODAY_STR] + [ts_dict.get(col, "") for col in headers[1:]]
        if existing[-1][0] == TODAY_STR:
            ws_ts.update(range_name=f"A{len(existing)}", values=[new_row], value_input_option="USER_ENTERED")
        else:
            ws_ts.append_row(new_row, value_input_option="USER_ENTERED")

    # ================= 3. 更新 01_跨资产与宏观硬数据_MarketsAndMacro =================
    mkt_matrix = [["资产类别", "代码", "资产名称", "最新价", "1D涨跌", "5D涨跌", "20D涨跌", "第一性原理定价驱动"]]
    for item in cross_asset_items:
        mkt_matrix.append([item["cat"], item["code"], item["name"], item["price"], item["pct_1d"], item["pct_5d"], item["pct_20d"], item["driver"]])
    mkt_matrix.append([])
    mkt_matrix.append(["国家/地区", "六层维度", "核心经济硬指标", "最新值", "参考对比", "数据日期", "周期信号解读"])
    mkt_matrix.extend(macro_rows)
    ws_mkt_mac.clear()
    ws_mkt_mac.update(range_name="A1", values=mkt_matrix, value_input_option="USER_ENTERED")

    # ================= 4. 更新 03_全量指标底表与日志_RawAndLog =================
    raw_headers = ["观测日期", "国家/地区", "六层归属", "指标代码", "指标名称", "最新值", "前值", "边际变化", "数据源", "同步时间"]
    raw_matrix = [raw_headers] + [[r.get(h, "") for h in raw_headers] for r in raw_records]
    ws_raw_log.clear()
    ws_raw_log.update(range_name="A1", values=raw_matrix, value_input_option="USER_ENTERED")

    # ================= 5. 更新 00_宏观驾驶舱_Dashboard (AI深度研判 + 36维风险雷达 + 多国对比) =================
    dash_matrix = [
        ["🌍 全球宏观第一性原理自动驾驶舱 (36维黑天鹅/灰犀牛雷达版)", "", "", "", "", "", "", "", f"同步时间: {NOW_STR}"],
        [report_text],
        [],
        ["🚨 36维系统性风险雷达 (BS-1~18 急性黑天鹅 | GR-1~18 慢性灰犀牛)"],
        ["风险编号", "风险类型与维度", "核心监测指标", "当前最新值", "黄灯警戒线", "红灯熔断线", "当前状态", "传导引爆机制", "距红线进度"],
    ]
    # 按距离红灯熔断线危急程度降序排列，最危险的排在最前面
    sorted_risks = sorted(risk_rows, key=lambda x: x[8], reverse=True)
    dash_matrix.extend(sorted_risks)
    dash_matrix.append([])
    dash_matrix.append(["🌐 全球主要经济体周期与外债脆弱性横向对比"])
    dash_matrix.append(["ISO3", "国家/地区", "实际GDP增速(%)", "CPI通胀(%)", "10Y国债(%)", "备注", "经常账户/GDP(%)", "财政赤字/GDP(%)", "短期外债/外储(%)"])
    dash_matrix.extend(country_rows)
    dash_matrix.append([])
    dash_matrix.append(["📰 今日全球顶级财经与央行核心头条 (LLM 推演输入源)"])
    for n in news_list:
        dash_matrix.append([n["source"], n["time"], n["title"]])

    ws_dash.clear()
    ws_dash.update(range_name="A1", values=dash_matrix, value_input_option="USER_ENTERED")


if __name__ == "__main__":
    print(f"[{NOW_STR}] 启动全球宏观数据库同步与 36 维风险扫描...")
    raw_fred, s_obs, s_dates, s_hist, lt_hist = fetch_fred_data()
    raw_mkt, cross_items, mkt_stats = fetch_market_assets(s_obs, s_hist, lt_hist)
    raw_cn = fetch_china_macro_akshare(s_obs, s_dates)
    raw_ext, wb_m, imf_m, ext_v = fetch_external_apis(s_obs, s_dates)
    news = fetch_global_macro_news()

    all_raw = raw_fred + raw_mkt + raw_cn + raw_ext
    risk_r, country_r, macro_r, ts_snap, report, meta = analyze_macro_and_blackswan(
        s_obs, s_dates, wb_m, imf_m, ext_v, mkt_stats, news
    )
    seeds = build_historical_seed_rows(s_hist)
    lt_h, lt_m = build_long_term_cycle_matrix(lt_hist)

    sync_all_sheets(
        all_raw, cross_items, risk_r, country_r, macro_r,
        ts_snap, seeds, lt_h, lt_m, news, report, meta
    )
    print(f"[{NOW_STR}] 同步完成！共写入 {len(all_raw)} 条基础指标，完成 {len(risk_r)} 维黑天鹅/灰犀牛扫描。")
