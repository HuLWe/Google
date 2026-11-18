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
FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()
_raw_ss = os.environ.get("SPREADSHEET_ID", "").strip()
_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", _raw_ss)
SPREADSHEET_ID = _match.group(1) if _match else _raw_ss.split("/")[0].split("?")[0].strip()
GCP_SA_JSON = json.loads(os.environ.get("GCP_SA_KEY", "{}"))

# AI 模型与中转站配置
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.starwish.fit/v1").strip().rstrip("/")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-6.2-astra").strip()
OPENAI_REASONING_EFFORT = os.environ.get("OPENAI_REASONING_EFFORT", "max").strip()
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

TZ_CST = timezone(timedelta(hours=8))
NOW_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d %H:%M:%S")
TODAY_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d")

# 工作表名称
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json, application/xml, text/xml, */*",
    })
    return session

HTTP = create_http_session()

# ================= 2. 宏观基础指标字典 (20维黑天鹅+灰犀牛) =================
FRED_SERIES = [
    # L1: 信用引擎与货币存量
    {"id": "M1SL", "country": "US", "layer": "L1-信用引擎", "freq": "M", "lt": False, "name": "美国M1存量 (十亿美元)"},
    {"id": "M2SL", "country": "US", "layer": "L1-信用引擎", "freq": "M", "lt": False, "name": "美国M2存量 (十亿美元)"},
    {"id": "TOTLL", "country": "US", "layer": "L1-信用引擎", "freq": "W", "lt": True, "name": "美国商业银行总信贷-H.8周频 (十亿美元)"},
    {"id": "BUSLOANS", "country": "US", "layer": "L1-信用引擎", "freq": "M", "lt": False, "name": "美国工商业贷款余额-H.8 (十亿美元)"},
    {"id": "DRTSCILM", "country": "US", "layer": "L1-信用引擎", "freq": "Q", "lt": False, "name": "美联储SLOOS银行收紧贷款比例 (%)"},
    {"id": "BAMLH0A0HYM2", "country": "US", "layer": "L1-信用引擎", "freq": "D", "lt": True, "name": "美国高收益债信用利差 (%)"},
    {"id": "BAMLC0A0CM", "country": "US", "layer": "L1-信用引擎", "freq": "D", "lt": False, "name": "美国投资级企业债信用利差 (%)"},
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
    {"id": "IRLTLT01JPM156N", "country": "JP", "layer": "L2-利率之锚", "freq": "M", "lt": False, "name": "日本10年期国债收益率 (%)"},
    {"id": "IRLTLT01DEM156N", "country": "DE", "layer": "L2-利率之锚", "freq": "M", "lt": False, "name": "德国(欧元区)10Y国债收益率 (%)"},
    # L3: 美国经济硬数据与财政生死线
    {"id": "PAYEMS", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "lt": False, "name": "美国非农就业总人数 (千人)"},
    {"id": "ICSA", "country": "US", "layer": "L3-就业硬数据", "freq": "W", "lt": True, "name": "美国周度初请失业金人数 (人)"},
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
    # L6: 黑天鹅 + 灰犀牛专属风险数据库
    {"id": "VXVCLS", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "D", "lt": False, "name": "CBOE 3个月期VIX3M指数"},
    {"id": "STLFSI4", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "lt": True, "name": "圣路易斯联储金融压力指数"},
    {"id": "NFCI", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "lt": False, "name": "芝加哥联储金融状况指数"},
    {"id": "DISBORROW", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "lt": False, "name": "美联储贴现窗口紧急借款总额 (百万美元)"},
    {"id": "DRCRELEXFACBS", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国商业地产(CRE)银行贷款违约率 (%)"},
    {"id": "DRCCLACBS", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国商业银行信用卡贷款违约率 (%)"},
    {"id": "DRSFRMACBS", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国居民住房抵押贷款违约率 (%)"},
    {"id": "PSAVERT", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "M", "lt": False, "name": "美国个人储蓄率占可支配收入比 (%)"},
    {"id": "TDSP", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "Q", "lt": False, "name": "美国家庭债务偿付占可支配收入比 (%)"},
    {"id": "DPSACBW027SBOG", "country": "US", "layer": "L6-灰犀牛雷达", "freq": "W", "lt": False, "name": "美国商业银行总存款余额 (十亿美元)"},
]

DAILY_WEEKLY_FRED_IDS = {x["id"] for x in FRED_SERIES if x["freq"] in ("D", "W")}
LT_FRED_IDS = {x["id"] for x in FRED_SERIES if x.get("lt")}

MARKET_ASSETS = [
    {"cat": "重要股指", "ticker": "^GSPC", "code": "SPX", "country": "US", "lt": True, "name": "标普500指数 (S&P 500)", "driver": "美股盈利预期 × 10Y贴现率"},
    {"cat": "重要股指", "ticker": "^NDX", "code": "NDX", "country": "US", "lt": True, "name": "纳斯达克100指数 (Nasdaq 100)", "driver": "远期现金流久期资产，对10Y TIPS极敏感"},
    {"cat": "重要股指", "ticker": "000300.SS", "code": "CSI300", "country": "CN", "lt": True, "name": "沪深300指数 (CSI 300)", "driver": "中国社融脉冲与M1-M2活化剪刀差"},
    {"cat": "风险晴雨表", "ticker": "KRE", "code": "US_REG_BANK", "country": "US", "lt": False, "name": "美国区域银行ETF (KRE)", "driver": "中小银行存款稳定性与商业地产坏账"},
    {"cat": "风险晴雨表", "ticker": "HYG", "code": "US_HY_ETF", "country": "US", "lt": False, "name": "美国高收益企业债ETF (HYG)", "driver": "企业信用利差与垃圾债再融资流动性"},
    {"cat": "全球货币", "ticker": "DX-Y.NYB", "code": "DXY", "country": "US", "lt": True, "name": "ICE美元指数 (DXY)", "driver": "全球离岸美元松紧总阀门"},
    {"cat": "全球货币", "ticker": "JPY=X", "code": "USDJPY", "country": "JP", "lt": True, "name": "美元兑日元 (USD/JPY)", "driver": "全球套息交易(Carry Trade)风向标"},
    {"cat": "黄金贵金属", "ticker": "GC=F", "code": "GOLD", "country": "GLOBAL", "lt": True, "name": "COMEX黄金期货", "driver": "10Y TIPS实际利率 + 央行去美元化购金"},
    {"cat": "大宗商品", "ticker": "BZ=F", "code": "BRENT", "country": "GLOBAL", "lt": True, "name": "布伦特原油期货", "driver": "全球通胀预期与供给冲击滞胀扳机"},
    {"cat": "大宗商品", "ticker": "HG=F", "code": "COPPER", "country": "GLOBAL", "lt": True, "name": "COMEX精炼铜", "driver": "全球制造业与实体需求"},
    {"cat": "波动率", "ticker": "^VIX", "code": "VIX", "country": "US", "lt": True, "name": "CBOE VIX恐慌指数", "driver": "美股期权尾部对冲成本"},
]

CRYPTO_ASSETS = [
    {"cg_id": "bitcoin", "yf_ticker": "BTC-USD", "code": "BTC", "lt": True, "name": "比特币 (Bitcoin)", "driver": "美元流动性放大器"},
]

COUNTRIES_LIST = ["USA", "CHN", "JPN", "DEU", "IND", "BRA", "IDN", "TUR"]

# ================= 3. 大模型研判生成与接口兼容修复 =================
def generate_macro_synthesis(context_payload, fallback_report):
    """通过 OpenAI / OpenRouter / Gemini 生成高密度宏观分析报告"""
    prompt = (
        "你是首席宏观策略师。结合【六层硬数据 + 20维黑天鹅/灰犀牛数据库 + 今日热点】，"
        "输出一段中文自动研判报告（550字以内）。\n"
        "结构要求：\n"
        "1.【周期定位与新闻催化】；2.【美元水管与财政生死线】；3.【跨资产定价与利差博弈】；"
        "4.【黑天鹅与灰犀牛20维扫描】；5.【三情景概率与强制证伪条件】。\n\n"
        f"数据上下文 JSON:\n{json.dumps(context_payload, ensure_ascii=False)}"
    )

    if OPENAI_API_KEY:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        # 标准 Chat Completions 端点调用（兼容中转站）
        try:
            chat_url = f"{OPENAI_BASE_URL}/chat/completions"
            body_chat = {
                "model": OPENAI_MODEL,
                "reasoning_effort": OPENAI_REASONING_EFFORT,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 3000,
            }
            resp = HTTP.post(chat_url, headers=headers, json=body_chat, timeout=(15, 120))
            if resp.status_code == 200:
                txt = resp.json()["choices"][0]["message"]["content"].strip()
                if txt:
                    return f"【{OPENAI_MODEL} (思考:{OPENAI_REASONING_EFFORT}) 跨因子研判 | {NOW_STR}】\n{txt}"
        except Exception as e:
            print(f"[WARN] Relay Chat Completions failed: {e}")

    if OPENROUTER_API_KEY:
        try:
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
            body = {
                "model": f"openai/{OPENAI_MODEL}" if "/" not in OPENAI_MODEL else OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
            }
            resp = HTTP.post(url, headers=headers, json=body, timeout=(15, 120))
            if resp.status_code == 200:
                return f"【OpenRouter 研判 | {NOW_STR}】\n{resp.json()['choices'][0]['message']['content'].strip()}"
        except Exception as e:
            print(f"[WARN] OpenRouter failed: {e}")

    return fallback_report

# ================= 4. Google Sheets 数据同步补全 =================
def get_or_create_ws(sh, title, rows=300, cols=26):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def sync_all_sheets(raw_records, cross_asset_items, risk_rows, country_rows, macro_rows, ts_dict, seed_rows, lt_headers, lt_matrix, news_list, report_text, meta_ctx):
    if not raw_records:
        raise RuntimeError("无有效数据，取消写入以保护表格。")
        
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_dash = get_or_create_ws(sh, TAB_DASH, rows=135, cols=25)
    ws_ts = get_or_create_ws(sh, TAB_TS, rows=2000, cols=110)
    ws_raw_log = get_or_create_ws(sh, TAB_RAW_LOG, rows=400, cols=16)
    ws_lt = get_or_create_ws(sh, TAB_LT, rows=300, cols=60)

    # 1. 更新长期周期库 LongTermCycle
    if lt_matrix and len(lt_matrix) > 1:
        ws_lt.clear()
        ws_lt.update(range_name="A1", values=lt_matrix, value_input_option="USER_ENTERED")

    # 2. 更新日频时序库 TimeSeries (补齐之前截断的逻辑)
    existing = [r for r in ws_ts.get_all_values() if r and len(r) > 0 and str(r[0]).strip()]
    all_keys = ["Date"] + sorted(ts_dict.keys())
    
    if len(existing) <= 1:
        headers = all_keys
        matrix = [headers]
        new_row = [TODAY_STR] + [ts_dict.get(col, "") for col in headers[1:]]
        matrix.append(new_row)
        ws_ts.clear()
        ws_ts.update(range_name="A1", values=matrix, value_input_option="USER_ENTERED")
    else:
        headers = existing[0]
        new_cols = [k for k in all_keys if k not in headers]
        if new_cols:
            headers = headers + new_cols
            ws_ts.update(range_name="A1", values=[headers], value_input_option="USER_ENTERED")
        
        # 追加今日新行
        new_row = [TODAY_STR] + [ts_dict.get(col, "") for col in headers[1:]]
        ws_ts.append_row(new_row, value_input_option="USER_ENTERED")

    # 3. 更新全量指标底表与日志
    raw_df = pd.DataFrame(raw_records)
    if not raw_df.empty:
        log_matrix = [raw_df.columns.tolist()] + raw_df.values.tolist()
        ws_raw_log.clear()
        ws_raw_log.update(range_name="A1", values=log_matrix, value_input_option="USER_ENTERED")

    # 4. 更新宏观驾驶舱 Dashboard
    ws_dash.update(range_name="A1", values=[[report_text]], value_input_option="USER_ENTERED")
    print(f"[SUCCESS] 数据及大模型研判成功同步至 Google Sheets! 时间: {NOW_STR}")

# 主入口
if __name__ == "__main__":
    print(f"[START] 开始执行宏观数据 ETL 及大模型研判 pipeline... 当前时间: {NOW_STR}")
    # 此处可依次调用 fetch_fred_data, fetch_market_assets, analyze_macro_and_blackswan 等主过程
