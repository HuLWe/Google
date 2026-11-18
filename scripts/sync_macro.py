import os
import re
import json
import time
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

# 可选：若配置了 Gemini 或 OpenRouter Key，自动启用大模型深度五步推演（未配置则自动走高级多因子规则引擎）
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()

TZ_CST = timezone(timedelta(hours=8))
NOW_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d %H:%M:%S")
TODAY_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d")

TAB_DASH = "00_宏观驾驶舱_Dashboard"
TAB_MKT_MAC = "01_跨资产与宏观硬数据_MarketsAndMacro"
TAB_TS = "02_历史时序库_TimeSeries"
TAB_RAW_LOG = "03_全量指标底表与日志_RawAndLog"

LEGACY_TABS = [
    "00_总览与自动研判_Dashboard",
    "01_最新快照_Snapshot",
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    return session


HTTP = create_http_session()

# ================= 2. 宏观基础指标字典 (FRED — 全六层补齐) =================
FRED_SERIES = [
    # L1: 信用引擎与货币存量 (补齐美国H.8商业银行总信贷与工商业贷款)
    {"id": "M1SL", "country": "US", "layer": "L1-信用引擎", "freq": "M", "name": "美国M1存量 (十亿美元)"},
    {"id": "M2SL", "country": "US", "layer": "L1-信用引擎", "freq": "M", "name": "美国M2存量 (十亿美元)"},
    {"id": "TOTLL", "country": "US", "layer": "L1-信用引擎", "freq": "W", "name": "美国商业银行总信贷-H.8周频 (十亿美元)"},
    {"id": "BUSLOANS", "country": "US", "layer": "L1-信用引擎", "freq": "M", "name": "美国工商业贷款余额-H.8 (十亿美元)"},
    {"id": "DRTSCILM", "country": "US", "layer": "L1-信用引擎", "freq": "Q", "name": "美联储SLOOS银行收紧贷款比例 (%)"},
    {"id": "BAMLH0A0HYM2", "country": "US", "layer": "L1-信用引擎", "freq": "D", "name": "美国高收益债信用利差 (%)"},
    # L2: 美元水管与利率锚
    {"id": "WALCL", "country": "US", "layer": "L2-流动性管道", "freq": "W", "name": "美联储总资产 (百万美元)"},
    {"id": "WDTGAL", "country": "US", "layer": "L2-流动性管道", "freq": "W", "name": "财政部TGA账户余额-周三水平 (百万美元)"},
    {"id": "RRPONTSYD", "country": "US", "layer": "L2-流动性管道", "freq": "D", "name": "隔夜逆回购 ON RRP (十亿美元)"},
    {"id": "WRESBAL", "country": "US", "layer": "L2-流动性管道", "freq": "W", "name": "商业银行准备金余额 (百万美元)"},
    {"id": "IORB", "country": "US", "layer": "L2-流动性管道", "freq": "D", "name": "美联储准备金余额利率 IORB (%)"},
    {"id": "DFF", "country": "US", "layer": "L2-利率之锚", "freq": "D", "name": "联邦基金有效利率 EFFR (%)"},
    {"id": "DGS2", "country": "US", "layer": "L2-利率之锚", "freq": "D", "name": "美国2年期国债收益率 (%)"},
    {"id": "DGS10", "country": "US", "layer": "L2-利率之锚", "freq": "D", "name": "美国10年期国债收益率 (%)"},
    {"id": "DFII10", "country": "US", "layer": "L2-利率之锚", "freq": "D", "name": "美国10Y TIPS实际利率 (%)"},
    {"id": "T10Y2Y", "country": "US", "layer": "L2-利率之锚", "freq": "D", "name": "美债10Y-2Y期限利差 (%)"},
    {"id": "IRLTLT01JPM156N", "country": "JP", "layer": "L2-利率之锚", "freq": "M", "name": "日本10年期国债收益率 (%)"},
    {"id": "IRLTLT01DEM156N", "country": "DE", "layer": "L2-利率之锚", "freq": "M", "name": "德国(欧元区)10Y国债收益率 (%)"},
    # L3: 美国经济硬数据与财政生死线 (补齐月度赤字、联邦利息支出、财政收入、名义GDP)
    {"id": "PAYEMS", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "name": "美国非农就业总人数 (千人)"},
    {"id": "ICSA", "country": "US", "layer": "L3-就业硬数据", "freq": "W", "name": "美国周度初请失业金人数 (人)"},
    {"id": "UNRATE", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "name": "美国失业率 (%)"},
    {"id": "SAHMREALTIME", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "name": "官方萨姆规则衰退指标 (%)"},
    {"id": "CES0500000003", "country": "US", "layer": "L3-就业硬数据", "freq": "M", "name": "美国私营部门平均时薪 (美元/小时)"},
    {"id": "INDPRO", "country": "US", "layer": "L3-工业与消费", "freq": "M", "name": "美国工业生产总指数 (2017=100)"},
    {"id": "RSAFS", "country": "US", "layer": "L3-工业与消费", "freq": "M", "name": "美国零售与食品服务销售额 (百万美元)"},
    {"id": "UMCSENT", "country": "US", "layer": "L3-工业与消费", "freq": "M", "name": "密歇根大学消费者信心指数"},
    {"id": "CPIAUCSL", "country": "US", "layer": "L3-通胀硬数据", "freq": "M", "name": "美国CPI总体物价指数"},
    {"id": "PCEPILFE", "country": "US", "layer": "L3-通胀硬数据", "freq": "M", "name": "美国核心PCE物价指数"},
    {"id": "PPIACO", "country": "US", "layer": "L3-通胀硬数据", "freq": "M", "name": "美国PPI全商品生产者物价指数"},
    {"id": "MTSDS133FMS", "country": "US", "layer": "L3-财政生死线", "freq": "M", "name": "美国联邦月度财政盈余/赤字 (百万美元)"},
    {"id": "A091RC1Q027SBEA", "country": "US", "layer": "L3-财政生死线", "freq": "Q", "name": "美国联邦政府利息支出年率 (十亿美元)"},
    {"id": "FGRECPT", "country": "US", "layer": "L3-财政生死线", "freq": "Q", "name": "美国联邦政府经常性财政收入年率 (十亿美元)"},
    {"id": "GDP", "country": "US", "layer": "L3-财政生死线", "freq": "Q", "name": "美国名义GDP年率 (十亿美元)"},
    # L3: 欧元区与日本宏观硬数据
    {"id": "CP0000EZ19M086NEST", "country": "EA", "layer": "L3-通胀硬数据", "freq": "M", "name": "欧元区调和CPI物价指数 (HICP)"},
    {"id": "LRHUTTTTEZM156S", "country": "EA", "layer": "L3-就业硬数据", "freq": "M", "name": "欧元区失业率 (%)"},
    {"id": "JPNPRMNTO01GPSAM", "country": "JP", "layer": "L3-工业与消费", "freq": "M", "name": "日本制造业生产环比增速 (%)"},
    {"id": "JPNCPIALLMINMEI", "country": "JP", "layer": "L3-通胀硬数据", "freq": "M", "name": "日本全国CPI物价指数"},
    # L4/L5/L6: 跨境资金流量、实物库存与系统性金融压力
    {"id": "DTWEXBGS", "country": "US", "layer": "L4-跨境汇率", "freq": "D", "name": "美联储广义贸易加权美元指数"},
    {"id": "FDHBFIN", "country": "US", "layer": "L4-跨境资本流", "freq": "Q", "name": "海外投资者持有美债总额-TIC (十亿美元)"},
    {"id": "WCESTUS1", "country": "US", "layer": "L5-实物库存", "freq": "W", "name": "EIA美国商业原油库存-不含SPR (千桶)"},
    {"id": "VXVCLS", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "D", "name": "CBOE 3个月期VIX3M指数"},
    {"id": "STLFSI4", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "name": "圣路易斯联储金融压力指数"},
    {"id": "NFCI", "country": "US", "layer": "L6-黑天鹅雷达", "freq": "W", "name": "芝加哥联储金融状况指数"},
]

DAILY_WEEKLY_FRED_IDS = {x["id"] for x in FRED_SERIES if x["freq"] in ("D", "W")}

# ================= 3. 全球跨资产实时行情字典 =================
MARKET_ASSETS = [
    {"cat": "重要股指", "ticker": "^GSPC", "code": "SPX", "country": "US", "name": "标普500指数 (S&P 500)", "driver": "美股盈利预期 × 10Y贴现率"},
    {"cat": "重要股指", "ticker": "^NDX", "code": "NDX", "country": "US", "name": "纳斯达克100指数 (Nasdaq 100)", "driver": "远期现金流久期资产，对10Y TIPS极敏感"},
    {"cat": "重要股指", "ticker": "000300.SS", "code": "CSI300", "country": "CN", "name": "沪深300指数 (CSI 300)", "driver": "中国社融脉冲与M1-M2活化剪刀差"},
    {"cat": "重要股指", "ticker": "^HSI", "code": "HSI", "country": "HK", "name": "香港恒生指数 (Hang Seng)", "driver": "中国基本面分子 × 美元流动性分母双重定价"},
    {"cat": "重要股指", "ticker": "^N225", "code": "N225", "country": "JP", "name": "日经225指数 (Nikkei 225)", "driver": "日元汇率与日本央行加息路径"},
    {"cat": "重要股指", "ticker": "^GDAXI", "code": "DAX", "country": "DE", "name": "德国DAX指数", "driver": "欧元区制造业周期与欧央行利率路径"},
    {"cat": "重要股指", "ticker": "EEM", "code": "EEM", "country": "EM", "name": "MSCI新兴市场ETF", "driver": "强美元周期与跨境热钱风险偏好"},
    {"cat": "全球工业", "ticker": "EXI", "code": "GLOBAL_IND", "country": "GLOBAL", "name": "iShares全球工业指数ETF (EXI)", "driver": "全球跨国制造业资本开支与贸易景气度"},
    {"cat": "全球工业", "ticker": "XLI", "code": "US_IND_ETF", "country": "US", "name": "美国工业板块指数ETF (XLI)", "driver": "北美基建回流与制造业PMI市场领先指标"},
    {"cat": "全球货币", "ticker": "DX-Y.NYB", "code": "DXY", "country": "US", "name": "ICE美元指数 (DXY)", "driver": "全球离岸美元松紧总阀门"},
    {"cat": "全球货币", "ticker": "JPY=X", "code": "USDJPY", "country": "JP", "name": "美元兑日元 (USD/JPY)", "driver": "全球套息交易(Carry Trade)核心风向标"},
    {"cat": "全球货币", "ticker": "CNH=X", "code": "USDCNH", "country": "CN", "name": "美元兑离岸人民币 (USD/CNH)", "driver": "中美利差与出口企业结汇意愿"},
    {"cat": "全球货币", "ticker": "EURUSD=X", "code": "EURUSD", "country": "EA", "name": "欧元兑美元 (EUR/USD)", "driver": "欧美真实利差与能源贸易条件"},
    {"cat": "全球货币", "ticker": "GBPUSD=X", "code": "GBPUSD", "country": "UK", "name": "英镑兑美元 (GBP/USD)", "driver": "英美利差与英国财政信用溢价"},
    {"cat": "全球货币", "ticker": "AUDUSD=X", "code": "AUDUSD", "country": "AU", "name": "澳元兑美元 (AUD/USD)", "driver": "全球工业大宗商品周期与亚洲需求"},
    {"cat": "黄金贵金属", "ticker": "GC=F", "code": "GOLD", "country": "GLOBAL", "name": "COMEX黄金期货 (美元/盎司)", "driver": "10Y TIPS实际利率 + 央行去美元化购金"},
    {"cat": "黄金贵金属", "ticker": "SI=F", "code": "SILVER", "country": "GLOBAL", "name": "COMEX白银期货 (美元/盎司)", "driver": "金融属性(随黄金) + 光伏/工业需求弹性"},
    {"cat": "大宗商品", "ticker": "CL=F", "code": "WTI", "country": "GLOBAL", "name": "WTI原油期货 (美元/桶)", "driver": "北美现货库存与OPEC+供给约束"},
    {"cat": "大宗商品", "ticker": "BZ=F", "code": "BRENT", "country": "GLOBAL", "name": "布伦特原油期货 (美元/桶)", "driver": "全球通胀预期与供给冲击滞胀扳机"},
    {"cat": "大宗商品", "ticker": "HG=F", "code": "COPPER", "country": "GLOBAL", "name": "COMEX精炼铜 (美元/磅)", "driver": "全球制造业与电网AI基建实体需求"},
    {"cat": "大宗商品", "ticker": "NG=F", "code": "NATGAS", "country": "US", "name": "NYMEX天然气期货", "driver": "欧美能源成本与季节性库存周期"},
    {"cat": "大宗商品", "ticker": "ZW=F", "code": "WHEAT", "country": "GLOBAL", "name": "CBOT小麦期货 (美分/蒲式耳)", "driver": "全球食品通胀与新兴市场社会稳定"},
    {"cat": "波动率", "ticker": "^VIX", "code": "VIX", "country": "US", "name": "CBOE VIX恐慌指数", "driver": "美股期权隐含尾部对冲成本"},
]

CRYPTO_ASSETS = [
    {"cg_id": "bitcoin", "yf_ticker": "BTC-USD", "code": "BTC", "name": "比特币 (Bitcoin)", "driver": "全球美元净流动性高弹性放大器 + 现货ETF资金流"},
    {"cg_id": "ethereum", "yf_ticker": "ETH-USD", "code": "ETH", "name": "以太坊 (Ethereum)", "driver": "链上金融流动性与风险偏好Beta"},
    {"cg_id": "solana", "yf_ticker": "SOL-USD", "code": "SOL", "name": "Solana (SOL)", "driver": "高Beta加密投机情绪与动物精神因子"},
]

COUNTRIES_LIST = ["USA", "CHN", "JPN", "DEU", "IND", "BRA", "IDN", "TUR"]


def fetch_yahoo_chart(ticker):
    for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
        url = f"https://{host}/v8/finance/chart/{ticker}?range=1mo&interval=1d"
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
    """获取 SPY ETF 滚动/远期市盈率以计算股权风险溢价 ERP = 1/PE - 10Y美债"""
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


def fetch_market_assets(series_obs, series_hist_dates):
    raw_records = []
    cross_asset_items = []
    market_stats = {}

    def calc_pct(arr, idx):
        if len(arr) > idx and arr[idx] != 0:
            return round((arr[0] / arr[idx] - 1) * 100, 2)
        return None

    for item in MARKET_ASSETS:
        dt_str, prices, date_val_map = fetch_yahoo_chart(item["ticker"])
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
        time.sleep(0.06)

    # 加密资产
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
        _, yf_prices, yf_date_map = fetch_yahoo_chart(c_item["yf_ticker"])
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
    for item in FRED_SERIES:
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
            # 仅对日/周频指标保留日历映射用于20日时序回填，避免月度水平直线污染波动率公式
            if item["id"] in DAILY_WEEKLY_FRED_IDS:
                series_hist_dates[item["id"]] = {o["date"]: float(o["value"]) for o in obs[:30]}
            val_curr = vals[0]
            val_prev = vals[1] if len(vals) > 1 else val_curr
            records.append({
                "观测日期": obs[0]["date"], "国家/地区": item["country"], "六层归属": item["layer"],
                "指标代码": item["id"], "指标名称": item["name"],
                "最新值": round(val_curr, 4), "前值": round(val_prev, 4), "边际变化": round(val_curr - val_prev, 4),
                "数据源": "FRED API", "同步时间": NOW_STR,
            })
            time.sleep(0.08)
        except Exception as e:
            print(f"[WARN] FRED {item['id']} failed: {e}")
    return records, series_obs, series_dates, series_hist_dates


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

    # 1. 中国官方制造业与非制造业 PMI
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

    # 2. 中国官方口径 M1 同比 & M2 同比 (解决 FRED 口径滞后与统计修订问题)
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

    # 3. 中国规模以上工业增加值同比 (%)
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

    # 4. 中国社会融资规模增量 (亿元)
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

    # 5. 中国 CPI & PPI 同比 (%)
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

    # 0. 中债 2Y/10Y 日频收益率
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

    # 1. NY Fed 日频 SOFR
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

    # 2. 美国财政部近期 10Y/30Y 美债拍卖投标倍数 + 尾部利差 (Tail)
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

    # 3. IMF DataMapper 多国年度结构预测
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

    # 4. World Bank 新兴市场短期外债 & 外汇储备
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


def build_historical_seed_rows(series_hist_dates, ts_snapshot_dict):
    """仅对日/周频金融指标回填近20个交易日序列，避免低频月度直线扭曲Z-Score与分位数"""
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
        if all(k in row_dict for k in ["WALCL", "WDTGAL", "RRPONTSYD"]):
            row_dict["NET_LIQUIDITY"] = round((row_dict["WALCL"] - row_dict["WDTGAL"]) / 1000.0 - row_dict["RRPONTSYD"], 2)
        if all(k in row_dict for k in ["DGS10", "DGS2"]):
            row_dict["SPREAD_10Y2Y"] = round(row_dict["DGS10"] - row_dict["DGS2"], 3)
        if all(k in row_dict for k in ["DGS10", "DFII10"]):
            row_dict["BREAKEVEN_10Y"] = round(row_dict["DGS10"] - row_dict["DFII10"], 3)
        if all(k in row_dict for k in ["COPPER", "GOLD"]) and row_dict["GOLD"] > 0:
            row_dict["COPPER_GOLD_RATIO"] = round((row_dict["COPPER"] * 1000) / row_dict["GOLD"], 3)
        if all(k in row_dict for k in ["GOLD", "BRENT"]) and row_dict["BRENT"] > 0:
            row_dict["GOLD_OIL_RATIO"] = round(row_dict["GOLD"] / row_dict["BRENT"], 2)
        if "WALCL" in row_dict:
            row_dict["WALCL_B"] = round(row_dict["WALCL"] / 1000.0, 2)
        if "WDTGAL" in row_dict:
            row_dict["TGA_B"] = round(row_dict["WDTGAL"] / 1000.0, 2)
        seed_rows.append(row_dict)
    return seed_rows


def generate_macro_synthesis(context_payload, fallback_report):
    """若配置了 Gemini 或 OpenRouter Key，调用大模型进行深度跨因子五步推演；否则使用防滞后多因子规则引擎"""
    prompt = (
        "你是严格遵循《宏观经济六层第一性原理实战通关总纲》的首席宏观策略师。"
        "请根据以下实时抓取的六层全量硬数据与跨资产快照，输出一段精炼、硬核的中文自动研判报告（不超过450字）。\n"
        "严格按五步结构输出：\n"
        "1.【周期与机制定调】：结合高频市场定价与低频硬数据，识别是否存在“低频数据滞后 vs 高频市场抢跑背离”，判断当前处于四象限哪一象限及交易主线（分子盈利 vs 分母贴现）；\n"
        "2.【美元水管与财政生死线】：点评美联储净流动性边际变化、美债曲线形态（牛陡/熊陡）及美国财政利息吞噬率/r-g压力；\n"
        "3.【跨资产背离与资金流】：分析实际利率与黄金/纳指、铜金比与原油、中美/美日利差背后的真实资金博弈；\n"
        "4.【黑天鹅 8 维扫描】：评估当前距离熔断线最近的风险源；\n"
        "5.【三情景概率与强制证伪条件】：给出基准/乐观/悲观概率分布，并写出未来10个交易日推翻基准判断的2条具体量化证伪条件。\n\n"
        f"实时数据JSON：\n{json.dumps(context_payload, ensure_ascii=False)}"
    )

    if GEMINI_API_KEY:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
            resp = HTTP.post(url, json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=(10, 35))
            if resp.status_code == 200:
                txt = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                if txt:
                    return f"【AI 深度第一性原理研判 | {NOW_STR}】\n{txt}"
        except Exception as e:
            print(f"[WARN] Gemini synthesis failed, falling back to rule engine: {e}")

    if OPENROUTER_API_KEY:
        try:
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
            body = {"model": "google/gemini-2.5-flash", "messages": [{"role": "user", "content": prompt}]}
            resp = HTTP.post(url, headers=headers, json=body, timeout=(10, 35))
            if resp.status_code == 200:
                txt = resp.json()["choices"][0]["message"]["content"].strip()
                if txt:
                    return f"【AI 深度第一性原理研判 | {NOW_STR}】\n{txt}"
        except Exception as e:
            print(f"[WARN] OpenRouter synthesis failed, falling back to rule engine: {e}")

    return fallback_report


def analyze_macro_and_blackswan(s_obs, s_dates, wb_metrics, imf_metrics, extra_vals, market_stats):
    blackswan_rows = []
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

    # 美国财政生死线: 利息吞噬率 & 名义GDP增速
    fed_int, fed_rev = get("A091RC1Q027SBEA"), get("FGRECPT")
    interest_rev_ratio = None
    if fed_int and fed_rev and fed_rev > 0:
        interest_rev_ratio = round((fed_int / fed_rev) * 100, 2)
        ts_snapshot_dict["US_INTEREST_REV_RATIO"] = interest_rev_ratio
        macro_indicator_rows.append(["美国 (US)", "财政(L3)", "联邦利息支出 / 财政收入 (%)", f"{interest_rev_ratio:.2f}%", f"利息 ${fed_int:.0f}B", s_dates.get("A091RC1Q027SBEA", "-"), "⚠️ 逼近或突破20%财政空间锁定红线" if interest_rev_ratio >= 18.0 else "财政付息压力可控"])

    us_nom_gdp_yoy = calc_yoy("GDP", lag=4)
    if us_nom_gdp_yoy is not None:
        ts_snapshot_dict["US_NOM_GDP_YOY"] = us_nom_gdp_yoy

    eia_inv_0, eia_inv_1 = get("WCESTUS1", 0), get("WCESTUS1", 1)
    if eia_inv_0 is not None and eia_inv_1 is not None:
        inv_chg = round((eia_inv_0 - eia_inv_1) / 1000.0, 2)
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

    # ================= B. 衍生指标 Python 侧状态计算 =================
    walcl, tga, rrp = get("WALCL"), get("WDTGAL"), get("RRPONTSYD")
    walcl_p, tga_p, rrp_p = get("WALCL", 1), get("WDTGAL", 1), get("RRPONTSYD", 1)
    net_liq, net_liq_chg = None, 0.0
    if all(v is not None for v in [walcl, tga, rrp]):
        net_liq = round((walcl - tga) / 1000.0 - rrp, 2)
        ts_snapshot_dict["NET_LIQUIDITY"] = net_liq
        ts_snapshot_dict["WALCL_B"] = round(walcl / 1000.0, 2)
        ts_snapshot_dict["TGA_B"] = round(tga / 1000.0, 2)
        if all(v is not None for v in [walcl_p, tga_p, rrp_p]):
            net_liq_chg = round(net_liq - ((walcl_p - tga_p) / 1000.0 - rrp_p), 2)

    y10, y2 = get("DGS10"), get("DGS2")
    y10_5, y2_5 = get("DGS10", 5), get("DGS2", 5)
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
    if gold_stat.get("price") and copper_stat.get("price"):
        ts_snapshot_dict["COPPER_GOLD_RATIO"] = round((copper_stat["price"] * 1000) / gold_stat["price"], 3)
    if gold_stat.get("price") and brent_stat.get("price"):
        ts_snapshot_dict["GOLD_OIL_RATIO"] = round(gold_stat["price"] / brent_stat["price"], 2)

    gold_5d = gold_stat.get("pct_5d")
    gold_regime_note = ""
    if tips_0 is not None and tips_5 is not None and gold_5d is not None:
        tips_5d_chg = round(tips_0 - tips_5, 3)
        if tips_5d_chg > 0.05 and gold_5d > 1.0:
            gold_regime_note = "⚠️ 黄金与10Y TIPS实际利率同涨背离：央行去美元化购金主导定价。"

    # ================= C. 黑天鹅 8 维全满编雷达 (BS-1 ~ BS-8) =================
    red_alerts, yellow_alerts = [], []

    sofr, iorb = extra_vals.get("SOFR"), get("IORB")
    if sofr is not None and iorb is not None:
        sofr_sp = round((sofr - iorb) * 100, 1)
        ratio_pct = round(max(0.0, min(1.5, sofr_sp / 25.0)), 3)
        if sofr_sp >= 25:
            st = "🔴 红灯熔断"
            red_alerts.append(f"SOFR-IORB 利差飙升至 +{sofr_sp}bps")
        elif sofr_sp >= 10:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"SOFR-IORB 利差走阔至 +{sofr_sp}bps")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-1", "美元回购管道", "SOFR - IORB 利差 (bps)", sofr_sp, "10 bps", "25 bps", st, "衡量银行间美元准备金充裕度，飙升预示钱荒", ratio_pct])

    hy = get("BAMLH0A0HYM2")
    if hy is not None:
        hy_bps = round(hy * 100, 0)
        ratio_pct = round(max(0.0, min(1.5, hy_bps / 500.0)), 3)
        if hy_bps >= 500:
            st = "🔴 红灯熔断"
            red_alerts.append(f"高收益债利差突破 {hy_bps}bps")
        elif hy_bps >= 350:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"高收益债利差升至 {hy_bps}bps")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-2", "企业信用违约", "高收益债信用利差 (bps)", hy_bps, "350 bps", "500 bps", st, "垃圾债融资成本飙升将引发再融资断裂与违约潮", ratio_pct])

    fsi = get("STLFSI4")
    if fsi is not None:
        ratio_pct = round(max(0.0, min(1.5, (fsi + 1.0) / 3.0)), 3)  # -1为极宽松，+2.0为100%熔断
        if fsi >= 2.0:
            st = "🔴 红灯熔断"
            red_alerts.append(f"金融压力指数 STLFSI4 达 {fsi}")
        elif fsi >= 0.5:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"金融压力指数 STLFSI4 升至 {fsi}")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-3", "综合金融压力", "圣路易斯金融压力 STLFSI4", round(fsi, 2), "0.50", "2.00", st, "0为历史均值，突破2.0代表系统性金融压力爆发", ratio_pct])

    spx_5d = market_stats.get("SPX", {}).get("pct_5d")
    dxy_5d = market_stats.get("DXY", {}).get("pct_5d")
    if all(v is not None for v in [spx_5d, gold_5d, dxy_5d, y10, y10_5]):
        y10_5d_bps = round((y10 - y10_5) * 100, 1)
        summary_str = f"SPX:{spx_5d:+.1f}% | 金:{gold_5d:+.1f}% | 10Y:{y10_5d_bps:+.0f}bp"
        stress_score = max(0.0, (-spx_5d / 4.0) * 0.5 + (-gold_5d / 2.0) * 0.5)
        ratio_pct = round(min(1.5, stress_score), 3)
        if spx_5d <= -4.0 and gold_5d <= -2.0 and y10_5d_bps >= 20 and dxy_5d >= 1.5:
            st = "🔴 红灯熔断"
            red_alerts.append(f"现金为王流动性挤兑 ({summary_str})")
        elif spx_5d <= -2.5 and gold_5d <= -1.0:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"股金同跌流动性趋紧 ({summary_str})")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-4", "现金为王挤兑", "5日股/金/债联动状态", summary_str, "股金同跌", "股金暴跌+债率飙升", st, "检测Margin Call下机构无差别抛售黄金美债换美元", ratio_pct])

    jpy_5d = market_stats.get("USDJPY", {}).get("pct_5d")
    if jpy_5d is not None:
        ratio_pct = round(max(0.0, min(1.5, -jpy_5d / 4.0)), 3)
        if jpy_5d <= -4.0:
            st = "🔴 红灯熔断"
            red_alerts.append(f"USD/JPY 5日暴跌 {jpy_5d}%")
        elif jpy_5d <= -2.5:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"USD/JPY 5日下跌 {jpy_5d}%")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-5", "套息交易踩踏", "USD/JPY 5日涨跌幅 (%)", jpy_5d, "-2.5%", "-4.0%", st, "日元急速升值迫使全球低息日元融资盘平仓抛售美股", ratio_pct])

    vix = market_stats.get("VIX", {}).get("price")
    vix3m = get("VXVCLS")
    if vix is not None and vix3m is not None and vix3m > 0:
        vix_ratio = round(vix / vix3m, 2)
        ratio_pct = round(max(0.0, min(1.5, max(vix / 35.0, vix_ratio / 1.15))), 3)
        if vix >= 35 or vix_ratio >= 1.15:
            st = "🔴 红灯熔断"
            red_alerts.append(f"VIX={vix}, VIX/VIX3M={vix_ratio}")
        elif vix >= 25 or vix_ratio >= 1.0:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"VIX={vix}, VIX/VIX3M={vix_ratio}")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-6", "波动率倒挂", f"VIX/VIX3M (VIX={vix})", vix_ratio, "1.00", "1.15", st, "短端隐含波动率高于3个月远期，代表短期尾部挤兑", ratio_pct])

    # BS-7: 新兴市场外债覆盖率
    em_ratios = {}
    for iso3 in COUNTRIES_LIST:
        res = wb_metrics.get((iso3, "FI.RES.TOTL.CD"))
        debt = wb_metrics.get((iso3, "DT.DOD.DSTC.CD"))
        if res and debt and res > 0:
            em_ratios[iso3] = round((debt / res) * 100, 1)
    if em_ratios:
        worst_iso = max(em_ratios, key=em_ratios.get)
        worst_val = em_ratios[worst_iso]
        ratio_pct = round(min(1.5, worst_val / 100.0), 3)
        if worst_val > 100:
            st = "🔴 红灯熔断"
            red_alerts.append(f"EM外债风险({worst_iso} 短期外债/外储={worst_val}%)")
        elif worst_val > 50:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"EM外债警戒({worst_iso} 短期外债/外储={worst_val}%)")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-7", f"EM外债({worst_iso})", "最大短期外债/外储比率 (%)", worst_val, "50.0%", "100.0%", st, "外储不足以覆盖1年内到期外债，强美元下易发货币危机", ratio_pct])

    # BS-8: 美债拍卖流动性枯竭
    btc_auc = extra_vals.get("UST_BID_COVER")
    tail_bps = extra_vals.get("UST_TAIL_BPS", 0.0)
    if btc_auc is not None and btc_auc > 0:
        ratio_pct = round(max(0.0, min(1.5, max(2.10 / btc_auc, tail_bps / 3.0))), 3)
        if btc_auc <= 2.10 or tail_bps >= 3.0:
            st = "🔴 红灯熔断"
            red_alerts.append(f"美债拍卖需求枯竭(倍数={btc_auc}, Tail={tail_bps}bps)")
        elif btc_auc <= 2.25 or tail_bps >= 1.5:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"美债拍卖需求偏弱(倍数={btc_auc}, Tail={tail_bps}bps)")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-8", "美债拍卖失灵", f"投标倍数(Tail={tail_bps}bp)", btc_auc, "2.25x", "2.10x", st, "长债一级市场投标不足与尾部利差扩大预示财政供给冲击", ratio_pct])

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

    # ================= E. 生成五步中文研判报告 (防滞后规则引擎 + 可选AI增强) =================
    sahm = get("SAHMREALTIME") or 0.0
    tips10 = tips_0 or 0.0
    growth_weak = (sahm >= 0.5) or (nfp_add_10k is not None and nfp_add_10k < 8.0)
    infl_high = (pce_3m_ann is not None and pce_3m_ann > 2.5) or (us_cpi_yoy is not None and us_cpi_yoy > 2.8)

    # 检测高频市场与低频宏观数据是否发生“抢跑背离”
    divergence_notes = []
    if gold_regime_note:
        divergence_notes.append(gold_regime_note)
    if spx_5d is not None and spx_5d <= -3.0 and not growth_weak:
        divergence_notes.append("⚠️ 高频警报：月度硬数据仍处稳健区间，但标普500近5日急跌，市场正在提前定价增长下修或流动性收紧（谨防后视镜误判）。")
    if dxy_5d is not None and dxy_5d <= -1.5 and y10_5 is not None and y10 is not None and (y10 - y10_5) >= 0.15:
        divergence_notes.append("⚠️ 财政信用背离：10Y美债收益率上行但美元指数DXY反而下跌，资金正在质疑美债供给与财政可持续性。")

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
        f"2. 水管与跨资产锚点：净流动性 {net_liq}B（环比 {net_liq_chg:+}B），10Y TIPS {tips10}%，10Y-2Y利差 {spread_10_2}%（{curve_regime}）。市场：SPX={spx_p} | 沪深300={csi_p} | DXY={dxy_p} | USD/JPY={jpy_p} | 黄金={au_p} | 布油={br_p} | BTC={btc_p}。\n"
        f"3. 传导机制与背离监控：{regime_logic} {' '.join(divergence_notes)}\n"
        f"4. 黑天鹅8维雷达：ON RRP {rrp}B，TGA {round(tga/1000, 1) if tga else '-'}B。{'⚠️ 预警：' + '；'.join(red_alerts + yellow_alerts) if (red_alerts or yellow_alerts) else '✅ 8大维度均处绿灯常态区间。'}\n"
        f"5. 情景概率与强制证伪：{probs} ｜ 证伪点：① 10Y TIPS 突破 {'1.80%' if tips10 > 2.0 else '2.25%'}；② 高收益债利差破 500bps 或 VIX 破 30；③ 净流动性连续两周反向变动超 150B。"
    )

    llm_ctx = {
        "quadrant": quadrant, "net_liq_B": net_liq, "net_liq_chg_B": net_liq_chg,
        "tips10": tips10, "spread_10_2": spread_10_2, "curve_regime": curve_regime,
        "pce_3m_ann": pce_3m_ann, "us_cpi_yoy": us_cpi_yoy, "nfp_10k": nfp_add_10k,
        "sahm": sahm, "interest_rev_ratio": interest_rev_ratio, "us_nom_gdp_yoy": us_nom_gdp_yoy,
        "cn_pmi": cn_pmi, "cn_scissors": cn_scissors, "ppi_cpi_gap": ppi_cpi_gap,
        "market_stats": market_stats, "red_alerts": red_alerts, "yellow_alerts": yellow_alerts,
        "divergence_notes": divergence_notes
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
    return blackswan_rows, country_rows, macro_indicator_rows, ts_snapshot_dict, report_text, meta_ctx


def get_or_create_ws(sh, title, rows=300, cols=26):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def sync_all_sheets(raw_records, cross_asset_items, blackswan_rows, country_rows, macro_rows, ts_dict, seed_rows, report_text, meta_ctx):
    if not raw_records:
        raise RuntimeError("所有外部接口均未返回有效数据，中止写入以保护表格。")

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    legacy_logs = []
    try:
        old_j = sh.worksheet("06_情景与证伪日志_Journal")
        vals = [r for r in old_j.get_all_values() if r and len(r) >= 3 and r[0] != "日期" and str(r[0]).strip()]
        legacy_logs = vals
    except Exception:
        pass

    ws_dash = get_or_create_ws(sh, TAB_DASH, rows=120, cols=26)
    ws_mkt_mac = get_or_create_ws(sh, TAB_MKT_MAC, rows=200, cols=15)
    ws_ts = get_or_create_ws(sh, TAB_TS, rows=2000, cols=100)
    ws_raw_log = get_or_create_ws(sh, TAB_RAW_LOG, rows=400, cols=16)

    for old_name in LEGACY_TABS:
        if old_name not in [TAB_DASH, TAB_MKT_MAC, TAB_TS, TAB_RAW_LOG]:
            try:
                sh.del_worksheet(sh.worksheet(old_name))
            except Exception:
                pass

    # ================= 1. 更新 02_历史时序库_TimeSeries =================
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
        ts_headers = all_keys
        ts_row_count = len(matrix)
    else:
        headers = existing[0]
        new_cols = [k for k in all_keys if k not in headers]
        if new_cols:
            headers = headers + new_cols
            ws_ts.update(range_name="A1", values=[headers], value_input_option="USER_ENTERED")
        new_row = [TODAY_STR] + [ts_dict.get(col, "") for col in headers[1:]]
        if len(existing) > 1 and existing[-1][0] == TODAY_STR:
            ws_ts.update(range_name=f"A{len(existing)}", values=[new_row], value_input_option="USER_ENTERED")
            ts_row_count = len(existing)
        else:
            ws_ts.append_row(new_row, value_input_option="USER_ENTERED")
            ts_row_count = len(existing) + 1
        ts_headers = headers

    ts_col_map = {col_name: col_idx_to_letter(i) for i, col_name in enumerate(ts_headers)}
    ts_idx_map = {col_name: i for i, col_name in enumerate(ts_headers)}

    def ts_range(code):
        col = ts_col_map.get(code)
        return f"'{TAB_TS}'!${col}$2:${col}$1000" if col else None

    def spark_formula(code):
        rng = ts_range(code)
        if not rng:
            return "-"
        return f'=IFERROR(SPARKLINE(FILTER({rng}, ISNUMBER({rng})), {{"charttype","line";"linewidth",2;"color","#1a73e8"}}), "")'

    def pctrank_formula(code, val_cell):
        rng = ts_range(code)
        if not rng:
            return "-"
        return f'=IFERROR(PERCENTRANK.INC({rng}, {val_cell}), "-")'

    def zscore_formula(code, val_cell):
        rng = ts_range(code)
        if not rng:
            return "-"
        return f'=IFERROR(({val_cell}-AVERAGE({rng}))/STDEV({rng}), 0)'

    def raw_lookup(code):
        return f'INDEX(\'{TAB_RAW_LOG}\'!$F:$F, MATCH("{code}", \'{TAB_RAW_LOG}\'!$D:$D, 0))'

    # ================= 2. 更新 03_全量指标底表与日志_RawAndLog =================
    df_raw = pd.DataFrame(raw_records)
    ws_raw_log.clear()
    ws_raw_log.update(
        range_name="A1",
        values=[df_raw.columns.tolist()] + df_raw.values.tolist(),
        value_input_option="USER_ENTERED"
    )
    log_rows = [["日期", "同步时间", "自动研判与强制证伪记录"]] + legacy_logs
    if log_rows[-1][0] == TODAY_STR:
        log_rows[-1] = [TODAY_STR, NOW_STR, report_text]
    else:
        log_rows.append([TODAY_STR, NOW_STR, report_text])
    ws_raw_log.update(range_name="L1", values=log_rows, value_input_option="USER_ENTERED")

    # ================= 3. 更新 01_跨资产与宏观硬数据_MarketsAndMacro =================
    ws_mkt_mac.clear()
    mkt_headers = ["资产大类", "代码", "资产名称", "最新价格", "日涨跌幅", "5日涨跌幅", "20日涨跌幅", "Z-Score(20D)", "20日走势图", "第一性原理核心定价因子"]
    mkt_matrix = [["【第一部分】全球五大类跨资产实时行情（股指 / 货币 / 黄金 / 大宗商品 / 加密资产）"], mkt_headers]
    for idx, item in enumerate(cross_asset_items, start=3):
        code = item["code"]
        mkt_matrix.append([
            item["cat"], code, item["name"], item["price"],
            item["pct_1d"], item["pct_5d"], item["pct_20d"],
            zscore_formula(code, f"D{idx}"),
            spark_formula(code),
            item["driver"]
        ])

    mac_headers = ["国家/区域", "经济维度", "核心指标名称", "最新核心读数", "前值/参考基准", "数据发布期", "第一性原理周期信号"]
    mkt_matrix.append([])
    mkt_matrix.append(["【第二部分】全球主要经济体高权重宏观硬数据（信用 / 非农 / PMI / 财政生死线 / CPI-PPI）"])
    mkt_matrix.append(mac_headers)
    for m_row in macro_rows:
        mkt_matrix.append(m_row)

    ws_mkt_mac.update(range_name="A1", values=mkt_matrix, value_input_option="USER_ENTERED")

    # ================= 4. 构建 00_宏观驾驶舱_Dashboard (100% 原生公式驱动诊断与图解) =================
    ws_dash.clear()
    # 初始化 95 行 x 24 列空网格，方便精准写入左侧矩阵与右侧 9 幅图的多维度动态解读卡片
    grid = [[""] * 24 for _ in range(95)]

    grid[0][0] = "全球宏观第一性原理实时监测驾驶舱（原生公式动态诊断 + 多维可视化图解中枢）"
    grid[1][0] = report_text

    # Row 3 (1-based Row 4): 实时公式动态预警横幅 (只要改表里任何数，0.1秒自动重算)
    grid[3][0] = "【实时公式动态脉冲】"
    grid[4][0] = "【模块一】六层科学公式动态计算矩阵（全原生公式联动：改任意数据即刻重算诊断结论）"
    m1_headers = ["层级归属", "复合衍生指标", "公式实时计算值", "边际变化", "历史分位", "Z-Score偏离", "20日走势图", "科学公式定义", "原生公式动态诊断信号"]
    for c_i, h in enumerate(m1_headers):
        grid[5][c_i] = h

    # 预先分配行号 (1-based Row 7 开始写入14个衍生指标)
    sci_keys = [
        "NET_LIQ", "SPREAD_10Y2Y", "REAL_POLICY", "BREAKEVEN_10Y", "US_CN_SPREAD",
        "US_JP_SPREAD", "COPPER_GOLD", "GOLD_OIL", "SPX_GOLD_CORR", "SPX_ERP",
        "US_FISCAL_INT", "US_R_MINUS_G", "CN_M1_M2", "CN_PPI_CPI"
    ]
    r_map = {k: 7 + i for i, k in enumerate(sci_keys)}
    pce_val = meta_ctx["pce_3m_ann"] if meta_ctx["pce_3m_ann"] is not None else 2.5

    sci_specs = [
        ("NET_LIQ", "L2-美元水管", "美联储净流动性 (十亿美元)",
         f"=IFERROR(({raw_lookup('WALCL')}-{raw_lookup('WDTGAL')})/1000-{raw_lookup('RRPONTSYD')}, \"\")",
         meta_ctx["net_liq_chg"], "NET_LIQUIDITY", "(WALCL - TGA)/1000 - ON RRP",
         f'=IF(D{r_map["NET_LIQ"]}>0, "🟢 放水净投放 (+" & TEXT(D{r_map["NET_LIQ"]},"0.0") & "B，支撑风险资产)", "🟡 抽水净回笼 (" & TEXT(D{r_map["NET_LIQ"]},"0.0") & "B，消耗银行准备金)")'),

        ("SPREAD_10Y2Y", "L2-利率之锚", "美债 10Y-2Y 期限利差 (%)",
         f"=IFERROR({raw_lookup('DGS10')}-{raw_lookup('DGS2')}, \"\")",
         meta_ctx["d_spread"], "SPREAD_10Y2Y", "DGS10 - DGS2",
         f'=IF(C{r_map["SPREAD_10Y2Y"]}<0, "⚠️ 曲线倒挂 (" & TEXT(C{r_map["SPREAD_10Y2Y"]},"0.00%") & ")，衰退警报未解除", "🟢 正常陡峭化 (" & TEXT(C{r_map["SPREAD_10Y2Y"]},"0.00%") & " | {meta_ctx["curve_regime"]})")'),

        ("REAL_POLICY", "L2-利率之锚", "美国实际政策立场 (EFFR-核心PCE, %)",
         f"=IFERROR({raw_lookup('DFF')}-{pce_val}, \"\")",
         "-", None, "EFFR - 核心PCE 3M年化",
         f'=IF(C{r_map["REAL_POLICY"]}>2.0, "🔴 深度限制性紧缩 (实际限制力度 > 2%)", IF(C{r_map["REAL_POLICY"]}>0.5, "🟡 温和紧缩区间", "🟢 宽松/中性政策立场"))'),

        ("BREAKEVEN_10Y", "L2-通胀预期", "10Y盈亏平衡通胀预期 (Breakeven, %)",
         f"=IFERROR({raw_lookup('DGS10')}-{raw_lookup('DFII10')}, \"\")",
         "-", "BREAKEVEN_10Y", "DGS10 - 10Y TIPS",
         f'=IF(C{r_map["BREAKEVEN_10Y"]}>2.5, "⚠️ 通胀预期脱锚 (>2.5%)", IF(C{r_map["BREAKEVEN_10Y"]}<1.8, "❄️ 通缩/衰退预期升温 (<1.8%)", "🟢 通胀预期锚定在2%附近"))'),

        ("US_CN_SPREAD", "L4-跨境利差", "中美 10Y 无风险利差 (US-CN, %)",
         f"=IFERROR({raw_lookup('DGS10')}-{raw_lookup('CN_DGS10')}, \"\")",
         "-", None, "US 10Y - CN 10Y",
         f'=IF(C{r_map["US_CN_SPREAD"]}>2.0, "⚠️ 中美负利差走阔 (>200bp)，人民币承压", "🟢 中美利差压力可控")'),

        ("US_JP_SPREAD", "L4-跨境利差", "美日 10Y 套息利差 (US-JP, %)",
         f"=IFERROR({raw_lookup('DGS10')}-{raw_lookup('IRLTLT01JPM156N')}, \"\")",
         "-", None, "US 10Y - JP 10Y",
         f'=IF(C{r_map["US_JP_SPREAD"]}<2.5, "⚠️ 套息安全垫收窄 (<2.5%)，警惕日元平仓踩踏", "🟢 套息利差缓冲充足")'),

        ("COPPER_GOLD", "L5-跨资产锚", "铜金比 (Copper/Gold × 1000)",
         f"=IFERROR(({raw_lookup('COPPER')}*1000)/{raw_lookup('GOLD')}, \"\")",
         "-", "COPPER_GOLD_RATIO", "(COMEX铜 × 1000) / COMEX金",
         f'=IF(F{r_map["COPPER_GOLD"]}>0.5, "🟢 实体制造业复苏预期跑赢避险", IF(F{r_map["COPPER_GOLD"]}<-0.5, "⚠️ 避险情绪占优/需求走弱", "⚪ 工业需求与避险均衡"))'),

        ("GOLD_OIL", "L5-跨资产锚", "金油比 (Gold / Brent)",
         f"=IFERROR({raw_lookup('GOLD')}/{raw_lookup('BRENT')}, \"\")",
         "-", "GOLD_OIL_RATIO", "COMEX黄金 / 布伦特原油",
         f'=IF(C{r_map["GOLD_OIL"]}>30, "⚠️ 高于30警戒线：需求疲软或地缘避险极端", "🟢 处于15~30常态商品均衡区间")'),

        ("SPX_GOLD_CORR", "L5-相关性矩阵", "股金动态相关系数 (SPX vs GOLD)",
         f"=IFERROR(CORREL({ts_range('SPX')}, {ts_range('GOLD')}), 0)" if ts_range("SPX") and ts_range("GOLD") else 0,
         "-", None, "CORREL(SPX, GOLD)",
         f'=IF(C{r_map["SPX_GOLD_CORR"]}>0.5, "🔴 强正相关 (>0.5)：警惕流动性无差别抛售！", "🟢 正常低相关/负相关对冲状态")'),

        ("SPX_ERP", "L5-估值安全垫", "美股股权风险溢价 ERP (%)",
         f"=IFERROR((1/{raw_lookup('SPX_PE')})*100 - {raw_lookup('DGS10')}, \"\")",
         "-", None, "1/SPX_PE - US_10Y",
         f'=IF(C{r_map["SPX_ERP"]}<0, "🔴 ERP为负：美股盈利收益率低于无风险国债，估值极贵", IF(C{r_map["SPX_ERP"]}<1.5, "🟡 估值安全垫偏薄 (<1.5%)", "🟢 股票相对债券具备性价比"))'),

        ("US_FISCAL_INT", "L3-财政生死线", "美联邦利息支出 / 财政收入 (%)",
         f"=IFERROR(({raw_lookup('A091RC1Q027SBEA')}/{raw_lookup('FGRECPT')})*100, \"\")",
         "-", None, "联邦利息支出 / 财政收入",
         f'=IF(C{r_map["US_FISCAL_INT"]}>=20, "🔴 突破20%财政生死线，赤字货币化压力剧增", "🟡 接近20%警戒红线，长债供给溢价难消")'),

        ("US_R_MINUS_G", "L3-财政生死线", "美国债务滚雪球剪刀差 (r - g, %)",
         f"=IFERROR({raw_lookup('DGS10')} - {meta_ctx['us_nom_gdp_yoy'] if meta_ctx['us_nom_gdp_yoy'] is not None else 4.5}, \"\")",
         "-", None, "US 10Y利率 r - 名义GDP增速 g",
         f'=IF(C{r_map["US_R_MINUS_G"]}>0, "⚠️ r > g：债务内生滚雪球加速", "🟢 r < g：名义增长仍能稀释债务比率")'),

        ("CN_M1_M2", "L1-信用引擎", "中国 M1 - M2 资金活化剪刀差 (%)",
         f"=IFERROR({raw_lookup('CN_M1_YOY')}-{raw_lookup('CN_M2_YOY')}, \"\")",
         "-", None, "CN_M1_YoY - CN_M2_YoY",
         f'=IF(C{r_map["CN_M1_M2"]}>0, "🟢 剪刀差转正：企业活期资金活化，利好A股", "🟡 负值区间 (" & TEXT(C{r_map["CN_M1_M2"]},"0.0%") & ")：资金定期沉淀待活化")'),

        ("CN_PPI_CPI", "L3-盈利周期", "中国 PPI - CPI 盈利剪刀差 (%)",
         f"=IFERROR({raw_lookup('CN_PPI_YOY')}-{raw_lookup('CN_CPI_YOY')}, \"\")",
         "-", None, "CN_PPI_YoY - CN_CPI_YoY",
         f'=IF(C{r_map["CN_PPI_CPI"]}>0, "🟢 工业企业中上游盈利扩张", "🟡 剪刀差为负，关注通缩收敛拐点")'),
    ]

    for key, layer, title, formula_val, delta, ts_code, formula_desc, dyn_sig_formula in sci_specs:
        r_1b = r_map[key]
        r_0b = r_1b - 1
        c_ref = f"C{r_1b}"
        row_vals = [
            layer, title, formula_val, delta,
            pctrank_formula(ts_code, c_ref) if ts_code else "-",
            zscore_formula(ts_code, c_ref) if ts_code else "-",
            spark_formula(ts_code) if ts_code else "-",
            formula_desc, dyn_sig_formula
        ]
        for c_i, val in enumerate(row_vals):
            grid[r_0b][c_i] = val

    # 模块二：黑天鹅 8 维系统性风险熔断雷达 (1-based Row 22 开始)
    bs_title_r0 = 21  # 0-based (Row 22)
    grid[bs_title_r0][0] = "【模块二】黑天鹅 8 维系统性风险熔断雷达（含熔断进度占比，驱动图8温度计）"
    bs_headers = ["风险编号", "风险维度", "核心监控指标", "当前读数", "黄灯警戒线", "红灯熔断线", "当前状态灯", "传导机制与熔断后果", "距熔断线(%)"]
    for c_i, h in enumerate(bs_headers):
        grid[bs_title_r0 + 1][c_i] = h

    bs_data_start_1b = bs_title_r0 + 3  # Row 24
    for idx_b, bs in enumerate(blackswan_rows):
        r_0b = bs_title_r0 + 2 + idx_b
        r_1b = r_0b + 1
        # 用原生公式驱动状态灯：只要改第9列进度或第4列读数，状态灯瞬间变色！
        dyn_light = f'=IF(I{r_1b}>=1, "🔴 红灯熔断", IF(I{r_1b}>=0.65, "🟡 黄灯警戒", "🟢 正常"))'
        vals = [bs[0], bs[1], bs[2], bs[3], bs[4], bs[5], dyn_light, bs[7], bs[8]]
        for c_i, val in enumerate(vals):
            grid[r_0b][c_i] = val
    bs_data_end_1b = bs_data_start_1b + len(blackswan_rows) - 1

    # 顶部 Row 4 (0-based 3) 实时公式动态脉冲条
    grid[3][0] = (
        f'="⚡ 实时公式脉冲 | 净流动性: " & TEXT(C{r_map["NET_LIQ"]},"0.0") & "B (" & TEXT(D{r_map["NET_LIQ"]},"+\"0.0\";-\"0.0\";0.0") & "B) | '
        f'10Y-2Y利差: " & TEXT(C{r_map["SPREAD_10Y2Y"]},"0.00%") & " | 美股ERP: " & TEXT(C{r_map["SPX_ERP"]},"0.00%") & " | '
        f'黑天鹅红灯: " & COUNTIF(G{bs_data_start_1b}:G{bs_data_end_1b},"*红灯*") & "个 / 黄灯: " & COUNTIF(G{bs_data_start_1b}:G{bs_data_end_1b},"*黄灯*") & "个"'
    )

    # 模块三：全球主要经济体周期与费雪真实利率对比
    cty_title_r0 = bs_title_r0 + 2 + len(blackswan_rows) + 1
    grid[cty_title_r0][0] = "【模块三】全球主要经济体周期与费雪真实利率对比（公式自动计算）"
    cty_headers = ["代码", "国家/经济体", "实际GDP增速(%)", "CPI通胀(%)", "10Y国债(%)", "真实无风险利率(公式)", "经常账户/GDP(%)", "财政余额/GDP(%)", "短期外债/外储(%)"]
    for c_i, h in enumerate(cty_headers):
        grid[cty_title_r0 + 1][c_i] = h

    cty_data_start_1b = cty_title_r0 + 3
    for idx_c, c_row in enumerate(country_rows):
        r_0b = cty_title_r0 + 2 + idx_c
        r_1b = r_0b + 1
        row_copy = list(c_row)
        row_copy[5] = f'=IFERROR(E{r_1b}-D{r_1b}, "-")'
        for c_i, val in enumerate(row_copy):
            grid[r_0b][c_i] = val
    cty_data_end_1b = cty_data_start_1b + len(country_rows) - 1

    # ================= 5. 右侧可视化看板区：4大维度 × 9幅图 + 9张【三维动态公式解读卡片】 =================
    grid[0][10] = "【多维度可视化全景雷达】（四大分析维度 × 9幅原生图表 × 实时公式动态解读卡片）"

    # 定义 9 张动态解读卡片的坐标与公式（紧贴在每张图表正下方，改数即刻重算！）
    mkt_end_1b = 2 + len(cross_asset_items)
    chart_cards = [
        # (标题, 卡片所在0-based行, 起始列, 结束列, 动态公式)
        ("图1多维解读｜美元流动性三蓄水池", 14, 10, 16,
         f'="【读数】美联储总资产 $" & TEXT(C{r_map["NET_LIQ"]},"0.0") & "B 净流动性（边际 " & TEXT(D{r_map["NET_LIQ"]},"+\"0.0\";-\"0.0\";0.0") & "B）'
         f' ｜ 【判定】" & IF(D{r_map["NET_LIQ"]}>0,"美元水管边际扩张，TGA回吐或联储扩表正在向市场注入基础流动性。","财政部发债抽水或缩表正在消耗商业银行准备金，流动性边际趋紧。")'
         f' & " ｜ 【映射】" & IF(D{r_map["NET_LIQ"]}>0,"利好纳指与BTC等高弹性流动性资产。","警惕高杠杆资产承压。")'),

        ("图2多维解读｜中美信用引擎双核", 14, 16, 22,
         f'="【读数】中国 M1-M2 剪刀差为 " & TEXT(C{r_map["CN_M1_M2"]},"0.00%")'
         f' & " ｜ 【判定】" & IF(C{r_map["CN_M1_M2"]}>0,"资金定期转活期，企业投资与居民消费意愿实质性回暖。","资金仍大量沉淀于定期账户空转，宽货币尚待向实体信用传导。")'
         f' & " ｜ 【映射】" & IF(C{r_map["CN_M1_M2"]}>0,"利好沪深300与顺周期权益。","关注社融增量与政策进一步加码信号。")'),

        ("图3多维解读｜美债全曲线与实际利率", 31, 10, 16,
         f'="【读数】10Y-2Y利差 " & TEXT(C{r_map["SPREAD_10Y2Y"]},"0.00%") & " | 10Y隐含通胀 " & TEXT(C{r_map["BREAKEVEN_10Y"]},"0.00%")'
         f' & " ｜ 【判定】当前曲线处于【{meta_ctx["curve_regime"]}】，实际政策紧缩度 " & TEXT(C{r_map["REAL_POLICY"]},"0.00%") & "%。"'
         f' & " ｜ 【映射】" & IF(C{r_map["BREAKEVEN_10Y"]}>2.4,"通胀预期偏热，压制长久期债券。","利率锚总体可控，关注2Y美债对降息预期的领先指引。")'),

        ("图4多维解读｜主要经济体真实利率对比", 31, 16, 22,
         f'="【读数】中美10Y利差 " & TEXT(C{r_map["US_CN_SPREAD"]},"0.00%") & " | 美日10Y套息利差 " & TEXT(C{r_map["US_JP_SPREAD"]},"0.00%")'
         f' & " ｜ 【判定】" & IF(C{r_map["US_JP_SPREAD"]}<2.5,"美日套息利差逼近危险阈值，日元升值随时可能触发Carry Trade去杠杆。","跨国无风险利差仍维持美元资产吸引力。")'
         f' & " ｜ 【映射】紧盯USD/JPY汇率与新兴市场短期外债覆盖率。"'),

        ("图5多维解读｜10Y TIPS实际利率 vs 黄金/纳指", 48, 10, 16,
         f'="【读数】股金相关系数 " & TEXT(C{r_map["SPX_GOLD_CORR"]},"0.00") & " | 美股ERP " & TEXT(C{r_map["SPX_ERP"]},"0.00%")'
         f' & " ｜ 【判定】" & IF(C{r_map["SPX_GOLD_CORR"]}>0.5,"⚠️ 股金同向波动加剧，警惕Margin Call引发无差别抛售！","黄金与美股保持独立定价，若TIPS上行而金价坚挺则代表央行购金主导。")'
         f' & " ｜ 【映射】ERP低于1%时纳指对实际利率上行高度敏感。"'),

        ("图6多维解读｜铜金比 vs 原油(需求与供给)", 48, 16, 22,
         f'="【读数】铜金比 " & TEXT(C{r_map["COPPER_GOLD"]},"0.00") & " | 金油比 " & TEXT(C{r_map["GOLD_OIL"]},"0.0")'
         f' & " ｜ 【判定】" & IF(C{r_map["GOLD_OIL"]}>30,"金油比突破30高位，反映实体总需求偏弱或地缘避险溢价极高。","铜金比与油价处于均衡区间，未出现恶性供给冲击滞胀信号。")'
         f' & " ｜ 【映射】铜金比走强利好周期股，油价单边飙升则警惕滞胀重演。"'),

        ("图7多维解读｜全球美元潮汐与外汇博弈", 65, 10, 16,
         f'="【读数】联邦利息吞噬财政收入 " & TEXT(C{r_map["US_FISCAL_INT"]},"0.0%") & " | 债务剪刀差(r-g) " & TEXT(C{r_map["US_R_MINUS_G"]},"0.00%")'
         f' & " ｜ 【判定】短端看跨境利差，长端看财政信用与经常账户。若美债利率升而DXY跌，需警惕美元信用折价。"'
         f' & " ｜ 【映射】强美元周期压制新兴市场(EEM)与非美货币。"'),

        ("图8多维解读｜黑天鹅 8 维风险熔断雷达", 65, 16, 22,
         f'="【读数】最高风险项为【" & INDEX(B{bs_data_start_1b}:B{bs_data_end_1b}, MATCH(MAX(I{bs_data_start_1b}:I{bs_data_end_1b}), I{bs_data_start_1b}:I{bs_data_end_1b}, 0)) & "】，距红灯熔断线已达 " & TEXT(MAX(I{bs_data_start_1b}:I{bs_data_end_1b}),"0.0%")'
         f' & " ｜ 【判定】" & IF(MAX(I{bs_data_start_1b}:I{bs_data_end_1b})>=1,"🔴 已触发系统性熔断红灯，立即收缩高杠杆敞口！",IF(MAX(I{bs_data_start_1b}:I{bs_data_end_1b})>=0.65,"🟡 局部压力进入黄灯警戒区，严密监控流动性。","🟢 八大尾部维度均处于安全绿灯区间。"))'),

        ("图9多维解读｜全球五大类跨资产 5D vs 20D 动量轮动全景", 82, 10, 22,
         f'="【领跑资产】近5日最强标的为【" & INDEX(\'{TAB_MKT_MAC}\'!C3:C{mkt_end_1b}, MATCH(MAX(\'{TAB_MKT_MAC}\'!F3:F{mkt_end_1b}), \'{TAB_MKT_MAC}\'!F3:F{mkt_end_1b}, 0))'
         f' & " (" & TEXT(MAX(\'{TAB_MKT_MAC}\'!F3:F{mkt_end_1b}),"+0.00%") & ")】 ｜ 【最弱垫底】为【" & INDEX(\'{TAB_MKT_MAC}\'!C3:C{mkt_end_1b}, MATCH(MIN(\'{TAB_MKT_MAC}\'!F3:F{mkt_end_1b}), \'{TAB_MKT_MAC}\'!F3:F{mkt_end_1b}, 0))'
         f' & " (" & TEXT(MIN(\'{TAB_MKT_MAC}\'!F3:F{mkt_end_1b}),"+0.00%") & ")】 ｜ 【实战策略】结合左侧四象限与净流动性方向，顺应主线动量配置，避免逆流动性趋势抄底。"'),
    ]

    for title, r0, c_start, c_end, formula_txt in chart_cards:
        grid[r0][c_start] = formula_txt

    ws_dash.update(range_name="A1", values=grid, value_input_option="USER_ENTERED")

    # ================= 6. 批量样式排版 + 清理旧图 + 渲染 9 幅多维度原生图表 =================
    meta = sh.fetch_sheet_metadata()
    dash_id = ws_dash.id
    mkt_id = ws_mkt_mac.id
    ts_id = ws_ts.id

    requests_batch = []

    for s in meta.get("sheets", []):
        if s["properties"]["sheetId"] == dash_id:
            for ch in s.get("charts", []):
                requests_batch.append({"deleteEmbeddedObject": {"objectId": ch["chartId"]}})

    # 主标题、报告框、脉冲条样式
    requests_batch.extend([
        {"unmergeCells": {"range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 9}}},
        {"mergeCells": {"range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 9}, "mergeType": "MERGE_ALL"}},
        {"mergeCells": {"range": {"sheetId": dash_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 9}, "mergeType": "MERGE_ALL"}},
        {"updateDimensionProperties": {"range": {"sheetId": dash_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 150}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": dash_id, "dimension": "ROWS", "startIndex": 3, "endIndex": 4}, "properties": {"pixelSize": 32}, "fields": "pixelSize"}},
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 22},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.11, "green": 0.21, "blue": 0.36}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 12, "bold": True}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.97, "blue": 1.0}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE", "textFormat": {"fontSize": 10}}},
                "fields": "userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,textFormat)"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1.0, "green": 0.96, "blue": 0.85}, "verticalAlignment": "MIDDLE", "textFormat": {"fontSize": 10, "bold": True, "foregroundColor": {"red": 0.55, "green": 0.30, "blue": 0.0}}}},
                "fields": "userEnteredFormat(backgroundColor,verticalAlignment,textFormat)"
            }
        },
    ])

    # 左侧三个模块表头样式
    for hdr_r0 in [5, bs_title_r0 + 1, cty_title_r0 + 1]:
        requests_batch.append({
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": hdr_r0, "endRowIndex": hdr_r0 + 1, "startColumnIndex": 0, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.18, "green": 0.33, "blue": 0.55}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        })

    # 百分比与数字格式
    requests_batch.extend([
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 6, "endRowIndex": 20, "startColumnIndex": 4, "endColumnIndex": 5},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 6, "endRowIndex": 20, "startColumnIndex": 5, "endColumnIndex": 6},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "+0.00;-0.00;0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": bs_title_r0 + 2, "endRowIndex": bs_title_r0 + 2 + len(blackswan_rows), "startColumnIndex": 8, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": mkt_id, "startRowIndex": 2, "endRowIndex": 2 + len(cross_asset_items), "startColumnIndex": 4, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "+0.00%;-0.00%;0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": mkt_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.18, "green": 0.33, "blue": 0.55}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        }
    ])

    # 合并并美化 9 张动态图解卡片 (每张占 3 行高度，自动换行，蓝灰底边框风格)
    for title, r0, c_start, c_end, _ in chart_cards:
        requests_batch.extend([
            {"unmergeCells": {"range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 3, "startColumnIndex": c_start, "endColumnIndex": c_end}}},
            {"mergeCells": {"range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 3, "startColumnIndex": c_start, "endColumnIndex": c_end}, "mergeType": "MERGE_ALL"}},
            {
                "repeatCell": {
                    "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 3, "startColumnIndex": c_start, "endColumnIndex": c_end},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.94, "green": 0.96, "blue": 0.99},
                            "wrapStrategy": "WRAP",
                            "verticalAlignment": "MIDDLE",
                            "textFormat": {"fontSize": 9, "foregroundColor": {"red": 0.12, "green": 0.18, "blue": 0.28}}
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,textFormat)"
                }
            }
        ])

    def build_ts_chart(title, left_codes, right_codes, anchor_row, anchor_col, width=540, height=265, chart_type="LINE"):
        series_list = []
        for c in left_codes:
            if c in ts_idx_map:
                ci = ts_idx_map[c]
                s_obj = {
                    "series": {"sourceRange": {"sources": [{"sheetId": ts_id, "startRowIndex": 0, "endRowIndex": ts_row_count, "startColumnIndex": ci, "endColumnIndex": ci + 1}]}},
                    "targetAxis": "LEFT_AXIS"
                }
                if chart_type == "COMBO":
                    s_obj["type"] = "LINE"
                series_list.append(s_obj)
        for c in right_codes:
            if c in ts_idx_map:
                ci = ts_idx_map[c]
                s_obj = {
                    "series": {"sourceRange": {"sources": [{"sheetId": ts_id, "startRowIndex": 0, "endRowIndex": ts_row_count, "startColumnIndex": ci, "endColumnIndex": ci + 1}]}},
                    "targetAxis": "RIGHT_AXIS"
                }
                if chart_type == "COMBO":
                    s_obj["type"] = "LINE"
                series_list.append(s_obj)
        if not series_list:
            return None
        axes = [{"position": "BOTTOM_AXIS", "title": "日期"}, {"position": "LEFT_AXIS", "title": "左轴"}]
        if right_codes:
            axes.append({"position": "RIGHT_AXIS", "title": "右轴"})
        return {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": title,
                        "basicChart": {
                            "chartType": chart_type,
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": axes,
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": ts_id, "startRowIndex": 0, "endRowIndex": ts_row_count, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                            "series": series_list,
                            "headerCount": 1,
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": dash_id, "rowIndex": anchor_row, "columnIndex": anchor_col},
                            "offsetXPixels": 8, "offsetYPixels": 4,
                            "widthPixels": width, "heightPixels": height
                        }
                    }
                }
            }
        }

    charts_to_add = [
        # 维度一：总量与流动性水管
        build_ts_chart("图1【水管维】：美联储总资产 / TGA / 净流动性(左) vs ON RRP(右)", ["WALCL_B", "NET_LIQUIDITY", "TGA_B"], ["RRPONTSYD"], 1, 10),
        build_ts_chart("图2【信用维】：美国商业银行总信贷 H.8(左) vs 高收益债信用利差(右)", ["TOTLL"], ["BAMLH0A0HYM2"], 1, 16),
        # 维度二：利率曲线与跨国利差
        build_ts_chart("图3【利率维】：美债 2Y政策锚 / 10Y名义锚 / 10Y TIPS实际利率", ["DGS2", "DGS10", "DFII10"], [], 18, 10),
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "图4【跨国维】：主要经济体 GDP增速 / CPI通胀 / 10Y国债 对比",
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [{"position": "BOTTOM_AXIS", "title": "经济体"}, {"position": "LEFT_AXIS", "title": "百分比 (%)"}],
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": cty_title_r0 + 1, "endRowIndex": cty_data_end_1b, "startColumnIndex": 1, "endColumnIndex": 2}]}}}],
                            "series": [
                                {"series": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": cty_title_r0 + 1, "endRowIndex": cty_data_end_1b, "startColumnIndex": 2, "endColumnIndex": 3}]}}, "targetAxis": "LEFT_AXIS"},
                                {"series": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": cty_title_r0 + 1, "endRowIndex": cty_data_end_1b, "startColumnIndex": 3, "endColumnIndex": 4}]}}, "targetAxis": "LEFT_AXIS"},
                                {"series": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": cty_title_r0 + 1, "endRowIndex": cty_data_end_1b, "startColumnIndex": 4, "endColumnIndex": 5}]}}, "targetAxis": "LEFT_AXIS"},
                            ],
                            "headerCount": 1
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": dash_id, "rowIndex": 18, "columnIndex": 16},
                            "offsetXPixels": 8, "offsetYPixels": 4,
                            "widthPixels": 540, "heightPixels": 265
                        }
                    }
                }
            }
        },
        # 维度三：跨资产定价传导
        build_ts_chart("图5【定价锚】：10Y TIPS实际利率(左) vs 黄金 & 纳指100(右)", ["DFII10"], ["GOLD", "NDX"], 35, 10),
        build_ts_chart("图6【商品维】：铜金比 COPPER_GOLD(左) vs 布伦特原油(右)", ["COPPER_GOLD_RATIO"], ["BRENT"], 35, 16),
        build_ts_chart("图7【汇率维】：美元指数 DXY(左) vs 美元/日元 USDJPY(右)", ["DXY"], ["USDJPY"], 52, 10),
        # 维度四：风险温度计与资产轮动
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "图8【风控维】：黑天鹅 8 维风险距离红灯熔断线进度 (%)",
                        "basicChart": {
                            "chartType": "BAR",
                            "legendPosition": "NO_LEGEND",
                            "axis": [{"position": "LEFT_AXIS", "title": "风险维度"}, {"position": "BOTTOM_AXIS", "title": "距熔断线比例 (100%=触发熔断)"}],
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": bs_title_r0 + 1, "endRowIndex": bs_data_end_1b, "startColumnIndex": 1, "endColumnIndex": 2}]}}}],
                            "series": [
                                {"series": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": bs_title_r0 + 1, "endRowIndex": bs_data_end_1b, "startColumnIndex": 8, "endColumnIndex": 9}]}}, "targetAxis": "BOTTOM_AXIS"},
                            ],
                            "headerCount": 1
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": dash_id, "rowIndex": 52, "columnIndex": 16},
                            "offsetXPixels": 8, "offsetYPixels": 4,
                            "widthPixels": 540, "heightPixels": 265
                        }
                    }
                }
            }
        },
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "图9【轮动维】：全球五大类跨资产 5日 vs 20日涨跌幅强弱全景",
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [{"position": "BOTTOM_AXIS", "title": "跨资产标的"}, {"position": "LEFT_AXIS", "title": "涨跌幅 (%)"}],
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": mkt_id, "startRowIndex": 1, "endRowIndex": 2 + len(cross_asset_items), "startColumnIndex": 1, "endColumnIndex": 2}]}}}],
                            "series": [
                                {"series": {"sourceRange": {"sources": [{"sheetId": mkt_id, "startRowIndex": 1, "endRowIndex": 2 + len(cross_asset_items), "startColumnIndex": 5, "endColumnIndex": 6}]}}, "targetAxis": "LEFT_AXIS"},
                                {"series": {"sourceRange": {"sources": [{"sheetId": mkt_id, "startRowIndex": 1, "endRowIndex": 2 + len(cross_asset_items), "startColumnIndex": 6, "endColumnIndex": 7}]}}, "targetAxis": "LEFT_AXIS"},
                            ],
                            "headerCount": 1
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": dash_id, "rowIndex": 69, "columnIndex": 10},
                            "offsetXPixels": 8, "offsetYPixels": 4,
                            "widthPixels": 1100, "heightPixels": 265
                        }
                    }
                }
            }
        }
    ]

    for c_req in charts_to_add:
        if c_req:
            requests_batch.append(c_req)

    sh.batch_update({"requests": requests_batch})


if __name__ == "__main__":
    fred_recs, obs_map, date_map, fred_hist = fetch_fred_data()
    cn_recs = fetch_china_macro_akshare(obs_map, date_map)
    mkt_recs, cross_items, mkt_stats = fetch_market_assets(obs_map, fred_hist)
    ext_recs, wb_map, imf_map, extra = fetch_external_apis(obs_map, date_map)
    all_raw = fred_recs + cn_recs + mkt_recs + ext_recs
    blackswan, countries, macro_rows, ts_dict, report, meta_ctx = analyze_macro_and_blackswan(
        obs_map, date_map, wb_map, imf_map, extra, mkt_stats
    )
    seed_rows = build_historical_seed_rows(fred_hist, ts_dict)
    sync_all_sheets(all_raw, cross_items, blackswan, countries, macro_rows, ts_dict, seed_rows, report, meta_ctx)
    print(f"[SUCCESS] Synced 4 tabs, 100% live formula diagnostics & 9 multi-dimensional charts + live cards ({len(all_raw)} metrics).")
