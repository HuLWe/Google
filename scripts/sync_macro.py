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

# ================= 1. 读取并清洗 GitHub Secrets =================
FRED_API_KEY = os.environ["FRED_API_KEY"].strip()
_raw_ss = os.environ["SPREADSHEET_ID"].strip()
_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", _raw_ss)
SPREADSHEET_ID = _match.group(1) if _match else _raw_ss.split("/")[0].split("?")[0].strip()
GCP_SA_JSON = json.loads(os.environ["GCP_SA_KEY"])

TZ_CST = timezone(timedelta(hours=8))
NOW_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d %H:%M:%S")
TODAY_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d")

# 统一精简后的 4 张核心工作表名称
TAB_DASH = "00_宏观驾驶舱_Dashboard"
TAB_MKT_MAC = "01_跨资产与宏观硬数据_MarketsAndMacro"
TAB_TS = "02_历史时序库_TimeSeries"
TAB_RAW_LOG = "03_全量指标底表与日志_RawAndLog"

# 旧版冗余分页（迁移日志后自动清理）
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
    """0-based 列索引转 Excel/Sheets 列字母 (0->A, 25->Z, 26->AA)"""
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
        status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]
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

# ================= 2. 宏观基础指标字典 (FRED) =================
FRED_SERIES = [
    # L2: 美元水管与利率锚
    {"id": "WALCL", "country": "US", "layer": "L2-流动性管道", "name": "美联储总资产 (百万美元)"},
    {"id": "WDTGAL", "country": "US", "layer": "L2-流动性管道", "name": "财政部TGA账户余额-周三水平 (百万美元)"},
    {"id": "RRPONTSYD", "country": "US", "layer": "L2-流动性管道", "name": "隔夜逆回购 ON RRP (十亿美元)"},
    {"id": "WRESBAL", "country": "US", "layer": "L2-流动性管道", "name": "商业银行准备金余额 (百万美元)"},
    {"id": "IORB", "country": "US", "layer": "L2-流动性管道", "name": "美联储准备金余额利率 IORB (%)"},
    {"id": "DFF", "country": "US", "layer": "L2-利率之锚", "name": "联邦基金有效利率 EFFR (%)"},
    {"id": "DGS2", "country": "US", "layer": "L2-利率之锚", "name": "美国2年期国债收益率 (%)"},
    {"id": "DGS10", "country": "US", "layer": "L2-利率之锚", "name": "美国10年期国债收益率 (%)"},
    {"id": "DFII10", "country": "US", "layer": "L2-利率之锚", "name": "美国10Y TIPS实际利率 (%)"},
    {"id": "T10Y2Y", "country": "US", "layer": "L2-利率之锚", "name": "美债10Y-2Y期限利差 (%)"},
    # L1: 信用引擎与货币存量
    {"id": "M1SL", "country": "US", "layer": "L1-信用引擎", "name": "美国M1存量 (十亿美元)"},
    {"id": "M2SL", "country": "US", "layer": "L1-信用引擎", "name": "美国M2存量 (十亿美元)"},
    {"id": "DRTSCILM", "country": "US", "layer": "L1-信用引擎", "name": "美联储SLOOS银行收紧贷款比例 (%)"},
    {"id": "BAMLH0A0HYM2", "country": "US", "layer": "L1-信用引擎", "name": "美国高收益债信用利差 (%)"},
    {"id": "MYAGM1CNM189N", "country": "CN", "layer": "L1-信用引擎", "name": "中国M1存量 (本币)"},
    {"id": "MYAGM2CNM189N", "country": "CN", "layer": "L1-信用引擎", "name": "中国M2存量 (本币)"},
    {"id": "MYAGM1JPM189S", "country": "JP", "layer": "L1-信用引擎", "name": "日本M1存量 (本币)"},
    {"id": "MYAGM2JPM189S", "country": "JP", "layer": "L1-信用引擎", "name": "日本M2存量 (本币)"},
    # L2: 欧日10Y国债
    {"id": "IRLTLT01JPM156N", "country": "JP", "layer": "L2-利率之锚", "name": "日本10年期国债收益率 (%)"},
    {"id": "IRLTLT01DEM156N", "country": "DE", "layer": "L2-利率之锚", "name": "德国(欧元区)10Y国债收益率 (%)"},
    # L3: 美国核心经济硬数据
    {"id": "PAYEMS", "country": "US", "layer": "L3-就业硬数据", "name": "美国非农就业总人数 (千人)"},
    {"id": "ICSA", "country": "US", "layer": "L3-就业硬数据", "name": "美国周度初请失业金人数 (人)"},
    {"id": "UNRATE", "country": "US", "layer": "L3-就业硬数据", "name": "美国失业率 (%)"},
    {"id": "SAHMREALTIME", "country": "US", "layer": "L3-就业硬数据", "name": "官方萨姆规则衰退指标 (%)"},
    {"id": "CES0500000003", "country": "US", "layer": "L3-就业硬数据", "name": "美国私营部门平均时薪 (美元/小时)"},
    {"id": "INDPRO", "country": "US", "layer": "L3-工业与消费", "name": "美国工业生产总指数 (2017=100)"},
    {"id": "RSAFS", "country": "US", "layer": "L3-工业与消费", "name": "美国零售与食品服务销售额 (百万美元)"},
    {"id": "UMCSENT", "country": "US", "layer": "L3-工业与消费", "name": "密歇根大学消费者信心指数"},
    {"id": "CPIAUCSL", "country": "US", "layer": "L3-通胀硬数据", "name": "美国CPI总体物价指数"},
    {"id": "PCEPILFE", "country": "US", "layer": "L3-通胀硬数据", "name": "美国核心PCE物价指数"},
    {"id": "PPIACO", "country": "US", "layer": "L3-通胀硬数据", "name": "美国PPI全商品生产者物价指数"},
    # L3: 欧元区与日本宏观硬数据
    {"id": "CP0000EZ19M086NEST", "country": "EA", "layer": "L3-通胀硬数据", "name": "欧元区调和CPI物价指数 (HICP)"},
    {"id": "LRHUTTTTEZM156S", "country": "EA", "layer": "L3-就业硬数据", "name": "欧元区失业率 (%)"},
    {"id": "JPNPRMNTO01GPSAM", "country": "JP", "layer": "L3-工业与消费", "name": "日本制造业生产环比增速 (%)"},
    {"id": "JPNCPIALLMINMEI", "country": "JP", "layer": "L3-通胀硬数据", "name": "日本全国CPI物价指数"},
    {"id": "CHNCPIALLMINMEI", "country": "CN", "layer": "L3-通胀硬数据", "name": "中国CPI定基指数"},
    # L4/L6: 贸易加权汇率与系统性金融压力
    {"id": "DTWEXBGS", "country": "US", "layer": "L4-跨境汇率", "name": "美联储广义贸易加权美元指数"},
    {"id": "VXVCLS", "country": "US", "layer": "L6-黑天鹅雷达", "name": "CBOE 3个月期VIX3M指数"},
    {"id": "STLFSI4", "country": "US", "layer": "L6-黑天鹅雷达", "name": "圣路易斯联储金融压力指数"},
    {"id": "NFCI", "country": "US", "layer": "L6-黑天鹅雷达", "name": "芝加哥联储金融状况指数"},
]

# ================= 3. 全球跨资产实时行情字典 =================
MARKET_ASSETS = [
    # 重要股指 & 全球工业
    {"cat": "重要股指", "ticker": "^GSPC", "code": "SPX", "country": "US", "name": "标普500指数 (S&P 500)", "driver": "美股盈利预期 × 10Y贴现率"},
    {"cat": "重要股指", "ticker": "^NDX", "code": "NDX", "country": "US", "name": "纳斯达克100指数 (Nasdaq 100)", "driver": "远期现金流久期资产，对10Y TIPS极敏感"},
    {"cat": "重要股指", "ticker": "000300.SS", "code": "CSI300", "country": "CN", "name": "沪深300指数 (CSI 300)", "driver": "中国社融脉冲与M1-M2活化剪刀差"},
    {"cat": "重要股指", "ticker": "^HSI", "code": "HSI", "country": "HK", "name": "香港恒生指数 (Hang Seng)", "driver": "中国基本面分子 × 美元流动性分母双重定价"},
    {"cat": "重要股指", "ticker": "^N225", "code": "N225", "country": "JP", "name": "日经225指数 (Nikkei 225)", "driver": "日元汇率与日本央行加息路径"},
    {"cat": "重要股指", "ticker": "^GDAXI", "code": "DAX", "country": "DE", "name": "德国DAX指数", "driver": "欧元区制造业周期与欧央行利率路径"},
    {"cat": "重要股指", "ticker": "EEM", "code": "EEM", "country": "EM", "name": "MSCI新兴市场ETF", "driver": "强美元周期与跨境热钱风险偏好"},
    {"cat": "全球工业", "ticker": "EXI", "code": "GLOBAL_IND", "country": "GLOBAL", "name": "iShares全球工业指数ETF (EXI)", "driver": "全球跨国制造业资本开支与贸易景气度"},
    {"cat": "全球工业", "ticker": "XLI", "code": "US_IND_ETF", "country": "US", "name": "美国工业板块指数ETF (XLI)", "driver": "北美基建回流与制造业PMI市场领先指标"},
    # 全球货币
    {"cat": "全球货币", "ticker": "DX-Y.NYB", "code": "DXY", "country": "US", "name": "ICE美元指数 (DXY)", "driver": "全球离岸美元松紧总阀门"},
    {"cat": "全球货币", "ticker": "JPY=X", "code": "USDJPY", "country": "JP", "name": "美元兑日元 (USD/JPY)", "driver": "全球套息交易(Carry Trade)核心风向标"},
    {"cat": "全球货币", "ticker": "CNH=X", "code": "USDCNH", "country": "CN", "name": "美元兑离岸人民币 (USD/CNH)", "driver": "中美利差与出口企业结汇意愿"},
    {"cat": "全球货币", "ticker": "EURUSD=X", "code": "EURUSD", "country": "EA", "name": "欧元兑美元 (EUR/USD)", "driver": "欧美真实利差与能源贸易条件"},
    {"cat": "全球货币", "ticker": "GBPUSD=X", "code": "GBPUSD", "country": "UK", "name": "英镑兑美元 (GBP/USD)", "driver": "英美利差与英国财政信用溢价"},
    {"cat": "全球货币", "ticker": "AUDUSD=X", "code": "AUDUSD", "country": "AU", "name": "澳元兑美元 (AUD/USD)", "driver": "全球工业大宗商品周期与亚洲需求"},
    # 黄金与贵金属
    {"cat": "黄金贵金属", "ticker": "GC=F", "code": "GOLD", "country": "GLOBAL", "name": "COMEX黄金期货 (美元/盎司)", "driver": "10Y TIPS实际利率 + 央行去美元化购金"},
    {"cat": "黄金贵金属", "ticker": "SI=F", "code": "SILVER", "country": "GLOBAL", "name": "COMEX白银期货 (美元/盎司)", "driver": "金融属性(随黄金) + 光伏/工业需求弹性"},
    # 大宗商品
    {"cat": "大宗商品", "ticker": "CL=F", "code": "WTI", "country": "GLOBAL", "name": "WTI原油期货 (美元/桶)", "driver": "北美现货库存与OPEC+供给约束"},
    {"cat": "大宗商品", "ticker": "BZ=F", "code": "BRENT", "country": "GLOBAL", "name": "布伦特原油期货 (美元/桶)", "driver": "全球通胀预期与供给冲击滞胀扳机"},
    {"cat": "大宗商品", "ticker": "HG=F", "code": "COPPER", "country": "GLOBAL", "name": "COMEX精炼铜 (美元/磅)", "driver": "全球制造业与电网AI基建实体需求"},
    {"cat": "大宗商品", "ticker": "NG=F", "code": "NATGAS", "country": "US", "name": "NYMEX天然气期货", "driver": "欧美能源成本与季节性库存周期"},
    {"cat": "大宗商品", "ticker": "ZW=F", "code": "WHEAT", "country": "GLOBAL", "name": "CBOT小麦期货 (美分/蒲式耳)", "driver": "全球食品通胀与新兴市场社会稳定"},
    # 波动率
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
            valid = [(datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"), float(c))
                     for ts, c in zip(timestamps, closes) if c is not None]
            if len(valid) >= 2:
                valid.reverse()  # 索引 0 为最新交易日
                return valid[0][0], [v[1] for v in valid], dict(valid)
        except Exception:
            continue
    return None, [], {}


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
        time.sleep(0.08)

    # 加密资产 (优先 CoinGecko，失败自动回退 Yahoo Finance)
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
        print(f"[WARN] CoinGecko primary failed, switching to Yahoo fallback: {e}")

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
            f"&file_type=json&sort_order=desc&limit=25"
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
            series_hist_dates[item["id"]] = {o["date"]: float(o["value"]) for o in obs}
            val_curr = vals[0]
            val_prev = vals[1] if len(vals) > 1 else val_curr
            records.append({
                "观测日期": obs[0]["date"], "国家/地区": item["country"], "六层归属": item["layer"],
                "指标代码": item["id"], "指标名称": item["name"],
                "最新值": round(val_curr, 4), "前值": round(val_prev, 4), "边际变化": round(val_curr - val_prev, 4),
                "数据源": "FRED API", "同步时间": NOW_STR,
            })
            time.sleep(0.1)
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

    try:
        df_pmi = ak.macro_china_pmi()
        col_dt = next((c for c in df_pmi.columns if "月份" in str(c) or "日期" in str(c)), df_pmi.columns[0])
        col_mfg = next((c for c in df_pmi.columns if "制造业" in str(c) and "非制造业" not in str(c) and "指数" in str(c)), None)
        col_non = next((c for c in df_pmi.columns if "非制造业" in str(c) and "指数" in str(c)), None)
        if col_mfg:
            df_c = df_pmi.dropna(subset=[col_mfg]).copy()
            if len(df_c) >= 2:
                r0, r1 = (df_c.iloc[0], df_c.iloc[1]) if str(df_c.iloc[0][col_dt]) > str(df_c.iloc[-1][col_dt]) else (df_c.iloc[-1], df_c.iloc[-2])
                dt_pmi = str(r0[col_dt])[:10]
                add_cn_rec(dt_pmi, "L3-工业与消费", "CN_PMI_MFG", "中国官方制造业PMI (荣枯线=50)", float(r0[col_mfg]), float(r1[col_mfg]))
                if col_non and pd.notna(r0[col_non]) and pd.notna(r1[col_non]):
                    add_cn_rec(dt_pmi, "L3-工业与消费", "CN_PMI_NONMFG", "中国官方非制造业PMI (荣枯线=50)", float(r0[col_non]), float(r1[col_non]))
    except Exception as e:
        print(f"[WARN] CN PMI failed: {e}")

    try:
        df_ind = ak.macro_china_gyzjz()
        col_dt = next((c for c in df_ind.columns if "月份" in str(c) or "日期" in str(c)), df_ind.columns[0])
        col_yoy = next((c for c in df_ind.columns if "同比" in str(c)), None)
        if col_yoy:
            df_c = df_ind.dropna(subset=[col_yoy])
            if len(df_c) >= 2:
                r0, r1 = (df_c.iloc[0], df_c.iloc[1]) if str(df_c.iloc[0][col_dt]) > str(df_c.iloc[-1][col_dt]) else (df_c.iloc[-1], df_c.iloc[-2])
                add_cn_rec(str(r0[col_dt])[:10], "L3-工业与消费", "CN_IND_YOY", "中国规模以上工业增加值同比 (%)", float(r0[col_yoy]), float(r1[col_yoy]))
    except Exception as e:
        print(f"[WARN] CN Industrial failed: {e}")

    try:
        df_tsf = ak.macro_china_shrzgm()
        col_dt = next((c for c in df_tsf.columns if "月份" in str(c) or "日期" in str(c)), df_tsf.columns[0])
        col_val = next((c for c in df_tsf.columns if "社会融资规模增量" in str(c)), None)
        if col_val:
            df_c = df_tsf.dropna(subset=[col_val])
            if len(df_c) >= 2:
                r0, r1 = (df_c.iloc[0], df_c.iloc[1]) if str(df_c.iloc[0][col_dt]) > str(df_c.iloc[-1][col_dt]) else (df_c.iloc[-1], df_c.iloc[-2])
                add_cn_rec(str(r0[col_dt])[:10], "L1-信用引擎", "CN_TSF_NEW", "中国社会融资规模增量 (亿元)", float(r0[col_val]), float(r1[col_val]))
    except Exception as e:
        print(f"[WARN] CN TSF failed: {e}")

    try:
        df_cpi = ak.macro_china_cpi()
        col_dt = next((c for c in df_cpi.columns if "月份" in str(c) or "日期" in str(c)), df_cpi.columns[0])
        col_yoy = next((c for c in df_cpi.columns if "全国" in str(c) and "同比" in str(c)), next((c for c in df_cpi.columns if "同比" in str(c)), None))
        if col_yoy:
            df_c = df_cpi.dropna(subset=[col_yoy])
            if len(df_c) >= 2:
                r0, r1 = (df_c.iloc[0], df_c.iloc[1]) if str(df_c.iloc[0][col_dt]) > str(df_c.iloc[-1][col_dt]) else (df_c.iloc[-1], df_c.iloc[-2])
                add_cn_rec(str(r0[col_dt])[:10], "L3-通胀硬数据", "CN_CPI_YOY", "中国CPI同比增速 (%)", float(r0[col_yoy]), float(r1[col_yoy]))
    except Exception as e:
        print(f"[WARN] CN CPI failed: {e}")

    try:
        df_ppi = ak.macro_china_ppi()
        col_dt = next((c for c in df_ppi.columns if "月份" in str(c) or "日期" in str(c)), df_ppi.columns[0])
        col_yoy = next((c for c in df_ppi.columns if "同比" in str(c)), None)
        if col_yoy:
            df_c = df_ppi.dropna(subset=[col_yoy])
            if len(df_c) >= 2:
                r0, r1 = (df_c.iloc[0], df_c.iloc[1]) if str(df_c.iloc[0][col_dt]) > str(df_c.iloc[-1][col_dt]) else (df_c.iloc[-1], df_c.iloc[-2])
                add_cn_rec(str(r0[col_dt])[:10], "L3-通胀硬数据", "CN_PPI_YOY", "中国PPI工业品出厂价格同比 (%)", float(r0[col_yoy]), float(r1[col_yoy]))
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
            records.append({
                "观测日期": sofr[0]["effectiveDate"], "国家/地区": "US", "六层归属": "L2-流动性管道",
                "指标代码": "NYFED_SOFR", "指标名称": "纽约联储 SOFR 隔夜利率 (%)",
                "最新值": c, "前值": p, "边际变化": round(c - p, 4),
                "数据源": "NY Fed API", "同步时间": NOW_STR,
            })
    except Exception as e:
        print(f"[WARN] NYFed failed: {e}")

    # 2. 美国财政部近期 10Y/30Y 美债拍卖投标倍数
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
                extra_vals["UST_BID_COVER"] = bid_to_cover
                records.append({
                    "观测日期": auc.get("record_date", TODAY_STR), "国家/地区": "US", "六层归属": "L6-黑天鹅雷达",
                    "指标代码": "UST_AUCTION_BTC", "指标名称": f"美债({auc.get('security_term')})拍卖投标倍数",
                    "最新值": bid_to_cover, "前值": high_yield, "边际变化": 0.0,
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
    """利用已拉取的近20个交易日Yahoo/FRED序列，生成历史回填序列，确保首日图表与统计公式完整"""
    base_dates = sorted(series_hist_dates.get("SPX", {}).keys())
    if not base_dates:
        return []
    seed_rows = []
    last_known = {}
    for dt in base_dates[-20:]:
        row_dict = {"Date": dt}
        for sid, dmap in series_hist_dates.items():
            # 取 <= dt 的最近有效值（前向填充周频/月频宏观序列）
            valid_dts = [d for d in dmap.keys() if d <= dt]
            if valid_dts:
                val = round(dmap[max(valid_dts)], 4)
                last_known[sid] = val
                row_dict[sid] = val
            elif sid in last_known:
                row_dict[sid] = last_known[sid]
            elif sid in ts_snapshot_dict:
                row_dict[sid] = ts_snapshot_dict[sid]
        # 同步计算历史每日衍生指标
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
        seed_rows.append(row_dict)
    return seed_rows


def analyze_macro_and_blackswan(s_obs, s_dates, wb_metrics, imf_metrics, extra_vals, market_stats):
    blackswan_rows = []
    macro_indicator_rows = []
    ts_snapshot_dict = {}

    def get(sid, idx=0):
        arr = s_obs.get(sid, [])
        return arr[idx] if len(arr) > idx else None

    def calc_yoy(sid):
        v0, v12 = get(sid, 0), get(sid, 12)
        if v0 is not None and v12 is not None and v12 != 0:
            return round((v0 / v12 - 1) * 100, 2)
        return None

    for sid, arr in s_obs.items():
        if arr:
            ts_snapshot_dict[sid] = round(arr[0], 4)

    # ================= A. 经济硬数据汇总 =================
    nfp_0, nfp_1 = get("PAYEMS", 0), get("PAYEMS", 1)
    nfp_add_10k = None
    if nfp_0 is not None and nfp_1 is not None:
        nfp_add_10k = round((nfp_0 - nfp_1) / 10.0, 2)
        ts_snapshot_dict["US_NFP_ADD_10K"] = nfp_add_10k
        nfp_sig = "强劲扩张 (>20万，抑制降息)" if nfp_add_10k > 20 else ("温和着陆 (10~20万)" if nfp_add_10k >= 10 else "⚠️ 就业显著降温 (<10万)")
        macro_indicator_rows.append(["美国 (US)", "就业", "单月新增非农就业 (万人)", f"{nfp_add_10k:+.2f}", f"总就业 {nfp_0:.0f}千人", s_dates.get("PAYEMS", "-"), nfp_sig])

    icsa_0, icsa_1 = get("ICSA", 0), get("ICSA", 1)
    if icsa_0 is not None:
        icsa_sig = "⚠️ 裁员加速 (>25万)" if icsa_0 > 250000 else "低位稳健 (<25万)"
        macro_indicator_rows.append(["美国 (US)", "就业", "周度初请失业金人数 (人)", f"{icsa_0:,.0f}", f"{icsa_1:,.0f}" if icsa_1 else "-", s_dates.get("ICSA", "-"), icsa_sig])

    unrate_0, unrate_1 = get("UNRATE", 0), get("UNRATE", 1)
    if unrate_0 is not None:
        macro_indicator_rows.append(["美国 (US)", "就业", "美国失业率 (%)", f"{unrate_0:.1f}%", f"{unrate_1:.1f}%" if unrate_1 else "-", s_dates.get("UNRATE", "-"), f"萨姆规则={get('SAHMREALTIME') or 0:.2f}%"])

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
        macro_indicator_rows.append(["美国 (US)", "工业", "美国工业生产总指数 (2017=100)", f"{ind_0:.2f}", f"同比 {us_ind_yoy:+.2f}%" if us_ind_yoy is not None else "-", s_dates.get("INDPRO", "-"), "美国制造业与采矿产出硬指标"])

    rs_0, rs_1 = get("RSAFS", 0), get("RSAFS", 1)
    if rs_0 is not None and rs_1:
        rs_mom = round((rs_0 / rs_1 - 1) * 100, 2)
        macro_indicator_rows.append(["美国 (US)", "消费", "美国零售销售环比 (%)", f"{rs_mom:+.2f}%", f"${rs_0:,.0f}M", s_dates.get("RSAFS", "-"), "居民消费韧性" if rs_mom > 0 else "消费支出走弱"])

    us_cpi_yoy = calc_yoy("CPIAUCSL")
    if us_cpi_yoy is not None:
        ts_snapshot_dict["US_CPI_YOY"] = us_cpi_yoy
        macro_indicator_rows.append(["美国 (US)", "通胀", "美国总体 CPI 同比 (%)", f"{us_cpi_yoy:.2f}%", "-", s_dates.get("CPIAUCSL", "-"), "高于2%目标" if us_cpi_yoy > 2.5 else "接近通胀目标"])

    pce_0, pce_3 = get("PCEPILFE", 0), get("PCEPILFE", 3)
    pce_3m_ann = None
    if pce_0 and pce_3 and pce_3 > 0:
        pce_3m_ann = round(((pce_0 / pce_3) ** 4 - 1) * 100, 2)
        ts_snapshot_dict["US_CORE_PCE_3M_ANN"] = pce_3m_ann
        pce_sig = "通胀黏性偏高 (>2.5%)" if pce_3m_ann > 2.5 else "通胀温和可控 (<=2.5%)"
        macro_indicator_rows.append(["美国 (US)", "通胀", "核心 PCE 3M环比折年率 (%)", f"{pce_3m_ann:.2f}%", "-", s_dates.get("PCEPILFE", "-"), pce_sig])

    cn_pmi = get("CN_PMI_MFG")
    if cn_pmi is not None:
        pmi_sig = "🟢 扩张区间 (>50)" if cn_pmi >= 50.0 else "🟡 收缩区间 (<50，待政策发力)"
        macro_indicator_rows.append(["中国 (CN)", "工业/景气", "官方制造业 PMI (荣枯线=50)", f"{cn_pmi:.1f}", f"{get('CN_PMI_MFG', 1):.1f}", s_dates.get("CN_PMI_MFG", "-"), pmi_sig])

    cn_ind = get("CN_IND_YOY")
    if cn_ind is not None:
        macro_indicator_rows.append(["中国 (CN)", "工业", "规模以上工业增加值同比 (%)", f"{cn_ind:.2f}%", f"{get('CN_IND_YOY', 1):.2f}%", s_dates.get("CN_IND_YOY", "-"), "中国工业生产实际增速"])

    cn_tsf = get("CN_TSF_NEW")
    if cn_tsf is not None:
        macro_indicator_rows.append(["中国 (CN)", "信用", "社会融资规模增量 (亿元)", f"{cn_tsf:,.0f}", f"{get('CN_TSF_NEW', 1):,.0f}", s_dates.get("CN_TSF_NEW", "-"), "中国宽信用脉冲总量"])

    cn_cpi, cn_ppi = get("CN_CPI_YOY"), get("CN_PPI_YOY")
    ppi_cpi_gap = None
    if cn_cpi is not None and cn_ppi is not None:
        ppi_cpi_gap = round(cn_ppi - cn_cpi, 2)
        ts_snapshot_dict["CN_PPI_CPI_GAP"] = ppi_cpi_gap
        macro_indicator_rows.append(["中国 (CN)", "通胀", "CPI同比 / PPI同比 (%)", f"CPI {cn_cpi:.1f}% / PPI {cn_ppi:.1f}%", f"剪刀差 {ppi_cpi_gap:+.1f}%", s_dates.get("CN_CPI_YOY", "-"), "通缩压力观测" if cn_ppi < 0 else "再通胀周期"])

    ea_cpi_yoy = calc_yoy("CP0000EZ19M086NEST")
    if ea_cpi_yoy is not None:
        ts_snapshot_dict["EA_CPI_YOY"] = ea_cpi_yoy
        macro_indicator_rows.append(["欧元区 (EA)", "通胀", "欧元区调和 CPI 同比 (%)", f"{ea_cpi_yoy:.2f}%", "-", s_dates.get("CP0000EZ19M086NEST", "-"), "欧央行利率路径核心锚"])
    ea_unemp = get("LRHUTTTTEZM156S")
    if ea_unemp is not None:
        macro_indicator_rows.append(["欧元区 (EA)", "就业", "欧元区失业率 (%)", f"{ea_unemp:.1f}%", f"{get('LRHUTTTTEZM156S', 1):.1f}%", s_dates.get("LRHUTTTTEZM156S", "-"), "欧洲劳动力市场状态"])

    jp_ind_mom = get("JPNPRMNTO01GPSAM")
    if jp_ind_mom is not None:
        macro_indicator_rows.append(["日本 (JP)", "工业", "日本制造业生产环比 (%)", f"{jp_ind_mom:+.2f}%", f"{get('JPNPRMNTO01GPSAM', 1):+.2f}%", s_dates.get("JPNPRMNTO01GPSAM", "-"), "日本出口与工业产出动能"])
    jp_cpi_yoy = calc_yoy("JPNCPIALLMINMEI")
    if jp_cpi_yoy is not None:
        ts_snapshot_dict["JP_CPI_YOY"] = jp_cpi_yoy
        macro_indicator_rows.append(["日本 (JP)", "通胀", "日本全国 CPI 同比 (%)", f"{jp_cpi_yoy:.2f}%", "-", s_dates.get("JPNCPIALLMINMEI", "-"), "日本央行加息判定指标"])

    gl_ind_stat = market_stats.get("GLOBAL_IND", {})
    if gl_ind_stat.get("price"):
        macro_indicator_rows.append(["全球 (GLOBAL)", "工业景气", "全球工业指数 ETF (EXI)", f"{gl_ind_stat['price']}", f"5日 {gl_ind_stat.get('pct_5d', 0):+.2f}% | 20日 {gl_ind_stat.get('pct_20d', 0):+.2f}%", TODAY_STR, "全球制造业资本开支高频温度计"])

    # ================= B. 衍生指标 Python 侧状态计算 (用于时序存储与解读文本) =================
    walcl, tga, rrp = get("WALCL"), get("WDTGAL"), get("RRPONTSYD")
    walcl_p, tga_p, rrp_p = get("WALCL", 1), get("WDTGAL", 1), get("RRPONTSYD", 1)
    net_liq, net_liq_chg = None, 0.0
    if all(v is not None for v in [walcl, tga, rrp]):
        net_liq = round((walcl - tga) / 1000.0 - rrp, 2)
        ts_snapshot_dict["NET_LIQUIDITY"] = net_liq
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

    cn_m1_0, cn_m1_12 = get("MYAGM1CNM189N", 0), get("MYAGM1CNM189N", 12)
    cn_m2_0, cn_m2_12 = get("MYAGM2CNM189N", 0), get("MYAGM2CNM189N", 12)
    cn_scissors = None
    if all(v is not None and v > 0 for v in [cn_m1_0, cn_m1_12, cn_m2_0, cn_m2_12]):
        cn_scissors = round(((cn_m1_0 / cn_m1_12) - (cn_m2_0 / cn_m2_12)) * 100, 2)
        ts_snapshot_dict["CN_M1_M2_SCISSORS"] = cn_scissors

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

    # ================= C. 黑天鹅 8 维雷达评估 =================
    red_alerts, yellow_alerts = [], []

    sofr, iorb = extra_vals.get("SOFR"), get("IORB")
    if sofr is not None and iorb is not None:
        sofr_sp = round((sofr - iorb) * 100, 1)
        if sofr_sp >= 25:
            st = "🔴 红灯熔断"
            red_alerts.append(f"SOFR-IORB 利差飙升至 +{sofr_sp}bps")
        elif sofr_sp >= 10:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"SOFR-IORB 利差走阔至 +{sofr_sp}bps")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-1", "美元回购管道", "SOFR - IORB 利差 (bps)", sofr_sp, "10 bps", "25 bps", st, "衡量银行间美元准备金充裕度，飙升预示钱荒"])

    hy = get("BAMLH0A0HYM2")
    if hy is not None:
        hy_bps = round(hy * 100, 0)
        if hy_bps >= 500:
            st = "🔴 红灯熔断"
            red_alerts.append(f"高收益债利差突破 {hy_bps}bps")
        elif hy_bps >= 350:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"高收益债利差升至 {hy_bps}bps")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-2", "企业信用违约", "高收益债信用利差 (bps)", hy_bps, "350 bps", "500 bps", st, "垃圾债融资成本飙升将引发再融资断裂与违约潮"])

    fsi = get("STLFSI4")
    if fsi is not None:
        if fsi >= 2.0:
            st = "🔴 红灯熔断"
            red_alerts.append(f"金融压力指数 STLFSI4 达 {fsi}")
        elif fsi >= 0.5:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"金融压力指数 STLFSI4 升至 {fsi}")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-3", "综合金融压力", "圣路易斯金融压力 STLFSI4", round(fsi, 2), "0.50", "2.00", st, "0为历史均值，突破2.0代表系统性金融压力爆发"])

    spx_5d = market_stats.get("SPX", {}).get("pct_5d")
    dxy_5d = market_stats.get("DXY", {}).get("pct_5d")
    if all(v is not None for v in [spx_5d, gold_5d, dxy_5d, y10, y10_5]):
        y10_5d_bps = round((y10 - y10_5) * 100, 1)
        summary_str = f"SPX:{spx_5d:+.1f}% | 金:{gold_5d:+.1f}% | 10Y:{y10_5d_bps:+.0f}bp"
        if spx_5d <= -4.0 and gold_5d <= -2.0 and y10_5d_bps >= 20 and dxy_5d >= 1.5:
            st = "🔴 红灯熔断"
            red_alerts.append(f"现金为王流动性挤兑 ({summary_str})")
        elif spx_5d <= -2.5 and gold_5d <= -1.0:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"股金同跌流动性趋紧 ({summary_str})")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-4", "现金为王挤兑", "5日股/金/债联动状态", summary_str, "股金同跌", "股金暴跌+债率飙升", st, "检测Margin Call下机构无差别抛售黄金美债换美元"])

    jpy_5d = market_stats.get("USDJPY", {}).get("pct_5d")
    if jpy_5d is not None:
        if jpy_5d <= -4.0:
            st = "🔴 红灯熔断"
            red_alerts.append(f"USD/JPY 5日暴跌 {jpy_5d}%")
        elif jpy_5d <= -2.5:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"USD/JPY 5日下跌 {jpy_5d}%")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-5", "套息交易踩踏", "USD/JPY 5日涨跌幅 (%)", jpy_5d, "-2.5%", "-4.0%", st, "日元急速升值迫使全球低息日元融资盘平仓抛售美股"])

    vix = market_stats.get("VIX", {}).get("price")
    vix3m = get("VXVCLS")
    if vix is not None and vix3m is not None and vix3m > 0:
        vix_ratio = round(vix / vix3m, 2)
        if vix >= 35 or vix_ratio >= 1.15:
            st = "🔴 红灯熔断"
            red_alerts.append(f"VIX={vix}, VIX/VIX3M={vix_ratio}")
        elif vix >= 25 or vix_ratio >= 1.0:
            st = "🟡 黄灯警戒"
            yellow_alerts.append(f"VIX={vix}, VIX/VIX3M={vix_ratio}")
        else:
            st = "🟢 正常"
        blackswan_rows.append(["BS-6", "波动率倒挂", f"VIX/VIX3M (VIX={vix})", vix_ratio, "1.00", "1.15", st, "短端隐含波动率高于3个月远期，代表短期尾部挤兑"])

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
        res = wb_metrics.get((iso3, "FI.RES.TOTL.CD"))
        debt = wb_metrics.get((iso3, "DT.DOD.DSTC.CD"))
        debt_ratio = round((debt / res) * 100, 1) if (res and debt) else ""
        if isinstance(debt_ratio, float):
            st = "🔴 红灯熔断" if debt_ratio > 100 else ("🟡 黄灯警戒" if debt_ratio > 50 else "🟢 正常")
            blackswan_rows.append([f"BS-7", f"EM外债({iso3})", "短期外债/外储比率 (%)", debt_ratio, "50.0%", "100.0%", st, "外储不足以覆盖1年内到期外债，强美元下易发货币危机"])
        country_rows.append([iso3, country_names.get(iso3, iso3), gdp, cpi, b10, "", ca, fiscal, debt_ratio])

    # ================= E. 生成五步中文研判报告 =================
    sahm = get("SAHMREALTIME") or 0.0
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

    report_text = (
        f"【宏观第一性原理自动研判简报 | 更新时间: {NOW_STR}】\n"
        f"1. 周期定位与核心硬数据：{quadrant} 美国单月新增非农 {nfp_add_10k if nfp_add_10k is not None else '-'} 万人、"
        f"CPI同比 {us_cpi_yoy if us_cpi_yoy is not None else '-'}%、工业生产同比 {us_ind_yoy if us_ind_yoy is not None else '-'}%、萨姆规则 {sahm}%；"
        f"中国制造业PMI {cn_pmi if cn_pmi is not None else '-'}、工业增加值同比 {cn_ind if cn_ind is not None else '-'}%。\n"
        f"2. 水管与跨资产锚点：美联储净流动性 {net_liq} 十亿美元（环比 {net_liq_chg:+}），10Y TIPS实际利率 {tips10}%，"
        f"美债10Y-2Y利差 {spread_10_2}%（{curve_regime}）。市场定价：SPX={spx_p} | 沪深300={csi_p} | DXY={dxy_p} | USD/JPY={jpy_p} | 黄金={au_p} | 布油={br_p} | BTC={btc_p}。\n"
        f"3. 传导机制与定价主导：{regime_logic} {gold_regime_note}\n"
        f"4. 黑天鹅雷达扫描：ON RRP {rrp} 十亿美元，TGA {round(tga/1000, 1) if tga else '-'} 十亿美元。"
        f"{'⚠️ 预警触发：' + '；'.join(red_alerts + yellow_alerts) if (red_alerts or yellow_alerts) else '✅ 8大黑天鹅雷达均处于绿灯常态区间，未见流动性挤兑信号。'}\n"
        f"5. 情景概率分布：{probs}\n"
        f"6. 强制证伪条件（若未来 10 个交易日出现以下任一信号，需推翻当前基准判断）："
        f"① 10Y TIPS 实际利率反向突破 {'1.80%' if tips10 > 2.0 else '2.25%'}；"
        f"② 美国高收益债信用利差突破 500bps 或 VIX 飙升破 30；"
        f"③ 美联储净流动性连续两周反向变动超 1,500 亿美元。"
    )

    meta_ctx = {
        "net_liq_chg": net_liq_chg,
        "d_spread": d_spread,
        "curve_regime": curve_regime,
        "cn_scissors": cn_scissors,
        "pce_3m_ann": pce_3m_ann,
        "ppi_cpi_gap": ppi_cpi_gap,
        "nfp_add_10k": nfp_add_10k,
        "us_ind_yoy": us_ind_yoy,
        "cn_pmi": cn_pmi,
        "gold_regime_note": gold_regime_note,
    }
    return blackswan_rows, country_rows, macro_indicator_rows, ts_snapshot_dict, report_text, meta_ctx


def get_or_create_ws(sh, title, rows=300, cols=25):
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

    # 迁移旧版日志（若存在）
    legacy_logs = []
    try:
        old_j = sh.worksheet("06_情景与证伪日志_Journal")
        vals = [r for r in old_j.get_all_values() if r and len(r) >= 3 and r[0] != "日期" and str(r[0]).strip()]
        legacy_logs = vals
    except Exception:
        pass

    # 创建/获取 4 张精简核心工作表
    ws_dash = get_or_create_ws(sh, TAB_DASH, rows=120, cols=26)
    ws_mkt_mac = get_or_create_ws(sh, TAB_MKT_MAC, rows=200, cols=15)
    ws_ts = get_or_create_ws(sh, TAB_TS, rows=2000, cols=100)
    ws_raw_log = get_or_create_ws(sh, TAB_RAW_LOG, rows=400, cols=16)

    # 清理所有旧版冗余分页（保证表格精简为4张）
    for old_name in LEGACY_TABS:
        if old_name not in [TAB_DASH, TAB_MKT_MAC, TAB_TS, TAB_RAW_LOG]:
            try:
                old_ws = sh.worksheet(old_name)
                sh.del_worksheet(old_ws)
            except Exception:
                pass

    # ================= 1. 更新 02_历史时序库_TimeSeries (含首日自动历史回填) =================
    existing = [r for r in ws_ts.get_all_values() if r and len(r) > 0 and str(r[0]).strip()]
    all_keys = ["Date"] + sorted(ts_dict.keys())

    if len(existing) <= 2 and seed_rows:
        # 历史数据不足时，写入近20个交易日历史回填基底 + 今日最新值
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

    # 建立 {指标代码: 列字母} 映射字典，供驾驶舱动态生成科学公式与原生图表
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
    # 右侧 L:N 写入研判与证伪日志
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

    mac_start_row = len(mkt_matrix) + 2
    mac_headers = ["国家/区域", "经济维度", "核心指标名称", "最新核心读数", "前值/参考基准", "数据发布期", "第一性原理周期信号"]
    mkt_matrix.append([])
    mkt_matrix.append(["【第二部分】全球主要经济体高权重宏观硬数据（非农 / PMI / 工业产出 / 社融 / CPI-PPI）"])
    mkt_matrix.append(mac_headers)
    for m_row in macro_rows:
        mkt_matrix.append(m_row)

    ws_mkt_mac.update(range_name="A1", values=mkt_matrix, value_input_option="USER_ENTERED")

    # ================= 4. 更新 00_宏观驾驶舱_Dashboard (科学公式矩阵 + 雷达 + 多国对比) =================
    ws_dash.clear()
    dash_rows = []
    # Row 1-2: 标题与五步研判简报
    dash_rows.append(["全球宏观第一性原理实时监测驾驶舱（科学公式计算 + 多维可视化中枢）"])
    dash_rows.append([report_text])
    dash_rows.append([])

    # Row 4-5: 模块一表头
    dash_rows.append(["【模块一】科学公式动态计算矩阵（实时联动全量底表与历史时序库）"])
    dash_rows.append(["层级归属", "复合衍生指标", "公式实时计算值", "边际变化", "历史分位", "Z-Score偏离", "20日走势图", "科学公式定义", "第一性原理诊断信号"])

    # 动态生成 12 行科学公式指标 (从 Row 6 开始)
    sci_specs = [
        ("L2-美元水管", "美联储净流动性 (十亿美元)",
         f"=IFERROR(({raw_lookup('WALCL')}-{raw_lookup('WDTGAL')})/1000-{raw_lookup('RRPONTSYD')}, \"\")",
         meta_ctx["net_liq_chg"], "NET_LIQUIDITY", "(WALCL - TGA)/1000 - ON RRP",
         "放水扩张 (支撑风险资产)" if meta_ctx["net_liq_chg"] > 0 else "抽水收缩 (警惕准备金消耗)"),
        ("L2-利率之锚", "美债 10Y-2Y 期限利差 (%)",
         f"=IFERROR({raw_lookup('DGS10')}-{raw_lookup('DGS2')}, \"\")",
         meta_ctx["d_spread"], "SPREAD_10Y2Y", "DGS10 - DGS2", meta_ctx["curve_regime"]),
        ("L2-利率之锚", "美国实际政策立场 (EFFR-核心PCE, %)",
         f"=IFERROR({raw_lookup('DFF')}-C15, \"\")",
         "-", None, "EFFR - 核心PCE 3M年化", "正值越高代表货币政策实际紧缩力度越强"),
        ("L2-通胀预期", "10Y盈亏平衡通胀预期 (Breakeven, %)",
         f"=IFERROR({raw_lookup('DGS10')}-{raw_lookup('DFII10')}, \"\")",
         "-", "BREAKEVEN_10Y", "DGS10 - 10Y TIPS", "市场隐含未来10年平均通胀预期"),
        ("L4-跨境利差", "中美 10Y 无风险利差 (US-CN, %)",
         f"=IFERROR({raw_lookup('DGS10')}-{raw_lookup('CN_DGS10')}, \"\")",
         "-", None, "US 10Y - CN 10Y", "决定人民币汇率压力与跨境资本流向"),
        ("L4-跨境利差", "美日 10Y 套息利差 (US-JP, %)",
         f"=IFERROR({raw_lookup('DGS10')}-{raw_lookup('IRLTLT01JPM156N')}, \"\")",
         "-", None, "US 10Y - JP 10Y", "决定日元套息交易(Carry Trade)安全垫"),
        ("L5-跨资产锚", "铜金比 (Copper/Gold × 1000)",
         f"=IFERROR(({raw_lookup('COPPER')}*1000)/{raw_lookup('GOLD')}, \"\")",
         "-", "COPPER_GOLD_RATIO", "(COMEX铜 × 1000) / COMEX金", "全球实体制造业增长预期 vs 避险溢价"),
        ("L5-跨资产锚", "金油比 (Gold / Brent)",
         f"=IFERROR({raw_lookup('GOLD')}/{raw_lookup('BRENT')}, \"\")",
         "-", "GOLD_OIL_RATIO", "COMEX黄金 / 布伦特原油", "高于30通常预示需求疲软或地缘避险极端"),
        ("L5-相关性矩阵", "股金动态相关系数 (SPX vs GOLD)",
         f"=IFERROR(CORREL({ts_range('SPX')}, {ts_range('GOLD')}), 0)" if ts_range("SPX") and ts_range("GOLD") else 0,
         "-", None, "CORREL(SPX, GOLD)", "正常为负或弱相关；趋近+1警示流动性无差别抛售"),
        ("L3-通胀硬锚", "美国核心PCE 3M环比折年率 (%)",
         meta_ctx["pce_3m_ann"] if meta_ctx["pce_3m_ann"] is not None else "-",
         "-", "US_CORE_PCE_3M_ANN", "(PCE_t / PCE_{t-3})^4 - 1", "美联储最看重的短期核心通胀动量"),
        ("L1-信用引擎", "中国 M1-M2 资金活化剪刀差 (%)",
         meta_ctx["cn_scissors"] if meta_ctx["cn_scissors"] is not None else "-",
         "-", "CN_M1_M2_SCISSORS", "CN_M1_YoY - CN_M2_YoY", "负值走阔代表资金定期化沉淀，转正利好A股"),
        ("L3-盈利周期", "中国 PPI - CPI 盈利剪刀差 (%)",
         f"=IFERROR({raw_lookup('CN_PPI_YOY')}-{raw_lookup('CN_CPI_YOY')}, \"\")",
         "-", "CN_PPI_CPI_GAP", "CN_PPI_YoY - CN_CPI_YoY", "剪刀差收敛或转正领先工业企业利润修复"),
    ]

    for idx, (layer, title, formula_val, delta, ts_code, formula_desc, sig) in enumerate(sci_specs, start=6):
        c_ref = f"C{idx}"
        dash_rows.append([
            layer, title, formula_val, delta,
            pctrank_formula(ts_code, c_ref) if ts_code else "-",
            zscore_formula(ts_code, c_ref) if ts_code else "-",
            spark_formula(ts_code) if ts_code else "-",
            formula_desc, sig
        ])

    # 模块二：黑天鹅 8 维熔断雷达
    dash_rows.append([])
    bs_title_row = len(dash_rows) + 1
    dash_rows.append(["【模块二】黑天鹅 8 维系统性风险熔断雷达（实时阈值监控）"])
    dash_rows.append(["风险编号", "风险维度", "核心监控指标", "当前读数", "黄灯警戒线", "红灯熔断线", "当前状态灯", "传导机制与熔断触发后果", ""])
    bs_data_start = len(dash_rows) + 1
    for bs in blackswan_rows:
        dash_rows.append([bs[0], bs[1], bs[2], bs[3], bs[4], bs[5], bs[6], bs[7], ""])
    bs_data_end = len(dash_rows)

    # 模块三：多国周期与真实无风险利率对比
    dash_rows.append([])
    cty_title_row = len(dash_rows) + 1
    dash_rows.append(["【模块三】全球主要经济体周期与费雪真实利率对比（公式自动计算）"])
    dash_rows.append(["代码", "国家/经济体", "实际GDP增速(%)", "CPI通胀(%)", "10Y国债(%)", "真实无风险利率(公式)", "经常账户/GDP(%)", "财政余额/GDP(%)", "短期外债/外储(%)"])
    cty_data_start = len(dash_rows) + 1
    for idx_c, c_row in enumerate(country_rows, start=cty_data_start):
        row_copy = list(c_row)
        row_copy[5] = f'=IFERROR(E{idx_c}-D{idx_c}, "-")'  # 费雪公式: 10Y名义国债 - CPI通胀
        dash_rows.append(row_copy)
    cty_data_end = len(dash_rows)

    ws_dash.update(range_name="A1", values=dash_rows, value_input_option="USER_ENTERED")

    # ================= 5. 批量应用专业样式 + 清理旧图 + 生成 6 幅原生可视化图表 =================
    meta = sh.fetch_sheet_metadata()
    dash_id = ws_dash.id
    mkt_id = ws_mkt_mac.id
    ts_id = ws_ts.id

    requests_batch = []

    # 5.1 删除驾驶舱上已有的旧图表，防止每日定时运行产生重复图表
    for s in meta.get("sheets", []):
        if s["properties"]["sheetId"] == dash_id:
            for ch in s.get("charts", []):
                requests_batch.append({"deleteEmbeddedObject": {"objectId": ch["chartId"]}})

    # 5.2 合并研判简报单元格 A2:I2 并开启自动换行与行高
    requests_batch.extend([
        {
            "unmergeCells": {
                "range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 9}
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 9},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": dash_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 155},
                "fields": "pixelSize"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 9},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.95, "green": 0.97, "blue": 1.0},
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"fontSize": 10, "bold": False}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,wrapStrategy,verticalAlignment,textFormat)"
            }
        },
        # 设置主标题行样式 (Row 1)
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 9},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.11, "green": 0.21, "blue": 0.36},
                        "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 12, "bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        }
    ])

    # 为三个模块的表头行设置深蓝底白字
    for hdr_row_1based in [5, bs_title_row + 1, cty_title_row + 1]:
        requests_batch.append({
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": hdr_row_1based - 1, "endRowIndex": hdr_row_1based, "startColumnIndex": 0, "endColumnIndex": 9},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.18, "green": 0.33, "blue": 0.55},
                        "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        })

    # 格式化模块一的历史分位(E列百分比)和Z-Score(F列两位小数)
    requests_batch.extend([
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 5, "endRowIndex": 17, "startColumnIndex": 4, "endColumnIndex": 5},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 5, "endRowIndex": 17, "startColumnIndex": 5, "endColumnIndex": 6},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "+0.00;-0.00;0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        # 格式化 01_跨资产表 涨跌幅为百分比
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
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.18, "green": 0.33, "blue": 0.55},
                        "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        }
    ])

    # 5.3 构建 6 幅原生 Google Sheets 可视化图表 (放置在驾驶舱右侧 K:V 列)
    def build_dual_axis_line_chart(title, left_codes, right_codes, anchor_row, anchor_col):
        series_list = []
        for c in left_codes:
            if c in ts_idx_map:
                ci = ts_idx_map[c]
                series_list.append({
                    "series": {"sourceRange": {"sources": [{"sheetId": ts_id, "startRowIndex": 0, "endRowIndex": ts_row_count, "startColumnIndex": ci, "endColumnIndex": ci + 1}]}},
                    "targetAxis": "LEFT_AXIS"
                })
        for c in right_codes:
            if c in ts_idx_map:
                ci = ts_idx_map[c]
                series_list.append({
                    "series": {"sourceRange": {"sources": [{"sheetId": ts_id, "startRowIndex": 0, "endRowIndex": ts_row_count, "startColumnIndex": ci, "endColumnIndex": ci + 1}]}},
                    "targetAxis": "RIGHT_AXIS"
                })
        if not series_list:
            return None
        return {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": title,
                        "basicChart": {
                            "chartType": "LINE",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "日期"},
                                {"position": "LEFT_AXIS", "title": "左轴指标"},
                                {"position": "RIGHT_AXIS", "title": "右轴指标"},
                            ],
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": ts_id, "startRowIndex": 0, "endRowIndex": ts_row_count, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                            "series": series_list,
                            "headerCount": 1,
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": dash_id, "rowIndex": anchor_row, "columnIndex": anchor_col},
                            "offsetXPixels": 10, "offsetYPixels": 5,
                            "widthPixels": 530, "heightPixels": 305
                        }
                    }
                }
            }
        }

    charts_to_add = [
        # 图1 (K1): 美债10Y名义利率 & 10Y TIPS实际利率 vs 黄金价格
        build_dual_axis_line_chart("图1：美债10Y名义/TIPS实际利率(左) vs 黄金(右)", ["DGS10", "DFII10"], ["GOLD"], 0, 10),
        # 图2 (Q1): 美联储净流动性 vs 标普500 & 比特币
        build_dual_axis_line_chart("图2：美联储净流动性(左) vs 标普500(右)", ["NET_LIQUIDITY"], ["SPX"], 0, 16),
        # 图3 (K17): ICE美元指数 vs 美元/日元
        build_dual_axis_line_chart("图3：美元指数 DXY(左) vs 美元/日元 USDJPY(右)", ["DXY"], ["USDJPY"], 16, 10),
        # 图4 (Q17): 美债10Y-2Y利差 vs 铜金比
        build_dual_axis_line_chart("图4：美债10Y-2Y利差(左) vs 铜金比(右)", ["SPREAD_10Y2Y"], ["COPPER_GOLD_RATIO"], 16, 16),
        # 图5 (K33): 多国横向周期对比柱状图 (GDP增速 vs CPI通胀 vs 10Y国债)
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "图5：主要经济体 GDP增速 / CPI通胀 / 10Y国债 对比",
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [{"position": "BOTTOM_AXIS", "title": "经济体"}, {"position": "LEFT_AXIS", "title": "百分比 (%)"}],
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": cty_title_row, "endRowIndex": cty_data_end, "startColumnIndex": 1, "endColumnIndex": 2}]}}}],
                            "series": [
                                {"series": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": cty_title_row, "endRowIndex": cty_data_end, "startColumnIndex": 2, "endColumnIndex": 3}]}}, "targetAxis": "LEFT_AXIS"},
                                {"series": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": cty_title_row, "endRowIndex": cty_data_end, "startColumnIndex": 3, "endColumnIndex": 4}]}}, "targetAxis": "LEFT_AXIS"},
                                {"series": {"sourceRange": {"sources": [{"sheetId": dash_id, "startRowIndex": cty_title_row, "endRowIndex": cty_data_end, "startColumnIndex": 4, "endColumnIndex": 5}]}}, "targetAxis": "LEFT_AXIS"},
                            ],
                            "headerCount": 1
                        }
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": dash_id, "rowIndex": 32, "columnIndex": 10},
                            "offsetXPixels": 10, "offsetYPixels": 5,
                            "widthPixels": 530, "heightPixels": 305
                        }
                    }
                }
            }
        },
        # 图6 (Q33): 全球跨资产 5日 vs 20日涨跌幅对比
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "图6：全球跨资产 5日 vs 20日涨跌幅轮动对比",
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
                            "anchorCell": {"sheetId": dash_id, "rowIndex": 32, "columnIndex": 16},
                            "offsetXPixels": 10, "offsetYPixels": 5,
                            "widthPixels": 530, "heightPixels": 305
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
    print(f"[SUCCESS] Consolidated into 4 tabs, injected live formulas & 6 native charts ({len(all_raw)} metrics).")
