import io
import json
import os
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone

import gspread
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ==========================================
# 1. 环境与密钥初始化
# ==========================================
FRED_API_KEY = os.environ["FRED_API_KEY"].strip()
GCP_SA_KEY = os.environ["GCP_SA_KEY"].strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"].strip()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
sa_info = json.loads(GCP_SA_KEY)
creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
gc = gspread.authorize(creds)
sheets_service = build("sheets", "v4", credentials=creds)

DASH_SHEET_NAME = "📊 宏观与市场看板"
RAW_SHEET_NAME = "📁 底层数据明细"


# ==========================================
# 2. 真实数据抓取与资金流计算函数
# ==========================================
def fetch_fred(series_id: str, start_date: str = "2021-01-01") -> pd.Series:
    """从 St. Louis Fed (FRED) 拉取官方时间序列"""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    records = []
    for o in obs:
        if o["value"] != ".":
            records.append((pd.to_datetime(o["date"]), float(o["value"])))
    if not records:
        return pd.Series(dtype=float, name=series_id)
    s = pd.Series(
        [v for _, v in records],
        index=pd.DatetimeIndex([d for d, _ in records]),
        name=series_id,
    )
    return s[~s.index.duplicated(keep="last")].sort_index()


def yoy_pct(s: pd.Series, periods: int = 12) -> pd.Series:
    """计算同比变化百分比 (%)"""
    return (s.pct_change(periods=periods) * 100).dropna()


def fetch_yf_ohlcv(tickers: list, period: str = "4y") -> dict:
    """批量拉取 Yahoo Finance 完整 OHLCV 数据"""
    raw = yf.download(tickers, period=period, auto_adjust=True, progress=False)
    data = {}
    for field in ["Open", "High", "Low", "Close", "Volume"]:
        df = raw[field].copy()
        if isinstance(df, pd.Series):
            df = df.to_frame(name=tickers[0])
        df.index = pd.to_datetime(df.index).tz_localize(None)
        data[field] = df.sort_index()
    return data


def compute_rolling_dollar_flow(ohlcv: dict, ticker_labels: dict, window: int = 20) -> pd.DataFrame:
    """
    基于机构级 Chaikin Money Flow Dollar Volume 计算滚动资金净流入/流出 (单位: 亿美元)
    公式: Daily Flow = [((Close - Low) - (High - Close)) / (High - Low)] * Volume * Close
    """
    high = ohlcv["High"]
    low = ohlcv["Low"]
    close = ohlcv["Close"]
    vol = ohlcv["Volume"]

    flow_df = pd.DataFrame(index=close.index)
    for ticker, label in ticker_labels.items():
        if ticker not in close.columns:
            continue
        hl_range = (high[ticker] - low[ticker]).replace(0, np.nan)
        clv = ((close[ticker] - low[ticker]) - (high[ticker] - close[ticker])) / hl_range
        clv = clv.fillna(0.0)
        # 每日资金净流向（亿美元）
        daily_flow_100m = (clv * vol[ticker] * close[ticker]) / 1e8
        # 20个交易日（约1个月）滚动累计净流向
        flow_df[label] = daily_flow_100m.rolling(window=window, min_periods=10).sum()

    return flow_df.iloc[-250:].resample("W-FRI").last().dropna(how="all")


def fetch_aaii_sentiment() -> pd.DataFrame:
    """抓取 AAII 散户情绪调查 (看涨% - 看跌%)"""
    try:
        url = "https://www.aaii.com/files/surveys/sentiment.xls"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        raw = pd.read_excel(io.BytesIO(r.content))
        raw.columns = [str(c).strip().lower() for c in raw.columns]
        date_col = [c for c in raw.columns if "date" in c][0]
        bull_col = [c for c in raw.columns if "bull" in c][0]
        bear_col = [c for c in raw.columns if "bear" in c][0]
        df = raw[[date_col, bull_col, bear_col]].copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna().set_index(date_col).sort_index()
        if df[bull_col].max() <= 1.5:
            df[bull_col] *= 100
            df[bear_col] *= 100
        spread = (df[bull_col] - df[bear_col]).rename("散户净看涨差值(%)")
        ma20 = spread.rolling(20).mean().rename("20周均线(%)")
        return pd.concat([spread, ma20], axis=1).dropna()
    except Exception as e:
        print(f"[Warning] AAII 数据抓取异常: {e}")
        return pd.DataFrame()


def fetch_naaim_exposure() -> pd.DataFrame:
    """抓取 NAAIM 机构投资经理风险敞口 (指定 bs4 解析器消除警告)"""
    try:
        page = requests.get(
            "https://www.naaim.org/programs/naaim-exposure-index/",
            headers=HEADERS,
            timeout=30,
        )
        page.raise_for_status()
        tables = pd.read_html(io.StringIO(page.text), flavor="bs4")
        df = tables[0].copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        date_col = df.columns[0]
        val_col = [c for c in df.columns if "mean" in c or "number" in c or "exposure" in c][0]
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        df = df.dropna().set_index(date_col).sort_index()
        exp = df[val_col].rename("机构风险敞口(%)")
        ma20 = exp.rolling(20).mean().rename("20周均线(%)")
        return pd.concat([exp, ma20], axis=1).dropna()
    except Exception as e:
        print(f"[Warning] NAAIM 数据抓取异常: {e}")
        return pd.DataFrame()


def fetch_cboe_put_call() -> pd.DataFrame:
    """抓取 CBOE 股票看跌/看涨期权成交量比值 (正则精准过滤顶部免责声明)"""
    try:
        url = "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/equitypc.csv"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        valid_lines = []
        for ln in r.text.splitlines():
            s = ln.strip()
            if s.upper().startswith("DATE,") or re.match(r"^\d{1,2}/\d{1,2}/\d{4},", s):
                valid_lines.append(s)
        df = pd.read_csv(io.StringIO("\n".join(valid_lines)))
        df.columns = [str(c).strip().upper() for c in df.columns]
        date_col = df.columns[0]
        ratio_col = [c for c in df.columns if "RATIO" in c or "P/C" in c][-1]
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[ratio_col] = pd.to_numeric(df[ratio_col], errors="coerce")
        df = df.dropna().set_index(date_col).sort_index()
        pcr = df[ratio_col].rename("Put/Call比值")
        ma20 = pcr.rolling(20).mean().rename("20日均线")
        return pd.concat([pcr, ma20], axis=1).dropna()
    except Exception as e:
        print(f"[Warning] CBOE Put/Call 抓取异常: {e}")
        return pd.DataFrame()


def fetch_cftc_cot() -> pd.DataFrame:
    """抓取 CFTC 标普500 与 纳斯达克100 投机者(非商业)净持仓"""
    try:
        year = datetime.now().year
        frames = []
        for y in [year - 2, year - 1, year]:
            url = f"https://www.cftc.gov/files/dea/history/deacot{y}.zip"
            r = requests.get(url, headers=HEADERS, timeout=45)
            if r.status_code != 200:
                continue
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                fname = zf.namelist()[0]
                with zf.open(fname) as f:
                    df_y = pd.read_csv(f, low_memory=False)
                    frames.append(df_y)
        if not frames:
            return pd.DataFrame()
        cot = pd.concat(frames, ignore_index=True)
        name_col = [c for c in cot.columns if "Market and Exchange Names" in c][0]
        date_col = [c for c in cot.columns if "As of Date in Form YYYY-MM-DD" in c][0]
        long_col = [c for c in cot.columns if "Noncommercial Positions-Long (All)" in c][0]
        short_col = [c for c in cot.columns if "Noncommercial Positions-Short (All)" in c][0]

        cot[date_col] = pd.to_datetime(cot[date_col])
        sp = cot[cot[name_col].str.contains("E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE", na=False)].copy()
        nq = cot[cot[name_col].str.contains("NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE", na=False)].copy()

        sp_net = (
            sp.set_index(date_col)
            .apply(lambda x: float(x[long_col]) - float(x[short_col]), axis=1)
            .sort_index()
            .rename("标普500投机净持仓(手)")
        )
        nq_net = (
            nq.set_index(date_col)
            .apply(lambda x: float(x[long_col]) - float(x[short_col]), axis=1)
            .sort_index()
            .rename("纳指100投机净持仓(手)")
        )
        return pd.concat([sp_net, nq_net], axis=1)
    except Exception as e:
        print(f"[Warning] CFTC COT 抓取异常: {e}")
        return pd.DataFrame()


def fetch_sp500_breadth() -> pd.DataFrame:
    """实时计算标普500成分股高于50日/200日均线的市场宽度比例"""
    try:
        r = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=HEADERS, timeout=30)
        sp_table = pd.read_html(io.StringIO(r.text), flavor="bs4")[0]
        tickers = [t.replace(".", "-") for t in sp_table["Symbol"].tolist()]
        prices = yf.download(tickers, period="2y", auto_adjust=True, progress=False)["Close"]
        prices.index = pd.to_datetime(prices.index).tz_localize(None)
        ma50 = prices.rolling(50).mean()
        ma200 = prices.rolling(200).mean()
        valid_50 = ma50.notna().sum(axis=1)
        valid_200 = ma200.notna().sum(axis=1)
        b50 = ((prices > ma50).sum(axis=1) / valid_50 * 100).rename("50日市场宽度(%)")
        b200 = ((prices > ma200).sum(axis=1) / valid_200 * 100).rename("200日市场宽度(%)")
        df = pd.concat([b50, b200], axis=1).dropna()
        return df.iloc[-300:]
    except Exception as e:
        print(f"[Warning] 市场宽度计算异常: {e}")
        return pd.DataFrame()


# ==========================================
# 3. 组装全部 22 张图表的数据与读图指南
# ==========================================
def build_all_charts() -> list:
    print(">>> 正在拉取市场行情、商品期货、AI板块与全球大类资产 ETF 数据...")
    market_tickers = [
        "^GSPC", "^NDX", "HG=F", "GC=F", "CL=F", "^VIX",
        # 全市场与11大经济板块
        "VTI", "XLK", "XLF", "XLV", "XLY", "XLP", "XLE", "XLI", "XLB", "XLU", "XLRE", "XLC",
        # AI 板块与四大宽基指数
        "SMH", "AIQ", "BOTZ", "SPY", "QQQ", "IWM", "DIA",
        # 全球核心大类资产旗舰 ETF (美股、新兴市场、美债、黄金、商品、比特币)
        "EEM", "TLT", "GLD", "DBC", "IBIT",
    ]
    ohlcv = fetch_yf_ohlcv(market_tickers, period="4y")
    mkt = ohlcv["Close"].ffill()

    spx = mkt["^GSPC"].rename("标普500指数")
    ndx = mkt["^NDX"].rename("纳斯达克100指数")
    copper_gold = (mkt["HG=F"] / mkt["GC=F"] * 1000).rename("铜金比(×1000)")
    oil_gold = (mkt["CL=F"] / mkt["GC=F"] * 100).rename("油金比(×100)")

    print(">>> 正在拉取情绪、期权、CFTC持仓与市场宽度...")
    aaii_df = fetch_aaii_sentiment()
    naaim_df = fetch_naaim_exposure()
    pcr_df = fetch_cboe_put_call()
    cot_df = fetch_cftc_cot()
    breadth_df = fetch_sp500_breadth()

    print(">>> 正在拉取 FRED 宏观与利率数据...")
    # 消费端
    icsa = fetch_fred("ICSA").rename("初请失业金(人)")
    ccsa = fetch_fred("CCSA").rename("续请失业金(人)")
    payems_yoy = yoy_pct(fetch_fred("PAYEMS")).rename("非农就业同比(%)")

    pi_yoy = yoy_pct(fetch_fred("PI")).rename("个人收入同比(%)")
    pce_yoy = yoy_pct(fetch_fred("PCE")).rename("个人消费支出同比(%)")
    psavert = fetch_fred("PSAVERT").rename("个人储蓄率(%)")

    exhos_yoy = yoy_pct(fetch_fred("EXHOSLUSM495S")).rename("成屋销售同比(%)")
    hsn1f_yoy = yoy_pct(fetch_fred("HSN1F")).rename("新屋销售同比(%)")
    cs20_yoy = yoy_pct(fetch_fred("SPCS20RSA")).rename("20大都市房价同比(%)")
    med_new_yoy = yoy_pct(fetch_fred("MSPNHSUS")).rename("新房价格中位数同比(%)")

    veh_sales = fetch_fred("TOTALSA").rename("全美汽车销量(百万辆)")
    veh_yoy = yoy_pct(fetch_fred("TOTALSA")).rename("汽车销量同比(%)")
    used_car = fetch_fred("CUSR0000SETA02").rename("二手车价格指数")

    retail_yoy = yoy_pct(fetch_fred("RSAFS")).rename("总体零售同比(%)")
    ex_auto_yoy = yoy_pct(fetch_fred("RSXFS")).rename("核心连锁零售同比(%)")
    ecom_yoy = yoy_pct(fetch_fred("MRTSSM4541USS")).rename("电商非实体零售同比(%)")

    # 供给端
    permit_yoy = yoy_pct(fetch_fred("PERMIT")).rename("建房许可同比(%)")
    houst_yoy = yoy_pct(fetch_fred("HOUST")).rename("新屋开工同比(%)")
    house_inv_yoy = yoy_pct(fetch_fred("HNFSEPUSSA")).rename("新房库存同比(%)")

    dur_yoy = yoy_pct(fetch_fred("DGORDER")).rename("耐用品订单同比(%)")
    core_cap_yoy = yoy_pct(fetch_fred("NEWORDER")).rename("核心资本品订单同比(%)")
    mfg_pmi = fetch_fred("BSCICP03USM665S").rename("制造业景气PMI指数")

    wei = fetch_fred("WEI").rename("纽约联储WEI周度经济指数")
    ppi_yoy = yoy_pct(fetch_fred("PPIACO")).rename("PPI出厂价格同比(%)")
    cpi_yoy = yoy_pct(fetch_fred("CPIAUCSL")).rename("CPI通胀同比(%)")

    # 利率端
    fed_rate = fetch_fred("DFF").rename("美联储政策利率(%)")
    dgs10 = fetch_fred("DGS10").rename("10年期美债收益率(%)")
    dgs5 = fetch_fred("DGS5").rename("5年期美债收益率(%)")
    dgs2 = fetch_fred("DGS2").rename("2年期美债收益率(%)")
    dgs3m = fetch_fred("DGS3MO").rename("3个月期美债利率(%)")

    ccc_yield = fetch_fred("BAMLH0A3HYEY")
    real_10y = fetch_fred("DFII10")
    credit_spread = (ccc_yield - real_10y).dropna().rename("CCC级信用利差(%)")
    vix = mkt["^VIX"].rename("VIX恐慌指数")

    def align_tail(series_list, freq="W-FRI", n=156):
        df = pd.concat(series_list, axis=1).sort_index().ffill().dropna()
        if freq:
            df = df.resample(freq).last().dropna()
        return df.iloc[-n:]

    # 图 20：完整 11 个经济部门 ETF 相对全市场 VTI 强弱 (起点=100)
    sector_map = {
        "XLK": "科技XLK",
        "XLF": "金融XLF",
        "XLV": "医疗XLV(防御)",
        "XLY": "可选消费XLY",
        "XLP": "必需消费XLP(防御)",
        "XLE": "能源XLE",
        "XLI": "工业XLI",
        "XLB": "原材料XLB",
        "XLU": "公用事业XLU(防御)",
        "XLRE": "房地产XLRE",
        "XLC": "通讯XLC",
    }
    sec_df = pd.DataFrame(index=mkt.index)
    for code, label in sector_map.items():
        rel = (mkt[code] / mkt["VTI"]).dropna().iloc[-260:]
        sec_df[label] = (rel / rel.iloc[0]) * 100
    sec_df = sec_df.resample("W-FRI").last().dropna()

    # 图 21：AI 板块对比标普500及三大股指相对强弱比 (起点=100)
    ai_ratio_pairs = [
        ("SMH", "SPY", "AI芯片SMH / 标普SPY"),
        ("AIQ", "SPY", "AI应用AIQ / 标普SPY"),
        ("BOTZ", "SPY", "AI机器人BOTZ / 标普SPY"),
        ("SMH", "QQQ", "AI芯片SMH / 纳指QQQ"),
        ("SMH", "IWM", "AI芯片SMH / 罗素小盘IWM"),
        ("SMH", "DIA", "AI芯片SMH / 道指DIA"),
    ]
    ai_df = pd.DataFrame(index=mkt.index)
    for num, den, label in ai_ratio_pairs:
        rel = (mkt[num] / mkt[den]).dropna().iloc[-260:]
        ai_df[label] = (rel / rel.iloc[0]) * 100
    ai_df = ai_df.resample("W-FRI").last().dropna()

    # 图 22：全球六大核心资产 20日滚动资金净流入/流出 (亿美元)
    global_flow_tickers = {
        "SPY": "美股大盘(SPY)",
        "QQQ": "美股科技(QQQ)",
        "EEM": "新兴市场(EEM)",
        "TLT": "美债长债(TLT)",
        "GLD": "实物黄金(GLD)",
        "DBC": "大宗商品(DBC)",
        "IBIT": "比特币ETF(IBIT)",
    }
    global_flow_df = compute_rolling_dollar_flow(ohlcv, global_flow_tickers, window=20)

    charts = [
        # --- 一、消费端 ---
        {
            "id": "01",
            "category": "消费",
            "title": "01. 就业市场 (Employment)",
            "df": align_tail([icsa, ccsa, payems_yoy], freq="W-FRI", n=156),
            "right_cols": ["非农就业同比(%)"],
            "left_title": "申领人数 (人)",
            "right_title": "非农同比 (%)",
            "what": "• 蓝线：初次申领失业金人数（每周新增失业）\n• 红线：连续申领失业金人数（持续失业规模）\n• 黄线(右轴)：非农就业人数年度同比变化(%)",
            "how": "美国70%GDP靠消费，就业是消费的源头。失业金曲线低位走平、非农同比稳健说明居民有持续收入；若初请/续请连续飙升且非农同比快速下滑，说明劳动力市场恶化。",
            "signal": "⚠️ 预警信号：初请失业金持续突破 30万人 且续请同步抬头，往往预示消费即将降温。",
        },
        {
            "id": "02",
            "category": "消费",
            "title": "02. 个人财务状况 (Personal Finances)",
            "df": align_tail([pi_yoy, pce_yoy, psavert], freq="ME", n=48),
            "right_cols": [],
            "left_title": "百分比 (%)",
            "right_title": "",
            "what": "• 蓝线：个人总收入同比变化(%)\n• 红线：个人消费支出(PCE)同比变化(%)\n• 黄线：个人储蓄率(%)",
            "how": "三者互为因果。健康状态是「收入增速 ≥ 消费增速」，储蓄率维持在 5%~8% 区间；若消费支出远高于收入增速，说明居民在透支超额储蓄或依赖信用卡负债。",
            "signal": "⚠️ 预警信号：个人储蓄率跌破 3.5% 且个人收入同比低于通胀，消费动能将面临断崖风险。",
        },
        {
            "id": "03",
            "category": "消费",
            "title": "03. 房屋销售与房价 (Home Sales)",
            "df": align_tail([exhos_yoy, hsn1f_yoy, cs20_yoy, med_new_yoy], freq="ME", n=48),
            "right_cols": [],
            "left_title": "同比变化 (%)",
            "right_title": "",
            "what": "• 蓝线：成屋销售同比(%)  • 红线：新屋销售同比(%)\n• 黄线：20大都市房价指数同比(%)\n• 绿线：新房销售中位价同比(%)",
            "how": "房子是美国家庭最大宗消费，牵动家电、装修、金融等全产业链。成交量（蓝/红线）通常领先房价（黄/绿线）3~6个月见顶或见底。",
            "signal": "💡 观察要点：量在价先——当成屋/新屋销量同比由负转正并持续回升时，房价随后止跌回暖。",
        },
        {
            "id": "04",
            "category": "消费",
            "title": "04. 汽车消费与二手车价 (Auto Sales)",
            "df": align_tail([veh_sales, veh_yoy, used_car], freq="ME", n=48),
            "right_cols": ["二手车价格指数"],
            "left_title": "销量(百万辆) / 同比(%)",
            "right_title": "二手车指数",
            "what": "• 蓝线：全美汽车月度折年销量(百万辆)\n• 红线：汽车销量同比增速(%)\n• 黄线(右轴)：二手车价格指数",
            "how": "汽车是第二大耐用消费品。供应链正常时，销量稳定在 1500~1700 万辆区间；若二手车价格指数飙升（如缺芯期），会推高核心CPI并迫使消费者观望。",
            "signal": "💡 观察要点：二手车价格回落 + 汽车销量同比转正，代表大件消费恢复且商品通胀缓和。",
        },
        {
            "id": "05",
            "category": "消费",
            "title": "05. 零售与电商结构 (Retail Sales)",
            "df": align_tail([retail_yoy, ex_auto_yoy, ecom_yoy], freq="ME", n=48),
            "right_cols": [],
            "left_title": "同比增速 (%)",
            "right_title": "",
            "what": "• 蓝线：美国总体零售销售额同比(%)\n• 红线：核心连锁零售(剔除汽车)同比(%)\n• 黄线：非实体店(亚马逊等电商网购)同比(%)",
            "how": "直接反映美国居民日常消费热度。对比总体零售与电商增速，可判断消费习惯向线上迁移的速度以及实体连锁商超的景气度。",
            "signal": "⚠️ 预警信号：总体零售同比跌破 0% 负增长，通常对应经济步入实质性衰退。",
        },
        # --- 二、供给端 ---
        {
            "id": "06",
            "category": "供给",
            "title": "06. 房屋营建与库存 (Housing Supply)",
            "df": align_tail([permit_yoy, houst_yoy, house_inv_yoy, cs20_yoy], freq="ME", n=48),
            "right_cols": [],
            "left_title": "同比变化 (%)",
            "right_title": "",
            "what": "• 蓝线：建房许可同比(%)  • 红线：新屋开工同比(%)\n• 黄线：新房库存同比(%)  • 绿线：20大都市房价同比(%)",
            "how": "建房许可领先新屋开工，开工后约 6~12 个月转化为实际新房供应。最核心看黄线「房屋库存」：库存下降代表供不应求推高房价；库存飙升代表供大于求压制房价。",
            "signal": "💡 领先规律：建房许可增速拐点领先未来半年新屋供应；库存低位 + 许可回升利好地产链。",
        },
        {
            "id": "07",
            "category": "供给",
            "title": "07. 耐用品订单与制造业PMI (Supply Orders)",
            "df": align_tail([dur_yoy, core_cap_yoy, mfg_pmi], freq="ME", n=48),
            "right_cols": ["制造业景气PMI指数"],
            "left_title": "订单同比 (%)",
            "right_title": "景气指数 (100为荣枯线)",
            "what": "• 蓝线：耐用品新增订单同比(%)\n• 红线：核心资本品订单(剔除国防航空)同比(%)\n• 黄线(右轴)：制造业景气PMI指数(100为荣枯分界)",
            "how": "工厂接单后需 3~6 个月采购备货与生产。订单与PMI下行预示未来半年社会总供给收缩；若此时消费端强劲，就会引发供需缺口与通胀。",
            "signal": "⚠️ 信号提示：核心资本品订单持续负增长 + PMI跌破100荣枯线，反映企业削减资本开支。",
        },
        {
            "id": "08",
            "category": "供给",
            "title": "08. 纽约联储每周经济指数 (WEI Index)",
            "df": align_tail([wei], freq="W-FRI", n=156),
            "right_cols": [],
            "left_title": "WEI 年化GDP增速映射 (%)",
            "right_title": "",
            "what": "• 蓝线：纽约联储每周经济指数 (Weekly Economic Index)",
            "how": "汇总消费、工业与政府部门的10项高频周度指标，是美国季度 GDP 的实时先行风向标，比月度宏观数据更灵敏。",
            "signal": "🚨 衰退红线：WEI 跌入 0 以下负值区（尤其是连续数周处于负值），大概率预示经济步入衰退。",
        },
        {
            "id": "09",
            "category": "供给",
            "title": "09. 铜金比与PPI/PMI (Industrial & PPI)",
            "df": align_tail([copper_gold, ppi_yoy, mfg_pmi], freq="W-FRI", n=156),
            "right_cols": ["PPI出厂价格同比(%)", "制造业景气PMI指数"],
            "left_title": "铜金比 (×1000)",
            "right_title": "PPI同比(%) / PMI指数",
            "what": "• 蓝线：铜金比（铜期货 ÷ 黄金期货 ×1000）\n• 红线(右轴)：工业品出厂价格指数 PPI 同比(%)\n• 黄线(右轴)：制造业景气 PMI 指数",
            "how": "铜代表全球工业需求，黄金代表避险情绪。铜金比衡量「隐含风险溢价的工业景气度」，是 PPI 与制造业 PMI 的高频先行指标。",
            "signal": "💡 领先规律：铜金比持续走强，预示未来几个月工业景气回升、PPI 通胀压力抬头。",
        },
        {
            "id": "10",
            "category": "供给",
            "title": "10. 油金比与CPI (Energy & CPI)",
            "df": align_tail([oil_gold, cpi_yoy], freq="W-FRI", n=156),
            "right_cols": ["CPI通胀同比(%)"],
            "left_title": "油金比 (×100)",
            "right_title": "CPI同比 (%)",
            "what": "• 蓝线：油金比（WTI原油期货 ÷ 黄金期货 ×100）\n• 红线(右轴)：消费者物价指数 CPI 同比(%)",
            "how": "原油是百业之母与社会基础运营成本，油金比剔除货币避险扰动后，领先反映终端消费物价 CPI 的涨跌方向。",
            "signal": "💡 领先规律：油金比见顶回落通常领先 CPI 通胀见顶回落；油金比飙升警惕二次通胀。",
        },
        {
            "id": "11",
            "category": "供给",
            "title": "11. 抽象化的宏观经济 (Macro Consensus)",
            "df": align_tail([copper_gold, oil_gold, dgs10], freq="W-FRI", n=156),
            "right_cols": ["10年期美债收益率(%)"],
            "left_title": "铜金比 / 油金比",
            "right_title": "10年美债收益率 (%)",
            "what": "• 蓝线：铜金比  • 红线：油金比\n• 黄线(右轴)：美国10年期国债收益率(%)",
            "how": "铜、油、黄金与美债每日23小时全球交易，凝聚了全球大资金对经济、通胀与货币政策的实时共识，可过滤媒体杂音。",
            "signal": "💡 观察要点：三者同步上行＝经济扩张/再通胀交易；三者同步下行＝衰退/降息避险交易。",
        },
        # --- 三、利率端 ---
        {
            "id": "12",
            "category": "利率",
            "title": "12. 美联储利率与美债收益率曲线 (Rates)",
            "df": align_tail([fed_rate, dgs10, dgs5, dgs2, dgs3m], freq="W-FRI", n=156),
            "right_cols": [],
            "left_title": "年化利率 (%)",
            "right_title": "",
            "what": "• 蓝线：美联储政策利率  • 红线：10年期美债\n• 黄线：5年期美债  • 绿线：2年期美债  • 紫线：3个月美债",
            "how": "利率是资本成本与资产定价之锚。市场利率（2Y/10Y）往往领先引导美联储政策利率；短端利率高于长端利率称为「利率倒挂」。",
            "signal": "🚨 宏观警报：3个月/2年期利率高于10年期（倒挂）代表经济异常紧缩；倒挂结束「快速变陡」时风险最高。",
        },
        {
            "id": "13",
            "category": "利率",
            "title": "13. 信用利差与系统性风险 (Credit Spread)",
            "df": align_tail([credit_spread, vix, spx], freq="W-FRI", n=156),
            "right_cols": ["标普500指数"],
            "left_title": "信用利差(%) / VIX",
            "right_title": "标普500点位",
            "what": "• 蓝线：CCC级高收益债与10年实际美债信用利差(%)\n• 红线：VIX恐慌指数\n• 黄线(右轴)：标普500指数",
            "how": "评估企业违约与金融系统性风险。当经济恶化、垃圾债违约风险上升时，信用利差会与 VIX 同步飙升，美股往往承压暴跌。",
            "signal": "🚨 危机信号：信用利差急剧扩大且 VIX 突破 30，说明流动性收紧，需高度防范系统性下跌。",
        },
        # --- 四、市场端 ---
        {
            "id": "14",
            "category": "市场",
            "title": "14. 散户投资者情绪 (AAII Sentiment)",
            "df": align_tail([aaii_df["散户净看涨差值(%)"], aaii_df["20周均线(%)"], spx], freq="W-FRI", n=156)
            if not aaii_df.empty
            else align_tail([spx], freq="W-FRI", n=156),
            "right_cols": ["标普500指数"],
            "left_title": "看涨减看跌差值 (%)",
            "right_title": "标普500点位",
            "what": "• 蓝线：AAII散户看涨比例减看跌比例差值(%)\n• 红线：该情绪差值的20周移动平均线\n• 黄线(右轴)：标普500指数",
            "how": "反向指标！散户往往在顶部极度乐观、在底部极度恐慌。观察蓝线触及历史极端正负值时的反向拐点。",
            "signal": "🎯 逆向阈值：差值 > +25% 提示情绪过热阶段性见顶；差值 < -25% 提示极度悲观阶段性见底。",
        },
        {
            "id": "15",
            "category": "市场",
            "title": "15. 机构投资者情绪 (NAAIM Exposure)",
            "df": align_tail([naaim_df["机构风险敞口(%)"], naaim_df["20周均线(%)"], spx], freq="W-FRI", n=156)
            if not naaim_df.empty
            else align_tail([spx], freq="W-FRI", n=156),
            "right_cols": ["标普500指数"],
            "left_title": "机构股票仓位敞口 (%)",
            "right_title": "标普500点位",
            "what": "• 蓝线：NAAIM活跃投资经理平均股票仓位敞口(%)\n• 红线：敞口20周均线  • 黄线(右轴)：标普500指数",
            "how": "机构同样存在追涨杀跌行为！当机构把仓位加满（接近100%甚至加杠杆）时买盘枯竭；当机构恐慌砍仓至低位时卖盘出清。",
            "signal": "🎯 逆向阈值：机构敞口 ≥ 95%~100% 警惕短期回调；敞口 ≤ 40% 往往对应波段买点。",
        },
        {
            "id": "16",
            "category": "市场",
            "title": "16. 期权市场交易方向 (Equity Put/Call)",
            "df": align_tail([pcr_df["Put/Call比值"], pcr_df["20日均线"], spx], freq="D", n=250)
            if not pcr_df.empty
            else align_tail([spx], freq="D", n=250),
            "right_cols": ["标普500指数"],
            "left_title": "看跌/看涨成交量比值",
            "right_title": "标普500点位",
            "what": "• 蓝线：CBOE股票看跌期权(Put)÷看涨期权(Call)成交比\n• 红线：20日移动平均线  • 黄线(右轴)：标普500指数",
            "how": "真金白银的日度交易情绪。比值越高，说明市场买入看跌期权对冲/做空的恐慌情绪越浓。",
            "signal": "🎯 逆向阈值：Put/Call 比值 > 1.0（尤其20日均线显著抬升）往往对应市场阶段性底部。",
        },
        {
            "id": "17",
            "category": "市场",
            "title": "17. S&P 500 投机者持仓 (CFTC COT)",
            "df": align_tail([cot_df["标普500投机净持仓(手)"], spx], freq="W-FRI", n=156)
            if not cot_df.empty and "标普500投机净持仓(手)" in cot_df
            else align_tail([spx], freq="W-FRI", n=156),
            "right_cols": ["标普500指数"],
            "left_title": "非商业净持仓 (手)",
            "right_title": "标普500点位",
            "what": "• 蓝线：CFTC E-Mini标普500期货大型投机者净多空持仓\n• 红线(右轴)：标普500指数",
            "how": "正值代表总体净做多，负值代表总体净做空（每周五公布上周二数据）。重点观察极端净空头仓位下的「轧空 (Short Squeeze)」潜力。",
            "signal": "🎯 轧空提示：当净空头持仓处于历史极低负值区而股指拒跌反弹时，空头平仓止损会引发剧烈上涨。",
        },
        {
            "id": "18",
            "category": "市场",
            "title": "18. Nasdaq 100 投机者持仓 (CFTC COT)",
            "df": align_tail([cot_df["纳指100投机净持仓(手)"], ndx], freq="W-FRI", n=156)
            if not cot_df.empty and "纳指100投机净持仓(手)" in cot_df
            else align_tail([ndx], freq="W-FRI", n=156),
            "right_cols": ["纳斯达克100指数"],
            "left_title": "非商业净持仓 (手)",
            "right_title": "纳斯达克100点位",
            "what": "• 蓝线：CFTC 纳斯达克100期货大型投机者净多空持仓\n• 红线(右轴)：纳斯达克100指数",
            "how": "对比图17，观察大型机构对「大型科技成长股」与「全市场大盘」的态度分化。",
            "signal": "💡 观察要点：若标普净空而纳指净多，说明资金抱团AI与科技龙头；若纳指多头极度拥挤则防科技股获利回吐。",
        },
        {
            "id": "19",
            "category": "市场",
            "title": "19. S&P 500 市场宽度 (Market Breadth)",
            "df": align_tail([breadth_df["50日市场宽度(%)"], breadth_df["200日市场宽度(%)"], spx], freq="D", n=250)
            if not breadth_df.empty
            else align_tail([spx], freq="D", n=250),
            "right_cols": ["标普500指数"],
            "left_title": "站上均线股票占比 (%)",
            "right_title": "标普500点位",
            "what": "• 蓝线：标普500成分股股价高于50日均线的比例(%)\n• 红线：股价高于200日均线的比例(%)\n• 黄线(右轴)：标普500指数",
            "how": "最直观的「贪婪与恐惧」实战量化器，识别指数虚涨（少数巨头拉升、多数个股下跌的顶背离）或普跌超卖。",
            "signal": "🎯 买卖极区：50日宽度 > 85% 提示短期顶部；< 15% 提示短期底部。若50日与200日宽度同时 >85% 或 <15%，常为中长线大拐点！",
        },
        {
            "id": "20",
            "category": "市场",
            "title": "20. 美股11大板块相对强度轮动 (11 Sectors)",
            "df": sec_df,
            "right_cols": [],
            "left
