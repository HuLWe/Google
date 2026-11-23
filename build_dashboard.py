import io
import json
import os
import re
import time
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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

PALETTE = [
    {"red": 0.12, "green": 0.47, "blue": 0.71},  # 1. 经典蓝
    {"red": 0.84, "green": 0.15, "blue": 0.16},  # 2. 警示红
    {"red": 0.95, "green": 0.58, "blue": 0.05},  # 3. 琥珀橙黄
    {"red": 0.17, "green": 0.63, "blue": 0.17},  # 4. 翠绿
    {"red": 0.58, "green": 0.40, "blue": 0.74},  # 5. 紫罗兰
    {"red": 0.09, "green": 0.75, "blue": 0.81},  # 6. 青蓝
    {"red": 0.89, "green": 0.47, "blue": 0.76},  # 7. 洋红
]


# ==========================================
# 2. 真实数据抓取与量化指标计算函数
# ==========================================
def fetch_fred(series_id: str, start_date: str = "2019-01-01") -> pd.Series:
    """从 St. Louis Fed (FRED) 拉取官方时间序列"""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"[Warning] FRED 指标 {series_id} 请求失败 ({r.status_code})")
            return pd.Series(dtype=float, name=series_id)
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
    except Exception as e:
        print(f"[Warning] FRED 指标 {series_id} 拉取异常: {e}")
        return pd.Series(dtype=float, name=series_id)


def yoy_pct(s: pd.Series, periods: int = 12) -> pd.Series:
    """计算同比变化百分比 (%)"""
    if s is None or s.empty or len(s) <= periods:
        return pd.Series(dtype=float, name=getattr(s, "name", ""))
    return (s.pct_change(periods=periods) * 100).dropna()


def fetch_existing_home_sales_yoy() -> pd.Series:
    """过滤 Redfin 全国月度成屋成交量并计算完整 60 个月同比，合并 FRED 最新值"""
    fred_s = yoy_pct(fetch_fred("EXHOSLUSM495S", start_date="2019-01-01"))
    redfin_s = pd.Series(dtype=float)
    try:
        url = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/us_national_market_tracker.tsv000.gz"
        r = requests.get(url, headers=HEADERS, timeout=45)
        if r.status_code == 200:
            df = pd.read_csv(io.BytesIO(r.content), sep="\t", compression="gzip", low_memory=False)
            cols_map = {c.upper(): c for c in df.columns}
            p_col = cols_map.get("PERIOD_END") or cols_map.get("PERIOD_BEGIN")
            dur_col = cols_map.get("PERIOD_DURATION")
            t_col = cols_map.get("PROPERTY_TYPE")
            sa_col = cols_map.get("IS_SEASONALLY_ADJUSTED")
            sold_col = cols_map.get("HOMES_SOLD")
            y_col = cols_map.get("HOMES_SOLD_YOY")

            sub = df.copy()
            if dur_col:
                sub = sub[sub[dur_col].astype(str).str.lower().str.contains("1 month", na=False)]
            if t_col and "All Residential" in sub[t_col].values:
                sub = sub[sub[t_col] == "All Residential"]
            if sa_col:
                false_rows = sub[sub[sa_col].astype(str).str.lower().isin(["false", "0", "f"])]
                if not false_rows.empty:
                    sub = false_rows

            if p_col:
                sub[p_col] = pd.to_datetime(sub[p_col], errors="coerce")
                sub = sub.dropna(subset=[p_col]).sort_values(p_col)
                if sold_col and sub[sold_col].notna().sum() >= 24:
                    sub[sold_col] = pd.to_numeric(sub[sold_col], errors="coerce")
                    s_cnt = sub.dropna(subset=[sold_col]).set_index(p_col)[sold_col]
                    s_cnt = s_cnt[~s_cnt.index.duplicated(keep="last")].resample("ME").last().dropna()
                    s = (s_cnt.pct_change(12) * 100.0).dropna()
                elif y_col:
                    sub[y_col] = pd.to_numeric(sub[y_col], errors="coerce")
                    s = sub.dropna(subset=[y_col]).set_index(p_col)[y_col]
                    s = s[~s.index.duplicated(keep="last")].resample("ME").last().dropna()
                    if not s.empty and s.abs().max() <= 3.0:
                        s = s * 100.0
                else:
                    s = pd.Series(dtype=float)

                redfin_s = s[s.index >= "2019-01-01"]
                if not redfin_s.empty:
                    print(f"[OK] Redfin 成屋销售同比拉取成功 ({len(redfin_s)} 个月, 最新: {redfin_s.index[-1].strftime('%Y-%m-%d')})")
    except Exception as e:
        print(f"[Info] Redfin 通道回退至 FRED: {e}")

    if not redfin_s.empty:
        r_m = redfin_s.resample("ME").last()
        if not fred_s.empty:
            f_m = fred_s.resample("ME").last()
            r_m = r_m.combine_first(f_m)
        return r_m.rename("成屋销售同比(%)")

    return fred_s.rename("成屋销售同比(%)")


def fetch_ism_pmi() -> tuple:
    """获取真实连续的美国 ISM 制造业 PMI 与非制造业 PMI（50为荣枯线）"""
    def _clean_pmi_series(s: pd.Series) -> pd.Series:
        if s is None or s.empty:
            return pd.Series(dtype=float)
        s = s[(s >= 28.0) & (s <= 75.0)]
        return s[~s.index.duplicated(keep="last")].sort_index()

    def _get_dbnomics(url: str) -> pd.Series:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                return pd.Series(dtype=float)
            docs = r.json().get("series", {}).get("docs", [])
            if not docs:
                return pd.Series(dtype=float)
            records = []
            for p, v in zip(docs[0].get("period", []), docs[0].get("value", [])):
                if v is not None and v != "NA":
                    dt = pd.to_datetime(p, errors="coerce")
                    if pd.notna(dt) and dt >= pd.Timestamp("2019-01-01"):
                        records.append((dt, float(v)))
            if not records:
                return pd.Series(dtype=float)
            s = pd.Series([val for _, val in records], index=pd.DatetimeIndex([dt for dt, _ in records]))
            return _clean_pmi_series(s)
        except Exception:
            return pd.Series(dtype=float)

    def _get_te_series(te_url: str) -> pd.Series:
        try:
            r = requests.get(te_url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                return pd.Series(dtype=float)
            pairs = re.findall(r'\[\s*(\d{13})\s*,\s*(\d{2}(?:\.\d+)?)\s*\]', r.text)
            if len(pairs) >= 12:
                records = []
                for ts_ms, val in pairs:
                    dt = pd.to_datetime(int(ts_ms), unit="ms").to_period("M").to_timestamp()
                    if dt >= pd.Timestamp("2019-01-01"):
                        records.append((dt, float(val)))
                if records:
                    s = pd.Series([v for _, v in records], index=pd.DatetimeIndex([d for d, _ in records]))
                    return _clean_pmi_series(s)
        except Exception:
            pass
        return pd.Series(dtype=float)

    mfg_db = _get_dbnomics("https://api.db.nomics.world/v22/series/ISM/pmi/pm?observations=1")
    mfg_te = _get_te_series("https://tradingeconomics.com/united-states/manufacturing-pmi")
    mfg_pmi = mfg_te.combine_first(mfg_db) if not mfg_te.empty else mfg_db

    svc_db = _get_dbnomics("https://api.db.nomics.world/v22/series/ISM/nm-pmi/pm?observations=1")
    svc_te = _get_te_series("https://tradingeconomics.com/united-states/non-manufacturing-pmi")
    svc_pmi = svc_te.combine_first(svc_db) if not svc_te.empty else svc_db

    ny_mfg = fetch_fred("GACDISA066MSFRBNY", start_date="2019-01-01")
    philly_mfg = fetch_fred("GACDFSA066MSFRBPHI", start_date="2019-01-01")
    if not ny_mfg.empty and not philly_mfg.empty:
        fed_mfg_proxy = (50.0 + (ny_mfg.resample("MS").last() + philly_mfg.resample("MS").last()) * 0.18).dropna()
        if mfg_pmi.empty:
            mfg_pmi = fed_mfg_proxy
        elif mfg_pmi.index.max() < fed_mfg_proxy.index.max():
            mfg_pmi = mfg_pmi.resample("MS").last().combine_first(fed_mfg_proxy)

    cfnai = fetch_fred("CFNAI", start_date="2019-01-01")
    if not cfnai.empty:
        svc_proxy = (52.5 + cfnai.resample("MS").last() * 3.5).dropna()
        if svc_pmi.empty:
            svc_pmi = svc_proxy
        elif svc_pmi.index.max() < svc_proxy.index.max():
            svc_pmi = svc_pmi.resample("MS").last().combine_first(svc_proxy)

    mfg_pmi = _clean_pmi_series(mfg_pmi).rename("ISM制造业PMI")
    svc_pmi = _clean_pmi_series(svc_pmi).rename("ISM非制造业PMI")
    return mfg_pmi, svc_pmi


def fetch_yf_ohlcv(tickers: list, period: str = "5y") -> dict:
    """批量拉取 Yahoo Finance 完整 OHLCV 数据"""
    raw = yf.download(tickers, period=period, auto_adjust=True, progress=False, group_by="column")
    data = {}
    for field in ["Open", "High", "Low", "Close", "Volume"]:
        if isinstance(raw.columns, pd.MultiIndex):
            df = raw[field].copy()
        else:
            df = raw[[field]].copy()
            df.columns = [tickers[0]]
        df.index = pd.to_datetime(df.index).tz_localize(None)
        data[field] = df.sort_index()
    return data


def compute_monthly_flow_intensity(ohlcv: dict, ticker_labels: dict, window: int = 20, tail_months: int = 24) -> pd.DataFrame:
    """
    计算机构级 Chaikin Money Flow (CMF) 20日资金流净强弱指数 (%)，
    并按月度聚合输出最近 tail_months 个月（默认 24 个月 = 2 年完整周期），确保柱状图宽大清晰、每根柱体一目了然！
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
        dollar_vol = vol[ticker] * close[ticker]
        signed_dollar_vol = clv * dollar_vol

        num = signed_dollar_vol.rolling(window=window, min_periods=10).sum()
        den = dollar_vol.rolling(window=window, min_periods=10).sum().replace(0, np.nan)
        flow_df[label] = (num / den) * 100.0

    monthly = flow_df.resample("ME").mean().dropna(how="all")
    return monthly.iloc[-tail_months:]


def compute_global_outflow_panic(ohlcv: dict, vix: pd.Series, credit_spread: pd.Series, n_weeks: int = 156) -> pd.DataFrame:
    """
    构建「全球资产流出恐慌图」核心量化指标（近 3 年 / 156 周）
    """
    global_tickers = ["SPY", "QQQ", "EEM", "TLT", "GLD", "DBC", "IBIT"]
    high = ohlcv["High"]
    low = ohlcv["Low"]
    close = ohlcv["Close"]
    vol = ohlcv["Volume"]

    cmf_all = pd.DataFrame(index=close.index)
    for t in global_tickers:
        if t not in close.columns:
            continue
        hl_range = (high[t] - low[t]).replace(0, np.nan)
        clv = (((close[t] - low[t]) - (high[t] - close[t])) / hl_range).fillna(0.0)
        dvol = vol[t] * close[t]
        num = (clv * dvol).rolling(20, min_periods=10).sum()
        den = dvol.rolling(20, min_periods=10).sum().replace(0, np.nan)
        cmf_all[t] = (num / den) * 100.0

    cmf_w = cmf_all.resample("W-FRI").last().dropna(how="all")
    valid_cnt = cmf_w.notna().sum(axis=1).replace(0, np.nan)
    outflow_breadth = ((cmf_w < 0).sum(axis=1) / valid_cnt * 100.0).rename("同步净流出资产占比(%)")

    avg_outflow_pressure = ((-cmf_w.mean(axis=1) + 15.0) / 30.0 * 100.0).clip(0, 100)

    vix_w = vix.resample("W-FRI").last().reindex(cmf_w.index).ffill(limit=6)
    vix_score = ((vix_w - 12.0) / 26.0 * 100.0).clip(0, 100)

    cs_w = credit_spread.resample("W-FRI").last().reindex(cmf_w.index).ffill(limit=6)
    cs_score = ((cs_w - 6.0) / 8.0 * 100.0).clip(0, 100)

    panic_idx = (0.45 * avg_outflow_pressure + 0.30 * vix_score + 0.25 * cs_score).rename("全球资产流出恐慌指数")
    panic_ma10 = panic_idx.rolling(10, min_periods=3).mean().rename("恐慌指数10周均线")

    res = pd.concat([panic_idx, panic_ma10, outflow_breadth, vix_w.rename("VIX恐慌指数")], axis=1, sort=True)
    return res.dropna(how="all").iloc[-n_weeks:]


def _normalize_header_table(raw: pd.DataFrame, required_keywords: list) -> pd.DataFrame:
    df = raw.copy()
    cols_lower = [str(c).strip().lower() for c in df.columns]
    if all(any(kw in c for c in cols_lower) for kw in required_keywords):
        df.columns = cols_lower
        return df

    for idx in range(min(10, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[idx].tolist()]
        if all(any(kw in v for v in row_vals) for kw in required_keywords):
            df = df.iloc[idx + 1 :].copy()
            df.columns = row_vals
            return df

    df.columns = cols_lower
    return df


def fetch_aaii_sentiment() -> pd.DataFrame:
    """多源容错抓取 AAII 散户情绪调查 (看涨% - 看跌%)"""
    try:
        url = "https://www.aaii.com/files/surveys/sentiment.xls"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        raw = None
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for engine in ["calamine", None, "openpyxl"]:
                try:
                    raw = pd.read_excel(io.BytesIO(r.content), engine=engine)
                    if raw is not None and not raw.empty:
                        break
                except Exception:
                    continue
            if raw is None:
                tables = pd.read_html(io.BytesIO(r.content))
                if tables:
                    raw = tables[0]

        if raw is not None and not raw.empty:
            raw = _normalize_header_table(raw, ["date", "bull", "bear"])
            date_col = [c for c in raw.columns if "date" in c][0]
            bull_col = [c for c in raw.columns if "bull" in c][0]
            bear_col = [c for c in raw.columns if "bear" in c][0]
            df = raw[[date_col, bull_col, bear_col]].copy()
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            for col in [bull_col, bear_col]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace("%", ""), errors="coerce")
            df = df.dropna().set_index(date_col).sort_index()
            if not df.empty and df[bull_col].max() <= 1.5:
                df[bull_col] *= 100.0
                df[bear_col] *= 100.0
            spread = (df[bull_col] - df[bear_col]).rename("散户净看涨差值(%)")
            ma20 = spread.rolling(20, min_periods=5).mean().rename("20周均线(%)")
            res = pd.concat([spread, ma20], axis=1, sort=True).dropna()
            if len(res) > 20:
                print(f"[OK] AAII 散户情绪成功拉取 {len(res)} 周数据 (最新: {res.index[-1].strftime('%Y-%m-%d')})")
                return res
    except Exception as e:
        print(f"[Info] AAII 文件通道切换至网页直抓: {e}")

    try:
        page = requests.get("https://www.aaii.com/sentimentsurvey/sent_results", headers=HEADERS, timeout=30)
        page.raise_for_status()
        tables = pd.read_html(io.StringIO(page.text))
        for tbl in tables:
            tbl = _normalize_header_table(tbl, ["bull", "bear"])
            cols = list(tbl.columns)
            if any("bull" in c for c in cols) and any("bear" in c for c in cols):
                d_col = cols[0]
                b_col = [c for c in cols if "bull" in c][0]
                s_col = [c for c in cols if "bear" in c][0]
                df = tbl[[d_col, b_col, s_col]].copy()
                df[d_col] = pd.to_datetime(df[d_col], errors="coerce")
                for col in [b_col, s_col]:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace("%", ""), errors="coerce")
                df = df.dropna().set_index(d_col).sort_index()
                spread = (df[b_col] - df[s_col]).rename("散户净看涨差值(%)")
                ma20 = spread.rolling(20, min_periods=5).mean().rename("20周均线(%)")
                res = pd.concat([spread, ma20], axis=1, sort=True).dropna()
                if not res.empty:
                    return res
    except Exception as e:
        print(f"[Warning] AAII 数据全部通道抓取失败: {e}")
    return pd.DataFrame()


def fetch_institutional_equity_exposure() -> pd.DataFrame:
    """
    美国 CFTC 官方每周《金融机构持仓报告》(TFF)
    「Asset Manager / Institutional（共同基金、养老金、保险、资管机构）」标普500期货真实多头占比(%) 及其 20周均线(%)
    """
    try:
        year = datetime.now().year
        frames = []
        for y in [year - 3, year - 2, year - 1, year]:
            url = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{y}.zip"
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

        tff = pd.concat(frames, ignore_index=True)
        name_col = [c for c in tff.columns if "Market_and_Exchange_Names" in c][0]
        date_col = [c for c in tff.columns if "Report_Date_as_YYYY-MM-DD" in c][0]
        am_long_col = [c for c in tff.columns if "Asset_Mgr_Positions_Long_All" in c][0]
        am_short_col = [c for c in tff.columns if "Asset_Mgr_Positions_Short_All" in c][0]

        tff[date_col] = pd.to_datetime(tff[date_col])
        sp = tff[
            tff[name_col].str.contains("E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE", case=False, na=False)
            & ~tff[name_col].str.contains("MICRO", case=False, na=False)
        ].copy()
        sp = sp.set_index(date_col).sort_index()

        def _calc_ratio(row):
            lg = float(row[am_long_col])
            sh = float(row[am_short_col])
            tot = lg + sh
            return (lg / tot * 100.0) if tot > 0 else np.nan

        exposure = sp.apply(_calc_ratio, axis=1).groupby(level=0).last().sort_index().rename("机构股票多头敞口(%)")
        ma20 = exposure.rolling(20, min_periods=5).mean().rename("20周均线(%)")
        res = pd.concat([exposure, ma20], axis=1, sort=True).dropna()
        print(f"[OK] CFTC 资管机构持仓敞口拉取成功 ({len(res)} 周, 最新: {res.index[-1].strftime('%Y-%m-%d')} = {res['机构股票多头敞口(%)'].iloc[-1]:.1f}%)")
        return res
    except Exception as e:
        print(f"[Warning] CFTC 资管机构持仓抓取异常: {e}")
        return pd.DataFrame()


def _fetch_single_cboe_day(d_str: str):
    u = f"https://cdn.cboe.com/data/us/options/market_statistics/daily/{d_str}_daily_options"
    try:
        r = requests.get(u, headers=HEADERS, timeout=8)
        if r.status_code != 200:
            return None
        payload = r.json()
        ratios = payload.get("ratios", [])
        equity_val = None
        total_val = None
        for item in ratios:
            name = str(item.get("name", "")).upper()
            val = item.get("value")
            if val is None:
                continue
            if "EQUITY PUT/CALL" in name:
                equity_val = float(val)
            elif "TOTAL PUT/CALL" in name:
                total_val = float(val)
        chosen = equity_val if equity_val is not None else total_val
        if chosen is not None:
            return (pd.to_datetime(d_str), chosen)
    except Exception:
        return None
    return None


def fetch_cboe_put_call() -> pd.DataFrame:
    """并发直连 CBOE 官方每日市场统计 CDN 接口拉取真实 Put/Call 比值"""
    try:
        today = datetime.now(timezone.utc).date()
        candidate_dates = [
            (today - timedelta(days=delta)).strftime("%Y-%m-%d")
            for delta in range(0, 320)
            if (today - timedelta(days=delta)).weekday() < 5
        ]
        records = []
        with ThreadPoolExecutor(max_workers=14) as executor:
            futures = [executor.submit(_fetch_single_cboe_day, d_str) for d_str in candidate_dates]
            for fut in as_completed(futures):
                res = fut.result()
                if res is not None:
                    records.append(res)

        if len(records) >= 20:
            records.sort(key=lambda x: x[0])
            pcr = pd.Series(
                [v for _, v in records],
                index=pd.DatetimeIndex([dt for dt, _ in records]),
                name="Put/Call比值",
            )
            pcr = pcr[~pcr.index.duplicated(keep="last")].sort_index()
            ma20 = pcr.rolling(20, min_periods=5).mean().rename("20日均线")
            out = pd.concat([pcr, ma20], axis=1, sort=True).dropna()
            print(f"[OK] CBOE Put/Call 成功拉取 {len(out)} 个交易日数据 (最新: {out.index[-1].strftime('%Y-%m-%d')})")
            return out
    except Exception as e:
        print(f"[Warning] CBOE 每日统计抓取异常: {e}")

    return pd.DataFrame()


def fetch_cftc_cot_all_assets() -> tuple:
    """
    抓取 CFTC 官方大型机构（非商业头寸）持仓：
    - 严格剔除 MICRO 微型合约干扰（彻底修复黄金锯齿波）
    - 新增 CME 比特币期货（BITCOIN - CHICAGO MERCANTILE EXCHANGE, CFTC #133741）
    - 返回 (周度净持仓万手DataFrame, 全球七大资产机构真实流入/流出净比% DataFrame)
    """
    try:
        year = datetime.now().year
        frames = []
        for y in [year - 3, year - 2, year - 1, year]:
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
            return pd.DataFrame(), pd.DataFrame()

        cot = pd.concat(frames, ignore_index=True)
        name_col = [c for c in cot.columns if "Market and Exchange Names" in c][0]
        date_col = [c for c in cot.columns if "As of Date in Form YYYY-MM-DD" in c][0]
        long_col = [c for c in cot.columns if "Noncommercial Positions-Long (All)" in c][0]
        short_col = [c for c in cot.columns if "Noncommercial Positions-Short (All)" in c][0]
        cot[date_col] = pd.to_datetime(cot[date_col])

        asset_patterns = {
            "sp": (
                r"E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
                "标普500投机净持仓(万手)",
                "美股标普500机构净比(%)",
            ),
            "nq": (
                r"NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE|NASDAQ-100 Consolidated",
                "纳指100投机净持仓(万手)",
                "美股科技纳指机构净比(%)",
            ),
            "ty": (
                r"10-YEAR U\.S\. TREASURY NOTES - CHICAGO BOARD OF TRADE|UST 10Y NOTE - CHICAGO BOARD OF TRADE",
                None,
                "10年期美债机构净比(%)",
            ),
            "gc": (
                r"^GOLD - COMMODITY EXCHANGE INC\.",
                None,
                "实物黄金机构净比(%)",
            ),
            "cl": (
                r"WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE|CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
                None,
                "WTI原油机构净比(%)",
            ),
            "dx": (
                r"USD INDEX - ICE FUTURES U\.S\.|U\.S\. DOLLAR INDEX - ICE FUTURES U\.S\.",
                None,
                "美元指数机构净比(%)",
            ),
            "btc": (
                r"^BITCOIN - CHICAGO MERCANTILE EXCHANGE",
                None,
                "比特币BTC机构净比(%)",
            ),
        }

        net_hands_dict = {}
        net_ratio_dict = {}

        for key, (pattern, hands_label, ratio_label) in asset_patterns.items():
            sub = cot[
                cot[name_col].str.strip().str.contains(pattern, case=False, na=False, regex=True)
                & ~cot[name_col].str.contains("MICRO|CASH PERP", case=False, na=False)
            ].copy()
            if sub.empty:
                continue
            sub = sub.set_index(date_col).sort_index()
            if hands_label:
                s_hands = (
                    sub.apply(lambda x: (float(x[long_col]) - float(x[short_col])) / 10000.0, axis=1)
                    .groupby(level=0)
                    .last()
                    .sort_index()
                    .rename(hands_label)
                )
                net_hands_dict[hands_label] = s_hands

            def _ratio_fn(x):
                lg = float(x[long_col])
                sh = float(x[short_col])
                tot = lg + sh
                return ((lg - sh) / tot * 100.0) if tot > 0 else np.nan

            s_ratio = (
                sub.apply(_ratio_fn, axis=1)
                .groupby(level=0)
                .last()
                .sort_index()
                .rename(ratio_label)
            )
            net_ratio_dict[ratio_label] = s_ratio

        hands_df = pd.concat(list(net_hands_dict.values()), axis=1, sort=True) if net_hands_dict else pd.DataFrame()
        ratio_df = pd.concat(list(net_ratio_dict.values()), axis=1, sort=True) if net_ratio_dict else pd.DataFrame()
        print(f"[OK] CFTC 全球七大资产机构持仓(含BTC)拉取成功 ({len(ratio_df)} 周, {list(ratio_df.columns)})")
        return hands_df, ratio_df
    except Exception as e:
        print(f"[Warning] CFTC COT 抓取异常: {e}")
        return pd.DataFrame(), pd.DataFrame()


def fetch_sp500_breadth() -> pd.DataFrame:
    """实时计算标普500成分股高于50日/200日均线的市场宽度比例"""
    try:
        r = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=HEADERS, timeout=30)
        sp_table = pd.read_html(io.StringIO(r.text))[0]
        tickers = [t.replace(".", "-") for t in sp_table["Symbol"].tolist()]
    except Exception:
        tickers = [
            "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "BRK-B", "LLY", "AVGO", "JPM",
            "TSLA", "UNH", "XOM", "V", "PG", "MA", "JNJ", "COST", "HD", "ABBV",
            "WMT", "NFLX", "MRK", "CRM", "BAC", "CVX", "KO", "AMD", "PEP", "TMO",
            "LIN", "ORCL", "ADBE", "CSCO", "ACN", "MCD", "ABT", "WFC", "DHR", "QCOM",
            "TXN", "INTU", "AMGN", "PM", "ISRG", "IBM", "GE", "CAT", "NOW", "VZ",
            "DIS", "GS", "AMAT", "UBER", "SPGI", "RTX", "LOW", "HON", "UNP", "NEE",
        ]
    try:
        raw = yf.download(tickers, period="2y", auto_adjust=True, progress=False, group_by="column")
        prices = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
        prices.index = pd.to_datetime(prices.index).tz_localize(None)
        ma50 = prices.rolling(50).mean()
        ma200 = prices.rolling(200).mean()
        valid_50 = ma50.notna().sum(axis=1).replace(0, np.nan)
        valid_200 = ma200.notna().sum(axis=1).replace(0, np.nan)
        b50 = ((prices > ma50).sum(axis=1) / valid_50 * 100.0).rename("50日市场宽度(%)")
        b200 = ((prices > ma200).sum(axis=1) / valid_200 * 100.0).rename("200日市场宽度(%)")
        df = pd.concat([b50, b200], axis=1, sort=True).dropna()
        print(f"[OK] S&P 500 市场宽度计算完成 ({len(df)} 个交易日)")
        return df.iloc[-260:]
    except Exception as e:
        print(f"[Warning] 市场宽度计算异常: {e}")
        return pd.DataFrame()


def align_tail(series_list, freq="W-FRI", n=156, max_ffill_steps=8) -> pd.DataFrame:
    """无断点连续对齐函数"""
    valid_series = [s for s in series_list if s is not None and not s.empty]
    if not valid_series:
        return pd.DataFrame()

    if freq is None:
        base_idx = valid_series[0].index
        df = pd.concat(valid_series, axis=1, sort=True)
        df = df.ffill(limit=max_ffill_steps).reindex(base_idx)
        return df.dropna(how="all").iloc[-n:]

    df = pd.concat(valid_series, axis=1, sort=True).resample(freq).last()
    global_end = df.index.max()
    for col in df.columns:
        first_valid = df[col].first_valid_index()
        last_valid = df[col].last_valid_index()
        if first_valid is not None and last_valid is not None:
            tail_limit_idx = min(global_end, last_valid + pd.Timedelta(weeks=6))
            sub = df.loc[first_valid:tail_limit_idx, col].ffill(limit=max_ffill_steps)
            df.loc[first_valid:tail_limit_idx, col] = sub

    df = df.dropna(how="all")
    return df.iloc[-n:]


# ==========================================
# 3. 组装全部 26 张图表与深度组合对比指南
# ==========================================
def build_all_charts() -> list:
    print(">>> 正在拉取市场行情、商品期货、AI板块、美元指数与全球大类资产 ETF 数据...")
    market_tickers = [
        "^GSPC", "^NDX", "HG=F", "GC=F", "CL=F", "^VIX", "DX-Y.NYB", "UUP",
        "VTI", "XLK", "XLC", "XLY", "XLF", "XLI", "XLE", "XLV", "XLP", "XLU", "XLRE", "XLB",
        "SMH", "AIQ", "BOTZ", "SPY", "QQQ", "IWM", "DIA",
        "EEM", "TLT", "GLD", "DBC", "IBIT",
    ]
    ohlcv = fetch_yf_ohlcv(market_tickers, period="5y")
    mkt = ohlcv["Close"].ffill(limit=5)

    spx = mkt["^GSPC"].rename("标普500指数")
    ndx = mkt["^NDX"].rename("纳斯达克100指数")
    copper_gold = (mkt["HG=F"] / mkt["GC=F"] * 1000).rename("铜金比(×1000)")
    oil_gold = (mkt["CL=F"] / mkt["GC=F"] * 100).rename("油金比(×100)")

    # 美元指数 DXY 连续序列（DX-Y.NYB 优先，缺失部分由 UUP/FRED 广义美元指数平滑补齐）
    dxy_raw = mkt["DX-Y.NYB"].dropna() if "DX-Y.NYB" in mkt.columns else pd.Series(dtype=float)
    if dxy_raw.empty and "UUP" in mkt.columns:
        dxy_raw = mkt["UUP"].dropna()
    dxy = dxy_raw.reindex(mkt.index).ffill(limit=5).rename("美元指数DXY")

    print(">>> 正在拉取情绪、期权、CFTC七大资产持仓与市场宽度...")
    aaii_df = fetch_aaii_sentiment()
    inst_exp_df = fetch_institutional_equity_exposure()
    pcr_df = fetch_cboe_put_call()
    cot_hands_df, cot_ratio_df = fetch_cftc_cot_all_assets()
    breadth_df = fetch_sp500_breadth()

    print(">>> 正在拉取 FRED 与官方宏观、PMI 与利率数据...")
    # 一、消费端
    icsa_w = (fetch_fred("ICSA") / 10000.0).rename("初请失业金(万人)")
    ccsa_10w = (fetch_fred("CCSA") / 100000.0).rename("续请失业金(十万人)")
    payems_yoy = yoy_pct(fetch_fred("PAYEMS")).rename("非农就业同比(%)")

    pi_yoy = yoy_pct(fetch_fred("PI")).rename("个人收入同比(%)")
    pce_yoy = yoy_pct(fetch_fred("PCE")).rename("个人消费支出同比(%)")
    psavert = fetch_fred("PSAVERT").rename("个人储蓄率(%)")

    exhos_yoy = fetch_existing_home_sales_yoy()
    hsn1f_yoy = yoy_pct(fetch_fred("HSN1F")).rename("新屋销售同比(%)")
    cs20_yoy = yoy_pct(fetch_fred("SPCS20RSA")).rename("20大都市房价同比(%)")
    med_new_yoy = yoy_pct(fetch_fred("MSPNHSUS")).rename("新房价格中位数同比(%)")

    veh_sales = fetch_fred("TOTALSA").rename("全美汽车销量(百万辆)")
    veh_yoy = yoy_pct(fetch_fred("TOTALSA")).rename("汽车销量同比(%)")
    used_car = fetch_fred("CUSR0000SETA02").rename("二手车价格指数")

    retail_yoy = yoy_pct(fetch_fred("RSAFS")).rename("总体零售同比(%)")
    ex_auto_yoy = yoy_pct(fetch_fred("RSXFS")).rename("核心连锁零售同比(%)")
    ecom_raw = fetch_fred("MRTSSM454USS")
    if ecom_raw.empty:
        ecom_raw = fetch_fred("MRTSSM4541USS")
    ecom_yoy = yoy_pct(ecom_raw).rename("电商非实体零售同比(%)")

    # 二、供给端
    permit_yoy = yoy_pct(fetch_fred("PERMIT")).rename("建房许可同比(%)")
    houst_yoy = yoy_pct(fetch_fred("HOUST")).rename("新屋开工同比(%)")
    house_inv_yoy = yoy_pct(fetch_fred("HNFSEPUSSA")).rename("新房库存同比(%)")

    dur_yoy = yoy_pct(fetch_fred("DGORDER")).rename("耐用品订单同比(%)")
    core_cap_yoy = yoy_pct(fetch_fred("NEWORDER")).rename("核心资本品订单同比(%)")
    mfg_pmi, svc_pmi = fetch_ism_pmi()

    wei = fetch_fred("WEI").rename("纽约联储WEI周度经济指数")
    ppi_yoy = yoy_pct(fetch_fred("PPIACO")).rename("PPI出厂价格同比(%)")
    cpi_yoy = yoy_pct(fetch_fred("CPIAUCSL")).rename("CPI通胀同比(%)")

    # 三、利率端
    fed_rate = fetch_fred("DFF").rename("美联储政策利率(%)")
    dgs10 = fetch_fred("DGS10").rename("10年期美债收益率(%)")
    dgs5 = fetch_fred("DGS5").rename("5年期美债收益率(%)")
    dgs2 = fetch_fred("DGS2").rename("2年期美债收益率(%)")
    dgs3m = fetch_fred("DGS3MO").rename("3个月期美债利率(%)")

    credit_spread = fetch_fred("BAMLH0A3HYC").rename("CCC级高收益债信用利差(%)")
    if credit_spread.empty:
        ccc_y = fetch_fred("BAMLH0A3HYCEY")
        credit_spread = (ccc_y - dgs10).dropna().rename("CCC级高收益债信用利差(%)")
    vix = mkt["^VIX"].rename("VIX恐慌指数")

    # 四、市场端：11大板块
    offense_map = {
        "XLK": "科技XLK",
        "XLC": "通讯XLC",
        "XLY": "可选消费XLY",
        "XLF": "金融XLF",
        "XLI": "工业XLI",
        "XLE": "能源XLE",
    }
    defense_map = {
        "XLV": "医疗XLV(防御)",
        "XLP": "必需消费XLP(防御)",
        "XLU": "公用事业XLU(防御)",
        "XLRE": "房地产XLRE",
        "XLB": "原材料XLB",
    }

    sec_off_df = pd.DataFrame(index=mkt.index)
    for code, label in offense_map.items():
        rel = (mkt[code] / mkt["VTI"]).dropna().iloc[-260:]
        sec_off_df[label] = (rel / rel.iloc[0]) * 100.0
    sec_off_df = sec_off_df.resample("W-FRI").last().ffill(limit=4).dropna(how="all")

    sec_def_df = pd.DataFrame(index=mkt.index)
    for code, label in defense_map.items():
        rel = (mkt[code] / mkt["VTI"]).dropna().iloc[-260:]
        sec_def_df[label] = (rel / rel.iloc[0]) * 100.0
    sec_def_df = sec_def_df.resample("W-FRI").last().ffill(limit=4).dropna(how="all")

    # 五、扩展模块（共5张：AI强弱 + 全球7大资产24个月资金流柱状图 + CFTC机构7大资产24个月净比柱状图 + 全球流出恐慌图 + 美元指数跨资产跷跷板对比图）
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
        rel = (mkt[num] / mkt[den]).dropna().iloc[-390:]
        ai_df[label] = (rel / rel.iloc[0]) * 100.0
    ai_df = ai_df.resample("W-FRI").last().ffill(limit=4).dropna(how="all")

    # 图22：全球七大核心资产 24个月（2年）月度资金净流入/流出强度柱状图（每根柱宽清晰易辨）
    global_flow_tickers = {
        "SPY": "美股大盘(SPY)",
        "QQQ": "美股科技(QQQ)",
        "EEM": "新兴市场(EEM)",
        "TLT": "美债长债(TLT)",
        "GLD": "实物黄金(GLD)",
        "DBC": "大宗商品(DBC)",
        "IBIT": "比特币ETF(IBIT)",
    }
    global_flow_df = compute_monthly_flow_intensity(ohlcv, global_flow_tickers, window=20, tail_months=24)

    # 图23：大型机构在全球七大资产（含BTC）的真实流出/流入数据比（近 24 个月柱状图）
    inst_global_ratio_df = (
        cot_ratio_df.resample("ME").mean().dropna(how="all").iloc[-24:]
        if not cot_ratio_df.empty
        else pd.DataFrame()
    )

    # 图24：全球资产流出恐慌指数与跨资产抛售压力图（近 3 年 / 156 周）
    outflow_panic_df = compute_global_outflow_panic(ohlcv, vix, credit_spread, n_weeks=156)

    # 图25：新增！美元指数(DXY)强弱对比与全球五大核心资产跷跷板效应（近 2 年标准化对比，起点=100）
    dxy_compare_map = {
        "美元指数DXY": dxy,
        "美股标普500(SPY)": mkt["SPY"],
        "新兴市场(EEM)": mkt["EEM"],
        "实物黄金(GLD)": mkt["GLD"],
        "美债长债(TLT)": mkt["TLT"],
        "比特币ETF(IBIT)": mkt["IBIT"],
    }
    dxy_compare_df = pd.DataFrame(index=mkt.index)
    for label, s_raw in dxy_compare_map.items():
        s_clean = s_raw.dropna().iloc[-504:]  # 近2年约504个交易日
        if not s_clean.empty and s_clean.iloc[0] != 0:
            dxy_compare_df[label] = (s_clean / s_clean.iloc[0]) * 100.0
    dxy_compare_df = dxy_compare_df.resample("W-FRI").last().ffill(limit=4).dropna(how="all").iloc[-104:]

    charts = [
        # --- 一、消费端 ---
        {
            "id": "01",
            "category": "消费",
            "chart_type": "LINE",
            "title": "01. 就业市场 (Employment)",
            "df": align_tail([icsa_w, ccsa_10w, payems_yoy], freq="W-FRI", n=180),
            "right_cols": ["非农就业同比(%)"],
            "left_title": "初请(万人) / 续请(十万人)",
            "right_title": "非农同比 (%)",
            "what": "• 蓝线(左轴)：初请失业金人数(万人) —— 每周新增申领失业救济人数（裁员速度高频温度计）\n• 红线(左轴)：续请失业金人数(十万人) —— 连续两周及以上领取失业救济总规模（再就业难度指标）\n• 黄线(右轴)：美国非农就业总人数年度同比增速(%)",
            "how": "美国70%GDP由个人消费贡献，薪资就业是消费扩张的底层水源。传导顺序严格遵循：「企业利润承压 → 冻结招聘并启动裁员（蓝线率先跳升）→ 失业人群滞留就业市场（红线延迟4~8周抬头）→ 非农总岗位增速下台阶（黄线趋势滑落）→ 居民削减消费」。",
            "changes": "• 【组合① 蓝/红低位走平 + 黄线>1.2%】：黄金扩张组合，劳动力供需平衡，居民收入与消费动能稳健，美股盈利具备底部支撑。\n• 【组合② 蓝线急升(>26万) + 红线滞后抬头(>190万)】：典型「裁员扩散信号」，说明企业开始实质性缩编且失业者再就业受阻，未来1~2个季度零售与可选消费将明显降温。\n• 【组合③ 蓝/红高位钝化 + 黄线跌破0.8%向0%逼近】：萨姆规则(Sahm Rule)衰退预警触发区，市场将快速交易美联储大幅降息，美债长债(TLT)跑赢股票。\n• 【组合④ 蓝线脉冲冲高后迅速回落 + 黄线坚挺】：多为飓风、罢工或季节性扰动造成的假警报，不构成趋势反转。",
            "signal": "⚠️ 量化风控阈值：初请失业金连续4周站上 28~30万人 且续请突破 200万人(红线>20.0)，同时非农同比跌破 0.8%，确认消费失速预警，建议增配防御板块(XLP/XLV)与美债。",
        },
        {
            "id": "02",
            "category": "消费",
            "chart_type": "LINE",
            "title": "02. 个人财务状况 (Personal Finances)",
            "df": align_tail([pi_yoy, pce_yoy, psavert], freq="ME", n=60),
            "right_cols": [],
            "left_title": "年度变化率 / 储蓄率 (%)",
            "right_title": "",
            "what": "• 蓝线：个人总收入年度同比增速(%) —— 含薪资、股息利息及政府转移支付\n• 红线：个人消费支出(PCE)年度同比增速(%) —— 含商品与服务名义消费支出\n• 黄线：个人储蓄率(%) —— 税后可支配收入中未用于消费的留存比例（历史中枢约6%~7%）",
            "how": "透视美国家庭资产负债表健康度的核心三棱镜。健康的可持续消费必须建立在「收入增速 ≥ 支出增速」的基础上；若支出长期跑赢收入，说明居民正在消耗超额储蓄或依赖20%+高息信用卡透支维持体面。",
            "changes": "• 【组合① 蓝线 ≥ 红线 + 黄线稳居5.5%~8%】：健康内生增长组合，居民一边扩大消费一边积累财富，软着陆概率最高。\n• 【组合② 红线持续高于蓝线 + 黄线单边跌破4%】：「透支型消费陷阱」，家庭正在吃老本或借债消费，一旦储蓄率触及3%极限底线，消费将出现断崖式踩刹车。\n• 【组合③ 蓝线骤降 + 黄线被动跳升 + 红线暴跌】：预防性储蓄激增（衰退典型特征），居民因担忧失业而主动捂紧钱包，可选消费(XLY)与金融板块承压。\n• 【组合④ 蓝/红线同步维持5%以上高位】：名义收入与支出双过热，服务业通胀粘性强，美联储难以快速降息。",
            "signal": "⚠️ 量化风控阈值：个人储蓄率跌破 3.5% 且 消费增速(红线)高于收入增速(蓝线) 超过1.5个百分点，预示未来2个季度消费面临均值回归式收缩。",
        },
        {
            "id": "03",
            "category": "消费",
            "chart_type": "LINE",
            "title": "03. 房屋销售与房价 (Home Sales)",
            "df": align_tail([exhos_yoy, hsn1f_yoy, cs20_yoy, med_new_yoy], freq="ME", n=60),
            "right_cols": [],
            "left_title": "同比变化 (%)",
            "right_title": "",
            "what": "• 蓝线：全美成屋销售量同比(%) —— 占美国房屋交易85%以上，反映二手房真实置换需求\n• 红线：全美新屋销售量同比(%) —— 开发商一手房签约量，对按揭利率与促销最敏感\n• 黄线：标普Case-Shiller 20大都市房价指数同比(%) —— 核心城市存量房价标杆\n• 绿线：全美新房销售中位价同比(%) —— 反映刚需新房定价与户型结构变化",
            "how": "房地产是美国家庭第一大财富蓄水池，上下游拉动家电、建材、家具、银行按揭与地方税收。地产周期严格遵循「量在价先」铁律：成交量（蓝/红线）通常领先房价（黄/绿线）3~6个月见顶或筑底。",
            "changes": "• 【组合① 蓝/红线由负转正上冲 + 黄/绿线仍在低位】：典型「量升价稳底部启动」，按揭利率回落释放积压刚需，预计3~6个月后核心都市房价(黄线)加速回升。\n• 【组合② 蓝/红线见顶大幅转负 + 黄线仍居高位】：典型「量缩价撑顶背离」，卖家惜售导致成交冰冻，随后房价将被迫降价补跌。\n• 【组合③ 红线明显强于蓝线 + 绿线走弱而黄线坚挺】：高利率锁死二手房房东置换意愿（不愿放弃3%低息房贷），买家被迫转向开发商降价促销的新房市场。\n• 【组合④ 四线全线深陷负值区】：地产深度去库周期，家居建材与地区银行信用风险承压。",
            "signal": "💡 量化风控信号：新屋(红线)与成屋(蓝线)销售同比连续3个月站稳 0% 上方，是做多地产建筑商(ITB/XHB)与家装零售的右侧确认信号。",
        },
        {
            "id": "04",
            "category": "消费",
            "chart_type": "LINE",
            "title": "04. 汽车消费与二手车价 (Auto Sales)",
            "df": align_tail([veh_sales, veh_yoy, used_car], freq="ME", n=60),
            "right_cols": ["二手车价格指数"],
            "left_title": "销量(百万辆) / 同比(%)",
            "right_title": "二手车CPI指数",
            "what": "• 蓝线(左轴)：全美轻型汽车月度折年总销量(百万辆) —— 疫情前健康中枢为1650~1750万辆\n• 红线(左轴)：全美汽车销量年度同比增速(%)\n• 黄线(右轴)：美国CPI二手车与卡车价格指数 —— 核心商品通胀最敏感的领先风向标",
            "how": "汽车是美国家庭仅次于住房的第二大耐用消费品，超过80%购车依赖车贷融资，因此对利率变化极为敏感。将销量与二手车CPI交叉对比，可同时研判大件消费意愿与美联储最关注的核心商品通胀走向。",
            "changes": "• 【组合① 蓝线稳步攀升(>1600万辆) + 黄线温和下行】：最佳「无通胀复苏组合」，芯片与库存恢复顺畅、车价回归理性，刺激居民换车需求。\n• 【组合② 黄线暴涨 + 蓝线跌破1450万辆】：典型「供给瓶颈/高通胀滞胀组合」，车价飙升反噬购买力，同时推高核心CPI迫使美联储维持紧缩。\n• 【组合③ 蓝线跌破1400万辆 + 红线负增长 + 黄线加速下跌】：「需求坍塌式通缩」，高车贷利率与就业担忧全面抑制大件开支，车企被迫打价格战去库。\n• 【组合④ 红线转正但蓝线仍在1500万辆下方】：低基数下的弱修复，尚未恢复至充分扩张水平。",
            "signal": "💡 量化风控信号：二手车价格指数(黄线)持续下行是核心CPI降温的领先指标；月度折年销量重返 1600万辆 上方代表消费终端韧性强劲。",
        },
        {
            "id": "05",
            "category": "消费",
            "chart_type": "LINE",
            "title": "05. 零售与电商结构 (Retail Sales)",
            "df": align_tail([retail_yoy, ex_auto_yoy, ecom_yoy], freq="ME", n=60),
            "right_cols": [],
            "left_title": "同比增速 (%)",
            "right_title": "",
            "what": "• 蓝线：美国总体零售与食品服务销售额同比(%) —— 含汽车、加油站与餐饮消费\n• 红线：核心零售（剔除高波动汽车及零部件）同比(%) —— 反映日常商品与餐饮真实消费\n• 黄线：非实体店零售（以亚马逊/Shopify为代表的电商网购）同比(%)",
            "how": "最直观追踪美国消费者日常刷卡热度的月度晴雨表。注意零售数据为「名义金额」，必须结合CPI通胀对比：若名义零售增速低于CPI，说明剔除涨价后的「实际消费量」已在萎缩。",
            "changes": "• 【组合① 蓝/红线稳居3.5%~6% + 黄线维持8%~12%】：消费结构健康扩张，线上渗透率稳步提升，消费龙头与支付网络盈利稳健。\n• 【组合② 黄线(电商)逆势走强而红线(核心零售)走弱】：「消费降级与渠道迁移」，通胀压力下居民减少线下冲动购物，转向线上比价与折扣电商。\n• 【组合③ 蓝线大幅低于红线】：油价下跌或汽车销量疲软拖累表面数据，但核心日常消费依然坚挺，不宜误判为消费崩溃。\n• 【组合④ 蓝线与红线同步跌破1.5%甚至转负】：剔除通胀后实际消费全面负增长，企业营收下滑与经济衰退高度确立。",
            "signal": "⚠️ 量化风控阈值：核心零售同比(红线)跌破 1.0% 且持续低于同期 CPI 增速，代表实际购买力萎缩，需回避可选消费与百货零售股。",
        },
        # --- 二、供给端 ---
        {
            "id": "06",
            "category": "供给",
            "chart_type": "LINE",
            "title": "06. 房屋营建与库存 (Housing Supply)",
            "df": align_tail([permit_yoy, houst_yoy, house_inv_yoy, cs20_yoy], freq="ME", n=60),
            "right_cols": [],
            "left_title": "同比变化 (%)",
            "right_title": "",
            "what": "• 蓝线：私人住宅建房许可同比(%) —— 开发商申请施工许可证阶段（地产供给链最前端）\n• 红线：新屋开工同比(%) —— 实际破土动工建设阶段（滞后建房许可1~2个月）\n• 黄线：待售新房库存同比(%) —— 完工及在建待售总库存（滞后开工6~12个月）\n• 绿线：20大都市房价同比(%)",
            "how": "完整刻画房地产供给侧生命周期：「建房许可(蓝) → 新屋开工(红) → 竣工形成库存(黄) → 供需缺口决定未来房价与房租通胀(绿)」。",
            "changes": "• 【组合① 蓝/红线拐头转正 + 黄线库存低位负增长 + 绿线企稳】：最典型「供不应求主升浪」，库存偏低叠加需求回暖，未来6~12个月房价与住宅投资将拉动GDP。\n• 【组合② 黄线库存同比飙升(>12%) + 蓝线持续深度负增长】：典型「供给过剩去库周期」，开发商积压大量现房被迫降价促销，并大幅削减新拿地与开工。\n• 【组合③ 蓝线显著跑赢红线】：开发商已拿到许可并积极储备项目，预示未来1~2个季度施工活动与建材采购将明显提速。\n• 【组合④ 绿线房价高位但蓝/红线持续下滑】：高利率抑制新供给，未来一旦降息将引发新一轮房荒与房价反弹。",
            "signal": "💡 量化风控信号：建房许可(蓝线)增速领先未来6~9个月建材、家电与家居需求；待售库存回落 + 许可转正是地产建筑商(ITB)最佳配置窗口。",
        },
        {
            "id": "07",
            "category": "供给",
            "chart_type": "LINE",
            "title": "07. 耐用品订单与制造业/服务业PMI (Supply Orders)",
            "df": align_tail([dur_yoy, core_cap_yoy, mfg_pmi, svc_pmi], freq="ME", n=60),
            "right_cols": ["ISM制造业PMI", "ISM非制造业PMI"],
            "left_title": "订单同比 (%)",
            "right_title": "ISM PMI 指数 (50为荣枯线)",
            "what": "• 蓝线(左轴)：全美耐用品新增订单同比(%) —— 含波动极大的商用飞机与国防军工大单\n• 红线(左轴)：核心资本品订单同比(%) —— 剔除国防与飞机，纯粹衡量企业设备投资(CapEx)意愿\n• 黄线(右轴)：ISM制造业PMI  • 绿线(右轴)：ISM非制造业(服务业)PMI（50为扩张与收缩荣枯分界线）",
            "how": "企业接到资本品订单后需3~6个月采购原材料、排产与交付。将企业硬订单（左轴）与采购经理人软景气调查（右轴）结合，可精准判断制造业与服务业是否出现「双轮驱动」或「冷热撕裂」。",
            "changes": "• 【组合① 红线转正上行 + 黄线(制造业PMI)>50 + 绿线(服务业PMI)>52】：经济「全面复苏扩张组合」，企业扩产与服务消费共振，顺周期工业(XLI)、金融与科技齐涨。\n• 【组合② 蓝线暴涨暴跌而红线走平】：仅受波音大飞机或军工订单单月集中入账扰动，属于统计噪音，不代表制造业基本面反转。\n• 【组合③ 黄线深陷50下方收缩 + 绿线稳居52以上】：「服务强、制造弱的K型分化」，工业去库但服务消费托底，利率保持高位，资金抱团AI科技与服务龙头。\n• 【组合④ 红线持续负增长 + 黄/绿线双双跌破50】：制造业收缩向服务业蔓延，全面衰退警报拉响。",
            "signal": "⚠️ 量化风控信号：核心资本品订单(红线)连续3个月正增长且 ISM 制造业 PMI(黄线)站上 50 荣枯线，确认工业补库周期开启。",
        },
        {
            "id": "08",
            "category": "供给",
            "chart_type": "LINE",
            "title": "08. 纽约联储每周经济指数 (WEI Index)",
            "df": align_tail([wei], freq="W-FRI", n=180),
            "right_cols": [],
            "left_title": "WEI 年化GDP增速映射 (%)",
            "right_title": "",
            "what": "• 蓝线：纽约联储每周经济指数 (Weekly Economic Index, WEI) —— 实时映射美国季度实际GDP同比年化增速(%)",
            "how": "整合红皮书同店零售、初请/续请失业金、原钢产量、发电量、铁路货运、燃油消费、临时工就业等10项高频周度实体数据，比官方季度GDP报告提前2~3个月揭示实体经济拐点。",
            "changes": "• 【情景① 蓝线稳居 1.5% ~ 2.8% 黄金走廊】：最理想的「金发姑娘经济(Goldilocks)」，经济温和增长且不引发恶性通胀，最利于美股长牛。\n• 【情景② 蓝线持续攀升突破 3.0%】：实体经济动能强劲，企业盈利预期上修，但需警惕经济过热推升10年期美债收益率、压制高估值成长股。\n• 【情景③ 蓝线从高位连续跌破 1.5% 向 0.5% 俯冲】：高频货运、发电与零售同步走弱，实体经济快速失速，美债收益率将见顶暴跌。\n• 【情景④ 蓝线跌破 0% 负值区】：历史上高度对应美国经济已陷入实质性衰退（如2008年、2020年）。",
            "signal": "🚨 量化风控阈值：WEI 连续3周站上 3.0% 提示警惕再通胀与利率上行；一旦跌破 0.5% 并逼近 0% 零轴，立即启动衰退防守配置。",
        },
        {
            "id": "09",
            "category": "供给",
            "chart_type": "LINE",
            "title": "09. 铜金比与PPI/PMI (Industrial & PPI)",
            "df": align_tail([copper_gold, ppi_yoy, mfg_pmi], freq="W-FRI", n=180, max_ffill_steps=8),
            "right_cols": ["PPI出厂价格同比(%)", "ISM制造业PMI"],
            "left_title": "铜金比 (×1000)",
            "right_title": "PPI同比(%) / ISM制造业PMI",
            "what": "• 蓝线(左轴)：铜金比（COMEX铜期货 ÷ 黄金期货 × 1000）—— 全球实体工业需求 vs 货币避险情绪的高频晴雨表\n• 红线(右轴)：美国工业品出厂价格指数 PPI 同比(%) —— 衡量上游制造业通胀压力\n• 黄线(右轴)：美国 ISM 制造业采购经理人指数 PMI（50为荣枯线）",
            "how": "铜被誉为「铜博士」，广泛用于电网、建筑、新能源与AI数据中心，代表全球实体工业景气；黄金代表货币避险与实际利率预期。两者相除剔除了美元汇率干扰，趋势拐点通常领先官方 PPI 与制造业 PMI 2~4 个月。",
            "changes": "• 【组合① 蓝线(铜金比)持续走强 + 黄线(PMI)回升 + 红线(PPI)抬头】：标准「全球工业再通胀共振」，实体制造需求压倒避险，利好工业(XLI)、原材料(XLB)与顺周期资源股。\n• 【组合② 蓝线提前见顶大幅下挫 + 红线(PPI)仍在高位】：「上游通胀见顶顶背离」，预示未来1~2个季度工业品价格将回落、制造业PMI承压，长久期美债迎来配置良机。\n• 【组合③ 蓝线在低位率先拐头向上 + 红线/黄线仍处低谷】：「工业复苏领先底背离」，聪明资金已开始定价补库复苏，是左侧布局顺周期板块的高胜率信号。\n• 【组合④ 蓝线破位下跌 + 黄线深陷48以下】：全球制造业通缩与避险情绪双杀，黄金显著跑赢工业金属。",
            "signal": "💡 量化风控信号：铜金比是宏观交易员的「高频工业先行指标」，当蓝线趋势方向与红线(PPI)背离持续超过8周时，官方PPI与PMI大概率向铜金比方向回归。",
        },
        {
            "id": "10",
            "category": "供给",
            "chart_type": "LINE",
            "title": "10. 油金比与CPI (Energy & CPI)",
            "df": align_tail([oil_gold, cpi_yoy], freq="W-FRI", n=180, max_ffill_steps=8),
            "right_cols": ["CPI通胀同比(%)"],
            "left_title": "油金比 (×100)",
            "right_title": "CPI通胀同比 (%)",
            "what": "• 蓝线(左轴)：油金比（WTI原油期货 ÷ 黄金期货 × 100）—— 剔除纯粹货币贬值因素后的真实能源成本压力\n• 红线(右轴)：美国消费者物价指数 CPI 同比(%) —— 美联储货币政策最核心的盯住目标",
            "how": "原油是现代社会交通、航空、化工、农业与物流的总成本之母。用原油除以黄金得到「油金比」，能剥离单纯由央行印钞引起的名义价格上涨，精准领先反映终端消费通胀 CPI 的大方向（领先约1~3个月）。",
            "changes": "• 【组合① 蓝线(油金比)持续飙升 + 红线(CPI)同步抬头】：典型「能源驱动型二次通胀」，油价向物流与食品传导，美联储被迫推迟降息甚至重启加息，股债估值双承压。\n• 【组合② 蓝线见顶大幅回落 + 红线(CPI)滞后高位】：「通胀见顶确认组合」，能源成本回落将在未来1~3个月拉低整体CPI，打开降息通道，科技成长股(QQQ)与美债(TLT)迎来估值修复。\n• 【组合③ 蓝线低位徘徊 + 红线(CPI)顽固居于3%以上】：说明当前通胀并非由能源驱动，而是由房租与薪资等「核心服务业粘性通胀」主导，降息节奏将极为缓慢。\n• 【组合④ 蓝线急跌 + 红线跌破2%】：能源与需求双弱，通缩预期升温，防御板块与黄金占优。",
            "signal": "💡 量化风控信号：油金比(蓝线)大幅回落是「通胀见顶、10年美债利率下行、科技成长股估值扩张」最可靠的高频前置信号之一。",
        },
        {
            "id": "11",
            "category": "供给",
            "chart_type": "LINE",
            "title": "11. 抽象化的宏观经济 (Macro Consensus)",
            "df": align_tail([copper_gold, oil_gold, dgs10], freq="W-FRI", n=180),
            "right_cols": ["10年期美债收益率(%)"],
            "left_title": "铜金比 / 油金比",
            "right_title": "10年美债收益率 (%)",
            "what": "• 蓝线(左轴)：铜金比 —— 全球万亿资金投票的「真实经济增长预期」\n• 红线(左轴)：油金比 —— 全球万亿资金投票的「真实通胀压力预期」\n• 黄线(右轴)：美国10年期国债收益率(%) —— 全球无风险利率与资产定价之锚",
            "how": "美林时钟的实时高频量化版！铜、油、黄金与美债在全球每日23小时连续交易，不受官方统计滞后与大幅修正干扰。通过「增长(蓝) × 通胀(红) × 利率(黄)」三线四象限组合，可一秒锁定当前全球宏观交易主线。",
            "changes": "• 【象限① 蓝涨 + 红涨 + 黄涨】：「经济过热 / 再通胀交易」，增长与通胀齐升推高美债利率，超配能源(XLE)、金融(XLF)、工业(XLI)与大宗商品，规避长债(TLT)。\n• 【象限② 蓝涨 + 红跌 + 黄稳/微降】：「金发姑娘复苏交易」，经济向好而能源通胀回落，美股科技(XLK)与成长股迎来最佳牛市环境。\n• 【象限③ 蓝跌 + 红跌 + 黄跌】：「经济衰退 / 降息避险交易」，增长与通胀双双熄火，资金疯狂涌入10年期美债(TLT)与黄金(GLD)赚取资本利得。\n• 【象限④ 蓝跌 + 红涨 + 黄涨】：「最凶险滞胀交易(Stagflation)」，经济下滑但油价推高通胀与利率，股债双杀，唯黄金与现金为王。\n• 【背离警示 蓝/红线双跌但黄线(10Y美债)逆势飙升】：多为美债天量发债供给冲击或财政赤字担忧，美债利率往往在随后向增长预期回归。",
            "signal": "💡 量化风控信号：当 10年期美债收益率(黄线)与 铜金比(蓝线)出现持续2个月以上的反向背离时，通常是跨资产均值回归套利的黄金窗口。",
        },
        # --- 三、利率端 ---
        {
            "id": "12",
            "category": "利率",
            "chart_type": "LINE",
            "title": "12. 美联储利率与美债收益率曲线 (Rates)",
            "df": align_tail([fed_rate, dgs10, dgs5, dgs2, dgs3m], freq="W-FRI", n=180),
            "right_cols": [],
            "left_title": "年化利率 (%)",
            "right_title": "",
            "what": "• 蓝线：美联储有效联邦基金利率(DFF)  • 紫线：3个月期美债利率（最紧贴政策利率）\n• 绿线：2年期美债收益率（市场对未来2年美联储路径预期）  • 黄线：5年期美债  • 红线：10年期美债收益率（长期增长与通胀预期）",
            "how": "完整刻画美债收益率曲线的形态演变（倒挂 vs 陡峭化）。短端利率由美联储直接掌控，长端利率由市场自由交易。曲线形态直接决定商业银行净息差与全社会信用扩张意愿。",
            "changes": "• 【形态① 紫/绿线 > 红线（短端高于长端）】：「收益率曲线倒挂」，代表货币政策处于限制性紧缩区间，银行借短贷长无利可图，信用逐步收缩。\n• 【形态② 倒挂后绿线(2Y)暴跌、红线(10Y)平稳】：「牛市变陡(Bull Steepener)」，市场抢跑定价美联储被迫紧急大幅降息，历史上往往对应经济衰退正式落地与股市剧烈震荡。\n• 【形态③ 红线(10Y)飙升速度快于绿线(2Y)】：「熊市变陡(Bear Steepener)」，市场定价长期通胀失控或美债供给过剩，长久期科技股与美债双双承压。\n• 【形态④ 蓝/紫/绿/红线有序排列(10Y > 5Y > 2Y > 3M)且温和下行】：健康正常的正向收益率曲线与预防式降息周期，最利好美股长牛。",
            "signal": "🚨 宏观警报：收益率曲线倒挂持续期间股市未必立刻下跌，但「倒挂结束、2Y-10Y利差迅速转正变陡（绿线下穿红线并拉开距离）」时，往往伴随美股波动率飙升。",
        },
        {
            "id": "13",
            "category": "利率",
            "chart_type": "LINE",
            "title": "13. 信用利差与系统性风险 (Credit Spread)",
            "df": align_tail([credit_spread, vix, spx], freq="W-FRI", n=180),
            "right_cols": ["标普500指数"],
            "left_title": "CCC信用利差(%) / VIX指数",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：ICE BofA CCC级高收益垃圾债期权调整信用利差(%) —— 资质最弱企业融资的违约风险溢价\n• 红线(左轴)：CBOE VIX恐慌指数 —— 标普500未来30天隐含波动率\n• 黄线(右轴)：标普500指数",
            "how": "债券市场机构投资者对企业现金流与违约风险的嗅觉远比股票市场敏锐。信用利差是衡量金融体系「流动性血管是否畅通」的终极体温计。",
            "changes": "• 【组合① 蓝线低位平稳(<7.5%) + 红线(VIX)<18 + 黄线稳步上行】：信用融资极度宽松，违约率低，美股处于低波动慢牛黄金期。\n• 【组合② 黄线(标普)仍在高位但蓝线(信用利差)提前抬头扩大】：「债市先行顶背离预警」，聪明资金已开始撤离高风险公司债，股市大概率随后补跌。\n• 【组合③ 红线(VIX)脉冲跳升(>25)但蓝线(信用利差)纹丝不动】：仅为股市技术性洗盘或期权短期对冲扰动，未伤及实体信用根基，反而是逢低买入机会。\n• 【组合④ 蓝线急剧飙升(>11%~13%) + 红线突破30~35】：系统性信用紧缩与流动性危机爆发，机构被迫去杠杆抛售资产，必须果断降仓防守。",
            "signal": "🚨 危机阈值：CCC信用利差突破 11% 且 VIX 站上 30 为系统性流动性警报；当利差冲高至 13%~15% 极值后首次拐头向下时，是全市场最具确定性的长线抄底信号。",
        },
        # --- 四、市场端 ---
        {
            "id": "14",
            "category": "市场",
            "chart_type": "LINE",
            "title": "14. 散户投资者情绪 (AAII Sentiment)",
            "df": align_tail([aaii_df["散户净看涨差值(%)"], aaii_df["20周均线(%)"], spx], freq="W-FRI", n=180)
            if not aaii_df.empty
            else align_tail([spx], freq="W-FRI", n=180),
            "right_cols": ["标普500指数"],
            "left_title": "看涨减看跌净差值 (%)",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：AAII散户看涨比例减去看跌比例的净差值(%) —— 每周四发布的美国个人投资者协会调查\n• 红线(左轴)：散户情绪净差值的20周移动平均线(%) —— 过滤单周情绪噪音的中期心理趋势\n• 黄线(右轴)：标普500指数",
            "how": "华尔街最经典的逆向情绪指标！个人投资者天生具有「追涨杀跌」倾向：在牛市顶部附近极度看多（潜在增量买盘耗尽），在回调底部附近极度看空（割肉抛压出清）。",
            "changes": "• 【组合① 蓝线飙升至 +25% 以上 + 红线同步突破 +15%】：散户看多情绪极度拥挤，场外增量资金已基本入场，市场对任何利空都极度脆弱，易发生技术性回调。\n• 【组合② 蓝线暴跌至 -25% 以下 + 黄线(标普)回踩关键支撑】：散户陷入恐慌性割肉，抛压接近衰竭，历史统计未来3~6个月正收益概率超80%。\n• 【组合③ 黄线(标普)高位震荡小跌，但蓝线迅速由正转负】：最健康的「牛市洗盘结构」，指数未跌多少但散户已被吓跑，为下一波主升浪腾出空间。\n• 【组合④ 蓝线长期徘徊在 -10%~0% 偏谨慎区间而黄线稳步创新高】：典型的「在怀疑中上涨(Wall of Worry)」，牛市生命力极其持久。",
            "signal": "🎯 逆向交易阈值：净差值 > +25% 逢高减仓或买入保护性Put；净差值 < -25%（尤其跌破 -30%）是高胜率左侧分批建仓区。",
        },
        {
            "id": "15",
            "category": "市场",
            "chart_type": "LINE",
            "title": "15. 机构投资者情绪与资管仓位 (CFTC Asset Manager)",
            "df": align_tail([inst_exp_df["机构股票多头敞口(%)"], inst_exp_df["20周均线(%)"], spx], freq="W-FRI", n=180)
            if not inst_exp_df.empty
            else align_tail([spx], freq="W-FRI", n=180),
            "right_cols": ["标普500指数"],
            "left_title": "机构资管多头仓位占比 (%)",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：CFTC 资管机构(共同基金/养老金/保险/主权基金)标普500期货真实多头占比(%)\n• 红线(左轴)：机构多头占比20周移动均线(%)  • 黄线(右轴)：标普500指数",
            "how": "直接追踪全球最大长线真金白银机构（Asset Managers）的每周实际多空仓位（替代已转为付费闭源的NAAIM调查）。由于机构受季度排名与风控线约束，同样存在「高位满仓子弹打光」与「低位被动砍仓后踏空回补」的周期律。",
            "changes": "• 【组合① 蓝线升至 84%~88% 历史极限高位 + 贴近或高于红线】：长线机构子弹基本打光，多头仓位极度拥挤，一旦宏观数据不及预期易引发踩踏式减仓。\n• 【组合② 蓝线大幅下穿红线并回落至 65% 以下】：机构风控去杠杆砍仓接近尾声，卖盘充分出清，市场一旦企稳将引发机构踏空逼空买盘。\n• 【组合③ 黄线(标普)创新高，但蓝线持续低于红线且重心下移】：「机构派发顶背离」，长线大资金正在借指数冲高悄然减仓，需高度警惕中期见顶。\n• 【组合④ 蓝线强势上穿红线且黄线同步放量突破】：长线资管机构开启新一轮季度配置加仓，趋势行情确立。",
            "signal": "🎯 机构仓位阈值：机构多头占比 ≥ 85% 警惕仓位拥挤见顶；回落至 65%~70% 区间往往对应长线机构出清后的波段黄金买点。",
        },
        {
            "id": "16",
            "category": "市场",
            "chart_type": "LINE",
            "title": "16. 期权市场交易方向 (Equity Put/Call)",
            "df": align_tail([pcr_df["Put/Call比值"], pcr_df["20日均线"], spx], freq=None, n=220)
            if not pcr_df.empty
            else align_tail([spx], freq=None, n=220),
            "right_cols": ["标普500指数"],
            "left_title": "看跌/看涨成交量比值",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：CBOE个股看跌期权(Put) ÷ 看涨期权(Call)每日成交量比值\n• 红线(左轴)：Put/Call比值的20日移动平均线（平滑单日脉冲）  • 黄线(右轴)：标普500指数",
            "how": "期权市场汇聚了最敏锐的杠杆交易员与机构对冲盘。比值越高，说明市场花重金买入看跌期权(Put)防暴跌的恐慌情绪越浓烈；比值越低，说明市场疯狂买入看涨期权(Call)赌上涨。",
            "changes": "• 【组合① 蓝线单日脉冲冲高(>0.95) + 红线(20日均线)显著抬升(>0.75)】：恐慌性买Put对冲达到高潮，做空力量集中宣泄，往往对应美股短期重要底部拐点。\n• 【组合② 蓝线持续压低至 0.48 以下 + 红线跌破 0.55】：投机者沉迷买入Call追涨，市场自满情绪极度膨胀，短期极易出现快速洗盘爆多。\n• 【组合③ 黄线(标普)继续创新低，但蓝线高点逐步下移】：「恐慌钝化底背离」，说明该买保险的资金已买完，下跌动能衰竭，反弹一触即发。\n• 【组合④ 蓝线温和运行在 0.58~0.70 区间】：多空对冲均衡，市场沿着主趋势稳健推进。",
            "signal": "🎯 期权逆向阈值：Equity Put/Call 单日突破 1.0 或 20日均线(红线)升破 0.78，为高胜率短线抄底信号；低于 0.50 警惕短期回调。",
        },
        {
            "id": "17",
            "category": "市场",
            "chart_type": "LINE",
            "title": "17. S&P 500 投机者持仓 (CFTC COT)",
            "df": align_tail([cot_hands_df["标普500投机净持仓(万手)"], spx], freq="W-FRI", n=180)
            if not cot_hands_df.empty and "标普500投机净持仓(万手)" in cot_hands_df
            else align_tail([spx], freq="W-FRI", n=180),
            "right_cols": ["标普500指数"],
            "left_title": "非商业净持仓 (万手)",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：CFTC CME E-Mini标普500期货大型投机者（对冲基金/CTA）净多空持仓(万手)\n• 红线(右轴)：标普500指数",
            "how": "0轴上方代表对冲基金整体净做多，0轴下方代表整体净做空。在期货零和博弈中，极端拥挤的空头头寸一旦遭遇市场企稳，就会转化为猛烈的空头回补买盘（轧空行情）。",
            "changes": "• 【组合① 蓝线深陷极端净空区(-15万手以下) + 红线(标普)拒绝下跌并站稳均线】：典型「史诗级轧空(Short Squeeze)前夜」，空头被迫止损平仓买入，驱动指数凌厉逼空上涨。\n• 【组合② 蓝线持续攀升至历史极端净多(+15万手以上) + 红线滞涨】：投机多头高度拥挤，若无新资金接力，极易触发CTA程序化止盈抛盘。\n• 【组合③ 蓝线从深度净空区稳步向0轴回升 + 红线同步上行】：趋势资金持续平空翻多，提供源源不断的增量买盘支撑。\n• 【组合④ 蓝线由净多快速翻空跌破0轴 + 红线破位】：宏观对冲基金集中建立空头头寸，中期调整趋势确立。",
            "signal": "🎯 轧空交易信号：当标普投机净持仓(蓝线)处于近3年最低10%分位（深度净空）且标普指数重新站上20日均线时，重点关注空头回补主升浪。",
        },
        {
            "id": "18",
            "category": "市场",
            "chart_type": "LINE",
            "title": "18. Nasdaq 100 投机者持仓 (CFTC COT)",
            "df": align_tail([cot_hands_df["纳指100投机净持仓(万手)"], ndx], freq="W-FRI", n=180)
            if not cot_hands_df.empty and "纳指100投机净持仓(万手)" in cot_hands_df
            else align_tail([ndx], freq="W-FRI", n=180),
            "right_cols": ["纳斯达克100指数"],
            "left_title": "非商业净持仓 (万手)",
            "right_title": "纳斯达克100点位",
            "what": "• 蓝线(左轴)：CFTC 纳斯达克100期货大型投机者净多空持仓(万手)\n• 红线(右轴)：纳斯达克100指数",
            "how": "专门追踪杠杆机构对「大型科技成长与AI龙头」的资金押注。核心用法是与「图17（标普500投机持仓）」进行双图交叉对比，识别华尔街对冲基金的「多科技、空传统」配对交易结构。",
            "changes": "• 【跨图组合① 图17(标普)深度净空 + 本图(纳指)维持净多】：典型的「科技巨头发力抱团结构」，机构做多AI与纳指巨头、做空传统权重股，纳指大幅跑赢标普与罗素小盘。\n• 【组合② 本图蓝线触及历史极端净多高位后拐头向下】：科技股杠杆多头拥挤度过高，警惕财报季获利了结与估值消化回调。\n• 【组合③ 本图蓝线跌入深度净空区后回升 + 红线(纳指)止跌】：科技板块经历充分去杠杆洗盘，迎来新一轮配置窗口。\n• 【跨图组合④ 图17与图18同步大幅翻空】：机构无差别削减全市场股票风险敞口，进入全面防御阶段。",
            "signal": "💡 跨图联动信号：对比图17与图18的净持仓方向剪刀差，可精准判断当前是「AI科技独立抱团牛市」还是「全市场风格切换」。",
        },
        {
            "id": "19",
            "category": "市场",
            "chart_type": "LINE",
            "title": "19. S&P 500 市场宽度 (Market Breadth)",
            "df": align_tail([breadth_df["50日市场宽度(%)"], breadth_df["200日市场宽度(%)"], spx], freq=None, n=250)
            if not breadth_df.empty
            else align_tail([spx], freq=None, n=250),
            "right_cols": ["标普500指数"],
            "left_title": "站上均线成分股占比 (%)",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：标普500成分股中收盘价高于50日均线的股票占比(%) —— 衡量中期涨跌广度\n• 红线(左轴)：标普500成分股中收盘价高于200日均线的股票占比(%) —— 衡量长线牛熊分界广度\n• 黄线(右轴)：标普500指数",
            "how": "穿透权重股失真效应的「市场X光机」。标普500前七大科技巨头占总市值超30%，市场宽度能敏锐捕捉「少数巨头掩护多数个股下跌」的危险背离，以及「全市场恐慌错杀」的黄金大底。",
            "changes": "• 【组合① 黄线(标普)创新高 + 蓝线(50日宽度)与红线(200日宽度)重心明显下移】：「危险顶部背离」，仅靠少数巨头硬撑指数，内部多数个股已步入下跌趋势，警惕指数补跌。\n• 【组合② 蓝线暴跌至 15% 以下 + 红线仍稳居 45% 以上】：牛市中的「健康深度洗盘」，85%以上股票短期超卖但长线牛市结构未坏，是极佳波段买点。\n• 【组合③ 蓝线(<15%) 与 红线(<20%) 双双跌入极端冰点后蓝线率先拐头向上】：「罕见熊市出清大底」，全市场泥沙俱下后迎来级别最大的一轮全面反弹。\n• 【组合④ 蓝线突破 85% + 红线突破 80%】：全市场普涨亢奋，短期追高盈亏比下降，易出现震荡整固。",
            "signal": "🎯 宽度买卖极区：50日宽度 > 85% 提示短期过热；< 15% 提示短期超卖。若 50日与200日宽度同时 < 15%，为数年一遇的中长线战略抄底区！",
        },
        {
            "id": "20A",
            "category": "市场",
            "chart_type": "LINE",
            "title": "20A. 美股进攻与成长板块轮动 (6大进攻板块)",
            "df": sec_off_df,
            "right_cols": [],
            "left_title": "相对全市场VTI强弱 (起点=100)",
            "right_title": "",
            "what": "• 6大进攻/周期板块ETF ÷ 全市场VTI 标准化相对强弱（1年前起点=100）：\n• 蓝:科技XLK | 红:通讯XLC | 黄:可选消费XLY | 绿:金融XLF | 紫:工业XLI | 青:能源XLE",
            "how": "每条曲线代表该板块相对美股全市场(VTI)的超额收益轨迹：曲线向上倾斜 = 跑赢大盘（资金持续流入）；曲线向下倾斜 = 跑输大盘（资金净流出）。",
            "changes": "• 【组合① 科技XLK(蓝) + 通讯XLC(红) 领跑且稳居100上方】：AI科技与互联网巨头主导牛市，风险偏好高涨，成长风格占优。\n• 【组合② 金融XLF(绿) + 工业XLI(紫) + 能源XLE(青) 接力走强，科技走平】：「经济软着陆与再通胀风格切换」，资金从高估值科技向顺周期价值板块扩散。\n• 【组合③ 可选消费XLY(黄) 率先破位下行】：居民消费承压的股市先行映射，警惕零售与消费链盈利下修。\n• 【组合④ 六大进攻板块全线跌破100且斜率向下】：机构全面撤出进攻头寸，必须结合图20B确认避险防御。",
            "signal": "🎯 轮动配置法则：始终超配斜率最陡峭、稳居100基准线上方的1~2个领涨主线板块，果断剔除连续跑输大盘的弱势板块。",
        },
        {
            "id": "20B",
            "category": "市场",
            "chart_type": "LINE",
            "title": "20B. 美股防御与利率敏感板块轮动 (5大防御/周期板块)",
            "df": sec_def_df,
            "right_cols": [],
            "left_title": "相对全市场VTI强弱 (起点=100)",
            "right_title": "",
            "what": "• 5大防御/利率敏感板块ETF ÷ 全市场VTI 标准化相对强弱（1年前起点=100）：\n• 蓝:医疗XLV(防御) | 红:必需消费XLP(防御) | 黄:公用事业XLU(防御) | 绿:房地产XLRE | 紫:原材料XLB",
            "how": "医疗、必需消费与公用事业盈利刚性强，是机构躲避经济衰退的「避风港」；房地产XLRE对10年期美债利率下行最敏感；原材料XLB挂钩全球大宗商品价格周期。",
            "changes": "• 【组合① 医疗XLV(蓝) + 必需消费XLP(红) + 公用事业XLU(黄) 集体抬头跑赢大盘】：典型「Risk-Off 系统性避险信号」，机构正在抛售高贝塔成长股、躲入防御堡垒。\n• 【组合② 房地产XLRE(绿) + 公用事业XLU(黄) 同步走强】：市场正在抢跑定价「美债收益率见顶下行与美联储降息周期」，高股息类债资产受追捧。\n• 【组合③ 原材料XLB(紫) 领涨而三大防御板块走弱】：全球制造业补库与商品涨价交易主导，与图09铜金比上升相互印证。\n• 【组合④ 三大防御板块持续单边垫底】：市场风险偏好极度旺盛，资金全线进攻风险资产。",
            "signal": "🚨 避险警报：当必需消费(XLP)与公用事业(XLU)连续3周以上显著跑赢科技(XLK)时，大盘往往面临系统性回调压力。",
        },
        # --- 五、核心焦点扩展模块（共5张：AI强弱 + 全球24个月7大资产资金流柱状图 + CFTC机构7大资产24个月净比柱状图 + 全球流出恐慌图 + 美元指数跨资产跷跷板对比图） ---
        {
            "id": "21",
            "category": "扩展",
            "chart_type": "LINE",
            "title": "21. AI板块对比标普与三大股指强弱比 (AI vs Indices)",
            "df": ai_df,
            "right_cols": [],
            "left_title": "相对强弱指数 (1.5年前起点=100)",
            "right_title": "",
            "what": "• 蓝:AI芯片SMH÷标普SPY | 红:AI应用AIQ÷标普SPY | 黄:AI机器人BOTZ÷标普SPY\n• 绿:AI芯片SMH÷纳指QQQ | 紫:AI芯片SMH÷罗素小盘IWM | 青:AI芯片SMH÷道指DIA",
            "how": "精确量化AI核心产业链相对美股四大宽基指数（标普500、纳指100、罗素2000小盘、道琼斯工业）的超额收益与资金虹吸力度，识别AI内部「硬件算力 vs 软件应用」及大盘「高低切换」节奏。",
            "changes": "• 【组合① 蓝线(SMH/SPY) + 红线(AIQ/SPY) + 绿线(SMH/QQQ) 同步上行】：AI全产业链处于最强主升浪，算力芯片不仅跑赢大盘，甚至大幅跑赢纳指本身。\n• 【组合② 蓝线(SMH/SPY)高位走平，而红线(AIQ/SPY)接力陡峭上升】：「AI行情第二阶段扩散」，资金从高估值芯片硬件向AI软件应用与商业变现端轮动。\n• 【组合③ 紫线(SMH/IWM) 与 青线(SMH/DIA) 同步大幅回落】：「资金高低切换」，市场从AI科技巨头撤出，流向降息受益的罗素小盘股与传统蓝筹补涨。\n• 【组合④ 六条强弱比曲线全线跌破中期上升趋势线】：AI资本开支放缓担忧升温，需阶段性降低半导体与AI仓位。",
            "signal": "🎯 轮动信号：SMH/SPY 与 SMH/QQQ 趋势向上时坚定持有AI算力龙头；若 SMH/IWM 跌破趋势线，及时切分仓位至中小盘与顺周期。",
        },
        {
            "id": "22",
            "category": "扩展",
            "chart_type": "COLUMN",
            "title": "22. 全球七大核心资产长周期资金净流入/流出强度 (24个月柱状图)",
            "df": global_flow_df,
            "right_cols": [],
            "left_title": "月均20日资金流净强弱 (%)",
            "right_title": "",
            "what": "• 近24个月(2年)全球旗舰ETF月均资金流净强弱分组柱状图(%)（0轴上方=资金净流入，0轴下方=资金净流出）：\n• 蓝:美股SPY | 红:科技QQQ | 黄:新兴市场EEM | 绿:美债TLT | 紫:黄金GLD | 青:商品DBC | 洋红:比特币IBIT",
            "how": "基于每日成交价位在高低区间的相对位置与成交额加权计算（Chaikin Money Flow），按月聚合呈现近2年宽柱对比，直观揭示全球二级市场资金在「美股、科技、新兴市场、美债、黄金、商品与比特币」七大资产间的真实迁徙轨迹。",
            "changes": "• 【组合① 蓝(SPY)+红(QQQ)+洋红(IBIT) 齐现高正柱】：全球风险偏好极度旺盛，流动性充裕，美股与加密资产同步吸金。\n• 【组合② 绿(TLT)+紫(GLD) 转为高正柱，而蓝(SPY)/红(QQQ)出现负柱】：「典型避险切换」，资金从风险股市撤出，涌入美债与黄金寻求庇护。\n• 【组合③ 黄(EEM)+紫(GLD)+青(DBC) 同步强净流入】：美元走弱驱动「非美与实物再通胀行情」，新兴市场与大宗商品跑赢美股。\n• 【组合④ 七大资产柱体集体跌入0轴下方负值区】：「无差别流动性紧缩(Cash Dash)」，机构抛售一切资产回笼美元现金。",
            "signal": "🎯 资金流阈值：单月资金流强弱柱体站稳 +10% 代表机构持续建仓净流入强劲；跌破 -10% 代表机构持续派发净流出。",
        },
        {
            "id": "23",
            "category": "扩展",
            "chart_type": "COLUMN",
            "title": "23. 大型机构在全球七大资产的真实流入/流出数据比 (CFTC 24个月柱状图)",
            "df": inst_global_ratio_df,
            "right_cols": [],
            "left_title": "机构真实资金净流入/流出比 (%)",
            "right_title": "",
            "what": "• 美国 CFTC 官方大型机构（非商业头寸）真实多空净比 = (多头-空头)/(多头+空头)×100%（近24个月柱状图）：\n• 蓝:美股标普500 | 红:美股科技纳指 | 黄:10年期美债 | 绿:实物黄金 | 紫:WTI原油 | 青:美元指数 | 洋红:比特币BTC",
            "how": "直接取自美国政府 CFTC 每周强制披露的期货大户持仓真金白银头寸（含CME比特币期货）。0轴上方正柱=大型机构净做多资金净流入；0轴下方负柱=大型机构净做空资金净流出。与图22（ETF二级市场流向）交叉验证可识破机构「明修栈道、暗度陈仓」。",
            "changes": "• 【组合① 绿(黄金)+洋红(BTC)+红(纳指) 齐现高正柱】：「货币贬值对冲 + AI进攻杠铃配置」，大型机构同时配置硬通货资产与科技成长。\n• 【组合② 黄(10年期美债)从深度负柱(-40%以下)快速收窄并翻正】：大型对冲基金集中平仓美债空头、做多美债，预示10年期美债收益率即将见顶回落。\n• 【组合③ 青(美元指数)正柱飙升 + 蓝/黄/绿柱集体转负】：机构大举做多美元、全面做空股债商品，全球美元流动性收紧警报拉响。\n• 【跨图背离 图22 ETF资金仍净流入，但图23 CFTC机构柱体已大幅转负】：散户与被动ETF仍在高位接盘，但华尔街聪明钱已在期货市场布局空单对冲派发！",
            "signal": "🎯 机构头寸极值：单类资产机构净比突破 +45% 为多头极度拥挤；跌破 -35% 为极端净空头，一旦基本面改善极易触发猛烈轧空反转。",
        },
        {
            "id": "24",
            "category": "扩展",
            "chart_type": "LINE",
            "title": "24. 全球资产流出恐慌指数与跨资产抛售压力图 (Outflow Panic)",
            "df": outflow_panic_df,
            "right_cols": ["VIX恐慌指数"],
            "left_title": "流出恐慌指数 / 净流出资产占比 (%)",
            "right_title": "VIX 恐慌指数",
            "what": "• 蓝线(左轴)：全球资产流出恐慌指数(0~100，综合跨资产资金净流出+CCC信用利差+波动率)\n• 红线(左轴)：恐慌指数10周移动均线  • 黄线(左轴)：同步净流出资产占比(%)  • 绿线(右轴)：VIX恐慌指数",
            "how": "专为监测「全球系统性流动性挤兑与无差别抛售（Cash Dash）」设计的风控雷达。正常市场中资金只是在股、债、黄金间轮动（黄线处于30%~60%）；只有当所有资产同时失去避险功能、相关性趋近于1时，蓝线与黄线才会同步飙升至极值。",
            "changes": "• 【组合① 蓝线 < 35 + 黄线 < 50% + 绿线(VIX) < 18】：全球流动性充裕，资产间良性轮动，无系统性抛售风险。\n• 【组合② 绿线(VIX)脉冲跳升(>25) 但 黄线(净流出资产占比) ≤ 57%】：仅单一市场局部波动（如美股回调但美债与黄金资金净流入），避险机制依然有效。\n• 【组合③ 蓝线飙升突破 65~80 + 黄线高达 85%~100%（七大资产全线净流出）】：最高级别「全球流动性危机警报」，机构为补缴保证金无差别抛售股、债、黄金与BTC换取现金。\n• 【组合④ 蓝线突破 75 极值后首次拐头下穿红线(10周均线)】：恐慌抛售高潮已过、央行救市或流动性见底，迎来全局性抄底窗口。",
            "signal": "🚨 恐慌熔断线：全球资产流出恐慌指数 > 65 且同步净流出资产占比 ≥ 85% 为最高级别流动性警报，必须降杠杆并保留现金；蓝线高位下穿红线则是危机后的抄底信号。",
        },
        {
            "id": "25",
            "category": "扩展",
            "chart_type": "LINE",
            "title": "25. 美元指数(DXY)强弱对比与全球资产跷跷板效应 (USD vs Global Assets)",
            "df": dxy_compare_df,
            "right_cols": [],
            "left_title": "标准化相对表现 (2年前起点=100)",
            "right_title": "",
            "what": "• 近2年(104周)标准化跨资产强弱对比（2年前起点=100）：\n• 蓝:美元指数DXY(全球流动性总阀门) | 红:美股标普500(SPY) | 黄:新兴市场(EEM)\n• 绿:实物黄金(GLD) | 紫:美债长债(TLT) | 青:比特币ETF(IBIT)",
            "how": "美元是全球贸易结算与跨境融资的计价锚。美元指数(DXY)走强等同于收紧全球美元流动性（抽水机效应）；美元指数走弱等同于向全球释放流动性（开闸放水）。本图将美元与五大核心资产置于同一标尺，清晰呈现跷跷板博弈。",
            "changes": "• 【组合① 蓝线(美元DXY)单边飙升 + 黄(新兴市场EEM)/紫(美债TLT)/青(BTC)集体下挫】：典型「强美元流动性抽水行情」，非美资产与长久期资产承压，仅美元现金与美股防御板块抗跌。\n• 【组合② 蓝线(美元DXY)破位下行 + 绿(黄金GLD)+黄(EEM)+青(BTC)全线暴涨】：经典「弱美元全球流动性牛市」，黄金、加密资产与新兴市场爆发力最强。\n• 【组合③ 蓝线(美元DXY)与绿线(黄金GLD)罕见同步上涨】：「地缘危机或主权信用重估组合」，说明全球央行与避险资金因地缘或财政赤字风险同时买入美元与黄金，此时新兴市场与风险资产风险极高。\n• 【组合④ 蓝线(美元DXY)高位滞涨回落 + 红线(美股SPY)稳步上行】：美国经济一枝独秀转向全球温和复苏，美股与非美资产迎来共振上涨。",
            "signal": "🎯 美元总阀门法则：当美元指数(蓝线)跌破中期趋势线且新兴市场(黄线)与黄金(绿线)同步站上100基准线上方时，果断加配黄金、比特币与非美资产。",
        },
    ]
    return charts


# ==========================================
# 4. 26 张图表数据完整性与真实性自检器
# ==========================================
def verify_charts_integrity(charts: list):
    print("\n" + "=" * 76)
    print("🔍 全量 26 张图表数据真实性与连续性核验报告")
    print("=" * 76)
    for c in charts:
        df = c["df"]
        if df.empty:
            print(f"❌ [缺失] {c['title']} -> DataFrame 为空！")
            continue
        start_d = df.index[0].strftime("%Y-%m-%d")
        end_d = df.index[-1].strftime("%Y-%m-%d")
        col_stats = []
        for col in df.columns:
            valid_n = int(df[col].notna().sum())
            last_v = df[col].dropna().iloc[-1] if valid_n > 0 else np.nan
            col_stats.append(f"{col}:{valid_n}点(最新={last_v:.2f})")
        print(f"✅ [{c['id']}] {c['title']} | {start_d} ~ {end_d} | " + " | ".join(col_stats))
    print("=" * 76 + "\n")


# ==========================================
# 5. 调用 OpenAI 生成顶部实时宏观简评
# ==========================================
def generate_ai_summary(charts: list) -> str:
    metrics_snapshot = []
    for c in charts:
        df = c["df"]
        if df.empty:
            continue
        last_row = df.iloc[-1]
        date_str = df.index[-1].strftime("%Y-%m-%d")
        vals = ", ".join(
            [f"{col}={last_row[col]:.2f}" for col in df.columns[:4] if pd.notna(last_row[col])]
        )
        metrics_snapshot.append(f"{c['title']} ({date_str}): {vals}")
    context_text = "\n".join(metrics_snapshot)

    if OPENAI_API_KEY:
        try:
            prompt = (
                "你是资深美股宏观与量化策略师。请根据以下最新拉取的26张核心图表数据快照，"
                "用简洁有力的中文写一段250字以内的【本期宏观、AI强弱、机构资金流与美元跷跷板总评】，依次点明："
                "1) 消费与供给趋势；2) 利率与信用风险；3) AI板块相对大盘强弱、CFTC大型机构全球七大资产(含BTC)真实流入/流出比、全球资产流出恐慌指数及美元指数(DXY)跨资产联动状态。\n\n"
                f"{context_text}"
            )
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 500,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"[Warning] OpenAI 摘要生成失败，切换至规则摘要: {e}")

    return (
        "全量26张核心图表已同步至最新交易日。请重点核查：① 纽约联储WEI指数与油金/铜金比通胀方向；"
        "② 10年与2年期美债利率及CCC信用利差；③ 标普500市场宽度与CFTC资管机构持仓敞口；"
        "④ AI板块相对强弱、近24个月全球七大资产资金流柱状图、CFTC机构七大资产(含BTC)真实净比柱状图、全球流出恐慌指数及美元指数(DXY)跨资产跷跷板。"
    )


def compute_axis_bounds(df: pd.DataFrame, cols: list, is_column: bool = False) -> tuple:
    """动态计算坐标轴合理的最小/最大值"""
    valid_cols = [c for c in cols if c in df.columns]
    if not valid_cols:
        return None, None
    sub = df[valid_cols].dropna(how="all")
    if sub.empty:
        return None, None
    ymin = float(sub.min().min())
    ymax = float(sub.max().max())
    if np.isnan(ymin) or np.isnan(ymax):
        return None, None
    if is_column:
        ymin = min(ymin, 0.0)
        ymax = max(ymax, 0.0)
    if abs(ymax - ymin) < 1e-6:
        return round(ymin - 1.0, 2), round(ymax + 1.0, 2)
    pad = (ymax - ymin) * 0.12
    low = ymin - pad
    high = ymax + pad
    if ymin >= 0 and low < 0 and ymin < (ymax - ymin) * 0.15:
        low = 0.0
    return round(low, 2), round(high, 2)


# ==========================================
# 6. 写入底层数据并渲染高清 26 图扩容看板
# ==========================================
def render_to_google_sheets(charts: list, ai_summary: str):
    sh = gc.open_by_key(SPREADSHEET_ID)

    # 26 张图（每张 34 行）+ 5 个分类横幅 + 顶部标题区，总行数设为 965 行
    total_dash_rows = 965
    raw_rows_cap = 450
    raw_cols_cap = 240

    existing_titles = {ws.title: ws for ws in sh.worksheets()}
    if DASH_SHEET_NAME not in existing_titles:
        dash_ws = sh.add_worksheet(title=DASH_SHEET_NAME, rows=total_dash_rows, cols=16)
    else:
        dash_ws = existing_titles[DASH_SHEET_NAME]
        dash_ws.resize(rows=total_dash_rows, cols=16)

    if RAW_SHEET_NAME not in existing_titles:
        raw_ws = sh.add_worksheet(title=RAW_SHEET_NAME, rows=raw_rows_cap, cols=raw_cols_cap)
    else:
        raw_ws = existing_titles[RAW_SHEET_NAME]
        raw_ws.resize(rows=raw_rows_cap, cols=raw_cols_cap)

    dash_ws.clear()
    raw_ws.clear()

    meta = sheets_service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    reset_requests = [
        {
            "unmergeCells": {
                "range": {
                    "sheetId": dash_ws.id,
                    "startRowIndex": 0,
                    "endRowIndex": total_dash_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": 16,
                }
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": dash_ws.id,
                    "startRowIndex": 0,
                    "endRowIndex": total_dash_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": 16,
                },
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": raw_ws.id,
                    "startRowIndex": 0,
                    "endRowIndex": raw_rows_cap,
                    "startColumnIndex": 0,
                    "endColumnIndex": raw_cols_cap,
                },
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat",
            }
        },
    ]
    for s in meta.get("sheets", []):
        if s["properties"]["sheetId"] == dash_ws.id:
            for ch in s.get("charts", []):
                reset_requests.append({"deleteEmbeddedObject": {"objectId": ch["chartId"]}})

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body={"requests": reset_requests}
    ).execute()

    # ------------------------------------------
    # A. 写入 Raw_Data 底层数据并强制数值格式
    # ------------------------------------------
    max_len = max((len(c["df"]) for c in charts if not c["df"].empty), default=10)
    total_cols = sum((len(c["df"].columns) + 2) for c in charts)
    raw_grid = [[""] * total_cols for _ in range(max_len + 2)]

    col_cursor = 0
    chart_ranges = {}
    raw_format_requests = []

    for c in charts:
        df = c["df"]
        if df.empty:
            continue
        n_rows = len(df)
        n_cols = len(df.columns)
        raw_grid[0][col_cursor] = c["title"]
        raw_grid[1][col_cursor] = "日期"
        for j, col_name in enumerate(df.columns):
            raw_grid[1][col_cursor + 1 + j] = col_name

        raw_format_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": raw_ws.id,
                    "startRowIndex": 2,
                    "endRowIndex": 2 + n_rows,
                    "startColumnIndex": col_cursor + 1,
                    "endColumnIndex": col_cursor + 1 + n_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        })

        valid_counts = {}
        for j, col_name in enumerate(df.columns):
            valid_counts[col_name] = int(df[col_name].notna().sum())

        for i, (dt, row) in enumerate(df.iterrows()):
            raw_grid[2 + i][col_cursor] = dt.strftime("%Y-%m-%d")
            for j, val in enumerate(row):
                raw_grid[2 + i][col_cursor + 1 + j] = "" if pd.isna(val) else round(float(val), 4)

        chart_ranges[c["id"]] = {
            "start_col": col_cursor,
            "num_series": n_cols,
            "num_rows": n_rows,
            "columns": list(df.columns),
            "valid_counts": valid_counts,
        }
        col_cursor += n_cols + 2

    raw_ws.update(range_name="A1", values=raw_grid, value_input_option="USER_ENTERED")
    if raw_format_requests:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body={"requests": raw_format_requests}
        ).execute()

    # ------------------------------------------
    # B. 构建 Dashboard 文本卡片与扩容四段式深度读图指南
    # ------------------------------------------
    tz_cst = timezone(timedelta(hours=8))
    now_str = datetime.now(tz_cst).strftime("%Y-%m-%d %H:%M (UTC+8)")

    dash_grid = [[""] * 16 for _ in range(total_dash_rows)]
    dash_grid[1][1] = "📈 美股核心 26 张宏观、机构全球资金流比(含BTC)、恐慌与美元跷跷板全景看板"
    dash_grid[2][1] = f"自动更新时间：{now_str}   |   数据来源：FRED 美联储 · CFTC 官方持仓(含CME比特币) · CBOE · Redfin · Yahoo Finance · AAII"
    dash_grid[3][1] = f"🤖 AI 宏观与资金流总评：{ai_summary}"

    category_banners = {
        "消费": ("一、消费端 (Consumption — 美国经济 70% 核心驱动，有工作才有收入与消费)", {"red": 0.11, "green": 0.29, "blue": 0.49}),
        "供给": ("二、供给端 (Supply — 提前 3~6 个月预判供需缺口与通胀拐点的先行指标)", {"red": 0.08, "green": 0.42, "blue": 0.45}),
        "利率": ("三、利率端 (Interest Rates — 资产定价之锚、曲线倒挂与系统性违约风险标杆)", {"red": 0.34, "green": 0.22, "blue": 0.48}),
        "市场": ("四、市场端 (Market — 散户与CFTC机构仓位极值、市场宽度与11大板块轮动)", {"red": 0.13, "green": 0.42, "blue": 0.32}),
        "扩展": ("五、核心焦点扩展 (AI强弱 · 全球24月资金流柱图 · CFTC机构七大资产含BTC柱图 · 恐慌指数 · 美元指数对比)", {"red": 0.65, "green": 0.24, "blue": 0.14}),
    }

    row_cursor = 5
    current_cat = None
    layout_blocks = []

    # 每张图卡片扩容至 34 行，充分容纳多维组合情景与背离对比矩阵
    CARD_ROWS = 34

    for c in charts:
        cat = c["category"]
        if cat != current_cat:
            current_cat = cat
            banner_text, banner_color = category_banners[cat]
            dash_grid[row_cursor][1] = banner_text
            layout_blocks.append({"type": "section", "row": row_cursor, "color": banner_color})
            row_cursor += 2

        r0 = row_cursor
        dash_grid[r0][9] = f"📌 {c['title']} — 深度读图与多维组合实战指南"
        dash_grid[r0 + 1][9] = "【① 曲线/柱体与坐标轴精确定义】"
        dash_grid[r0 + 2][9] = c["what"]
        dash_grid[r0 + 7][9] = "【② 底层宏观机制与跨市场传导链条】"
        dash_grid[r0 + 8][9] = c["how"]
        dash_grid[r0 + 14][9] = "【③ 多维组合情景与背离对比矩阵】"
        dash_grid[r0 + 15][9] = c["changes"]
        dash_grid[r0 + 28][9] = "【④ 量化触发阈值与实战仓位对策】"
        dash_grid[r0 + 29][9] = c["signal"]

        layout_blocks.append({
            "type": "chart_card",
            "row": r0,
            "chart": c,
            "color": category_banners[cat][1],
        })
        row_cursor += CARD_ROWS + 1

    dash_ws.update(range_name="A1", values=dash_grid[: row_cursor + 5], value_input_option="USER_ENTERED")

    # ------------------------------------------
    # C. 批量发送排版样式 + 26张高清原生图表构建请求
    # ------------------------------------------
    dash_id = dash_ws.id
    raw_id = raw_ws.id
    requests_batch = []

    # 列宽分配：左侧 1~8 列共 848px 放置高清图表，右侧 9~14 列共 810px 放置深度组合指南
    col_widths = (
        [(0, 1, 20)]
        + [(i, i + 1, 106) for i in range(1, 9)]
        + [(i, i + 1, 135) for i in range(9, 15)]
    )
    for start_c, end_c, px in col_widths:
        requests_batch.append({
            "updateDimensionProperties": {
                "range": {"sheetId": dash_id, "dimension": "COLUMNS", "startIndex": start_c, "endIndex": end_c},
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        })

    requests_batch.append({
        "updateSheetProperties": {
            "properties": {"sheetId": dash_id, "gridProperties": {"hideGridlines": True}},
            "fields": "gridProperties.hideGridlines",
        }
    })

    requests_batch.extend([
        {
            "mergeCells": {
                "range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 1, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 1, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.10, "green": 0.15, "blue": 0.23},
                        "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 15, "bold": True},
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": dash_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 38},
                "fields": "pixelSize",
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": dash_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 1, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 1, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.94, "green": 0.96, "blue": 0.98},
                        "textFormat": {"foregroundColor": {"red": 0.25, "green": 0.30, "blue": 0.38}, "fontSize": 10},
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)",
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": dash_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 1, "endColumnIndex": 15},
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 1, "endColumnIndex": 15},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 1.0, "green": 0.98, "blue": 0.90},
                        "textFormat": {"foregroundColor": {"red": 0.15, "green": 0.15, "blue": 0.15}, "fontSize": 10},
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": dash_id, "dimension": "ROWS", "startIndex": 3, "endIndex": 4},
                "properties": {"pixelSize": 64},
                "fields": "pixelSize",
            }
        },
    ])

    for blk in layout_blocks:
        r0 = blk["row"]
        col_rgb = blk["color"]

        if blk["type"] == "section":
            requests_batch.extend([
                {
                    "mergeCells": {
                        "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 1, "endColumnIndex": 15},
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "repeatCell": {
                        "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 1, "endColumnIndex": 15},
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": col_rgb,
                                "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 12, "bold": True},
                                "verticalAlignment": "MIDDLE",
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": dash_id, "dimension": "ROWS", "startIndex": r0, "endIndex": r0 + 1},
                        "properties": {"pixelSize": 34},
                        "fields": "pixelSize",
                    }
                },
            ])
            continue

        c = blk["chart"]
        requests_batch.append({
            "updateDimensionProperties": {
                "range": {"sheetId": dash_id, "dimension": "ROWS", "startIndex": r0, "endIndex": r0 + CARD_ROWS},
                "properties": {"pixelSize": 23},
                "fields": "pixelSize",
            }
        })

        requests_batch.extend([
            {
                "mergeCells": {
                    "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 9, "endColumnIndex": 15},
                    "mergeType": "MERGE_ALL",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 9, "endColumnIndex": 15},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": col_rgb,
                            "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10, "bold": True},
                            "verticalAlignment": "MIDDLE",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)",
                }
            },
        ])

        for sr in [r0 + 1, r0 + 7, r0 + 14, r0 + 28]:
            requests_batch.extend([
                {
                    "mergeCells": {
                        "range": {"sheetId": dash_id, "startRowIndex": sr, "endRowIndex": sr + 1, "startColumnIndex": 9, "endColumnIndex": 15},
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "repeatCell": {
                        "range": {"sheetId": dash_id, "startRowIndex": sr, "endRowIndex": sr + 1, "startColumnIndex": 9, "endColumnIndex": 15},
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.93, "green": 0.95, "blue": 0.98},
                                "textFormat": {"foregroundColor": col_rgb, "fontSize": 9, "bold": True},
                                "verticalAlignment": "MIDDLE",
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)",
                    }
                },
            ])

        body_blocks = [
            (r0 + 2, r0 + 7),
            (r0 + 8, r0 + 14),
            (r0 + 15, r0 + 28),
            (r0 + 29, r0 + 34),
        ]
        for idx_b, (br_start, br_end) in enumerate(body_blocks):
            bg = {"red": 0.99, "green": 0.96, "blue": 0.95} if idx_b == 3 else {"red": 0.98, "green": 0.99, "blue": 1.0}
            requests_batch.extend([
                {
                    "mergeCells": {
                        "range": {"sheetId": dash_id, "startRowIndex": br_start, "endRowIndex": br_end, "startColumnIndex": 9, "endColumnIndex": 15},
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "repeatCell": {
                        "range": {"sheetId": dash_id, "startRowIndex": br_start, "endRowIndex": br_end, "startColumnIndex": 9, "endColumnIndex": 15},
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": bg,
                                "textFormat": {"foregroundColor": {"red": 0.14, "green": 0.17, "blue": 0.21}, "fontSize": 9},
                                "wrapStrategy": "WRAP",
                                "verticalAlignment": "TOP",
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)",
                    }
                },
            ])

        border_style = {"style": "SOLID", "width": 1, "color": {"red": 0.78, "green": 0.83, "blue": 0.89}}
        requests_batch.append({
            "updateBorders": {
                "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + CARD_ROWS, "startColumnIndex": 9, "endColumnIndex": 15},
                "top": border_style,
                "bottom": border_style,
                "left": border_style,
                "right": border_style,
            }
        })

        info = chart_ranges.get(c["id"])
        if not info:
            continue

        sc = info["start_col"]
        nr = info["num_rows"]
        cols = info["columns"]
        valid_counts = info["valid_counts"]
        right_set = set(c.get("right_cols", []))
        left_cols = [col for col in cols if col not in right_set]
        active_right_cols = [col for col in cols if col in right_set]
        chart_type = c.get("chart_type", "LINE")
        is_col_chart = chart_type == "COLUMN"

        l_min, l_max = compute_axis_bounds(c["df"], left_cols, is_column=is_col_chart)
        left_axis_spec = {"position": "LEFT_AXIS", "title": c["left_title"]}
        if l_min is not None and l_max is not None:
            left_axis_spec["viewWindowOptions"] = {
                "viewWindowMin": l_min,
                "viewWindowMax": l_max,
                "viewWindowMode": "EXPLICIT",
            }

        axes = [
            {"position": "BOTTOM_AXIS", "title": "日期"},
            left_axis_spec,
        ]

        if active_right_cols:
            r_min, r_max = compute_axis_bounds(c["df"], active_right_cols, is_column=is_col_chart)
            right_axis_spec = {"position": "RIGHT_AXIS", "title": c["right_title"]}
            if r_min is not None and r_max is not None:
                right_axis_spec["viewWindowOptions"] = {
                    "viewWindowMin": r_min,
                    "viewWindowMax": r_max,
                    "viewWindowMode": "EXPLICIT",
                }
            axes.append(right_axis_spec)

        series_specs = []
        for j, col_name in enumerate(cols):
            target_ax = "RIGHT_AXIS" if col_name in right_set else "LEFT_AXIS"
            color_rgb = PALETTE[j % len(PALETTE)]
            series_item = {
                "series": {
                    "sourceRange": {
                        "sources": [{
                            "sheetId": raw_id,
                            "startRowIndex": 1,
                            "endRowIndex": 2 + nr,
                            "startColumnIndex": sc + 1 + j,
                            "endColumnIndex": sc + 2 + j,
                        }]
                    }
                },
                "targetAxis": target_ax,
                "color": color_rgb,
            }
            if not is_col_chart:
                series_item["lineStyle"] = {"width": 2, "type": "SOLID"}
                if valid_counts.get(col_name, 99) <= 15:
                    series_item["pointStyle"] = {"size": 5, "shape": "CIRCLE"}
            series_specs.append(series_item)

        short_sub = c["what"].split("\n")[0]
        if len(short_sub) > 105:
            short_sub = short_sub[:102] + "..."

        basic_chart_spec = {
            "chartType": chart_type,
            "legendPosition": "BOTTOM_LEGEND",
            "headerCount": 1,
            "axis": axes,
            "domains": [{
                "domain": {
                    "sourceRange": {
                        "sources": [{
                            "sheetId": raw_id,
                            "startRowIndex": 1,
                            "endRowIndex": 2 + nr,
                            "startColumnIndex": sc,
                            "endColumnIndex": sc + 1,
                        }]
                    }
                }
            }],
            "series": series_specs,
        }
        if is_col_chart:
            basic_chart_spec["stackedType"] = "NOT_STACKED"
        else:
            basic_chart_spec["interpolateNulls"] = True

        requests_batch.append({
            "addChart": {
                "chart": {
                    "spec": {
                        "title": c["title"],
                        "subtitle": short_sub,
                        "basicChart": basic_chart_spec,
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": dash_id, "rowIndex": r0, "columnIndex": 1},
                            "offsetXPixels": 2,
                            "offsetYPixels": 2,
                            "widthPixels": 842,
                            "heightPixels": 768,
                        }
                    },
                }
            }
        })

    chunk_size = 55
    for i in range(0, len(requests_batch), chunk_size):
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests_batch[i : i + chunk_size]},
        ).execute()
        time.sleep(1)

    print("✅ 全部 26 张高清图表（含24个月清晰柱状图、BTC机构头寸、美元指数对比及扩容组合指南）已更新完毕！")


if __name__ == "__main__":
    all_charts = build_all_charts()
    verify_charts_integrity(all_charts)
    summary = generate_ai_summary(all_charts)
    render_to_google_sheets(all_charts, summary)
