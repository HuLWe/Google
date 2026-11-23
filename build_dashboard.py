import io
import json
import os
import re
import time
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

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
# 2. 真实数据抓取与计算函数（全量容错与连续性保障）
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
            print(f"[Warning] FRED 指标 {series_id} 请求失败 ({r.status_code}): {r.text[:160]}")
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
    """
    修复图03蓝线成屋销售单点问题：
    FRED 的 EXHOSLUSM495S 因 NAR 限制仅保留近 13 个月导致同比只剩 1 个点。
    优先从 Redfin 官方公开全国房市追踪数据获取完整多年成屋成交同比序列，并与 FRED 最新值合并。
    """
    fred_s = yoy_pct(fetch_fred("EXHOSLUSM495S", start_date="2019-01-01"))
    redfin_s = pd.Series(dtype=float)
    try:
        url = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/us_national_market_tracker.tsv000.gz"
        r = requests.get(url, headers=HEADERS, timeout=45)
        if r.status_code == 200:
            df = pd.read_csv(io.BytesIO(r.content), sep="\t", compression="gzip", low_memory=False)
            cols_map = {c.upper(): c for c in df.columns}
            p_col = cols_map.get("PERIOD_END") or cols_map.get("PERIOD_BEGIN")
            t_col = cols_map.get("PROPERTY_TYPE")
            y_col = cols_map.get("HOMES_SOLD_YOY")
            if p_col and y_col:
                sub = df.copy()
                if t_col and "All Residential" in sub[t_col].values:
                    sub = sub[sub[t_col] == "All Residential"]
                sub[p_col] = pd.to_datetime(sub[p_col], errors="coerce")
                sub[y_col] = pd.to_numeric(sub[y_col], errors="coerce")
                sub = sub.dropna(subset=[p_col, y_col]).sort_values(p_col)
                s = sub.set_index(p_col)[y_col]
                s = s[~s.index.duplicated(keep="last")]
                if not s.empty and s.abs().max() <= 3.0:
                    s = s * 100.0
                redfin_s = s[s.index >= "2019-01-01"]
                print(f"[OK] Redfin 成屋销售同比拉取成功 ({len(redfin_s)} 个月, 最新: {redfin_s.index[-1].strftime('%Y-%m-%d')})")
    except Exception as e:
        print(f"[Info] Redfin 通道回退至 FRED: {e}")

    if not redfin_s.empty:
        # 统一对齐到月末时间戳，并用 FRED 最新一期值补充（若 FRED 更新更晚）
        r_m = redfin_s.resample("ME").last()
        if not fred_s.empty:
            f_m = fred_s.resample("ME").last()
            r_m = r_m.combine_first(f_m)
        return r_m.rename("成屋销售同比(%)")

    return fred_s.rename("成屋销售同比(%)")


def fetch_ism_pmi() -> tuple:
    """
    修复图07与图09黄线在 2024-02 截断问题：
    FRED 的 OECD 指标已停更，改从 DBnomics 官方公开接口获取完整 ISM 制造业 PMI 与非制造业 PMI（50为荣枯线）。
    """
    def _get_dbnomics_series(url: str, name: str) -> pd.Series:
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code != 200:
                return pd.Series(dtype=float, name=name)
            docs = r.json().get("series", {}).get("docs", [])
            if not docs:
                return pd.Series(dtype=float, name=name)
            periods = docs[0].get("period", [])
            values = docs[0].get("value", [])
            records = []
            for p, v in zip(periods, values):
                if v is not None and v != "NA":
                    dt = pd.to_datetime(p, errors="coerce")
                    if pd.notna(dt) and dt >= pd.Timestamp("2019-01-01"):
                        records.append((dt, float(v)))
            if not records:
                return pd.Series(dtype=float, name=name)
            s = pd.Series(
                [val for _, val in records],
                index=pd.DatetimeIndex([dt for dt, _ in records]),
                name=name,
            )
            s = s[~s.index.duplicated(keep="last")].sort_index()
            print(f"[OK] {name} 成功拉取 {len(s)} 个月数据 (最新: {s.index[-1].strftime('%Y-%m-%d')})")
            return s
        except Exception as e:
            print(f"[Warning] {name} 拉取异常: {e}")
            return pd.Series(dtype=float, name=name)

    mfg_pmi = _get_dbnomics_series(
        "https://api.db.nomics.world/v22/series/ISM/pmi/pm?observations=1",
        "ISM制造业PMI",
    )
    svc_pmi = _get_dbnomics_series(
        "https://api.db.nomics.world/v22/series/ISM/nm-pmi/pm?observations=1",
        "ISM非制造业PMI",
    )

    # 兜底通道：若 DBnomics 网络异常，回退至 FRED 制造业产能利用率
    if mfg_pmi.empty:
        mfg_pmi = fetch_fred("MCUMFN").rename("ISM制造业PMI")
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


def compute_rolling_flow_intensity(ohlcv: dict, ticker_labels: dict, window: int = 20, tail_weeks: int = 10) -> pd.DataFrame:
    """
    计算机构级 Chaikin Money Flow (CMF) 20日资金流净强弱指数 (%)
    输出最近 tail_weeks 周数据，适配分组柱状图清晰展示正流入与负流出
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

    weekly = flow_df.resample("W-FRI").last().dropna(how="all")
    return weekly.iloc[-tail_weeks:]


def _normalize_header_table(raw: pd.DataFrame, required_keywords: list) -> pd.DataFrame:
    """自动扫描前 10 行定位真实表头"""
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
                    print(f"[OK] AAII 网页通道成功拉取 {len(res)} 周数据 (最新: {res.index[-1].strftime('%Y-%m-%d')})")
                    return res
    except Exception as e:
        print(f"[Warning] AAII 数据全部通道抓取失败: {e}")
    return pd.DataFrame()


def _parse_naaim_df(raw: pd.DataFrame) -> pd.DataFrame:
    """统一清洗 NAAIM 数据表"""
    raw = _normalize_header_table(raw, ["date"])
    d_candidates = [c for c in raw.columns if "date" in c]
    d_col = d_candidates[0] if d_candidates else raw.columns[0]
    v_candidates = [c for c in raw.columns if "mean" in c or "number" in c or "exposure" in c]
    v_col = v_candidates[0] if v_candidates else raw.columns[1]
    df = raw[[d_col, v_col]].copy()
    df[d_col] = pd.to_datetime(df[d_col], errors="coerce")
    df[v_col] = pd.to_numeric(df[v_col].astype(str).str.replace("%", ""), errors="coerce")
    df = df.dropna().set_index(d_col).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    exp = df[v_col].rename("机构风险敞口(%)")
    ma20 = exp.rolling(20, min_periods=5).mean().rename("20周均线(%)")
    return pd.concat([exp, ma20], axis=1, sort=True).dropna()


def _try_download_naaim_file(file_url: str) -> pd.DataFrame:
    """尝试下载并解析单个 NAAIM Excel/CSV 链接"""
    try:
        fr = requests.get(file_url, headers=HEADERS, timeout=25)
        if fr.status_code != 200:
            return pd.DataFrame()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            if ".csv" in file_url.lower():
                raw = pd.read_csv(io.BytesIO(fr.content))
                return _parse_naaim_df(raw)
            for engine in ["openpyxl", "calamine", None]:
                try:
                    raw = pd.read_excel(io.BytesIO(fr.content), engine=engine)
                    res = _parse_naaim_df(raw)
                    if len(res) > 20:
                        return res
                except Exception:
                    continue
    except Exception:
        pass
    return pd.DataFrame()


def fetch_naaim_exposure() -> pd.DataFrame:
    """五通道容错抓取 NAAIM 机构投资经理风险敞口（WordPress REST API + 附件直链 + Datawrapper）"""
    # 通道 1: NAAIM WordPress Media REST API（直接获取最新上传的官方 Excel/CSV 媒体直链）
    for wp_api in [
        "https://www.naaim.org/wp-json/wp/v2/media?search=Exposure&per_page=15",
        "https://www.naaim.org/wp-json/wp/v2/media?search=NAAIM&per_page=15",
    ]:
        try:
            mr = requests.get(wp_api, headers=HEADERS, timeout=20)
            if mr.status_code == 200:
                items = mr.json()
                for item in items:
                    src = item.get("source_url", "")
                    if any(ext in src.lower() for ext in [".xlsx", ".xls", ".csv"]):
                        res = _try_download_naaim_file(src)
                        if len(res) > 20:
                            print(f"[OK] NAAIM WP-API通道成功拉取 {len(res)} 周数据 (最新: {res.index[-1].strftime('%Y-%m-%d')})")
                            return res
        except Exception:
            continue

    # 通道 2~5: 官网页面直链、Datawrapper、内嵌 JS 与 HTML 表格
    try:
        base_url = "https://www.naaim.org/programs/naaim-exposure-index/"
        page = requests.get(base_url, headers=HEADERS, timeout=30)
        page.raise_for_status()
        html = page.text

        all_links = re.findall(r'href=["\']([^"\']+\.(?:xlsx|xls|csv)(?:\?[^"\']*)?)["\']', html, flags=re.I)
        all_links = sorted(
            set(all_links),
            key=lambda u: (0 if any(k in u.lower() for k in ["naaim", "exposure", "uploads"]) else 1),
        )
        for link in all_links:
            res = _try_download_naaim_file(urljoin(base_url, link))
            if len(res) > 20:
                print(f"[OK] NAAIM 附件通道成功拉取 {len(res)} 周数据 (最新: {res.index[-1].strftime('%Y-%m-%d')})")
                return res

        dw_links = re.findall(r'https?://datawrapper\.dwcdn\.net/([A-Za-z0-9]+)/\d+/?', html)
        for chart_id in set(dw_links):
            for ver in ["1", "2", "3", "4"]:
                res = _try_download_naaim_file(f"https://datawrapper.dwcdn.net/{chart_id}/{ver}/dataset.csv")
                if len(res) > 20:
                    print(f"[OK] NAAIM Datawrapper通道成功拉取 {len(res)} 周数据 (最新: {res.index[-1].strftime('%Y-%m-%d')})")
                    return res

        js_pairs = re.findall(r'\[\s*["\'](\d{4}-\d{2}-\d{2})["\']\s*,\s*(-?\d+(?:\.\d+)?)\s*\]', html)
        if len(js_pairs) > 20:
            s = pd.Series(
                [float(v) for _, v in js_pairs],
                index=pd.to_datetime([d for d, _ in js_pairs]),
                name="机构风险敞口(%)",
            )
            s = s[~s.index.duplicated(keep="last")].sort_index()
            ma20 = s.rolling(20, min_periods=5).mean().rename("20周均线(%)")
            res = pd.concat([s, ma20], axis=1, sort=True).dropna()
            if len(res) > 20:
                print(f"[OK] NAAIM 内嵌图通道成功拉取 {len(res)} 周数据 (最新: {res.index[-1].strftime('%Y-%m-%d')})")
                return res
    except Exception as e:
        print(f"[Warning] NAAIM 数据抓取异常: {e}")

    print("[Warning] NAAIM 官网暂未返回可解析文件，图15将仅展示右轴标普500基准")
    return pd.DataFrame()


def _fetch_single_cboe_day(d_str: str):
    """请求单个交易日的 CBOE 官方市场统计 JSON"""
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


def fetch_cftc_cot() -> pd.DataFrame:
    """抓取 CFTC 标普500 与 纳斯达克100 投机者(非商业)净持仓（单位：万手）"""
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
            return pd.DataFrame()
        cot = pd.concat(frames, ignore_index=True)
        name_col = [c for c in cot.columns if "Market and Exchange Names" in c][0]
        date_col = [c for c in cot.columns if "As of Date in Form YYYY-MM-DD" in c][0]
        long_col = [c for c in cot.columns if "Noncommercial Positions-Long (All)" in c][0]
        short_col = [c for c in cot.columns if "Noncommercial Positions-Short (All)" in c][0]

        cot[date_col] = pd.to_datetime(cot[date_col])
        sp = cot[cot[name_col].str.contains("E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE", case=False, na=False)].copy()
        nq = cot[cot[name_col].str.contains("NASDAQ MINI|NASDAQ-100 Consolidated", case=False, na=False)].copy()

        sp_net = (
            sp.set_index(date_col)
            .apply(lambda x: (float(x[long_col]) - float(x[short_col])) / 10000.0, axis=1)
            .groupby(level=0)
            .last()
            .sort_index()
            .rename("标普500投机净持仓(万手)")
        )
        nq_net = (
            nq.set_index(date_col)
            .apply(lambda x: (float(x[long_col]) - float(x[short_col])) / 10000.0, axis=1)
            .groupby(level=0)
            .last()
            .sort_index()
            .rename("纳指100投机净持仓(万手)")
        )
        res = pd.concat([sp_net, nq_net], axis=1, sort=True)
        print(f"[OK] CFTC COT 持仓成功拉取 {len(res)} 周数据 (最新: {res.index[-1].strftime('%Y-%m-%d')})")
        return res
    except Exception as e:
        print(f"[Warning] CFTC COT 抓取异常: {e}")
        return pd.DataFrame()


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


def align_tail(series_list, freq="W-FRI", n=156, max_ffill_steps=3) -> pd.DataFrame:
    """
    时间序列对齐函数：
    - freq="W-FRI" / "ME"：按周/月对齐
    - freq=None：保留原生交易日历（用于日频图表，避免插入周末 NaN 导致曲线断裂）
    """
    valid_series = [s for s in series_list if s is not None and not s.empty]
    if not valid_series:
        return pd.DataFrame()

    if freq is None:
        # 以第一条有效序列的交易日历为基准对齐，避免非交易日空洞
        base_idx = valid_series[0].index
        df = pd.concat(valid_series, axis=1, sort=True)
        df = df.ffill(limit=max_ffill_steps).reindex(base_idx)
        return df.dropna(how="all").iloc[-n:]

    df = pd.concat(valid_series, axis=1, sort=True).resample(freq).last()
    for col in df.columns:
        last_valid = df[col].last_valid_index()
        first_valid = df[col].first_valid_index()
        if last_valid is not None and first_valid is not None:
            sub = df.loc[first_valid:last_valid, col].ffill(limit=max_ffill_steps)
            df.loc[first_valid:last_valid, col] = sub

    df = df.dropna(how="all")
    return df.iloc[-n:]


# ==========================================
# 3. 组装全部 23 张图表的数据与超详细四段式读图指南
# ==========================================
def build_all_charts() -> list:
    print(">>> 正在拉取市场行情、商品期货、AI板块与全球大类资产 ETF 数据...")
    market_tickers = [
        "^GSPC", "^NDX", "HG=F", "GC=F", "CL=F", "^VIX",
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

    print(">>> 正在拉取情绪、期权、CFTC持仓与市场宽度...")
    aaii_df = fetch_aaii_sentiment()
    naaim_df = fetch_naaim_exposure()
    pcr_df = fetch_cboe_put_call()
    cot_df = fetch_cftc_cot()
    breadth_df = fetch_sp500_breadth()

    print(">>> 正在拉取 FRED 与 DBnomics 宏观、PMI 与利率数据...")
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

    # 四、市场端：11大板块拆分为「20A 进攻周期6板块」与「20B 防御利率敏感5板块」
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
    sec_off_df = sec_off_df.resample("W-FRI").last().dropna(how="all")

    sec_def_df = pd.DataFrame(index=mkt.index)
    for code, label in defense_map.items():
        rel = (mkt[code] / mkt["VTI"]).dropna().iloc[-260:]
        sec_def_df[label] = (rel / rel.iloc[0]) * 100.0
    sec_def_df = sec_def_df.resample("W-FRI").last().dropna(how="all")

    # 五、扩展模块：AI强弱 & 全球七大资产资金净流入/流出强度（柱状图）
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
        ai_df[label] = (rel / rel.iloc[0]) * 100.0
    ai_df = ai_df.resample("W-FRI").last().dropna(how="all")

    global_flow_tickers = {
        "SPY": "美股大盘(SPY)",
        "QQQ": "美股科技(QQQ)",
        "EEM": "新兴市场(EEM)",
        "TLT": "美债长债(TLT)",
        "GLD": "实物黄金(GLD)",
        "DBC": "大宗商品(DBC)",
        "IBIT": "比特币ETF(IBIT)",
    }
    global_flow_df = compute_rolling_flow_intensity(ohlcv, global_flow_tickers, window=20, tail_weeks=10)

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
            "what": "• 蓝线(左轴)：初请失业金人数(万人)，每周新增申领失业救济人数\n• 红线(左轴)：续请失业金人数(十万人)，持续处于失业状态的总规模\n• 黄线(右轴)：非农就业总人数同比增速(%)",
            "how": "美国70%GDP由个人消费驱动，而薪资就业是消费的源头活水。初请反映企业「裁员速度」（高频领先），续请反映失业者「再就业难度」（滞后确认），非农同比衡量整体就业扩张斜率。",
            "changes": "• 蓝/红线低位走平 + 黄线稳健(>1%)：劳动力市场供需健康，居民消费具备坚实支撑\n• 蓝线率先跳升、随后红线抬头：企业开始裁员且失业者难找新工作，消费即将降温\n• 黄线持续下破1%向0%逼近：全社会岗位扩张停滞，通常对应经济步入衰退前夜",
            "signal": "⚠️ 实战阈值：初请失业金持续突破 30万人 且续请突破 200万人(红线>20)，为消费失速预警信号。",
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
            "what": "• 蓝线：个人总收入年度同比增速(%)\n• 红线：个人消费支出(PCE)年度同比增速(%)\n• 黄线：个人储蓄率(%)，即税后可支配收入中未消费的留存比例",
            "how": "观察美国家庭资产负债表的健康度。可持续的消费必须建立在「收入增长覆盖支出增长」之上；若支出增速长期高于收入增速，说明家庭正在消耗历史超额储蓄或依赖高息信用卡透支。",
            "changes": "• 蓝线 ≥ 红线 且 黄线位于5%~8%：内生消费健康循环，通胀压力可控\n• 红线持续高于蓝线 + 黄线单边下滑：透支型消费，一旦储蓄耗尽消费将出现断崖式回落\n• 黄线突然急升 + 红线暴跌：居民进入防御性存钱模式，零售与可选消费承压",
            "signal": "⚠️ 实战阈值：个人储蓄率跌破 3.5% 为历史警戒低位；低于通胀的收入增速预示消费后继乏力。",
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
            "what": "• 蓝线：成屋销售同比(%)\n• 红线：新屋销售同比(%)\n• 黄线：标普Case-Shiller 20大都市房价指数同比(%)\n• 绿线：全美新房销售中位价同比(%)",
            "how": "房地产是美国家庭最大宗资产，直接牵动家电、建材、家具、按揭金融全产业链。地产周期严格遵循「量在价先」规律：成交量（蓝/红线）通常领先房价（黄/绿线）3~6个月见顶或见底。",
            "changes": "• 红线/蓝线由负转正且持续上行：按揭利率压力缓和，买家入市，预计3~6个月后房价止跌回升\n• 红线见顶大幅回落、黄线仍在高位：典型的「量缩价撑」顶背离，预示随后房价将步入阴跌\n• 黄线与绿线剪刀差扩大：豪宅/核心都市房价坚挺，而大众刚需新房正在降价促销",
            "signal": "💡 实战信号：新屋与成屋销售同比连续3个月站稳 0% 上方，是布局地产建筑商(ITB/XHB)与家居链的领先信号。",
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
            "what": "• 蓝线(左轴)：全美轻型汽车月度折年总销量(百万辆)\n• 红线(左轴)：全美汽车销量年度同比增速(%)\n• 黄线(右轴)：美国CPI二手车与卡车价格指数",
            "how": "汽车是美国第二大耐用消费品，对车贷利率与供应链极为敏感。结合销量与二手车价格，可同时判断居民大件消费意愿以及核心商品通胀的演变方向。",
            "changes": "• 蓝线回升至1550~1700万辆 + 黄线平稳回落：供应链顺畅、价格合理，大件消费健康复苏\n• 黄线暴涨 + 蓝线跌破1400万辆：缺芯或通胀推高车价抑制需求，同时抬升美联储核心CPI压力\n• 红线持续处于负值区：高利率抑制车贷需求，耐用消费品处于去库周期",
            "signal": "💡 实战信号：二手车价格指数持续下行是核心商品通胀回落的先导；销量重返 1600万辆 上方代表消费韧性极强。",
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
            "what": "• 蓝线：美国总体零售与食品服务销售额同比(%)\n• 红线：核心零售（剔除波动较大的汽车及零部件）同比(%)\n• 黄线：非实体店零售（以亚马逊为代表的电商网购）同比(%)",
            "how": "最直观反映美国居民日常高频消费热度。通过对比总体零售（蓝）、核心实体零售（红）与电商（黄），可洞察消费渠道的结构性迁移与中低收入群体消费力。",
            "changes": "• 三线同步维持在3%~6%稳健区间：名义消费扩张健康，零售与消费龙头盈利有保障\n• 黄线显著高于蓝/红线：消费向线上集中，传统百货商超承压，利好电商与物流科技\n• 蓝线与红线同步跌破0%：剔除通胀后实际消费大幅萎缩，经济衰退风险显著升温",
            "signal": "⚠️ 实战阈值：核心零售同比跌破 0% 负增长往往对应经济实质性衰退；高于 5% 则需警惕消费过热延缓降息。",
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
            "what": "• 蓝线：私人住宅建房许可同比(%)（开发商拿地规划阶段，最领先）\n• 红线：新屋开工同比(%)（实际动工建设阶段）\n• 黄线：待售新房库存同比(%)（供给压力核心指标）\n• 绿线：20大都市房价同比(%)",
            "how": "完整刻画房地产供给链条：「建房许可(蓝) → 1~2个月后新屋开工(红) → 6~12个月后竣工形成新房库存(黄) → 供需关系决定房价(绿)」。",
            "changes": "• 蓝线与红线拐头向上 + 黄线库存处于低位负增长：供不应求格局确立，未来半年房价(绿线)具强支撑\n• 黄线库存同比飙升(>10%) + 蓝线持续负增长：新房积压严重，开发商被迫降价促销并削减新开工\n• 蓝线明显强于红线：开发商正在积极储备项目，预示未来一季度施工活动将回暖",
            "signal": "💡 实战信号：建房许可增速领先未来6~9个月建筑材料与家电需求；库存低位+许可转正是地产股最佳买点。",
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
            "what": "• 蓝线(左轴)：全美耐用品新增订单同比(%)（含飞机与国防）\n• 红线(左轴)：核心资本品订单同比(%)（剔除国防与飞机，衡量企业设备投资）\n• 黄线(右轴)：ISM制造业PMI  • 绿线(右轴)：ISM非制造业(服务业)PMI（50为荣枯分界线）",
            "how": "企业接到订单后需 3~6 个月采购原材料、扩产与交付。将耐用品订单与 ISM 制造业/服务业 PMI 结合，可提前预判未来 3~6 个月的全社会商品与服务供给扩张节奏。",
            "changes": "• 红线由负转正 + 黄/绿线站稳50荣枯线上方：企业主动补库与设备投资升温，工业与顺周期走强\n• 蓝线暴涨暴跌而红线平稳：仅受波音飞机或军工大单扰动，非全面经济过热\n• 红线持续负增长 + 黄线深陷50下方：制造业收缩，未来半年工业产出承压",
            "signal": "⚠️ 实战信号：核心资本品订单(红线)连续3个月正增长且 ISM 制造业 PMI 站上 50 荣枯线，确认制造业复苏周期开启。",
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
            "what": "• 蓝线：纽约联储每周经济指数 (Weekly Economic Index, WEI)",
            "how": "整合红皮书零售、初请失业金、钢产量、发电量、铁路货运、燃油消费等10项高频周度实体数据，实时映射美国季度实际GDP的同比年化增长率，比官方季度GDP提前数月揭示拐点。",
            "changes": "• 蓝线稳定在 1.5% ~ 3.0% 区间：美国经济处于温和扩张的「金发姑娘」状态，利好风险资产\n• 蓝线持续抬升突破 3.0%：实体经济动能强劲，企业盈利上修，但降息预期可能延后\n• 蓝线趋势性跌破 1.0% 并向 0% 俯冲：经济失速迹象明确，市场将快速定价降息与避险交易",
            "signal": "🚨 衰退红线：WEI 跌破 0% 负值区（尤其是连续3周以上为负），历史上高度对应美国经济实质性衰退。",
        },
        {
            "id": "09",
            "category": "供给",
            "chart_type": "LINE",
            "title": "09. 铜金比与PPI/PMI (Industrial & PPI)",
            "df": align_tail([copper_gold, ppi_yoy, mfg_pmi], freq="W-FRI", n=180),
            "right_cols": ["PPI出厂价格同比(%)", "ISM制造业PMI"],
            "left_title": "铜金比 (×1000)",
            "right_title": "PPI同比(%) / ISM制造业PMI",
            "what": "• 蓝线(左轴)：铜金比（COMEX铜期货 ÷ 黄金期货 × 1000）\n• 红线(右轴)：美国工业品出厂价格指数 PPI 同比(%)\n• 黄线(右轴)：美国 ISM 制造业采购经理人指数 PMI（50为荣枯线）",
            "how": "铜广泛用于电力、建筑与高端制造，代表「全球实体工业需求」；黄金代表「货币避险属性」。铜金比剔除了美元汇率干扰，高频领先工业通胀 PPI 与制造业 PMI 2~4 个月。",
            "changes": "• 蓝线持续走强上升：工业需求压倒避险情绪，预示未来数月制造业 PMI(黄线)回升、PPI(红线)抬头\n• 蓝线持续下挫破位：全球工业需求疲软或避险情绪升温，预示工业品面临通缩压力\n• 蓝线提前拐头向上而红线仍在低位：PPI 即将见底回升，利好顺周期资源股",
            "signal": "💡 实战信号：铜金比是宏观交易员的「高频工业晴雨表」，其趋势拐点通常领先官方 PPI 与 PMI 约 1~2 个季度。",
        },
        {
            "id": "10",
            "category": "供给",
            "chart_type": "LINE",
            "title": "10. 油金比与CPI (Energy & CPI)",
            "df": align_tail([oil_gold, cpi_yoy], freq="W-FRI", n=180),
            "right_cols": ["CPI通胀同比(%)"],
            "left_title": "油金比 (×100)",
            "right_title": "CPI通胀同比 (%)",
            "what": "• 蓝线(左轴)：油金比（WTI原油期货 ÷ 黄金期货 × 100）\n• 红线(右轴)：美国消费者物价指数 CPI 同比(%)",
            "how": "原油是现代经济交通、化工与物流的基础成本。用原油除以黄金得到「油金比」，可剥离单纯由货币超发引起的名义涨价，精准领先反映终端消费通胀 CPI 的大方向。",
            "changes": "• 蓝线持续飙升：能源成本向全社会传导，预示未来 1~3 个月 CPI(红线)存在反弹或二次通胀风险\n• 蓝线见顶大幅回落：能源通胀压力解除，CPI 大概率随之降温，为美联储降息打开空间\n• 蓝线低位徘徊而红线仍高：核心服务业粘性通胀主导，需关注薪资与房租分项",
            "signal": "💡 实战信号：油金比大幅回落是「通胀见顶、长债收益率下行、科技成长股估值修复」的重要前置信号。",
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
            "what": "• 蓝线(左轴)：铜金比（反映经济增长预期）\n• 红线(左轴)：油金比（反映通胀预期）\n• 黄线(右轴)：美国10年期国债收益率(%)（反映无风险利率定价）",
            "how": "铜、油、黄金与美债在全球每日23小时连续交易，凝聚了万亿级宏观对冲基金用真金白银投票的实时宏观共识，不受滞后统计修正与媒体噪音干扰。",
            "changes": "• 蓝+红+黄三线同步上行：典型「再通胀/经济过热交易」，利好价值股、能源、金融，利空长久期美债\n• 蓝+红+黄三线同步下行：典型「衰退/降息避险交易」，资金涌入美债(TLT)与黄金(GLD)\n• 蓝线跌、红线涨、黄线涨：最恶劣的「滞胀组合」，股债双杀风险最高",
            "signal": "💡 实战信号：当 10年美债收益率(黄线)与铜金比(蓝线)出现严重顶背离时，美债利率往往会向铜金比方向回归。",
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
            "what": "• 蓝线：美联储有效联邦基金利率(DFF)  • 紫线：3个月期美债利率\n• 绿线：2年期美债收益率  • 黄线：5年期美债  • 红线：10年期美债收益率",
            "how": "利率是全球一切风险资产定价的贴现率之锚。短端利率（3M/2Y）受美联储货币政策直接控制，长端利率（10Y）反映市场对未来长期经济增速与通胀的预期。",
            "changes": "• 紫/绿线高于红线（短端 > 长端）：称为「收益率曲线倒挂」，代表货币政策极度紧缩，银行放贷意愿受抑\n• 倒挂后绿线暴跌、红线平稳（牛市变陡）：市场抢跑定价美联储被迫紧急降息，往往对应经济衰退正式落地\n• 红线与绿线同步温和下行：通胀受控下的预防式降息周期，最利好美股与科技成长股",
            "signal": "🚨 宏观警报：倒挂持续期间市场未必立刻下跌，但「倒挂结束、2Y-10Y利差迅速转正变陡」时往往伴随美股剧烈波动。",
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
            "what": "• 蓝线(左轴)：ICE BofA CCC级高收益垃圾债期权调整信用利差(%)\n• 红线(左轴)：CBOE VIX恐慌指数\n• 黄线(右轴)：标普500指数",
            "how": "信用利差衡量资质最弱的企业融资相比无风险美债需要支付的额外风险补偿。债券市场往往比股票市场更早嗅到流动性危机与企业违约风险。",
            "changes": "• 蓝线低位平稳(<8%) + 红线<18：信用市场融资顺畅，流动性充裕，美股处于低波动慢牛环境\n• 黄线仍在高位但蓝线已提前抬头扩大：聪明的债市资金开始撤离垃圾债，股市存在补跌风险\n• 蓝线急剧飙升(>12%) + 红线突破30：流动性危机爆发，金融机构去杠杆，需果断降仓防守",
            "signal": "🚨 危机阈值：CCC信用利差突破 10%~12% 且 VIX 站上 30，代表系统性信用紧缩；利差见顶回落则是抄底黄金信号。",
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
            "what": "• 蓝线(左轴)：AAII散户看涨比例减去看跌比例的净差值(%)\n• 红线(左轴)：该情绪净差值的20周移动平均线(%)\n• 黄线(右轴)：标普500指数",
            "how": "经典的逆向心理指标！个人投资者往往在市场连续上涨后变得极度乐观（追高），而在市场大跌后陷入极度恐慌（割肉）。观察蓝线触及极端正负值时的均值回归拐点。",
            "changes": "• 蓝线飙升至 +25% 以上且红线同步高位：散户看多情绪极度拥挤，潜在买盘枯竭，警惕阶段性回调\n• 蓝线暴跌至 -25% 以下：散户极度悲观恐慌，抛压接近尾声，往往对应中短期重要底部\n• 指数回调但蓝线迅速转负：健康的洗盘消化浮筹，牛市趋势未改",
            "signal": "🎯 逆向阈值：净差值 > +25% 逢高减仓/对冲；净差值 < -25%（尤其 <-30%）是高胜率左侧分批布局区。",
        },
        {
            "id": "15",
            "category": "市场",
            "chart_type": "LINE",
            "title": "15. 机构投资者情绪 (NAAIM Exposure)",
            "df": align_tail([naaim_df["机构风险敞口(%)"], naaim_df["20周均线(%)"], spx], freq="W-FRI", n=180)
            if not naaim_df.empty
            else align_tail([spx], freq="W-FRI", n=180),
            "right_cols": ["标普500指数"],
            "left_title": "机构平均股票仓位 (%)",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：NAAIM美国主动投资经理平均股票仓位敞口(%)（可超100%代表加杠杆）\n• 红线(左轴)：机构仓位20周均线(%)  • 黄线(右轴)：标普500指数",
            "how": "机构基金经理同样面临季度排名考核压力，存在明显的「追涨杀跌与被迫补仓」行为。当机构满仓加杠杆时，后续增量资金枯竭；当机构砍仓至低位时，一旦市场企稳将引发踏空逼空买盘。",
            "changes": "• 蓝线升至 95%~105% 满仓区：机构子弹已打光，市场对利空变得极为脆弱，易发生技术性回调\n• 蓝线骤降至 40% 以下：机构风控砍仓接近极限，卖盘出清，市场随时可能迎来报复性反弹\n• 黄线创新高但蓝线重心下移：机构在指数上涨过程中悄悄降仓派发，需警惕顶部背离",
            "signal": "🎯 逆向阈值：机构敞口 ≥ 95% 警惕短期见顶；敞口 ≤ 40% 往往对应波段重要买点。",
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
            "what": "• 蓝线(左轴)：CBOE个股看跌期权(Put) ÷ 看涨期权(Call)日成交量比值\n• 红线(左轴)：Put/Call比值的20日移动平均线  • 黄线(右轴)：标普500指数",
            "how": "反映交易员用真金白银押注下跌对冲（买Put）与押注上涨（买Call）的实时比例。比值越高，说明市场花重金买保险对冲暴跌的恐慌情绪越浓烈。",
            "changes": "• 蓝线单日脉冲冲高(>0.95)且红线显著抬升：市场恐慌性买入看跌期权对冲，做空力量达到高潮，底部临近\n• 蓝线持续压低至 0.50 以下：市场沉迷买入看涨期权追涨，投机过热，短期易出现震荡洗盘\n• 黄线下跌过程中蓝线不再创新高：恐慌情绪边际钝化，卖压衰竭",
            "signal": "🎯 逆向阈值：Equity Put/Call 单日突破 1.0 或 20日均线升破 0.78，往往对应美股阶段性底部拐点。",
        },
        {
            "id": "17",
            "category": "市场",
            "chart_type": "LINE",
            "title": "17. S&P 500 投机者持仓 (CFTC COT)",
            "df": align_tail([cot_df["标普500投机净持仓(万手)"], spx], freq="W-FRI", n=180)
            if not cot_df.empty and "标普500投机净持仓(万手)" in cot_df
            else align_tail([spx], freq="W-FRI", n=180),
            "right_cols": ["标普500指数"],
            "left_title": "非商业净持仓 (万手)",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：CFTC CME E-Mini标普500期货大型投机者（对冲基金）净多空持仓(万手)\n• 红线(右轴)：标普500指数",
            "how": "0轴上方代表对冲基金整体净做多，0轴下方代表整体净做空（每周五发布截至周二数据）。在衍生品市场中，极端拥挤的空头仓位往往成为未来上涨的最大燃料。",
            "changes": "• 蓝线处于深幅负值区(-15万手以下)但红线拒跌反弹：空头被套被迫平仓回补，极易触发「轧空(Short Squeeze)」暴涨\n• 蓝线持续攀升至历史高位(+15万手以上)：多头持仓高度拥挤，若无新资金接力易引发获利了结\n• 蓝线随红线同步稳步回升：趋势资金顺势加仓，上涨动能健康",
            "signal": "🎯 轧空提示：当标普投机净空头处于历史极值区而指数站稳关键均线时，空头回补将驱动凌厉主升浪。",
        },
        {
            "id": "18",
            "category": "市场",
            "chart_type": "LINE",
            "title": "18. Nasdaq 100 投机者持仓 (CFTC COT)",
            "df": align_tail([cot_df["纳指100投机净持仓(万手)"], ndx], freq="W-FRI", n=180)
            if not cot_df.empty and "纳指100投机净持仓(万手)" in cot_df
            else align_tail([ndx], freq="W-FRI", n=180),
            "right_cols": ["纳斯达克100指数"],
            "left_title": "非商业净持仓 (万手)",
            "right_title": "纳斯达克100点位",
            "what": "• 蓝线(左轴)：CFTC 纳斯达克100期货大型投机者净多空持仓(万手)\n• 红线(右轴)：纳斯达克100指数",
            "how": "专门追踪机构资金对「大型科技成长股」的杠杆押注态度。将本图与图17（标普500持仓）交叉对比，可看清主力资金是「全面看多」还是「做多科技、做空传统大盘」的结构化抱团。",
            "changes": "• 图17净空而本图蓝线净多：机构明显偏好AI与科技龙头，科技股超额收益显著\n• 蓝线触及历史极端净多高位后拐头向下：科技板块抱团松动，需防范高估值成长股获利回吐\n• 蓝线跌入深度净空区后回升：科技股经历充分去杠杆，迎来新一轮配置窗口",
            "signal": "💡 实战信号：对比图17与图18的剪刀差，可精准识别市场处于「科技抱团」还是「风格切换」阶段。",
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
            "what": "• 蓝线(左轴)：标普500成分股中收盘价高于50日均线的股票占比(%)\n• 红线(左轴)：标普500成分股中收盘价高于200日均线的股票占比(%)\n• 黄线(右轴)：标普500指数",
            "how": "穿透指数表面涨跌的「透视镜」。标普500受前七大权重股影响极大，市场宽度能识别「指数虚涨、多数个股下跌」的危险顶背离，以及「全面超卖」的黄金买点。",
            "changes": "• 黄线创新高，但蓝线/红线重心明显下移（顶背离）：仅靠少数巨头撑盘，内部多数股票走弱，警惕指数补跌\n• 蓝线暴跌至 15% 以下：85%以上成分股跌破中期均线，短期恐慌超卖极致，随时反弹\n• 红线跌破 20% 后重新站上 30%：熊市全面出清后的长线大牛市启动信号",
            "signal": "🎯 买卖极区：50日宽度 > 85% 提示短期过热；< 15% 提示短期超卖。若50日与200日宽度同时 < 15%，为罕见中长线大底！",
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
            "how": "曲线向上倾斜代表该板块跑赢大盘（资金净流入），向下倾斜代表跑输大盘（资金流出）。观察进攻板块内部是由「科技成长」领涨，还是切换至「金融/工业/能源顺周期」。",
            "changes": "• 科技XLK + 通讯XLC 领跑上升：AI与成长股主导牛市，风险偏好高涨\n• 金融XLF + 工业XLI + 能源XLE 接力走强：经济软着陆与再通胀交易主导，资金向顺周期价值扩散\n• 6大进攻板块全线跌破100走弱：市场进入全面防御避险模式",
            "signal": "🎯 轮动策略：顺势超配斜率最陡峭、处于100线上方的1~2个领涨板块，回避持续阴跌的弱势板块。",
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
            "how": "防御板块（医疗、必需消费、公用事业）盈利受经济周期影响小，是机构避险的「资金蓄水池」；房地产XLRE对美债利率下行最敏感；原材料XLB挂钩大宗商品价格。",
            "changes": "• 蓝/红/黄三线集体抬头跑赢大盘：典型的「Risk-Off 避险预警」，说明机构正在撤出高波动股票转向防御\n• 绿线(房地产XLRE)与黄线(公用事业XLU)同步走强：市场正在交易美债收益率下行与降息周期\n• 三大防御板块持续单边下行：市场风险偏好极强，资金全线进攻",
            "signal": "🚨 避险警报：当必需消费(XLP)与公用事业(XLU)连续数周显著跑赢科技(XLK)时，大盘往往面临系统性回调压力。",
        },
        # --- 五、核心焦点扩展模块 ---
        {
            "id": "21",
            "category": "扩展",
            "chart_type": "LINE",
            "title": "21. AI板块对比标普与三大股指强弱比 (AI vs Indices)",
            "df": ai_df,
            "right_cols": [],
            "left_title": "相对强弱指数 (1年前起点=100)",
            "right_title": "",
            "what": "• 蓝:AI芯片SMH÷标普SPY | 红:AI应用AIQ÷标普SPY | 黄:AI机器人BOTZ÷标普SPY\n• 绿:AI芯片SMH÷纳指QQQ | 紫:AI芯片SMH÷罗素小盘IWM | 青:AI芯片SMH÷道指DIA",
            "how": "精确量化AI核心产业链相对美股四大宽基指数的超额收益与资金虹吸效应。对比「芯片(SMH) vs 应用(AIQ)」可判断AI行情处于算力基建期还是软件变现期；对比「SMH/IWM 与 SMH/DIA」可监测资金高低切换。",
            "changes": "• 蓝线(SMH/SPY)与红线(AIQ/SPY)同步上行：AI全产业链领涨大盘，主升浪确立\n• 蓝线走平或回落，而红线(AIQ/SPY)接力走强：资金从高估值算力芯片向AI软件与应用端扩散\n• 紫线(SMH/IWM)与青线(SMH/DIA)大幅回落：资金从AI科技巨头撤出，流向中小盘与传统蓝筹补涨",
            "signal": "🎯 轮动信号：SMH/SPY 趋势向上时坚定持有AI核心龙头；若 SMH/QQQ 与 SMH/IWM 同步跌破中期趋势线，注意科技股阶段休整。",
        },
        {
            "id": "22",
            "category": "扩展",
            "chart_type": "COLUMN",
            "title": "22. 全球七大核心资产资金净流入/流出强度 (柱状图)",
            "df": global_flow_df,
            "right_cols": [],
            "left_title": "20日资金流净强弱指数 (%)",
            "right_title": "",
            "what": "• 近10周旗舰ETF 20日资金流净强弱柱状图(%)（0轴上方柱体=资金净流入，0轴下方柱体=资金净流出）：\n• 蓝:美股SPY | 红:科技QQQ | 黄:新兴市场EEM | 绿:美债TLT | 紫:黄金GLD | 青:商品DBC | 洋红:比特币IBIT",
            "how": "基于每日成交价格在高低点之间的位置与成交额加权计算的资金流柱状模型。每组柱体直观对比同一周内全球七大核心资产谁在吸金、谁在失血。",
            "changes": "• 美股SPY/QQQ + 比特币IBIT 柱体齐站0轴上方：全球流动性充裕，风险资产全面净流入\n• 美债TLT + 黄金GLD 正向高柱，而股市出现负向绿/蓝柱：全球大资金转向防御避险\n• 黄金GLD + 商品DBC 连续出现正柱：通胀预期升温，实物抗通胀资产获资金持续增配",
            "signal": "🎯 资金风向：单周柱体突破 +10% 代表机构持续建仓净流入强劲；跌破 -10% 代表机构持续派发净流出。",
        },
    ]
    return charts


# ==========================================
# 4. 调用 OpenAI 生成顶部实时宏观简评
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
                "你是资深美股宏观与量化策略师。请根据以下最新拉取的23张核心图表数据快照，"
                "用简洁有力的中文写一段220字以内的【本期宏观、AI强弱与全球资金流总评】，依次点明："
                "1) 消费与供给趋势；2) 利率与信用风险；3) 市场宽度、AI板块相对大盘强弱及全球主要资产柱状资金净流入/流出方向。\n\n"
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
                    "max_tokens": 420,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"[Warning] OpenAI 摘要生成失败，切换至规则摘要: {e}")

    return (
        "数据已自动同步至最新交易日。请重点核查：① 纽约联储WEI指数与油金/铜金比通胀方向；"
        "② 10年与2年期美债利率倒挂状态及CCC信用利差；③ 标普500市场宽度是否触及85%/15%极区；"
        "④ AI芯片(SMH)/应用(AIQ)相对标普SPY超额强弱，以及全球股/债/黄金/比特币20日柱状资金净流向。"
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
# 5. 写入底层数据并渲染高清 23 图看板
# ==========================================
def render_to_google_sheets(charts: list, ai_summary: str):
    sh = gc.open_by_key(SPREADSHEET_ID)

    total_dash_rows = 680
    raw_rows_cap = 420
    raw_cols_cap = 200

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

    # 清除旧图表对象 + 彻底解除旧版合并单元格 + 清除两张表的旧单元格格式（修复右轴显示1920-01-01日期串扰！）
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
    # A. 写入 Raw_Data 底层数据并显式指定数值格式
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

        # 显式强制将指标列格式化为 NUMBER，彻底杜绝 Google Sheets 误判为日期
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
    # B. 构建 Dashboard 文本卡片与四段式详细读图指南
    # ------------------------------------------
    tz_cst = timezone(timedelta(hours=8))
    now_str = datetime.now(tz_cst).strftime("%Y-%m-%d %H:%M (UTC+8)")

    dash_grid = [[""] * 15 for _ in range(total_dash_rows)]
    dash_grid[1][1] = "📈 美股核心 23 张宏观、AI强弱与全球资金流全景看板"
    dash_grid[2][1] = f"自动更新时间：{now_str}   |   数据来源：FRED 美联储 · DBnomics ISM · Redfin · Yahoo Finance · AAII · NAAIM · CBOE · CFTC"
    dash_grid[3][1] = f"🤖 AI 宏观与资金流总评：{ai_summary}"

    category_banners = {
        "消费": ("一、消费端 (Consumption — 美国经济 70% 核心驱动，有工作才有收入与消费)", {"red": 0.11, "green": 0.29, "blue": 0.49}),
        "供给": ("二、供给端 (Supply — 提前 3~6 个月预判供需缺口与通胀拐点的先行指标)", {"red": 0.08, "green": 0.42, "blue": 0.45}),
        "利率": ("三、利率端 (Interest Rates — 资产定价之锚、曲线倒挂与系统性违约风险标杆)", {"red": 0.34, "green": 0.22, "blue": 0.48}),
        "市场": ("四、市场端 (Market — 群体情绪极值反转、机构持仓拥挤度与11大板块轮动)", {"red": 0.13, "green": 0.42, "blue": 0.32}),
        "扩展": ("五、核心焦点扩展 (AI 板块对比各大股指相对强弱 & 全球七大资产柱状资金净流入/流出)", {"red": 0.65, "green": 0.24, "blue": 0.14}),
    }

    row_cursor = 5
    current_cat = None
    layout_blocks = []

    CARD_ROWS = 25

    for c in charts:
        cat = c["category"]
        if cat != current_cat:
            current_cat = cat
            banner_text, banner_color = category_banners[cat]
            dash_grid[row_cursor][1] = banner_text
            layout_blocks.append({"type": "section", "row": row_cursor, "color": banner_color})
            row_cursor += 2

        r0 = row_cursor
        dash_grid[r0][9] = f"📌 {c['title']} — 深度读图与实战指南"
        dash_grid[r0 + 1][9] = "【① 曲线/柱体与坐标轴含义】"
        dash_grid[r0 + 2][9] = c["what"]
        dash_grid[r0 + 6][9] = "【② 怎么读 / 核心传导逻辑】"
        dash_grid[r0 + 7][9] = c["how"]
        dash_grid[r0 + 12][9] = "【③ 走势变化深度解读（上升 / 下降 / 背离）】"
        dash_grid[r0 + 13][9] = c["changes"]
        dash_grid[r0 + 20][9] = "【④ 关键阈值与实战风控信号】"
        dash_grid[r0 + 21][9] = c["signal"]

        layout_blocks.append({
            "type": "chart_card",
            "row": r0,
            "chart": c,
            "color": category_banners[cat][1],
        })
        row_cursor += CARD_ROWS + 1

    dash_ws.update(range_name="A1", values=dash_grid[: row_cursor + 5], value_input_option="USER_ENTERED")

    # ------------------------------------------
    # C. 批量发送排版样式 + 23张高清原生图表构建请求
    # ------------------------------------------
    dash_id = dash_ws.id
    raw_id = raw_ws.id
    requests_batch = []

    col_widths = (
        [(0, 1, 24)]
        + [(i, i + 1, 102) for i in range(1, 9)]
        + [(i, i + 1, 136) for i in range(9, 14)]
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
                "range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 1, "endColumnIndex": 14},
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 1, "endColumnIndex": 14},
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
                "range": {"sheetId": dash_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 1, "endColumnIndex": 14},
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 1, "endColumnIndex": 14},
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
                "range": {"sheetId": dash_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 1, "endColumnIndex": 14},
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": dash_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 1, "endColumnIndex": 14},
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
                "properties": {"pixelSize": 60},
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
                        "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 1, "endColumnIndex": 14},
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "repeatCell": {
                        "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 1, "endColumnIndex": 14},
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
                "properties": {"pixelSize": 22},
                "fields": "pixelSize",
            }
        })

        requests_batch.extend([
            {
                "mergeCells": {
                    "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 9, "endColumnIndex": 14},
                    "mergeType": "MERGE_ALL",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 9, "endColumnIndex": 14},
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

        for sr in [r0 + 1, r0 + 6, r0 + 12, r0 + 20]:
            requests_batch.extend([
                {
                    "mergeCells": {
                        "range": {"sheetId": dash_id, "startRowIndex": sr, "endRowIndex": sr + 1, "startColumnIndex": 9, "endColumnIndex": 14},
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "repeatCell": {
                        "range": {"sheetId": dash_id, "startRowIndex": sr, "endRowIndex": sr + 1, "startColumnIndex": 9, "endColumnIndex": 14},
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
            (r0 + 2, r0 + 6),
            (r0 + 7, r0 + 12),
            (r0 + 13, r0 + 20),
            (r0 + 21, r0 + 25),
        ]
        for idx_b, (br_start, br_end) in enumerate(body_blocks):
            bg = {"red": 0.99, "green": 0.96, "blue": 0.95} if idx_b == 3 else {"red": 0.98, "green": 0.99, "blue": 1.0}
            requests_batch.extend([
                {
                    "mergeCells": {
                        "range": {"sheetId": dash_id, "startRowIndex": br_start, "endRowIndex": br_end, "startColumnIndex": 9, "endColumnIndex": 14},
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "repeatCell": {
                        "range": {"sheetId": dash_id, "startRowIndex": br_start, "endRowIndex": br_end, "startColumnIndex": 9, "endColumnIndex": 14},
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
                "range": {"sheetId": dash_id, "startRowIndex": r0, "endRowIndex": r0 + CARD_ROWS, "startColumnIndex": 9, "endColumnIndex": 14},
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
                            "widthPixels": 808,
                            "heightPixels": 544,
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

    print("✅ 全部 23 张高清图表与四段式深度读图指南已更新完毕！")


if __name__ == "__main__":
    all_charts = build_all_charts()
    summary = generate_ai_summary(all_charts)
    render_to_google_sheets(all_charts, summary)
