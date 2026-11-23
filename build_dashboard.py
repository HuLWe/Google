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

# 隔离 yfinance 本地 SQLite 缓存路径，防止多线程/多进程锁死
try:
    yf.set_tz_cache_location("/tmp/yf_tz_cache")
except Exception:
    pass

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
    """从 St. Louis Fed (FRED) 拉取官方时间序列并做去重与数值校验"""
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
    return (s.pct_change(periods=periods) * 100.0).replace([np.inf, -np.inf], np.nan).dropna()


def fetch_existing_home_sales_yoy() -> pd.Series:
    """
    获取完整 60 个月全美成屋销售同比(%)：
    兼容 Redfin 的 '30 days' / '1 month' 月度周期字段，并在样本充足时合并 FRED 官方最新一期数据。
    """
    fred_s = yoy_pct(fetch_fred("EXHOSLUSM495S", start_date="2019-01-01"))
    if not fred_s.empty:
        fred_s = fred_s.resample("ME").last().dropna()

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
                dur_mask = sub[dur_col].astype(str).str.lower().str.contains(r"1 month|30 day", regex=True, na=False)
                if dur_mask.any():
                    sub = sub[dur_mask]
            if t_col and "All Residential" in sub[t_col].values:
                sub = sub[sub[t_col] == "All Residential"]
            if sa_col:
                false_rows = sub[sub[sa_col].astype(str).str.lower().isin(["false", "0", "f"])]
                true_rows = sub[sub[sa_col].astype(str).str.lower().isin(["true", "1", "t"])]
                if not false_rows.empty:
                    sub = false_rows
                elif not true_rows.empty:
                    sub = true_rows

            if p_col and not sub.empty:
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
                    s = s * 100.0
                else:
                    s = pd.Series(dtype=float)

                redfin_s = s[s.index >= "2019-01-01"]
                if not redfin_s.empty:
                    print(f"[OK] Redfin 成屋销售同比拉取成功 ({len(redfin_s)} 个月, 最新: {redfin_s.index[-1].strftime('%Y-%m-%d')})")
    except Exception as e:
        print(f"[Info] Redfin 通道回退至 FRED: {e}")

    # 若 FRED 有完整 24 个月以上历史，以 FRED 为主并补充 Redfin 更新月份
    if len(fred_s) >= 24:
        if not redfin_s.empty:
            newer_redfin = redfin_s[redfin_s.index > fred_s.index.max()]
            if not newer_redfin.empty:
                fred_s = pd.concat([fred_s, newer_redfin]).sort_index()
        return fred_s.rename("成屋销售同比(%)")

    # 若 FRED 仅返回近 13 个月（同比仅 1 个点），以 Redfin 完整 60 个月为底座并叠加 FRED 最新点
    if not redfin_s.empty:
        r_m = redfin_s.resample("ME").last()
        if not fred_s.empty:
            r_m = fred_s.combine_first(r_m).sort_index()
        return r_m.rename("成屋销售同比(%)")

    return fred_s.rename("成屋销售同比(%)")


def fetch_ism_pmi() -> tuple:
    """获取美国 ISM 制造业 PMI 与非制造业 PMI（50为荣枯线），若官方通道滞后则标注联储模型映射"""
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
            arr_matches = re.findall(
                r'data\s*:\s*\[\s*(\[\s*\d{13}\s*,\s*\d{2}(?:\.\d+)?\s*\](?:\s*,\s*\[\s*\d{13}\s*,\s*\d{2}(?:\.\d+)?\s*\]){11,})\s*\]',
                r.text,
            )
            best_records = []
            for arr_str in arr_matches:
                pairs = re.findall(r'\[\s*(\d{13})\s*,\s*(\d{2}(?:\.\d+)?)\s*\]', arr_str)
                records = []
                for ts_ms, val in pairs:
                    dt = pd.to_datetime(int(ts_ms), unit="ms").to_period("M").to_timestamp()
                    if dt >= pd.Timestamp("2019-01-01"):
                        records.append((dt, float(val)))
                if len(records) > len(best_records):
                    best_records = records
            if best_records:
                s = pd.Series([v for _, v in best_records], index=pd.DatetimeIndex([d for d, _ in best_records]))
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

    mfg_used_proxy = False
    ny_mfg = fetch_fred("GACDISA066MSFRBNY", start_date="2019-01-01")
    philly_mfg = fetch_fred("GACDFSA066MSFRBPHI", start_date="2019-01-01")
    if not ny_mfg.empty and not philly_mfg.empty:
        fed_mfg_proxy = (50.0 + (ny_mfg.resample("MS").last() + philly_mfg.resample("MS").last()) * 0.12).dropna()
        if mfg_pmi.empty:
            mfg_pmi = fed_mfg_proxy
            mfg_used_proxy = True
        elif mfg_pmi.index.max() < fed_mfg_proxy.index.max():
            mfg_pmi = mfg_pmi.resample("MS").last().combine_first(fed_mfg_proxy)
            mfg_used_proxy = True

    svc_used_proxy = False
    cfnai = fetch_fred("CFNAI", start_date="2019-01-01")
    if not cfnai.empty:
        svc_proxy = (52.5 + cfnai.resample("MS").last() * 3.0).dropna()
        if svc_pmi.empty:
            svc_pmi = svc_proxy
            svc_used_proxy = True
        elif svc_pmi.index.max() < svc_proxy.index.max():
            svc_pmi = svc_pmi.resample("MS").last().combine_first(svc_proxy)
            svc_used_proxy = True

    mfg_label = "ISM制造业PMI(含联储映射)" if mfg_used_proxy else "ISM制造业PMI"
    svc_label = "ISM非制造业PMI(含CFNAI映射)" if svc_used_proxy else "ISM非制造业PMI"
    mfg_pmi = _clean_pmi_series(mfg_pmi).rename(mfg_label)
    svc_pmi = _clean_pmi_series(svc_pmi).rename(svc_label)
    return mfg_pmi, svc_pmi


def fetch_yf_ohlcv(tickers: list, period: str = "5y") -> dict:
    """批量拉取 Yahoo Finance 完整 OHLCV 数据（关闭多线程防 SQLite 锁死 + 失败自动单只补抓）"""
    raw = yf.download(
        tickers, period=period, auto_adjust=True, progress=False, group_by="column", threads=False
    )
    data = {}
    for field in ["Open", "High", "Low", "Close", "Volume"]:
        if isinstance(raw.columns, pd.MultiIndex):
            df = raw[field].copy()
        else:
            df = raw[[field]].copy()
            df.columns = [tickers[0]]
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df = df[~df.index.duplicated(keep="last")].sort_index()
        data[field] = df

    # 对任何意外失败的标的自动重试补抓（如 XLC 等）
    missing = [t for t in tickers if t not in data["Close"].columns or data["Close"][t].dropna().empty]
    for t in missing:
        print(f"[Retry] 正在补抓缺失标的: {t}")
        time.sleep(1.5)
        retry = yf.download(t, period=period, auto_adjust=True, progress=False, threads=False)
        if not retry.empty:
            retry.index = pd.to_datetime(retry.index).tz_localize(None)
            retry = retry[~retry.index.duplicated(keep="last")].sort_index()
            for field in ["Open", "High", "Low", "Close", "Volume"]:
                if field in retry.columns:
                    s = retry[field].iloc[:, 0] if isinstance(retry[field], pd.DataFrame) else retry[field]
                    data[field][t] = s

    return data


def normalize_to_common_base(series_dict: dict, tail_days: int = 252, freq: str = "W-FRI") -> pd.DataFrame:
    """
    先将所有资产对齐到完全相同的共同交易日历并截取统一起点日 t0，
    再统一除以 t0 当日收盘价 × 100，保证所有曲线在起点严格重合于 100.00！
    """
    valid = {k: v.dropna() for k, v in series_dict.items() if v is not None and not v.dropna().empty}
    if not valid:
        return pd.DataFrame()
    df = pd.concat(valid, axis=1, sort=True).ffill(limit=4).dropna(how="any")
    if df.empty:
        return pd.DataFrame()
    df = df.iloc[-tail_days:]
    base_vals = df.iloc[0].replace(0, np.nan)
    norm_df = (df / base_vals) * 100.0
    if freq:
        norm_df = norm_df.resample(freq).last().ffill(limit=3).dropna(how="all")
    return norm_df


def compute_monthly_flow_intensity(ohlcv: dict, ticker_labels: dict, window: int = 20, tail_months: int = 12) -> pd.DataFrame:
    """
    计算机构级 Chaikin Money Flow (CMF) 20日资金流净强弱指数 (%)，
    按月度聚合输出最近 12 个月，保证每根柱体宽阔清晰！
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
    使用全周期完整的 6 大核心跨资产 ETF，保持 3 年分母口径一致并消除零轴基准漂移。
    """
    global_tickers = ["SPY", "QQQ", "EEM", "TLT", "GLD", "DBC"]
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

    avg_outflow_pressure = ((-cmf_w.mean(axis=1) + 5.0) / 25.0 * 100.0).clip(0, 100)

    vix_w = vix.resample("W-FRI").last().reindex(cmf_w.index).ffill(limit=6)
    vix_score = ((vix_w - 13.0) / 25.0 * 100.0).clip(0, 100)

    cs_w = credit_spread.resample("W-FRI").last().reindex(cmf_w.index).ffill(limit=6)
    cs_score = ((cs_w - 6.5) / 8.5 * 100.0).clip(0, 100)

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

        tff = pd.concat(frames, ignore_index=True).copy()
        name_col = [c for c in tff.columns if "Market_and_Exchange_Names" in c][0]
        date_col = [c for c in tff.columns if "Report_Date_as_YYYY-MM-DD" in c][0]
        am_long_col = [c for c in tff.columns if "Asset_Mgr_Positions_Long_All" in c][0]
        am_short_col = [c for c in tff.columns if "Asset_Mgr_Positions_Short_All" in c][0]

        tff[date_col] = pd.to_datetime(tff[date_col])
        tff[am_long_col] = pd.to_numeric(tff[am_long_col], errors="coerce")
        tff[am_short_col] = pd.to_numeric(tff[am_short_col], errors="coerce")

        sp = tff[
            tff[name_col].str.strip().str.match(r"^E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE$", case=False, na=False)
        ].copy()
        sp = sp.dropna(subset=[am_long_col, am_short_col]).set_index(date_col).sort_index()

        tot = (sp[am_long_col] + sp[am_short_col]).replace(0, np.nan)
        exposure = (sp[am_long_col] / tot * 100.0).groupby(level=0).first().sort_index().rename("机构股票多头敞口(%)")
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
        if chosen is not None and 0.15 <= chosen <= 2.50:
            return (pd.to_datetime(d_str), chosen)
    except Exception:
        return None
    return None


def fetch_cboe_put_call() -> pd.DataFrame:
    """并发直连 CBOE 官方每日市场统计 CDN 接口拉取真实 Put/Call 比值并过滤极值异常"""
    try:
        today = datetime.now(timezone.utc).date()
        candidate_dates = [
            (today - timedelta(days=delta)).strftime("%Y-%m-%d")
            for delta in range(0, 380)
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
    严格首尾锚定匹配 CFTC 官方标准主力合约（排除 ULTRA / MICRO / FINANCIAL 衍生合约串线）：
    - 完整覆盖全球七大核心资产：标普500、纳指100、10年期美债、实物黄金、WTI原油、美元指数、比特币BTC
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

        # 使用 .copy() 消除 DataFrame 内存碎片化警告
        cot = pd.concat(frames, ignore_index=True).copy()
        name_col = [c for c in cot.columns if "Market and Exchange Names" in c][0]
        date_col = [c for c in cot.columns if "As of Date in Form YYYY-MM-DD" in c][0]
        long_col = [c for c in cot.columns if "Noncommercial Positions-Long (All)" in c][0]
        short_col = [c for c in cot.columns if "Noncommercial Positions-Short (All)" in c][0]

        cot[date_col] = pd.to_datetime(cot[date_col])
        cot[long_col] = pd.to_numeric(cot[long_col], errors="coerce")
        cot[short_col] = pd.to_numeric(cot[short_col], errors="coerce")
        clean_names = cot[name_col].astype(str).str.strip().str.upper()

        asset_configs = [
            (
                ["E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE"],
                "标普500投机净持仓(万手)",
                "美股标普500机构净比(%)",
            ),
            (
                ["NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE", "NASDAQ-100 CONSOLIDATED - CHICAGO MERCANTILE EXCHANGE"],
                "纳指100投机净持仓(万手)",
                "美股科技纳指机构净比(%)",
            ),
            (
                ["UST 10Y NOTE - CHICAGO BOARD OF TRADE", "10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE"],
                None,
                "10年期美债机构净比(%)",
            ),
            (
                ["GOLD - COMMODITY EXCHANGE INC."],
                None,
                "实物黄金机构净比(%)",
            ),
            (
                ["WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE", "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE"],
                None,
                "WTI原油机构净比(%)",
            ),
            (
                ["USD INDEX - ICE FUTURES U.S.", "U.S. DOLLAR INDEX - ICE FUTURES U.S."],
                None,
                "美元指数机构净比(%)",
            ),
            (
                ["BITCOIN - CHICAGO MERCANTILE EXCHANGE"],
                None,
                "比特币BTC机构净比(%)",
            ),
        ]

        net_hands_dict = {}
        net_ratio_dict = {}

        for exact_names, hands_label, ratio_label in asset_configs:
            combined_hands = pd.Series(dtype=float)
            combined_ratio = pd.Series(dtype=float)
            for cname in exact_names:
                sub = cot[clean_names == cname].dropna(subset=[long_col, short_col]).set_index(date_col).sort_index()
                if sub.empty:
                    continue
                if hands_label:
                    s_h = ((sub[long_col] - sub[short_col]) / 10000.0).groupby(level=0).first()
                    combined_hands = combined_hands.combine_first(s_h)

                tot = (sub[long_col] + sub[short_col]).replace(0, np.nan)
                s_r = ((sub[long_col] - sub[short_col]) / tot * 100.0).groupby(level=0).first()
                combined_ratio = combined_ratio.combine_first(s_r)

            if hands_label and not combined_hands.empty:
                net_hands_dict[hands_label] = combined_hands.sort_index().rename(hands_label)
            if not combined_ratio.empty:
                net_ratio_dict[ratio_label] = combined_ratio.sort_index().rename(ratio_label)

        hands_df = pd.concat(list(net_hands_dict.values()), axis=1, sort=True) if net_hands_dict else pd.DataFrame()
        ratio_df = pd.concat(list(net_ratio_dict.values()), axis=1, sort=True) if net_ratio_dict else pd.DataFrame()
        print(f"[OK] CFTC 全球七大资产标准合约(含BTC)严格核验拉取成功 ({len(ratio_df)} 周, 列: {list(ratio_df.columns)})")
        return hands_df, ratio_df
    except Exception as e:
        print(f"[Warning] CFTC COT 抓取异常: {e}")
        return pd.DataFrame(), pd.DataFrame()


def fetch_sp500_breadth() -> pd.DataFrame:
    """实时计算标普500成分股高于50日/200日均线的市场宽度比例"""
    used_proxy = False
    try:
        r = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=HEADERS, timeout=30)
        sp_table = pd.read_html(io.StringIO(r.text))[0]
        tickers = [t.replace(".", "-") for t in sp_table["Symbol"].tolist()]
    except Exception:
        used_proxy = True
        tickers = [
            "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "BRK-B", "LLY", "AVGO", "JPM",
            "TSLA", "UNH", "XOM", "V", "PG", "MA", "JNJ", "COST", "HD", "ABBV",
            "WMT", "NFLX", "MRK", "CRM", "BAC", "CVX", "KO", "AMD", "PEP", "TMO",
            "LIN", "ORCL", "ADBE", "CSCO", "ACN", "MCD", "ABT", "WFC", "DHR", "QCOM",
            "TXN", "INTU", "AMGN", "PM", "ISRG", "IBM", "GE", "CAT", "NOW", "VZ",
            "DIS", "GS", "AMAT", "UBER", "SPGI", "RTX", "LOW", "HON", "UNP", "NEE",
        ]
    try:
        raw = yf.download(tickers, period="2y", auto_adjust=True, progress=False, group_by="column", threads=False)
        prices = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
        prices.index = pd.to_datetime(prices.index).tz_localize(None)
        ma50 = prices.rolling(50).mean()
        ma200 = prices.rolling(200).mean()
        valid_50 = ma50.notna().sum(axis=1).replace(0, np.nan)
        valid_200 = ma200.notna().sum(axis=1).replace(0, np.nan)
        suffix = "(60龙头代理)" if used_proxy else ""
        b50 = ((prices > ma50).sum(axis=1) / valid_50 * 100.0).rename(f"50日市场宽度(%){suffix}")
        b200 = ((prices > ma200).sum(axis=1) / valid_200 * 100.0).rename(f"200日市场宽度(%){suffix}")
        df = pd.concat([b50, b200], axis=1, sort=True).dropna()
        print(f"[OK] S&P 500 市场宽度计算完成 ({len(df)} 个交易日, 样本数: {prices.shape[1]})")
        return df.iloc[-260:]
    except Exception as e:
        print(f"[Warning] 市场宽度计算异常: {e}")
        return pd.DataFrame()


def align_tail(series_list, freq="W-FRI", n=156, max_ffill_steps=10) -> pd.DataFrame:
    """无断点连续对齐函数（支持月度发布滞后 10 周平滑衔接）"""
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
            tail_limit_idx = min(global_end, last_valid + pd.Timedelta(weeks=10))
            sub = df.loc[first_valid:tail_limit_idx, col].ffill(limit=max_ffill_steps)
            df.loc[first_valid:tail_limit_idx, col] = sub

    df = df.dropna(how="all")
    return df.iloc[-n:]


# ==========================================
# 3. 组装全部 26 张图表与五段式深度指南
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
    copper_gold = (mkt["HG=F"] / mkt["GC=F"] * 1000.0).rename("铜金比(×1000)")
    oil_gold = (mkt["CL=F"] / mkt["GC=F"] * 100.0).rename("油金比(×100)")

    dxy_raw = mkt["DX-Y.NYB"].dropna() if "DX-Y.NYB" in mkt.columns else pd.Series(dtype=float)
    dxy_label = "美元指数DXY"
    if dxy_raw.empty and "UUP" in mkt.columns:
        dxy_raw = mkt["UUP"].dropna()
        dxy_label = "美元指数ETF(UUP)"
    dxy = dxy_raw.rename(dxy_label)

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

    totalsa_raw = fetch_fred("TOTALSA")
    veh_sales = (totalsa_raw * 100.0).rename("全美汽车销量(万辆)")
    veh_yoy = yoy_pct(totalsa_raw).rename("汽车销量同比(%)")
    used_car_yoy = yoy_pct(fetch_fred("CUSR0000SETA02")).rename("二手车价格同比(%)")

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

    # 四、市场端：11大板块（严格统一同一 t0 起点=100）
    offense_raw = {
        "科技XLK": mkt["XLK"] / mkt["VTI"],
        "通讯XLC": mkt["XLC"] / mkt["VTI"],
        "可选消费XLY": mkt["XLY"] / mkt["VTI"],
        "金融XLF": mkt["XLF"] / mkt["VTI"],
        "工业XLI": mkt["XLI"] / mkt["VTI"],
        "能源XLE": mkt["XLE"] / mkt["VTI"],
    }
    sec_off_df = normalize_to_common_base(offense_raw, tail_days=252, freq="W-FRI")

    defense_raw = {
        "医疗XLV(防御)": mkt["XLV"] / mkt["VTI"],
        "必需消费XLP(防御)": mkt["XLP"] / mkt["VTI"],
        "公用事业XLU(防御)": mkt["XLU"] / mkt["VTI"],
        "房地产XLRE": mkt["XLRE"] / mkt["VTI"],
        "原材料XLB": mkt["XLB"] / mkt["VTI"],
    }
    sec_def_df = normalize_to_common_base(defense_raw, tail_days=252, freq="W-FRI")

    # 五、扩展模块（严格统一同一 t0 起点=100）
    ai_raw = {
        "AI芯片SMH / 标普SPY": mkt["SMH"] / mkt["SPY"],
        "AI应用AIQ / 标普SPY": mkt["AIQ"] / mkt["SPY"],
        "AI机器人BOTZ / 标普SPY": mkt["BOTZ"] / mkt["SPY"],
        "AI芯片SMH / 纳指QQQ": mkt["SMH"] / mkt["QQQ"],
        "AI芯片SMH / 罗素小盘IWM": mkt["SMH"] / mkt["IWM"],
        "AI芯片SMH / 道指DIA": mkt["SMH"] / mkt["DIA"],
    }
    ai_df = normalize_to_common_base(ai_raw, tail_days=378, freq="W-FRI")

    global_flow_tickers = {
        "SPY": "美股大盘(SPY)",
        "QQQ": "美股科技(QQQ)",
        "EEM": "新兴市场(EEM)",
        "TLT": "美债长债(TLT)",
        "GLD": "实物黄金(GLD)",
        "DBC": "大宗商品(DBC)",
        "IBIT": "比特币ETF(IBIT)",
    }
    global_flow_df = compute_monthly_flow_intensity(ohlcv, global_flow_tickers, window=20, tail_months=12)

    if not cot_ratio_df.empty:
        cot_monthly = cot_ratio_df.resample("ME").mean().dropna(how="all")
        if not global_flow_df.empty:
            common_months = global_flow_df.index.intersection(cot_monthly.index)
            inst_global_ratio_df = (
                cot_monthly.loc[common_months] if len(common_months) >= 6 else cot_monthly.iloc[-12:]
            )
        else:
            inst_global_ratio_df = cot_monthly.iloc[-12:]
    else:
        inst_global_ratio_df = pd.DataFrame()

    outflow_panic_df = compute_global_outflow_panic(ohlcv, vix, credit_spread, n_weeks=156)

    dxy_compare_raw = {
        dxy_label: dxy,
        "美股标普500(SPY)": mkt["SPY"],
        "新兴市场(EEM)": mkt["EEM"],
        "实物黄金(GLD)": mkt["GLD"],
        "美债长债(TLT)": mkt["TLT"],
        "比特币ETF(IBIT)": mkt["IBIT"],
    }
    dxy_compare_df = normalize_to_common_base(dxy_compare_raw, tail_days=504, freq="W-FRI")

    breadth_cols = list(breadth_df.columns) if not breadth_df.empty else []

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
            "what": "• 蓝线(左轴)：初请失业金人数(万人) —— 每周新增申领失业救济人数\n• 红线(左轴)：续请失业金人数(十万人) —— 连续两周及以上领取失业救济总规模\n• 黄线(右轴)：美国非农就业总人数年度同比增速(%)",
            "up_down": "• 蓝线(初请)：上涨 = 企业裁员加速（利空经济）；下跌 = 裁员减少、用工稳健。\n• 红线(续请)：上涨 = 失业者再就业难度加大、失业周期拉长；下跌 = 劳动力市场吸纳能力强。\n• 黄线(非农同比)：上涨 = 全美总就业岗位加速扩张；下跌 = 招聘需求放缓、薪资消费水源收窄。",
            "zones": "• ✅ 合理健康区间：初请 `19~24万人`、续请 `160~185万人(16.0~18.5)`、非农同比 `+1.0% ~ +2.2%`（充分就业且不引发恶性薪资通胀）。\n• 🚨 不合理/过热区间：初请 `<18万人` 且非农同比 `>+3.0%`（用工极度紧缺推高服务业通胀，迫使美联储紧缩）。\n• 🚨 不合理/衰退区间：初请 `>28万人` + 续请 `>200万人(红线>20)` + 非农同比 `<+0.8%`（裁员扩散，消费失速警报）。",
            "changes": "• 【组合① 蓝/红低位走平 + 黄线>1.2%】：黄金扩张组合，居民收入与消费稳健，美股盈利有底。\n• 【组合② 蓝线急升(>26万) + 红线滞后抬头(>190万)】：裁员扩散信号，未来1~2个季度零售与可选消费明显降温。\n• 【组合③ 蓝/红高位钝化 + 黄线跌破0.8%向0%逼近】：萨姆规则衰退预警触发区，美债长债(TLT)跑赢股票。\n• 【组合④ 蓝线脉冲冲高后迅速回落 + 黄线坚挺】：多为飓风、罢工或季节性扰动，不构成趋势反转。",
            "signal": "⚠️ 量化风控阈值：初请失业金连续4周站上 28万人 且续请突破 200万人(红线>20.0)，同时非农同比跌破 0.8%，确认消费失速预警，建议增配防御板块(XLP/XLV)与美债。",
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
            "what": "• 蓝线：个人总收入年度同比增速(%) —— 含薪资、股息利息及政府转移支付\n• 红线：个人消费支出(PCE)年度同比增速(%) —— 含商品与服务名义消费支出\n• 黄线：个人储蓄率(%) —— 税后可支配收入中未用于消费的留存比例",
            "up_down": "• 蓝线(个人收入)：上涨 = 家庭挣钱能力增强；下跌 = 薪资与经营性收入增速放缓。\n• 红线(消费支出)：上涨 = 居民花钱意愿与通胀名义支出扩大；下跌 = 消费支出收缩。\n• 黄线(储蓄率)：上涨 = 居民留存现金增多（消费降幅大于收入降幅，或预防性囤钱）；下跌 = 居民消耗老本或借债消费。",
            "zones": "• ✅ 合理健康区间：储蓄率 `5.0% ~ 7.5%`，且 `蓝线(收入增速) ≥ 红线(消费增速)`（量入为出的可持续内生增长）。\n• 🚨 不合理/过热区间：蓝/红线双双 `>+6.5%`（名义收支螺旋过热，服务业通胀顽固）。\n• 🚨 不合理/透支衰退区间：储蓄率 `<3.5%` 且 `红线 > 蓝线`（靠高息信用卡透支维持消费，随时断崖）；或储蓄率骤升 `>9%` 伴随红线暴跌（衰退期恐慌捂钱包）。",
            "changes": "• 【组合① 蓝线 ≥ 红线 + 黄线稳居5%~7.5%】：健康内生增长组合，软着陆概率最高。\n• 【组合② 红线持续高于蓝线 + 黄线单边跌破4%】：透支型消费陷阱，一旦储蓄率触及3%底线，消费将断崖刹车。\n• 【组合③ 红线(消费)降幅远超蓝线(收入) + 黄线(储蓄率)跳升】：衰退期预防性储蓄激增（或财政补贴托底收入但居民拒不消费），可选消费(XLY)承压。\n• 【组合④ 蓝/红线同步维持5%以上高位】：名义收入与支出双过热，美联储难以快速降息。",
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
            "what": "• 蓝线：全美成屋销售量同比(%) —— 二手房真实置换需求（占交易量85%以上）\n• 红线：全美新屋销售量同比(%) —— 开发商一手房签约量（对按揭利率最敏感）\n• 黄线：标普20大都市房价同比(%)  • 绿线：全美新房中位价同比(%)",
            "up_down": "• 蓝/红线(成交量)：上涨 = 房贷利率回落或刚需入场，交易活跃；下跌 = 高利率抑制成交或锁死二手房置换流动性。\n• 黄/绿线(房价)：上涨 = 家庭房产财富效应增强，但也推高未来CPI房租项；下跌 = 资产价格缩水与抵押物贬值。",
            "zones": "• ✅ 合理健康区间：成屋/新屋销量同比 `0% ~ +12%`，20大都市房价同比 `+2.0% ~ +6.0%`（量价温和复苏且不引发房租二次通胀）。\n• 🚨 不合理/过热泡沫区间：房价同比 `>+12%`（购买力恶化且迫使央行维持高利率）。\n• 🚨 不合理/冰冻衰退区间：成屋与新屋销量同比双双 `<-15%` 且库存大幅积压导致房价转负 `<-3%`。",
            "changes": "• 【组合① 蓝/红线由负转正上冲 + 黄/绿线仍在低位】：利率回落驱动量升价稳，预计3~6个月后核心都市房价(黄线)加速回升。\n• 【组合② 蓝/红线大幅转负 + 待售库存高企】：需求萎缩式量缩价跌，房价将被迫降价补跌。\n• 【组合③ 蓝线(成屋)低迷但红线(新屋)走强 + 黄线(二手房价)坚挺】：高利率「锁死效应(Lock-in)」，二手房房东惜售导致供给枯竭支撑房价，买家转向开发商降价促销的新房。\n• 【组合④ 四线全线深陷负值区】：地产深度去库周期，家居建材与地区银行承压。",
            "signal": "💡 量化风控信号：新屋(红线)与成屋(蓝线)销售同比连续3个月站稳 0% 上方，是做多地产建筑商(ITB/XHB)与家装零售的右侧确认信号。",
        },
        {
            "id": "04",
            "category": "消费",
            "chart_type": "LINE",
            "title": "04. 汽车消费与二手车通胀 (Auto Sales)",
            "df": align_tail([veh_sales, veh_yoy, used_car_yoy], freq="ME", n=60),
            "right_cols": ["汽车销量同比(%)", "二手车价格同比(%)"],
            "left_title": "全美汽车销量 (万辆)",
            "right_title": "销量 / 二手车价格同比 (%)",
            "what": "• 蓝线(左轴)：全美轻型汽车月度折年总销量(万辆)\n• 红线(右轴)：全美汽车销量年度同比增速(%)\n• 黄线(右轴)：美国CPI二手车与卡车价格年度同比增速(%)",
            "up_down": "• 蓝/红线(汽车销量)：上涨 = 居民大件耐用品消费信心强、车贷融资顺畅；下跌 = 高车贷利率或就业担忧抑制换车。\n• 黄线(二手车价格同比)：上涨 = 二手车价格反弹推高核心商品CPI；下跌 = 车市供给充足、核心商品通胀降温。",
            "zones": "• ✅ 合理健康区间：月度折年销量 `1600~1750万辆`、销量同比 `0% ~ +8%`、二手车价格同比 `-5% ~ +2%`（销量稳健且不推升通胀）。\n• 🚨 不合理/通胀过热区间：黄线(二手车价格同比)单边急升突破 `+8%`（核心CPI反弹警报）。\n• 🚨 不合理/需求萎缩区间：蓝线(销量)跌破 `1400万辆` 且红线持续负增长（大件消费熄火）。",
            "changes": "• 【组合① 蓝线稳步攀升(>1600万辆) + 黄线温和负增长】：最佳无通胀复苏组合，车价回归理性刺激换车需求。\n• 【组合② 黄线暴涨(>10%) + 蓝线跌破1450万辆】：供给瓶颈/滞胀组合，车价飙升反噬购买力并推高核心CPI。\n• 【组合③ 蓝线跌破1400万辆 + 红线负增长 + 黄线加速下跌】：需求坍塌式通缩，车企被迫打价格战去库。\n• 【组合④ 红线转正但蓝线仍在1500万辆下方】：低基数下的弱修复，尚未恢复至充分扩张水平。",
            "signal": "💡 量化风控信号：二手车价格同比(黄线)持续处于负值或低位是核心CPI降温的领先指标；月度折年销量重返 1600万辆 上方代表消费终端韧性强劲。",
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
            "what": "• 蓝线：总体零售与食品服务销售额同比(%) —— 含汽车与加油站\n• 红线：核心零售(剔除高波动汽车)同比(%) —— 反映日常商品与餐饮真实消费\n• 黄线：电商非实体店零售同比(%)",
            "up_down": "• 蓝/红线(整体与核心零售)：上涨 = 消费者日常刷卡热度上升；下跌 = 居民缩减日常开支。\n• 黄线(电商零售)：上涨 = 线上消费渗透率提升；若黄线远强于红线，往往伴随消费者转向线上比价与折扣消费。",
            "zones": "• ✅ 合理健康区间：总体与核心零售 `+3.5% ~ +6.0%`，电商零售 `+8.0% ~ +12.0%`（名义增速稳健跑赢2%~3%温和通胀）。\n• 🚨 不合理/过热区间：蓝/红线 `>+8.5%`（需求过热推高商品通胀）。\n• 🚨 不合理/实际萎缩区间：红线 `<+1.5%` 且低于同期 CPI（剔除涨价后实际消费量已在萎缩）。",
            "changes": "• 【组合① 蓝/红线稳居3.5%~6% + 黄线维持8%~12%】：消费结构健康扩张，消费龙头与支付网络盈利稳健。\n• 【组合② 黄线(电商)逆势走强而红线(核心零售)走弱】：消费降级与渠道迁移，居民减少线下冲动购物转向线上比价。\n• 【组合③ 蓝线大幅低于红线】：油价下跌或汽车销量疲软拖累表面数据，但核心日常消费依然坚挺。\n• 【组合④ 蓝线与红线同步跌破1.5%甚至转负】：剔除通胀后实际消费全面负增长，衰退高度确立。",
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
            "what": "• 蓝线：私人住宅建房许可同比(%)  • 红线：新屋开工同比(%)\n• 黄线：待售新房总库存同比(含在建与完工)  • 绿线：20大都市房价同比(%)",
            "up_down": "• 蓝/红线(许可与开工)：上涨 = 开发商看好后市、扩大拿地施工（拉动建材家电）；下跌 = 开发商收缩新项目。\n• 黄线(待售新房库存)：需结合许可判断——若许可同步高增，说明期房预售与施工旺盛；若许可暴跌而库存飙升，说明现房滞销积压。",
            "zones": "• ✅ 合理健康区间：建房许可与开工 `0% ~ +10%`，待售库存同比 `-5% ~ +8%`（供需紧平衡）。\n• 🚨 不合理/过热区间：许可与开工 `>+18%` 且房价 `>+10%`（地产施工与通胀过热）。\n• 🚨 不合理/滞销去库区间：黄线(库存) `>+15%` 同时伴随蓝/红线(许可/开工) `<-10%`（现房积压迫使开发商降价促销）。",
            "changes": "• 【组合① 蓝/红线拐头转正 + 黄线库存低位 + 绿线企稳】：供不应求主升浪，未来6~12个月房价与住宅投资拉动GDP。\n• 【组合② 黄线库存同比飙升(>15%) + 蓝线持续深度负增长(<-10%)】：供给过剩去库周期，开发商积压现房被迫降价并削减开工。\n• 【组合③ 蓝线与黄线同步温和上行】：开发商加快拿地并扩充在建期房供给，拉动建材与家装产业链。\n• 【组合④ 绿线房价高位但蓝/红线持续下滑】：高利率抑制新供给，未来一旦降息将引发新一轮房荒与房价反弹。",
            "signal": "💡 量化风控信号：建房许可(蓝线)增速领先未来6~9个月建材、家电与家居需求；待售库存回落 + 许可转正是地产建筑商(ITB)最佳配置窗口。",
        },
        {
            "id": "07",
            "category": "供给",
            "chart_type": "LINE",
            "title": "07. 耐用品订单与制造业/服务业PMI (Supply Orders)",
            "df": align_tail([dur_yoy, core_cap_yoy, mfg_pmi, svc_pmi], freq="ME", n=60),
            "right_cols": [mfg_pmi.name, svc_pmi.name],
            "left_title": "订单同比 (%)",
            "right_title": "ISM PMI 指数 (50为荣枯线)",
            "what": f"• 蓝线(左轴)：耐用品总订单同比(%)  • 红线(左轴)：核心资本品订单同比(%)\n• 黄线(右轴)：{mfg_pmi.name}  • 绿线(右轴)：{svc_pmi.name}",
            "up_down": "• 蓝/红线(订单同比)：上涨 = 企业设备投资(CapEx)与扩产意愿增强；下跌 = 企业削减资本开支。\n• 黄/绿线(PMI)：站上50并上涨 = 行业加速扩张；跌破50并下跌 = 行业陷入萎缩。",
            "zones": "• ✅ 合理健康区间：核心资本品订单 `+2% ~ +8%`，制造业PMI `50.0 ~ 55.0`，服务业PMI `52.0 ~ 57.0`。\n• 🚨 不合理/过热区间：双PMI `>59.0` 且订单 `>+12%`（供应链瓶颈与通胀飙升）。\n• 🚨 不合理/全面萎缩区间：核心资本品订单负增长且制造业与服务业PMI双双 `<48.5`（实体经济全面衰退）。",
            "changes": "• 【组合① 红线转正上行 + 黄线(制造业PMI)>50 + 绿线(服务业PMI)>52】：全面复苏扩张组合，顺周期工业(XLI)、金融与科技齐涨。\n• 【组合② 蓝线暴涨暴跌而红线走平】：仅受波音大飞机或军工订单单月集中入账扰动，属于统计噪音。\n• 【组合③ 黄线深陷50下方收缩 + 绿线稳居52以上】：服务强、制造弱的K型分化，资金抱团AI科技与服务龙头。\n• 【组合④ 红线持续负增长 + 黄/绿线双双跌破50】：制造业收缩向服务业蔓延，全面衰退警报拉响。",
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
            "what": "• 蓝线：纽约联储每周经济指数 (WEI) —— 整合10项高频实体指标，实时映射季度实际GDP年化同比增速(%)",
            "up_down": "• 蓝线上涨：全美高频零售、货运、发电、原钢与用工活动加速，未来季度GDP预期上修。\n• 蓝线下跌：高频实体经济动能走弱，未来季度GDP增速放缓甚至转负。",
            "zones": "• ✅ 合理健康区间：`+1.5% ~ +2.8%`（最理想的金发姑娘经济走廊，温和增长且不引发恶性通胀）。\n• 🚨 不合理/过热区间：`>+3.2%`（经济过热推升10年期美债收益率，压制高估值成长股）。\n• 🚨 不合理/衰退区间：`<+0.5%` 并跌破 `0.0%`（历史上高度对应美国经济已陷入实质性衰退）。",
            "changes": "• 【情景① 蓝线稳居 1.5% ~ 2.8% 黄金走廊】：金发姑娘经济，最利于美股长牛。\n• 【情景② 蓝线持续攀升突破 3.0%】：实体经济动能强劲，企业盈利上修，但需警惕利率上行。\n• 【情景③ 蓝线从高位连续跌破 1.5% 向 0.5% 俯冲】：高频货运、发电与零售同步走弱，美债收益率将见顶暴跌。\n• 【情景④ 蓝线跌破 0% 负值区】：实质性衰退确认，全面切入防御与长债。",
            "signal": "🚨 量化风控阈值：WEI 连续3周站上 3.0% 提示警惕再通胀与利率上行；一旦跌破 0.5% 并逼近 0% 零轴，立即启动衰退防守配置。",
        },
        {
            "id": "09",
            "category": "供给",
            "chart_type": "LINE",
            "title": "09. 铜金比与PPI/PMI (Industrial & PPI)",
            "df": align_tail([copper_gold, ppi_yoy, mfg_pmi], freq="W-FRI", n=180, max_ffill_steps=10),
            "right_cols": ["PPI出厂价格同比(%)", mfg_pmi.name],
            "left_title": "铜金比 (×1000)",
            "right_title": f"PPI同比(%) / {mfg_pmi.name}",
            "what": f"• 蓝线(左轴)：铜金比（COMEX铜 ÷ 黄金 × 1000）  • 红线(右轴)：PPI工业出厂价格同比(%)  • 黄线(右轴)：{mfg_pmi.name}",
            "up_down": "• 蓝线(铜金比)：上涨 = 全球实体工业需求跑赢货币避险（领先PPI与PMI 2~4个月）；下跌 = 工业需求萎缩或避险升温。\n• 红线(PPI)：上涨 = 上游原材料与出厂通胀压力增大；下跌 = 制造业通缩压力上升。\n• 黄线(制造业PMI)：上涨 = 制造业景气扩张；下跌 = 制造业收缩。",
            "zones": "• ✅ 合理健康区间：铜金比稳步温和上行、PPI同比 `0.0% ~ +3.0%`、制造业PMI `50.0 ~ 54.0`。\n• 🚨 不合理/过热通胀区间：铜金比急飙且 PPI `>+6.0%`（上游成本恶性通胀挤压中下游利润）。\n• 🚨 不合理/工业通缩区间：铜金比破位暴跌且 PPI `<-2.0%`、PMI `<47.0`（全球制造业深度通缩）。",
            "changes": "• 【组合① 蓝线(铜金比)持续走强 + 黄线(PMI)回升 + 红线(PPI)抬头】：全球工业再通胀共振，利好工业(XLI)、原材料(XLB)与资源股。\n• 【组合② 蓝线提前见顶大幅下挫 + 红线(PPI)仍在高位】：上游通胀见顶顶背离，未来1~2个季度PPI将回落，长久期美债迎来配置良机。\n• 【组合③ 蓝线在低位率先拐头向上 + 红线/黄线仍处低谷】：工业复苏领先底背离，左侧布局顺周期的高胜率信号。\n• 【组合④ 蓝线破位下跌 + 黄线深陷48以下】：全球制造业通缩与避险情绪双杀，黄金显著跑赢工业金属。",
            "signal": "💡 量化风控信号：铜金比是宏观交易员的「高频工业先行指标」，当蓝线趋势方向与红线(PPI)背离持续超过8周时，官方PPI与PMI大概率向铜金比方向回归。",
        },
        {
            "id": "10",
            "category": "供给",
            "chart_type": "LINE",
            "title": "10. 油金比与CPI (Energy & CPI)",
            "df": align_tail([oil_gold, cpi_yoy], freq="W-FRI", n=180, max_ffill_steps=10),
            "right_cols": ["CPI通胀同比(%)"],
            "left_title": "油金比 (×100)",
            "right_title": "CPI通胀同比 (%)",
            "what": "• 蓝线(左轴)：油金比（WTI原油 ÷ 黄金 × 100）  • 红线(右轴)：美国CPI消费者物价指数同比(%)",
            "up_down": "• 蓝线(油金比)：上涨 = 剔除货币贬值后的真实能源成本上升（领先CPI 1~3个月）；下跌 = 能源成本压力缓解、通胀趋降。\n• 红线(CPI同比)：上涨 = 终端消费通胀反弹，限制美联储降息空间；下跌 = 通胀回落，利好货币宽松。",
            "zones": "• ✅ 合理健康区间：油金比低位平稳或温和下行、CPI同比 `+1.8% ~ +2.8%`（贴近美联储2%目标）。\n• 🚨 不合理/二次通胀区间：油金比单边飙升且 CPI `>+3.5%`（能源驱动型二次通胀，股债估值双杀）。\n• 🚨 不合理/通缩衰退区间：油金比暴跌且 CPI `<+1.0%`（总需求坍塌引发通缩担忧）。",
            "changes": "• 【组合① 蓝线(油金比)持续飙升 + 红线(CPI)同步抬头】：能源驱动型二次通胀，美联储被迫推迟降息甚至加息。\n• 【组合② 蓝线见顶大幅回落 + 红线(CPI)滞后高位】：通胀见顶确认组合，能源成本回落将在1~3个月内拉低CPI，利好科技股(QQQ)与美债(TLT)。\n• 【组合③ 蓝线低位徘徊 + 红线(CPI)顽固居于3%以上】：通胀由房租与薪资等核心服务业粘性主导，降息节奏缓慢。\n• 【组合④ 蓝线急跌 + 红线跌破2%】：能源与需求双弱，通缩预期升温，防御板块与黄金占优。",
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
            "what": "• 蓝线(左轴)：铜金比(全球增长预期)  • 红线(左轴)：油金比(全球通胀预期)  • 黄线(右轴)：10年期美债收益率(%)",
            "up_down": "• 蓝线上涨/下跌 = 市场定价经济增长加速 / 放缓。\n• 红线上涨/下跌 = 市场定价能源通胀升温 / 降温。\n• 黄线上涨/下跌 = 无风险利率走高（压制股票估值）/ 利率走低（支撑股票估值与长债价格）。",
            "zones": "• ✅ 合理健康区间：蓝线温和上行 + 红线低位平稳 + 黄线(10年美债)运行在 `3.50% ~ 4.30%`（无通胀温和复苏）。\n• 🚨 不合理/利率冲击区间：黄线急升突破 `4.75% ~ 5.00%`（高利率直接刺破高估值成长股）。\n• 🚨 不合理/滞胀与衰退区间：蓝跌+红涨+黄涨（最凶险滞胀）或蓝跌+红跌+黄线暴跌破 `3.20%`（深度衰退避险）。",
            "changes": "• 【象限① 蓝涨 + 红涨 + 黄涨】：经济过热/再通胀交易，超配能源(XLE)、金融(XLF)、工业(XLI)，规避长债(TLT)。\n• 【象限② 蓝涨 + 红跌 + 黄稳/微降】：金发姑娘复苏交易，美股科技(XLK)与成长股迎来最佳牛市环境。\n• 【象限③ 蓝跌 + 红跌 + 黄跌】：经济衰退/降息避险交易，资金涌入10年期美债(TLT)与黄金(GLD)。\n• 【象限④ 蓝跌 + 红涨 + 黄涨】：滞胀交易(Stagflation)，股债双杀，唯黄金与现金为王。",
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
            "what": "• 蓝线：联邦基金利率  • 紫线：3个月美债  • 绿线：2年期美债  • 黄线：5年期美债  • 红线：10年期美债",
            "up_down": "• 短端(蓝/紫/绿)上涨/下跌 = 市场定价美联储加息 / 降息预期。\n• 长端(红线10Y)上涨/下跌 = 长期名义GDP增速与通胀预期升温 / 降温。",
            "zones": "• ✅ 合理健康区间：正常正向收益率曲线，长端高于短端（`红 > 黄 > 绿 > 紫`，10Y减2Y利差 `+0.4% ~ +1.5%`）。\n• 🚨 不合理/紧缩倒挂区间：短端高于长端（`紫/绿 > 红`，**收益率曲线倒挂**，银行借短贷长无利可图，信用逐步收缩）。\n• 🚨 不合理/衰退牛陡区间：长期倒挂后绿线(2Y)突然暴跌下穿红线(10Y)并迅速拉开正利差（市场抢跑衰退紧急降息，伴随股市剧震）。",
            "changes": "• 【形态① 紫/绿线 > 红线（短端高于长端）】：收益率曲线倒挂，货币政策处于限制性紧缩区间。\n• 【形态② 倒挂后绿线(2Y)暴跌、红线(10Y)平稳】：牛市变陡(Bull Steepener)，往往对应经济衰退正式落地与股市剧烈震荡。\n• 【形态③ 红线(10Y)飙升速度快于绿线(2Y)】：熊市变陡(Bear Steepener)，市场定价长期通胀或财政赤字供给过剩。\n• 【形态④ 10Y > 5Y > 2Y > 3M 有序排列且温和下行】：健康正向曲线与预防式降息周期，最利好美股长牛。",
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
            "what": "• 蓝线(左轴)：CCC级高收益垃圾债信用利差(%)  • 红线(左轴)：VIX恐慌指数  • 黄线(右轴)：标普500指数",
            "up_down": "• 蓝线(信用利差)：上涨 = 弱资质企业融资成本飙升、违约风险恶化；下跌 = 信用融资环境宽松、企业违约率低。\n• 红线(VIX)：上涨 = 期权市场恐慌对冲升温、股市波动率扩大；下跌 = 市场情绪平稳。",
            "zones": "• ✅ 合理健康区间：CCC级信用利差 `5.5% ~ 8.0%`，VIX恐慌指数 `12.0 ~ 19.0`（信用血管畅通，低波动慢牛）。\n• 🚨 不合理/自满低波区间：VIX `<11.5` 且信用利差 `<5.2%`（市场过度自满，对突发黑天鹅毫无防备）。\n• 🚨 不合理/系统性危机区间：CCC利差 `>11.0%` 且 VIX `>30.0`（信用紧缩与流动性危机爆发，机构被迫去杠杆抛售）。",
            "changes": "• 【组合① 蓝线低位平稳(<7.5%) + 红线(VIX)<18 + 黄线稳步上行】：信用融资宽松，美股处于低波动慢牛黄金期。\n• 【组合② 黄线(标普)仍在高位但蓝线(信用利差)提前抬头扩大】：债市先行顶背离预警，聪明资金撤离公司债，股市随后补跌。\n• 【组合③ 红线(VIX)脉冲跳升(>25)但蓝线(信用利差)纹丝不动】：仅为股市技术性洗盘或期权短期对冲扰动，是逢低买入机会。\n• 【组合④ 蓝线急剧飙升(>11%~13%) + 红线突破30~35】：系统性信用紧缩爆发，必须果断降仓防守。",
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
            "what": "• 蓝线(左轴)：AAII散户看涨%减看跌%净差值  • 红线(左轴)：20周移动均线  • 黄线(右轴)：标普500指数",
            "up_down": "• 蓝/红线上涨 = 个人投资者越来越乐观追涨（反向指标，增量买盘逐步耗尽）。\n• 蓝/红线下跌 = 个人投资者恐慌看空割肉（反向指标，卖盘抛压逐步出清）。",
            "zones": "• ✅ 合理健康区间：`-10% ~ +15%`（多空分歧适度，牛市在怀疑中稳步推进）。\n• 🚨 不合理/散户极度亢奋区间：`>+25%`（散户全面满仓追涨，市场对利空极度脆弱，易发生技术性回调）。\n• 🚨 不合理/散户恐慌出清区间：`<-25%`（尤其跌破 `-30%`，恐慌割肉盘耗尽，未来3~6个月正收益概率超80%）。",
            "changes": "• 【组合① 蓝线飙升至 +25% 以上 + 红线同步突破 +15%】：散户看多极度拥挤，易发生技术性回调。\n• 【组合② 蓝线暴跌至 -25% 以下 + 黄线(标普)回踩支撑】：散户恐慌性割肉，抛压接近衰竭，高胜率逆向买点。\n• 【组合③ 黄线(标普)高位小跌，但蓝线迅速由正转负】：最健康的牛市洗盘结构，指数未跌多少但散户已被吓跑。\n• 【组合④ 蓝线长期徘徊在 -10%~0% 而黄线创新高】：在怀疑中上涨(Wall of Worry)，牛市生命力持久。",
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
            "what": "• 蓝线(左轴)：CFTC资管机构(共同基金/养老金/保险)标普500期货多头/(多头+空头)占比(%)  • 红线(左轴)：20周均线  • 黄线(右轴)：标普500指数",
            "up_down": "• 蓝线上涨 = 长线大型资管机构增加净多头配置。\n• 蓝线下跌 = 长线资管机构减多增空、触发风控去杠杆。",
            "zones": "• ✅ 合理健康区间：`78% ~ 90%`（公募与养老金以做多为主，牛市中多头占比常态维持在80%以上，健康且有趋势惯性）。\n• 🚨 不合理/机构极度拥挤区间：`≥92%`（长线资管空头对冲几乎归零、多头极度拥挤，谨防宏观利空引发减仓）。\n• 🚨 不合理/机构深度去杠杆区间：`≤72%`（长线机构罕见大幅砍仓并建立空头对冲，卖盘出清后往往孕育大级别底部）。",
            "changes": "• 【组合① 蓝线升至 92% 以上历史极值 + 贴近或高于红线】：长线机构多头配置极度拥挤，短期上行斜率易趋缓。\n• 【组合② 蓝线大幅下穿红线并回落至 72% 以下】：长线机构风控砍仓接近尾声，卖盘充分出清。\n• 【组合③ 黄线(标普)创新高，但蓝线持续低于红线且重心下移】：机构派发顶背离，长线大资金正在借指数冲高悄然减仓。\n• 【组合④ 蓝线强势上穿红线且黄线同步突破】：长线资管机构开启新一轮配置加仓，趋势行情确立。",
            "signal": "🎯 机构仓位阈值：资管多头占比 ≥ 92% 警惕仓位拥挤；当蓝线大幅跌破 72% 后首次拐头向上穿过红线(20周均线)，为高胜率中长线加仓信号。",
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
            "what": "• 蓝线(左轴)：CBOE个股看跌期权(Put) ÷ 看涨期权(Call)每日成交量比值  • 红线(左轴)：20日均线  • 黄线(右轴)：标普500指数",
            "up_down": "• 蓝/红线上涨 = 市场花重金买入Put防暴跌，恐慌对冲情绪升温。\n• 蓝/红线下跌 = 市场疯狂买入Call追涨，投机自满情绪膨胀。",
            "zones": "• ✅ 合理健康区间：`0.55 ~ 0.72`（多空对冲均衡，市场沿主趋势稳健推进）。\n• 🚨 不合理/极度自满低波区间：单日 `<0.48` 或 20日均线 `<0.55`（投机Call过度拥挤，极易出现快速洗盘爆多）。\n• 🚨 不合理/恐慌买Put高潮区间：单日 `>0.95` 或 20日均线 `>0.78`（恐慌情绪集中宣泄，对应短期重要底部）。",
            "changes": "• 【组合① 蓝线单日脉冲冲高(>0.95) + 红线(20日均线)显著抬升(>0.75)】：恐慌性买Put达到高潮，对应短期重要底部拐点。\n• 【组合② 蓝线持续压低至 0.48 以下 + 红线跌破 0.55】：投机者沉迷买入Call追涨，短期极易快速洗盘。\n• 【组合③ 黄线(标普)继续创新低，但蓝线高点逐步下移】：恐慌钝化底背离，下跌动能衰竭，反弹一触即发。\n• 【组合④ 蓝线温和运行在 0.58~0.70 区间】：多空对冲均衡，沿主趋势推进。",
            "signal": "🎯 期权逆向阈值：Equity Put/Call 单日突破 1.0 或 20日均线(红线)升破 0.78，为高胜率短线抄底信号；低于 0.50 警惕短期回调。",
        },
        {
            "id": "17",
            "category": "市场",
            "chart_type": "LINE",
            "title": "17. S&P 500 非商业净持仓 (CFTC COT)",
            "df": align_tail([cot_hands_df["标普500投机净持仓(万手)"], spx], freq="W-FRI", n=180)
            if not cot_hands_df.empty and "标普500投机净持仓(万手)" in cot_hands_df
            else align_tail([spx], freq="W-FRI", n=180),
            "right_cols": ["标普500指数"],
            "left_title": "非商业净持仓 (万手)",
            "right_title": "标普500点位",
            "what": "• 蓝线(左轴)：CFTC 标普500期货非商业(含资管+杠杆对冲基金)净持仓(万手)  • 红线(右轴)：标普500指数\n• 注：高利率环境下受「期现基差套利(做多现货+做空期货)」影响，净持仓常呈结构性负值，重点看边际斜率变化。",
            "up_down": "• 蓝线上涨 = 机构平空翻多或基差空头减仓、净多头边际改善。\n• 蓝线下跌 = 机构增加期货空头对冲或套利空单扩大。",
            "zones": "• ✅ 合理健康区间：围绕近1年均值上下波动（斜率温和向上或走平）。\n• 🚨 不合理/净多极端拥挤区间：`>+15万手`（纯方向性多头过于拥挤，缺乏增量资金接力）。\n• 🚨 不合理/净空极端拥挤区间：`<-25万手`（若非基差套利而是单边做空，一旦市场企稳极易触发剧烈轧空）。",
            "changes": "• 【组合① 蓝线跌入近3年极端净空分位后显著拐头向上 + 红线(标普)站稳均线】：空头回补与轧空(Short Squeeze)助推主升浪。\n• 【组合② 蓝线持续攀升至历史极端净多(+15万手以上) + 红线滞涨】：投机多头高度拥挤，极易触发止盈抛盘。\n• 【组合③ 蓝线在负值区缓慢抬升 + 红线稳步上行】：空头持续回补提供增量买盘支撑。\n• 【组合④ 蓝线急剧下挫(单周净减超5万手) + 红线破位】：宏观对冲基金集中建立方向性空头，中期调整确立。",
            "signal": "🎯 边际拐点信号：因期现套利使标普期货长期呈结构性净空，切勿仅因数值<0而做空；当净持仓(蓝线)处于近3年最低10%分位且连续2周拐头向上、同时标普站稳20日均线时，重点关注轧空行情。",
        },
        {
            "id": "18",
            "category": "市场",
            "chart_type": "LINE",
            "title": "18. Nasdaq 100 非商业净持仓 (CFTC COT)",
            "df": align_tail([cot_hands_df["纳指100投机净持仓(万手)"], ndx], freq="W-FRI", n=180)
            if not cot_hands_df.empty and "纳指100投机净持仓(万手)" in cot_hands_df
            else align_tail([ndx], freq="W-FRI", n=180),
            "right_cols": ["纳斯达克100指数"],
            "left_title": "非商业净持仓 (万手)",
            "right_title": "纳斯达克100点位",
            "what": "• 蓝线(左轴)：CFTC 纳指100期货非商业净多空持仓(万手)  • 红线(右轴)：纳斯达克100指数",
            "up_down": "• 蓝线上涨 = 机构加码押注大型科技成长与AI龙头。\n• 蓝线下跌 = 机构削减科技股多头敞口或对冲回调风险。",
            "zones": "• ✅ 合理健康区间：`-2万手 ~ +5万手`（科技板块资金流向健康）。\n• 🚨 不合理/科技多头拥挤区间：`>+7万手`（科技股杠杆多头拥挤度过高，警惕财报季获利了结）。\n• 🚨 不合理/科技超跌净空区间：`<-4万手`（科技股去杠杆充分，酝酿超跌反弹）。",
            "changes": "• 【跨图组合① 图17(标普)深度净空 + 本图(纳指)维持净多】：机构做多AI与纳指巨头、做空传统权重股，纳指大幅跑赢。\n• 【组合② 本图蓝线触及历史极端净多高位后拐头向下】：科技股杠杆拥挤度过高，警惕财报季回调。\n• 【组合③ 本图蓝线跌入深度净空区后回升 + 红线(纳指)止跌】：科技板块经历充分去杠杆洗盘，迎来新一轮配置窗口。\n• 【跨图组合④ 图17与图18同步大幅翻空】：机构无差别削减全市场股票敞口，进入全面防御。",
            "signal": "💡 跨图联动信号：对比图17与图18的净持仓方向剪刀差，可精准判断当前是「AI科技独立抱团牛市」还是「全市场风格切换」。",
        },
        {
            "id": "19",
            "category": "市场",
            "chart_type": "LINE",
            "title": "19. S&P 500 市场宽度 (Market Breadth)",
            "df": align_tail([breadth_df[breadth_cols[0]], breadth_df[breadth_cols[1]], spx], freq=None, n=250)
            if len(breadth_cols) >= 2
            else align_tail([spx], freq=None, n=250),
            "right_cols": ["标普500指数"],
            "left_title": "站上均线成分股占比 (%)",
            "right_title": "标普500点位",
            "what": f"• 蓝线(左轴)：{breadth_cols[0] if len(breadth_cols)>=2 else '50日市场宽度(%)'}  • 红线(左轴)：{breadth_cols[1] if len(breadth_cols)>=2 else '200日市场宽度(%)'}  • 黄线(右轴)：标普500指数",
            "up_down": "• 蓝/红线上涨 = 上涨行情向多数成分股扩散，牛市根基牢固。\n• 蓝/红线下跌 = 内部多数个股走弱（若此时黄线指数仍涨，则是少数巨头掩护撤退的危险背离）。",
            "zones": "• ✅ 合理健康区间：50日宽度 `45% ~ 75%`，200日宽度 `55% ~ 78%`（健康的轮动普涨结构）。\n• 🚨 不合理/短期过热区间：50日宽度 `>85%` 且 200日宽度 `>80%`（全市场普涨亢奋，短期追高盈亏比下降）。\n• 🚨 不合理/极端超卖大底区间：50日宽度 `<15%`（短期极度超卖；若50日与200日宽度同时 `<15%`，为数年一遇中长线战略大底）。",
            "changes": "• 【组合① 黄线(标普)创新高 + 蓝线与红线重心明显下移】：危险顶部背离，仅靠少数巨头硬撑指数，警惕指数补跌。\n• 【组合② 蓝线暴跌至 15% 以下 + 红线仍稳居 45% 以上】：牛市中的健康深度洗盘，长线牛市未坏，极佳波段买点。\n• 【组合③ 蓝线(<15%) 与 红线(<20%) 双双跌入冰点后蓝线拐头向上】：罕见熊市出清大底，迎来级别最大全面反弹。\n• 【组合④ 蓝线突破 85% + 红线突破 80%】：全市场普涨亢奋，易出现震荡整固。",
            "signal": "🎯 宽度买卖极区：50日宽度 > 85% 提示短期过热；< 15% 提示短期超卖。若 50日与200日宽度同时 < 15%，为数年一遇的中长线战略抄底区！",
        },
        {
            "id": "20A",
            "category": "市场",
            "chart_type": "LINE",
            "title": "20A. 美股进攻与成长板块轮动 (6大进攻板块)",
            "df": sec_off_df,
            "right_cols": [],
            "left_title": "相对全市场VTI强弱 (同一起点=100)",
            "right_title": "",
            "what": "• 6大进攻/周期板块ETF ÷ 全市场VTI 标准化相对强弱（同一起点=100）：\n• 蓝:科技XLK | 红:通讯XLC | 黄:可选消费XLY | 绿:金融XLF | 紫:工业XLI | 青:能源XLE",
            "up_down": "• 曲线上涨(斜率向上) = 该板块持续跑赢美股大盘，机构资金净流入。\n• 曲线下跌(斜率向下) = 该板块持续跑输美股大盘，机构资金净流出。",
            "zones": "• ✅ 合理健康区间：各进攻板块围绕 `95 ~ 112` 有序轮动接力，领涨主线稳居 `100` 上方。\n• 🚨 不合理/单一拥挤区间：单一板块冲高突破 `125` 以上且斜率垂直陡峭（资金极度抱团泡沫，防风格切换急跌）。\n• 🚨 不合理/全线撤退区间：六大进攻板块全部跌破 `100` 且斜率向下（机构全面撤出风险资产）。",
            "changes": "• 【组合① 科技XLK(蓝) + 通讯XLC(红) 领跑且稳居100上方】：AI科技与互联网巨头主导牛市，成长风格占优。\n• 【组合② 金融XLF(绿) + 工业XLI(紫) + 能源XLE(青) 接力走强，科技走平】：软着陆与再通胀风格切换，顺周期补涨。\n• 【组合③ 可选消费XLY(黄) 率先破位下行】：居民消费承压的股市先行映射，警惕零售与消费链盈利下修。\n• 【组合④ 六大进攻板块全线跌破100且斜率向下】：机构全面撤出进攻头寸，结合图20B确认避险。",
            "signal": "🎯 轮动配置法则：始终超配斜率最陡峭、稳居100基准线上方的1~2个领涨主线板块，果断剔除连续跑输大盘的弱势板块。",
        },
        {
            "id": "20B",
            "category": "市场",
            "chart_type": "LINE",
            "title": "20B. 美股防御与利率敏感板块轮动 (5大防御/周期板块)",
            "df": sec_def_df,
            "right_cols": [],
            "left_title": "相对全市场VTI强弱 (同一起点=100)",
            "right_title": "",
            "what": "• 5大防御/利率敏感板块ETF ÷ 全市场VTI 标准化相对强弱（同一起点=100）：\n• 蓝:医疗XLV(防御) | 红:必需消费XLP(防御) | 黄:公用事业XLU(防御) | 绿:房地产XLRE | 紫:原材料XLB",
            "up_down": "• 三大防御(蓝/红/黄)上涨 = 机构抛售高波动成长股、躲入避风港（Risk-Off信号）。\n• 房地产(绿)上涨 = 市场抢跑定价美债利率下行与降息；原材料(紫)上涨 = 全球制造业补库与商品涨价。",
            "zones": "• ✅ 合理健康区间：防御板块运行在 `90 ~ 103` 区间（市场风险偏好正常，进攻板块主导）。\n• 🚨 不合理/系统性避险区间：医疗XLV、必需消费XLP、公用事业XLU同步飙升突破 `106 ~ 110`（典型衰退或股灾避险特征）。\n• 🚨 不合理/利率紧缩重创区间：房地产XLRE单边暴跌破 `85`（高利率严重压制利率敏感型资产）。",
            "changes": "• 【组合① 医疗XLV(蓝) + 必需消费XLP(红) + 公用事业XLU(黄) 集体抬头跑赢大盘】：Risk-Off系统性避险信号。\n• 【组合② 房地产XLRE(绿) + 公用事业XLU(黄) 同步走强】：市场抢跑定价美债收益率见顶下行与降息周期。\n• 【组合③ 原材料XLB(紫) 领涨而三大防御板块走弱】：全球制造业补库与商品涨价交易主导，与图09铜金比印证。\n• 【组合④ 三大防御板块持续单边垫底】：市场风险偏好极度旺盛，资金全线进攻风险资产。",
            "signal": "🚨 避险警报：当必需消费(XLP)与公用事业(XLU)连续3周以上显著跑赢科技(XLK)时，大盘往往面临系统性回调压力。",
        },
        # --- 五、核心焦点扩展模块 ---
        {
            "id": "21",
            "category": "扩展",
            "chart_type": "LINE",
            "title": "21. AI板块对比标普与三大股指强弱比 (AI vs Indices)",
            "df": ai_df,
            "right_cols": [],
            "left_title": "相对强弱指数 (同一起点=100)",
            "right_title": "",
            "what": "• 蓝:AI芯片SMH÷标普SPY | 红:AI应用AIQ÷标普SPY | 黄:AI机器人BOTZ÷标普SPY\n• 绿:AI芯片SMH÷纳指QQQ | 紫:AI芯片SMH÷罗素小盘IWM | 青:AI芯片SMH÷道指DIA",
            "up_down": "• 曲线上涨 = AI核心产业链跑赢对应宽基指数，资金虹吸效应增强。\n• 曲线下跌 = AI板块跑输宽基指数，资金向传统蓝筹或中小盘股高低切换。",
            "zones": "• ✅ 合理健康区间：强弱比沿中期均线稳步阶梯抬升（`100 ~ 135`），芯片与应用良性接力。\n• 🚨 不合理/算力泡沫拥挤区间：SMH/SPY 与 SMH/QQQ 短期垂直飙升偏离均线过高（筹码极度拥挤，财报稍有不及预期即暴跌）。\n• 🚨 不合理/主线退潮区间：六条强弱比曲线同步跌破中期上升趋势线且跌破 `100`（AI资本开支放缓担忧升温）。",
            "changes": "• 【组合① 蓝线(SMH/SPY) + 红线(AIQ/SPY) + 绿线(SMH/QQQ) 同步上行】：AI全产业链处于最强主升浪。\n• 【组合② 蓝线(SMH/SPY)高位走平，而红线(AIQ/SPY)接力陡峭上升】：AI行情第二阶段扩散，资金从芯片向AI软件应用轮动。\n• 【组合③ 紫线(SMH/IWM) 与 青线(SMH/DIA) 同步大幅回落】：资金高低切换，流向降息受益的罗素小盘与传统蓝筹补涨。\n• 【组合④ 六条强弱比曲线全线跌破中期上升趋势线】：AI资本开支放缓担忧升温，阶段性降低半导体与AI仓位。",
            "signal": "🎯 轮动信号：SMH/SPY 与 SMH/QQQ 趋势向上时坚定持有AI算力龙头；若 SMH/IWM 跌破趋势线，及时切分仓位至中小盘与顺周期。",
        },
        {
            "id": "22",
            "category": "扩展",
            "chart_type": "COLUMN",
            "title": "22. 全球七大核心资产资金净流入/流出强度 (近12个月柱状图)",
            "df": global_flow_df,
            "right_cols": [],
            "left_title": "月均20日资金流净强弱 (%)",
            "right_title": "",
            "what": "• 近12个月全球旗舰ETF月均Chaikin资金流净强弱分组柱状图(%)：\n• 蓝:美股SPY | 红:科技QQQ | 黄:新兴市场EEM | 绿:美债TLT | 紫:黄金GLD | 青:商品DBC | 洋红:比特币IBIT",
            "up_down": "• 0轴上方正柱(柱体越高) = 资金主动买盘推升，月度净流入强度越强。\n• 0轴下方负柱(柱体越深) = 资金主动抛压打压，月度净流出强度越深。",
            "zones": "• ✅ 合理健康区间：`-8% ~ +15%`（各大类资产在股、债、黄金、商品间有序轮动）。\n• 🚨 不合理/单资产极度拥挤区间：单类资产柱体冲高 `>+25%`（资金过度拥挤，警惕获利回吐）。\n• 🚨 不合理/全局流动性挤兑区间：七大资产柱体集体跌入 `<-10%` 负值区（无差别抛售一切资产回笼美元现金）。",
            "changes": "• 【组合① 蓝(SPY)+红(QQQ)+洋红(IBIT) 齐现高正柱】：全球风险偏好旺盛，美股与加密资产同步吸金。\n• 【组合② 绿(TLT)+紫(GLD) 转为高正柱，而蓝(SPY)/红(QQQ)出现负柱】：典型避险切换，资金从股市撤出涌入美债与黄金。\n• 【组合③ 黄(EEM)+紫(GLD)+青(DBC) 同步强净流入】：美元走弱驱动非美与实物再通胀行情。\n• 【组合④ 七大资产柱体集体跌入0轴下方负值区】：无差别流动性紧缩(Cash Dash)，现金为王。",
            "signal": "🎯 资金流阈值：单月资金流强弱柱体站稳 +10% 代表机构持续建仓净流入强劲；跌破 -10% 代表机构持续派发净流出。",
        },
        {
            "id": "23",
            "category": "扩展",
            "chart_type": "COLUMN",
            "title": "23. 大型机构在全球七大资产的非商业净持仓比 (CFTC 近12个月柱状图)",
            "df": inst_global_ratio_df,
            "right_cols": [],
            "left_title": "非商业多空净持仓比 (%)",
            "right_title": "",
            "what": "• 美国 CFTC 非商业头寸(资管+对冲基金)多空净比 = (多头-空头)/(多头+空头)×100%（近12个月柱状图）：\n• 蓝:美股标普500 | 红:美股科技纳指 | 黄:10年期美债 | 绿:实物黄金 | 紫:WTI原油 | 青:美元指数 | 洋红:比特币BTC",
            "up_down": "• 柱体向上抬升(正柱增高或负柱收窄) = 机构增加净多头或平仓空头（资金边际流入）。\n• 柱体向下延伸(正柱回落或负柱加深) = 机构减多或增加空头对冲（资金边际流出；注：美债与标普含期现套利空单）。",
            "zones": "• ✅ 合理健康区间：非套利商品/货币/科技围绕 `-25% ~ +35%` 均衡配置；美债与标普重点观察月度柱体边际增减方向。\n• 🚨 不合理/机构多头极度拥挤区间：`>+45%`（如黄金或BTC净比长期超+50%，多头过度拥挤防踩踏回调）。\n• 🚨 不合理/机构空头极度拥挤区间：`<-45%`（空头头寸极度拥挤，一旦基本面改善极易触发猛烈轧空）。",
            "changes": "• 【组合① 绿(黄金)+洋红(BTC)+红(纳指) 齐现高正柱】：货币贬值对冲 + AI进攻杠铃配置，大型机构同时配置硬通货与科技成长。\n• 【组合② 黄(10年期美债)负柱连续收窄并向0轴回升】：对冲基金集中平仓美债空头，预示10年美债收益率即将见顶回落。\n• 【组合③ 青(美元指数)正柱飙升 + 蓝/黄/绿柱集体转弱】：机构大举做多美元、全面减仓股债商品，全球美元流动性收紧。\n• 【跨图背离 图22 ETF资金仍净流入，但图23 CFTC柱体连续大幅转弱】：被动ETF在接盘，期货市场机构已在减仓对冲！",
            "signal": "🎯 机构头寸极值：单类资产净比突破 +45% 为多头极度拥挤；对于长期受基差套利影响的10年美债(黄柱)与标普(蓝柱)，重点捕捉负柱连续收窄的轧空拐点。",
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
            "what": "• 蓝线(左轴)：全球资产流出恐慌指数(0~100，基于6大核心跨资产CMF资金流+VIX+信用利差合成)  • 红线(左轴)：10周均线\n• 黄线(左轴)：6大核心跨资产(SPY/QQQ/EEM/TLT/GLD/DBC)同步净流出占比(%)  • 绿线(右轴)：VIX恐慌指数",
            "up_down": "• 蓝/黄/绿线上涨 = 全球跨资产同步抛售压力与流动性紧张加剧。\n• 蓝/黄/绿线下跌 = 恐慌抛售消退，市场恢复正常轮动与流动性宽松。",
            "zones": "• ✅ 合理健康区间：恐慌指数 `<35`、同步净流出资产占比 `≤50%`、VIX `<19.0`（资产间良性轮动，股债跷跷板有效）。\n• 🚨 不合理/局部波动区间：VIX `22~28` 但同步净流出占比 `≤50%`（仅单一市场回调，美债与黄金仍发挥避险作用）。\n• 🚨 不合理/全局流动性危机区间：恐慌指数 `>65` 且同步净流出资产占比 `≥83%`（6大资产几乎全线失血，无差别抛售回笼现金）。",
            "changes": "• 【组合① 蓝线 < 35 + 黄线 ≤ 50% + 绿线(VIX) < 18】：全球流动性充裕，无系统性抛售风险。\n• 【组合② 绿线(VIX)脉冲跳升(>25) 但 黄线(净流出占比) ≤ 50%】：仅单一市场局部波动，避险机制依然有效。\n• 【组合③ 蓝线飙升突破 65~80 + 黄线高达 83%~100%】：最高级别全球流动性危机警报，必须降杠杆并保留现金。\n• 【组合④ 蓝线突破 75 极值后首次拐头下穿红线(10周均线)】：恐慌抛售高潮已过、流动性见底，迎来全局性抄底窗口。",
            "signal": "🚨 恐慌熔断线：全球资产流出恐慌指数 > 65 且同步净流出资产占比 ≥ 83% 为最高级别流动性警报，必须降杠杆并保留现金；蓝线高位下穿红线则是危机后的抄底信号。",
        },
        {
            "id": "25",
            "category": "扩展",
            "chart_type": "LINE",
            "title": "25. 美元指数强弱对比与全球资产跷跷板效应 (USD vs Global Assets)",
            "df": dxy_compare_df,
            "right_cols": [],
            "left_title": "标准化相对表现 (同一起点=100)",
            "right_title": "",
            "what": f"• 近2年(104周)同一起点(`100.0`)标准化跨资产相对强弱对比：\n• 蓝:{dxy_label}(全球流动性总阀门) | 红:美股标普500(SPY) | 黄:新兴市场(EEM)\n• 绿:实物黄金(GLD) | 紫:美债长债(TLT) | 青:比特币ETF(IBIT)",
            "up_down": f"• 蓝线({dxy_label})上涨(>100) = 美元相对起点升值、全球流动性抽紧（抽水机效应，压制新兴市场、黄金、美债与比特币）。\n• 蓝线({dxy_label})下跌(<100) = 美元相对起点走弱、全球流动性释放（开闸放水，助推黄金、比特币与新兴市场爆发）。",
            "zones": "• ✅ 合理健康区间：蓝线(美元相对强弱)围绕起点基准 `95 ~ 104`（相对起点波动 ±5% 以内）温和震荡，全球资本有序配置。\n• 🚨 不合理/强美元抽水危机区间：蓝线单边急飙突破 `105+`（相对起点升值超 5%，新兴市场汇率承压、全球风险资产面临美元紧缩）。\n• 🚨 不合理/信用重估避险区间：蓝线(美元)与绿线(黄金GLD)罕见同步急涨（地缘危机或主权信用风险引发无差别避险）。",
            "changes": "• 【组合① 蓝线(美元)单边飙升(>105) + 黄(EEM)/紫(TLT)/青(BTC)集体下挫】：强美元流动性抽水行情，仅美元现金与美股防御板块抗跌。\n• 【组合② 蓝线(美元)破位下行(<96) + 绿(黄金GLD)+黄(EEM)+青(BTC)全线暴涨】：弱美元全球流动性牛市，黄金、加密资产与新兴市场爆发力最强。\n• 【组合③ 蓝线(美元)与绿线(黄金GLD)罕见同步上涨】：地缘危机或主权信用重估组合，此时新兴市场与风险资产风险极高。\n• 【组合④ 蓝线(美元)高位滞涨回落 + 红线(美股SPY)稳步上行】：美国经济一枝独秀转向全球温和复苏，美股与非美资产共振上涨。",
            "signal": "🎯 美元总阀门法则：当美元相对强弱(蓝线)跌破中期趋势线且新兴市场(黄线)与黄金(绿线)同步站上100基准线上方时，果断加配黄金、比特币与非美资产。",
        },
    ]
    return charts


# ==========================================
# 4. 26 张图表数据真实性与异常值自动体检器
# ==========================================
def verify_charts_integrity(charts: list):
    print("\n" + "=" * 84)
    print("🔍 全量 26 张图表数据真实性、基准对齐与异常值体检报告")
    print("=" * 84)
    now_ts = pd.Timestamp.now()
    for c in charts:
        df = c["df"]
        if df.empty:
            print(f"❌ [严重缺失] {c['title']} -> DataFrame 为空！")
            continue
        start_d = df.index[0].strftime("%Y-%m-%d")
        end_d = df.index[-1].strftime("%Y-%m-%d")
        lag_days = (now_ts - df.index[-1]).days
        lag_flag = f" ⚠️滞后{lag_days}天" if lag_days > 45 else ""

        col_stats = []
        for col in df.columns:
            s_valid = df[col].dropna()
            valid_n = len(s_valid)
            if valid_n == 0:
                col_stats.append(f"{col}:❌全空")
                continue
            last_v = float(s_valid.iloc[-1])
            std_v = float(s_valid.std()) if valid_n > 1 else 0.0
            flat_warn = "(⚠️零波动)" if std_v < 1e-6 else ""
            col_stats.append(f"{col}:{valid_n}点[最新={last_v:.2f}]{flat_warn}")

        print(f"✅ [{c['id']}] {c['title']} | {start_d} ~ {end_d}{lag_flag} | " + " | ".join(col_stats))
    print("=" * 84 + "\n")


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
                "用简洁有力的中文写一段260字以内的【本期宏观、AI强弱、机构资金流与美元跷跷板总评】，依次点明："
                "1) 消费与供给是否处于合理健康区间；2) 利率与信用风险；3) AI强弱、CFTC大型机构七大资产(含BTC)非商业净比、全球流出恐慌指数及美元跨资产联动状态。\n\n"
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
                    "max_tokens": 520,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"[Warning] OpenAI 摘要生成失败，切换至规则摘要: {e}")

    return (
        "全量26张核心图表已同步至最新交易日并完成同起点基准校验。请重点核查：① 纽约联储WEI指数与油金/铜金比所处区间；"
        "② 美债收益率曲线形态与CCC信用利差；③ 标普500市场宽度与CFTC资管机构持仓；"
        "④ AI强弱、近12个月全球七大资产资金流柱状图、CFTC机构七大资产(含BTC)净持仓比柱状图、全球流出恐慌指数及美元跨资产跷跷板。"
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
# 6. 写入底层数据并渲染高清 26 图五段式指南看板
# ==========================================
def render_to_google_sheets(charts: list, ai_summary: str):
    sh = gc.open_by_key(SPREADSHEET_ID)

    max_len = max((len(c["df"]) for c in charts if not c["df"].empty), default=10)
    CARD_ROWS = 44
    total_dash_rows = 1220
    raw_rows_cap = max(450, max_len + 20)
    raw_cols_cap = max(240, sum((len(c["df"].columns) + 2) for c in charts) + 10)

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
    # B. 构建 Dashboard 文本卡片与五段式深度读图指南
    # ------------------------------------------
    tz_cst = timezone(timedelta(hours=8))
    now_str = datetime.now(tz_cst).strftime("%Y-%m-%d %H:%M (UTC+8)")

    dash_grid = [[""] * 16 for _ in range(total_dash_rows)]
    dash_grid[1][1] = "📈 美股核心 26 张宏观、机构全球资金流比(含BTC)、恐慌与美元跷跷板全景看板"
    dash_grid[2][1] = (
        f"最近数据完成时间：{now_str}   |   手动刷新：顶部菜单【📊 宏观量化看板 → 🔄 手动刷新全部 26 张图表】   |   "
        "来源：FRED · CFTC · CBOE · Redfin · Yahoo Finance · AAII"
    )
    dash_grid[3][1] = f"🤖 AI 宏观与资金流总评：{ai_summary}"

    category_banners = {
        "消费": ("一、消费端 (Consumption — 美国经济 70% 核心驱动，有工作才有收入与消费)", {"red": 0.11, "green": 0.29, "blue": 0.49}),
        "供给": ("二、供给端 (Supply — 提前 3~6 个月预判供需缺口与通胀拐点的先行指标)", {"red": 0.08, "green": 0.42, "blue": 0.45}),
        "利率": ("三、利率端 (Interest Rates — 资产定价之锚、曲线倒挂与系统性违约风险标杆)", {"red": 0.34, "green": 0.22, "blue": 0.48}),
        "市场": ("四、市场端 (Market — 散户与CFTC机构仓位极值、市场宽度与11大板块轮动)", {"red": 0.13, "green": 0.42, "blue": 0.32}),
        "扩展": ("五、核心焦点扩展 (AI强弱 · 全球12月资金流柱图 · CFTC机构七大资产含BTC柱图 · 恐慌指数 · 美元强弱对比)", {"red": 0.65, "green": 0.24, "blue": 0.14}),
    }

    row_cursor = 5
    current_cat = None
    layout_blocks = []

    for c in charts:
        cat = c["category"]
        if cat != current_cat:
            current_cat = cat
            banner_text, banner_color = category_banners[cat]
            dash_grid[row_cursor][1] = banner_text
            layout_blocks.append({"type": "section", "row": row_cursor, "color": banner_color})
            row_cursor += 2

        r0 = row_cursor
        dash_grid[r0][9] = f"📌 {c['title']} — 五段式深度读图、区间阈值与组合指南"
        dash_grid[r0 + 1][9] = "【① 每根线/柱体与坐标轴精确定义】"
        dash_grid[r0 + 2][9] = c["what"]
        dash_grid[r0 + 7][9] = "【② 每根线上涨与下跌分别代表什么】"
        dash_grid[r0 + 8][9] = c["up_down"]
        dash_grid[r0 + 15][9] = "【③ 合理区间 vs 不合理（极端警戒）区间划分】"
        dash_grid[r0 + 16][9] = c["zones"]
        dash_grid[r0 + 24][9] = "【④ 多维组合情景与背离对比矩阵】"
        dash_grid[r0 + 25][9] = c["changes"]
        dash_grid[r0 + 38][9] = "【⑤ 量化触发阈值与实战仓位对策】"
        dash_grid[r0 + 39][9] = c["signal"]

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

    col_widths = (
        [(0, 1, 20)]
        + [(i, i + 1, 107) for i in range(1, 9)]
        + [(i, i + 1, 140) for i in range(9, 15)]
        + [(15, 16, 20)]
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
                "properties": {"pixelSize": 66},
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
                "properties": {"pixelSize": 22},
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

        for sr in [r0 + 1, r0 + 7, r0 + 15, r0 + 24, r0 + 38]:
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
            (r0 + 8, r0 + 15),
            (r0 + 16, r0 + 24),
            (r0 + 25, r0 + 38),
            (r0 + 39, r0 + 44),
        ]
        for idx_b, (br_start, br_end) in enumerate(body_blocks):
            if idx_b == 2:
                bg = {"red": 0.96, "green": 0.99, "blue": 0.96}
            elif idx_b == 4:
                bg = {"red": 0.99, "green": 0.96, "blue": 0.95}
            else:
                bg = {"red": 0.98, "green": 0.99, "blue": 1.0}
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
                            "widthPixels": 850,
                            "heightPixels": 950,
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

    print("✅ 全部 26 张高清图表（含数据精度修复 + 五段式全量读图与区间指南）已更新完毕！")


if __name__ == "__main__":
    all_charts = build_all_charts()
    verify_charts_integrity(all_charts)
    summary = generate_ai_summary(all_charts)
    render_to_google_sheets(all_charts, summary)
