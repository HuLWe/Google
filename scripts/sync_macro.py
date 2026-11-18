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

# 自动兼容用户填入“完整 Google Sheets 网址”或“纯 ID”两种情况
_raw_ss = os.environ["SPREADSHEET_ID"].strip()
_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", _raw_ss)
SPREADSHEET_ID = _match.group(1) if _match else _raw_ss.split("/")[0].split("?")[0].strip()

GCP_SA_JSON = json.loads(os.environ["GCP_SA_KEY"])

TZ_CST = timezone(timedelta(hours=8))
NOW_STR = datetime.now(TZ_CST).strftime("%Y-%m-%d %H:%M:%S")


def create_http_session():
    """构建带指数退避重试与标准请求头的 HTTP 会话"""
    session = requests.Session()
    retries = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; GlobalMacroBot/1.0)",
        "Accept": "application/json",
    })
    return session


HTTP = create_http_session()

# ================= 2. 多国核心指标字典 (L1 - L5) =================
FRED_SERIES = [
    # L2: 美元流动性管道与美债定价锚
    {"id": "WALCL", "country": "US", "layer": "L2-流动性管道", "name": "美联储总资产 (百万美元)"},
    {"id": "WTREGEN", "country": "US", "layer": "L2-流动性管道", "name": "财政部TGA余额 (百万美元)"},
    {"id": "RRPONTSYD", "country": "US", "layer": "L2-流动性管道", "name": "隔夜逆回购 ON RRP (十亿美元)"},
    {"id": "WRESBAL", "country": "US", "layer": "L2-流动性管道", "name": "商业银行准备金余额 (百万美元)"},
    {"id": "DGS2", "country": "US", "layer": "L2-利率之锚", "name": "美国2年期国债收益率 (%)"},
    {"id": "DGS10", "country": "US", "layer": "L2-利率之锚", "name": "美国10年期国债收益率 (%)"},
    {"id": "DFII10", "country": "US", "layer": "L2-利率之锚", "name": "美国10Y TIPS实际利率 (%)"},
    {"id": "T10Y2Y", "country": "US", "layer": "L2-利率之锚", "name": "美债10Y-2Y期限利差 (%)"},
    # L1: 信用引擎与货币活化 (美/中/日)
    {"id": "M1SL", "country": "US", "layer": "L1-信用引擎", "name": "美国M1货币供应 (十亿美元)"},
    {"id": "M2SL", "country": "US", "layer": "L1-信用引擎", "name": "美国M2货币供应 (十亿美元)"},
    {"id": "DRTSCILM", "country": "US", "layer": "L1-信用引擎", "name": "美联储SLOOS银行收紧贷款比例 (%)"},
    {"id": "BAMLH0A0HYM2", "country": "US", "layer": "L1-信用引擎", "name": "美国高收益债信用利差 (%)"},
    {"id": "MYAGM1CNM189N", "country": "CN", "layer": "L1-信用引擎", "name": "中国M1货币供应"},
    {"id": "MYAGM2CNM189N", "country": "CN", "layer": "L1-信用引擎", "name": "中国M2货币供应"},
    {"id": "MYAGM1JPM189S", "country": "JP", "layer": "L1-信用引擎", "name": "日本M1货币供应"},
    {"id": "MYAGM2JPM189S", "country": "JP", "layer": "L1-信用引擎", "name": "日本M2货币供应"},
    # L2/L3: 多国10Y国债与周期象限
    {"id": "IRLTLT01CNM156N", "country": "CN", "layer": "L2-利率之锚", "name": "中国10年期国债收益率 (%)"},
    {"id": "IRLTLT01JPM156N", "country": "JP", "layer": "L2-利率之锚", "name": "日本10年期国债收益率 (%)"},
    {"id": "IRLTLT01DEM156N", "country": "DE", "layer": "L2-利率之锚", "name": "德国(欧元区基准)10Y国债收益率 (%)"},
    {"id": "PCEPILFE", "country": "US", "layer": "L3-周期象限", "name": "美国核心PCE物价指数"},
    {"id": "UNRATE", "country": "US", "layer": "L3-周期象限", "name": "美国失业率 (%)"},
    {"id": "CHNCPIALLMINMEI", "country": "CN", "layer": "L3-周期象限", "name": "中国CPI指数"},
    # L4/L5: 跨境汇率与跨资产定价
    {"id": "DTWEXBGS", "country": "US", "layer": "L4-跨境汇率", "name": "广义美元指数"},
    {"id": "DEXJPUS", "country": "JP", "layer": "JP", "layer": "L4-跨境汇率", "name": "美元兑日元 (USD/JPY)"},
    {"id": "DEXCHUS", "country": "CN", "layer": "L4-跨境汇率", "name": "美元兑在岸人民币 (USD/CNY)"},
    {"id": "VIXCLS", "country": "US", "layer": "L5-跨资产定价", "name": "CBOE VIX恐慌指数"},
    {"id": "DCOILBRENTEU", "country": "GLOBAL", "layer": "L5-跨资产定价", "name": "布伦特原油现货价 (美元/桶)"},
]

COUNTRIES_LIST = ["USA", "CHN", "JPN", "DEU", "IND", "BRA", "IDN", "TUR"]


def fetch_fred_data():
    records = []
    latest_map = {}
    for item in FRED_SERIES:
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={item['id']}&api_key={FRED_API_KEY}"
            f"&file_type=json&sort_order=desc&limit=15"
        )
        try:
            resp = HTTP.get(url, timeout=(10, 30))
            resp.raise_for_status()
            obs = [o for o in resp.json().get("observations", []) if o.get("value") != "."]
            if not obs:
                continue
            val_curr = float(obs[0]["value"])
            val_prev = float(obs[1]["value"]) if len(obs) > 1 else val_curr
            latest_map[item["id"]] = [float(o["value"]) for o in obs]
            records.append({
                "观测日期": obs[0]["date"],
                "国家/地区": item["country"],
                "六层归属": item["layer"],
                "指标代码": item["id"],
                "指标名称": item["name"],
                "最新值": round(val_curr, 4),
                "前值": round(val_prev, 4),
                "边际变化": round(val_curr - val_prev, 4),
                "数据源": "FRED API",
                "同步时间": NOW_STR,
            })
            time.sleep(0.15)
        except Exception as e:
            print(f"[WARN] FRED {item['id']} failed: {e}")
    return records, latest_map


def fetch_nyfed_and_wb():
    records = []
    wb_metrics = {}

    # 1. NY Fed 日频 SOFR
    try:
        resp = HTTP.get("https://markets.newyorkfed.org/api/rates/secured/sofr/last/2.json", timeout=(10, 30))
        resp.raise_for_status()
        sofr = resp.json().get("refRates", [])
        if len(sofr) >= 2:
            c, p = float(sofr[0]["percentRate"]), float(sofr[1]["percentRate"])
            records.append({
                "观测日期": sofr[0]["effectiveDate"],
                "国家/地区": "US",
                "六层归属": "L2-流动性管道",
                "指标代码": "NYFED_SOFR",
                "指标名称": "纽约联储 SOFR 隔夜利率 (%)",
                "最新值": c,
                "前值": p,
                "边际变化": round(c - p, 4),
                "数据源": "NY Fed API",
                "同步时间": NOW_STR,
            })
    except Exception as e:
        print(f"[WARN] NYFed failed: {e}")

    # 2. IMF DataMapper 多国宏观结构
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
            resp = HTTP.get(url, timeout=(10, 35))
            resp.raise_for_status()
            vals = resp.json().get("values", {}).get(code, {})
            for c in COUNTRIES_LIST:
                if c in vals:
                    curr = vals[c].get(str(year)) or vals[c].get(str(year - 1))
                    prev = vals[c].get(str(year - 1)) or curr
                    if curr is not None:
                        records.append({
                            "观测日期": f"{year}-12-31",
                            "国家/地区": c,
                            "六层归属": layer,
                            "指标代码": f"IMF_{code}",
                            "指标名称": name,
                            "最新值": round(float(curr), 2),
                            "前值": round(float(prev), 2),
                            "边际变化": round(float(curr) - float(prev), 2),
                            "数据源": "IMF DataMapper",
                            "同步时间": NOW_STR,
                        })
        except Exception as e:
            print(f"[WARN] IMF {code} failed: {e}")

    # 3. World Bank 新兴市场短期外债 & 外汇储备 (放宽 read timeout 至 45 秒)
    wb_codes = {
        "FI.RES.TOTL.CD": "官方外汇储备总额 (美元)",
        "DT.DOD.DSTC.CD": "短期外债余额 (美元)",
    }
    for code, name in wb_codes.items():
        try:
            url = f"https://api.worldbank.org/v2/country/{';'.join(COUNTRIES_LIST)}/indicator/{code}?format=json&mrv=2"
            resp = HTTP.get(url, timeout=(10, 45))
            resp.raise_for_status()
            data = resp.json()
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
                        "观测日期": f"{c_date}-12-31",
                        "国家/地区": iso3,
                        "六层归属": "L4-跨境汇率",
                        "指标代码": f"WB_{code}",
                        "指标名称": name,
                        "最新值": round(c_val, 2),
                        "前值": round(p_val, 2),
                        "边际变化": round(c_val - p_val, 2),
                        "数据源": "World Bank API",
                        "同步时间": NOW_STR,
                    })
        except Exception as e:
            print(f"[WARN] WorldBank {code} failed: {e}")

    return records, wb_metrics


def build_derived_signals(latest_map, wb_metrics):
    derived = []

    def get_val(sid, idx=0):
        arr = latest_map.get(sid, [])
        return arr[idx] if len(arr) > idx else None

    # 1. 美联储净流动性
    walcl, tga, rrp = get_val("WALCL"), get_val("WTREGEN"), get_val("RRPONTSYD")
    walcl_p, tga_p, rrp_p = get_val("WALCL", 1), get_val("WTREGEN", 1), get_val("RRPONTSYD", 1)
    if all(v is not None for v in [walcl, tga, rrp]):
        net_liq = (walcl - tga - rrp * 1000) / 1000.0
        net_liq_p = (walcl_p - tga_p - rrp_p * 1000) / 1000.0
        delta = round(net_liq - net_liq_p, 2)
        signal = "放水扩张 (利好风险资产)" if delta > 0 else "抽水收缩 (警惕流动性紧缩)"
        derived.append(["L2-美元水管", "美联储净流动性 (十亿美元)", round(net_liq, 2), delta, signal])

    # 2. 美债收益率曲线牛陡 / 熊陡
    y10, y2 = get_val("DGS10"), get_val("DGS2")
    y10_p, y2_p = get_val("DGS10", 5), get_val("DGS2", 5)
    if all(v is not None for v in [y10, y2, y10_p, y2_p]):
        spread = round(y10 - y2, 3)
        spread_chg = round(spread - (y10_p - y2_p), 3)
        if spread < 0:
            regime = "曲线倒挂 (衰退预警区间)"
        elif spread_chg > 0 and (y2 - y2_p) < 0:
            regime = "牛陡 (短端降息预期主导，利好短债/黄金)"
        elif spread_chg > 0 and (y10 - y10_p) > 0:
            regime = "熊陡 (再通胀或美债供给过剩，压制高估值)"
        else:
            regime = "曲线趋平 (紧缩或观望过渡期)"
        derived.append(["L2-利率之锚", "美债10Y-2Y利差与曲线形态 (%)", spread, spread_chg, regime])

    # 3. 跨境套息利差
    for c_name, sid in [
        ("中美10Y利差 (US-CN)", "IRLTLT01CNM156N"),
        ("美日10Y利差 (US-JP)", "IRLTLT01JPM156N"),
        ("美德10Y利差 (US-DE)", "IRLTLT01DEM156N"),
    ]:
        c_y = get_val(sid)
        if y10 is not None and c_y is not None:
            diff = round(y10 - c_y, 3)
            note = "美元高息虹吸，警惕非美货币贬值/套息波动" if diff > 2.0 else "利差收窄，跨境套息资金回流风险上升"
            derived.append(["L4-跨境利差", c_name, diff, "-", note])

    # 4. 萨姆规则衰退触发器
    unrate_arr = latest_map.get("UNRATE", [])
    if len(unrate_arr) >= 12:
        ma3_list = [sum(unrate_arr[i:i+3]) / 3.0 for i in range(len(unrate_arr) - 2)]
        sahm_val = round(ma3_list[0] - min(ma3_list[:12]), 2)
        sahm_sig = "⚠️ 已触发萨姆规则 (>=0.50%)" if sahm_val >= 0.5 else "未触发 (<0.50% 安全线)"
        derived.append(["L3-衰退预警", "美国萨姆规则指数 (%)", sahm_val, "-", sahm_sig])

    # 5. 新兴市场短期外债 / 外汇储备防爆比
    for iso3 in ["BRA", "IND", "IDN", "TUR"]:
        res = wb_metrics.get((iso3, "FI.RES.TOTL.CD"))
        debt = wb_metrics.get((iso3, "DT.DOD.DSTC.CD"))
        if res and debt:
            ratio = round((debt / res) * 100, 1)
            risk = "🔴 高危 (>100% 覆盖不足)" if ratio > 100 else ("🟡 警戒 (50%-100%)" if ratio > 50 else "🟢 安全 (<50%)")
            derived.append(["L4-EM防爆", f"{iso3} 短期外债/外储比率 (%)", ratio, "-", risk])

    return derived


def write_to_gsheet(raw_records, derived_rows):
    if not raw_records:
        raise RuntimeError("所有外部数据源本次均未返回有效数据，已中止写入以保护表格原有历史记录。")

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)

    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        sa_email = GCP_SA_JSON.get("client_email", "未知服务账号")
        raise RuntimeError(
            f"无法打开 Google 表格 (解析后的 ID: '{SPREADSHEET_ID}')。"
            f"请检查两点：1) 该 ID 是否正确；2) 是否已在 Google 表格右上角【共享】中将 '{sa_email}' 设为编辑者。原始错误: {e}"
        )

    df_raw = pd.DataFrame(raw_records)
    ws_raw = get_or_create_ws(sh, "01_原始指标库_Raw")
    ws_raw.clear()
    ws_raw.update([df_raw.columns.tolist()] + df_raw.values.tolist())

    ws_der = get_or_create_ws(sh, "03_衍生组合与信号_Derived")
    ws_der.clear()
    der_headers = ["层级归属", "衍生组合名称", "当前计算值", "边际变化", "第一性原理诊断信号"]
    ws_der.update([der_headers] + derived_rows)


def get_or_create_ws(sh, title):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=200, cols=15)


if __name__ == "__main__":
    fred_recs, l_map = fetch_fred_data()
    other_recs, wb_map = fetch_nyfed_and_wb()
    all_raw = fred_recs + other_recs
    derived = build_derived_signals(l_map, wb_map)
    write_to_gsheet(all_raw, derived)
    print(f"[SUCCESS] Synced {len(all_raw)} raw metrics and {len(derived)} derived signals.")
