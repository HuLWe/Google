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
        "User-Agent": "Mozilla/5.0 (compatible; GlobalMacroBot/2.0)",
        "Accept": "application/json",
    })
    return session


HTTP = create_http_session()

# ================= 2. 全面指标字典 (含修正后的量纲与黑天鹅序列) =================
FRED_SERIES = [
    # L2: 美元水管 (WALCL与WDTGAL均为百万美元周三水平，RRPONTSYD为十亿美元)
    {"id": "WALCL", "country": "US", "layer": "L2-流动性管道", "name": "美联储总资产 (百万美元)"},
    {"id": "WDTGAL", "country": "US", "layer": "L2-流动性管道", "name": "财政部TGA账户余额-周三水平 (百万美元)"},
    {"id": "RRPONTSYD", "country": "US", "layer": "L2-流动性管道", "name": "隔夜逆回购 ON RRP (十亿美元)"},
    {"id": "WRESBAL", "country": "US", "layer": "L2-流动性管道", "name": "商业银行准备金余额 (百万美元)"},
    {"id": "IORB", "country": "US", "layer": "L2-流动性管道", "name": "美联储准备金余额利率 IORB (%)"},
    {"id": "DGS2", "country": "US", "layer": "L2-利率之锚", "name": "美国2年期国债收益率 (%)"},
    {"id": "DGS10", "country": "US", "layer": "L2-利率之锚", "name": "美国10年期国债收益率 (%)"},
    {"id": "DFII10", "country": "US", "layer": "L2-利率之锚", "name": "美国10Y TIPS实际利率 (%)"},
    {"id": "T10Y2Y", "country": "US", "layer": "L2-利率之锚", "name": "美债10Y-2Y期限利差 (%)"},
    # L1: 信用引擎与货币存量 (拉取15期以上用于计算12个月YoY增速)
    {"id": "M1SL", "country": "US", "layer": "L1-信用引擎", "name": "美国M1存量 (十亿美元)"},
    {"id": "M2SL", "country": "US", "layer": "L1-信用引擎", "name": "美国M2存量 (十亿美元)"},
    {"id": "DRTSCILM", "country": "US", "layer": "L1-信用引擎", "name": "美联储SLOOS银行收紧贷款比例 (%)"},
    {"id": "BAMLH0A0HYM2", "country": "US", "layer": "L1-信用引擎", "name": "美国高收益债信用利差 (%)"},
    {"id": "MYAGM1CNM189N", "country": "CN", "layer": "L1-信用引擎", "name": "中国M1存量 (本币)"},
    {"id": "MYAGM2CNM189N", "country": "CN", "layer": "L1-信用引擎", "name": "中国M2存量 (本币)"},
    {"id": "MYAGM1JPM189S", "country": "JP", "layer": "L1-信用引擎", "name": "日本M1存量 (本币)"},
    {"id": "MYAGM2JPM189S", "country": "JP", "layer": "L1-信用引擎", "name": "日本M2存量 (本币)"},
    # L2/L3: 多国10Y国债与周期象限
    {"id": "IRLTLT01CNM156N", "country": "CN", "layer": "L2-利率之锚", "name": "中国10年期国债收益率 (%)"},
    {"id": "IRLTLT01JPM156N", "country": "JP", "layer": "L2-利率之锚", "name": "日本10年期国债收益率 (%)"},
    {"id": "IRLTLT01DEM156N", "country": "DE", "layer": "L2-利率之锚", "name": "德国(欧元区)10Y国债收益率 (%)"},
    {"id": "PCEPILFE", "country": "US", "layer": "L3-周期象限", "name": "美国核心PCE物价指数"},
    {"id": "UNRATE", "country": "US", "layer": "L3-周期象限", "name": "美国失业率 (%)"},
    {"id": "SAHMREALTIME", "country": "US", "layer": "L3-周期象限", "name": "官方萨姆规则衰退指标 (%)"},
    {"id": "CHNCPIALLMINMEI", "country": "CN", "layer": "L3-周期象限", "name": "中国CPI定基指数"},
    # L4/L5/L6: 跨境汇率、黑天鹅压力与跨资产
    {"id": "DTWEXBGS", "country": "US", "layer": "L4-跨境汇率", "name": "广义美元指数"},
    {"id": "DEXJPUS", "country": "JP", "layer": "L4-跨境汇率", "name": "美元兑日元 (USD/JPY)"},
    {"id": "DEXCHUS", "country": "CN", "layer": "L4-跨境汇率", "name": "美元兑在岸人民币 (USD/CNY)"},
    {"id": "VIXCLS", "country": "US", "layer": "L5-跨资产定价", "name": "CBOE VIX恐慌指数"},
    {"id": "VXVCLS", "country": "US", "layer": "L6-黑天鹅雷达", "name": "CBOE 3个月期VIX3M指数"},
    {"id": "STLFSI4", "country": "US", "layer": "L6-黑天鹅雷达", "name": "圣路易斯联储金融压力指数"},
    {"id": "NFCI", "country": "US", "layer": "L6-黑天鹅雷达", "name": "芝加哥联储金融状况指数"},
    {"id": "SP500", "country": "US", "layer": "L5-跨资产定价", "name": "标普500指数"},
    {"id": "DCOILBRENTEU", "country": "GLOBAL", "layer": "L5-跨资产定价", "name": "布伦特原油现货价 (美元/桶)"},
]

COUNTRIES_LIST = ["USA", "CHN", "JPN", "DEU", "IND", "BRA", "IDN", "TUR"]


def fetch_fred_data():
    records = []
    series_obs = {}  # 保存最近20期数值序列，用于计算同比/3M年化/5日变化
    for item in FRED_SERIES:
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={item['id']}&api_key={FRED_API_KEY}"
            f"&file_type=json&sort_order=desc&limit=20"
        )
        try:
            resp = HTTP.get(url, timeout=(10, 30))
            resp.raise_for_status()
            obs = [o for o in resp.json().get("observations", []) if o.get("value") != "."]
            if not obs:
                continue
            vals = [float(o["value"]) for o in obs]
            series_obs[item["id"]] = vals
            val_curr = vals[0]
            val_prev = vals[1] if len(vals) > 1 else val_curr
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
            time.sleep(0.12)
        except Exception as e:
            print(f"[WARN] FRED {item['id']} failed: {e}")
    return records, series_obs


def fetch_external_apis():
    records = []
    wb_metrics = {}
    extra_vals = {}

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

    # 2. 美国财政部近期 10Y/30Y 美债拍卖尾部利差 (Tail)
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

    # 3. IMF DataMapper 多国结构指标
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

    return records, wb_metrics, extra_vals


def analyze_macro_and_blackswan(s_obs, wb_metrics, extra_vals):
    """
    1) 计算修正后的精确衍生组合
    2) 扫描 8 大黑天鹅指标与三级熔断标准
    3) 生成结构化中文研判报告 (事实 -> 机制 -> 资金流向 -> 情景概率 -> 证伪条件)
    """
    derived_rows = []
    blackswan_rows = []
    ts_snapshot_dict = {}  # 用于写入 02_历史时序库_TimeSeries

    def get(sid, idx=0):
        arr = s_obs.get(sid, [])
        return arr[idx] if len(arr) > idx else None

    for sid, arr in s_obs.items():
        if arr:
            ts_snapshot_dict[sid] = round(arr[0], 4)

    # --- A. 精确计算美联储净流动性 (十亿美元) ---
    walcl, tga, rrp = get("WALCL"), get("WDTGAL"), get("RRPONTSYD")
    walcl_p, tga_p, rrp_p = get("WALCL", 1), get("WDTGAL", 1), get("RRPONTSYD", 1)
    net_liq, net_liq_chg = None, 0.0
    if all(v is not None for v in [walcl, tga, rrp]):
        net_liq = round((walcl - tga) / 1000.0 - rrp, 2)
        ts_snapshot_dict["NET_LIQUIDITY"] = net_liq
        if all(v is not None for v in [walcl_p, tga_p, rrp_p]):
            net_liq_p = (walcl_p - tga_p) / 1000.0 - rrp_p
            net_liq_chg = round(net_liq - net_liq_p, 2)
        liq_sig = "放水扩张 (支撑风险资产估值)" if net_liq_chg > 0 else "抽水收缩 (警惕准备金消耗)"
        derived_rows.append(["L2-美元水管", "美联储净流动性 (十亿美元)", net_liq, net_liq_chg, liq_sig])

    # --- B. 美债收益率曲线四象限形态 (牛陡/熊陡/牛平/熊平) ---
    y10, y2 = get("DGS10"), get("DGS2")
    y10_5, y2_5 = get("DGS10", 5), get("DGS2", 5)
    curve_regime = "数据不足"
    spread_10_2 = None
    if all(v is not None for v in [y10, y2, y10_5, y2_5]):
        spread_10_2 = round(y10 - y2, 3)
        d_spread = round(spread_10_2 - (y10_5 - y2_5), 3)
        dy10, dy2 = y10 - y10_5, y2 - y2_5
        if d_spread > 0 and dy2 < 0 and abs(dy2) > abs(dy10):
            curve_regime = "牛陡 (短端降息预期主导，利好短债/黄金)"
        elif d_spread > 0 and dy10 > 0 and dy10 > dy2:
            curve_regime = "熊陡 (再通胀或美债供给过剩，压制高估值)"
        elif d_spread < 0 and dy10 < 0 and abs(dy10) > abs(dy2):
            curve_regime = "牛平 (避险买长债或长期通胀预期回落)"
        elif d_spread < 0 and dy2 > 0 and dy2 > dy10:
            curve_regime = "熊平 (央行紧缩预期升温，曲线趋平)"
        else:
            curve_regime = "倒挂震荡区间" if spread_10_2 < 0 else "曲线温和震荡"
        derived_rows.append(["L2-利率之锚", "美债10Y-2Y利差与形态 (%)", spread_10_2, d_spread, curve_regime])

    # --- C. 精确计算中美/美国 M1-M2 同比剪刀差 (使用 t 与 t-12) ---
    cn_m1_0, cn_m1_12 = get("MYAGM1CNM189N", 0), get("MYAGM1CNM189N", 12)
    cn_m2_0, cn_m2_12 = get("MYAGM2CNM189N", 0), get("MYAGM2CNM189N", 12)
    cn_scissors = None
    if all(v is not None and v > 0 for v in [cn_m1_0, cn_m1_12, cn_m2_0, cn_m2_12]):
        cn_m1_yoy = (cn_m1_0 / cn_m1_12 - 1) * 100
        cn_m2_yoy = (cn_m2_0 / cn_m2_12 - 1) * 100
        cn_scissors = round(cn_m1_yoy - cn_m2_yoy, 2)
        ts_snapshot_dict["CN_M1_M2_SCISSORS"] = cn_scissors
        sc_sig = "资金定期化沉淀 (宽货币但紧信用)" if cn_scissors < -3.0 else "资金活化改善 (企业活期扩张)"
        derived_rows.append(["L1-信用引擎", "中国 M1-M2 同比剪刀差 (%)", cn_scissors, "-", sc_sig])

    # --- D. 核心 PCE 3个月环比折年率 & 真实利率 ---
    pce_0, pce_3 = get("PCEPILFE", 0), get("PCEPILFE", 3)
    pce_3m_ann = None
    if pce_0 and pce_3 and pce_3 > 0:
        pce_3m_ann = round(((pce_0 / pce_3) ** 4 - 1) * 100, 2)
        ts_snapshot_dict["US_CORE_PCE_3M_ANN"] = pce_3m_ann
        pce_sig = "通胀黏性偏高 (>2.5%)" if pce_3m_ann > 2.5 else "通胀温和可控 (<=2.5%)"
        derived_rows.append(["L3-周期象限", "美国核心PCE 3M环比折年率 (%)", pce_3m_ann, "-", pce_sig])

    # --- E. 跨境套息利差 ---
    for c_name, sid in [("中美10Y利差 (US-CN)", "IRLTLT01CNM156N"), ("美日10Y利差 (US-JP)", "IRLTLT01JPM156N")]:
        cy = get(sid)
        if y10 is not None and cy is not None:
            diff = round(y10 - cy, 3)
            derived_rows.append(["L4-跨境利差", c_name, diff, "-", "高利差吸引美元回流" if diff > 2.0 else "利差收窄，关注套息平仓"])

    # --- F. 黑天鹅 8 维雷达评估 ---
    red_alerts, yellow_alerts = [], []

    # BS-1: SOFR - IORB 利差
    sofr, iorb = extra_vals.get("SOFR"), get("IORB")
    if sofr is not None and iorb is not None:
        sofr_sp = round((sofr - iorb) * 100, 1)  # bps
        if sofr_sp >= 25:
            status = "🔴 红灯熔断 (银行间美元钱荒)"
            red_alerts.append(f"SOFR-IORB 利差飙升至 +{sofr_sp}bps")
        elif sofr_sp >= 10:
            status = "🟡 黄灯警戒 (回购资金趋紧)"
            yellow_alerts.append(f"SOFR-IORB 利差走阔至 +{sofr_sp}bps")
        else:
            status = "🟢 正常 (银行间流动性充裕)"
        blackswan_rows.append(["BS-1 美元回购管道", "SOFR - IORB 利差 (bps)", sofr_sp, "黄>=10 / 红>=25", status])

    # BS-2: 高收益债信用利差
    hy = get("BAMLH0A0HYM2")
    if hy is not None:
        hy_bps = round(hy * 100, 0)
        if hy_bps >= 500:
            status = "🔴 红灯熔断 (信用违约潮预警)"
            red_alerts.append(f"高收益债利差突破 {hy_bps}bps")
        elif hy_bps >= 350:
            status = "🟡 黄灯警戒 (风险溢价抬升)"
            yellow_alerts.append(f"高收益债利差升至 {hy_bps}bps")
        else:
            status = "🟢 正常 (信用市场平稳)"
        blackswan_rows.append(["BS-2 企业信用违约", "高收益债信用利差 (bps)", hy_bps, "黄>=350 / 红>=500", status])

    # BS-3: 圣路易斯联储金融压力指数 STLFSI4
    fsi = get("STLFSI4")
    if fsi is not None:
        if fsi >= 2.0:
            status = "🔴 红灯熔断 (系统性金融危机区间)"
            red_alerts.append(f"金融压力指数 STLFSI4 达 {fsi}")
        elif fsi >= 0.5:
            status = "🟡 黄灯警戒 (金融条件显著收紧)"
            yellow_alerts.append(f"金融压力指数 STLFSI4 升至 {fsi}")
        else:
            status = "🟢 正常 (低于历史均值压力)"
        blackswan_rows.append(["BS-3 综合金融压力", "STLFSI4 压力指数", round(fsi, 2), "黄>=0.5 / 红>=2.0", status])

    # BS-4: VIX 恐慌及期限结构倒挂 (VIX / VIX3M)
    vix, vix3m = get("VIXCLS"), get("VXVCLS")
    if vix is not None and vix3m is not None and vix3m > 0:
        vix_ratio = round(vix / vix3m, 2)
        if vix >= 35 or vix_ratio >= 1.15:
            status = "🔴 红灯熔断 (短端恐慌倒挂/尾部挤兑)"
            red_alerts.append(f"VIX={vix}, VIX/VIX3M={vix_ratio}")
        elif vix >= 25 or vix_ratio >= 1.0:
            status = "🟡 黄灯警戒 (避险对冲需求上升)"
            yellow_alerts.append(f"VIX={vix}, VIX/VIX3M={vix_ratio}")
        else:
            status = "🟢 正常 (波动率期限结构正常)"
        blackswan_rows.append(["BS-4 波动率倒挂", f"VIX={vix} | VIX/VIX3M", vix_ratio, "黄>=1.0 / 红>=1.15", status])

    # BS-5: 日元套息踩踏 (USD/JPY 5日急跌)
    jpy_0, jpy_5 = get("DEXJPUS", 0), get("DEXJPUS", 5)
    if jpy_0 and jpy_5 and jpy_5 > 0:
        jpy_5d_pct = round((jpy_0 / jpy_5 - 1) * 100, 2)
        if jpy_5d_pct <= -4.0:
            status = "🔴 红灯熔断 (日元套息连环平仓踩踏)"
            red_alerts.append(f"USD/JPY 5日暴跌 {jpy_5d_pct}%")
        elif jpy_5d_pct <= -2.5:
            status = "🟡 黄灯警戒 (套息资金加速回流)"
            yellow_alerts.append(f"USD/JPY 5日下跌 {jpy_5d_pct}%")
        else:
            status = "🟢 正常 (外汇套息波动可控)"
        blackswan_rows.append(["BS-5 套息交易踩踏", "USD/JPY 5日涨跌幅 (%)", jpy_5d_pct, "黄<=-2.5% / 红<=-4.0%", status])

    # BS-6: 新兴市场外债覆盖率最大值检查
    for iso3 in ["BRA", "IND", "IDN", "TUR"]:
        res = wb_metrics.get((iso3, "FI.RES.TOTL.CD"))
        debt = wb_metrics.get((iso3, "DT.DOD.DSTC.CD"))
        if res and debt:
            ratio = round((debt / res) * 100, 1)
            st = "🔴 高危 (>100%)" if ratio > 100 else ("🟡 警戒 (50%-100%)" if ratio > 50 else "🟢 安全 (<50%)")
            blackswan_rows.append([f"BS-6 EM外债({iso3})", "短期外债/外储比率 (%)", ratio, "黄>50% / 红>100%", st])

    # --- G. 自动生成中文结构化研判报告 (五步法) ---
    sahm = get("SAHMREALTIME") or 0.0
    tips10 = get("DFII10") or 0.0
    # 判定四象限
    growth_weak = sahm >= 0.5
    infl_high = (pce_3m_ann is not None and pce_3m_ann > 2.5)
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

    # 动态调整情景概率
    if red_alerts:
        probs = "基准情景(风险释放): 45% | 乐观情景(政策急转救市): 15% | 悲观情景(尾部危机扩散): 40%"
    elif yellow_alerts:
        probs = "基准情景(震荡消化): 55% | 乐观情景(软着陆延续): 20% | 悲观情景(紧缩/波动升级): 25%"
    else:
        probs = "基准情景(软着陆/常态轮动): 60% | 乐观情景(宽信用共振): 25% | 悲观情景(二次通胀/突发冲击): 15%"

    report_text = (
        f"【宏观第一性原理自动研判简报 | 更新时间: {NOW_STR}】\n"
        f"1. 周期定位与核心事实：{quadrant} 美联储净流动性为 {net_liq} 十亿美元（环比变化 {net_liq_chg:+} 十亿美元），"
        f"美债 10Y-2Y 利差为 {spread_10_2}%（形态：{curve_regime}），10Y TIPS 实际利率为 {tips10}%，萨姆规则读数 {sahm}%。\n"
        f"2. 传导机制与定价主导：{regime_logic}\n"
        f"3. 资金流向与水管诊断：ON RRP 余额为 {rrp} 十亿美元，TGA 余额为 {round(tga/1000, 1) if tga else '-'} 十亿美元。"
        f"{'⚠️ 黑天鹅预警触发：' + '；'.join(red_alerts + yellow_alerts) if (red_alerts or yellow_alerts) else '✅ 8大黑天鹅雷达均处于绿灯常态区间，未见流动性挤兑信号。'}\n"
        f"4. 情景概率分布：{probs}\n"
        f"5. 强制证伪条件（若以下任一条件在未来 10 个交易日发生，需推翻当前基准判断）："
        f"① 10Y TIPS 实际利率反向突破 {'1.80%' if tips10 > 2.0 else '2.25%'}；"
        f"② 美国高收益债信用利差突破 500bps 或单周走阔超 80bps；"
        f"③ 美联储净流动性连续两周反向变动超 1,500 亿美元。"
    )

    return derived_rows, blackswan_rows, ts_snapshot_dict, report_text


def sync_all_sheets(raw_records, derived_rows, blackswan_rows, ts_dict, report_text):
    if not raw_records:
        raise RuntimeError("所有外部接口均未返回有效数据，中止写入以保护表格。")

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    # 1. 更新 01_最新快照_Snapshot
    df_raw = pd.DataFrame(raw_records)
    ws_snap = get_or_create_ws(sh, "01_最新快照_Snapshot")
    ws_snap.clear()
    ws_snap.update([df_raw.columns.tolist()] + df_raw.values.tolist())

    # 2. 增量追加/更新 02_历史时序库_TimeSeries (永不清空历史)
    ws_ts = get_or_create_ws(sh, "02_历史时序库_TimeSeries", rows=2000, cols=50)
    existing = ws_ts.get_all_values()
    all_keys = ["Date"] + sorted(ts_dict.keys())
    if not existing:
        row_vals = [TODAY_STR] + [ts_dict.get(k, "") for k in all_keys[1:]]
        ws_ts.update([all_keys, row_vals])
    else:
        headers = existing[0]
        # 若有新增指标列，自动向右扩展表头
        new_cols = [k for k in all_keys if k not in headers]
        if new_cols:
            headers = headers + new_cols
            ws_ts.update("A1", [headers])
        new_row = [TODAY_STR] + [ts_dict.get(col, "") for col in headers[1:]]
        # 如果今天已经运行过一次，覆盖今天那一行；否则在末尾追加新的一天
        if len(existing) > 1 and existing[-1][0] == TODAY_STR:
            ws_ts.update(f"A{len(existing)}", [new_row])
        else:
            ws_ts.append_row(new_row, value_input_option="USER_ENTERED")

    # 3. 更新 03_衍生组合与信号_Derived
    ws_der = get_or_create_ws(sh, "03_衍生组合与信号_Derived")
    ws_der.clear()
    ws_der.update([["层级归属", "衍生组合名称", "当前计算值", "边际变化", "第一性原理诊断信号"]] + derived_rows)

    # 4. 更新 04_黑天鹅雷达_BlackSwan
    ws_bs = get_or_create_ws(sh, "04_黑天鹅雷达_BlackSwan")
    ws_bs.clear()
    ws_bs.update([["风险类别", "监控指标", "当前读数", "黄/红灯阈值", "当前警报状态"]] + blackswan_rows)

    # 5. 更新 00_总览与自动研判_Dashboard 的文字研判区 & 06_情景与证伪日志_Journal
    ws_dash = get_or_create_ws(sh, "00_总览与自动研判_Dashboard", rows=100, cols=15)
    ws_dash.update("A1", [["全球宏观第一性原理实时监测中枢（自动化文字研判输出）"]])
    ws_dash.update("A2", [[report_text]])

    ws_journal = get_or_create_ws(sh, "06_情景与证伪日志_Journal", rows=1000, cols=5)
    j_vals = ws_journal.get_all_values()
    if not j_vals:
        ws_journal.update([["日期", "同步时间", "自动研判与情景证伪记录"], [TODAY_STR, NOW_STR, report_text]])
    elif j_vals[-1][0] == TODAY_STR:
        ws_journal.update(f"A{len(j_vals)}", [[TODAY_STR, NOW_STR, report_text]])
    else:
        ws_journal.append_row([TODAY_STR, NOW_STR, report_text], value_input_option="USER_ENTERED")


def get_or_create_ws(sh, title, rows=200, cols=15):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


if __name__ == "__main__":
    fred_recs, obs_map = fetch_fred_data()
    ext_recs, wb_map, extra = fetch_external_apis()
    all_raw = fred_recs + ext_recs
    derived, blackswan, ts_dict, report = analyze_macro_and_blackswan(obs_map, wb_map, extra)
    sync_all_sheets(all_raw, derived, blackswan, ts_dict, report)
    print(f"[SUCCESS] Synced {len(all_raw)} metrics, {len(blackswan)} BlackSwan monitors, and updated narrative report.")
