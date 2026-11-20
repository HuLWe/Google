import time
import xml.etree.ElementTree as ET
from datetime import datetime
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from config import SEC_USER_AGENT, FRED_API_KEY, FRED_SERIES, CROSS_ASSET_TICKERS, GICS_SECTORS, GURU_WATCHLIST, XBRL_TAG_MAP

SEC_HEADERS = {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
_LAST_SEC_CALL = 0.0

def sec_get(url: str, is_json: bool = True):
    global _LAST_SEC_CALL
    elapsed = time.time() - _LAST_SEC_CALL
    if elapsed < 0.12:
        time.sleep(0.12 - elapsed)
        
    for attempt in range(4):
        try:
            resp = requests.get(url, headers=SEC_HEADERS, timeout=20)
            _LAST_SEC_CALL = time.time()
            if resp.status_code == 200:
                return resp.json() if is_json else resp.text
            elif resp.status_code == 429:
                time.sleep(2.0 * (attempt + 1))
            else:
                time.sleep(1.5 * (attempt + 1))
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"SEC API 请求失败: {url}")

def get_cik_and_submissions(ticker: str) -> tuple[str, dict]:
    tickers_json = sec_get("https://www.sec.gov/files/company_tickers.json")
    cik = next((str(item["cik_str"]).zfill(10) for item in tickers_json.values() if item["ticker"].upper() == ticker.upper()), None)
    if not cik:
        raise ValueError(f"SEC 未找到代码: {ticker}")
    subs = sec_get(f"https://data.sec.gov/submissions/CIK{cik}.json")
    return cik, subs

def _extract_series(us_gaap: dict, tags: list, mode: str = "annual") -> dict:
    for tag in tags:
        if tag not in us_gaap: continue
        units = us_gaap[tag].get("units", {})
        unit_key = "USD" if "USD" in units else ("shares" if "shares" in units else None)
        if not unit_key: continue
        records = {}
        for rec in units[unit_key]:
            form = rec.get("form", "")
            if mode == "annual" and form not in ("10-K", "20-F"): continue
            if mode == "quarterly" and form not in ("10-Q", "10-K"): continue
            end_dt, start_dt = rec.get("end"), rec.get("start")
            if start_dt:
                days = (datetime.strptime(end_dt, "%Y-%m-%d") - datetime.strptime(start_dt, "%Y-%m-%d")).days
                if mode == "annual" and not (350 <= days <= 375): continue
                if mode == "quarterly" and not (80 <= days <= 100): continue
            elif mode == "quarterly": continue
            key = end_dt[:4] if mode == "annual" else end_dt[:7]
            if key not in records or rec["filed"] > records[key]["filed"]:
                records[key] = {"val": float(rec["val"]), "filed": rec["filed"], "end": end_dt}
        if len(records) >= 2: return records
    return {}

def fetch_sec_financials_and_audit(ticker: str) -> dict:
    cik, subs = get_cik_and_submissions(ticker)
    facts = sec_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    annual = {k: _extract_series(us_gaap, v, "annual") for k, v in XBRL_TAG_MAP.items()}
    q_rev = _extract_series(us_gaap, XBRL_TAG_MAP["Revenue"], "quarterly")
    
    years = sorted(annual["Revenue"].keys())[-10:]
    if not years: raise ValueError(f"{ticker} 缺失有效财报数据")
    yr = years[-1]
    
    assets = annual["Assets"].get(yr, {}).get("val")
    liab = annual["Liabilities"].get(yr, {}).get("val")
    eq = annual["Equity"].get(yr, {}).get("val")
    if assets and eq and liab is None: liab = assets - eq
    bs_err = abs(assets - (liab + eq)) / assets if (assets and liab is not None and eq is not None) else 1.0
    audit_status = "PASS" if bs_err <= 0.01 else f"WARN({bs_err:.1%})"
    
    gm_list, rev_list = [], []
    for y in years:
        r_v = annual["Revenue"].get(y, {}).get("val")
        gp_v = annual["GrossProfit"].get(y, {}).get("val")
        cogs_v = annual["COGS"].get(y, {}).get("val")
        if r_v and r_v > 0:
            rev_list.append(r_v)
            if gp_v is not None: gm_list.append(gp_v / r_v)
            elif cogs_v is not None: gm_list.append((r_v - cogs_v) / r_v)
                
    rev_cagr = (rev_list[-1] / rev_list[0]) ** (1 / (len(rev_list) - 1)) - 1.0 if len(rev_list) >= 3 and rev_list[0] > 0 else 0.08
    gm_22_23 = 0.0
    if "2022" in annual["Revenue"] and "2023" in annual["Revenue"]:
        r22, r23 = annual["Revenue"]["2022"]["val"], annual["Revenue"]["2023"]["val"]
        g22 = annual["GrossProfit"].get("2022", {}).get("val", 0) / r22
        g23 = annual["GrossProfit"].get("2023", {}).get("val", 0) / r23
        gm_22_23 = g23 - g22
        
    rev_now = annual["Revenue"][yr]["val"]
    cogs_now = annual["COGS"].get(yr, {}).get("val") or (rev_now * (1 - (gm_list[-1] if gm_list else 0.4)))
    ar_now = annual["AR"].get(yr, {}).get("val") or 0.0
    ap_now = annual["AP"].get(yr, {}).get("val") or 0.0
    inv_now = annual["Inventory"].get(yr, {}).get("val") or 0.0
    
    dso = (ar_now / rev_now) * 365.0 if rev_now > 0 else 45.0
    dpo = (ap_now / cogs_now) * 365.0 if cogs_now > 0 else 45.0
    dio = (inv_now / cogs_now) * 365.0 if cogs_now > 0 else 0.0
    ccc = dio + dso - dpo
    
    sh_curr = annual["Shares"].get(yr, {}).get("val") or 1.0
    sh_prev = annual["Shares"].get(years[0], {}).get("val") or sh_curr
    dilution_rate = ((sh_curr / sh_prev) ** (1.0 / max(1, len(years)-1))) - 1.0 if sh_prev > 0 else 0.0
    
    q_keys = sorted(q_rev.keys())
    q_yoy = [(q_rev[q_keys[i]]["val"] - q_rev[q_keys[i - 4]]["val"]) / q_rev[q_keys[i - 4]]["val"]
             for i in range(4, len(q_keys)) if q_rev[q_keys[i - 4]]["val"] > 0][-20:]
             
    def_curr = annual["DeferredRev_Curr"].get(yr, {}).get("val") or 0.0
    def_noncurr = annual["DeferredRev_NonCurr"].get(yr, {}).get("val") or 0.0
    
    ebit_now = annual["EBIT"].get(yr, {}).get("val") or (rev_now * 0.20)
    tax_rate = 0.21
    nopat = ebit_now * (1.0 - tax_rate)
    invested_capital = (annual["Equity"].get(yr, {}).get("val") or 1.0) + (annual["Debt_Long"].get(yr, {}).get("val") or 0.0)
    roic = nopat / invested_capital if invested_capital > 0 else 0.15

    return {
        "ticker": ticker.upper(), "cik": cik, "subs": subs, "latest_yr": yr,
        "filing_date": annual["Revenue"][yr]["filed"], "annual": annual,
        "audit_status": audit_status, "rev_cagr": rev_cagr, "dilution_rate": max(0.0, dilution_rate),
        "gross_margin_10y_mean": float(np.mean(gm_list)) if gm_list else 0.4,
        "gross_margin_10y_std": float(np.std(gm_list)) if gm_list else 0.05,
        "gm_change_2022_2023": gm_22_23,
        "inventory_to_assets": (inv_now / assets) if assets else 0.0,
        "ppe_to_assets": (annual["PPE"].get(yr, {}).get("val", 0.0) / assets) if assets else 0.0,
        "capex_to_revenue": (annual["CapEx"].get(yr, {}).get("val", 0.0) / rev_now) if rev_now else 0.0,
        "inventory_turnover": (cogs_now / inv_now) if inv_now > 0 else 99.0,
        "dso_days": dso, "dpo_days": dpo, "ccc_days": ccc,
        "deferred_rev_to_ttm_rev": (def_curr + def_noncurr) / rev_now if rev_now > 0 else 0.0,
        "quarterly_yoy_growth_5y": q_yoy, "roic": roic
    }

def fetch_macro_and_cross_assets() -> dict:
    macro = {}
    for name, series_id in FRED_SERIES.items():
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=260"
        resp = requests.get(url, timeout=15).json()
        vals = [float(o["value"]) for o in resp.get("observations", []) if o["value"] != "."]
        macro[name] = vals[::-1]
        
    n = min(len(macro["WALCL"]), len(macro["RRP"]), len(macro["TGA"]))
    net_liq = [macro["WALCL"][-n + i] / 1000.0 - macro["RRP"][-n + i] - macro["TGA"][-n + i] for i in range(n)]
    liq_slope_13w = (net_liq[-1] - net_liq[-13]) / net_liq[-13] if len(net_liq) >= 13 else 0.0
    liq_accel_4w = ((net_liq[-1] - net_liq[-4]) - (net_liq[-4] - net_liq[-8])) / net_liq[-1] if len(net_liq) >= 8 else 0.0
    
    hy_oas = macro["HY_OAS"][-1] if macro["HY_OAS"] else 3.5
    tips_10y = macro["TIPS_10Y"][-1] if macro["TIPS_10Y"] else 1.8
    t10y2y = macro["YIELD_CURVE"][-1] if macro["YIELD_CURVE"] else 0.2
    t10yie = macro["T10YIE"][-1] if macro["T10YIE"] else 2.2
    
    ca_symbols = ["SPY"] + list(CROSS_ASSET_TICKERS.values()) + list(GICS_SECTORS.keys())
    ca_hist = yf.download(ca_symbols, period="2y", progress=False)["Close"].ffill()
    
    spy = ca_hist["SPY"]
    above_200 = float(spy.iloc[-1] > spy.rolling(200).mean().iloc[-1])
    dxy_now = float(ca_hist["DX-Y.NYB"].iloc[-1])
    usdjpy_now = float(ca_hist["JPY=X"].iloc[-1])
    copper_gold = float(ca_hist["HG=F"].iloc[-1] / ca_hist["GC=F"].iloc[-1])
    oil_now = float(ca_hist["CL=F"].iloc[-1])
    vix_now = float(ca_hist["^VIX"].iloc[-1])
    move_now = float(ca_hist["^MOVE"].iloc[-1]) if "^MOVE" in ca_hist else 85.0
    btc_now = float(ca_hist["BTC-USD"].iloc[-1])
    
    if hy_oas > 5.0 or vix_now > 32 or move_now > 120 or (liq_slope_13w < -0.03 and not above_200):
        gate_signal, max_alloc = "🔴 危机防守", 0.30
    elif liq_accel_4w > 0 and hy_oas < 4.2 and above_200:
        gate_signal, max_alloc = "🟢 全面进攻", 0.90
    else:
        gate_signal, max_alloc = "🟡 震荡控仓", 0.65

    # 计算 11 大板块 RS
    sector_rs_matrix = []
    spy_perf = {
        "5D": (spy.iloc[-1] - spy.iloc[-6]) / spy.iloc[-6],
        "20D": (spy.iloc[-1] - spy.iloc[-21]) / spy.iloc[-21],
        "60D": (spy.iloc[-1] - spy.iloc[-61]) / spy.iloc[-61],
        "120D": (spy.iloc[-1] - spy.iloc[-121]) / spy.iloc[-121],
        "250D": (spy.iloc[-1] - spy.iloc[-251]) / spy.iloc[-251],
    }
    
    for sec_code, meta in GICS_SECTORS.items():
        if sec_code in ca_hist:
            sec_series = ca_hist[sec_code]
            rs_5d = ((sec_series.iloc[-1] - sec_series.iloc[-6]) / sec_series.iloc[-6]) - spy_perf["5D"]
            rs_20d = ((sec_series.iloc[-1] - sec_series.iloc[-21]) / sec_series.iloc[-21]) - spy_perf["20D"]
            rs_60d = ((sec_series.iloc[-1] - sec_series.iloc[-61]) / sec_series.iloc[-61]) - spy_perf["60D"]
            rs_120d = ((sec_series.iloc[-1] - sec_series.iloc[-121]) / sec_series.iloc[-121]) - spy_perf["120D"]
            rs_250d = ((sec_series.iloc[-1] - sec_series.iloc[-251]) / sec_series.iloc[-251]) - spy_perf["250D"]
            
            if rs_20d > 0 and rs_60d > 0 and rs_120d > 0: signal = "🟢 主升持有"
            elif rs_250d < -0.05 and rs_5d > 0.01 and rs_20d > 0: signal = "🔥 底部启动买"
            elif rs_250d > 0.15 and rs_5d < -0.01 and rs_20d < 0: signal = "🔴 高位衰竭卖"
            else: signal = "⚪ 弱势回避"
            
            sector_rs_matrix.append({
                "code": sec_code, "name": meta["name"], "cycle": meta["cycle"],
                "rs_5d": rs_5d, "rs_20d": rs_20d, "rs_60d": rs_60d, "rs_120d": rs_120d, "rs_250d": rs_250d,
                "signal": signal
            })
        
    return {
        "net_liq_now_t": round(net_liq[-1] / 1000.0, 2),
        "liq_slope_13w": liq_slope_13w, "liq_accel_4w": liq_accel_4w,
        "hy_oas_now": round(hy_oas, 2), "tips_now": round(tips_10y, 2), "t10y2y_now": round(t10y2y, 2), "t10yie_now": round(t10yie, 2),
        "dxy_now": round(dxy_now, 2), "usdjpy_now": round(usdjpy_now, 1),
        "copper_gold": round(copper_gold, 4), "oil_now": round(oil_now, 1),
        "vix_now": round(vix_now, 1), "move_now": round(move_now, 1),
        "btc_now": round(btc_now / 1000.0, 1),
        "gate_signal": gate_signal, "max_alloc": max_alloc,
        "sector_rs_matrix": sector_rs_matrix
    }

def fetch_guru_13f_5state_holdings() -> dict:
    guru_5states = {}
    for cik, meta in GURU_WATCHLIST.items():
        try:
            subs = sec_get(f"https://data.sec.gov/submissions/CIK{cik}.json")
            recent = subs["filings"]["recent"]
            
            filings_13f = []
            for idx, form in enumerate(recent["form"]):
                if form == "13F-HR":
                    acc_num = recent["accessionNumber"][idx].replace("-", "")
                    report_date = recent["reportDate"][idx]
                    filings_13f.append((acc_num, report_date))
                if len(filings_13f) >= 2: break
                    
            if not filings_13f: continue
                
            def _parse_xml(acc_num):
                cik_int = str(int(cik))
                idx_json = sec_get(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_num}/index.json")
                xml_f = next((f["name"] for f in idx_json["directory"]["item"] if f["name"].endswith(".xml") and "primary_doc" not in f["name"]), None)
                if not xml_f: return {}
                xml_t = sec_get(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_num}/{xml_f}", is_json=False)
                root = ET.fromstring(xml_t)
                h_map = {}
                for info in root.iter():
                    if info.tag.endswith("infoTable"):
                        name_el = next((c for c in info if c.tag.endswith("nameOfIssuer")), None)
                        sh_el = next((c for c in info if c.tag.endswith("sshPrnamt")), None)
                        put_call = next((c for c in info if c.tag.endswith("putCall")), None)
                        if put_call is not None and put_call.text: continue
                        if name_el is not None and sh_el is not None:
                            h_map[name_el.text.upper()] = float(sh_el.text)
                return h_map

            curr_map = _parse_xml(filings_13f[0][0])
            prev_map = _parse_xml(filings_13f[1][0]) if len(filings_13f) > 1 else {}
            
            states = {}
            all_issuers = set(curr_map.keys()).union(set(prev_map.keys()))
            for iss in all_issuers:
                c_sh, p_sh = curr_map.get(iss, 0.0), prev_map.get(iss, 0.0)
                if p_sh == 0 and c_sh > 0: st = "★ 新建仓"
                elif c_sh == 0 and p_sh > 0: st = "⛔ 清仓"
                elif c_sh > p_sh * 1.05: st = "▲ 增持"
                elif c_sh < p_sh * 0.95: st = "▼ 减持"
                else: st = "➖ 维持"
                states[iss] = st
                
            guru_5states[cik] = {
                "name": meta["name"], "camp": meta["camp"],
                "report_date": filings_13f[0][1], "states": states
            }
        except Exception:
            continue
    return guru_5states
