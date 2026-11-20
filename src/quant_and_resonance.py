import numpy as np
import pandas as pd


def calculate_business_model_score(metrics: dict) -> dict:
    """计算商业模式百分制得分（Q1 业务形态 30 分 + Q2 客户与现金流 30 分 + Q3 变现可持续性 40 分）"""
    score_q1, score_q2, score_q3 = 0.0, 0.0, 0.0
    tags, alerts = [], []

    # 第一部分：做什么生意（30 分）
    seg_shares = metrics.get("segment_shares", [1.0])
    seg_hhi = sum(s ** 2 for s in seg_shares)
    if seg_hhi < 0.65:
        score_q1 += 12.0
    elif seg_hhi <= 0.75:
        score_q1 += 9.0
    else:
        score_q1 += 4.0
        alerts.append(f"⚠️主业集中(HHI={seg_hhi:.2f})")

    max_foreign = metrics.get("max_single_foreign_geo_pct", 0.0)
    if max_foreign > 0.35:
        score_q1 += 2.0
        alerts.append(f"🚨海外风险>{max_foreign:.0%}")
    else:
        score_q1 += 6.0

    gm_mean = metrics.get("gross_margin_10y_mean", 0.4)
    inv_assets = metrics.get("inventory_to_assets", 0.05)
    capex_rev = metrics.get("capex_to_revenue", 0.10)

    if gm_mean > 0.70 and inv_assets < 0.01 and capex_rev < 0.08:
        archetype = "①轻资产软件/平台型"
        score_q1 += 12.0
    elif gm_mean >= 0.50:
        archetype = "准轻资产高毛利型"
        score_q1 += 9.5
    else:
        archetype = "传统制造/流通型"
        score_q1 += 6.0

    # 第二部分：赚谁的钱（30 分）
    max_cust = metrics.get("max_single_customer_pct", 0.0)
    if max_cust >= 0.25:
        alerts.append(f"🚨客户集中({max_cust:.0%})")
    elif max_cust >= 0.10:
        score_q2 += 8.0
    else:
        score_q2 += 12.0

    dso, dpo, ccc = metrics.get("dso_days", 45.0), metrics.get("dpo_days", 45.0), metrics.get("ccc_days", 30.0)
    spread = dpo - dso
    if spread > 30 and ccc < 0:
        score_q2 += 18.0
        tags.append(f"产业链占款霸主(+{spread:.0f}天)")
    elif spread > 0:
        score_q2 += 12.0
    else:
        score_q2 += 6.0

    # 第三部分：变现可持续性（40 分）
    def_ratio = metrics.get("deferred_rev_to_ttm_rev", 0.0)
    score_q3 += 12.0 if def_ratio >= 0.30 else (8.0 if def_ratio >= 0.15 else 4.0)

    q_series = metrics.get("quarterly_yoy_growth_5y", [])
    rev_sharpe = float(np.mean(q_series) / (np.std(q_series, ddof=1) + 1e-6)) if len(q_series) >= 8 else 1.0
    score_q3 += 16.0 if rev_sharpe >= 2.0 else (10.0 if rev_sharpe >= 1.0 else 4.0)

    gm_std = metrics.get("gross_margin_10y_std", 0.05)
    score_q3 += 12.0 if gm_std <= 0.025 else 6.0

    total = round(score_q1 + score_q2 + score_q3, 1)
    grade = "S级" if total >= 80 else ("A级" if total >= 65 else ("B级" if total >= 50 else "C/D级"))

    return {
        "total": total, "grade": grade,
        "q1": round(score_q1, 1), "q2": round(score_q2, 1), "q3": round(score_q3, 1),
        "seg_hhi": round(seg_hhi, 3), "archetype": archetype,
        "tags": " | ".join(tags), "alerts": " | ".join(alerts) if alerts else "无红线"
    }


def calculate_institutional_buy_sell_prices(
    ticker: str, sec_data: dict, bm_grade: str, hist: pd.Series, guru_5states: dict = None
) -> dict:
    """接收外部传入的收盘价序列，计算 DCF 内在价值、双层买入价、警戒卖点与买卖信号"""
    s = pd.to_numeric(hist, errors="coerce").dropna()
    if len(s) == 0:
        raise ValueError(f"股票 {ticker} 历史收盘价序列为空")

    curr_price = float(s.iloc[-1])

    yr = sec_data["latest_yr"]
    ann = sec_data["annual"]
    shares = ann["Shares"].get(yr, {}).get("val") or 1e9
    cash = ann["Cash"].get(yr, {}).get("val") or 0.0
    debt = (ann["Debt_Short"].get(yr, {}).get("val") or 0.0) + (ann["Debt_Long"].get(yr, {}).get("val") or 0.0)

    ocf = ann["OCF"].get(yr, {}).get("val") or 0.0
    capex = ann["CapEx"].get(yr, {}).get("val") or 0.0
    fcf_base = max(ocf - capex, 0.0)

    dilution_rate = sec_data.get("dilution_rate", 0.0)
    dilution_penalty = min(0.20, dilution_rate * 2.0)

    wacc, g_term = 0.09, 0.025
    g_start = min(0.20, max(0.04, sec_data.get("rev_cagr", 0.08)))

    pv = 0.0
    fcf_t = fcf_base
    for t in range(1, 11):
        g_t = g_start - (g_start - 0.03) * ((t - 1) / 9.0)
        fcf_t *= (1.0 + g_t)
        pv += fcf_t / ((1.0 + wacc) ** t)

    term_val = (fcf_t * (1.0 + g_term)) / (wacc - g_term)
    pv_term = term_val / ((1.0 + wacc) ** 10)

    eq_val = max(pv + pv_term + cash - debt, 0.0) * (1.0 - dilution_penalty)
    p_dcf_intrinsic = eq_val / shares

    mos_req = 0.20 if "S" in bm_grade else 0.28
    p_buy_dcf = p_dcf_intrinsic * (1.0 - mos_req)
    p_hist_20 = float(np.percentile(s, 20))

    w_close = s.resample("W").last().dropna()
    ma_20w = float(w_close.rolling(20).mean().iloc[-1]) if len(w_close) >= 20 else float(w_close.mean())

    p_base_buy = round((p_buy_dcf + p_hist_20) / 2.0, 2)
    p_aggressive_buy = round(max(ma_20w, min(p_buy_dcf, p_hist_20 * 1.15)), 2)
    p_alert_sell = round(max(p_dcf_intrinsic * 1.30, float(np.percentile(s, 85))), 2)

    smart_money_tag = "➖ 维持/观望"
    if guru_5states:
        new_inc, dec_exit = 0, 0
        for g_data in guru_5states.values():
            for iss, st in g_data.get("states", {}).items():
                if ticker.upper() in iss:
                    if "★" in st or "▲" in st:
                        new_inc += 1
                    if "▼" in st or "⛔" in st:
                        dec_exit += 1
        if new_inc >= 2 and dec_exit == 0:
            smart_money_tag = "🔥 聪明钱强力加码"
        elif new_inc > dec_exit:
            smart_money_tag = "🟢 聪明钱偏多"
        elif dec_exit >= 2:
            smart_money_tag = "🔴 聪明钱撤退"

    if curr_price <= p_base_buy:
        action = "🔥 强力买入(四维共振)"
    elif curr_price <= p_aggressive_buy:
        action = "🟢 分批建仓"
    elif curr_price >= p_alert_sell:
        action = "🔴 逢高止盈"
    else:
        action = "🟡 持有观望"

    return {
        "curr_price": round(curr_price, 2),
        "p_intrinsic": round(p_dcf_intrinsic, 2),
        "p_base_buy": p_base_buy,
        "p_aggressive_buy": p_aggressive_buy,
        "p_alert_sell": p_alert_sell,
        "smart_money_tag": smart_money_tag,
        "action_signal": action
    }
