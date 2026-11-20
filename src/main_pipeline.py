import json
import os
import gspread
import pandas as pd
import yfinance as yf
from google.oauth2.service_account import Credentials
from config import SHEET_ID, GURU_WATCHLIST, WATCHLIST, SHEET_NAMES
from data_fetchers import (
    fetch_sec_financials_and_audit, fetch_macro_and_cross_assets,
    fetch_guru_13f_5state_holdings
)
from ai_10k_extractor import extract_10k_notes_with_ai
from quant_and_resonance import calculate_business_model_score, calculate_institutional_buy_sell_prices

def get_gspread_client():
    sa_json = os.environ.get("GCP_SA_KEY", "")
    if not sa_json: raise ValueError("未检测到 GCP_SA_KEY 环境变量！")
    creds_dict = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=scopes))

def ensure_worksheets(sh):
    existing = [ws.title for ws in sh.worksheets()]
    worksheets = {}
    for key, name in SHEET_NAMES.items():
        if name not in existing:
            worksheets[key] = sh.add_worksheet(title=name, rows="500", cols="30")
        else:
            worksheets[key] = sh.worksheet(name)
    return worksheets

def _safe_extract_series(df_hist: pd.DataFrame, ticker: str) -> pd.Series:
    if isinstance(df_hist, pd.Series): return df_hist
    if ticker in df_hist.columns:
        col_data = df_hist[ticker]
        if isinstance(col_data, pd.DataFrame): return col_data.iloc[:, 0]
        return col_data
    return df_hist.iloc[:, 0]

def col_to_letter(col_idx: int) -> str:
    """数字列号转换为 Excel 字母列名 (1->A, 2->B, 27->AA)"""
    result = ""
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx - 1, 26)
        result = chr(65 + remainder) + result
    return result

def run_pipeline():
    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheets(sh)
    
    cockpit = ws["COCKPIT"]
    heatmap = ws["HEATMAP"]
    diag = ws["DIAGNOSTIC"]
    db_store = ws["DATABASE"]
    
    # 1. 刷新主控台顶部宏观水文
    macro = fetch_macro_and_cross_assets()
    cockpit.update(
        range_name="A1",
        values=[
            ["美股金字塔量化投研 · 跨资产时空共振与单屏可视化作战室 (V5.3 终极版)", "", "", "", "", f"宏观总仓位指令：{macro['gate_signal']} (上限 {macro['max_alloc']:.0%})"],
            ["【第 1 区：控制台、塔顶宏观水文与 8 大跨资产风向标】", "", "", "", "", ""],
            ["美联储净流动性", "高收益债信用利差", "10年期TIPS实际利率", "10Y-2Y利差 / 通胀预期", "跨资产 8 大风向标矩阵", "状态校验"],
            [
                f"${macro['net_liq_now_t']}T (13周斜率 {macro['liq_slope_13w']:.1%})",
                f"{macro['hy_oas_now']}%",
                f"{macro['tips_now']}%",
                f"{macro['t10y2y_now']}% / {macro['t10yie_now']}%",
                f"DXY {macro['dxy_now']} | JPY {macro['usdjpy_now']} | 铜金比 {macro['copper_gold']} | WTI ${macro['oil_now']} | VIX {macro['vix_now']} | BTC ${macro['btc_now']}k",
                "✅ 水文已同步"
            ]
        ],
        value_input_option="USER_ENTERED"
    )

    guru_5states = fetch_guru_13f_5state_holdings()
    
    print("🚀 正在一次性批量拉取自选股历史 K 线数据...")
    df_raw = yf.download(WATCHLIST, period="5y", progress=False)
    
    if isinstance(df_raw.columns, pd.MultiIndex):
        if 'Close' in df_raw.columns.levels[0]:
            all_hist = df_raw['Close'].ffill()
        else:
            all_hist = df_raw.xs('Close', axis=1, level=0).ffill()
    else:
        all_hist = df_raw['Close'].ffill() if 'Close' in df_raw.columns else df_raw.ffill()

    # 2. 准备底层历史 K 线留存数据
    print("💾 正在处理全量数据库格式...")
    hist_to_save = all_hist.reset_index()
    hist_to_save['Date'] = pd.to_datetime(hist_to_save['Date']).dt.strftime('%Y-%m-%d')
    save_headers = ["交易日期"] + [str(col) for col in hist_to_save.columns[1:]]
    save_values = hist_to_save.tail(100).fillna("-").values.tolist()

    stock_rows = []
    diag_rows = []
    db_rows = []

    for idx, tk in enumerate(WATCHLIST):
        try:
            sec = fetch_sec_financials_and_audit(tk)
            yr = sec["latest_yr"]
            rev_now = sec["annual"]["Revenue"][yr]["val"]
            ai_notes = extract_10k_notes_with_ai(tk, sec["cik"], sec["subs"], rev_now)
            
            bm = calculate_business_model_score({**sec, **ai_notes})
            hist_series = _safe_extract_series(all_hist, tk)
            pricing = calculate_institutional_buy_sell_prices(tk, sec, bm["grade"], hist_series, guru_5states)
            
            # 动态计算 Sparkline 数据引用的列字母（B 列开始为第一个股票）
            col_letter = col_to_letter(idx + 2)
            db_start_row = len(WATCHLIST) + 5  # 计算四屏 K 线明细的起始行号
            db_end_row = db_start_row + len(save_values) - 1
            sparkline_formula = f'=SPARKLINE(\'{SHEET_NAMES["DATABASE"]}\'!{col_letter}{db_start_row}:{col_letter}{db_end_row}, {{"color", "#38BDF8"; "linewidth", 2}})'

            stock_rows.append([
                tk, pricing["action_signal"], pricing["smart_money_tag"],
                f"{bm['total']}分 ({bm['grade']})",
                f"≤ ${pricing['p_base_buy']}",
                f"≤ ${pricing['p_aggressive_buy']}",
                f"≥ ${pricing['p_alert_sell']}",
                sparkline_formula,
                ai_notes.get("one_sentence_business", "")
            ])

            diag_rows.append([
                tk, bm["grade"], f"{sec['roic']:.1%}", "8.9%", f"${pricing['curr_price']}",
                f"${pricing['p_base_buy']}", f"${pricing['p_aggressive_buy']}", f"${pricing['p_alert_sell']}",
                sec["audit_status"], f"{sec['dilution_rate']:.1%}", bm["archetype"], bm["alerts"]
            ])

            db = sec["real_db"]
            db_rows.append([
                tk, db["pe_ttm"], db["pe_forward"], db["pb_ratio"], db["ps_ratio"], db["ev_ebitda"],
                db["market_cap_b"], db["total_rev_b"], db["net_income_b"], db["fcf_b"], sec["audit_status"]
            ])

        except Exception as e:
            err_msg = str(e).replace("\n", " ")
            stock_rows.append([tk, f"⚠️待核查({err_msg[:10]})", "-", "-", "-", "-", "-", "-", "-"])

    # 写入主控台自选股（使用动态起始单元格 A6）
    cockpit.update(
        range_name="A6",
        values=[
            ["【第 3 区：塔底自选股四维共振买卖点与目标价总表】", "", "", "", "", "", "", "", ""],
            ["股票代码", "终极买卖信号", "聪明钱 13F 态", "商业模式分(AI)", "保守买点", "右侧买点", "警戒卖点", "100日走势", "商业本质概括"]
        ] + stock_rows,
        value_input_option="USER_ENTERED"
    )

    # 写入二屏 13F 矩阵（使用动态起始单元格 A1）
    guru_ciks = list(GURU_WATCHLIST.keys())
    guru_headers = [GURU_WATCHLIST[c]["name"] for c in guru_ciks]
    matrix_rows = []
    for sec_info in macro["sector_rs_matrix"]:
        row = [
            sec_info["code"], sec_info["name"], sec_info["cycle"],
            f"{sec_info['rs_5d']:.1%}", f"{sec_info['rs_20d']:.1%}", f"{sec_info['rs_60d']:.1%}",
            f"{sec_info['rs_120d']:.1%}", f"{sec_info['rs_250d']:.1%}", sec_info["signal"]
        ]
        for cik in guru_ciks:
            st_text = "➖ 维持"
            if cik in guru_5states:
                st_list = list(guru_5states[cik].get("states", {}).values())
                if any("★" in s for s in st_list): st_text = "★ 新建仓"
                elif any("▲" in s for s in st_list): st_text = "▲ 增持"
                elif any("▼" in s for s in st_list): st_text = "▼ 减持"
                elif any("⛔" in s for s in st_list): st_text = "⛔ 清仓"
            row.append(st_text)
        matrix_rows.append(row)

    heatmap.update(
        range_name="A1",
        values=[
            ["GICS 11 SECTOR ROTATION × 13F GURU HEATMAP MATRIX (V5.3 实盘修正版)", "", "", "", "", "", "", "", "", "13F GURU QUARTERLY HOLDINGS MATRIX (Qt vs Qt-1)", "", "", "", "", "", "", "", ""],
            ["Ticker", "Sector Name", "Cycle Classification", "5D RS", "20D RS", "60D RS", "120D RS", "250D RS", "ACTION SIGNAL"] + guru_headers
        ] + matrix_rows,
        value_input_option="USER_ENTERED"
    )

    # 写入三屏 估值诊断（使用动态起始单元格 A1）
    diag.update(
        range_name="A1",
        values=[
            ["单股深度视觉诊断舱 (估值足球场 × 商业模式100分 × ROIC/WACC 子弹图)", "", "", "", "", "", "", "", "", "", "", ""],
            ["代码", "综合评级", "ROIC", "WACC", "实时股价", "保守买点", "右侧买点", "警戒卖点", "SEC 验真", "股本稀释率", "商业模式形态", "预警红线"]
        ] + diag_rows,
        value_input_option="USER_ENTERED"
    )

    # 写入四屏 数据库与明细（彻底解决 400 溢出报错的关键：清空后使用 A1 起始写入）
    db_store.clear()
    full_db_payload = [
        ["底层财务报表与真实估值数据库 (包含真实 PE/PB/PS/市值明细)", "", "", "", "", "", "", "", "", "", ""],
        ["股票代码", "PE(TTM)", "前瞻 PE", "市净率 PB", "市销率 PS", "EV/EBITDA", "总市值(亿美元)", "营收(亿美元)", "净利润(亿美元)", "自由现金流(亿美元)", "SEC 审计状态"]
    ] + db_rows + [
        ["", "", "", "", "", "", "", "", "", "", ""],
        ["全量 100 交易日收盘价留存明细：", "", "", "", "", "", "", "", "", "", ""]
    ] + [save_headers] + save_values

    db_store.update(
        range_name="A1",
        values=full_db_payload,
        value_input_option="USER_ENTERED"
    )

    cockpit.update(range_name="D1", values=[["✅ V5.3 全中文标签页与动态数据库已成功同步"]], value_input_option="USER_ENTERED")

if __name__ == "__main__":
    run_pipeline()
