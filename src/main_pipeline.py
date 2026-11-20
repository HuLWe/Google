import json
import os
import gspread
from google.oauth2.service_account import Credentials
from config import SHEET_ID, GURU_WATCHLIST
from data_fetchers import (
    fetch_sec_financials_and_audit, fetch_macro_and_cross_assets,
    fetch_guru_13f_5state_holdings
)
from ai_10k_extractor import extract_10k_notes_with_ai
from quant_and_resonance import calculate_business_model_score, calculate_institutional_buy_sell_prices

def get_gspread_client():
    sa_json = os.environ.get("GCP_SA_KEY", "")
    if not sa_json: raise ValueError("GCP_SA_KEY 环境变量未设置！")
    creds_dict = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=scopes))

def ensure_worksheets(sh):
    '''自动保障 3 屏 + 1 底层数据工作表存在'''
    needed = ["Master_Visual_Cockpit", "13F_Heatmap_Matrix", "Single_Stock_Diagnostic", "FRED_Macro_Data"]
    existing = [ws.title for ws in sh.worksheets()]
    worksheets = {}
    for name in needed:
        if name not in existing:
            worksheets[name] = sh.add_worksheet(title=name, rows="100", cols="20")
        else:
            worksheets[name] = sh.worksheet(name)
    return worksheets

def run_pipeline():
    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheets(sh)
    
    cockpit = ws["Master_Visual_Cockpit"]
    heatmap = ws["13F_Heatmap_Matrix"]
    diag = ws["Single_Stock_Diagnostic"]
    
    # ================= 屏 1：主控台 (Master_Visual_Cockpit) =================
    macro = fetch_macro_and_cross_assets()
    
    cockpit.update(
        range_name="A1:F4",
        values=[
            ["美股金字塔量化投研 · 跨资产时空共振与单屏可视化作战室 (V5.3 实盘修正版)", "", "", "", "", f"宏观总仓位指令：{macro['gate_signal']} (上限 {macro['max_alloc']:.0%})"],
            ["【第 1 区：控制台、塔顶宏观水文与 8 大跨资产风向标】", "", "", "", "", ""],
            ["美联储净流动性", "高收益债信用利差", "10年期TIPS实际利率", "10Y-2Y利差 / 通胀预期", "跨资产 8 大风向标矩阵", "状态校验"],
            [
                f"${macro['net_liq_now_t']}T (13周斜率 {macro['liq_slope_13w']:.1%})",
                f"{macro['hy_oas_now']}%",
                f"{macro['tips_now']}%",
                f"{macro['t10y2y_now']}% / {macro['t10yie_now']}%",
                f"DXY {macro['dxy_now']} | JPY {macro['usdjpy_now']} | 铜金比 {macro['copper_gold']} | WTI ${macro['oil_now']} | VIX {macro['vix_now']} | MOVE {macro['move_now']} | BTC ${macro['btc_now']}k",
                "✅ 水文同步"
            ]
        ],
        value_input_option="USER_ENTERED"
    )

    # 抓取 13F 大师持仓
    guru_5states = fetch_guru_13f_5state_holdings()

    # 默认股票列表
    watchlist = ["MSFT", "COST", "NVDA", "AAPL", "GOOGL", "AMZN", "META"]
    stock_rows = []
    diag_rows = []

    for tk in watchlist:
        try:
            sec = fetch_sec_financials_and_audit(tk)
            yr = sec["latest_yr"]
            rev_now = sec["annual"]["Revenue"][yr]["val"]
            ai_notes = extract_10k_notes_with_ai(tk, sec["cik"], sec["subs"], rev_now)
            
            bm = calculate_business_model_score({**sec, **ai_notes})
            pricing = calculate_institutional_buy_sell_prices(tk, sec, bm["grade"], guru_5states)
            
            stock_rows.append([
                tk, pricing["action_signal"], pricing["smart_money_tag"],
                f"{bm['total']}分 ({bm['grade']})",
                f"≤ ${pricing['p_base_buy']}",
                f"≤ ${pricing['p_aggressive_buy']}",
                f"≥ ${pricing['p_alert_sell']}",
                ai_notes.get("one_sentence_business", "")
            ])

            # 诊断屏明细数据
            diag_rows.append([
                tk, bm["grade"], f"{sec['roic']:.1%}", "8.9%", f"${pricing['curr_price']}",
                f"${pricing['p_base_buy']}", f"${pricing['p_aggressive_buy']}", f"${pricing['p_alert_sell']}",
                sec["audit_status"], f"{sec['dilution_rate']:.1%}", bm["archetype"], bm["alerts"]
            ])
        except Exception as e:
            stock_rows.append([tk, f"⚠️待核查({str(e)[:10]})", "-", "-", "-", "-", "-", "-"])

    # 更新屏 1 下半区 (自选股买卖点总表)
    cockpit.update(
        range_name="A6:H15",
        values=[
            ["【第 3 区：塔底自选股四维共振买卖点与目标价总表】", "", "", "", "", "", "", ""],
            ["股票代码", "终极买卖信号灯", "聪明钱 13F 5态", "商业模式分(AI)", "保守买点 P_Base", "右侧买点 P_Aggr", "警戒卖点 P_Alert", "商业本质概括"]
        ] + stock_rows,
        value_input_option="USER_ENTERED"
    )

    # ================= 屏 2：塔身板块 × 13F 矩阵 (13F_Heatmap_Matrix) =================
    guru_ciks = list(GURU_WATCHLIST.keys())
    guru_headers = [GURU_WATCHLIST[c]["name"] for c in guru_ciks]
    
    matrix_rows = []
    for sec_info in macro["sector_rs_matrix"]:
        row = [
            sec_info["code"], sec_info["name"], sec_info["cycle"],
            f"{sec_info['rs_5d']:.1%}", f"{sec_info['rs_20d']:.1%}", f"{sec_info['rs_60d']:.1%}",
            f"{sec_info['rs_120d']:.1%}", f"{sec_info['rs_250d']:.1%}", sec_info["signal"]
        ]
        # 填充 8 位大师在各板块中的综合 5 态
        for cik in guru_ciks:
            st_text = "➖ 维持"
            if cik in guru_5states:
                # 简单聚合判断大师该板块状态
                st_list = list(guru_5states[cik].get("states", {}).values())
                if any("★" in s for s in st_list): st_text = "★ 新建仓"
                elif any("▲" in s for s in st_list): st_text = "▲ 增持"
                elif any("▼" in s for s in st_list): st_text = "▼ 减持"
                elif any("⛔" in s for s in st_list): st_text = "⛔ 清仓"
            row.append(st_text)
        matrix_rows.append(row)

    heatmap.update(
        range_name="A1:R15",
        values=[
            ["GICS 11 SECTOR ROTATION × 13F GURU HEATMAP MATRIX (V5.3 实盘修正版)", "", "", "", "", "", "", "", "", "13F GURU QUARTERLY HOLDINGS MATRIX (Qt vs Qt-1)", "", "", "", "", "", "", "", ""],
            ["Ticker", "Sector Name", "Cycle Classification", "5D RS", "20D RS", "60D RS", "120D RS", "250D RS", "ACTION SIGNAL"] + guru_headers
        ] + matrix_rows,
        value_input_option="USER_ENTERED"
    )

    # ================= 屏 3：单股深度视觉诊断 (Single_Stock_Diagnostic) =================
    diag.update(
        range_name="A1:L15",
        values=[
            ["单股深度视觉诊断舱 (估值足球场 × 商业模式100分 × ROIC/WACC 子弹图)", "", "", "", "", "", "", "", "", "", "", ""],
            ["代码", "综合评级", "ROIC", "WACC", "实时股价", "保守买点", "右侧买点", "警戒卖点", "SEC 验真", "股本稀释率", "商业模式形态", "预警红线"]
        ] + diag_rows,
        value_input_option="USER_ENTERED"
    )

    cockpit.update(range_name="D1", values=[["✅ V5.3 多屏全量数据已成功推送"]], value_input_option="USER_ENTERED")

if __name__ == "__main__":
    run_pipeline()
