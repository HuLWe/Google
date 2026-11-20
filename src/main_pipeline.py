import json
import os
import gspread
import pandas as pd
import yfinance as yf
from google.oauth2.service_account import Credentials
from config import SHEET_ID, GURU_WATCHLIST
from data_fetchers import (
    fetch_sec_financials_and_audit,
    fetch_macro_and_cross_assets,
    fetch_guru_13f_5state_holdings
)
from ai_10k_extractor import extract_10k_notes_with_ai
from quant_and_resonance import (
    calculate_business_model_score,
    calculate_institutional_buy_sell_prices
)


def get_gspread_client():
    """使用服务账号密钥初始化 Google Sheets 客户端"""
    sa_json = os.environ.get("GCP_SA_KEY", "")
    if not sa_json:
        raise ValueError("未检测到 GCP_SA_KEY 环境变量")
    creds_dict = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=scopes))


def ensure_worksheets(sh):
    """确保目标工作簿包含全部四个多屏标签页"""
    needed = ["Master_Visual_Cockpit", "13F_Heatmap_Matrix", "Single_Stock_Diagnostic", "FRED_Macro_Data"]
    existing = [ws.title for ws in sh.worksheets()]
    worksheets = {}
    for name in needed:
        if name not in existing:
            worksheets[name] = sh.add_worksheet(title=name, rows="500", cols="30")
        else:
            worksheets[name] = sh.worksheet(name)
    return worksheets


def _safe_extract_series(df_hist: pd.DataFrame, ticker: str) -> pd.Series:
    """安全提取单只股票的历史收盘价序列，兼容单列与多列结构"""
    if isinstance(df_hist, pd.Series):
        return df_hist
    if ticker in df_hist.columns:
        col_data = df_hist[ticker]
        if isinstance(col_data, pd.DataFrame):
            return col_data.iloc[:, 0]
        return col_data
    return df_hist.iloc[:, 0]


def run_pipeline():
    """执行美股金字塔量化投研多屏流水线并写入 Google Sheets"""
    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheets(sh)

    cockpit = ws["Master_Visual_Cockpit"]
    heatmap = ws["13F_Heatmap_Matrix"]
    diag = ws["Single_Stock_Diagnostic"]
    macro_store = ws["FRED_Macro_Data"]

    # 1. 更新主控台顶部宏观水文与跨资产风向标
    macro = fetch_macro_and_cross_assets()
    cockpit.update(
        range_name="A1:F4",
        values=[
            ["美股金字塔量化投研 · 跨资产时空共振与单屏可视化作战室（V5.3 实盘修正版）", "", "", "", "", f"宏观总仓位指令：{macro['gate_signal']}（上限 {macro['max_alloc']:.0%}）"],
            ["【第一区：控制台、塔顶宏观水文与八大跨资产风向标】", "", "", "", "", ""],
            ["美联储净流动性", "高收益债信用利差", "10年期 TIPS 实际利率", "10Y-2Y 利差 / 通胀预期", "跨资产风向标矩阵", "同步状态"],
            [
                f"${macro['net_liq_now_t']}T（13周斜率 {macro['liq_slope_13w']:.1%}）",
                f"{macro['hy_oas_now']}%",
                f"{macro['tips_now']}%",
                f"{macro['t10y2y_now']}% / {macro['t10yie_now']}%",
                f"美元指数 {macro['dxy_now']} | 美元兑日元 {macro['usdjpy_now']} | 铜金比 {macro['copper_gold']} | 原油 ${macro['oil_now']} | 恐慌指数 {macro['vix_now']} | 比特币 ${macro['btc_now']}k",
                "✅ 水文已同步"
            ]
        ],
        value_input_option="USER_ENTERED"
    )

    guru_5states = fetch_guru_13f_5state_holdings()
    watchlist = ["MSFT", "COST", "NVDA", "AAPL", "GOOGL", "AMZN", "META"]

    # 2. 一次性批量拉取自选股五年历史收盘价并扁平化列名
    print("正在一次性批量拉取自选股历史收盘价数据...")
    df_raw = yf.download(watchlist, period="5y", progress=False)

    if isinstance(df_raw.columns, pd.MultiIndex):
        if "Close" in df_raw.columns.levels[0]:
            all_hist = df_raw["Close"].ffill()
        else:
            all_hist = df_raw.xs("Close", axis=1, level=0).ffill()
    else:
        all_hist = df_raw["Close"].ffill() if "Close" in df_raw.columns else df_raw.ffill()

    # 3. 留存全部股票历史收盘价序列至 FRED_Macro_Data 工作表
    print("正在留存全部股票历史收盘价数据至 FRED_Macro_Data...")
    hist_to_save = all_hist.reset_index()
    date_col = hist_to_save.columns[0]
    hist_to_save[date_col] = pd.to_datetime(hist_to_save[date_col]).dt.strftime("%Y-%m-%d")

    save_headers = ["交易日期" if idx == 0 else str(col) for idx, col in enumerate(hist_to_save.columns)]
    save_values = hist_to_save.tail(120).fillna("-").values.tolist()

    macro_store.clear()
    macro_store.update(
        range_name="A1",
        values=[["全量股票历史收盘价留存底仓（最近 120 个交易日）"]] + [save_headers] + save_values,
        value_input_option="USER_ENTERED"
    )

    stock_rows = []
    diag_rows = []

    for tk in watchlist:
        try:
            sec = fetch_sec_financials_and_audit(tk)
            yr = sec["latest_yr"]
            rev_now = sec["annual"]["Revenue"][yr]["val"]
            ai_notes = extract_10k_notes_with_ai(tk, sec["cik"], sec["subs"], rev_now)

            bm = calculate_business_model_score({**sec, **ai_notes})
            hist_series = _safe_extract_series(all_hist, tk)
            pricing = calculate_institutional_buy_sell_prices(tk, sec, bm["grade"], hist_series, guru_5states)

            stock_rows.append([
                tk,
                pricing["action_signal"],
                pricing["smart_money_tag"],
                f"{bm['total']}分（{bm['grade']}）",
                f"≤ ${pricing['p_base_buy']}",
                f"≤ ${pricing['p_aggressive_buy']}",
                f"≥ ${pricing['p_alert_sell']}",
                ai_notes.get("one_sentence_business", "")
            ])

            diag_rows.append([
                tk,
                bm["grade"],
                f"{sec['roic']:.1%}",
                "8.9%",
                f"${pricing['curr_price']}",
                f"${pricing['p_base_buy']}",
                f"${pricing['p_aggressive_buy']}",
                f"${pricing['p_alert_sell']}",
                sec["audit_status"],
                f"{sec['dilution_rate']:.1%}",
                bm["archetype"],
                bm["alerts"]
            ])
        except Exception as e:
            err_msg = str(e).replace("\n", " ").strip()
            stock_rows.append([tk, f"⚠️待核查（{err_msg[:18]}）", "-", "-", "-", "-", "-", "-"])

    cockpit.update(
        range_name="A6:H15",
        values=[
            ["【第三区：自选股四维共振买卖点与目标价总表】", "", "", "", "", "", "", ""],
            ["股票代码", "买卖信号灯", "聪明钱 13F 五态", "商业模式得分", "保守买点", "右侧买点", "警戒卖点", "商业本质概括"]
        ] + stock_rows,
        value_input_option="USER_ENTERED"
    )

    # 4. 更新板块轮动与大师 13F 矩阵表
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
                if any("★" in s for s in st_list):
                    st_text = "★ 新建仓"
                elif any("▲" in s for s in st_list):
                    st_text = "▲ 增持"
                elif any("▼" in s for s in st_list):
                    st_text = "▼ 减持"
                elif any("⛔" in s for s in st_list):
                    st_text = "⛔ 清仓"
            row.append(st_text)
        matrix_rows.append(row)

    heatmap.update(
        range_name="A1:R15",
        values=[
            ["GICS 十一大板块轮动 × 顶级投资大师 13F 持仓矩阵（V5.3 实盘修正版）", "", "", "", "", "", "", "", "", "顶级投资大师 13F 季度持仓变动矩阵（当期对比上期）", "", "", "", "", "", "", "", ""],
            ["板块代码", "板块名称", "周期属性", "5日相对强弱", "20日相对强弱", "60日相对强弱", "120日相对强弱", "250日相对强弱", "操作信号"] + guru_headers
        ] + matrix_rows,
        value_input_option="USER_ENTERED"
    )

    # 5. 更新单股深度诊断表
    diag.update(
        range_name="A1:L15",
        values=[
            ["单股深度诊断舱（估值区间 × 商业模式百分制 × 资本回报率对比）", "", "", "", "", "", "", "", "", "", "", ""],
            ["股票代码", "综合评级", "投入资本回报率", "加权资本成本", "最新收盘价", "保守买点", "右侧买点", "警戒卖点", "财报审计", "股本稀释率", "商业模式形态", "风险预警"]
        ] + diag_rows,
        value_input_option="USER_ENTERED"
    )

    cockpit.update(range_name="D1", values=[["✅ V5.3 多屏全量数据已推送并留存全部历史行情"]], value_input_option="USER_ENTERED")


if __name__ == "__main__":
    run_pipeline()
