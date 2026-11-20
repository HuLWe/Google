import os

# SEC EDGAR 请求头
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "HuLiweiQuantResearch admin@example.com")

# 环境变量与 API 密钥
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
AI_API_KEY = os.getenv("OPENAI_API_KEY", "")
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://api.openai.com/v1")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "gpt-astra-6.2")
SHEET_ID = os.getenv("SPREADSHEET_ID", "")

# 美联储 FRED 宏观水文代码
FRED_SERIES = {
    "WALCL": "WALCL",             # 美联储总资产 (百万美元)
    "RRP": "RRPONTSYD",           # 隔夜逆回购 (十亿美元)
    "TGA": "WTREGEN",             # 财政部一般账户 (十亿美元)
    "HY_OAS": "BAMLH0A0HYM2",     # 高收益债信用利差 (%)
    "TIPS_10Y": "DFII10",         # 10年期TIPS实际利率 (%)
    "YIELD_CURVE": "T10Y2Y",      # 10Y-2Y 收益率利差 (%)
    "T10YIE": "T10YIE"            # 10年通胀预期 Breakeven (%)
}

# 8 大跨资产风向标行情代码
CROSS_ASSET_TICKERS = {
    "DXY": "DX-Y.NYB",            # 美元指数
    "USDJPY": "JPY=X",            # 日元汇率
    "COPPER": "HG=F",             # COMEX铜期货
    "GOLD": "GC=F",               # COMEX黄金期货
    "OIL": "CL=F",                # WTI原油期货
    "VIX": "^VIX",                # 美股恐慌指数
    "MOVE": "^MOVE",              # 美债波动率指数
    "BTC": "BTC-USD"              # 比特币
}

# GICS 11 大板块 ETF
GICS_SECTORS = {
    "XLK": {"name": "信息技术 (Info Tech)", "cycle": "进攻/成长"},
    "XLC": {"name": "通信服务 (Comm Services)", "cycle": "进攻/成长"},
    "XLY": {"name": "非必需消费 (Cons. Disc.)", "cycle": "进攻/周期"},
    "XLF": {"name": "金融 (Financials)", "cycle": "进攻/周期"},
    "XLI": {"name": "工业 (Industrials)", "cycle": "进攻/周期"},
    "XLB": {"name": "原材料 (Materials)", "cycle": "周期/通胀"},
    "XLE": {"name": "能源 (Energy)", "cycle": "防御/抗通胀"},
    "XLP": {"name": "必需消费 (Cons. Staples)", "cycle": "核心防御"},
    "XLV": {"name": "医疗保健 (Health Care)", "cycle": "核心防御"},
    "XLU": {"name": "公用事业 (Utilities)", "cycle": "深度防御"},
    "XLRE": {"name": "房地产 (Real Estate)", "cycle": "利率敏感"}
}

# 8 位顶级投资大师 CIK
GURU_WATCHLIST = {
    "0001536411": {"name": "德鲁肯米勒", "camp": "macro"},
    "0001656456": {"name": "大卫·泰珀", "camp": "macro"},
    "0001336528": {"name": "比尔·阿克曼", "camp": "macro"},
    "0001067983": {"name": "巴菲特", "camp": "value"},
    "0001759760": {"name": "段永平", "camp": "value"},
    "0001709323": {"name": "李录", "camp": "value"},
    "0001569205": {"name": "Terry Smith", "camp": "value"},
    "0001061768": {"name": "塞思·卡拉曼", "camp": "value"}
}

# SEC US-GAAP XBRL 标签映射
XBRL_TAG_MAP = {
    "Revenue": ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet", "RevenueFromContractWithCustomerIncludingAssessedTax"],
    "GrossProfit": ["GrossProfit"],
    "COGS": ["CostOfGoodsAndServicesSold", "CostOfRevenue", "CostOfGoodsSold"],
    "EBIT": ["OperatingIncomeLoss"],
    "NetIncome": ["NetIncomeLoss", "ProfitLoss"],
    "OCF": ["NetCashProvidedByUsedInOperatingActivities"],
    "CapEx": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "SBC": ["ShareBasedCompensation", "AllocatedShareBasedCompensationExpense"],
    "SM_Expense": ["SellingAndMarketingExpense", "MarketingAndAdvertisingExpense"],
    "Assets": ["Assets"],
    "Liabilities": ["Liabilities"],
    "Equity": ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
    "Goodwill": ["Goodwill"],
    "Cash": ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsAndShortTermInvestments"],
    "Debt_Short": ["ShortTermBorrowings", "LongTermDebtCurrent"],
    "Debt_Long": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "Inventory": ["InventoryNet"],
    "AR": ["AccountsReceivableNetCurrent"],
    "AP": ["AccountsPayableCurrent"],
    "PPE": ["PropertyPlantAndEquipmentNet"],
    "DeferredRev_Curr": ["ContractWithCustomerLiabilityCurrent", "DeferredRevenueCurrent"],
    "DeferredRev_NonCurr": ["ContractWithCustomerLiabilityNoncurrent", "DeferredRevenueNoncurrent"],
    "Shares": ["WeightedAverageNumberOfDilutedSharesOutstanding", "CommonStockSharesOutstanding"]
}
