import os

# 美国证券交易委员会 SEC EDGAR 请求头（必须包含合规标识与联系邮箱）
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "HuLiweiQuantResearch admin@example.com")

# 环境变量与各类接口密钥
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
AI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# 大模型中转接口配置（默认使用 https://api.starwish.fit 与 gpt-astra-6.2）
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://api.starwish.fit")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "gpt-astra-6.2")
SHEET_ID = os.getenv("SPREADSHEET_ID", "")

# 美联储 FRED 宏观水文指标代码
FRED_SERIES = {
    "WALCL": "WALCL",             # 美联储总资产（百万美元）
    "RRP": "RRPONTSYD",           # 美联储隔夜逆回购（十亿美元）
    "TGA": "WTREGEN",             # 美国财政部一般账户余额（十亿美元）
    "HY_OAS": "BAMLH0A0HYM2",     # 美国高收益债信用利差（%）
    "TIPS_10Y": "DFII10",         # 10年期 TIPS 实际利率（%）
    "YIELD_CURVE": "T10Y2Y",      # 10年期与2年期美债收益率利差（%）
    "T10YIE": "T10YIE"            # 10年期盈亏平衡通胀预期（%）
}

# 八大跨资产风向标行情代码
CROSS_ASSET_TICKERS = {
    "DXY": "DX-Y.NYB",            # 美元指数
    "USDJPY": "JPY=X",            # 美元兑日元汇率
    "COPPER": "HG=F",             # COMEX 铜期货
    "GOLD": "GC=F",               # COMEX 黄金期货
    "OIL": "CL=F",                # WTI 原油期货
    "VIX": "^VIX",                # 美股恐慌指数
    "MOVE": "^MOVE",              # 美债波动率指数
    "BTC": "BTC-USD"              # 比特币价格
}

# 全球行业分类标准 GICS 十一大板块 ETF
GICS_SECTORS = {
    "XLK": {"name": "信息技术", "cycle": "进攻/成长"},
    "XLC": {"name": "通信服务", "cycle": "进攻/成长"},
    "XLY": {"name": "非必需消费", "cycle": "进攻/周期"},
    "XLF": {"name": "金融", "cycle": "进攻/周期"},
    "XLI": {"name": "工业", "cycle": "进攻/周期"},
    "XLB": {"name": "原材料", "cycle": "周期/通胀"},
    "XLE": {"name": "能源", "cycle": "防御/抗通胀"},
    "XLP": {"name": "必需消费", "cycle": "核心防御"},
    "XLV": {"name": "医疗保健", "cycle": "核心防御"},
    "XLU": {"name": "公用事业", "cycle": "深度防御"},
    "XLRE": {"name": "房地产", "cycle": "利率敏感"}
}

# 八位顶级投资大师 CIK 档案库
GURU_WATCHLIST = {
    "0001536411": {"name": "德鲁肯米勒", "camp": "macro"},
    "0001656456": {"name": "大卫·泰珀", "camp": "macro"},
    "0001336528": {"name": "比尔·阿克曼", "camp": "macro"},
    "0001067983": {"name": "巴菲特", "camp": "value"},
    "0001759760": {"name": "段永平", "camp": "value"},
    "0001709323": {"name": "李录", "camp": "value"},
    "0001569205": {"name": "特里·史密斯", "camp": "value"},
    "0001061768": {"name": "塞思·卡拉曼", "camp": "value"}
}

# SEC 美国通用会计准则 XBRL 科目映射表
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
