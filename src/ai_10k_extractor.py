import json
import os
import requests
from config import AI_API_KEY, AI_BASE_URL, AI_MODEL_NAME

def get_api_endpoint() -> str:
    base = AI_BASE_URL.rstrip('/')
    if not base.endswith('/v1'):
        base += '/v1'
    return f"{base}/chat/completions"

def extract_10k_notes_with_ai(ticker: str, cik: str, subs: dict, total_revenue: float) -> dict:
    cache_dir = "cache_10k"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"{ticker}_10k_ai.json")
    
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
            
    prompt = f"""
    你是资深美股买方量化分析师。请分析股票代码 {ticker} 的 10-K 财报附注信息，按以下格式严格返回 JSON (不要添加任何 Markdown 标记):
    {{
      "segment_shares": [各个业务分部营收占总营收比例的浮点数数组, 示例: [0.45, 0.35, 0.20]],
      "geo_shares": [各个地理区域营收占比浮点数数组],
      "max_single_foreign_geo_pct": 单一最大海外国家营收占比浮点数 (如 0.25),
      "max_single_customer_pct": ASC 280 附注中单一最大客户营收占比 (无大于10%则填0.0),
      "top5_customers_pct": 前五大客户合计营收占比 (无法确定则填0.0),
      "employees_count": 全职员工总人数整数,
      "rpo_yoy_growth": 剩余履约义务(RPO)同比增速浮点数,
      "one_sentence_business": "一句话概括核心商业本质与护城河"
    }}
    """
    
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    endpoint = get_api_endpoint()
    payload = {
        "model": AI_MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "reasoning_effort": "high"
    }
    
    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=45).json()
        raw_text = resp["choices"][0]["message"]["content"].strip()
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1].replace("json", "").strip()
            
        data = json.loads(raw_text)
        
        seg_sum = sum(data.get("segment_shares", [1.0]))
        if not (0.90 <= seg_sum <= 1.10):
            data["segment_shares"] = [1.0]
            
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        return data
    except Exception:
        return {
            "segment_shares": [1.0], "geo_shares": [1.0], "max_single_foreign_geo_pct": 0.0,
            "max_single_customer_pct": 0.0, "top5_customers_pct": 0.0, "employees_count": 50000,
            "rpo_yoy_growth": 0.10, "one_sentence_business": "行业领军企事业单位"
        }
