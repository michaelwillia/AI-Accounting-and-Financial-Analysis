"""AI parser module that converts natural language into structured JSON."""

import json
from datetime import date
from typing import Any, Dict, Optional

import requests

from config import get_dashscope_api_key

API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
MODEL_NAME = "qwen-turbo"

SYSTEM_PROMPT_TEMPLATE = """你是记账管家的语义解析器，只负责输出JSON。
当前日期：{today}

你的任务：将用户输入解析成结构化命令。

解析原则：
1. 只输出JSON，不要解释，不要代码块。
2. 支持意图：create_batch | query | explain | update | delete | undo。
3. 删除意图必须输出 delete_scope：single | by_date | by_group | all | unclear。
4. 查询意图必须输出 response_mode：summary | list | detail_all。
5. 用户说“全部/所有/列出来/一条不漏/详细清单”时，response_mode 必须是 detail_all。
6. 用户说“详细说明/花费在哪/那个费用/怎么回事”时，intent 优先是 explain。
7. update 时尽量提取 amount、target_scope、reference_hint。
8. create_batch 时，一个输入可包含多笔，必须拆到 items。
9. time 尽量解析为 YYYY-MM-DD；无法确定可为 null。
10. category 只允许：餐饮 | 交通 | 购物 | 其他。
11. 每条 item 需要 title（6-20字，具体场景）。
12. direction：回款/退款用 income，消费用 expense。
13. accounting_basis 默认 personal_net；若用户明确“按实付/现金流”可输出 cash_flow。
14. AA/代垫场景尽量输出 cash_flow_amount、net_personal_amount、advance_amount。
15. 若出现“每人/每杯A元 + 一共B元”歧义，need_clarification=true 并输出 clarification_question。
16. 仅在信息确实不足时设置 need_clarification=true。

输出格式：
{{
    "intent": "create_batch | query | explain | update | delete | undo",
    "parse_confidence": number | null,
  "date_context": "YYYY-MM-DD" | null,
    "response_mode": "summary | list | detail_all" | null,
    "delete_scope": "single | by_date | by_group | all | unclear" | null,
    "accounting_basis": "personal_net | cash_flow" | null,
  "detail_level": "brief | normal | detail" | null,
  "target_scope": "last_record | last_group | by_date | by_visible" | null,
    "reference_type": "last_record | last_group | by_content" | null,
    "reference_hint": string | null,
  "amount": number | null,
  "raw_text_summary": string | null,
  "summary_for_human": string | null,
  "need_clarification": boolean,
    "clarification_question": string | null,
  "items": [
    {{
      "amount": number | null,
      "category": "餐饮 | 交通 | 购物 | 其他" | null,
      "time": "YYYY-MM-DD" | null,
      "title": string | null,
      "segment_text": string | null,
      "merchant": string | null,
      "pay_method": string | null,
      "note": string | null,
      "direction": "expense | income" | null,
    "cash_flow_amount": number | null,
    "net_personal_amount": number | null,
    "advance_amount": number | null,
      "relation_type": "advance | reimbursement | refund | normal" | null,
      "relation_group_id": string | null,
      "confidence": number | null
    }}
  ]
}}
"""


def _normalize_parser_output(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize parser output to stable enums/defaults for service layer."""
    intent_allowed = {"create_batch", "query", "explain", "update", "delete", "undo", "create"}
    response_mode_allowed = {"summary", "list", "detail_all"}
    delete_scope_allowed = {"single", "by_date", "by_group", "all", "unclear"}
    accounting_basis_allowed = {"personal_net", "cash_flow"}

    intent = parsed.get("intent")
    if intent not in intent_allowed:
        parsed["intent"] = None

    response_mode = parsed.get("response_mode")
    if response_mode not in response_mode_allowed:
        parsed["response_mode"] = None

    delete_scope = parsed.get("delete_scope")
    if delete_scope not in delete_scope_allowed:
        parsed["delete_scope"] = None

    accounting_basis = parsed.get("accounting_basis")
    if accounting_basis not in accounting_basis_allowed:
        parsed["accounting_basis"] = "personal_net"

    if not isinstance(parsed.get("items"), list):
        parsed["items"] = []

    need_clarification = parsed.get("need_clarification")
    parsed["need_clarification"] = bool(need_clarification)

    return parsed


def _extract_json_text(content: str) -> Optional[str]:
    """Extract JSON text even if model accidentally wraps it with code fences."""
    text = content.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:].strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    return text[start : end + 1]


def parse_text(user_input: str) -> Optional[Dict[str, Any]]:
    """Call DashScope API and parse user input into a structured dict."""
    api_key = get_dashscope_api_key()
    if not api_key:
        return None

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(today=date.today().isoformat())

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_input},
        ],
        "stream": False,
        "temperature": 0.1,
    }

    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]

        json_text = _extract_json_text(content)
        if not json_text:
            return None

        parsed = json.loads(json_text)
        if not isinstance(parsed, dict):
            return None
        return _normalize_parser_output(parsed)
    except (requests.RequestException, KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError):
        return None
