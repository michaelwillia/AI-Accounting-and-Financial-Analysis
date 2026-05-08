"""Business service layer for conversational bookkeeping."""

import json
import re
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ai_parser import parse_text
from db import (
    delete_record,
    delete_records_by_date,
    get_category_summary_by_date,
    get_last_operation_log,
    get_last_record,
    get_records_by_date,
    get_records_by_date_range,
    get_records_by_ids,
    get_summary_by_date,
    get_summary_by_date_range,
    insert_operation_log,
    insert_records_batch,
    insert_source_message,
    update_record,
)


SESSION_STATE: Dict[str, Any] = {
    "pending_confirm": None,
    "last_operation": None,
    "last_query_records": [],
}


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Parse float safely from mixed parser output."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value)
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    if not m:
        return default
    return float(m.group(1))


def _safe_str(value: Any, default: str = "") -> str:
    """Normalize unknown values to compact string."""
    if value is None:
        return default
    return str(value).strip()


def _today_str() -> str:
    """Return today date string in yyyy-mm-dd format."""
    return date.today().isoformat()


def _normalize_date_text(value: Optional[str]) -> str:
    """Normalize parser date values into yyyy-mm-dd where possible."""
    text = _safe_str(value)
    if not text:
        return _today_str()
    if text in ["today", "今天"]:
        return _today_str()
    if text in ["yesterday", "昨天"]:
        return (date.today() - timedelta(days=1)).isoformat()
    if text in ["tomorrow", "明天"]:
        return (date.today() + timedelta(days=1)).isoformat()

    # Accept parser output like 2025/01/02 and normalize separators.
    text = text.replace("/", "-")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return _today_str()


def _extract_time_range(text: str, parser_date: Optional[str]) -> Tuple[str, str]:
    """Resolve time span for query/delete operations."""
    src = _safe_str(text)
    today = date.today()

    if any(k in src for k in ["这周", "本周", "这一周"]):
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
        return start.isoformat(), end.isoformat()

    if any(k in src for k in ["这个月", "本月"]):
        start = today.replace(day=1)
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1)
        else:
            next_month = start.replace(month=start.month + 1)
        end = next_month - timedelta(days=1)
        return start.isoformat(), end.isoformat()

    one_day = _normalize_date_text(parser_date)
    return one_day, one_day


def _extract_amount_from_text(text: str) -> Optional[float]:
    """Extract numeric amount from free text."""
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    if not m:
        return None
    return float(m.group(1))


def _intent_from_fallback(text: str) -> str:
    """Fallback intent recognition when model output is noisy."""
    src = _safe_str(text)
    if any(k in src for k in ["撤销", "撤回", "undo"]):
        return "undo"
    if any(k in src for k in ["删除", "删掉", "清空", "移除"]):
        return "delete"
    if any(k in src for k in ["改成", "更正", "修改", "金额不对", "不是"]):
        return "update"
    if any(k in src for k in ["为什么", "为啥", "解释", "来源", "根据"]):
        return "explain"
    if any(k in src for k in ["统计", "汇总", "多少", "花了", "查询", "看看", "明细", "账单"]):
        return "query"
    return "create"


def _resolve_response_mode(text: str, parser_mode: Optional[str]) -> str:
    """Resolve query style: summary/list/detail_all."""
    mode = _safe_str(parser_mode)
    src = _safe_str(text)
    if mode in ["summary", "list", "detail_all"]:
        return mode
    if any(k in src for k in ["全部", "完整", "所有", "每一笔"]):
        return "detail_all"
    if any(k in src for k in ["明细", "列出", "有哪些"]):
        return "list"
    return "summary"


def _resolve_delete_scope(text: str, parser_scope: Optional[str]) -> str:
    """Delete scope whitelist; default to single to avoid over-delete."""
    scope = _safe_str(parser_scope)
    src = _safe_str(text)
    if scope in ["single", "by_date", "all"]:
        return scope
    if any(k in src for k in ["今天所有", "今天全部", "清空今天", "删掉今天"]):
        return "by_date"
    if any(k in src for k in ["全部", "所有记录", "所有账单"]):
        return "all"
    return "single"


def _contains_non_event_or_future(text: str) -> bool:
    """Block statements that are plans, hypotheticals, or explicitly not happened."""
    src = _safe_str(text)
    non_event_patterns = [
        r"(还没|没有|没买|没花|并未|不是我花的)",
        r"(打算|准备|计划|想要|可能会|如果)",
        r"(明天|后天).*(再|去|买|花)",
        r"(本来|原本).*(但|结果).*(没|没有)",
    ]
    return any(re.search(p, src) for p in non_event_patterns)


def _is_credit_repayment(text: str, category: str) -> bool:
    """Credit card repayment is liability settlement, not new expense."""
    src = _safe_str(text)
    if category == "credit_repayment":
        return True
    return bool(re.search(r"(还信用卡|信用卡还款)", src))


def _seems_aa_scene(text: str) -> bool:
    """Detect AA-like wording from user text."""
    src = _safe_str(text)
    return any(k in src for k in ["AA", "平摊", "均摊", "每人", "转回", "大家", "朋友"])


def _extract_group_count(text: str) -> Optional[int]:
    """Extract participant count from Arabic or Chinese numerals."""
    src = _safe_str(text)
    m = re.search(r"([0-9一二三四五六七八九十两]+)\s*个人", src)
    if not m:
        return None
    token = m.group(1)
    if token.isdigit():
        return int(token)
    mapping = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}
    if token == "十":
        return 10
    total = 0
    for c in token:
        total += mapping.get(c, 0)
    return total or None


def _extract_aa_adjustment(text: str, amount: float) -> Dict[str, Any]:
    """Derive AA adjustment from text when parser does not provide full accounting split."""
    src = _safe_str(text)
    if not _seems_aa_scene(src):
        return {"is_aa": False}

    # Priority 1: explicit per-person amount.
    per_person = None
    m = re.search(r"每人\s*(\d+(?:\.\d+)?)", src)
    if m:
        per_person = float(m.group(1))

    # Priority 2: total amount + participant count.
    if per_person is None:
        group_count = _extract_group_count(src)
        if group_count and group_count > 0:
            per_person = round(float(amount) / float(group_count), 2)

    if per_person is None:
        return {
            "is_aa": True,
            "need_clarification": True,
            "message": "识别到AA场景，但缺少每人分摊金额或人数，先补充一下我再入账。",
        }

    reimbursed = any(k in src for k in ["转回", "收回", "已收", "都给我", "转给我"])
    advance_amount = round(float(amount) - float(per_person), 2)
    return {
        "is_aa": True,
        "need_clarification": False,
        "net_personal_amount": per_person,
        "cash_flow_amount": float(amount),
        "advance_amount": advance_amount if reimbursed or advance_amount > 0 else 0.0,
        "relation_type": "aa_shared",
    }


def _category_guess(text: str) -> str:
    """Cheap fallback category classifier for plain create messages."""
    src = _safe_str(text)
    rules = [
        ("food", ["吃", "饭", "奶茶", "咖啡", "早餐", "午餐", "晚餐", "外卖", "火锅"]),
        ("transport", ["打车", "地铁", "公交", "滴滴", "油费", "停车"]),
        ("shopping", ["买", "购物", "衣服", "鞋", "日用品", "超市"]),
        ("entertainment", ["电影", "唱歌", "游戏", "演出", "旅游"]),
        ("housing", ["房租", "水电", "物业", "宽带"]),
        ("medical", ["医院", "挂号", "药", "体检", "牙"]),
        ("transfer", ["转账", "转给", "红包", "借给", "借出"]),
    ]
    for name, words in rules:
        if any(w in src for w in words):
            return name
    return "other"


def _direction_from_category(category: str, amount: float) -> str:
    """Determine income/expense direction."""
    if amount < 0:
        return "income"
    if category in ["income", "salary", "refund"]:
        return "income"
    return "expense"


def _normalize_create_items(parsed: Dict[str, Any], user_text: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Normalize parser create items and return optional clarification message."""
    items_raw = parsed.get("items") if isinstance(parsed.get("items"), list) else []
    normalized_items: List[Dict[str, Any]] = []

    for idx, raw in enumerate(items_raw):
        if not isinstance(raw, dict):
            continue

        amount = _safe_float(raw.get("amount"))
        if amount is None:
            continue

        segment_text = _safe_str(raw.get("segment_text")) or user_text
        category = _safe_str(raw.get("category"), "other") or "other"

        if _is_credit_repayment(segment_text, category):
            # Do not convert repayment to expense automatically.
            return [], "检测到‘信用卡还款’，这不是新的消费。要不要我按‘负债结清’做备注记录？"

        base = {
            "source_id": parsed.get("source_id"),
            "group_key": _safe_str(parsed.get("group_key")) or None,
            "amount": round(abs(amount), 2),
            "cash_flow_amount": round(abs(amount), 2),
            "net_personal_amount": round(abs(amount), 2),
            "advance_amount": 0.0,
            "accounting_basis": _safe_str(parsed.get("accounting_basis"), "personal_net") or "personal_net",
            "category": category,
            "time": _normalize_date_text(raw.get("time") or parsed.get("date")),
            "title": _safe_str(raw.get("title")) or _safe_str(raw.get("merchant")) or f"记录{idx + 1}",
            "segment_text": segment_text,
            "pay_method": _safe_str(raw.get("pay_method")),
            "merchant": _safe_str(raw.get("merchant")),
            "note": _safe_str(raw.get("note")),
            "direction": _safe_str(raw.get("direction")) or _direction_from_category(category, amount),
            "relation_type": _safe_str(raw.get("relation_type")),
            "relation_group_id": _safe_str(raw.get("relation_group_id")),
            "confidence": _safe_float(raw.get("confidence"), 0.8),
        }

        # If parser provides explicit accounting split, use parser first.
        parser_net = _safe_float(raw.get("net_personal_amount"))
        parser_cash = _safe_float(raw.get("cash_flow_amount"))
        parser_advance = _safe_float(raw.get("advance_amount"))
        if parser_net is not None:
            base["net_personal_amount"] = round(abs(parser_net), 2)
        if parser_cash is not None:
            base["cash_flow_amount"] = round(abs(parser_cash), 2)
        if parser_advance is not None:
            base["advance_amount"] = round(abs(parser_advance), 2)

        # AA fallback calculation from natural language if parser omitted split fields.
        if _seems_aa_scene(segment_text) and parser_net is None and parser_cash is None:
            aa = _extract_aa_adjustment(segment_text, base["amount"])
            if aa.get("need_clarification"):
                return [], aa.get("message")
            if aa.get("is_aa"):
                base["net_personal_amount"] = aa["net_personal_amount"]
                base["cash_flow_amount"] = aa["cash_flow_amount"]
                base["advance_amount"] = aa["advance_amount"]
                base["relation_type"] = aa.get("relation_type", base["relation_type"])
                base["accounting_basis"] = "personal_net"

        normalized_items.append(base)

    if normalized_items:
        return normalized_items, None

    # Fallback for plain statement with amount when parser returned no valid item.
    fallback_amount = _extract_amount_from_text(user_text)
    if fallback_amount is None:
        return [], "没有识别到有效金额，请补充具体金额。"

    if _contains_non_event_or_future(user_text):
        return [], "这句话更像计划或未发生事件，我先不入账。发生后再告诉我即可。"

    return [
        {
            "source_id": parsed.get("source_id"),
            "group_key": _safe_str(parsed.get("group_key")) or None,
            "amount": round(abs(fallback_amount), 2),
            "cash_flow_amount": round(abs(fallback_amount), 2),
            "net_personal_amount": round(abs(fallback_amount), 2),
            "advance_amount": 0.0,
            "accounting_basis": "personal_net",
            "category": _category_guess(user_text),
            "time": _normalize_date_text(parsed.get("date")),
            "title": _safe_str(user_text)[:40],
            "segment_text": _safe_str(user_text),
            "pay_method": "",
            "merchant": "",
            "note": "",
            "direction": "expense",
            "relation_type": "",
            "relation_group_id": "",
            "confidence": 0.5,
        }
    ], None


def _format_record_line(rec: Dict[str, Any]) -> str:
    """Render one record line for list output."""
    amount = float(rec.get("amount") or 0.0)
    direction = _safe_str(rec.get("direction"), "expense")
    prefix = "+" if direction == "income" else "-"
    title = _safe_str(rec.get("title")) or _safe_str(rec.get("segment_text"))[:18] or "未命名"
    category = _safe_str(rec.get("category"), "other")
    return f"#{rec.get('id')} {title} [{category}] {prefix}{amount:.2f}"


def _detect_duplicate_candidates(items: List[Dict[str, Any]]) -> List[str]:
    """Find likely duplicates by comparing same-day same-amount close semantic title."""
    warnings: List[str] = []
    for item in items:
        item_date = _safe_str(item.get("time"))
        amount = float(item.get("amount") or 0.0)
        title = _safe_str(item.get("title"))
        todays = get_records_by_date(item_date, limit=80)
        for old in todays:
            if abs(float(old.get("amount") or 0.0) - amount) > 0.001:
                continue
            old_title = _safe_str(old.get("title"))
            if not title or not old_title:
                continue
            if title in old_title or old_title in title:
                warnings.append(f"可能重复：#{old.get('id')} {old_title} {amount:.2f}")
                break
    return warnings


def _commit_create_items(user_text: str, items: List[Dict[str, Any]]) -> str:
    """Persist create items and return user-facing summary."""
    source_id = insert_source_message(full_text=user_text, summary_text=f"create:{len(items)}")
    for item in items:
        item["source_id"] = source_id

    inserted_ids = insert_records_batch(items)
    insert_operation_log(
        operation_type="create",
        record_ids=inserted_ids,
        before_payload={},
        after_payload={"items": items},
    )
    SESSION_STATE["last_operation"] = {
        "type": "create",
        "record_ids": inserted_ids,
    }

    total = sum(float(i.get("amount") or 0.0) for i in items)
    lines = [f"已记账 {len(items)} 笔，总额 {total:.2f} 元。"]
    for rid, item in zip(inserted_ids, items):
        lines.append(
            f"#{rid} {item.get('title')} [{item.get('category')}] "
            f"{item.get('direction')} {float(item.get('amount') or 0.0):.2f}"
        )
        if float(item.get("advance_amount") or 0.0) > 0:
            lines.append(
                f"  口径说明：现金流 {float(item.get('cash_flow_amount') or 0.0):.2f}，"
                f"个人净支出 {float(item.get('net_personal_amount') or 0.0):.2f}，"
                f"代垫 {float(item.get('advance_amount') or 0.0):.2f}"
            )
    return "\n".join(lines)


def _handle_pending_confirmation(user_text: str) -> Optional[str]:
    """Handle yes/no (and amount correction) for pending operations."""
    pending = SESSION_STATE.get("pending_confirm")
    if not pending:
        return None

    src = _safe_str(user_text)

    # Allow quick correction before confirmation: "金额不对，是95".
    new_amount = _extract_amount_from_text(src)
    if (
        pending.get("type") == "create"
        and new_amount is not None
        and any(k in src for k in ["不对", "不是", "改成", "改为", "改下"])
    ):
        items = pending.get("items") or []
        if len(items) == 1:
            items[0]["amount"] = round(new_amount, 2)
            items[0]["cash_flow_amount"] = round(new_amount, 2)
            items[0]["net_personal_amount"] = round(new_amount, 2)
            return f"已将金额改为 {new_amount:.2f} 元。回复“确认”即可入账。"
        return "检测到你想改金额，但当前有多笔待确认。请说“第几笔改成多少”。"

    yes = any(k in src for k in ["确认", "是", "对", "好", "行", "可以", "yes", "y"])
    no = any(k in src for k in ["取消", "不用", "否", "不", "no", "n"])

    if not yes and not no:
        return "请回复“确认”或“取消”。"

    if no:
        SESSION_STATE["pending_confirm"] = None
        return "已取消。"

    if pending["type"] == "create":
        items = pending.get("items") or []
        SESSION_STATE["pending_confirm"] = None
        return _commit_create_items(pending.get("raw_text", ""), items)

    if pending["type"] == "delete":
        scope = pending.get("scope")
        backup = pending.get("backup", [])
        SESSION_STATE["pending_confirm"] = None

        if scope == "single":
            record_id = int(pending.get("record_id"))
            ok = delete_record(record_id)
            if not ok:
                return "删除失败：未找到该记录。"
            insert_operation_log(
                operation_type="delete_single",
                record_ids=[record_id],
                before_payload={"records": backup},
                after_payload={},
            )
            SESSION_STATE["last_operation"] = {
                "type": "delete",
                "record_ids": [record_id],
                "backup": backup,
            }
            return f"已删除记录 #{record_id}。"

        if scope == "by_date":
            date_text = _safe_str(pending.get("date"))
            deleted_count = delete_records_by_date(date_text)
            ids = [int(r.get("id")) for r in backup if r.get("id") is not None]
            insert_operation_log(
                operation_type="delete_by_date",
                record_ids=ids,
                before_payload={"records": backup, "date": date_text},
                after_payload={},
            )
            SESSION_STATE["last_operation"] = {
                "type": "delete_by_date",
                "date": date_text,
                "record_ids": ids,
                "backup": backup,
            }
            return f"已删除 {date_text} 的 {deleted_count} 笔记录。"

        if scope == "all":
            # MVP safety: we intentionally avoid implementing hard global purge.
            return "出于安全考虑，不支持直接删除全部记录。请按日期删除。"

    return "已处理。"


def _handle_create(parsed: Dict[str, Any], user_text: str) -> str:
    """Handle create intent with confirmation flow and duplicate warning."""
    if _contains_non_event_or_future(user_text):
        return "这句话看起来是计划或未发生事件，我先不记账。发生后再告诉我。"

    items, clarification = _normalize_create_items(parsed, user_text)
    if clarification:
        return clarification

    duplicates = _detect_duplicate_candidates(items)

    total = sum(float(i.get("amount") or 0.0) for i in items)
    preview = [f"待记账 {len(items)} 笔，总额 {total:.2f} 元。"]
    for item in items:
        preview.append(
            f"- {item.get('title')} [{item.get('category')}] {item.get('direction')} {float(item.get('amount') or 0.0):.2f}"
        )
    if duplicates:
        preview.extend(duplicates)

    preview.append("确认入账吗？回复“确认”或“取消”。")
    SESSION_STATE["pending_confirm"] = {
        "type": "create",
        "items": items,
        "raw_text": user_text,
    }
    return "\n".join(preview)


def _handle_query(parsed: Dict[str, Any], user_text: str) -> str:
    """Handle query intent with summary/list/detail_all modes."""
    start_date, end_date = _extract_time_range(user_text, parsed.get("date"))
    mode = _resolve_response_mode(user_text, parsed.get("response_mode"))
    expense_only = bool(parsed.get("expense_only")) or any(k in user_text for k in ["花了什么", "只看支出", "支出明细"])

    records = get_records_by_date_range(start_date, end_date, limit=2000)
    if expense_only:
        records = [r for r in records if _safe_str(r.get("direction"), "expense") == "expense"]
    SESSION_STATE["last_query_records"] = records

    if not records:
        if start_date == end_date:
            return f"{start_date} 暂无记录。"
        return f"{start_date} 到 {end_date} 暂无记录。"

    summary = get_summary_by_date(start_date) if start_date == end_date else get_summary_by_date_range(start_date, end_date)

    header = (
        f"{start_date} 到 {end_date} 统计：支出 {summary['expense']:.2f}，"
        f"收入 {summary['income']:.2f}，净支出 {summary['net_expense']:.2f}，记录 {int(summary['record_count'])} 笔。"
        if start_date != end_date
        else f"{start_date} 统计：支出 {summary['expense']:.2f}，收入 {summary['income']:.2f}，净支出 {summary['net_expense']:.2f}，记录 {int(summary['record_count'])} 笔。"
    )

    if mode == "summary":
        if start_date == end_date:
            cats = get_category_summary_by_date(start_date)
            if cats:
                top = cats[:3]
                top_text = "；".join([f"{c['category']} {float(c['total']):.2f}" for c in top])
                return header + f"\n主要支出：{top_text}。"
        return header

    if mode == "list":
        lines = [header, "最近明细："]
        for rec in records[:10]:
            lines.append(_format_record_line(rec))
        return "\n".join(lines)

    lines = [header, "完整明细："]
    for rec in records:
        lines.append(_format_record_line(rec))
    return "\n".join(lines)


def _pick_update_target(parsed: Dict[str, Any]) -> Optional[int]:
    """Resolve update target record id from parser or context."""
    target_id = parsed.get("target_record_id")
    if isinstance(target_id, int):
        return target_id
    if isinstance(target_id, str) and target_id.isdigit():
        return int(target_id)

    refs = parsed.get("record_refs")
    if isinstance(refs, list):
        for ref in refs:
            if isinstance(ref, dict):
                rid = ref.get("record_id")
                if isinstance(rid, int):
                    return rid
                if isinstance(rid, str) and rid.isdigit():
                    return int(rid)

    last = get_last_record()
    if last:
        return int(last["id"])
    return None


def _handle_update(parsed: Dict[str, Any], user_text: str) -> str:
    """Handle amount correction for a target record."""
    target_id = _pick_update_target(parsed)
    if target_id is None:
        return "没有找到要修改的记录。"

    amount = _safe_float(parsed.get("amount"))
    if amount is None:
        amount = _extract_amount_from_text(user_text)
    if amount is None:
        return "请提供要修改成的金额。"

    old = get_records_by_ids([target_id])
    if not old:
        return f"记录 #{target_id} 不存在。"

    ok = update_record(target_id, round(abs(amount), 2))
    if not ok:
        return f"修改失败：记录 #{target_id} 不存在。"

    insert_operation_log(
        operation_type="update_amount",
        record_ids=[target_id],
        before_payload={"records": old},
        after_payload={"amount": round(abs(amount), 2)},
    )
    SESSION_STATE["last_operation"] = {
        "type": "update",
        "record_ids": [target_id],
        "before": old,
    }
    return f"已将记录 #{target_id} 的金额改为 {round(abs(amount), 2):.2f} 元。"


def _find_explain_targets(parsed: Dict[str, Any], user_text: str) -> List[Dict[str, Any]]:
    """Find records to explain by explicit id, refs, or keyword fallback."""
    targets: List[int] = []

    tid = parsed.get("target_record_id")
    if isinstance(tid, int):
        targets.append(tid)
    elif isinstance(tid, str) and tid.isdigit():
        targets.append(int(tid))

    refs = parsed.get("record_refs")
    if isinstance(refs, list):
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            rid = ref.get("record_id")
            if isinstance(rid, int):
                targets.append(rid)
            elif isinstance(rid, str) and rid.isdigit():
                targets.append(int(rid))

    if targets:
        return get_records_by_ids(list(dict.fromkeys(targets)))

    date_text = _normalize_date_text(parsed.get("date"))
    records = get_records_by_date(date_text, limit=80)
    keyword = _safe_str(parsed.get("target_text") or user_text)
    if not keyword:
        return records[:1]
    matched = [r for r in records if keyword in _safe_str(r.get("title")) or keyword in _safe_str(r.get("segment_text"))]
    return matched or records[:1]


def _handle_explain(parsed: Dict[str, Any], user_text: str) -> str:
    """Explain why one or more records were interpreted as they are."""
    records = _find_explain_targets(parsed, user_text)
    if not records:
        return "没有找到可解释的记录。"

    src = _safe_str(user_text)
    ask_time = any(k in src for k in ["几点", "什么时候", "何时", "哪天", "时间"])

    lines: List[str] = []
    for rec in records:
        rid = rec.get("id")
        title = _safe_str(rec.get("title")) or "未命名"
        category = _safe_str(rec.get("category"), "other")
        amount = float(rec.get("amount") or 0.0)
        segment = _safe_str(rec.get("segment_text"))
        dt = _safe_str(rec.get("time"))

        lines.append(f"#{rid} {title}：")
        lines.append(f"- 分类：{category}")
        lines.append(f"- 金额：{amount:.2f}")
        lines.append(f"- 依据原文：{segment or '无'}")
        if ask_time:
            lines.append(f"- 时间：{dt or '未提供'}")
        if float(rec.get("advance_amount") or 0.0) > 0:
            lines.append(
                f"- 口径：现金流 {float(rec.get('cash_flow_amount') or 0.0):.2f}，"
                f"个人净支出 {float(rec.get('net_personal_amount') or 0.0):.2f}"
            )
    return "\n".join(lines)


def _handle_delete(parsed: Dict[str, Any], user_text: str) -> str:
    """Handle delete intent with strict confirmation and scope control."""
    scope = _resolve_delete_scope(user_text, parsed.get("delete_scope"))

    if scope == "all":
        return "出于安全考虑，不支持“删除全部”。请指定日期，例如“删除今天所有记录”。"

    if scope == "by_date":
        date_text = _normalize_date_text(parsed.get("date"))
        records = get_records_by_date(date_text, limit=2000)
        if not records:
            return f"{date_text} 没有可删除的记录。"

        SESSION_STATE["pending_confirm"] = {
            "type": "delete",
            "scope": "by_date",
            "date": date_text,
            "backup": records,
        }
        return f"将删除 {date_text} 的 {len(records)} 笔记录。回复“确认”或“取消”。"

    # single delete target resolution.
    target_id = parsed.get("target_record_id")
    rid: Optional[int] = None
    if isinstance(target_id, int):
        rid = target_id
    elif isinstance(target_id, str) and target_id.isdigit():
        rid = int(target_id)
    else:
        last = get_last_record()
        rid = int(last["id"]) if last else None

    if rid is None:
        return "没有找到要删除的记录。"

    record = get_records_by_ids([rid])
    if not record:
        return f"记录 #{rid} 不存在。"

    SESSION_STATE["pending_confirm"] = {
        "type": "delete",
        "scope": "single",
        "record_id": rid,
        "backup": record,
    }
    return f"将删除记录 #{rid}（{record[0].get('title')} {float(record[0].get('amount') or 0.0):.2f}）。回复“确认”或“取消”。"


def _restore_records(records: List[Dict[str, Any]]) -> List[int]:
    """Restore historical records by reinserting normalized payload."""
    items: List[Dict[str, Any]] = []
    for rec in records:
        # Keep all business fields except auto-generated id/created_at.
        item = {
            "source_id": rec.get("source_id"),
            "group_key": rec.get("group_key"),
            "amount": rec.get("amount"),
            "cash_flow_amount": rec.get("cash_flow_amount"),
            "net_personal_amount": rec.get("net_personal_amount"),
            "advance_amount": rec.get("advance_amount"),
            "accounting_basis": rec.get("accounting_basis"),
            "category": rec.get("category"),
            "time": rec.get("time"),
            "title": rec.get("title"),
            "segment_text": rec.get("segment_text"),
            "pay_method": rec.get("pay_method"),
            "merchant": rec.get("merchant"),
            "note": rec.get("note"),
            "direction": rec.get("direction"),
            "relation_type": rec.get("relation_type"),
            "relation_group_id": rec.get("relation_group_id"),
            "confidence": rec.get("confidence"),
        }
        items.append(item)
    return insert_records_batch(items)


def _handle_undo() -> str:
    """Undo latest operation from session memory or operation log."""
    op = SESSION_STATE.get("last_operation")

    if not op:
        log = get_last_operation_log()
        if not log:
            return "没有可撤销的操作。"

        if log.get("operation_type") in ["delete_single", "delete_by_date"]:
            before = log.get("before_payload", {}).get("records", [])
            if not before:
                return "没有可恢复的数据。"
            inserted_ids = _restore_records(before)
            insert_operation_log(
                operation_type="undo_restore",
                record_ids=inserted_ids,
                before_payload={},
                after_payload={"restored_from_log": int(log.get("id"))},
            )
            return f"已从日志撤销，恢复 {len(inserted_ids)} 笔记录。"

        return "当前仅支持撤销删除操作。"

    if op.get("type") == "delete" or op.get("type") == "delete_by_date":
        backup = op.get("backup") or []
        if not backup:
            return "没有可恢复的数据。"
        inserted_ids = _restore_records(backup)
        insert_operation_log(
            operation_type="undo_restore",
            record_ids=inserted_ids,
            before_payload={},
            after_payload={"restored_from_session": True},
        )
        SESSION_STATE["last_operation"] = None
        return f"已撤销删除，恢复 {len(inserted_ids)} 笔记录。"

    return "当前仅支持撤销最近的删除操作。"


def handle_message(user_text: str) -> str:
    """Top-level message handler for CLI."""
    user_text = _safe_str(user_text)
    if not user_text:
        return "请输入要处理的内容。"

    pending_result = _handle_pending_confirmation(user_text)
    if pending_result is not None:
        return pending_result

    parsed = parse_text(user_text)
    if not isinstance(parsed, dict):
        parsed = {}

    intent = _safe_str(parsed.get("intent")) or _intent_from_fallback(user_text)

    # If parser asks for clarification explicitly, respect it instead of forcing a write.
    if parsed.get("need_clarification") and _safe_str(parsed.get("clarification_question")):
        return _safe_str(parsed.get("clarification_question"))

    if intent == "create":
        return _handle_create(parsed, user_text)
    if intent == "query":
        return _handle_query(parsed, user_text)
    if intent == "update":
        return _handle_update(parsed, user_text)
    if intent == "delete":
        return _handle_delete(parsed, user_text)
    if intent == "explain":
        return _handle_explain(parsed, user_text)
    if intent == "undo":
        return _handle_undo()

    # Safe fallback: do not write data for unknown intent.
    return "我没完全听懂你的意图。你可以说“记账/查询/修改/删除/解释/撤销”。"


if __name__ == "__main__":
    while True:
        msg = input("你: ").strip()
        if msg in ["q", "quit", "exit"]:
            break
        print("AI:", handle_message(msg))
