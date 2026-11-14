"""加工规则解析引擎：把映射表达式、字段过滤、输入/输出格式解析成可核对的结构。

只做纯函数解析，不碰存储层；解析结果带指纹，供导入缓存复用。

语法约定：
- 输入/输出格式：``JSON:field1,field2`` 或 ``CSV:field1,field2``，冒号后是字段清单。
- 映射表达式：``目标字段=来源字段`` 或 ``目标字段=函数(来源字段)``，多条用 ``;`` 或换行分隔；
  支持的函数：trim / upper / lower。
- 字段过滤：逗号或分号分隔的输入字段名，表示要从输入中裁剪掉的字段。
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

RULE_FIELDS = ["规则编号", "输入格式", "输出格式", "映射表达式", "字段过滤", "编码设置"]
SUPPORTED_FUNCS = {"trim", "upper", "lower"}
_FORMAT_RE = re.compile(r"^(?P<fmt>[A-Za-z0-9]+)\s*(?::\s*(?P<fields>.+))?$")
_MAPPING_RE = re.compile(
    r"^(?P<target>[A-Za-z_][\w]*)\s*=\s*(?:(?P<func>[A-Za-z_][\w]*)\s*\(\s*)?(?P<source>[A-Za-z_][\w]*)\s*\)?$"
)


@dataclass
class ParsedRule:
    """一条加工规则的解析结果；ok=False 时 missing 列出缺失字段。"""

    rule_no: str
    ok: bool
    input_format: str = ""
    input_fields: list[str] = field(default_factory=list)
    output_format: str = ""
    output_fields: list[str] = field(default_factory=list)
    mappings: list[dict[str, str]] = field(default_factory=list)
    pruned_fields: list[str] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def fingerprint(rule: dict[str, Any]) -> str:
    """对规则的业务字段算指纹；内容相同指纹必然相同，用于解析缓存与去重。"""
    canonical = {name: str(rule.get(name) or "").strip() for name in RULE_FIELDS}
    blob = json.dumps(canonical, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def parse_format_decl(text: str, *, label: str, errors: list[str]) -> tuple[str, list[str]]:
    """解析 ``JSON:f1,f2`` 形式的格式声明，返回 (格式类型, 字段清单)。"""
    raw = str(text or "").strip()
    if not raw:
        errors.append(f"{label}为空")
        return "", []
    match = _FORMAT_RE.match(raw)
    if not match:
        errors.append(f"{label}无法解析：{raw}")
        return "", []
    fmt = match.group("fmt").upper()
    fields = [item.strip() for item in (match.group("fields") or "").split(",") if item.strip()]
    return fmt, fields


def parse_mappings(text: str, errors: list[str]) -> list[dict[str, str]]:
    """解析映射表达式，返回 [{target, source, func}]；语法错误记入 errors。"""
    mappings: list[dict[str, str]] = []
    chunks = [chunk.strip() for chunk in re.split(r"[;\n]+", str(text or "")) if chunk.strip()]
    if not chunks:
        errors.append("映射表达式为空")
        return mappings
    for chunk in chunks:
        match = _MAPPING_RE.match(chunk)
        if not match:
            errors.append(f"映射表达式无法解析：{chunk}")
            continue
        func = (match.group("func") or "").lower()
        if func and func not in SUPPORTED_FUNCS:
            errors.append(f"映射函数「{func}」不支持，仅支持 {sorted(SUPPORTED_FUNCS)}")
            continue
        mappings.append({
            "target": match.group("target"),
            "source": match.group("source"),
            "func": func,
        })
    return mappings


def parse_field_filter(text: str) -> list[str]:
    """解析字段过滤清单（逗号/分号分隔），保持声明顺序去重。"""
    seen: list[str] = []
    for chunk in re.split(r"[,，;；\s]+", str(text or "")):
        name = chunk.strip()
        if name and name not in seen:
            seen.append(name)
    return seen


def parse_rule(rule: dict[str, Any]) -> ParsedRule:
    """解析单条规则，产出可核对的结构：映射、被裁剪字段、输出字段、缺失字段。"""
    rule_no = str(rule.get("规则编号") or "").strip() or "未编号"
    errors: list[str] = []
    missing: list[str] = []

    input_format, input_fields = parse_format_decl(rule.get("输入格式", ""), label="输入格式", errors=errors)
    output_format, output_fields = parse_format_decl(rule.get("输出格式", ""), label="输出格式", errors=errors)
    mappings = parse_mappings(rule.get("映射表达式", ""), errors)
    declared_prune = parse_field_filter(rule.get("字段过滤", ""))

    input_set = set(input_fields)
    mapped_sources = {item["source"] for item in mappings}
    mapped_targets = {item["target"] for item in mappings}

    # 映射引用的来源字段必须存在于输入格式
    for source in sorted(mapped_sources - input_set):
        missing.append(source)
        errors.append(f"映射引用的输入字段缺失：{source}")
    # 输出字段必须都有映射来源
    for target in output_fields:
        if target not in mapped_targets:
            missing.append(target)
            errors.append(f"输出字段缺少映射：{target}")
    # 字段过滤必须引用已声明的输入字段，且不能与被映射引用的字段冲突
    for name in declared_prune:
        if name not in input_set:
            missing.append(name)
            errors.append(f"字段过滤引用的输入字段缺失：{name}")
        elif name in mapped_sources:
            errors.append(f"字段过滤与映射表达式冲突：{name}")

    # 被裁剪字段 = 显式过滤 ∪ 未被任何映射引用的输入字段
    pruned = [name for name in input_fields if name in declared_prune or name not in mapped_sources]

    return ParsedRule(
        rule_no=rule_no,
        ok=not errors,
        input_format=input_format,
        input_fields=input_fields,
        output_format=output_format,
        output_fields=output_fields,
        mappings=mappings,
        pruned_fields=pruned,
        missing_fields=sorted(set(missing)),
        errors=errors,
    )


def apply_func(func: str, value: Any) -> Any:
    """试运行时对样例值应用映射函数，保证输出内容可核对。"""
    text = str(value)
    if func == "trim":
        return text.strip()
    if func == "upper":
        return text.upper()
    if func == "lower":
        return text.lower()
    return text


def dry_run_record_from_view(parsed_view: dict[str, Any]) -> dict[str, Any]:
    """按缓存的解析结果视图跑一条样例记录，产出输出记录（仅含输出字段）。"""
    sample_input = {name: f"  样例-{name}  " for name in parsed_view["input_fields"]}
    output: dict[str, Any] = {}
    for item in parsed_view["mappings"]:
        if item["target"] in parsed_view["output_fields"]:
            output[item["target"]] = apply_func(item["func"], sample_input.get(item["source"], ""))
    return {name: output.get(name) for name in parsed_view["output_fields"]}


def dry_run_record(parsed: ParsedRule) -> dict[str, Any]:
    """按解析结果跑一条样例记录（ParsedRule 形式，供直接解析后试运行）。"""
    view = {
        "input_fields": parsed.input_fields,
        "output_fields": parsed.output_fields,
        "mappings": parsed.mappings,
    }
    return dry_run_record_from_view(view)
