"""加工规则批量导入与转换取证：解析缓存复用、试运行、结果版本、回滚与订阅同步。

设计要点：
- 指纹缓存：首次解析后把「指纹 → 解析结果 + 解析版本」存入缓存表，
  相同内容再次导入直接复用，只标注未变化，转换日志引用同一解析版本。
- 并发去重：导入提交全程持锁，同一规则版本（规则编号 + 指纹）只保留一份。
- 加工结果按版本保留：每次解析版本变化产生一条带书签的结果，
  回滚把当前指针切回上一版，并在服务版本回退模块留痕。
- 订阅同步：只有核对过的结果会发布「已验证的输出字段快照」，
  订阅任务的推送格式只允许引用这些快照。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.store import store
from app.transform_engine import RULE_FIELDS, dry_run_record_from_view, fingerprint, parse_rule

MODULE = "transform"
IMPORT_TABLE = "transform_imports"
CACHE_TABLE = "transform_parse_cache"
RESULT_TABLE = "transform_results"
SNAPSHOT_TABLE = "transform_snapshots"
SUBSCRIBE_TABLE = "subscribe"
ROLLBACK_TABLE = "rollback"

RESULT_STATUS_PENDING = "待核对"
RESULT_STATUS_VERIFIED = "已核对"

_import_lock = threading.Lock()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _next_id(table: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1


def _next_no(table: str, prefix: str) -> str:
    return f"{prefix}-{_next_id(table):04d}"


class TransformImportService:
    """批量导入 + 试运行 + 取证；所有会改共享状态的路径都在锁内完成。"""

    # ---------- 查询 ----------

    def list_batches(self) -> list[dict[str, Any]]:
        return list(reversed(store.rows(IMPORT_TABLE)))

    def list_parse_cache(self) -> list[dict[str, Any]]:
        rows = []
        for row in store.rows(CACHE_TABLE):
            rows.append({
                "指纹": row["指纹"],
                "规则编号": row["规则编号"],
                "解析版本": row["解析版本"],
                "输出字段": list(row["解析结果"]["output_fields"]),
                "被裁剪字段": list(row["解析结果"]["pruned_fields"]),
                "创建时间": row["创建时间"],
            })
        return rows

    def list_results(self, rule_no: str | None = None) -> list[dict[str, Any]]:
        rows = [row for row in store.rows(RESULT_TABLE) if not rule_no or row["规则编号"] == rule_no]
        return [self._result_view(row) for row in sorted(rows, key=lambda item: item["id"], reverse=True)]

    def list_snapshots(self) -> list[dict[str, Any]]:
        return [dict(row) for row in store.rows(SNAPSHOT_TABLE)]

    # ---------- 批量导入 ----------

    def import_batch(self, rules: list[dict[str, Any]], *, auto_dry_run: bool = True) -> dict[str, Any]:
        """批量导入：已解析过的规则直接复用缓存，只解析新增或变更项。"""
        if not rules:
            return {"ok": False, "message": "导入内容为空，至少需要一条加工规则"}
        with _import_lock:
            batch = {
                "id": _next_id(IMPORT_TABLE),
                "批次号": _next_no(IMPORT_TABLE, "IMP"),
                "导入时间": _now(),
                "总数": len(rules),
                "新增": 0,
                "变更": 0,
                "未变化": 0,
                "重复": 0,
                "失败": 0,
            }
            outcomes: list[dict[str, Any]] = []
            seen_in_batch: set[tuple[str, str]] = set()
            for raw in rules:
                outcome = self._import_one(raw, batch, seen_in_batch, auto_dry_run=auto_dry_run)
                outcomes.append(outcome)
                batch[outcome["类别"]] += 1
            batch["状态"] = "失败" if batch["失败"] == batch["总数"] else ("部分失败" if batch["失败"] else "完成")
            store.rows(IMPORT_TABLE).append(batch)
            return {"ok": batch["失败"] < batch["总数"], "批次": batch, "明细": outcomes}

    def _import_one(
        self,
        raw: dict[str, Any],
        batch: dict[str, Any],
        seen_in_batch: set[tuple[str, str]],
        *,
        auto_dry_run: bool,
    ) -> dict[str, Any]:
        rule = {name: str(raw.get(name) or "").strip() for name in RULE_FIELDS}
        rule_no = rule["规则编号"] or "未编号"
        fp = fingerprint(rule)
        dedupe_key = (rule_no, fp)

        # 并发/批内去重：同一规则版本只保留一份
        if dedupe_key in seen_in_batch:
            return {"规则编号": rule_no, "类别": "重复", "指纹": fp,
                    "说明": "同一规则版本已在本批次导入，仅保留一份"}
        seen_in_batch.add(dedupe_key)

        parsed = parse_rule(rule)
        if not parsed.ok:
            self._record_failure(rule, parsed)
            return {"规则编号": rule_no, "类别": "失败", "指纹": fp,
                    "缺失字段": parsed.missing_fields, "错误": parsed.errors}

        cache_row = self._cache_lookup(fp)
        rule_row = self._find_rule(rule_no)
        reused = cache_row is not None
        if reused:
            parse_version = cache_row["解析版本"]
        else:
            parse_version = _next_no(CACHE_TABLE, "PARSE")
            store.rows(CACHE_TABLE).append({
                "id": _next_id(CACHE_TABLE),
                "指纹": fp,
                "规则编号": rule_no,
                "解析版本": parse_version,
                "解析结果": {
                    "input_format": parsed.input_format,
                    "input_fields": parsed.input_fields,
                    "output_format": parsed.output_format,
                    "output_fields": parsed.output_fields,
                    "mappings": parsed.mappings,
                    "pruned_fields": parsed.pruned_fields,
                },
                "创建时间": _now(),
            })

        if rule_row is not None and rule_row.get("指纹") == fp:
            category = "未变化"
        else:
            # 规则行还没有指纹说明从未成功解析，首次解析记为新增
            category = "新增" if rule_row is None or not rule_row.get("指纹") else "变更"

        log_lines = self._build_log(parsed, parse_version, reused=reused, unchanged=category == "未变化")
        self._upsert_rule(rule_row, rule, fp, parse_version, log_lines)
        self._find_rule(rule_no)["导入版本"] = batch["批次号"]

        outcome: dict[str, Any] = {
            "规则编号": rule_no,
            "类别": category,
            "指纹": fp,
            "解析版本": parse_version,
            "解析来源": "复用缓存" if reused else "新解析",
            "输出格式": f"{parsed.output_format}:{','.join(parsed.output_fields)}",
            "被裁剪字段": parsed.pruned_fields,
            "转换日志": log_lines,
        }
        if auto_dry_run:
            outcome["试运行"] = self._dry_run_rule(rule_no, batch["批次号"])
        return outcome

    # ---------- 试运行 ----------

    def dry_run(self, rule_nos: list[str] | None = None) -> dict[str, Any]:
        """试运行：输出格式、被裁剪字段、转换日志；失败标明规则编号和缺失字段。"""
        with _import_lock:
            batch_no_fallback = f"DRYRUN-{_now()}"
            targets = rule_nos or [str(row.get("规则编号")) for row in store.rows(MODULE) if row.get("规则编号")]
            runs, failures = [], []
            for rule_no in targets:
                rule_row = self._find_rule(rule_no)
                if rule_row is None:
                    failures.append({"规则编号": rule_no, "缺失字段": [], "错误": ["规则不存在或未导入"]})
                    continue
                cache_row = self._cache_lookup(rule_row.get("指纹", ""))
                if cache_row is None:
                    failures.append({"规则编号": rule_no, "缺失字段": rule_row.get("缺失字段", []),
                                     "错误": ["规则尚未成功解析，请先修复导入"]})
                    continue
                runs.append(self._dry_run_rule(rule_no, rule_row.get("导入版本") or batch_no_fallback))
            return {"ok": not failures, "试运行": runs, "失败": failures}

    def _dry_run_rule(self, rule_no: str, import_version: str) -> dict[str, Any]:
        rule_row = self._find_rule(rule_no)
        cache_row = self._cache_lookup(rule_row["指纹"])
        parsed_view = cache_row["解析结果"]
        # 试运行直接复用缓存的解析结果，不重新解析
        output_record = dry_run_record_from_view(parsed_view)
        log_lines = list(rule_row.get("转换日志", []))
        result = self._ensure_result(
            rule_no=rule_no,
            parse_version=cache_row["解析版本"],
            import_version=import_version,
            output_record=output_record,
            log_lines=log_lines,
        )
        return {
            "规则编号": rule_no,
            "解析版本": cache_row["解析版本"],
            "输出格式": {"格式": parsed_view["output_format"], "字段": parsed_view["output_fields"]},
            "被裁剪字段": parsed_view["pruned_fields"],
            "转换日志": log_lines,
            "输出内容": output_record,
            "书签": result["书签"],
            "导入版本": result["导入版本"],
        }

    def _ensure_result(
        self,
        *,
        rule_no: str,
        parse_version: str,
        import_version: str,
        output_record: dict[str, Any],
        log_lines: list[str],
    ) -> dict[str, Any]:
        """同一规则同一解析版本只保留一份加工结果；重复试运行直接复用。"""
        for row in store.rows(RESULT_TABLE):
            if row["规则编号"] == rule_no and row["解析版本"] == parse_version:
                return row
        bookmark = _next_no(RESULT_TABLE, "BM")
        row = {
            "id": _next_id(RESULT_TABLE),
            "规则编号": rule_no,
            "书签": bookmark,
            "导入版本": import_version,
            "解析版本": parse_version,
            "输出内容": output_record,
            "转换日志": list(log_lines),
            "状态": RESULT_STATUS_PENDING,
            "下载路径": f"/api/transform/results/{bookmark}/download",
            "当前版本": False,
            "创建时间": _now(),
        }
        store.rows(RESULT_TABLE).append(row)
        for other in store.rows(RESULT_TABLE):
            if other["规则编号"] == rule_no:
                other["当前版本"] = other is row
        return row

    # ---------- 核对 / 下载 / 订阅同步 ----------

    def verify_result(self, bookmark: str) -> tuple[dict[str, Any] | None, str]:
        """核对加工结果：核对通过才发布已验证的输出字段快照。"""
        with _import_lock:
            row = self._find_result(bookmark)
            if row is None:
                return None, f"加工结果 {bookmark} 不存在"
            if row["状态"] == RESULT_STATUS_VERIFIED:
                return self._result_view(row), "加工结果此前已核对"
            row["状态"] = RESULT_STATUS_VERIFIED
            snapshot_ref = f"snapshot://{row['规则编号']}/{row['解析版本']}"
            if not any(item["快照引用"] == snapshot_ref for item in store.rows(SNAPSHOT_TABLE)):
                cache_row = self._cache_lookup_by_version(row["解析版本"])
                store.rows(SNAPSHOT_TABLE).append({
                    "id": _next_id(SNAPSHOT_TABLE),
                    "快照引用": snapshot_ref,
                    "规则编号": row["规则编号"],
                    "解析版本": row["解析版本"],
                    "输出字段": list((cache_row or {}).get("解析结果", {}).get("output_fields", [])),
                    "来源书签": bookmark,
                    "状态": "已验证",
                    "创建时间": _now(),
                })
            return self._result_view(row), f"加工结果 {bookmark} 已核对，输出字段快照 {snapshot_ref} 已验证"

    def download_result(self, bookmark: str) -> tuple[dict[str, Any] | None, str]:
        """下载路径只引用已核对的输出内容；未核对的结果不给下载。"""
        row = self._find_result(bookmark)
        if row is None:
            return None, f"加工结果 {bookmark} 不存在"
        if row["状态"] != RESULT_STATUS_VERIFIED:
            return None, f"加工结果 {bookmark} 尚未核对，下载路径仅引用已核对的输出内容"
        return {
            "书签": row["书签"],
            "规则编号": row["规则编号"],
            "导入版本": row["导入版本"],
            "解析版本": row["解析版本"],
            "输出内容": row["输出内容"],
            "快照引用": f"snapshot://{row['规则编号']}/{row['解析版本']}",
        }, "ok"

    def sync_subscribe(self, bookmark: str, *, push_target: str = "数据订阅推送通道") -> tuple[dict[str, Any] | None, str]:
        """把已核对的加工结果同步到数据订阅推送；推送格式只引用已验证的输出字段快照。"""
        with _import_lock:
            row = self._find_result(bookmark)
            if row is None:
                return None, f"加工结果 {bookmark} 不存在"
            if row["状态"] != RESULT_STATUS_VERIFIED:
                return None, f"加工结果 {bookmark} 尚未核对，不能同步到数据订阅推送"
            snapshot_ref = f"snapshot://{row['规则编号']}/{row['解析版本']}"
            snapshot = next((item for item in store.rows(SNAPSHOT_TABLE) if item["快照引用"] == snapshot_ref), None)
            if snapshot is None:
                return None, f"输出字段快照 {snapshot_ref} 未验证，推送格式不允许引用"
            for task in store.rows(SUBSCRIBE_TABLE):
                if task.get("推送格式") == snapshot_ref:
                    return dict(task), f"订阅任务 {task['任务编号']} 已引用该快照，无需重复同步"
            task = {
                "id": _next_id(SUBSCRIBE_TABLE),
                "status": "草稿",
                "pending": True,
                "abnormal": False,
                "任务编号": _next_no(SUBSCRIBE_TABLE, "SUBS"),
                "数据源名称": f"加工结果-{row['规则编号']}",
                "推送目标": push_target,
                "推送格式": snapshot_ref,
                "推送频率": "实时",
                "过滤条件": f"书签={row['书签']}",
                "最近推送": _now(),
                "任务状态": "待启动",
            }
            store.rows(SUBSCRIBE_TABLE).append(task)
            return dict(task), f"加工结果已同步到订阅任务 {task['任务编号']}，推送格式引用已验证快照 {snapshot_ref}"

    # ---------- 回滚 ----------

    def rollback(self, rule_no: str) -> tuple[dict[str, Any] | None, str]:
        """回滚到上一版加工结果，并在服务版本回退模块留痕。"""
        with _import_lock:
            versions = sorted(
                (row for row in store.rows(RESULT_TABLE) if row["规则编号"] == rule_no),
                key=lambda item: item["id"],
            )
            current = next((row for row in versions if row.get("当前版本")), None)
            if current is None:
                return None, f"加工规则 {rule_no} 还没有加工结果，无法回滚"
            index = versions.index(current)
            if index == 0:
                return None, f"加工规则 {rule_no} 已是最早一版，没有可回滚的上一版加工结果"
            previous = versions[index - 1]
            for row in versions:
                row["当前版本"] = row is previous
            rule_row = self._find_rule(rule_no)
            if rule_row is not None:
                rule_row["解析版本"] = previous["解析版本"]
                rule_row["转换日志"] = list(previous["转换日志"])
            snapshot_row = {
                "id": _next_id(ROLLBACK_TABLE),
                "status": "已回退",
                "pending": False,
                "abnormal": False,
                "快照编号": _next_no(ROLLBACK_TABLE, "ROLL"),
                "服务名称": "数据加工转换",
                "版本号": f"{rule_no}@{previous['解析版本']}",
                "配置快照": f"书签={previous['书签']}；导入版本={previous['导入版本']}",
                "创建时间": _now(),
                "回退次数": "1",
                "最近回退": _now(),
                "快照状态": "已回退",
            }
            store.rows(ROLLBACK_TABLE).append(snapshot_row)
            return self._result_view(previous), (
                f"加工规则 {rule_no} 已回滚到上一版加工结果 {previous['书签']}"
                f"（解析版本 {previous['解析版本']}）"
            )

    # ---------- 内部工具 ----------

    def _find_rule(self, rule_no: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if row.get("规则编号") == rule_no:
                return row
        return None

    def _find_result(self, bookmark: str) -> dict[str, Any] | None:
        for row in store.rows(RESULT_TABLE):
            if row["书签"] == bookmark:
                return row
        return None

    def _cache_lookup(self, fp: str) -> dict[str, Any] | None:
        for row in store.rows(CACHE_TABLE):
            if row["指纹"] == fp:
                return row
        return None

    def _cache_lookup_by_version(self, parse_version: str) -> dict[str, Any] | None:
        for row in store.rows(CACHE_TABLE):
            if row["解析版本"] == parse_version:
                return row
        return None

    def _upsert_rule(
        self,
        rule_row: dict[str, Any] | None,
        rule: dict[str, str],
        fp: str,
        parse_version: str,
        log_lines: list[str],
    ) -> None:
        if rule_row is None:
            rule_row = {"id": _next_id(MODULE), "status": "草稿", "pending": True, "abnormal": False}
            store.rows(MODULE).append(rule_row)
        rule_row.update(rule)
        rule_row["指纹"] = fp
        rule_row["解析版本"] = parse_version
        rule_row["规则状态"] = rule_row.get("status", "草稿")
        rule_row["转换日志"] = list(log_lines)

    def _record_failure(self, rule: dict[str, str], parsed) -> None:
        """解析失败的规则也登记进列表：标明缺失字段并标异常，方便核对。"""
        rule_row = self._find_rule(parsed.rule_no)
        if rule_row is None:
            rule_row = {"id": _next_id(MODULE), "status": "草稿", "pending": True, "abnormal": False}
            store.rows(MODULE).append(rule_row)
        rule_row.update(rule)
        rule_row["缺失字段"] = list(parsed.missing_fields)
        rule_row["abnormal"] = True
        rule_row["转换日志"] = [
            f"[{parsed.rule_no}] 解析失败：缺失字段 {', '.join(parsed.missing_fields) or '无'}；"
            + "；".join(parsed.errors)
        ]

    @staticmethod
    def _build_log(parsed, parse_version: str, *, reused: bool, unchanged: bool) -> list[str]:
        if unchanged:
            head = f"[{parsed.rule_no}] 解析版本 {parse_version}（规则未变化，复用同一解析版本）"
        elif reused:
            head = f"[{parsed.rule_no}] 解析版本 {parse_version}（指纹命中缓存，复用解析结果）"
        else:
            head = f"[{parsed.rule_no}] 解析版本 {parse_version}（新解析，指纹已缓存）"
        lines = [head]
        for item in parsed.mappings:
            source = f"{item['func']}({item['source']})" if item["func"] else item["source"]
            lines.append(f"[{parsed.rule_no}] 映射 {item['target']} <- {source}")
        if parsed.pruned_fields:
            lines.append(f"[{parsed.rule_no}] 裁剪字段 {'、'.join(parsed.pruned_fields)}")
        lines.append(
            f"[{parsed.rule_no}] 输出格式 {parsed.output_format}({', '.join(parsed.output_fields)})"
        )
        return lines

    def _result_view(self, row: dict[str, Any]) -> dict[str, Any]:
        """加工结果视图：下载路径只引用已核对的输出内容，未核对不暴露路径。"""
        verified = row["状态"] == RESULT_STATUS_VERIFIED
        return {
            "规则编号": row["规则编号"],
            "书签": row["书签"],
            "导入版本": row["导入版本"],
            "解析版本": row["解析版本"],
            "输出内容": row["输出内容"],
            "转换日志": list(row["转换日志"]),
            "状态": row["状态"],
            "当前版本": bool(row.get("当前版本")),
            "下载路径": row["下载路径"] if verified else None,
            "创建时间": row["创建时间"],
        }


transform_import_service = TransformImportService()
