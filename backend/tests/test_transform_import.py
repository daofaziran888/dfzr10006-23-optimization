"""加工规则批量导入与转换取证回归。

覆盖：
- TRAN-0002 映射表达式与输出格式的导入回归：首次解析缓存指纹，
  相同规则再次导入只标注未变化，转换日志引用同一解析版本。
- 并发导入同一规则版本只保留一份。
- 回滚恢复上一版加工结果（以 TRAN-0001 至 TRAN-0003 核对）。
- 试运行输出、失败定位、订阅推送快照校验、下载路径核对约束。
"""
from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.transform_import import (
    CACHE_TABLE,
    IMPORT_TABLE,
    RESULT_TABLE,
    SNAPSHOT_TABLE,
    transform_import_service,
)
from app.store import store
from app.transform_engine import fingerprint, parse_rule

client = TestClient(app)

RULE_NOS = ["TRAN-0001", "TRAN-0002", "TRAN-0003"]


@pytest.fixture(autouse=True)
def reset_store():
    """每个用例前后恢复内存仓库，保证用例互不影响。"""
    snapshot = copy.deepcopy(store._tables)
    yield
    store._tables.clear()
    store._tables.update(snapshot)


def seed_rule(rule_no: str) -> dict[str, str]:
    """从种子数据取出规则的业务字段，作为导入报文。"""
    row = next(item for item in store.rows("transform") if item["规则编号"] == rule_no)
    return {
        "规则编号": row["规则编号"],
        "输入格式": row["输入格式"],
        "输出格式": row["输出格式"],
        "映射表达式": row["映射表达式"],
        "字段过滤": row["字段过滤"],
        "编码设置": row["编码设置"],
    }


def seed_batch() -> list[dict[str, str]]:
    return [seed_rule(no) for no in RULE_NOS]


def cache_rows(rule_no: str) -> list[dict]:
    return [row for row in store.rows(CACHE_TABLE) if row["规则编号"] == rule_no]


def result_rows(rule_no: str) -> list[dict]:
    return [row for row in store.rows(RESULT_TABLE) if row["规则编号"] == rule_no]


# ---------- 解析引擎 ----------

def test_parse_tran0002_mapping_and_output():
    """TRAN-0002 的映射表达式与输出格式解析结果可核对。"""
    parsed = parse_rule(seed_rule("TRAN-0002"))
    assert parsed.ok
    assert parsed.output_format == "JSON"
    assert parsed.output_fields == ["device_id", "metric_name", "metric_value"]
    assert parsed.mappings == [
        {"target": "device_id", "source": "device_id", "func": ""},
        {"target": "metric_name", "source": "metric_name", "func": "upper"},
        {"target": "metric_value", "source": "metric_value", "func": ""},
    ]
    # 被裁剪字段 = 显式过滤 raw_payload + 未被映射引用的 collected_at
    assert parsed.pruned_fields == ["raw_payload", "collected_at"]
    assert parsed.missing_fields == []


def test_parse_failure_reports_missing_fields():
    """映射引用不存在的输入字段时，标明缺失字段。"""
    rule = seed_rule("TRAN-0001")
    rule["映射表达式"] = "order_id=order_id; buyer=trim(buyer_name)"
    parsed = parse_rule(rule)
    assert not parsed.ok
    assert "buyer_name" in parsed.missing_fields


# ---------- 批量导入与指纹缓存 ----------

def test_first_import_parses_and_caches_fingerprint():
    """首次导入 TRAN-0001~0003：全部解析成功并缓存指纹，生成带书签的加工结果。"""
    result = transform_import_service.import_batch(seed_batch())
    assert result["ok"]
    batch = result["批次"]
    assert (batch["新增"], batch["变更"], batch["未变化"], batch["失败"]) == (3, 0, 0, 0)
    assert len(store.rows(CACHE_TABLE)) == 3
    for detail in result["明细"]:
        assert detail["解析来源"] == "新解析"
        assert detail["试运行"]["输出格式"]["字段"]
        assert detail["试运行"]["书签"].startswith("BM-")
        assert detail["试运行"]["导入版本"] == batch["批次号"]
    # 指纹与规则内容一一对应
    for no in RULE_NOS:
        assert cache_rows(no)[0]["指纹"] == fingerprint(seed_rule(no))


def test_reimport_same_rules_marks_unchanged_and_reuses_parse_version():
    """相同规则再次导入只标注未变化，转换日志引用同一解析版本，不重复解析。"""
    first = transform_import_service.import_batch(seed_batch())
    first_versions = {d["规则编号"]: d["解析版本"] for d in first["明细"]}
    cache_size = len(store.rows(CACHE_TABLE))
    result_size = len(store.rows(RESULT_TABLE))

    second = transform_import_service.import_batch(seed_batch())
    assert second["批次"]["未变化"] == 3
    assert second["批次"]["新增"] == second["批次"]["变更"] == 0
    assert len(store.rows(CACHE_TABLE)) == cache_size, "未变化的规则不得重新解析"
    assert len(store.rows(RESULT_TABLE)) == result_size, "未变化的规则不得重复生成加工结果"
    for detail in second["明细"]:
        assert detail["类别"] == "未变化"
        assert detail["解析来源"] == "复用缓存"
        assert detail["解析版本"] == first_versions[detail["规则编号"]]
        assert any(first_versions[detail["规则编号"]] in line for line in detail["转换日志"])


def test_tran0002_import_regression():
    """TRAN-0002 导入回归：首次解析缓存指纹，再次导入只标注未变化。"""
    payload = [seed_rule("TRAN-0002")]
    first = transform_import_service.import_batch(payload)
    assert first["明细"][0]["类别"] == "新增"
    assert len(cache_rows("TRAN-0002")) == 1
    parse_version = first["明细"][0]["解析版本"]

    second = transform_import_service.import_batch(payload)
    detail = second["明细"][0]
    assert detail["类别"] == "未变化"
    assert detail["解析版本"] == parse_version
    assert len(cache_rows("TRAN-0002")) == 1
    assert len(result_rows("TRAN-0002")) == 1
    assert any(parse_version in line for line in detail["转换日志"])


def test_changed_rule_reparses_with_new_version_and_keeps_history():
    """规则变更后重新解析出新解析版本，旧版加工结果保留可回滚。"""
    transform_import_service.import_batch([seed_rule("TRAN-0001")])
    changed = seed_rule("TRAN-0001")
    changed["映射表达式"] = "order_id=order_id; user_name=trim(user_name); order_amount=order_amount; order_time=order_time"
    changed["输出格式"] = "JSON:order_id,user_name,order_amount,order_time"
    result = transform_import_service.import_batch([changed])
    detail = result["明细"][0]
    assert detail["类别"] == "变更"
    assert len(cache_rows("TRAN-0001")) == 2
    versions = result_rows("TRAN-0001")
    assert len(versions) == 2
    assert [row["当前版本"] for row in versions].count(True) == 1


def test_failed_import_marks_rule_no_and_missing_fields():
    """导入失败标明规则编号和缺失字段，规则行标异常留痕。"""
    broken = seed_rule("TRAN-0003")
    broken["映射表达式"] = "user_id=user_id; phone=ghost_field"
    result = transform_import_service.import_batch([broken])
    detail = result["明细"][0]
    assert detail["类别"] == "失败"
    assert detail["规则编号"] == "TRAN-0003"
    assert "ghost_field" in detail["缺失字段"]
    row = next(item for item in store.rows("transform") if item["规则编号"] == "TRAN-0003")
    assert row["abnormal"] is True
    assert "ghost_field" in row["缺失字段"]


# ---------- 并发导入 ----------

def test_concurrent_import_same_rule_version_keeps_single_copy():
    """并发导入同一规则版本只保留一份解析缓存与加工结果。"""
    payload = seed_rule("TRAN-0002")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: transform_import_service.import_batch([payload]), range(8)))
    assert all(item["ok"] for item in results)
    categories = sorted(item["明细"][0]["类别"] for item in results)
    assert categories.count("新增") == 1
    assert categories.count("未变化") == 7
    assert len(cache_rows("TRAN-0002")) == 1
    assert len(result_rows("TRAN-0002")) == 1
    assert len(store.rows(IMPORT_TABLE)) == 8


def test_duplicate_rule_in_single_batch_marked_repeat():
    """同一批次内重复的规则版本只保留一份，其余标注重复。"""
    result = transform_import_service.import_batch([seed_rule("TRAN-0001"), seed_rule("TRAN-0001")])
    categories = [detail["类别"] for detail in result["明细"]]
    assert categories == ["新增", "重复"]
    assert len(result_rows("TRAN-0001")) == 1


# ---------- 试运行 ----------

def test_dry_run_outputs_format_pruned_fields_and_log():
    """试运行输出输出格式、被裁剪字段和转换日志。"""
    transform_import_service.import_batch(seed_batch())
    run = transform_import_service.dry_run(["TRAN-0002"])
    assert run["ok"]
    item = run["试运行"][0]
    assert item["输出格式"] == {"格式": "JSON", "字段": ["device_id", "metric_name", "metric_value"]}
    assert item["被裁剪字段"] == ["raw_payload", "collected_at"]
    assert any("PARSE-" in line for line in item["转换日志"])
    # 映射函数在样例数据上真实生效：upper 大写、无 trim 时保留原始空白
    assert item["输出内容"]["metric_name"] == "  样例-METRIC_NAME  "


def test_dry_run_unknown_rule_reports_rule_no():
    run = transform_import_service.dry_run(["TRAN-9999"])
    assert not run["ok"]
    assert run["失败"][0]["规则编号"] == "TRAN-9999"


def test_standalone_dry_run_keeps_import_version():
    """导入时不自动试运行，事后独立试运行的加工结果仍引用导入批次号。"""
    imported = transform_import_service.import_batch([seed_rule("TRAN-0001")], auto_dry_run=False)
    assert not result_rows("TRAN-0001"), "不自动试运行时不生成加工结果"
    run = transform_import_service.dry_run(["TRAN-0001"])
    assert run["ok"]
    assert result_rows("TRAN-0001")[0]["导入版本"] == imported["批次"]["批次号"]


# ---------- 核对、下载、订阅同步 ----------

def test_download_path_only_serves_verified_output():
    """下载路径只引用已核对的输出内容：未核对 409，核对后可下载。"""
    transform_import_service.import_batch([seed_rule("TRAN-0001")])
    bookmark = result_rows("TRAN-0001")[0]["书签"]

    view = transform_import_service.list_results("TRAN-0001")[0]
    assert view["下载路径"] is None, "未核对的结果不暴露下载路径"
    assert client.get(f"/api/transform/results/{bookmark}/download").status_code == 409

    verify = client.post(f"/api/transform/results/{bookmark}/verify")
    assert verify.status_code == 200 and verify.json()["ok"]
    view = transform_import_service.list_results("TRAN-0001")[0]
    assert view["下载路径"] == f"/api/transform/results/{bookmark}/download"
    download = client.get(view["下载路径"])
    assert download.status_code == 200
    # 恒等映射保留样例原值，trim 映射去掉空白，输出内容可逐字段核对
    assert download.json()["输出内容"]["order_id"] == "  样例-order_id  "
    assert download.json()["输出内容"]["user_name"] == "样例-user_name"


def test_sync_subscribe_push_format_references_verified_snapshot():
    """结果同步到订阅推送：推送格式只引用已验证的输出字段快照。"""
    transform_import_service.import_batch([seed_rule("TRAN-0002")])
    bookmark = result_rows("TRAN-0002")[0]["书签"]

    # 未核对不能同步
    denied = client.post(f"/api/transform/results/{bookmark}/sync-subscribe")
    assert denied.json()["ok"] is False

    client.post(f"/api/transform/results/{bookmark}/verify")
    synced = client.post(f"/api/transform/results/{bookmark}/sync-subscribe")
    assert synced.json()["ok"]
    task = synced.json()["entry"]
    snapshot_ref = f"snapshot://TRAN-0002/{result_rows('TRAN-0002')[0]['解析版本']}"
    assert task["推送格式"] == snapshot_ref
    assert snapshot_ref in {row["快照引用"] for row in store.rows(SNAPSHOT_TABLE)}

    # 重复同步不重复建任务
    again = client.post(f"/api/transform/results/{bookmark}/sync-subscribe")
    assert again.json()["entry"]["任务编号"] == task["任务编号"]

    # 订阅模块直接登记未验证快照会被拦下
    rejected = client.post("/api/subscribe", json={"values": {
        "任务编号": "SUBS-9001",
        "数据源名称": "非法引用",
        "推送目标": "通道",
        "推送格式": "snapshot://TRAN-0002/PARSE-9999",
    }})
    assert rejected.json()["ok"] is False
    assert "未验证" in rejected.json()["message"]


# ---------- 回滚 ----------

@pytest.mark.parametrize("rule_no", RULE_NOS)
def test_rollback_restores_previous_result(rule_no: str):
    """以 TRAN-0001 至 TRAN-0003 核对：回滚可恢复上一版加工结果。"""
    transform_import_service.import_batch([seed_rule(rule_no)])
    first_version = result_rows(rule_no)[0]

    changed = seed_rule(rule_no)
    changed["字段过滤"] = ""
    changed["编码设置"] = "UTF-8"
    transform_import_service.import_batch([changed])
    assert len(result_rows(rule_no)) == 2

    rolled = client.post(f"/api/transform/rollback/{rule_no}")
    assert rolled.json()["ok"]
    restored = rolled.json()["entry"]
    assert restored["书签"] == first_version["书签"]
    assert restored["解析版本"] == first_version["解析版本"]
    assert restored["当前版本"] is True

    current = [row for row in result_rows(rule_no) if row["当前版本"]]
    assert len(current) == 1 and current[0]["书签"] == first_version["书签"]
    # 服务版本回退模块留痕
    trace = [row for row in store.rows("rollback") if row["版本号"] == f"{rule_no}@{first_version['解析版本']}"]
    assert trace, "回滚应在服务版本回退模块留痕"

    # 已是最早一版，再次回滚给出可读说明
    again = client.post(f"/api/transform/rollback/{rule_no}")
    assert again.json()["ok"] is False
    assert "没有可回滚的上一版" in again.json()["message"]


# ---------- 接口层冒烟 ----------

def test_import_api_and_export_route_order():
    """导入接口可用；/export 不被 /{entry_id} 遮蔽。"""
    response = client.post("/api/transform/import", json={"rules": seed_batch()})
    assert response.status_code == 200
    assert response.json()["批次"]["新增"] == 3
    export = client.get("/api/transform/export")
    assert export.status_code == 200
    assert export.json()["module"] == "transform"
    batches = client.get("/api/transform/batches")
    assert batches.json()["total"] == 1
    cache = client.get("/api/transform/parse-cache")
    assert cache.json()["total"] == 3
