#!/usr/bin/env python3
"""매니저/컨트롤러 통합 스모크 테스트.

검증 범위:
- Manager: Workload -> PreprocessingJob 생성
- Controller: PreprocessingJob -> 워커 디스패치
- Controller: stage1 완료 후 waiting_labels 진입
- Controller: 라벨 도착 후 stage2 디스패치
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from controller import preprocess_controller as controller_mod
from controller import preprocess_manager as manager_mod
from server.test_e2e_utils import create_synthetic_coco_dataset, make_workspace


class FakeThroughputStore:
    def get(self, _key):
        return None


class FakeCRD:
    def __init__(self):
        self.created: list[dict] = []

    def create_namespaced_custom_object(self, _group, _version, _namespace, _plural, body):
        self.created.append(copy.deepcopy(body))


class FakeManager(manager_mod.Manager):
    def __init__(self):
        self.crd = FakeCRD()
        self.throughput = FakeThroughputStore()
        self.status_updates: list[tuple[str, dict]] = []
        self.failures: list[tuple[str, str]] = []

    def patch_status(self, name: str, status: dict):
        self.status_updates.append((name, copy.deepcopy(status)))

    def fail(self, name: str, msg: str):
        self.failures.append((name, msg))
        self.patch_status(name, {"phase": "Failed", "error_message": msg[:1024]})


class FakeController(controller_mod.Controller):
    def __init__(self):
        self.status_updates: list[tuple[str, dict]] = []
        self.failures: list[tuple[str, str]] = []
        self.worker_jobs: list[dict] = []
        self.stage2_jobs: list[dict] = []

    def patch_status(self, name: str, status: dict):
        self.status_updates.append((name, copy.deepcopy(status)))

    def fail(self, name: str, msg: str):
        self.failures.append((name, msg))
        self.patch_status(name, {"status": "Failed", "error_message": msg[:1024]})

    def create_worker_job(self, cr: dict, target: str, shard: list, input_dir: Path, out_root: Path):
        self.worker_jobs.append({
            "job": cr["metadata"]["name"],
            "target": target,
            "count": len(shard),
            "input_dir": str(input_dir),
            "out_root": str(out_root),
        })

    def create_stage2_job(self, cr: dict, pipeline: list, out_root: Path,
                          label_path: str, placeholder: bool):
        self.stage2_jobs.append({
            "job": cr["metadata"]["name"],
            "steps": [step["op"] for step in pipeline],
            "out_root": str(out_root),
            "label_path": label_path,
            "placeholder": placeholder,
        })


def merge_status(cr: dict, update: dict) -> dict:
    merged = copy.deepcopy(cr)
    merged.setdefault("status", {})
    for key, value in update.items():
        merged["status"][key] = value
    return merged


def workload_cr(input_dir: Path, output_dir: Path) -> dict:
    return {
        "apiVersion": "edgeai.keti.re.kr/v1alpha1",
        "kind": "PreprocessingWorkload",
        "metadata": {"name": "smoke-wl", "uid": "wl-uid-001"},
        "spec": {
            "workload": {
                "dataset": {
                    "inputPath": str(input_dir),
                    "outputPath": str(output_dir),
                },
                "pipelineTemplate": "stage1_raw_ingestion",
                "stage2Template": "stage2_training_preparation",
                "waitForLabels": True,
                "algorithm": "STATIC",
            },
            "placement": {
                "nodeId": "node-smoke",
                "csdId": "csd-smoke",
                "cpuCores": 8,
                "memMb": 16384,
            },
        },
    }


def main() -> None:
    work = make_workspace("csd-manager-controller-smoke-")
    dataset = create_synthetic_coco_dataset(work, n_images=8, include_annotations=False)
    out_dir = work / "pj_out" / "smoke-wl"
    wl = workload_cr(dataset["images"], out_dir)

    manager = FakeManager()
    manager.plan_and_dispatch(copy.deepcopy(wl))

    if manager.failures:
        raise AssertionError(f"manager failed: {manager.failures}")
    if len(manager.crd.created) != 1:
        raise AssertionError(f"expected one PJ create, got {len(manager.crd.created)}")
    if not manager.status_updates or manager.status_updates[-1][1].get("phase") != "Dispatched":
        raise AssertionError(f"unexpected manager status updates: {manager.status_updates}")

    pj = manager.crd.created[0]
    pj_spec = pj["spec"]
    if pj_spec.get("job_id") != "smoke-wl":
        raise AssertionError(f"unexpected job_id: {pj_spec}")
    if pj_spec.get("stage2_template") != "stage2_training_preparation":
        raise AssertionError(f"missing stage2 template: {pj_spec}")
    if pj_spec.get("wait_for_labels") is not True:
        raise AssertionError(f"wait_for_labels not propagated: {pj_spec}")
    if sorted(pj_spec.get("execution_targets", [])) != ["CPU", "CSD"]:
        raise AssertionError(f"unexpected execution targets: {pj_spec}")
    if not pj_spec.get("partition_info", {}).get("basis", {}).get("algorithm_selection"):
        raise AssertionError(f"partition plan basis missing: {pj_spec.get('partition_info')}")

    controller = FakeController()
    controller.dispatch(copy.deepcopy(pj))

    if controller.failures:
        raise AssertionError(f"controller dispatch failed: {controller.failures}")
    if len(controller.worker_jobs) != 2:
        raise AssertionError(f"expected two worker jobs, got {controller.worker_jobs}")
    if sum(job["count"] for job in controller.worker_jobs) != 8:
        raise AssertionError(f"worker shard counts do not sum to 8: {controller.worker_jobs}")
    if not controller.status_updates:
        raise AssertionError("controller did not patch status on dispatch")

    running = controller.status_updates[-1][1]
    if running.get("status") != "Running":
        raise AssertionError(f"unexpected dispatch status: {running}")
    if running.get("output_dataset_path") != str(out_dir):
        raise AssertionError(f"output path not propagated: {running}")

    pj_running = merge_status(pj, running)

    controller.enter_stage2(copy.deepcopy(pj_running), summary={"ok": True})
    waiting = controller.status_updates[-1][1]
    if waiting.get("stage") != "waiting_labels":
        raise AssertionError(f"expected waiting_labels stage, got {waiting}")
    if (waiting.get("stage2") or {}).get("label_source") != "waiting":
        raise AssertionError(f"waiting stage metadata missing: {waiting}")

    # 라벨 도착 시뮬레이션
    dataset["annotation_file"].write_text(
        '{"images": [], "annotations": [], "categories": []}',
        encoding="utf-8",
    )
    pj_waiting = merge_status(pj_running, waiting)
    controller.check_waiting_labels(copy.deepcopy(pj_waiting))

    if len(controller.stage2_jobs) != 1:
        raise AssertionError(f"expected one stage2 dispatch, got {controller.stage2_jobs}")
    stage2 = controller.stage2_jobs[0]
    if stage2["label_path"] != str(dataset["annotations"]):
        raise AssertionError(f"unexpected stage2 label path: {stage2}")
    if stage2["placeholder"]:
        raise AssertionError(f"stage2 should use provided labels: {stage2}")

    stage2_status = controller.status_updates[-1][1]
    if stage2_status.get("stage") != "stage2":
        raise AssertionError(f"expected stage2 status, got {stage2_status}")
    if (stage2_status.get("stage2") or {}).get("label_source") != "provided":
        raise AssertionError(f"stage2 label source not updated: {stage2_status}")

    print("[PASS] manager/controller smoke test")
    print(f"  worker jobs: {controller.worker_jobs}")
    print(f"  stage2 dispatch: {stage2}")


if __name__ == "__main__":
    main()
