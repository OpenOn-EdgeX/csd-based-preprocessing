#!/usr/bin/env python3
"""분할 알고리즘 k8s E2E 회귀 (MTE / WRR / AUTO).

`test_partition_algorithms.py` 는 선택 로직을 로컬에서 결정론적으로 본다.
여기서는 그 선택이 **실 클러스터를 통과해 CR·워커 Job·산출물까지 이어지는지**를 본다 —
계획만 맞고 디스패치가 어긋나면 로컬 테스트로는 안 잡힌다.

각 알고리즘마다:
  1. 워크로드 제출 → Succeeded 대기
  2. pj.spec.partition_info 에 기록된 algorithm / split_index / 근거 확인
  3. 샤드 산출 수 합계가 입력과 일치하는지 확인 (배정 누락·중복 없음)
  4. CR 삭제로 연쇄 GC

MTE/WRR 은 실측 처리량이 필요하다. 프로파일이 비어 있으면 매니저가 MTE 를 보정 실행으로
돌리므로(WRR 은 실패), 워크로드에 throughput 을 명시해 실행마다 같은 조건을 만든다.

  ./run_local_python.sh server/test_k8s_algorithms.py
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from server.test_k8s_integration import (  # noqa: E402
    NAMESPACE, Skip, cleanup_cr, kubectl, require_preconditions, stage_dataset,
    submit, wait_terminal)
from worker.csd_worker import SHARED_LOCAL_ROOT  # noqa: E402

N_IMAGES = 20
# AUTO 는 N <= AUTO_SMALL_DATASET(100) 이면 무조건 MTE 다. 20장이면 이 갈래가 확정된다 —
# 나머지 갈래는 실측 처리량에 따라 달라져 E2E 로는 고정할 수 없다(로컬 테스트가 덮는다).
CASES = [
    ("MTE",  "algo-mte-001",  "MTE",  "explicit"),
    ("WRR",  "algo-wrr-001",  "WRR",  "explicit"),
    ("AUTO", "algo-auto-001", "MTE",  "auto"),
]


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def plan_of(name: str) -> dict:
    raw = kubectl("get", "pj", name, "-o", "jsonpath={.spec.partition_info}")
    check(bool(raw), f"{name}: partition_info 가 비었다")
    return json.loads(raw)


def run_case(requested: str, name: str, expect_alg: str, expect_mode: str,
             images: Path, labels: Path, out_root: Path) -> str:
    kubectl("delete", "pw", name, "--ignore-not-found", check=False, timeout=120)
    submit(images, labels, out_root, name=name, algorithm=requested,
           total_samples=N_IMAGES)
    phase = wait_terminal(name)
    check(phase == "Succeeded",
          f"{name}({requested}): {phase} 로 끝났다 "
          f"(kubectl -n {NAMESPACE} logs deploy/preprocess-controller)")

    plan = plan_of(name)
    check(plan.get("algorithm") == expect_alg,
          f"{name}: algorithm={plan.get('algorithm')} (기대 {expect_alg})")
    selection = (plan.get("basis") or {}).get("algorithm_selection") or {}
    check(selection.get("mode") == expect_mode,
          f"{name}: selection.mode={selection.get('mode')} (기대 {expect_mode})")

    # split_index 는 연속 분할의 경계다. WRR 은 비연속이라 0 으로 기록된다 —
    # 이 값이 알고리즘 성격을 그대로 드러내므로 모양 검증으로 쓴다.
    split = plan.get("split_index")
    if expect_alg == "WRR":
        check(split == 0, f"{name}: WRR 인데 split_index={split} (비연속이어야 0)")
        weights = (plan.get("basis") or {}).get("weights")
        check(bool(weights), f"{name}: WRR 인데 basis.weights 가 없다")
    else:
        check(isinstance(split, int) and 0 < split < N_IMAGES,
              f"{name}: {expect_alg} 인데 split_index={split} (0<split<{N_IMAGES} 이어야)")

    # 배정 누락·중복이 없어야 한다 — 샤드 산출 합계가 입력과 같아야 한다.
    shards = {}
    for f in sorted((out_root / "_shards").glob("*/result.json")):
        shards[f.parent.name] = json.loads(f.read_text(encoding="utf-8"))
    stage1 = sum(r.get("outputCount", 0) for k, r in shards.items()
                 if not k.endswith("stage2"))
    check(stage1 == N_IMAGES, f"{name}: stage1 산출 {stage1} != 입력 {N_IMAGES}")
    csd = [r for r in shards.values() if r.get("worker") == "CSD"]
    check(bool(csd), f"{name}: CSD 샤드 결과가 없다 — 분할이 CPU 로만 갔다")

    detail = (f"{requested:4s} → {plan['algorithm']:6s} "
              f"cpu={plan.get('cpu_ratio')} split_index={split}")
    if expect_alg == "WRR":
        detail += f" weights={(plan.get('basis') or {}).get('weights')}"
    if expect_mode == "auto":
        detail += f"\n         근거: {selection.get('reason')}"
    cleanup_cr(name)
    return detail


def main() -> None:
    try:
        require_preconditions()
    except Skip as exc:
        print(f"[SKIP] k8s algorithm test — {exc}")
        return

    work = Path(SHARED_LOCAL_ROOT) / "csd_regress" / "algorithms"
    shutil.rmtree(work, ignore_errors=True)
    out_root = work / "out"
    images, labels = stage_dataset(work, n_images=N_IMAGES)
    print(f"  입력 {N_IMAGES}장 → {images}")

    results = []
    try:
        for requested, name, expect_alg, expect_mode in CASES:
            results.append(run_case(requested, name, expect_alg, expect_mode,
                                    images, labels, out_root))
            shutil.rmtree(out_root, ignore_errors=True)   # 케이스 간 산출물 격리
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print("[PASS] k8s algorithm test")
    for line in results:
        print(f"  {line}")


if __name__ == "__main__":
    main()
