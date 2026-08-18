#!/usr/bin/env python3
"""분할 알고리즘 선택·계획 테스트 (로컬, 클러스터 불필요).

AUTO 선택은 계획 시점에 한 번 일어나고 결과가 CR 에 그대로 실린다. 그래서 실 클러스터
E2E 만으로는 **네 갈래 중 하나만** 밟게 된다 — 실측 처리량이 그날 값에 따라 달라지므로
어느 갈래를 밟았는지도 실행마다 바뀐다. 여기서는 입력을 직접 주어 네 갈래를 전부 고정
검증하고, 분할 계획이 지켜야 할 불변식을 확인한다.

  ./run_local_python.sh server/test_partition_algorithms.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from controller.partitioners import (  # noqa: E402
    AUTO_BALANCED_RATIO, AUTO_SMALL_DATASET, get_partitioner, select_algorithm)

FILES = [f"img_{i:04d}.jpg" for i in range(500)]


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def test_auto_decision_table() -> None:
    """§12 결정 테이블 네 갈래. 위에서부터 평가되고 먼저 걸리는 쪽이 이긴다."""
    cases = [
        # (n, cpu_tp, csd_tp, 기대 알고리즘, 기대 근거 키워드)
        (50, 40.0, 2.0, "MTE", "소규모"),          # ① 소규모가 편차보다 우선
        (500, None, None, "MTE", "실측값이 없어"),  # ② 프로파일 없음
        (500, 40.0, 20.0, "MTE", "차이가 작음"),    # ③ 비율 2.0 <= 3.0
        (500, 40.0, 2.0, "WRR", "편차 큼"),         # ④ 대규모 + 비율 20.0 > 3.0
    ]
    for n, cpu, csd, expect_alg, expect_kw in cases:
        alg, decision = select_algorithm(n, cpu, csd)
        check(alg == expect_alg,
              f"n={n} cpu={cpu} csd={csd} → {alg} (기대 {expect_alg}): {decision['reason']}")
        check(expect_kw in decision["reason"],
              f"근거 문구가 기대와 다르다: {decision['reason']}")
        check(decision["total_files"] == n and decision["mode"] == "auto",
              f"근거에 감사용 값이 빠졌다: {decision}")
    # 경계값 — 임계값은 '이하'가 MTE 다
    check(select_algorithm(AUTO_SMALL_DATASET, 40.0, 2.0)[0] == "MTE", "N == 임계값이면 MTE")
    check(select_algorithm(AUTO_SMALL_DATASET + 1, 40.0, 2.0)[0] == "WRR", "N > 임계값이면 WRR")
    ratio_tp = (AUTO_BALANCED_RATIO * 2.0, 2.0)     # 비율 == 임계값
    check(select_algorithm(500, *ratio_tp)[0] == "MTE", "비율 == 임계값이면 MTE")
    print(f"  [ok] AUTO 결정 테이블 4갈래 + 경계값 (small={AUTO_SMALL_DATASET}, "
          f"ratio={AUTO_BALANCED_RATIO})")


def test_partition_invariants() -> None:
    """어떤 알고리즘이든 모든 파일이 정확히 한 번씩 배정돼야 한다.

    이게 깨지면 샤드 누락(파일 유실)이나 중복 처리(같은 파일 두 번)가 된다.
    컨트롤러의 일관성 검증이 잡아주지만, 여기서 먼저 잡는 편이 싸다."""
    plans = {
        "STATIC": {"cpu_ratio": 0.5},
        "MTE": {"basis": {"throughput": {"cpu": 40.0, "csd": 10.0}}},
        "WRR": {"basis": {"weights": {"cpu": 4, "csd": 1}}},
    }
    for alg, info in plans.items():
        shards = get_partitioner(alg).plan(FILES, ["CPU", "CSD"], info)
        assigned = shards["CPU"] + shards["CSD"]
        check(len(assigned) == len(FILES), f"{alg}: 배정 수 불일치 {len(assigned)}")
        check(sorted(assigned) == sorted(FILES), f"{alg}: 배정 집합이 입력과 다르다")
        check(shards["CPU"] and shards["CSD"], f"{alg}: 한쪽 샤드가 비었다")
    print("  [ok] 배정 불변식 (STATIC/MTE/WRR, 500장 전부 정확히 1회)")


def test_partition_shapes() -> None:
    """알고리즘마다 샤드 '모양'이 다르다 — 이게 알고리즘의 정체다."""
    static = get_partitioner("STATIC").plan(FILES, ["CPU", "CSD"], {"cpu_ratio": 0.8})
    check(static["CPU"] == FILES[:400], "STATIC: CPU 는 앞쪽 연속 블록")
    check(static["CSD"] == FILES[400:], "STATIC: CSD 는 뒤쪽 연속 블록")

    # MTE: 만남 지점 = N * cpu/(cpu+csd) = 500 * 40/50 = 400.
    # CSD 는 뒤에서 중앙 방향으로 처리하므로 역순이다.
    mte = get_partitioner("MTE").plan(
        FILES, ["CPU", "CSD"], {"basis": {"throughput": {"cpu": 40.0, "csd": 10.0}}})
    check(mte["CPU"] == FILES[:400], f"MTE: 만남 지점이 400 이어야 한다 ({len(mte['CPU'])})")
    check(mte["CSD"] == list(reversed(FILES[400:])), "MTE: CSD 는 뒤 → 중앙 역순")

    # WRR: 라운드 길이 5 (4+1) 마다 앞 4개 CPU, 나머지 1개 CSD → 비연속
    wrr = get_partitioner("WRR").plan(
        FILES, ["CPU", "CSD"], {"basis": {"weights": {"cpu": 4, "csd": 1}}})
    check(wrr["CSD"] == [f for i, f in enumerate(FILES) if i % 5 == 4], "WRR: 인터리브 배정")
    check(len(wrr["CPU"]) == 400 and len(wrr["CSD"]) == 100, "WRR: 가중치 비율 4:1")
    # 연속 블록이 아님을 명시적으로 확인 — STATIC/MTE 와 갈리는 지점이다
    check(wrr["CPU"] != FILES[:400], "WRR 인데 연속 블록이 나왔다")
    print("  [ok] 샤드 모양 (STATIC 연속 / MTE 만남지점+역순 / WRR 인터리브)")


def main() -> None:
    test_auto_decision_table()
    test_partition_invariants()
    test_partition_shapes()
    print("[PASS] partition algorithm test")


if __name__ == "__main__":
    main()
