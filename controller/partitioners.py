"""Shard partitioners for the preprocessing manager/engine.

CR spec.partition_info.algorithm 으로 선택된다 (STATIC, MTE, WRR).
새 알고리즘 추가 방법: Partitioner 를 상속해 plan() 구현 후 PARTITIONERS 에 등록.

MTE (Moving Towards Each other):
  CPU/CSD 의 사전 측정 throughput(단위시간당 샘플 수) 비율로 만남 지점을 산출.
  CPU 는 Head(앞)에서, CSD 는 Tail(뒤)에서 중앙을 향해 진행하며, 양측 완료 시점이
  동기화되도록 경계를 잡는다: split = N * tp_cpu / (tp_cpu + tp_csd).
  분할 인덱스 계산만으로 동작 → 오버헤드 최소, 소규모 데이터셋에 적합.

WRR (Weighted Round Robin):
  처리 성능 비율을 정수 가중치(w_cpu:w_csd)로 부여하고, 라운드마다 CPU 에 w_cpu 개,
  CSD 에 w_csd 개를 순차 배치해 처리 큐를 구성. 양측이 자신의 샘플을 병렬 소비.
  MTE 대비 오버헤드는 높으나 가중치 변경 시 재분배가 용이 → 대규모 데이터셋에 적합.

plan() 은 결정적(deterministic)이다 — 같은 (files, info) 입력이면 매니저와 실행엔진이
독립적으로 호출해도 동일한 샤드가 나온다.
"""

import os
from abc import ABC, abstractmethod
from math import gcd
from typing import Dict, List, Optional, Tuple

# --------------------------------------------------------------------------- #
# 알고리즘 자동 선택 정책 (spec.workload.algorithm 미지정 또는 AUTO)
#
#   소규모 데이터셋 또는 CPU/CSD 성능 차이가 작으면  → MTE
#   대규모 데이터셋이고 성능 비율 편차가 크면        → WRR
#
# 근거(실측):
#  - 소규모: WRR 은 비율을 정수 가중치로 양자화하므로 라운드(cycle) 길이가 N 에
#    비해 크면 마지막 부분 사이클이 분할을 크게 왜곡한다. MTE 는 인덱스 계산이라
#    N 과 무관하게 정확하다.
#  - 편차 큼: 느린 쪽 샤드가 작아지는데, MTE 는 그 몫을 **연속 블록**(tail)으로
#    준다. 파일 비용이 데이터셋 순서에 따라 다르면 작은 연속 블록은 전체 평균에서
#    치우친다 — COCO 30장 기준 tail 3~5장은 평균 대비 -13~14%, 같은 크기의
#    인터리브 표본은 +2~5% 였다. 샤드가 커지면(비율 ~2, 10/30장) -4.6% vs +6.8%
#    로 차이가 사라진다. 그래서 교차점을 비율 3 부근으로 잡았다.
# 두 조건이 동시에 성립할 때(소규모 + 편차 큼)는 MTE 가 이긴다 — 양자화 왜곡이
# 표본 치우침보다 크고, MTE 는 오버헤드도 낮다.
# --------------------------------------------------------------------------- #
AUTO_SMALL_DATASET = int(os.environ.get("AUTO_SMALL_DATASET", "100"))
AUTO_BALANCED_RATIO = float(os.environ.get("AUTO_BALANCED_RATIO", "3.0"))
# WRR 라운드 길이 상한 — 실측 처리량비는 기약분수가 안 되므로(예 13474:2261)
# 제한하지 않으면 cycle 이 수천이 되어 느린 쪽에 파일이 한 장도 안 간다.
WRR_MAX_CYCLE = int(os.environ.get("WRR_MAX_CYCLE", "20"))


class PartitionError(Exception):
    """Raised when a shard plan cannot be produced."""


# --------------------------------------------------------------------------- #
# 파라미터 해석 helpers (매니저: workload spec 값 → info / 엔진: partition_info)
# --------------------------------------------------------------------------- #
def resolve_throughput(info: dict) -> Tuple[float, float]:
    """사전 측정 throughput (cpu, csd)을 info 에서 해석한다."""
    basis = info.get("basis") or {}
    tp = basis.get("throughput") or {}
    cpu = info.get("cpu_throughput") or tp.get("cpu")
    csd = info.get("csd_throughput") or tp.get("csd")
    if not cpu or not csd or cpu <= 0 or csd <= 0:
        raise PartitionError(
            "MTE/WRR requires pre-measured throughput "
            "(workload.throughput.cpu/csd or partition_info.basis.throughput)")
    return float(cpu), float(csd)


def approx_ratio_weights(cpu_share: float, csd_share: float,
                         max_cycle: int = 0) -> Tuple[int, int]:
    """비율을 라운드 길이가 max_cycle 이하인 정수 가중치로 근사한다.

    기약분수(gcd)만으로는 부족하다 — 실측 처리량비는 서로소인 경우가 대부분이라
    13.474:2.261 → 13474:2261 (cycle 15735) 처럼 커진다. 그러면 30장짜리 잡에서
    i % 15735 < 13474 이 항상 참이라 CSD 에 한 장도 안 간다(실제로 그랬다).

    분모를 1..max_cycle 로 훑으며 상대오차가 가장 작은 근사를 고른다.
    """
    max_cycle = max(2, max_cycle or WRR_MAX_CYCLE)
    if cpu_share <= 0 or csd_share <= 0:
        raise PartitionError(f"weights must be positive, got {cpu_share}:{csd_share}")

    target = cpu_share / csd_share
    # 비율 자체가 상한보다 크면(예 50:1) 상한 안에서는 표현이 불가능하다.
    # 그때 잘라내면 느린 쪽에 과도한 몫이 가므로, 최소 라운드 길이를 허용한다.
    if target >= max_cycle - 1:
        return max(1, round(target)), 1
    if 1 / target >= max_cycle - 1:
        return 1, max(1, round(1 / target))

    best, best_err = None, None
    for ws in range(1, max_cycle):
        wc = max(1, round(target * ws))
        if wc + ws > max_cycle:
            break
        err = abs(wc / ws - target) / target
        if best_err is None or err < best_err:
            best, best_err = (wc, ws), err
            if err == 0:
                break
    if best is None:                       # 비율이 극단적이라 상한 안에 못 넣음
        best = (max(1, max_cycle - 1), 1)  # → 느린 쪽에 최소 1장은 배정한다
    g = gcd(*best) or 1
    return best[0] // g, best[1] // g


def resolve_weights(info: dict) -> Tuple[int, int]:
    """WRR 정수 가중치 (w_cpu, w_csd)를 해석한다.

    우선순위: 명시적 weights → throughput 비율에서 근사 → cpu_ratio 에서 근사.
    라운드 길이는 WRR_MAX_CYCLE(또는 info.wrr_max_cycle)로 제한한다.
    """
    basis = info.get("basis") or {}
    w = basis.get("weights") or {}
    wc = info.get("cpu_weight") or w.get("cpu")
    ws = info.get("csd_weight") or w.get("csd")
    if wc and ws:
        if wc < 1 or ws < 1:
            raise PartitionError(f"WRR weights must be >= 1, got {wc}:{ws}")
        return int(wc), int(ws)

    max_cycle = int(info.get("wrr_max_cycle") or basis.get("wrr_max_cycle") or 0)
    try:
        cpu_tp, csd_tp = resolve_throughput(info)
        return approx_ratio_weights(cpu_tp, csd_tp, max_cycle)
    except PartitionError:
        ratio = info.get("cpu_ratio")
        if ratio is None or not (0 < float(ratio) < 1):
            raise PartitionError(
                "WRR requires weights, throughput, or cpu_ratio in (0,1)")
        return approx_ratio_weights(float(ratio), 1.0 - float(ratio), max_cycle)


def select_algorithm(n_files: int, cpu_tp: Optional[float] = None,
                     csd_tp: Optional[float] = None,
                     small_dataset: int = 0,
                     balanced_ratio: float = 0.0) -> Tuple[str, dict]:
    """워크로드 특성 → 분할 알고리즘. (algorithm, 판단근거) 를 돌려준다.

    근거는 status.partition_plan.basis 에 그대로 실려 감사 가능해야 하므로
    임계값과 실제 값을 함께 담는다.
    """
    small_dataset = small_dataset or AUTO_SMALL_DATASET
    balanced_ratio = balanced_ratio or AUTO_BALANCED_RATIO

    ratio = None
    if cpu_tp and csd_tp and cpu_tp > 0 and csd_tp > 0:
        ratio = round(max(cpu_tp, csd_tp) / min(cpu_tp, csd_tp), 4)

    decision = {
        "mode": "auto",
        "total_files": n_files,
        "perf_ratio": ratio,
        "thresholds": {"small_dataset": small_dataset,
                       "balanced_ratio": balanced_ratio},
    }
    if n_files <= small_dataset:
        decision["reason"] = (f"소규모 데이터셋 ({n_files} <= {small_dataset}장) — "
                              f"WRR 의 정수 가중치 양자화가 분할을 왜곡한다")
        return "MTE", decision
    if ratio is None:
        decision["reason"] = "처리량 실측값이 없어 보수적으로 MTE (오버헤드 최소)"
        return "MTE", decision
    if ratio <= balanced_ratio:
        decision["reason"] = (f"CPU/CSD 성능 차이가 작음 (비율 {ratio} <= "
                              f"{balanced_ratio}) — 연속 분할로 충분하다")
        return "MTE", decision
    decision["reason"] = (f"대규모 데이터셋({n_files}장) + 성능 비율 편차 큼 "
                          f"({ratio} > {balanced_ratio}) — 느린 쪽 샤드가 작아 "
                          f"연속 블록은 데이터셋 평균에서 치우친다. 인터리브 배정 사용")
    return "WRR", decision


# --------------------------------------------------------------------------- #
def _clamp(split: int, n: int) -> int:
    return max(0, min(split, n))


def _single_target(files: List[str], targets: List[str]) -> Dict[str, List[str]]:
    return {targets[0]: list(files)}


def _check_targets(targets: List[str]):
    if not targets:
        raise PartitionError("execution_targets is empty")
    if len(targets) > 1 and (len(targets) != 2 or set(targets) != {"CPU", "CSD"}):
        raise PartitionError(f"supported targets are [CPU, CSD], got {targets}")


class Partitioner(ABC):
    name = "base"

    @abstractmethod
    def plan(self, files: List[str], targets: List[str], info: dict) -> Dict[str, List[str]]:
        """Return {target: file shard}. 모든 파일이 정확히 한 번씩 배정된다.

        Args:
            files: 정렬된 입력 파일명 목록
            targets: spec.execution_targets (예: ["CPU", "CSD"])
            info: spec.partition_info (cpu_ratio, split_index, basis...)
        """


class StaticPartitioner(Partitioner):
    """정적 분할: split_index 가 있으면 그 경계로, 없으면 cpu_ratio 로 경계 산출."""

    name = "STATIC"

    def plan(self, files, targets, info):
        _check_targets(targets)
        if len(targets) == 1:
            return _single_target(files, targets)
        split = int(info.get("split_index") or 0)
        if split <= 0:
            cpu_ratio = float(info.get("cpu_ratio", 0.5))
            if not (0.0 <= cpu_ratio <= 1.0):
                raise PartitionError(f"cpu_ratio out of range: {cpu_ratio}")
            split = round(len(files) * cpu_ratio)
        split = _clamp(split, len(files))
        return {"CPU": list(files[:split]), "CSD": list(files[split:])}


class MTEPartitioner(Partitioner):
    """Moving Towards Each other — throughput 비율로 만남 지점 산출.

    split = N * tp_cpu / (tp_cpu + tp_csd) 이면
    Head(CPU) 처리시간 split/tp_cpu ≈ Tail(CSD) 처리시간 (N-split)/tp_csd
    → 양측이 중앙에서 동시에 만난다. CSD 샤드는 Tail 에서 중앙 방향 순서로 배정.
    """

    name = "MTE"

    def plan(self, files, targets, info):
        _check_targets(targets)
        if len(targets) == 1:
            return _single_target(files, targets)
        split = int(info.get("split_index") or 0)
        if split <= 0:
            cpu_tp, csd_tp = resolve_throughput(info)
            split = round(len(files) * cpu_tp / (cpu_tp + csd_tp))
        split = _clamp(split, len(files))
        head = list(files[:split])                 # CPU: 앞 → 중앙
        tail = list(reversed(files[split:]))       # CSD: 뒤 → 중앙
        return {"CPU": head, "CSD": tail}


class WRRPartitioner(Partitioner):
    """Weighted Round Robin — 가중치 비율로 샘플을 순차 배치해 처리 큐 구성.

    라운드(길이 w_cpu + w_csd)마다 앞 w_cpu 개는 CPU, 뒤 w_csd 개는 CSD 에 배정.
    비연속(interleaved) 샤드 — 가중치만 바꾸면 재분배되므로 재조정이 쉽다.
    """

    name = "WRR"

    def plan(self, files, targets, info):
        _check_targets(targets)
        if len(targets) == 1:
            return _single_target(files, targets)
        wc, ws = resolve_weights(info)
        cycle = wc + ws
        shards = {"CPU": [], "CSD": []}
        for i, f in enumerate(files):
            shards["CPU" if i % cycle < wc else "CSD"].append(f)
        return shards


PARTITIONERS = {
    cls.name: cls for cls in (StaticPartitioner, MTEPartitioner, WRRPartitioner)
}


def get_partitioner(name: str) -> Partitioner:
    if name not in PARTITIONERS:
        raise PartitionError(f"unknown algorithm '{name}'. Available: {sorted(PARTITIONERS)}")
    return PARTITIONERS[name]()
