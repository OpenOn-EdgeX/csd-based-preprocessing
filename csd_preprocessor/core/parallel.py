"""이미지 단위 작업의 파일 병렬 실행.

전처리 스텝은 대부분 "이미지 한 장씩 독립 처리 → 결과 취합" 구조라 파일 단위로
병렬화할 수 있다. OpenCV 가 연산 내부에서 이미 멀티스레드를 쓰지만 그것만으로는
코어를 다 못 쓴다 — 디코딩·파이썬 레벨 처리가 섞여 있어 한 장 처리의 병렬도는
CSD(4코어)에서 2.13× 에 그쳤다. 여러 장을 동시에 흘리면 남는 코어가 채워진다.

실측(30장, 0.28MP, stage1 6스텝의 이미지 단위 작업):

    CSD(ARM 4코어)   순차 121.1ms/장 → 풀4  36.5ms/장  (3.3×)
    CPU(컨테이너 2코어) 순차  20.6ms/장 → 풀2  10.4ms/장  (2.0×)

순서 보장: map_images 는 입력 순서대로 결과를 돌려준다. 각 연산은 기존과 동일한
순차 취합 코드를 그대로 쓰므로 누적 순서가 같고, 결과도 비트 단위로 같다.
(부동소수 누적은 순서에 따라 달라질 수 있어 취합은 반드시 순차로 둔다.)

스레드 수는 컨테이너에 실제로 할당된 CPU 를 따른다 — os.cpu_count() 는 호스트
코어를 반환해서 cpu limit 2 인 워커 파드가 8스레드를 띄우는 사고가 난다.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, List

logger = logging.getLogger(__name__)

# 상한 — 이보다 늘려도 디스크/메모리에 먼저 막힌다.
MAX_THREADS = int(os.environ.get("WORKER_THREADS_MAX", "8"))
_cached_threads = None
_cv2_configured = False


def _cgroup_cpu_quota() -> float:
    """컨테이너에 할당된 CPU 코어 수. 알 수 없으면 0.0."""
    try:                                   # cgroup v2: "<quota> <period>" 또는 "max <period>"
        raw = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if raw and raw[0] != "max":
            return float(raw[0]) / float(raw[1])
    except (OSError, ValueError, IndexError):
        pass
    try:                                   # cgroup v1
        quota = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text().strip())
        period = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text().strip())
        if quota > 0 and period > 0:
            return quota / period
    except (OSError, ValueError):
        pass
    return 0.0


def worker_threads() -> int:
    """이미지 병렬 처리에 쓸 스레드 수.

    우선순위: WORKER_THREADS 환경변수 → cgroup CPU 할당량 → os.cpu_count().
    """
    global _cached_threads
    if _cached_threads is not None:
        return _cached_threads

    explicit = os.environ.get("WORKER_THREADS", "").strip()
    if explicit:
        try:
            _cached_threads = max(1, min(MAX_THREADS, int(explicit)))
            return _cached_threads
        except ValueError:
            logger.warning(f"WORKER_THREADS='{explicit}' 를 해석할 수 없어 자동 판정합니다")

    quota = _cgroup_cpu_quota()
    cores = int(quota) if quota >= 1 else (os.cpu_count() or 1)
    _cached_threads = max(1, min(MAX_THREADS, cores))
    return _cached_threads


def _configure_cv2(threads: int):
    """풀을 쓸 때는 OpenCV 내부 스레드를 1로 내려 오버서브스크립션을 막는다."""
    global _cv2_configured
    if _cv2_configured or threads <= 1:
        return
    try:
        import cv2
        cv2.setNumThreads(1)
        _cv2_configured = True
    except Exception as e:                 # cv2 가 없거나 빌드가 스레드 미지원
        logger.debug(f"cv2.setNumThreads 설정 생략: {e}")


def map_images(items: Iterable[Any], fn: Callable[[Any], Any],
               threads: int = 0) -> List[Any]:
    """items 를 fn 으로 병렬 처리하고 **입력 순서대로** 결과를 반환한다.

    fn 은 이미지 한 장에 대한 순수 작업이어야 한다(공유 상태 변경 금지).
    누적·집계는 호출부에서 반환된 리스트를 순차 순회하며 수행한다.
    """
    items = list(items)
    threads = threads or worker_threads()
    if threads <= 1 or len(items) <= 1:
        return [fn(x) for x in items]
    _configure_cv2(threads)
    with ThreadPoolExecutor(max_workers=min(threads, len(items))) as pool:
        return list(pool.map(fn, items))   # map 은 입력 순서를 유지한다
