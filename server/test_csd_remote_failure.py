#!/usr/bin/env python3
"""CSD 원격 오프로드 실패 경로 테스트.

헬스체크(server/csd_healthcheck.py)는 환경이 **정상인지**를 본다. 이 테스트는 반대로,
환경이 어긋났을 때 워커가 **제대로 실패하는지**를 본다. 조용히 성공한 척하거나
비밀번호 프롬프트에서 매달리는 쪽이 훨씬 나쁘기 때문이다.

검사하는 네 가지:
  1. 비밀번호 누락        → 즉시 실패 (공개키로 조용히 폴백하지 않는다)
  2. 도달 불가 호스트     → 매달리지 않고 유한 시간 안에 실패
  3. 원격 코드 경로 오류  → CSD 실행 단계에서 실패 (CSD 필요)
  4. 공유 볼륨 미가시     → 실패가 아니라 copy 모드로 폴백 (CSD 필요)

3·4 는 실제 CSD 가 필요하므로 CSD_REMOTE_PASS 가 없으면 SKIP 한다.

  CSD_REMOTE_PASS=<비번> ./run_local_python.sh server/test_csd_remote_failure.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from server.test_e2e_utils import create_synthetic_coco_dataset, make_workspace, run_worker_raw

# 도달 불가 주소. TEST-NET-1(RFC 5737) 은 문서용으로 예약돼 실제 호스트가 없다 —
# 사설망의 남의 장비를 두드리지 않으면서 "응답 없는 호스트"를 만들 수 있다.
UNREACHABLE_HOST = "root@192.0.2.1"
# ssh ConnectTimeout=10 이므로 그 몇 배 안에는 끝나야 한다. 넘으면 매달린 것으로 본다.
HANG_LIMIT_SEC = 90

RESULTS: list[tuple[str, str]] = []


def record(name: str, detail: str) -> None:
    RESULTS.append((name, detail))
    print(f"  [ok] {name} — {detail}")


def base_env(work: Path, batch_id: str, steps=("validate", "resize")) -> tuple[dict, Path]:
    """샤드 하나 분량의 워커 env 를 만든다. 반환값의 Path 는 OUTPUT_DIR."""
    dataset = create_synthetic_coco_dataset(work / batch_id, n_images=3,
                                            include_annotations=False)
    out_dir = work / batch_id / "out"
    ds_dir = work / batch_id / "dataset"
    names = sorted(p.name for p in dataset["images"].glob("*.jpg"))
    env = dict(os.environ)
    env.pop("CSD_REMOTE_PASS", None)
    env.update({
        "WORKER_TYPE": "CSD",
        "BATCH_ID": batch_id,
        "BATCH_MANIFEST_JSON": json.dumps(
            {"batchId": batch_id, "worker": "CSD", "files": names}),
        "DATA_PATH": str(dataset["images"]),
        "OUTPUT_DIR": str(out_dir),
        "DATASET_DIR": str(ds_dir),
        "PREPROCESSING_STEPS": json.dumps(list(steps)),
    })
    return env, out_dir


def case_missing_password(work: Path) -> None:
    env, _ = base_env(work, "nopass")
    env["CSD_REMOTE_HOST"] = "root@10.2.1.2"
    proc = run_worker_raw(env, timeout=60)
    if proc.returncode == 0:
        raise AssertionError("비밀번호 없이 오프로드가 성공했다 — 키 인증으로 새는 경로가 있다")
    if "CSD_REMOTE_PASS" not in proc.stdout:
        raise AssertionError(f"원인을 알 수 없는 실패 메시지:\n{proc.stdout}")
    record("비밀번호 누락", f"rc={proc.returncode}, 메시지에 CSD_REMOTE_PASS 명시")


def case_unreachable_host(work: Path) -> None:
    env, _ = base_env(work, "unreachable")
    env["CSD_REMOTE_HOST"] = UNREACHABLE_HOST
    env["CSD_REMOTE_PASS"] = "dummy"
    started = time.perf_counter()
    proc = run_worker_raw(env, timeout=HANG_LIMIT_SEC + 30)
    elapsed = time.perf_counter() - started
    if proc.returncode == 0:
        raise AssertionError("도달 불가 호스트인데 오프로드가 성공했다")
    if elapsed > HANG_LIMIT_SEC:
        raise AssertionError(f"실패까지 {elapsed:.0f}s — 프롬프트나 재시도에 매달린 것으로 보인다")
    if "remote offload failed" not in proc.stdout:
        raise AssertionError(f"원격 실패로 보고되지 않았다:\n{proc.stdout[-500:]}")
    record("도달 불가 호스트", f"rc={proc.returncode}, {elapsed:.1f}s 만에 실패")


def case_bad_repo(work: Path, password: str) -> None:
    env, _ = base_env(work, "badrepo")
    env["CSD_REMOTE_HOST"] = os.environ.get("CSD_REMOTE_HOST", "root@10.2.1.2")
    env["CSD_REMOTE_PASS"] = password
    env["CSD_REMOTE_REPO"] = "/home/ngd/storage/this-path-does-not-exist"
    proc = run_worker_raw(env, timeout=180)
    if proc.returncode == 0:
        raise AssertionError("없는 코드 경로인데 오프로드가 성공했다")
    if "remote offload failed" not in proc.stdout:
        raise AssertionError(f"원격 실패로 보고되지 않았다:\n{proc.stdout[-500:]}")
    record("원격 코드 경로 오류", f"rc={proc.returncode}, 원격 실행 단계에서 실패")


def case_shared_volume_invisible(work: Path, password: str) -> None:
    """공유 볼륨으로 판정됐지만 CSD 가 그 경로를 못 보는 상황.

    실패시키면 안 된다 — 워커는 copy 모드로 폴백해 결과를 내야 한다. 마운트가
    흔들려도 잡이 죽지 않게 하려는 설계이고, 그 폴백이 실제로 도는지 확인한다."""
    env, out_dir = base_env(work, "novisible")
    env["CSD_REMOTE_HOST"] = os.environ.get("CSD_REMOTE_HOST", "root@10.2.1.2")
    env["CSD_REMOTE_PASS"] = password
    # 작업 디렉터리를 "공유 볼륨"이라고 우기되, CSD 쪽 짝은 없는 경로로 둔다.
    env["CSD_SHARED_LOCAL_ROOT"] = str(work)
    env["CSD_SHARED_REMOTE_ROOT"] = "/home/ngd/storage/not-mounted-here"
    proc = run_worker_raw(env, timeout=300)
    if proc.returncode != 0:
        raise AssertionError(f"copy 모드로 폴백하지 않고 실패했다:\n{proc.stdout[-500:]}")
    if "복사 방식으로 실행합니다" not in proc.stdout:
        raise AssertionError(f"폴백 경고가 없다:\n{proc.stdout[-500:]}")
    result = json.loads((out_dir / "result.json").read_text(encoding="utf-8"))
    mode = (result.get("offload") or {}).get("mode")
    if mode != "copy":
        raise AssertionError(f"폴백 후 mode 가 copy 가 아니다: {mode}")
    if result.get("outputCount") != 3:
        raise AssertionError(f"폴백 실행의 산출 수가 맞지 않는다: {result.get('outputCount')}")
    record("공유 볼륨 미가시", f"경고 후 mode=copy 로 폴백, 3장 처리")


def main() -> None:
    work = make_workspace("csd-remote-failure-")
    password = os.environ.get("CSD_REMOTE_PASS", "").strip()

    print("CSD 없이 확인 가능한 경로:")
    case_missing_password(work)
    case_unreachable_host(work)

    if password:
        print("실제 CSD 대상 경로:")
        case_bad_repo(work, password)
        case_shared_volume_invisible(work, password)
    else:
        print("실제 CSD 대상 경로: SKIP (CSD_REMOTE_PASS 미설정 — 원격 경로 오류·"
              "공유 볼륨 미가시 확인 생략)")

    print(f"[PASS] CSD remote failure test ({len(RESULTS)}개 경로 확인)")


if __name__ == "__main__":
    main()
