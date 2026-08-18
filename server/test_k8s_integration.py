#!/usr/bin/env python3
"""k8s 분산 체인 통합 회귀.

로컬 스모크는 매니저·컨트롤러 로직을 fake client 로 돌리고, 헬스체크는 CSD 만 본다.
그 사이 — 실제 CR 이 생성되고 워커 Job 이 뜨고 stage2 까지 이어지는 경로 — 는
2026-08-18 에 손으로 한 번 확인했을 뿐이었다. 이 스크립트가 그것을 대신한다.

하는 일:
  1. 공유 볼륨에 입력 이미지와 어노테이션을 깔고
  2. PreprocessingWorkload 를 제출해
  3. Succeeded 까지 기다린 뒤
  4. 샤드 분할·CSD 오프로드 모드·stage2 산출물·라벨 출처를 검증하고
  5. CR 을 지워 워커 Job 까지 연쇄 정리되는지 확인한다

전제(없으면 SKIP): kubectl, preprocessing CRD, preprocess-csd 네임스페이스,
컨트롤러·매니저 파드, 공유 볼륨 마운트.

  ./run_local_python.sh server/test_k8s_integration.py
  ./run_local_python.sh server/test_k8s_integration.py --keep   # 산출물 남기기
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from server.csd_healthcheck import RUNTIME_ITEMS, sha_tree  # noqa: E402
from server.test_e2e_utils import create_synthetic_coco_dataset  # noqa: E402
from worker.csd_worker import SHARED_LOCAL_ROOT  # noqa: E402

NAMESPACE = os.environ.get("WATCH_NAMESPACE", "preprocess-csd")
WL_NAME = "regress-wl-001"
N_IMAGES = 12
# 샤드 2개 + stage2 를 CSD 에서 도는 것까지 기다린다. 손으로 잰 완주 시간은 약 40초였고,
# 파드 스케줄과 이미지 기동이 붙으므로 넉넉히 잡는다.
TIMEOUT_SEC = float(os.environ.get("K8S_REGRESS_TIMEOUT", "600"))
POLL_SEC = 5


class Skip(Exception):
    """전제 조건이 없어 검사 자체가 성립하지 않는 경우."""


def kubectl(*args: str, check: bool = True, timeout: float = 60) -> str:
    proc = subprocess.run(["kubectl", "-n", NAMESPACE, *args],
                          capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise AssertionError(f"kubectl {' '.join(args)} 실패 (rc={proc.returncode}): "
                             f"{proc.stderr.strip()}")
    return proc.stdout.strip()


def require_preconditions() -> None:
    if shutil.which("kubectl") is None:
        raise Skip("kubectl 이 없다")
    if not Path(SHARED_LOCAL_ROOT).is_dir():
        raise Skip(f"공유 볼륨({SHARED_LOCAL_ROOT})이 없다")
    probe = subprocess.run(["kubectl", "get", "crd",
                            "preprocessingworkloads.edgeai.keti.re.kr"],
                           capture_output=True, text=True, timeout=30)
    if probe.returncode != 0:
        raise Skip("preprocessing CRD 가 설치돼 있지 않다")
    probe = subprocess.run(["kubectl", "get", "ns", NAMESPACE],
                           capture_output=True, text=True, timeout=30)
    if probe.returncode != 0:
        raise Skip(f"네임스페이스 {NAMESPACE} 가 없다")
    pods = kubectl("get", "pods", "-o",
                   "jsonpath={range .items[*]}{.metadata.labels.app}={.status.phase} {end}")
    running = {p.split("=")[0] for p in pods.split() if p.endswith("=Running")}
    for app in ("preprocess-manager", "preprocess-controller"):
        if app not in running:
            raise Skip(f"{app} 파드가 Running 이 아니다 (배포: ExecutionGuide §5)")


def verify_deployed_code() -> str:
    """배포된 파드가 도는 코드가 워킹트리와 같은지 확인한다.

    컨트롤러·매니저·워커는 볼륨이 아니라 **이미지 안의 코드**를 실행한다. 이미지를
    다시 굽지 않으면 옛 코드가 조용히 도는데, CSD 사본과 달리 이쪽은 감지 장치가
    없었다(2026-08-18 실제로 13일 된 이미지가 배포 직전까지 쓰였다). 이걸 먼저 보지
    않으면 이 회귀는 "무엇을 검증했는지" 자체가 불분명해진다."""
    find_expr = " -o ".join(f"-path './{i}' -o -path './{i}/*'" for i in RUNTIME_ITEMS)
    cmd = (f"cd /app && find . -type f ! -path '*/__pycache__/*' ! -name '*.pyc' "
           f"\\( {find_expr} \\) -exec sha256sum {{}} +")
    out = kubectl("exec", "deploy/preprocess-controller", "--", "sh", "-c", cmd,
                  timeout=180)
    deployed = {}
    for line in out.splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            deployed[parts[1].strip().lstrip("./")] = parts[0]
    local = sha_tree(PROJECT_ROOT, RUNTIME_ITEMS)
    missing = sorted(set(local) - set(deployed))
    differ = sorted(k for k in set(local) & set(deployed) if local[k] != deployed[k])
    if missing or differ:
        sample = ", ".join((missing + differ)[:4])
        raise AssertionError(
            f"배포된 이미지의 코드가 워킹트리와 다르다 "
            f"(없음 {len(missing)}, 다름 {len(differ)}): {sample}\n"
            f"  고치려면: ./k8s/deploy.sh image   "
            f"(재빌드 → containerd 반입 → rollout → 회귀)")
    return f"{len(local)}개 파일 일치"


def stage_dataset(root: Path, n_images: int = N_IMAGES) -> tuple[Path, Path]:
    """공유 볼륨 아래에 합성 데이터셋을 깐다.

    합성 이미지를 쓰는 이유: 회귀는 raw_data 의 내용에 기대면 안 되고, 남의 데이터를
    건드리지 않아야 하며, 12장이면 분할·집계·stage2 를 다 지나가기 때문이다."""
    root.mkdir(parents=True, exist_ok=True)
    ds = create_synthetic_coco_dataset(root, n_images=n_images)
    return ds["images"], ds["annotations"]


def submit(images: Path, labels: Path, out_root: Path, name: str = WL_NAME,
           algorithm: str = "STATIC", total_samples: int = N_IMAGES) -> None:
    manifest = {
        "apiVersion": "edgeai.keti.re.kr/v1alpha1",
        "kind": "PreprocessingWorkload",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "workload": {
                "dataset": {
                    "inputPath": str(images),
                    "outputPath": str(out_root),
                    "labelPath": str(labels),
                    "totalSamples": total_samples,
                },
                "pipelineTemplate": "stage1_raw_ingestion",
                "stage2Template": "stage2_training_preparation",
                "waitForLabels": False,
                "slo": {"type": "THROUGHPUT", "target": 100.0},
                "algorithm": algorithm,
            },
            "placement": {"nodeId": os.environ.get("NODE_HOSTNAME", "gpu-npu-server-02"),
                          "csdId": "csd-01", "cpuCores": 4, "memMb": 8192},
        },
    }
    proc = subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(manifest),
                          capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise AssertionError(f"워크로드 제출 실패: {proc.stderr.strip()}")


def wait_terminal(name: str = WL_NAME) -> str:
    deadline = time.time() + TIMEOUT_SEC
    last = ""
    while time.time() < deadline:
        phase = kubectl("get", "pw", name, "-o", "jsonpath={.status.phase}", check=False)
        stage = kubectl("get", "pj", name, "-o", "jsonpath={.status.stage}", check=False)
        line = f"{phase or '(대기)'} / stage={stage or '-'}"
        if line != last:
            print(f"    {int(time.time() - deadline + TIMEOUT_SEC):>4}s  {line}", flush=True)
            last = line
        if phase in ("Succeeded", "Failed"):
            return phase
        time.sleep(POLL_SEC)
    jobs = kubectl("get", "jobs", "--no-headers", check=False)
    raise AssertionError(f"{TIMEOUT_SEC:.0f}s 안에 종료 상태에 이르지 못했다.\n"
                         f"마지막: {last}\njobs:\n{jobs}")


def verify_artifacts(out_root: Path) -> dict:
    shards_dir = out_root / "_shards"
    if not shards_dir.is_dir():
        raise AssertionError(f"샤드 디렉터리가 없다: {shards_dir}")

    results = {}
    for f in sorted(shards_dir.glob("*/result.json")):
        results[f.parent.name] = json.loads(f.read_text(encoding="utf-8"))
    if len(results) < 2:
        raise AssertionError(f"샤드 결과가 2개 미만이다: {list(results)}")

    csd = [r for r in results.values() if r.get("worker") == "CSD"]
    if not csd:
        raise AssertionError("CSD 샤드 결과가 없다 — 분할이 CPU 로만 갔다")
    for r in csd:
        off = r.get("offload") or {}
        if off.get("mode") not in ("shared-volume", "copy"):
            raise AssertionError(f"CSD 샤드가 실제로 오프로드되지 않았다: offload={off}")

    stage1_total = sum(r.get("outputCount", 0) for k, r in results.items()
                       if not k.endswith("stage2"))
    if stage1_total != N_IMAGES:
        raise AssertionError(f"stage1 산출 수가 입력과 다르다: {stage1_total} != {N_IMAGES}")

    splits = {d: len(list((out_root / d / "images").glob("*.jpg")))
              for d in ("train", "val", "test") if (out_root / d / "images").is_dir()}
    if sum(splits.values()) == 0:
        raise AssertionError("stage2 split 산출물이 없다")

    data_yaml = out_root / "data.yaml"
    if not data_yaml.is_file():
        raise AssertionError("data.yaml 이 없다")
    text = data_yaml.read_text(encoding="utf-8")
    if "label_source: placeholder" in text:
        raise AssertionError("실 라벨을 넘겼는데 임시 라벨로 처리됐다")

    return {"shards": {k: v.get("outputCount") for k, v in results.items()},
            "offload": [(r.get("batchId"), (r.get("offload") or {}).get("mode")) for r in csd],
            "splits": splits}


def cleanup_cr(name: str = WL_NAME) -> None:
    """CR 삭제로 PJ·워커 Job 까지 연쇄 GC 되는지 확인한다(ownerReferences 회귀)."""
    kubectl("delete", "pw", name, "--wait=true", check=False, timeout=120)
    deadline = time.time() + 120
    while time.time() < deadline:
        jobs = kubectl("get", "jobs", "--no-headers", check=False)
        leftover = [l for l in jobs.splitlines() if name in l]
        if not leftover:
            return
        time.sleep(POLL_SEC)
    raise AssertionError(f"CR 삭제 후에도 워커 Job 이 남아 있다:\n{jobs}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="산출물과 CR 을 지우지 않는다")
    args = ap.parse_args()

    try:
        require_preconditions()
    except Skip as exc:
        print(f"[SKIP] k8s integration test — {exc}")
        return

    print(f"  배포 코드 대조: {verify_deployed_code()}")

    work = Path(SHARED_LOCAL_ROOT) / "csd_regress" / WL_NAME
    shutil.rmtree(work, ignore_errors=True)
    out_root = work / "out"
    images, labels = stage_dataset(work)
    print(f"  입력 {N_IMAGES}장 → {images}")

    # 이전 실행이 남긴 CR 이 있으면 먼저 치운다(회귀는 반복 실행돼야 한다).
    kubectl("delete", "pw", WL_NAME, "--ignore-not-found", check=False, timeout=120)

    submit(images, labels, out_root)
    print(f"  제출: PreprocessingWorkload/{WL_NAME}")
    phase = wait_terminal()
    if phase != "Succeeded":
        raise AssertionError(f"워크로드가 {phase} 로 끝났다 "
                             f"(kubectl -n {NAMESPACE} logs deploy/preprocess-controller)")

    summary = verify_artifacts(out_root)

    if not args.keep:
        cleanup_cr()
        shutil.rmtree(work, ignore_errors=True)

    print("[PASS] k8s integration test")
    print(f"  샤드 산출: {summary['shards']}")
    print(f"  CSD 오프로드: {summary['offload']}")
    print(f"  split: {summary['splits']}")
    print(f"  정리: {'생략(--keep)' if args.keep else 'CR·워커 Job 연쇄 GC 확인'}")


if __name__ == "__main__":
    main()
