#!/usr/bin/env python3
"""CSD 원격 실행 환경 헬스체크.

CurrentProgress §5.4 의 "손으로 돌린 1회" 확인을 스크립트로 고정한 것이다.
저장소 안의 흔적(과거 result.json 등)은 근거가 못 되므로, 전부 지금 실행해서 본다.

확인 항목:
  1. 공유 볼륨이 서버에 마운트돼 있는가
  2. CSD 서브넷 경로와 ping (tap 인터페이스가 내려가면 여기서 잡힌다)
  3. sshpass 설치 여부
  4. 비밀번호 SSH 접속 (공개키는 끄고 확인 — 워커와 같은 조건)
  5. 원격 python3
  6. 원격 numpy / OpenCV (pylibs)
  7. 원격 코드 경로 존재 및 임포트
  8. 공유 볼륨이 CSD 에서도 같은 파일로 보이는가 (토큰 파일 왕복)
  9. 서버 워킹트리와 CSD 코드 사본의 해시 일치
 10. (--offload) 실제 샤드 오프로드 1회 — shared-volume 모드 진입까지 확인

사용법:
  CSD_REMOTE_PASS=<비번> ./run_local_python.sh server/csd_healthcheck.py
  CSD_REMOTE_PASS=<비번> ./run_local_python.sh server/csd_healthcheck.py --offload

실패한 항목이 하나라도 있으면 종료코드 1.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from worker.csd_worker import (  # noqa: E402
    DEFAULT_REMOTE_REPO,
    SHARED_LOCAL_ROOT,
    SHARED_REMOTE_ROOT,
    _ssh_commands,
)

# CSD 사본과 대조할 런타임 코드. 실험·문서·서버 스크립트는 CSD 에서 돌지 않으므로 제외한다.
RUNTIME_ITEMS = ("cli.py", "requirements.txt", "config", "controller",
                 "csd_preprocessor", "worker")
SSH_TIMEOUT = 20


class Report:
    """항목별 OK/FAIL 을 모아 마지막에 한 번에 보여준다.

    첫 실패에서 멈추지 않는 이유: 환경이 여러 군데 동시에 어긋나 있는 경우가 잦아서
    (2026-08-14 처럼) 한 번 돌려 전체 그림을 보는 편이 빠르기 때문이다."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, bool, str]] = []

    def add(self, name: str, ok: bool, detail: str = "") -> bool:
        self.rows.append((name, ok, detail))
        mark = "OK  " if ok else "FAIL"
        print(f"[{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)
        return ok

    @property
    def failed(self) -> list[str]:
        return [n for n, ok, _ in self.rows if not ok]


def run(cmd: list[str], timeout: float = SSH_TIMEOUT) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, f"timeout after {timeout}s"
    except FileNotFoundError as exc:
        return 127, str(exc)
    return p.returncode, (p.stdout + p.stderr).strip()


def sha_tree(root: Path, items) -> dict[str, str]:
    """런타임 파일의 상대경로 → sha256. __pycache__ 와 .pyc 는 건너뛴다."""
    out: dict[str, str] = {}
    for item in items:
        base = root / item
        if not base.exists():
            continue
        files = [base] if base.is_file() else sorted(
            p for p in base.rglob("*")
            if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc")
        for f in files:
            out[str(f.relative_to(root))] = hashlib.sha256(f.read_bytes()).hexdigest()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="CSD 원격 실행 환경 헬스체크")
    ap.add_argument("--host", default=os.environ.get("CSD_REMOTE_HOST", "root@10.2.1.2"))
    ap.add_argument("--repo", default=os.environ.get("CSD_REMOTE_REPO", "") or DEFAULT_REMOTE_REPO)
    ap.add_argument("--pythonpath", default=os.environ.get("CSD_REMOTE_PYTHONPATH",
                                                           f"{SHARED_REMOTE_ROOT}/pylibs"))
    ap.add_argument("--offload", action="store_true",
                    help="실제 샤드 오프로드 1회까지 확인 (이미지 4장, 수 초)")
    args = ap.parse_args()

    password = os.environ.get("CSD_REMOTE_PASS", "").strip()
    if not password:
        print("error: CSD_REMOTE_PASS 가 필요합니다 (비밀번호 인증 전용)", file=sys.stderr)
        return 2

    rep = Report()
    host_only = args.host.split("@")[-1]
    print(f"CSD healthcheck — host={args.host} repo={args.repo}\n")

    # 1. 공유 볼륨 (서버 쪽)
    rc, out = run(["mountpoint", "-q", SHARED_LOCAL_ROOT], timeout=10)
    rep.add(f"shared volume mounted ({SHARED_LOCAL_ROOT})", rc == 0,
            "" if rc == 0 else "마운트돼 있지 않다 — shared-volume 모드가 copy 로 폴백한다")

    # 2. 경로와 ping
    rc, out = run(["ip", "route", "get", host_only], timeout=10)
    rep.add(f"route to {host_only}", rc == 0, out.splitlines()[0] if out else "")
    rc, out = run(["ping", "-c", "2", "-W", "3", host_only], timeout=15)
    rep.add(f"ping {host_only}", rc == 0,
            "" if rc == 0 else "tap 인터페이스가 내려갔을 수 있다 (eno2 와는 무관)")

    # 3. sshpass
    rep.add("sshpass installed", shutil.which("sshpass") is not None)

    ssh, _scp = _ssh_commands(args.host, password)

    # 4. 비밀번호 SSH — 워커와 똑같은 옵션으로 확인한다(공개키는 꺼진 상태)
    rc, out = run(ssh + ["echo ok"])
    ssh_ok = rep.add("ssh (password auth, pubkey off)", rc == 0 and "ok" in out,
                     "" if rc == 0 else out.splitlines()[-1] if out else "")
    if not ssh_ok:
        print("\nSSH 가 안 되면 이후 항목은 볼 수 없다.")
        print(f"실패: {', '.join(rep.failed)}")
        return 1

    # 5. 원격 python3
    rc, out = run(ssh + ["python3 -V"])
    rep.add("remote python3", rc == 0, out)

    # 6. 원격 numpy / OpenCV
    probe = (f"PYTHONPATH={shlex.quote(args.pythonpath)} python3 -c "
             f"'import cv2, numpy; print(cv2.__version__, numpy.__version__)'")
    rc, out = run(ssh + [probe], timeout=60)
    rep.add(f"remote cv2/numpy ({args.pythonpath})", rc == 0,
            out if rc == 0 else "pylibs 가 비었을 수 있다 — ExecutionGuide.md 참조")

    # 7. 원격 코드 경로
    rc, out = run(ssh + [f"test -d {shlex.quote(args.repo)}"])
    repo_ok = rep.add(f"remote repo ({args.repo})", rc == 0,
                      "" if rc == 0 else "경로 없음 — CSD_REMOTE_REPO 확인")
    if repo_ok:
        imp = (f"cd {shlex.quote(args.repo)} && "
               f"PYTHONPATH={shlex.quote(args.pythonpath)}:{shlex.quote(args.repo)} "
               f"python3 -c 'import csd_preprocessor.operations, worker.csd_worker; print(\"import ok\")'")
        rc, out = run(ssh + [imp], timeout=60)
        rep.add("remote package import", rc == 0, out.splitlines()[-1] if out else "")

    # 8. 공유 볼륨이 양쪽에서 같은 파일인가 — 로컬에 쓰고 원격에서 읽는다.
    #    마운트만 있고 매핑이 어긋나면 워커가 조용히 copy 모드로 떨어지므로 실체를 확인한다.
    token = f"healthcheck-{os.getpid()}-{int(time.time())}"
    probe_path = Path(SHARED_LOCAL_ROOT) / f".csd_healthcheck_{os.getpid()}"
    try:
        probe_path.write_text(token, encoding="utf-8")
        remote_probe = f"{SHARED_REMOTE_ROOT}/{probe_path.name}"
        rc, out = run(ssh + [f"cat {shlex.quote(remote_probe)}"])
        rep.add(f"shared volume visible from CSD ({SHARED_LOCAL_ROOT} ↔ {SHARED_REMOTE_ROOT})",
                rc == 0 and out.strip() == token,
                "" if rc == 0 and out.strip() == token
                else "같은 파일로 보이지 않는다 — 워커가 copy 모드로 폴백한다")
    except OSError as exc:
        rep.add("shared volume visible from CSD", False, str(exc))
    finally:
        probe_path.unlink(missing_ok=True)

    # 9. 코드 사본 드리프트 — CSD 는 공유 볼륨의 사본을 실행하므로 워킹트리와 달라질 수 있다.
    if repo_ok:
        local = sha_tree(PROJECT_ROOT, RUNTIME_ITEMS)
        remote_cmd = (f"cd {shlex.quote(args.repo)} && find . -type f ! -path '*/__pycache__/*' "
                      f"! -name '*.pyc' \\( " +
                      " -o ".join(f"-path './{i}' -o -path './{i}/*'" for i in RUNTIME_ITEMS) +
                      " \\) -exec sha256sum {} +")
        rc, out = run(ssh + [remote_cmd], timeout=120)
        if rc != 0:
            rep.add("code copy in sync", False, "원격 해시 계산 실패")
        else:
            remote = {}
            for line in out.splitlines():
                parts = line.split(None, 1)
                if len(parts) == 2:
                    remote[parts[1].strip().lstrip("./")] = parts[0]
            missing = sorted(set(local) - set(remote))
            differ = sorted(k for k in set(local) & set(remote) if local[k] != remote[k])
            detail = ""
            if missing or differ:
                detail = (f"불일치 {len(missing) + len(differ)}개 "
                          f"(없음 {len(missing)}, 다름 {len(differ)}): "
                          + ", ".join((missing + differ)[:4])
                          + (" …" if len(missing) + len(differ) > 4 else "")
                          + " → 고치려면: ./k8s/deploy.sh csd-sync")
            rep.add("code copy in sync", not (missing or differ), detail)

    # 10. 실제 오프로드
    if args.offload:
        ok, detail = run_offload_probe(args, password)
        rep.add("live shard offload", ok, detail)

    print()
    if rep.failed:
        print(f"실패 {len(rep.failed)}개: {', '.join(rep.failed)}")
        return 1
    print(f"전부 통과 ({len(rep.rows)}개 항목)"
          + ("" if args.offload else " — 실제 오프로드까지 보려면 --offload"))
    return 0


def run_offload_probe(args, password: str) -> tuple[bool, str]:
    """공유 볼륨 아래에서 샤드 하나를 실제로 CSD 에 오프로드해 본다.

    shared-volume 모드 진입까지 확인하는 것이 요점이다 — copy 로 폴백하면
    돌긴 돌지만 샤드마다 고정비가 붙어 분할 이득이 사라지기 때문이다."""
    import numpy as np
    from PIL import Image

    work = Path(tempfile.mkdtemp(prefix="csd-healthcheck-", dir=SHARED_LOCAL_ROOT))
    try:
        inp, out, ds = work / "input", work / "out", work / "dataset"
        for d in (inp, out, ds):
            d.mkdir(parents=True)
        rng = np.random.default_rng(0)
        names = []
        for i in range(4):
            arr = rng.integers(0, 255, (120, 160, 3), dtype=np.uint8)
            name = f"probe_{i:03d}.jpg"
            Image.fromarray(arr).save(inp / name, quality=90)
            names.append(name)

        env = dict(os.environ)
        env.update({
            "CSD_REMOTE_HOST": args.host,
            "CSD_REMOTE_PASS": password,
            "CSD_REMOTE_REPO": args.repo,
            "WORKER_TYPE": "CSD",
            "BATCH_ID": "healthcheck",
            "BATCH_MANIFEST_JSON": json.dumps(
                {"batchId": "healthcheck", "worker": "CSD", "files": names}),
            "DATA_PATH": str(inp),
            "OUTPUT_DIR": str(out),
            "DATASET_DIR": str(ds),
            "PREPROCESSING_STEPS": json.dumps(["validate", "resize"]),
        })
        proc = subprocess.run([sys.executable, str(PROJECT_ROOT / "worker" / "csd_worker.py")],
                              cwd=PROJECT_ROOT, env=env, capture_output=True,
                              text=True, timeout=600)
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout).strip().splitlines()
            return False, tail[-1] if tail else f"rc={proc.returncode}"

        result = json.loads((out / "result.json").read_text(encoding="utf-8"))
        off = result.get("offload") or {}
        mode = off.get("mode")
        produced = len(list((ds / "images").glob("*.jpg"))) if (ds / "images").is_dir() else 0
        detail = (f"mode={mode} on {off.get('executedOn')} "
                  f"exec={off.get('execMillis')}ms images={produced}/{len(names)}")
        if produced != len(names):
            return False, detail + " — 산출 이미지 수가 맞지 않는다"
        if mode != "shared-volume":
            return False, detail + " — copy 로 폴백했다 (공유 경로 판정 확인 필요)"
        return True, detail
    except Exception as exc:  # noqa: BLE001 — 어떤 실패든 항목 실패로 보고한다
        return False, f"{type(exc).__name__}: {exc}"
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
