# CSD 전처리 개발 현황

최종 갱신: 2026-08-18 (k8s 배포·헬스체크·데이터셋 복구 + 3층 회귀 + 배포 스크립트 + 분할 알고리즘 재확인)

이 문서는 논문용 요약이 아니라 현재 코드 기준의 개발 현황 문서다.
구현 범위, 최근 확인 결과, 부족한 부분, 다음 작업만 정리한다.

관련 문서:
- 구조 설명: [ARCHITECTURE.md](ARCHITECTURE.md)
- 실행 가이드: [ExecutionGuide.md](ExecutionGuide.md)
- 진입점: [cli.py](cli.py)

---

## 1. 현재 판단

현재 단계는 **운영형 PoC** 다.

의미는 다음과 같다.
- 분산 전처리 메인 경로가 구현돼 있다
- 핵심 전처리 연산이 구현돼 있다
- 로컬에서 성공/실패 경로를 포함한 최소 회귀 확인이 가능하다
- **실 CSD 오프로드가 shared-volume/copy 두 방식 모두 재확인됐다 (2026-08-18)**
- **k8s 분산 체인이 현재 클러스터에서 실제로 완주했다 (2026-08-18)** —
  PreprocessingWorkload → PreprocessingJob → CPU/CSD 워커 Job → stage2 → Succeeded

즉 아이디어 단계는 지났지만, 안정화된 제품 단계는 아니다.
설계한 경로는 전부 한 번씩 실제로 돌았고, 남은 약점은 **그 확인이 자동화돼 있지 않다는 것**이다.

---

## 2. 구현된 것

### 2.1 메인 분산 경로

현재 코드에 구현된 메인 흐름:

`PreprocessingWorkload -> PreprocessingJob -> CPU/CSD 워커 Job -> 샤드 집계 -> 라벨 대기 -> stage2`

핵심 파일:
- [controller/preprocess_manager.py](controller/preprocess_manager.py)
- [controller/preprocess_controller.py](controller/preprocess_controller.py)
- [worker/csd_worker.py](worker/csd_worker.py)

### 2.2 CLI 진입점

주 진입점:
- [cli.py](cli.py)

현재 노출된 주요 명령:
- `run`
- `status`
- `ops`
- `watch`
- `mock-verify`

### 2.3 전처리 연산

[csd_preprocessor/operations](csd_preprocessor/operations)에 다음 연산이 구현돼 있다.

- `validate`
- `deduplicate`
- `filter_quality`
- `resize`
- `normalize`
- `convert_annotation`
- `augment`
- `split`
- `statistics`
- `tile`

### 2.4 분할 및 처리량 피드백

컨트롤러 쪽에 다음 로직이 있다.
- 분할 계획 수립
- 알고리즘 선택
- 처리량 프로파일 누적
- 측정 처리량의 다음 잡 재사용

핵심 파일:
- [controller/partitioners.py](controller/partitioners.py)
- [controller/throughput_profile.py](controller/throughput_profile.py)

### 2.5 CSD 오프로드

워커가 CSD 실행 방식을 경로를 보고 자동 선택한다.

| mode | 조건 | 동작 |
|---|---|---|
| `shared-volume` | 입출력·라벨이 모두 공유 OCFS2 파티션(`/mnt/newport_1` ↔ CSD `/home/ngd/storage`) 아래 | 복사 없이 CSD 가 제자리에서 처리 |
| `copy` | 그 외 (노드 로컬 등) | scp 로 밀어넣고 결과 회수 |

접속은 **비밀번호(sshpass) 인증 전용**이다. 공개키 인증은 `PubkeyAuthentication=no` 로
명시적으로 끈다 — 원격 실행 주체마다 키를 심어야 하는 부담이 있고, 켜 두면 키가 있는
자리에서만 조용히 성공해 환경마다 다르게 동작하기 때문이다.
`CSD_REMOTE_PASS`(k8s 에서는 `csd-credentials` 시크릿)가 없으면 워커가 즉시 실패한다.
환경변수 표는 [worker/README.md](worker/README.md) 참조.

### 2.6 배포 자산

배포 및 패키징 자산이 있다.
- [k8s](k8s)
- [Dockerfile](Dockerfile)
- [deviceplugin](deviceplugin)

### 2.7 실험 코드

실험 및 분석 코드는 런타임 코드와 별도로 존재한다.
- [experiments/main](experiments/main)
- [experiments/corebudget](experiments/corebudget)
- [experiments/pilot](experiments/pilot)

이 구조는 분석에는 유리하지만, 런타임 코드와 실험 산출물이 한 리포에 섞여 있다는 뜻이기도 하다.

### 2.8 로컬 실행/회귀 자산

로컬 표준 실행 및 회귀 자산이 있다.
- [run_local_python.sh](run_local_python.sh)
- [run_smoke_tests.sh](run_smoke_tests.sh)
- [server](server)

---

## 3. 최근 확인한 것

확인 기준일: **2026-08-18** (아래는 전부 그날 실측)

### 3.1 로컬 실행 환경

- 로컬 표준 실행기: `./run_local_python.sh` (`/root/miniconda3/bin/python`)
- `cv2`, `kubernetes`, `yaml`, `PIL`, `imagehash`, `numpy` import 성공
- `./run_local_python.sh cli.py ops` 실행 성공

### 3.2 경량 테스트

`./run_smoke_tests.sh` 로 아래 8종 **전부 통과**.

- [server/test_trigger_flow.py](server/test_trigger_flow.py)
- [server/test_mock_verify.py](server/test_mock_verify.py)
- [server/test_stage1_smoke.py](server/test_stage1_smoke.py)
- [server/test_stage2_smoke.py](server/test_stage2_smoke.py)
- [server/test_worker_local_fallback.py](server/test_worker_local_fallback.py)
- [server/test_manager_controller_smoke.py](server/test_manager_controller_smoke.py)
- [server/test_stage2_missing_labels_failure.py](server/test_stage2_missing_labels_failure.py)
- [server/test_invalid_template_failure.py](server/test_invalid_template_failure.py)
- [server/test_csd_remote_failure.py](server/test_csd_remote_failure.py)

확인 범위: trigger 흐름 / mock worker / stage1·stage2 로컬 스모크 /
worker non-CSD fallback / manager·controller 상태 전이 / 대표 실패 경로 2개 /
**CSD 원격 실패 경로 4종**(§3.9).

### 3.3 실 CSD 연결 및 런타임 (라이브 확인)

- 경로: `tap1` (10.2.1.1/24) → **10.2.1.2 ping 약 1ms**. 물리 NIC `eno2` 와 무관하다.
- **비밀번호 인증 접속 확인** — CSD `sshd_config` 에 `PasswordAuthentication yes`,
  공개키를 끈 상태(`PubkeyAuthentication=no`)에서도 접속 성공
- CSD: Ubuntu, aarch64, Python 3.8.10
- `PYTHONPATH=/home/ngd/storage/pylibs` 로 **cv2 5.0.0 + numpy 1.24.4 import 성공**
- CSD 측 코드 경로: **`/home/ngd/storage/csd_preprocessing`**
  (2026-08-14 재구축 때 `csd-based-preprocessing` 에서 바뀜 — 옛 경로는 없다)
- 공유 OCFS2 두 파티션(`/mnt/newport_1`, `/mnt/newport_2`) 마운트 정상.
  `/mnt/newport_1` 은 CSD 의 `/home/ngd/storage` 와 같은 실체다.

### 3.4 실 CSD 오프로드 재검증 (라이브 실행)

`worker/csd_worker.py` 를 실제 CSD 대상으로 돌려 두 방식 모두 성공을 확인했다.
**비밀번호 인증**(공개키 비활성)으로 실행했다.

| mode | 입력 위치 | 결과 |
|---|---|---|
| `shared-volume` | `/mnt/newport_1` 아래 | 5장 처리, exec 1.75s, push 6ms / pull 0ms |
| `copy` | 공유 볼륨 밖 | 5장 처리, push 1.0s / exec 1.6s / pull 0.6s |

`result.json.offload.executedOn = root@10.2.1.2` 로 CSD 실행이 기록된다.
`CSD_REMOTE_PASS` 를 빼고 돌리면 `Missing required environment variable` 로 즉시 실패한다.
8/14 재구축 이후 **처음으로 확인된 실 오프로드 성공**이다.

### 3.5 헬스체크 스크립트

§3.3~3.4 를 손으로 확인하던 것을 [server/csd_healthcheck.py](server/csd_healthcheck.py)
로 고정했다. 11개 항목(공유 볼륨 마운트 / 경로·ping / sshpass / 비밀번호 SSH /
원격 python·cv2 / 원격 repo·임포트 / 공유 볼륨 왕복 / **코드 사본 해시 대조**)을 보고,
`--offload` 를 주면 실제 샤드 오프로드 1회까지 돌려 `shared-volume` 모드 진입을 확인한다.

```bash
CSD_REMOTE_PASS=<비번> ./run_local_python.sh server/csd_healthcheck.py --offload
```

2026-08-18 기준 전 항목 통과. 잘못된 `--repo` 를 주면 그 항목만 FAIL 로 잡히고,
워킹트리를 고치고 CSD 사본을 안 올리면 "code copy in sync" 가 실패한다(실제로 잡혔다).

### 3.6 k8s 분산 체인 완주 (라이브 배포)

빈 클러스터(2026-08-18 01:44 재생성)에 처음부터 세워 샘플 워크로드를 완주시켰다.
절차는 [ExecutionGuide.md](ExecutionGuide.md) §5 에 그대로 정리했다.

세운 것: 이미지 재빌드·containerd 반입 → 네임스페이스/쿼터/RBAC → CRD 2종 →
kubeconfig 재발급 → `csd-credentials` 시크릿 → csd-device-plugin → 컨트롤러·매니저.

결과 (`demo-wl-001`, 입력 50장, STATIC 분할):

| 단계 | 결과 |
|---|---|
| 분할 | CPU 25 / CSD 25 |
| CPU 샤드 | Complete 5s |
| CSD 샤드 | Complete — `mode=shared-volume`, CSD 내부 exec 3.07s, push 4ms / pull 0ms |
| stage2 | Complete 8s — CSD 에서 실행, `mode=shared-volume`, exec 3.57s |
| 라벨 | `provided` (임시 라벨 아님) — 실 COCO 어노테이션 변환 성공 |
| 산출 | train 48 / val 7 / test 3, `data.yaml` nc=80 |
| CR 상태 | PreprocessingWorkload · PreprocessingJob 모두 `Succeeded` |

배포 중 걸린 것 3가지와 조치:
- CSD 샤드 파드가 `Pending` (`Insufficient keti.re.kr/csd`) → csd-device-plugin 미배포였다
- 구 RBAC(`/root/preprocess-isolation/phase2/`)에 `edgeai.keti.re.kr` 그룹이 없어
  컨트롤러가 자기 CR 을 못 읽는 상태였다 → `k8s/namespace-rbac.yaml` 로 저장소에 편입
- kubeconfig 가 소멸한 구 클러스터(10.0.4.230)를 가리켰다 → SA 토큰으로 재발급

### 3.7 데이터셋 복구

8/14 OCFS2 재포맷으로 소실된 COCO val2017 을 공유 볼륨에 복구했다.
`images.cocodataset.org` 는 현재 접근이 막혀 있어(HTTP 000, 일반 인터넷은 정상)
`server/download_coco_sample.py` 대신 노드에 있던 사본
(`/root/models/warboy-vision-models/datasets/coco`)에서 가져왔다.

`/mnt/newport_1/csd_preprocessing/raw_data` = **이미지 5,000장 + 어노테이션 36,781건**
(80 카테고리, 807MB). CSD 쪽에서도 같은 파일로 보인다.

### 3.8 기타 확인

- 도커 이미지: `csd-preprocessor:latest` 를 현재 코드로 재빌드(865MB, 이전 1.13GB —
  `.dockerignore` 추가로 demo_data·experiments·문서·가중치 제외). `csd-device-plugin:latest` 는 기존 것 사용.

### 3.9 CSD 원격 실패 경로 회귀

헬스체크는 환경이 **정상인지**를 본다. [server/test_csd_remote_failure.py](server/test_csd_remote_failure.py)
는 반대로 어긋났을 때 워커가 **제대로 실패하는지**를 본다. 조용히 성공한 척하거나
프롬프트에서 매달리는 쪽이 훨씬 나쁘기 때문이다.

| 경로 | 기대 동작 | 결과 |
|---|---|---|
| 비밀번호 누락 | 즉시 실패(키로 새지 않음) | rc=1, 메시지에 `CSD_REMOTE_PASS` |
| 도달 불가 호스트 | 매달리지 않고 유한 시간 내 실패 | rc=1, 4.2s |
| 원격 코드 경로 오류 | 원격 실행 단계에서 실패 | rc=1 |
| 공유 볼륨 미가시 | **실패가 아니라** copy 폴백 | 경고 후 `mode=copy`, 3장 처리 |

뒤 두 건은 실제 CSD 가 필요해 `CSD_REMOTE_PASS` 가 없으면 스스로 SKIP 한다 —
그래서 `run_smoke_tests.sh` 에 넣어도 오프라인에서 묶음이 통째로 통과한다.

이 테스트를 쓰다 실제 결함이 하나 나왔다: `DEFAULT_REMOTE_REPO` 를 공유 볼륨 루트에서
파생시켜 둬서 **데이터 마운트 지점을 바꾸면 코드 경로까지 따라 움직였다**.
둘은 독립된 설정이므로 리터럴 기본값으로 분리했다(`DEFAULT_REMOTE_WORKDIR` 도 같이).

### 3.10 k8s 통합 회귀

[server/test_k8s_integration.py](server/test_k8s_integration.py) 가 §3.6 의 손 확인을
대신한다. 공유 볼륨에 합성 데이터셋 12장을 깔고 워크로드를 제출해 `Succeeded` 까지
기다린 뒤, 샤드 분할·CSD 오프로드 모드·stage1 산출 수·stage2 split·라벨 출처를
검증하고, 마지막에 CR 을 지워 **워커 Job 까지 연쇄 GC** 되는지 본다.

전제(kubectl / CRD / 네임스페이스 / 컨트롤러·매니저 Running / 공유 볼륨)가 없으면 SKIP 한다.
합성 데이터를 쓰는 이유는 회귀가 `raw_data` 내용에 기대면 안 되고 남의 데이터를
건드리지 않아야 하기 때문이다.

2026-08-18 실행 결과: 약 25초에 완주, CPU 6 / CSD 6 / stage2 12,
두 CSD 잡 모두 `shared-volume`, split train 12 / val 2, 연쇄 GC 확인.

### 3.11 배포 이미지 드리프트 감지

컨트롤러·매니저·워커는 볼륨이 아니라 **이미지 안의 코드**를 실행한다. CSD 사본은
헬스체크가 잡지만 이쪽은 감지 장치가 없어서, 실제로 13일 된 이미지가 배포 직전까지
쓰였다(§6.3). k8s 회귀가 시작할 때 실행 중인 컨트롤러 파드 안의 런타임 파일 해시를
워킹트리와 대조하고, 다르면 재빌드 명령을 띄우며 멈춘다.

도입 직후 바로 잡혔다 — 빌드 뒤에 고친 `worker/csd_worker.py` 1건을 지적했고,
안내대로 재빌드·재반입·rollout restart 한 뒤 통과했다.

### 3.12 배포 스크립트

§3.6 에서 손으로 밟은 절차를 [k8s/deploy.sh](k8s/deploy.sh) 에 담았다.
빈 클러스터면 `CSD_PASS=<비번> ./k8s/deploy.sh all` 한 줄이고, 부분 갱신은
서브커맨드(`image` / `kubeconfig` / `rbac` / `crd` / `secret` / `plugin` /
`controllers` / `status`)로 한다. 전부 idempotent 하다.

두 곳에 대기·검증을 넣었다. 손으로 할 때 실제로 걸렸던 자리다.
- `plugin` 은 `keti.re.kr/csd` 가 **광고될 때까지 기다렸다가** 넘어간다. 광고 전에
  워크로드를 넣으면 CSD 샤드가 `Pending` 에 머문다.
- `kubeconfig` 는 직전 파일을 `.prev` 로 남기고, 발급 후 CR 감시 권한을 확인한 뒤 끝난다.

`image` 는 재빌드 → containerd 반입 → rollout restart 까지 묶고, `csd-sync` 는 CSD 가
읽는 공유 볼륨 사본을 갱신한다 — 실행되는 코드 사본이 두 벌(§6.3)이므로 코드를 고쳤을
때의 표준 흐름은 이 둘이다. 드리프트를 지적하는 두 검사(§3.11, 헬스체크의 code copy)가
각각 이 명령을 안내한다.

**배포하면 회귀가 따라 돈다** — `all` 과 `image` 는 끝나고 `verify`(회귀 3층)를 자동으로
실행한다. 배포해 놓고 도는지 확인하지 않는 상태가 제일 나쁘기 때문이다.
`--no-verify` 로 끄고, 2층은 `CSD_PASS` 가 없으면 SKIP 한다. 실패하면 어느 층인지와
로그 경로(`/tmp/csd-verify-*.log`)를 찍고 rc=1 로 끝난다.

2026-08-18 에 10개 서브커맨드를 살아 있는 클러스터에서 전부 실행해 확인했고,
**드리프트 → 감지 → 수정 → 자동 재검증 고리를 실제로 한 바퀴 돌렸다**:
런타임 파일을 하나 고치자 2층(CSD 사본)과 3층(이미지)이 동시에 잡아 rc=1 로 멈췄고,
`csd-sync` + `image` 를 친 뒤 자동 회귀가 3층 모두 통과했다.

만드는 과정에서 잡은 함정: `kubectl -o jsonpath` 로 확장 자원을 조회할 때 키 안의
점(`keti.re.kr/csd`)을 이스케이프하지 않으면 **오류 없이 빈 값**이 나와 "광고 안 됨"과
구분되지 않는다. go-template 의 `index` 로 바꿨다.

### 3.13 분할 알고리즘 재확인

8/14 이전 클러스터에서만 검증됐던 MTE/WRR 을 현재 클러스터에서 재확인했다.
확인 방식을 두 층으로 나눴다 — AUTO 선택은 실측 처리량에 좌우돼 E2E 로는 네 갈래 중
하나만 밟게 되고, 그 하나가 실행마다 달라지기 때문이다.

**로컬 결정론 검증** — [server/test_partition_algorithms.py](server/test_partition_algorithms.py)
(스모크 묶음에 편입, 10종이 됨)

| 확인 | 내용 |
|---|---|
| AUTO 결정 테이블 | 네 갈래 전부 + 경계값(N=100, 비율=3.0 은 각각 MTE) |
| 배정 불변식 | STATIC/MTE/WRR 모두 500장이 정확히 한 번씩 배정 |
| 샤드 모양 | STATIC 연속 / MTE 만남지점+역순 / WRR 인터리브 |

**클러스터 E2E** — [server/test_k8s_algorithms.py](server/test_k8s_algorithms.py)
(`deploy.sh verify` 3층에 편입)

| 요청 | 결과 | 분할 |
|---|---|---|
| `MTE` | MTE | cpu 0.7, split_index 14 (연속) |
| `WRR` | WRR | cpu 0.8, split_index 0, weights 15:4 (비연속) |
| `AUTO` (20장) | MTE | "소규모 데이터셋 (20 <= 100장)" 근거 기록 |

셋 다 `Succeeded`, 샤드 산출 합계 = 입력 수, CSD 샤드 존재, CR 연쇄 GC 확인.
MTE 두 건의 분할 비율이 다른 것(0.7 vs 0.8)은 정상이다 — 처리량 프로파일이 실행
사이에 EWMA 로 갱신되기 때문이고, 그게 이 알고리즘의 설계다.

### 3.14 demo_data 정리

`demo_data` 는 현재 지원 경로와 과거 산출물을 분리했다.

현재 유지 대상: `_source/`(30장), `raw_data/`(50장), `backup/`
historical 이관: `backup/historical/20260818-demo-artifacts`
정리 기준 문서: [demo_data/README.md](demo_data/README.md)

---

## 4. 2026-08-18 수정 내역

8/14 CSD 재구축 이후 남아 있던 불일치 4건을 코드에서 정리했다.

1. **CSD 코드 경로 정정** — `CSD_REMOTE_REPO` 기본값이 존재하지 않는
   `/home/ngd/storage/csd-based-preprocessing` 를 가리키고 있었다.
   `/home/ngd/storage/csd_preprocessing` 로 바꾸고, 컨트롤러가 워커에 명시적으로 넘긴다.
   ([worker/csd_worker.py](worker/csd_worker.py),
   [controller/preprocess_controller.py](controller/preprocess_controller.py),
   [k8s/preprocess-controller-deployment.yaml](k8s/preprocess-controller-deployment.yaml),
   [experiments/corebudget/verify_offload.py](experiments/corebudget/verify_offload.py))
2. **인증을 비밀번호로 통일** — 공개키 인증은 쓰지 않기로 했다. 원격 실행 주체
   (서버 셸 / 워커 컨테이너 / 다른 노드)마다 키를 심어야 하는 부담이 있고, 켜 두면
   키가 있는 자리에서만 조용히 성공해 환경마다 다르게 동작하기 때문이다.
   워커·데모 스크립트·실험 스크립트 모두 `PubkeyAuthentication=no` +
   `PreferredAuthentications=password` 로 sshpass 경로를 강제한다.
   비밀번호가 없으면 즉시 실패한다(조용한 폴백 없음).
3. **컨트롤러가 접속 정보를 명시 전달** — `csd_remote_env()` 로 stage1·stage2 워커 Job
   양쪽에 `CSD_REMOTE_HOST`/`PASS`/`REPO` 를 같은 형태로 넘긴다. 이전에는 두 곳에
   따로 적혀 있어 한쪽만 고치기 쉬웠다.
4. **데모 스크립트 정리** — `send_data.sh`, `trigger_stage2.sh`, `server/copy_data.sh` 의
   ssh/scp 커맨드를 배열로 통일하고 공개키를 끈다. `send_data.sh` 주석의 CSD 주소
   오기(`10.1.1.2`)와 `server/copy_data.sh` 의 옛 경로 안내도 함께 고쳤다.
   `experiments/corebudget/verify_offload.py` 는 하드코딩돼 있던 비밀번호 기본값을
   없애고 `CSD_PASS` 환경변수에서 받는다.

부수 작업: 공유 볼륨의 CSD 코드 사본을 현재 워킹트리 기준으로 동기화했다
(`cli.py`, `config`, `controller`, `csd_preprocessor`, `worker`, `requirements.txt`).

이어서 같은 날 다음을 추가했다.

5. **헬스체크 스크립트** [server/csd_healthcheck.py](server/csd_healthcheck.py) (§3.5)
6. **`k8s/namespace-rbac.yaml` 신설** — 배포에 필요한 네임스페이스·쿼터·SA·RBAC 을
   저장소 안으로 옮겼다. 이전에는 `/root/preprocess-isolation/phase2/` 에만 있어
   클러스터가 재생성될 때마다 저장소 밖 자산에 의존해야 했고, 그 Role 에는 CR 그룹이
   옛 이름(`batch.csd.io`)으로 남아 있어 컨트롤러가 자기 CR 을 못 읽었다.
7. **`.dockerignore` 신설** — 이미지에서 demo_data·experiments·문서·모델 가중치를
   제외(1.13GB → 865MB). 루트의 한글 문서는 이름 대신 `/*.md` 로 거른다:
   buildkit 이 exclude 패턴을 HTTP 헤더로 넘겨 비ASCII 경로가 있으면 빌드가 깨진다.
8. **샘플 워크로드 경로 수정** — `k8s/sample-preprocessingworkload.yaml` 이
   `/home/ngd/storage/...` 를 가리켰는데, 노드에서 그 경로는 공유 볼륨이 아니라
   **동명의 로컬 디렉터리**다. 그대로 두면 공유 볼륨 판정에 실패해 copy 모드로
   떨어진다. `/mnt/newport_1/...` 로 바꾸고 이유를 주석에 남겼다.

---

## 5. 부족한 부분

### 5.1 배포는 스크립트로 되지만 클러스터 밖 자산에 의존한다

§3.12 로 배포·재발급·이미지 갱신은 한 줄이 됐다. 남은 결합은 kubeconfig 가
`/root/preprocess-isolation/` 아래에 hostPath 로 놓인다는 점이다 — 저장소 밖 경로이고,
Deployment 매니페스트에 박혀 있어 노드를 바꾸면 같이 손봐야 한다.

이미지 드리프트도 §3.11 이 감지하고 §3.12 가 고치지만, **회귀를 돌려야 감지된다**.
파드는 그 사이 조용히 옛 코드를 돌 수 있다.

### 5.2 COCO 원격 재취득 경로가 막혀 있다

데이터셋 자체는 §3.7 로 복구했지만, `server/download_coco_sample.py` 가 쓰는
`images.cocodataset.org` 는 현재 접근이 안 된다(HTTP 000, 일반 인터넷은 정상).
지금은 노드에 있던 사본에 의존하고 있고, 그 사본이 사라지면 재취득 수단이 없다.

### 5.3 CSD watcher 미기동

CSD 에 `csd_watcher` 프로세스가 떠 있지 않고, 공유 볼륨의 코드 사본에는
`server/` 가 없어 레거시 watcher 경로는 CSD 에서 실행할 수 없다.
데모 스크립트(`send_data.sh` → `trigger_stage2.sh`)는 CSD 쪽 watcher 기동을 전제로 하므로
지금 그대로 돌리면 전송만 되고 이후 단계가 진행되지 않는다.

### 5.4 회귀는 배포에만 물려 있다

회귀 3층은 `./k8s/deploy.sh verify` 한 줄이고, `all`·`image` 뒤에 자동으로 돈다.
남은 공백은 **배포하지 않는 변경**이다 — 코드만 고치고 배포를 안 하면 아무것도 돌지
않는다. 저장소에 `.git` 이 없어 커밋 훅도 걸 수 없고(§5.7), CI 도 없다.

현실적인 습관은 코드를 고치면 `./k8s/deploy.sh csd-sync && ./k8s/deploy.sh image` 를
치는 것이다 — 어차피 사본 두 벌을 갱신해야 하고, 그 끝에 회귀가 붙어 있다.

### 5.5 파이썬 실행 환경이 통일되지 않음

`/usr/bin/python3` 에는 `cv2` 가 없다. `./run_local_python.sh` 를 써야 한다.
무심코 `python3 cli.py ...` 를 치면 바로 실패한다.

### 5.6 비밀번호가 평문으로 흐른다

인증을 비밀번호로 통일한 대가다. 비밀번호는 워커 Job 의 env 로 들어가므로
`kubectl describe job` 에 그대로 보이고, 셸에서는 `CSD_PASS` 환경변수로 오간다.
k8s 에서는 `csd-credentials` 시크릿에서 읽지만 파드 env 에 풀리는 것은 같다.
과거 실험 산출물에도 남아 있다(`experiments/corebudget/results/verify_offload.json`
의 `config.password`) — 외부 공유 전에는 확인이 필요하다.

### 5.7 저장소 상태 추적 약함

작업 디렉터리에 `.git` 이 없다. 브랜치 상태, 최근 커밋, 변경 이력을 여기서
직접 검증할 수 없다. 변경 내역은 이 문서 §4 같은 수동 기록에 의존한다.

### 5.8 문서 최신성

주요 문서는 2026-08-18 기준으로 갱신했지만, `documents/backup` 과 historical
산출물에는 8월 초 또는 그 이전 기준 내용이 남아 있다. 참고는 되지만 그대로
운영 사실로 간주하면 안 된다.

---

## 6. 리스크

### 6.1 라이브 상태는 시간 단위로 바뀐다

8/14 에는 CSD 스토리지가 전소돼 있었고 k8s RBAC 이 막혀 있었다. 8/18 에는 둘 다 풀렸다.
**이 문서의 §3 도 확인 시점 기록이지 보증이 아니다.** 실행 전에 그 자리에서 다시 본다.

### 6.2 회귀 리스크

k8s 통합 회귀가 없어서, 컨트롤러나 워커 리팩터링 시 다음이 조용히 깨질 수 있다.
- stage 전이 / 샤드 집계 / 라벨 게이트 / stage2 연결 (로컬 스모크가 일부만 덮는다)
- 실 워커 Job 생성·완료 경로 — §3.6 을 사람이 다시 돌려야 안다

### 6.3 코드 사본이 두 벌 더 있다

실행되는 코드가 워킹트리 말고 두 군데 더 있다.
- **CSD**: 공유 볼륨의 사본(`/mnt/newport_1/csd_preprocessing`). 안 올리면 워커가
  "CSD 측 코드가 구버전"으로 감지해 실패시킨다(파이프라인 미보고 검사).
  헬스체크의 "code copy in sync" 가 이걸 잡는다.
- **k8s**: 컨테이너 이미지 안의 사본. 이미지를 다시 굽지 않으면 옛 코드가 조용히
  돈다 — 실제로 배포 직전 이미지가 13일 전(8/05) 것이라 8/06 라벨 정합 수정 이후
  변경분이 전부 빠져 있었다. 지금은 k8s 회귀가 시작할 때 잡아준다(§3.11).
  다만 **회귀를 돌려야 알 수 있다** — 파드는 여전히 조용히 옛 코드를 돌 수 있다.

### 6.4 재현성 리스크

런타임 코드, 실험 코드, 샘플 데이터, 과거 상태 파일이 한 리포에 섞여 있다.
`demo_data` 정리로 줄였지만 완전히 없어지지는 않았다.

### 6.5 잘못된 인터프리터

`python3 cli.py ...` 는 환경에 따라 바로 깨진다. `./run_local_python.sh` 를 쓴다.

---

## 7. 현재 권장 실행 기준

로컬:

```bash
./run_local_python.sh cli.py ops
./run_local_python.sh cli.py run --template <template> --input <input_dir> --output <output_dir>
./run_smoke_tests.sh
```

CSD 오프로드 (비밀번호 인증):

```bash
# 입출력을 /mnt/newport_1 아래에 두면 복사 없이 CSD 가 제자리에서 처리한다
CSD_REMOTE_HOST=root@10.2.1.2 CSD_REMOTE_PASS=<비번> WORKER_TYPE=CSD ... \
  ./run_local_python.sh worker/csd_worker.py
```

`CSD_REMOTE_REPO` 는 기본값(`/home/ngd/storage/csd_preprocessing`)으로 충분하다.
`CSD_REMOTE_PASS` 는 필수다.

회귀는 세 층으로 나뉜다. 각각 전제가 없으면 SKIP 하거나 명확히 실패하므로
되는 것부터 돌리면 된다.

```bash
./run_smoke_tests.sh                                               # 로컬 (오프라인 가능)
CSD_REMOTE_PASS=<비번> ./run_local_python.sh server/csd_healthcheck.py --offload
./run_local_python.sh server/test_k8s_integration.py               # k8s 분산 체인
```

---

## 8. 바로 다음 작업

2026-08-18 에 다음을 끝냈다: 헬스체크 / k8s 첫 배포 / 데이터셋 복구 /
CSD 실패 경로 회귀(§3.9) / k8s 통합 회귀(§3.10) / 이미지 드리프트 감지(§3.11).

남은 것:

1. **배포하지 않는 변경의 회귀**(§5.4) — 지금은 배포에만 물려 있다.
   `.git` 이 없어 훅을 못 걸므로, 저장소를 git 으로 올릴지부터 결정해야 한다.
2. **CSD watcher 기동 여부 정리** — 레거시 경로를 계속 지원할지 결정하고,
   지원한다면 `server/` 를 CSD 사본에 포함시킨다(§5.3).
3. **COCO 재취득 경로 확보**(§5.2) — 지금은 노드의 사본에만 의존한다.
4. **kubeconfig 경로 결합 정리**(§5.1) — 저장소 밖 hostPath 의존을 줄일지 결정.
5. historical 자료 정리 및 문서 보강

---

## 9. 한 줄 결론

2026-08-18 기준으로 **설계한 경로가 전부 한 번씩 실제로 돌았다** — 로컬 파이프라인,
실 CSD 오프로드(shared-volume/copy), 그리고 k8s 분산 체인 완주(PW → PJ → CPU·CSD
샤드 → stage2 → Succeeded)까지.

확인 수단도 세 층(로컬 스모크 / CSD 헬스체크 / k8s 통합 회귀)으로 갖춰졌고,
실행되는 코드 사본 두 벌(CSD·이미지)의 드리프트도 각각 감지된다.

배포·kubeconfig 재발급·이미지 갱신·CSD 사본 동기화도 `./k8s/deploy.sh` 한 줄이 됐고,
배포하면 회귀 3층이 따라 돈다.

남은 것은 **배포를 거치지 않는 변경**이다. 저장소에 `.git` 이 없어 커밋 훅을 걸 수 없고
CI 도 없으므로, 회귀가 도는 유일한 계기가 "배포할 때"에 묶여 있다.
