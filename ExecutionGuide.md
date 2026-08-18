# CSD 전처리 실행 가이드

최종 갱신: 2026-08-18 (CSD 오프로드 절 추가)

이 문서는 현재 코드 기준의 개발/운영 실행 가이드다.
실시간 클러스터 상태를 보장하는 문서는 아니며, 확인된 실행 기준과 코드 구조를 중심으로 정리한다.

관련 문서:
- 구조 설명: [ARCHITECTURE.md](ARCHITECTURE.md)
- 개발 현황: [CurrentProgress.md](CurrentProgress.md)

---

## 1. 먼저 알아둘 것

### 1.1 현재 권장 로컬 파이썬

로컬 표준 실행기는 다음 스크립트다.

```bash
./run_local_python.sh
```

주의:
- `/usr/bin/python3` 에는 현재 `cv2` 가 없다
- `run_local_python.sh` 는 내부적으로 `/root/miniconda3/bin/python` 을 사용한다
- 따라서 `python3 cli.py ...` 는 실패할 수 있다

### 1.2 현재 메인 실행 경로

현재 메인 경로는 분산 체인이다.

`PreprocessingWorkload -> PreprocessingJob -> CPU/CSD 워커 -> 샤드 집계 -> 라벨 게이트 -> stage2`

주요 코드:
- [controller/preprocess_manager.py](controller/preprocess_manager.py)
- [controller/preprocess_controller.py](controller/preprocess_controller.py)
- [worker/csd_worker.py](worker/csd_worker.py)

### 1.3 레거시 경로

`server/csd_watcher.py` 의 legacy/in-process 경로는 남아 있지만, 현재 주 경로는 아니다.
개발 기준으로는 분산 체인을 우선으로 본다.

---

## 2. 로컬에서 가장 먼저 할 확인

### 2.1 연산 목록 확인

```bash
./run_local_python.sh cli.py ops
```

정상이라면 `validate`, `deduplicate`, `resize`, `normalize`, `augment` 같은 연산 목록이 출력된다.

### 2.2 경량 테스트 실행

```bash
./run_local_python.sh server/test_trigger_flow.py
./run_local_python.sh server/test_mock_verify.py
./run_local_python.sh server/test_stage1_smoke.py
./run_local_python.sh server/test_stage2_smoke.py
./run_local_python.sh server/test_worker_local_fallback.py
./run_local_python.sh server/test_manager_controller_smoke.py
./run_local_python.sh server/test_stage2_missing_labels_failure.py
./run_local_python.sh server/test_invalid_template_failure.py
./run_local_python.sh server/test_csd_remote_failure.py
```

앞의 두 개는 경량 흐름 검증이고, 나머지는 로컬 E2E 스모크와 대표 실패 경로다.
2026-08-18 기준 8종 전부 통과한다.

한 번에 모두 돌리려면:

```bash
./run_smoke_tests.sh
```

---

## 3. CLI 사용법

### 3.1 연산 목록 보기

```bash
./run_local_python.sh cli.py ops
```

### 3.2 템플릿으로 전처리 실행

```bash
./run_local_python.sh cli.py run \
  --template stage1_raw_ingestion \
  --input <입력_디렉터리> \
  --output <출력_디렉터리>
```

라벨 경로가 필요한 경우:

```bash
./run_local_python.sh cli.py run \
  --template stage2_training_preparation \
  --input <입력_디렉터리> \
  --output <출력_디렉터리> \
  --labels <라벨_디렉터리>
```

### 3.3 잡 상태 확인

```bash
./run_local_python.sh cli.py status --output <출력_디렉터리>
```

### 3.4 watcher 실행

```bash
./run_local_python.sh cli.py watch \
  --dir <감시_디렉터리> \
  --template stage1_raw_ingestion \
  --output <출력_디렉터리>
```

---

## 4. 현재 지원되는 주요 템플릿

템플릿 위치:
- [config/pipeline_templates](config/pipeline_templates)

주요 템플릿:
- `stage1_raw_ingestion`
- `stage2_training_preparation`
- `yolo_object_detection`
- `image_classification`

stage1/stage2 분리의 의미:
- stage1: 샤드 병렬 처리 대상
- stage2: 데이터셋 전체를 한 번에 다루는 후처리

---

## 5. k8s 경로 요약

### 5.1 배포 스크립트

절차는 [k8s/deploy.sh](k8s/deploy.sh) 에 담겨 있다. 빈 클러스터라면 한 줄이다.

```bash
CSD_PASS=<비번> ./k8s/deploy.sh all
```

부분 갱신은 서브커맨드로 한다. 전부 idempotent 하다.

| 서브커맨드 | 하는 일 | 언제 |
|---|---|---|
| `all` | 아래를 순서대로 | 빈 클러스터 |
| `image` | 재빌드 → containerd 반입 → rollout restart | 코드를 고쳤을 때 |
| `kubeconfig` | 컨트롤러용 kubeconfig 재발급 | 클러스터 재생성 후 |
| `rbac` / `crd` | 네임스페이스·쿼터·SA·RBAC / CRD 2종 | 초기 1회 |
| `secret` | CSD 비밀번호 시크릿 (`CSD_PASS` 필요) | 비번이 바뀌었을 때 |
| `plugin` | csd-device-plugin + 확장 자원 광고 **대기** | 초기 1회 |
| `controllers` | 컨트롤러·매니저 apply + rollout 대기 | 매니페스트 변경 시 |
| `csd-sync` | CSD 가 실행할 코드 사본을 공유 볼륨에 동기화 | 코드를 고쳤을 때 |
| `verify` | 회귀 3층 실행 | 아무 때나 |
| `status` | 파드·CR·확장 자원·kubeconfig 요약 | 아무 때나 |

`plugin` 은 `keti.re.kr/csd` 가 실제로 광고될 때까지 기다렸다가 넘어간다 —
광고 전에 워크로드를 넣으면 CSD 샤드 파드가 `Pending` 에서 나오지 못하기 때문이다.
`kubeconfig` 는 직전 파일을 `.prev` 로 남기고, 발급 후 CR 감시 권한까지 확인한다.

### 5.1.1 배포하면 회귀가 따라 돈다

`all` 과 `image` 는 끝나고 **회귀 3층을 자동으로 실행한다**. 배포해 놓고 도는지
확인하지 않는 상태가 제일 나쁘기 때문이다. 건너뛰려면 `--no-verify` 를 붙인다.

```
== 회귀 3층
   1층: 로컬 스모크          통과 (9종)
   2층: CSD 헬스체크         전부 통과 (12개 항목)
   3층: k8s 통합 회귀        [PASS] k8s integration test
   회귀 전부 통과
```

2층은 `CSD_PASS` 가 없으면 SKIP 한다. 실패하면 어느 층인지와 로그 경로
(`/tmp/csd-verify-*.log`)를 찍고 rc=1 로 끝난다.

**코드를 고쳤을 때의 표준 흐름**은 두 줄이다 — 실행되는 사본이 두 벌이기 때문이다.

```bash
./k8s/deploy.sh csd-sync           # CSD 가 읽는 공유 볼륨 사본
CSD_PASS=<비번> ./k8s/deploy.sh image   # 파드가 읽는 이미지 (+ 자동 회귀)
```

### 5.1.2 스크립트가 하는 일 (수동 절차)

스크립트를 쓰지 않거나 중간에 막혔을 때 참고할 원래 순서다.

```bash
# 0) 이미지를 현재 코드로 굽고 노드 containerd 에 넣는다 (imagePullPolicy: Never)
docker build -t csd-preprocessor:latest .
docker save csd-preprocessor:latest | ctr -n k8s.io images import -
docker build -t csd-device-plugin:latest deviceplugin/     # 이미 있으면 생략
docker save csd-device-plugin:latest | ctr -n k8s.io images import -

# 1) 네임스페이스 · 쿼터 · SA · RBAC
kubectl apply -f k8s/namespace-rbac.yaml

# 2) CRD
kubectl apply -f k8s/preprocessingjob-crd.yaml -f k8s/preprocessing-workload-crd.yaml

# 3) 컨트롤러·매니저가 쓸 kubeconfig (§5.2) 와 CSD 비밀번호 시크릿
kubectl -n preprocess-csd create secret generic csd-credentials --from-literal=password=<비번>

# 4) 확장 자원 광고 — 이게 없으면 CSD 샤드 파드가 Pending 에서 안 나온다
kubectl apply -f k8s/csd-device-plugin.yaml
kubectl get node <노드> -o jsonpath='{.status.allocatable}' | tr ',' '\n' | grep keti.re.kr/csd

# 5) 컨트롤러 · 매니저
kubectl apply -f k8s/preprocess-controller-deployment.yaml \
              -f k8s/preprocess-manager-deployment.yaml
kubectl -n preprocess-csd get pods
```

### 5.2 kubeconfig

컨트롤러·매니저는 `/root/preprocess-isolation/preprocess-csd.kubeconfig` 를 hostPath 로
마운트해 쓴다. **클러스터를 재생성하면 이 파일의 토큰이 무효**가 되므로 다시 만들어야
한다(보름 사이 두 번 있었다). `./k8s/deploy.sh kubeconfig` 가 발급·백업·권한 확인까지 한다.

확인만 따로 하려면:

```bash
KUBECONFIG=/root/preprocess-isolation/preprocess-csd.kubeconfig \
  kubectl auth can-i watch preprocessingworkloads.edgeai.keti.re.kr -n preprocess-csd
```

### 5.3 워크로드 제출과 확인

```bash
kubectl apply -f k8s/sample-preprocessingworkload.yaml
kubectl -n preprocess-csd get pw,pj -w
kubectl -n preprocess-csd get jobs
kubectl -n preprocess-csd logs deploy/preprocess-controller --tail=20
```

입력·출력 경로는 **노드에서 본 경로**이고, `/mnt/newport_1` 아래에 두어야 CSD 가
복사 없이 제자리에서 처리한다(§9.2). 노드에도 `/home/ngd/storage` 라는 다른 로컬
디렉터리가 있으니 혼동하지 말 것.

### 5.4 자주 막히는 곳

| 증상 | 원인 | 조치 |
|---|---|---|
| CSD 샤드 파드가 `Pending`, `Insufficient keti.re.kr/csd` | csd-device-plugin 미배포 | §5.1 의 4) |
| 컨트롤러 파드가 CR 을 못 읽음(403) | Role 에 `edgeai.keti.re.kr` 그룹 누락 | `k8s/namespace-rbac.yaml` 적용 |
| 컨트롤러가 API 서버 접속 실패 | kubeconfig 가 옛 클러스터를 가리킴 | `./k8s/deploy.sh kubeconfig` |
| 코드를 고쳤는데 동작이 그대로 | 파드가 이미지 안의 옛 코드를 돈다 | `./k8s/deploy.sh image` |
| 오프로드가 `copy` 모드로 떨어짐 | 입출력이 `/mnt/newport_1` 밖 | 경로를 공유 볼륨 아래로 |

---

## 6. stage 흐름 요약

### 6.1 stage1

샤드 단위 병렬 처리다.
대표 연산은 다음 순서를 따른다.

`validate -> deduplicate -> filter_quality -> resize -> normalize`

추가로 `phash` 기록이 붙을 수 있다.

### 6.2 샤드 집계

컨트롤러가 샤드 결과를 읽어 통계를 집계한다.
샤드 결과 요약은 `_shards` 아래 결과 파일들에 남는다.

### 6.3 라벨 게이트

라벨이 필요한 stage2 템플릿이면, 어노테이션이 준비될 때까지 대기할 수 있다.

### 6.4 stage2

데이터셋 전체를 한 번에 다루는 후처리다.
대표 연산:

`convert_annotation -> augment -> split -> statistics`

---

## 7. 자주 헷갈리는 점

### 7.1 `/usr/bin/python3` 와 로컬 표준 실행기는 다르다

현재 프로젝트는 `./run_local_python.sh` 기준으로 확인됐다.
이 스크립트는 `/root/miniconda3/bin/python` 을 사용한다.
`/usr/bin/python3` 를 기본으로 생각하면 `cv2` 문제로 바로 막힐 수 있다.

### 7.2 실험 코드와 런타임 코드가 같이 있다

다음 디렉터리는 실험/분석용이다.
- [experiments](experiments)

즉 실험 스크립트가 있다고 해서 그것이 곧 현재 지원되는 운영 경로라는 뜻은 아니다.

### 7.3 과거 상태 파일은 현재 성공 상태를 보장하지 않는다

예를 들어 `demo_data` 아래 상태 파일은 과거 실패 흔적일 수 있다.
현재 동작 여부는 실제 실행이나 최신 테스트로 확인해야 한다.

---

## 8. 현재 확인된 최소 개발 체크리스트

새로 작업하기 전에 최소한 아래는 확인하는 편이 좋다.

```bash
./run_local_python.sh -c "import cv2; print(cv2.__version__)"
./run_local_python.sh cli.py ops
./run_smoke_tests.sh
```

여기까지 통과하면 최소 로컬 개발 진입 상태로 본다.

CSD·k8s 까지 살아 있는지 보려면 두 줄을 더 본다.

```bash
CSD_REMOTE_PASS=<비번> ./run_local_python.sh server/csd_healthcheck.py --offload
./run_local_python.sh server/test_k8s_integration.py
```

k8s 회귀는 워크로드를 제출해 `Succeeded` 까지 확인하고 CR·Job 을 지운다.
시작할 때 **배포된 파드의 코드가 워킹트리와 같은지** 먼저 대조하므로, 이미지를
다시 굽지 않은 채 회귀를 돌리면 그 자리에서 재빌드 명령과 함께 멈춘다.

---

## 9. 실 CSD 오프로드

### 9.1 접속 방식

CSD(`root@10.2.1.2`)는 **비밀번호(sshpass) 인증 전용**이다. 공개키 인증은
`PubkeyAuthentication=no` 로 끈다 — 실행하는 자리마다 키가 있기도 없기도 해서,
켜 두면 서버 셸에서만 조용히 키로 붙고 워커 컨테이너에서는 실패한다.

| 환경변수 | 기본값 | 비고 |
|---|---|---|
| `CSD_REMOTE_HOST` | (필수) | 예 `root@10.2.1.2` |
| `CSD_REMOTE_REPO` | `/home/ngd/storage/csd_preprocessing` | 2026-08-14 재구축 때 `csd-based-preprocessing` 에서 바뀜 |
| `CSD_REMOTE_PASS` | (필수) | sshpass 비밀번호. 없으면 워커가 즉시 실패 |
| `CSD_REMOTE_PYTHONPATH` | `/home/ngd/storage/pylibs` | CSD 의 numpy/OpenCV |

셸 스크립트(`send_data.sh`, `trigger_stage2.sh`, `server/copy_data.sh`)는
같은 비밀번호를 `CSD_PASS` 환경변수로 받는다. k8s 에서는 `csd-credentials` 시크릿이다.

### 9.2 실행 방식은 경로가 정한다

입출력·라벨이 모두 `/mnt/newport_1`(= CSD 의 `/home/ngd/storage`) 아래면
복사 없이 CSD 가 제자리에서 처리한다(`shared-volume`). 그 밖이면 scp 로
밀어넣고 회수한다(`copy`) — 샤드마다 고정비가 붙으므로 작은 데이터셋에서는
분할 이득이 사라진다. **입출력을 `/mnt/newport_1` 아래에 두는 편이 맞다.**

결과는 `result.json.offload` 에 `mode` 와 `executedOn` 으로 남는다.

### 9.3 최소 확인 (2026-08-18 실측 통과)

```bash
SSHP=(sshpass -p "$CSD_PASS" ssh -o StrictHostKeyChecking=no
      -o PubkeyAuthentication=no -o PreferredAuthentications=password)
"${SSHP[@]}" root@10.2.1.2 true                              # 비밀번호 인증
"${SSHP[@]}" root@10.2.1.2 \
  'PYTHONPATH=/home/ngd/storage/pylibs python3 -c "import cv2,numpy; print(cv2.__version__)"'
mount | grep newport                                         # 공유 볼륨
```

CSD 의 `sshd_config` 는 `PasswordAuthentication yes` 다 (2026-08-18 확인).

### 9.4 CSD 측 코드 동기화

CSD 는 공유 볼륨의 코드 사본(`/mnt/newport_1/csd_preprocessing`)을 실행한다.
워킹트리를 고쳤으면 사본도 올려야 한다. 안 올리면 워커가 "CSD 측 코드가
구버전"으로 감지해 실패시킨다.

```bash
./k8s/deploy.sh csd-sync
```

헬스체크의 "code copy in sync" 항목이 이 드리프트를 잡고, 실패 시 위 명령을 안내한다.

---

## 10. 아직 남아 있는 운영 체크

문서로만 확정할 수 없고, 실제 환경에서 그때그때 확인해야 하는 것:
- 실제 k8s 파드 상태 (현재 preprocessing CRD 미설치, 컨트롤러 파드 없음)
- 공유 볼륨의 `raw_data` 존재 여부 (8/14 재포맷으로 소실, 미복구)
- CSD watcher 기동 여부 (현재 미기동)

§9.3 은 2026-08-18 에 통과했지만, 이것도 그 시점 기록이다.

---

## 11. 권장 다음 작업

1. §9.3 확인 절차를 헬스체크 스크립트로 고정 (오프로드 1회 실행까지 포함)
2. k8s 경로 첫 배포 — CRD 설치 → 컨트롤러·매니저 배포 → 샘플 워크로드 완주
3. `server/download_coco_sample.py` 로 공유 볼륨 `raw_data` 재취득
4. CSD 원격 실패 경로 회귀 추가 (SSH 불가 / 경로 불일치 / shared-volume 미가시)
