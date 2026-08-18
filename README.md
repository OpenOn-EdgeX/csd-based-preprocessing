# CSD-Based Preprocessing

AI 학습 데이터 전처리를 **CSD(Computational Storage Device)로 오프로드**하는 분산 파이프라인이다.
Kubernetes 가 워크로드를 받아 샤드로 나누고, 일부는 노드 CPU 가, 일부는 CSD 내부(ARM)가
처리한다. 입출력이 공유 OCFS2 파티션 아래에 있으면 CSD 는 **파일을 복사하지 않고 제자리에서**
읽고 쓴다.

```
PreprocessingWorkload ──▶ PreprocessingJob ──▶ CPU/CSD 워커 Job ──▶ 샤드 집계
                                                                      │
                                                       라벨 게이트 ──▶ stage2 ──▶ 학습 데이터셋
```

- 구조: [ARCHITECTURE.md](ARCHITECTURE.md)
- 실행/운영: [ExecutionGuide.md](ExecutionGuide.md)
- 개발 현황과 한계: [CurrentProgress.md](CurrentProgress.md)

---

## 빠른 시작 (로컬)

이 프로젝트는 `/usr/bin/python3` 를 가정하지 않는다. `cv2` 가 없는 환경이 흔해서
표준 실행기를 따로 둔다.

```bash
./run_local_python.sh cli.py ops                 # 사용 가능한 전처리 연산 목록
./run_smoke_tests.sh                             # 로컬 회귀 9종 (오프라인 가능)
```

파이프라인 한 번 돌려보기:

```bash
./run_local_python.sh cli.py run \
  --template stage1_raw_ingestion \
  --input <입력_디렉터리> --output <출력_디렉터리>
```

## k8s 스택 배포

```bash
CSD_PASS=<비번> ./k8s/deploy.sh all      # 빈 클러스터에서 전체
./k8s/deploy.sh status                   # 현재 상태
```

부분 갱신은 서브커맨드로 한다(`image`, `kubeconfig`, `rbac`, `crd`, `secret`,
`plugin`, `controllers`, `csd-sync`, `verify`). 전부 idempotent 하다.
자세한 내용과 자주 막히는 곳은 [ExecutionGuide.md](ExecutionGuide.md) §5.

**코드를 고쳤으면 사본 두 벌을 갱신해야 한다** — CSD 는 공유 볼륨의 코드 사본을 실행하고,
k8s 파드는 컨테이너 이미지 안의 코드를 실행한다.

```bash
./k8s/deploy.sh csd-sync                 # 공유 볼륨 사본
CSD_PASS=<비번> ./k8s/deploy.sh image    # 이미지 (+ 회귀 자동 실행)
```

## 회귀

세 층으로 나뉜다. 전제가 없으면 SKIP 하므로 되는 것부터 돌리면 된다.

| 층 | 명령 | 확인 범위 |
|---|---|---|
| 로컬 | `./run_smoke_tests.sh` | 파이프라인·상태 전이·대표 실패 경로·CSD 원격 실패 경로·분할 알고리즘 선택 |
| CSD | `CSD_REMOTE_PASS=<비번> ./run_local_python.sh server/csd_healthcheck.py --offload` | 접속·원격 런타임·공유 볼륨·코드 사본 드리프트·실제 오프로드 1회 |
| k8s | `./run_local_python.sh server/test_k8s_integration.py` | 워크로드 제출 → Succeeded → 산출물 → CR·Job 연쇄 GC |
| k8s | `./run_local_python.sh server/test_k8s_algorithms.py` | MTE/WRR/AUTO 분할이 CR·워커까지 이어지는지 |

세 층을 한 번에: `CSD_PASS=<비번> ./k8s/deploy.sh verify`

## CSD 접속

**비밀번호(sshpass) 인증 전용이다.** 공개키 인증은 `PubkeyAuthentication=no` 로 명시적으로
끈다 — 실행 주체(서버 셸 / 워커 컨테이너 / 다른 노드)마다 키가 있기도 없기도 해서,
켜 두면 키가 있는 자리에서만 조용히 성공해 환경마다 다르게 동작하기 때문이다.

| 환경변수 | 기본값 | 비고 |
|---|---|---|
| `CSD_REMOTE_HOST` | (필수) | 예 `root@10.2.1.2` |
| `CSD_REMOTE_PASS` | (필수) | 없으면 워커가 즉시 실패한다 |
| `CSD_REMOTE_REPO` | `/home/ngd/storage/csd_preprocessing` | CSD 측 코드 경로 |
| `CSD_REMOTE_PYTHONPATH` | `/home/ngd/storage/pylibs` | CSD 의 numpy/OpenCV |

k8s 에서는 `csd-credentials` 시크릿으로 넣는다(`./k8s/deploy.sh secret`).

## 데이터

`demo_data/` 와 학습 가중치(`*.pt`)는 용량 때문에 저장소에 없다.
COCO 샘플을 받으려면:

```bash
./run_local_python.sh server/download_coco_sample.py
```

**주의**: 2026-08-18 기준 `images.cocodataset.org` 에 접근이 되지 않는다(HTTP 000,
일반 인터넷은 정상). 이 경우 노드에 있는 사본을 쓴다 —
`/root/models/warboy-vision-models/datasets/coco` (val2017 5,000장 + annotations).

파이프라인 입출력은 **공유 OCFS2 파티션(`/mnt/newport_1`, CSD 에서는 `/home/ngd/storage`)
아래에 두는 편이 맞다.** 그 밖에 두면 CSD 오프로드가 샤드마다 scp 고정비가 붙는
copy 모드로 떨어져 분할 이득이 사라진다.

## 저장소 구성

```
cli.py                진입점 (run / status / ops / watch / mock-verify)
csd_preprocessor/     전처리 연산 구현 (validate, resize, convert_annotation, ...)
controller/           매니저(분할 계획) · 컨트롤러(워커 디스패치 · 집계)
worker/               실 워커. CSD 오프로드(shared-volume / copy) 포함
server/               로컬 회귀 · 헬스체크 · 데이터 준비 스크립트
k8s/                  매니페스트 + deploy.sh
config/               파이프라인 템플릿 (stage1 / stage2 / yolo / classification)
deviceplugin/         keti.re.kr/csd 확장 자원 광고
```

저장소에 없는 것: `demo_data/`(용량), `documents/`(논문·연구노트, 별도 관리),
`experiments/`(실험 코드·결과, 로컬 tar 백업으로 관리 — 과거 실행 설정에 비밀번호가
남아 있어 제외한다).
