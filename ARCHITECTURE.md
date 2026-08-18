# CSD 전처리 아키텍처

2026-08-18 기준. 현재 진행상황 요약은 [CurrentProgress.md](CurrentProgress.md),
운영 절차는 [ExecutionGuide.md](ExecutionGuide.md), 워커 계약은
[worker/README.md](worker/README.md) 참조. 이 문서는 **구조·책임경계·정책**을 다룬다.

전처리 실행 경로는 **분산 체인 하나로 통합**돼 있다. PreprocessingWorkload(pw) 하나를
만들면 stage1(CPU/CSD 샤드 병렬) → 결과 집계 → 라벨 게이트 → stage2(단일 패스) 까지
자동으로 진행된다.

---

## 1. 전체 흐름

```
① 데이터 도착        raw_data/images/ 에 엣지 데이터 투입
                     csd-watcher: 감지 기록만 (watch 모드) ─ 전처리·워크로드 생성 안 함
                          │
② 워크로드 제출      PreprocessingWorkload (pw)  ← 스케줄러가 생성 (생산자 1개)
                          │  매니저: 입력 스캔 → 처리량 프로파일 조회
                          │          → 알고리즘 자동 선택(§12) → 분할계획 → 템플릿 해석
                          ▼
③ 잡 선언            PreprocessingJob (pj)
                     spec: preprocessing_pipeline[{op,params}] / stage2_pipeline / partition_info
                          │  실행엔진
        ┌─────────────────┴─────────────────┐
        ▼                                   ▼
④ stage1 CPU 샤드                    stage1 CSD 샤드
   노드에서 실행                      10.2.1.2 실장비로 오프로드
   validate→deduplicate→filter_quality→resize→normalize→phash   ← 양쪽 동일 6스텝
        └─────────────────┬─────────────────┘
                          ▼
⑤ 결과 집계          부분통계 가중평균 + 일관성 검증 + 전역 dedup(pHash)
                     → _shards/shard_summary.json
                     ※ 숫자만 모은다. 이미지는 샤드들이 같은 디렉터리에 직접
                       기록하므로 파일을 합치는 단계가 존재하지 않는다.
                          ▼
⑥ 라벨 게이트        annotations/ 도착까지 대기 (stage=waiting_labels)
                     라벨링은 HOST 담당 — 파일이 놓이면 자동 재개
                          ▼
⑦ stage2 단일 패스   convert_annotation→augment→split→statistics
                     기본 CSD 실행 — 공유 볼륨이면 데이터셋·어노테이션을 제자리에서
                     읽고 쓴다(복사 없음). 아니면 push/pull 폴백
                          ▼
⑧ 산출물             pj_out/<이름>/{train,val,test} + data.yaml + statistics.json
```

**stage1 은 SPMD 구조다** — 같은 파이프라인, 다른 데이터. 스텝을 나눈 게 아니라
파일을 나눈 것이므로 두 샤드가 6스텝 전부를 각자 맡은 파일에 수행한다.
그래서 두 샤드의 파이프라인이 조금이라도 다르면 안 된다(§7 안전장치 참조).

### 데이터가 실제로 움직이는 지점

"집계(aggregate)"를 데이터 합치기로 오해하기 쉬운데, 이 파이프라인에서 **파일을
합치는 단계는 없다**. 구분하면:

| 지점 | 데이터 이동 | 설명 |
|---|---|---|
| 샤드 → 데이터셋 디렉터리 | **없음** | 각 샤드가 같은 `DATASET_DIR/images` 에 직접 기록(co-locate). 옮기거나 합치지 않는다 |
| 샤드 결과 집계 (⑤) | **없음** | `result.json` 의 숫자(mean/std/pixels/카운트)와 `records.jsonl` 의 pHash 만 읽는다. 이미지를 다시 읽지도 않는다 |
| 전역 dedup | 중복분만 이동 | `images/` → `_duplicates/` (증빙 보존) |
| CSD 오프로드 (공유 볼륨) | **없음** | 데이터셋이 공유 OCFS2 파티션에 있으면 CSD 가 같은 파일을 제자리에서 읽고 쓴다 |
| CSD 오프로드 (노드 로컬) | 실제 복사 | 공유 볼륨 밖이면 scp 로 밀어넣고 결과를 되가져온다 (폴백) |

CSD 의 `/home/ngd/storage` 와 서버의 `/mnt/newport_1` 은 **같은 OCFS2 파티션**이다.
워커는 입출력·라벨 경로가 모두 그 아래인지 보고 실행 방식을 자동으로 고른다:

| mode | 조건 | 30장 기준 실측 |
|---|---|---|
| `shared-volume` | 입출력이 전부 공유 파티션 | push 4ms / pull 0ms |
| `copy` | 그 외 (노드 로컬 등) | push 1.1~1.5s / pull 0.8~1.5s |

`result.json.offload.mode` 로 어느 쪽이었는지 확인할 수 있다. 데이터셋을 어디에
두느냐가 곧 전송 비용을 결정하므로, **pw 의 `inputPath`/`outputPath` 를 공유
파티션(`/mnt/newport_1/...`)으로 잡는 것이 기본**이다.

---

### 라벨 정합 — 리사이즈 기하를 어노테이션에 전달

stage1 이 이미지를 리사이즈하고 stage2 가 어노테이션을 변환하는데, **두 단계가 서로
다른 프로세스(심지어 다른 기계)에서 돈다.** 그래서 리사이즈가 이미지에 무슨 짓을 했는지
stage2 가 알 방법이 필요하다. 모르면 어노테이션이 원본 해상도 기준으로 남아 리사이즈된
이미지와 어긋난 데이터셋이 조용히 만들어진다.

| 단계 | 파일 | 역할 |
|---|---|---|
| stage1 `resize` | `resize_transform.json` 기록 | 파일별 letterbox/stretch 기하(스케일·패딩·원본크기) |
| stage2 `convert_annotation` | `_load_resize_transform()` 으로 읽음 | stage1 산출물 쪽에서 찾아 읽는다 |
| 변환 | `formats/coco.py: get_bboxes_yolo(transform=)` | bbox 를 이미지와 **같은 변환**에 통과 |

`center_crop` 은 단순 affine 이 아니라 잘려나간 박스 처리가 필요해서, 표시만 남기고
변환하지 않는다.

이 경로가 끊기면 증상이 눈에 잘 띄지 않는다 — 파일 개수도 맞고 학습도 돌아가는데
박스만 어긋난다. `server/test_stage1_smoke.py` 가 `resize_transform.json` 이 전체 이미지를
덮는지 확인하는 이유다.

---

## 2. 컴포넌트 책임 경계

| 컴포넌트 | 하는 일 | 하지 않는 일 |
|---|---|---|
| `server/csd_watcher.py` | 데이터 도착 감지·기록 | 전처리, 워크로드 생성 (둘 다 옵션 모드) |
| `controller/preprocess_manager.py` | 분할 **계획**(알고리즘·split_index·근거), 파이프라인 **템플릿 해석**, 라벨 정책 결정, pw↔pj status 역전파 | 실행, 스텝 개별 선택 |
| `controller/preprocess_controller.py` | 워커 Job 디스패치, 진행 감시, 샤드 결과 집계·전역 dedup, 라벨 게이트, stage2 디스패치, 통계 통합·정리 | 계획 재수립 |
| `worker/csd_worker.py` | 지정된 파이프라인 순차 실행, CSD 오프로드, records/result 기록 | 무엇을 실행할지 결정 |

상주 파드 3개: `csd-watcher`, `preprocess-manager`, `preprocess-controller`
(+ `csd-device-plugin` 이 `keti.re.kr/csd` 확장 자원 광고).

---

## 3. 파이프라인 정의 — 템플릿이 정본

```
config/pipeline_templates/stage1_raw_ingestion.yaml        ← 매니저가 해석
config/pipeline_templates/stage2_training_preparation.yaml
        ↓ [{op, params}] (params 포함)
pj.spec.preprocessing_pipeline / stage2_pipeline
        ↓ PREPROCESSING_STEPS env (JSON)
워커 — csd_preprocessor 연산 레지스트리에서 해석 → 순차 실행
```

- 워크로드마다 스텝을 고르지 않는다. pw 는 `pipelineTemplate` / `stage2Template`
  **이름만** 주거나 생략한다(매니저 기본값).
- params 까지 CR 에 실려 내려가므로 실행 위치와 무관하게 같은 설정으로 돈다.
  CSD 오프로드 시에도 해석된 파이프라인을 통째로 넘겨 CSD 측에 YAML·PyYAML 이
  필요 없다.
- `phash` 는 레지스트리 연산이 아니라 워커의 레코드 단위 후처리다
  (`RECORD_PHASH=true` 면 매니저가 stage1 끝에 붙인다). 이 pHash 가 집계 단계
  전역 dedup 의 입력이 된다.

---

## 4. 상태 전이

`kubectl get pj` 의 STATUS / STAGE / LABELS 컬럼으로 관측한다.
(관측을 CR status 로 설계한 것은 구 클러스터에서 node-ip(tap0) 문제로 `kubectl logs` 가
안 됐기 때문이다. 2026-08-18 재생성된 클러스터에서는 `kubectl logs` 도 정상이지만,
CR status 로 관측하는 설계는 그대로 둔다 — 로그 접근성에 기대지 않는 편이 안전하다.)

| STAGE | progress | 의미 |
|---|---|---|
| `shards` | 0 → 0.8 | stage1 샤드 병렬 |
| `waiting_labels` | 0.8 | 어노테이션 도착 대기 (기본 무한) |
| `stage2` | 0.8 → 1.0 | 샤드 완료 후 단일 패스 |

stage2 가 없으면(`stage2Template: none`) 샤드 단계가 0 → 1.0 을 그대로 쓴다.

**재시작 안전성**: 상태는 CR 과 파일에서 복원 가능하다.
- 샤드 집계 결과 → `_shards/shard_summary.json` (stage2 가 `statistics.json` 을 덮어쓰므로 분리)
- 라벨 대기 시작 시각 → `status.stage2.waiting_since`
- stage2 Job 이 사라지면 재디스패치

---

## 5. 실행 위치

| 작업 | 위치 | 이유 |
|---|---|---|
| stage1 CPU 샤드 | 노드 (`gpu-npu-server-02`) | 분할된 절반 |
| stage1 CSD 샤드 | **10.2.1.2 실장비** | 근접 데이터 처리 (공유 볼륨이면 복사 없이 제자리) |
| 결과 집계·전역 dedup | 컨트롤러 파드 | pHash 문자열 비교만 (이미지 재읽기 없음) |
| stage2 | **10.2.1.2 실장비** (`STAGE2_TARGET=CPU` 로 변경 가능) | 라벨링만 HOST, 전처리는 CSD |

stage2 가 샤드 병렬이 아닌 이유는 **라벨 의존 + 데이터셋 전역 연산**이기 때문이지
CPU 에서 돌아야 해서가 아니다. CSD 가 데이터셋 전체와 어노테이션을 읽어 처리하고
결과(train/val/test + data.yaml + statistics.json)를 낸다 — 공유 볼륨이면 제자리에서,
아니면 push/pull 로.
실행 위치는 `statistics.json.stage2.executed_on` 과
`_shards/<job>-stage2/result.json.offload` 에 남는다.

---

## 6. 정책 스위치

### 매니저 (`preprocess-manager`)

| env | 기본값 | pw 필드로 재정의 |
|---|---|---|
| `DEFAULT_PIPELINE_TEMPLATE` | `stage1_raw_ingestion` | `workload.pipelineTemplate` |
| `DEFAULT_STAGE2_TEMPLATE` | `stage2_training_preparation` | `workload.stage2Template` (`none` = 생략) |
| `ENABLE_STAGE2` | `true` | — |
| `WAIT_FOR_LABELS` | `true` | `workload.waitForLabels` |
| `ALLOW_PLACEHOLDER_LABELS` | `true` | — |
| `ANNOTATION_DIRNAME` | `annotations` | `dataset.labelPath` (미지정 시 `<inputPath>/../annotations`) |
| `DEFAULT_ALGORITHM` | `AUTO` | `workload.algorithm` (AUTO/STATIC/MTE/WRR) |
| `AUTO_SMALL_DATASET` | `100` | — (이하면 소규모로 보고 MTE, §12) |
| `AUTO_BALANCED_RATIO` | `3.0` | — (성능비가 이하면 MTE, §12) |
| `WRR_MAX_CYCLE` | `20` | — (WRR 라운드 길이 상한, §12) |
| `DEFAULT_CPU_RATIO` | `0.5` | `workload.throughput` / `weights` |
| `RECORD_PHASH` | `true` | — |
| `THROUGHPUT_SOURCE` | `auto` | `workload.throughputSource` (auto=실측 프로파일 우선, spec=CR 값 강제) |
| `THROUGHPUT_METRIC` | `effective` | — (effective=전송 포함 실효, compute=CSD 내부 처리만) |
| `THROUGHPUT_EWMA_ALPHA` | `0.5` | — |
| `THROUGHPUT_MIN_SAMPLES` | `5` | — (이보다 작은 샤드는 측정 제외) |
| `THROUGHPUT_CONFIGMAP` | `preprocess-throughput-profile` | — |
| `DATASET_PIXEL_DRIFT` | `1.5` | — (학습 당시와 평균 입력 해상도가 이 배수 넘게 다르면 프로파일 무시·리셋. **0 이하 = 가드 끔**) |
| `DATASET_SAMPLE_FILES` | `16` | — (계획 시 해상도 추정에 헤더만 읽을 표본 수) |
| `CALIBRATION_CPU_RATIO` | `DEFAULT_CPU_RATIO` | — (프로파일 없을 때 MTE 보정 실행 비율) |

### 실행엔진 (`preprocess-controller`)

| env | 기본값 | 설명 |
|---|---|---|
| `STAGE2_TARGET` | `CSD` | stage2 실행 위치 (CSD / CPU) |
| `LABEL_WAIT_TIMEOUT` | `0` | 라벨 대기 제한(초), 0 = 무한 |
| `CSD_REMOTE_HOST` / `CSD_REMOTE_PASS` | `root@10.2.1.2` / (시크릿) | 비밀번호는 `csd-credentials` 시크릿에서 주입. 없으면 CSD 오프로드 불가 |
| `CSD_REMOTE_REPO` | `/home/ngd/storage/csd_preprocessing` | CSD 측 코드 경로. 워커 기본값에 기대지 않고 컨트롤러가 명시 전달 |
| `CSD_RESOURCE_NAME` | `keti.re.kr/csd` | 디바이스 플러그인 확장 자원 |
| `NODE_HOSTNAME` / `WORKER_IMAGE` | `gpu-npu-server-02` / `csd-preprocessor:latest` | 워커 Job 배치 |

**CSD 인증은 비밀번호(sshpass) 전용이다.** 공개키 인증은 `PubkeyAuthentication=no` 로
명시적으로 끈다 — 실행 주체(서버 셸 / 워커 컨테이너 / 다른 노드)마다 키가 있기도 없기도
해서, 켜 두면 키가 있는 자리에서만 조용히 성공해 환경마다 다르게 동작한다. 비밀번호가
비어 있으면 워커는 **즉시 실패한다**(조용한 폴백 없음).

### 워커 (Job env — 컨트롤러가 주입)

`PREPROCESSING_STEPS`(파이프라인 JSON), `LABEL_PATH`, `PLACEHOLDER_LABELS`,
`BATCH_MANIFEST_JSON`, `DATA_PATH`, `OUTPUT_DIR`, `DATASET_DIR`, `WORKER_TYPE`,
`CSD_REMOTE_*`, `CSD_REMOTE_EXEC_TIMEOUT`(기본 1800초).

`CSD_REMOTE_PYTHONPATH`(기본 `/home/ngd/storage/pylibs`)는 CSD 의 numpy/OpenCV 경로다.
CSD 는 최소 rootfs 라 복구할 때마다 `dist-packages` 가 초기화된다(2026-08-11 실제 발생).
그래서 패키지를 공유 OCFS2 파티션에 두고 실행 시점에 `PYTHONPATH` 로 얹는다 —
rootfs 가 리셋돼도 CSD 는 그대로 돈다.

이미지 병렬 처리(§11):

| env | 기본값 | 설명 |
|---|---|---|
| `WORKER_THREADS` | (자동) | 미지정 시 cgroup CPU 할당량 → `os.cpu_count()` 순으로 판정 |
| `WORKER_THREADS_MAX` | `8` | 자동 판정 상한 |

공유 볼륨 인플레이스 판정용:

| env | 기본값 | 설명 |
|---|---|---|
| `CSD_SHARED_LOCAL_ROOT` | `/mnt/newport_1` | 서버가 보는 공유 파티션 마운트 지점 |
| `CSD_SHARED_REMOTE_ROOT` | `/home/ngd/storage` | CSD 가 보는 같은 파티션 |
| `HOST_DATA_PATH` / `HOST_DATASET_DIR` / `HOST_LABEL_PATH` | — | 컨트롤러가 주입하는 **노드 경로** |

> 컨테이너 안에서는 `/data`·`/dataset` 같은 마운트 경로만 보여서 공유 볼륨 여부를
> 판정할 수 없다. 그래서 컨트롤러가 노드 경로를 `HOST_*` 로 따로 넘긴다
> (`OUTPUT_HOST_DIR` 과 같은 관례). 단독 실행 시에는 실제 경로를 그대로 쓴다.

### csd-watcher 실행 모드

| 모드 | 인자 | 동작 |
|---|---|---|
| watch (기본) | 없음 | 도착 감지 기록만. 워크로드는 스케줄러가 생성 |
| workload | `--emit-workload --host-base-dir <노드경로>` | 감지 → pw 자동 제출. **스케줄러와 동시 사용 금지** |
| legacy | `--legacy-inprocess` | 프로세스 안에서 Stage 1/2 직접 실행 (k8s 없는 CSD 단독 데모) |

> `--host-base-dir` 는 워커 Job 이 hostPath 로 마운트할 **노드 경로**다. 컨테이너가
> 보는 `--base-dir`(`/storage/...`) 와 다르므로 workload 모드에서는 반드시 지정한다.

---

## 7. 안전장치

| 장치 | 막는 문제 |
|---|---|
| 구버전 CSD 감지 (`preprocessingSteps` 미보고 검사) | CSD 가 파이프라인을 무시해 샤드 간 전처리가 달라지는 것 |
| 샤드 밖 파일 유입 감지 | 연산이 입력 디렉터리를 재스캔해 모든 워커가 전체를 중복 처리하는 것 |
| 일관성 검증 (샤드 입력 합계 = 전체) | 샤드 누락·중복 |
| 임시 라벨 표식 6곳 | 학습 불가 데이터셋을 실 데이터로 오인하는 것 |
| CSD 확장 자원 점유 (`DEVICE_COUNT=1`) | CSD 동시 접근 (자연 직렬화), 장치 불통 시 스케줄 차단 |
| 워크로드 생산자 단일화 | 같은 입력에 pw 중복 생성 |
| 알 수 없는 스텝 즉시 실패 | 오타가 조용히 무시되는 것 |
| 인플레이스 진입 전 원격 경로 확인 (`ssh test -d`) | CSD 가 공유 경로를 못 보는데 제자리 실행을 시도하는 것 (실패 시 복사 방식으로 폴백) |
| 비밀번호 없으면 즉시 실패 (`PubkeyAuthentication=no`) | 키가 있는 자리에서만 조용히 성공해 환경마다 다르게 도는 것 |
| CSD 코드 사본 해시 대조 (`csd_healthcheck.py`) | 워킹트리만 고치고 CSD 사본을 안 올려 옛 코드가 도는 것 |
| 배포 이미지 코드 해시 대조 (`test_k8s_integration.py`) | 이미지를 다시 굽지 않아 파드가 옛 코드를 도는 것 |
| 리사이즈 기하 전달 (`resize_transform.json`) | 어노테이션이 원본 해상도 기준으로 남아 이미지와 어긋나는 것 (§1) |

### 실행되는 코드 사본은 세 벌이다

이 구조에서 같은 코드가 세 군데에 존재하고, **각각 따로 갱신해야 한다.**
이게 이 시스템에서 가장 조용히 틀리는 지점이라 감지 장치를 각각 붙였다.

| 사본 | 누가 실행 | 갱신 | 드리프트 감지 |
|---|---|---|---|
| 워킹트리 | 로컬 CLI·테스트 | (원본) | — |
| 공유 볼륨 `/mnt/newport_1/csd_preprocessing` | CSD 가 SSH 로 진입해 실행 | `./k8s/deploy.sh csd-sync` | 헬스체크 `code copy in sync` |
| 컨테이너 이미지 | k8s 파드(컨트롤러·매니저·워커) | `./k8s/deploy.sh image` | k8s 회귀 시작 시 파드 내 해시 대조 |

CSD 사본이 어긋나면 워커가 "구버전 CSD" 로 감지해 실패시키지만(위 표),
그건 **파이프라인 계약이 바뀐 경우만** 걸린다. 해시 대조는 그보다 넓게 잡는다.

---

## 8. 산출물 구조

```
pj_out/<워크로드명>/
├── train|val|test/
│   ├── images/                  전처리된 이미지
│   └── labels/                  YOLO 라벨 (.txt)
├── data.yaml                    학습 설정 (placeholder 라벨이면 경고 주석 + label_source)
├── statistics.json              정본 통계
│   ├── (stage2 데이터셋 통계: dataset_summary, class_distribution, bbox_statistics …)
│   ├── shard_summary            전역 mean/std, 일관성 검증, global_dedup, 파이프라인 기록
│   └── stage2                   템플릿·스텝·label_source·splits·classes·executed_on
├── _duplicates/                 전역 dedup 이 걸러낸 중복 (삭제 아님 — 증빙 보존)
└── _shards/
    ├── <job>-cpu|csd|stage2/    records.jsonl + result.json
    └── shard_summary.json       샤드 결과 집계 (재시작 복원용)
```

`records.jsonl` 한 줄 = 입력 파일 하나:
`{index, queuePosition, filename, phash, resized, status, [droppedBy]}`.
`droppedBy` 로 어느 스텝이 파일을 탈락시켰는지 구분한다.

---

## 9. 알려진 제약

1. **스케줄러 연동 미완** — pw CRD 는 스케줄러 파트 스펙 확정 전 draft 이고,
   현재 운영은 `kubectl apply` 수동이다. 자동 인입이 필요하면
   `csd_watcher --emit-workload`(스케줄러와 배타 사용).
2. **전역 mean/std 에 중복 픽셀 포함** — 샤드가 집계값만 보고하므로 전역 dedup 으로
   제거된 파일의 기여분을 차감할 수 없다. `shard_summary.global_mean_includes_duplicates`
   로 표시한다. 엄밀한 값이 필요하면 stage2 템플릿 앞에 `- op: normalize` 추가
   (이미지 1패스 추가 비용).
3. **legacy 모드는 경로 이원화를 되살린다** — 운영 경로와 병행하면 같은 데이터를
   두 번 처리하고 노드 CPU 경합으로 KPI 측정이 오염된다. k8s 운영 중 사용 금지.
4. **분할계획의 비용 모형이 선형이다** — MTE/WRR 은 처리량(samples/s)만 쓰므로
   `시간 = n / tp` 를 가정하지만, 실측 벽시계 시간은 **고정비 + 한계비용**의
   affine 구조다(8건 회귀):

   | 측정 시점 | CPU (노드) | CSD `shared-volume` | CSD `copy` |
   |---|---|---|---|
   | 최적화 전 | `344 + 53.4×n` | `1250 + 305×n` | `2920 + 324×n` |
   | normalize 최적화 후 | `605 + 23.1×n` | `1227 + 164×n` | (미측정) |

   고정비는 CPU 0.3~0.6초(파드 기동), CSD shared-volume 약 1.23초(SSH 세션 + ARM
   파이썬·OpenCV 기동), CSD copy 약 2.92초(+ scp push/pull)다.
   분할 손익분기는 각각 shared-volume 약 27장, copy 약 48장이었다.

   > **파일 병렬화(§11) 이후로는 affine 계수를 재적합하지 않았다.** 검증용 산출물을
   > 정리했기 때문이다. 현재 확보된 값은 (a) 학습된 실효 처리량 — shared-volume
   > 기준 CPU 41.1/s, CSD 6.21/s (비율 6.62, 5개 잡 EWMA), (b) 순수 연산 실측 —
   > CPU 약 10ms/장, CSD 44.5ms/장 이다. 고정비는 병렬화의 영향을 받지 않으므로
   > 위 표의 고정비는 그대로 유효하다.

   공유 파티션 전환으로 전송 고정비는 사실상 사라졌다(push 5ms / pull 0ms,
   copy 는 push 1.1~2.1s + pull 0.7~1.5s). 남은 1.25초는 SSH 세션과 CSD(ARM)
   측 파이썬·OpenCV 기동이라 데이터 위치로는 더 줄지 않는다.
   그래서 처리량 하나로는 여전히 완료 시점이 어긋난다 — 30장 기준 실측에서
   CPU 약 1.0초, CSD 약 2.8초로 3배 가까이 벌어졌다.
   또한 실효 처리량이 샤드 크기에 의존하므로(n=5 → 1.79/s, n=15 → 2.57/s)
   EWMA 가 하나의 값으로 수렴하지 못한다.

   **다만 규모가 커지면 이 문제는 사라진다.** 고정비가 전체에서 차지하는 비중이
   `a/(a+b·n)` 로 희석되기 때문이다. COCO val2017 5000장 실측(§11)에서 측정 루프는
   3회 만에 수렴했다:

   | 실행 | 분할 CPU/CSD | CPU wall | CSD wall | 완료시점 차 |
   |---|---|---|---|---|
   | 1회차 | 4334 / 666 | 159.8s | 93.6s | 66.2s |
   | 2회차 | 4167 / 833 | 76.2s | 57.6s | 18.6s |
   | 3회차 | 4064 / 936 | 76.8s | 65.2s | 11.6s |
   | 4회차 | 4000 / 1000 | 73.1s | 69.3s | **3.8s** (실행시간의 4%) |

   실측 처리량도 안정적이었다 — CPU 54.7/52.9/54.7, CSD 14.5/14.4/14.4 samples/s
   (비율 3.79). 즉 **affine 확장 없이도 대규모에서는 완료시점이 맞는다.**
   남은 3.8초는 모형 오차가 아니라 WRR 정수 가중치의 양자화 잔차다(§12).
   affine 모형은 소규모(수백 장 이하)를 자주 돌릴 때만 값어치가 있다.
5. **MTE/WRR 검증 범위** — 실 클러스터에서 분할·디스패치·오프로드·병합까지 E2E
   성공을 확인했다: copy 모드 3건, shared-volume 모드 9건, 알고리즘 자동 선택 2건
   (소규모 30장 → MTE / 대규모 110장 → WRR).
   **규모 검증**: COCO val2017 5000장 + 실 어노테이션(36,781건)으로 stage1→stage2
   전 구간 성공. 산출 데이터셋은 train 4952 / val 743 / test 247(증강 1000장 포함),
   YOLO 라벨 개수 일치, 80개 클래스 `data.yaml`, 바운딩박스 54,103개.
   stage2 는 실 CSD 에서 `mode=shared-volume` 으로 실행됐다.
   위 검증들은 2026-08-14 이전 클러스터에서 이뤄졌고 그 클러스터는 소멸했다 —
   당시 남아 있던 검증 워크로드(`mte-test-001` 등)도 함께 사라졌다.
   **현재 클러스터(8/18 재생성)에서 재확인 완료**: STATIC E2E 완주
   (`server/test_k8s_integration.py`) + MTE/WRR/AUTO E2E
   (`server/test_k8s_algorithms.py`) — MTE 는 연속 분할(split_index>0), WRR 은
   비연속(split_index=0, 가중치 15:4), AUTO 는 소규모 갈래로 MTE 선택.
   AUTO 의 나머지 세 갈래는 실측 처리량에 좌우돼 E2E 로 고정할 수 없어
   `server/test_partition_algorithms.py` 가 결정론적으로 덮는다.
   다만 **WRR 은 실행 중 재분배가 아니라 디스패치 시점의 정적 인터리브 분할**이다 —
   가중치를 바꾸면 다음 잡부터 반영되고, 진행 중인 잡은 재분배하지 않는다.
6. **작은 데이터셋의 stratified split** — 클래스당 1~2장이면 계층 분할의 의미가
   약하다(전체 비율은 §10 수정으로 보장됨).
7. **실행되는 코드 사본이 세 벌이다**(§7) — 워킹트리·공유 볼륨·컨테이너 이미지.
   각각 따로 갱신해야 하고, 드리프트는 **검사를 돌려야** 드러난다. 특히 이미지 쪽은
   k8s 회귀를 돌리기 전까지 파드가 조용히 옛 코드를 돈다.
8. **회귀가 도는 계기가 배포뿐이다** — 회귀 3층은 `./k8s/deploy.sh verify` 로 묶여
   있고 `all`·`image` 뒤에 자동 실행되지만, 코드만 고치고 배포하지 않으면 아무것도
   돌지 않는다. 저장소에 커밋 훅을 걸 CI 가 아직 없다.
9. **kubeconfig 가 저장소 밖 경로에 묶여 있다** — 컨트롤러·매니저가
   `/root/preprocess-isolation/preprocess-csd.kubeconfig` 를 hostPath 로 마운트한다.
   경로가 Deployment 매니페스트에 박혀 있어 노드를 바꾸면 같이 손봐야 하고,
   클러스터를 재생성하면 토큰이 죽어 재발급이 필요하다(`./k8s/deploy.sh kubeconfig`).
10. **데이터셋 위치가 전송 비용을 결정한다** — 공유 OCFS2 파티션
   (`/mnt/newport_1/...`)에 두면 복사 없이 제자리 실행(`mode=shared-volume`)이지만,
   노드 로컬(`/home/ngd/storage/csd_preprocessing/...`)에 두면 매 실행마다 scp
   push/pull 이 발생한다(`mode=copy`, 30장 기준 약 5초). 기존 노드 로컬 데이터도
   그대로 동작하지만 새 워크로드는 공유 파티션 경로를 쓰는 것을 권장한다.

---

## 10. 설계 결정 기록

이 구조에 오기까지 고친 문제들. 같은 함정을 다시 밟지 않기 위해 남긴다.

| 문제 | 증상 | 해결 |
|---|---|---|
| `preprocessing_steps` 가 워커까지 전달 안 됨 | 스텝 선언이 실행에 반영되지 않음 | 컨트롤러가 `PREPROCESSING_STEPS` env 로 전달 |
| 스텝 이름만 전달 (params 없음) | 서버 경로와 설정 불일치 가능 | 템플릿을 `[{op, params}]` 로 해석해 CR 에 실음 |
| `validate` 가 샤드 무시하고 입력 디렉터리 재스캔 | **모든 워커가 전체 데이터셋을 중복 처리** | `ctx.valid_files` 가 차 있으면 그것만 검사 + 워커 측 방어선 |
| `deduplicate` 샤드-로컬 | 샤드 경계 넘는 중복 미제거 → 분할 방식에 따라 결과가 달라짐 | 집계 단계에서 pHash 전역 비교 (`_duplicates/` 로 이동) |
| `split` 이 dict 순서에 의존 | 쿠버네티스가 map 을 키 정렬 저장 → **경로별로 다른 분할 결과** | `ordered_splits()` 로 train→val→test 고정 |
| `split` 작은 그룹의 나머지 쏠림 | 30장에서 train 22 / val 14 / **test 0** | 최대잉여법 + 그룹 간 소수부 이월 → 24/4/2 |
| `normalize` 없을 때 KeyError | 빈 샤드에서 집계 실패 | `partial` 기본값 0 |
| stage2 가 CSD 오프로드 차단 | 라벨 의존을 이유로 CPU 강제 | 라벨을 CSD 로 함께 push, `STAGE2_TARGET=CSD` |
| 진입점 이원화 | csd-server 와 분산 경로가 같은 데이터를 각자 처리 | watcher 를 감지 전용으로 축소 |
| CSD 오프로드가 매번 데이터 복사 | 30장에 push/pull 약 5초, 데이터 크기에 비례 | 공유 볼륨이면 경로만 번역해 제자리 실행 (복사 방식 폴백 유지) |
| 워커가 컨테이너 경로만 봄 | 공유 볼륨인데도 `mode=copy` 로 떨어짐 | 컨트롤러가 `HOST_*` 노드 경로를 주입 |
| 어노테이션이 원본 해상도 기준 (2026-08-06) | 파일 수도 맞고 학습도 도는데 **박스만 어긋남** | stage1 이 `resize_transform.json` 을 남기고 stage2 가 같은 변환에 bbox 를 통과 (§1) |
| CSD 재구축으로 코드 경로 변경 (2026-08-14) | `CSD_REMOTE_REPO` 기본값이 없는 경로(`csd-based-preprocessing`)를 가리켜 원격 실행 실패 | 기본값을 `csd_preprocessing` 으로 정정하고 컨트롤러가 명시 전달 |
| 공개키 인증이 자리마다 다르게 동작 | 서버 셸에선 되고 워커 컨테이너에선 실패 | 비밀번호 전용으로 통일, `PubkeyAuthentication=no` 로 폴백 차단 |
| 코드 경로를 공유 볼륨 루트에서 파생 | **데이터 마운트 지점을 바꾸면 코드 경로까지 따라 이동** | `DEFAULT_REMOTE_REPO`/`DEFAULT_REMOTE_WORKDIR` 를 리터럴로 분리 (실패 경로 회귀가 잡아냄) |
| RBAC Role 의 CR 그룹이 옛 이름(`batch.csd.io`) | 컨트롤러가 자기 CR 을 못 읽음(403) | `k8s/namespace-rbac.yaml` 로 저장소에 편입하며 `edgeai.keti.re.kr` 추가 |
| device-plugin 없이 워크로드 제출 | CSD 샤드 파드가 `Insufficient keti.re.kr/csd` 로 Pending | `deploy.sh plugin` 이 광고될 때까지 대기 후 진행 |

---

## 11. 전처리 연산 성능 최적화

파이프라인 6스텝의 실측 분해(30장, 640×640 기준):

| 스텝 | CSD(ARM) 개선 전 | CPU 개선 전 |
|---|---|---|
| validate | 16.1ms/장 | 2.6ms/장 |
| deduplicate | 20.1ms/장 | 2.3ms/장 |
| filter_quality | 44.5ms/장 | 5.8ms/장 |
| resize | 35.6ms/장 | 5.3ms/장 |
| **normalize** | **154.3ms/장 (57%)** | **21.8ms/장 (58%)** |

원인은 병렬성 부족이 아니었다 — OpenCV 는 CSD 에서 이미 4스레드를 쓰고 있었다
(decode+resize 병렬도 2.13×). `normalize` 가 이미지마다 `astype(np.float64)` 로
9.8MB 배열을 만들고 제곱 배열을 또 만들어 5번 넘게 훑은 것이 병목이었다.

`cv2.meanStdDev`(C++ 1패스)로 채널별 평균/표준편차를 구하고 다음 항등식으로
누적하도록 바꿨다 — 산술적으로 동일하다:

```
sum   = mean × p
sumsq = (std² + mean²) × p          (std 는 모표준편차)
```

`filter_quality` 도 같은 성격이었다 — 세 지표 함수가 각자 `cvtColor` 를 호출해
그레이 변환을 이미지당 3번 했고, `cv2.Laplacian(CV_64F).var()` 는 numpy 로 두 번
훑었다. 그레이 변환 1회 + `cv2.meanStdDev` 1패스로 합쳤다(판정 결과 동일).

스텝별 실측(30장, 0.28MP, 파드 기동 제외한 순수 연산 시간):

| 스텝 | CSD 전 | CSD 후 | CPU 전 | CPU 후 |
|---|---|---|---|---|
| validate | 16.1 | 16.2 | 2.6 | 3.2 |
| deduplicate | 20.1 | 20.1 | 2.3 | 3.1 |
| filter_quality | 44.5 | **26.8** | 5.8 | **3.9** |
| resize | 35.6 | 35.4 | 5.3 | 5.3 |
| normalize | 154.3 | **19.3** | 21.8 | **3.4** |
| **합계** | **270.6ms/장** | **117.8ms/장 (2.3×)** | **37.8ms/장** | **18.9ms/장 (2.0×)** |

### 파일 단위 병렬화

OpenCV 는 연산 내부에서 이미 4스레드를 쓰지만(CSD 에서 decode+resize 병렬도 2.13×)
그것만으로는 코어가 남는다 — 디코딩과 파이썬 레벨 처리가 섞여 있기 때문이다.
여러 장을 동시에 흘리자 남는 코어가 채워졌다(`csd_preprocessor/core/parallel.py`).

각 연산의 이미지 단위 작업만 `map_images` 로 병렬 실행하고 **취합은 순차로** 둔다
(부동소수 누적 순서가 바뀌면 결과가 달라진다). 스레드 수는 cgroup 할당량을 읽어
정한다 — `os.cpu_count()` 는 호스트 코어를 돌려줘서 cpu limit 2 인 파드가 8스레드를
띄우게 된다.

| 단계 | CSD(ARM 4코어) | CPU(컨테이너 2코어) |
|---|---|---|
| 최초 | 270.6ms/장 | 37.8ms/장 |
| normalize 최적화 | 117.8 (2.3×) | 18.9 (2.0×) |
| + filter_quality | 117.8 | 18.9 |
| + 파일 병렬화 | **44.5 (2.65×)** | **~10 (2.0×)** |
| **누적** | **6.1×** | **3.8×** |

병렬화 후 스텝별(CSD): validate 10.6 / deduplicate 8.4 / filter_quality 8.5 /
resize 10.9 / normalize 6.0 ms/장.
검증: 순차 실행과 모든 스텝 메트릭·`valid_files`·전역 mean/std 가 완전 일치
(raw 0.28MP, 1080p 두 데이터셋).

> CPU 워커 Job 은 `resources.limits.cpu: 2` 라 2배에 그친다. 노드에 여유가 있으면
> 이 한도를 올리는 만큼 더 빨라진다 — 자원 정책 결정이라 기본값은 그대로 뒀다.

### deduplicate — 규모에서 드러난 O(n²)

중복 판정은 pHash 비트열의 해밍 거리가 임계값 이하인 쌍을 찾는 것이다. 정확 일치가
아니라 "5비트 이내"라 해시 버킷으로 줄일 수 없고 **모든 쌍을 비교**해야 한다.
파일 수의 제곱으로 늘어나므로 30~110장 규모에서는 보이지 않았다(쌍 6천 개).

거리 계산이 `sum(c1 != c2 for c1, c2 in zip(a, b))` — 256자를 파이썬으로 한 글자씩
비교했다. 쌍당 8.9µs(x86) / 232µs(CSD ARM)로, CSD 가 26배 느린 것은 순수 파이썬이기
때문이다(C++ 연산에서는 4~5배 차이였다).

비트열을 **정수로 한 번만** 변환해 두고 `XOR + popcount` 로 센다. 정수 변환을 쌍마다가
아니라 파일마다 하는 것이 핵심이다. CSD 는 python 3.8 이라 `int.bit_count()`(3.10+)가
없어 `bin(x).count("1")` 폴백을 쓴다.

| | 개선 전 | 개선 후 | |
|---|---|---|---|
| 쌍 비교 단가 | 8.9µs | 0.15µs | 59× |
| deduplicate (CPU 샤드, n=4334) | 22.7ms/장 | 1.4ms/장 | 16× |
| deduplicate (CSD 샤드) | 85.5ms/장 | 8.1ms/장 | 10.6× |
| stage1 중 dedup 비중 | 67.3% | 11.7% | |
| **5000장 전체 잡** | **411초** | **105초** | **3.9×** |

값 검증: 17.9만 쌍에서 기존 구현과 불일치 0건, 길이가 다른 해시는 기존 경로로 폴백.

> **복잡도는 그대로다** — 상수만 59배 줄였다. 5000장은 1250만 쌍 ≈ 1.9초로 무해하지만
> train2017(118k장)은 70억 쌍 ≈ 17분이 된다. 그 규모를 쓸 거라면 BK-tree(해밍 공간
> 인덱스)나 LSH 버킷으로 복잡도 자체를 낮춰야 한다.

### stage2 — split 하드링크, augment 병렬화

stage1 을 6.1배 줄이자 stage2 가 전체의 2/3 를 차지하게 됐다(5000장 실측: stage1 85초
vs stage2 222초). 두 스텝이 지배적이었다.

- **`split` 94.3초** — 5942장을 `shutil.copy2` 로 복제했다. 그런데 입력
  (`<out>/images`)과 출력(`<out>/train|val|test/images`)이 **같은 OCFS2 파티션**이라
  복사할 이유가 없다. `os.link` 는 디렉터리 엔트리만 추가하므로 데이터가 움직이지
  않는다(교차 파일시스템이면 복사로 폴백).
  분할 직후 컨트롤러가 중간 디렉터리를 지우는데, 하드링크는 링크 수만 줄어들 뿐이라
  train/val/test 파일은 그대로 남는다. 이후 이 이미지를 제자리에서 고치는 단계가
  없으므로 링크 공유는 문제되지 않는다.
- **`augment` 116.2초** — 모자이크 1000장, 각각 4장을 읽어 합성한다. 파일 단위
  병렬화가 안 돼 있어 CSD 4코어 중 1개만 썼다. 다만 헬퍼들이 전역 `random` 을
  내부에서 쓰므로(모자이크 중심점, cutout 위치, mixup 의 `np.random.beta`) 그대로
  병렬화하면 RNG 상태를 경합한다. **추첨(순차) → 실행(병렬) → 취합(순차)** 으로
  나누고, 각 항목에 `seed` 파생 RNG 를 주입했다.

| 스텝 | 개선 전 | 개선 후 | |
|---|---|---|---|
| convert_annotation | 5.9s | 5.7s | — |
| **augment** | **116.2s** | **36.0s** | 3.2× |
| **split** | **94.3s** | **67.1s** | 1.4× |
| statistics | 2.5s | 2.5s | — |
| **stage2 합** | **218.9s** | **111.3s** | **2.0×** |
| 잡 전체(5000장) | 335s | 252s | 1.3× |

> **`split` 이 1.4배에 그친 이유** — 남은 67초의 대부분은 이미지 배치가 아니라
> **라벨 파일 5942개 쓰기**다(YOLO TXT 를 파일당 하나씩 생성). OCFS2 는 클러스터
> 파일시스템이라 파일 생성 비용이 크다. 하드링크가 없앤 것은 데이터 복사분
> 약 27초이고, 나머지는 메타데이터 연산이다. 더 줄이려면 라벨 쓰기도 병렬화해야
> 한다 — 미적용.

> **augment 는 출력이 이전과 다르다** — 이번 절의 다른 최적화들과 달리 값이
> 동일하지 않다. 예전에는 전역 RNG 스트림에서, 지금은 항목별 스트림에서 난수를
> 뽑기 때문이다. 재현성은 오히려 강해져 **스레드 수와 무관하게 같은 seed 면 같은
> 결과**가 나온다(순차 vs 병렬 md5 일치 확인). 증강본은 합성 데이터라 정본이 없다.

### 남은 병목 — 디코딩

**아직 스텝마다 이미지를 다시 읽는다.** CSD 에서 `cv2.imread` 만 컬러 15.6ms / 그레이 10.1ms 인데,
스텝마다 각자 이미지를 다시 읽으므로 장당 5회 디코드 = 약 72ms — 개선 후 CSD
파이프라인(117.8ms)의 **약 61%** 다. 특히 `validate` 는 무결성 확인만 하는데 전체
디코드를 하므로 사실상 100% 가 디코드다.
없애려면 실행 순서를 스텝 우선(step-major)에서 파일 우선(file-major)으로 바꿔
한 번 디코드한 배열을 스텝들이 넘겨받아야 한다 — 전 이미지를 메모리에 올리는
캐시는 대규모 데이터셋에서 불가능하므로(5000장 × 1.2MB) 스트리밍 구조가 필요하다.

다만 **전면 융합은 안전하지 않다.** `deduplicate` 는 중간에 데이터셋 전역 비교가
들어가고, pHash 를 컬러 디코드 후 `cvtColor` 로 구하면 `IMREAD_GRAYSCALE` 로 구한
값과 최대 2비트 달라진다(30장 중 3~4장). 중복 판정과 기록 증빙이 바뀌므로 그 구간은
합칠 수 없다. 안전한 구간(filter_quality→resize→normalize)만 융합한 프로토타입은
CSD 82.0 → 64.7ms/장 으로 **전체의 17%** 에 그쳐, 실행 경로를 이원화할 만한 이득이
아니라고 보고 채택하지 않았다(같은 노력으로 파일 병렬화가 2.65× 를 냈다).

검증: raw(0.28MP)·resized(640²)·1080p 세 데이터셋에서 기존 구현과 mean/std 오차
5e-7 미만(보고값은 소수점 6자리 반올림이라 동일), 전역 통계도 이전 잡과 일치.

**주의 1**: 양쪽이 비슷한 비율로 빨라져 CPU:CSD 장당 비용비는 7.0 → 7.2 로 거의
그대로다. 즉 **분할 이득 천장(약 12%)은 변하지 않았고, 절대 시간이 절반 이하가 됐다.**
CPU 가 함께 빨라진 만큼 분할 손익분기는 오히려 17장 → 27장으로 올라갔다.

**주의 2 — 클러스터 잡에서 잰 CPU 계수는 이제 신뢰할 수 없다.** CPU 장당 비용이
파드 기동(약 1초)에 비해 너무 작아져서, 30장 규모에서는 wall time 이 샤드 크기와
거의 무관해진다(n=15/20/23 → 1052/1077/1087ms). 3점 회귀가 기울기를 못 잡아
`225장/s` 같은 허수가 나온다 — §9-4 의 CPU 계수는 이 규모에서 참고치일 뿐이고,
의미 있는 값을 얻으려면 샤드가 수백 장 이상이어야 한다. CSD 계수는 장당 비용이
커서 아직 유효하다(filter_quality 개선이 164→153ms 로 예측대로 반영됐다).

---

## 12. 분할 알고리즘 자동 선택

`spec.workload.algorithm` 이 `AUTO`(매니저 기본값)면 워크로드 특성으로 MTE/WRR 을
고른다. 판단은 계획 시점 1회이며 근거는
`status.partition_plan.basis.algorithm_selection` 에 임계값·실제값과 함께 남는다.

| 순서 | 조건 | 선택 | 이유 |
|---|---|---|---|
| 1 | `N <= AUTO_SMALL_DATASET` (100장) | MTE | WRR 의 정수 가중치 양자화가 소규모에서 분할을 왜곡 |
| 2 | 실측 처리량 없음 | MTE | 판단 근거가 없으면 오버헤드가 낮은 쪽 |
| 3 | 성능비 `<= AUTO_BALANCED_RATIO` (3.0) | MTE | 연속 분할로 충분 |
| 4 | 그 외 (대규모 + 편차 큼) | WRR | 인터리브 배정 |

조건은 위에서부터 평가한다. 소규모와 "편차 큼"이 동시에 성립하면 **MTE 가 이긴다** —
양자화 왜곡이 표본 치우침보다 크고 오버헤드도 낮기 때문이다. 성능비는 처리량 측정
프로파일에서 오므로, 프로파일이 비어 있는 첫 잡은 항상 MTE 로 간다(보정 실행).

### 임계값 근거

비율이 커지면 느린 쪽(CSD) 샤드가 작아진다. MTE 는 그 몫을 **연속 블록**(tail)으로
주는데, 파일 비용이 데이터셋 순서에 따라 다르면 작은 연속 블록은 전체 평균에서
치우친다. 같은 크기를 인터리브로 뽑으면 표본이 대표성을 갖는다.

COCO 30장(장당 픽셀 변동계수 21.3%) 기준, 느린 쪽 몫 크기별 평균 대비 편차:

| 느린 쪽 몫 | MTE (연속 tail) | WRR (인터리브) |
|---|---|---|
| 3장 | −13.2% | +5.0% |
| 5장 | −13.9% | +1.8% |
| 10장 | −4.6% | +6.8% |
| 15장 | −3.8% | −4.8% |

몫이 10장(비율 약 2) 부터는 차이가 사라진다 → 교차점을 **비율 3** 으로 잡았다.
해상도가 균일한 데이터셋에서는 양쪽 모두 편차 0% 이므로, 이 임계값은 파일 비용이
고르지 않은 데이터셋에서만 의미가 있다.

### WRR 가중치는 근사해야 한다

`resolve_weights` 는 처리량 비를 정수 가중치로 바꾼다. 기약분수(gcd)만 쓰면
실측값이 서로소라 그대로 남는다:

```
tp 13.474 : 2.261  →  13474 : 2261   (cycle 15735)
                   →  30장 잡에서 i % 15735 < 13474 이 항상 참
                   →  CSD 에 한 장도 배정되지 않는다
```

이 결함은 `weights` 를 명시로 넘기는 테스트에서는 드러나지 않았다(자동 유도 경로만
깨져 있었다). `approx_ratio_weights()` 로 라운드 길이가 `WRR_MAX_CYCLE`(20) 이하인
근사 중 상대오차가 가장 작은 것을 고른다. 비율 자체가 상한을 넘으면(예 50:1)
`(round(r), 1)` 로 라운드를 늘린다 — 잘라내면 느린 쪽에 과도한 몫이 간다.

| 처리량 비 | 근사 가중치 | cycle | 오차 |
|---|---|---|---|
| 13.474 : 2.261 | 6 : 1 | 7 | 0.7% |
| 43.3 : 6.09 | 7 : 1 | 8 | 1.5% |
| 100 : 22.5 | 9 : 2 | 11 | 1.2% |

1000장 기준 WRR 분할은 CPU 819 / CSD 181 로 이상치(184)에 근접한다.

### 남은 양자화 잔차

`WRR_MAX_CYCLE=20` 은 소규모를 기준으로 잡은 보수적 상한이다. 5000장 실측에서
비율 3.906 은 상한 안에서 `4:1`(cycle 5)로 근사됐고, 그 결과 CSD 몫이 정확히 20%
(1000장)가 되어 이상치(1019장)와 19장 차이가 났다 — 완료시점 차 3.8초의 정체다.
비율 3.791 이었다면 `15:4`(cycle 19)로 1053장을 배정해 더 가까웠다.

즉 잔차는 **상한이 아니라 근사 탐색이 먼저 찾은 해에서 멈추는 지점**에서 온다.
대규모에서는 부분 사이클 왜곡이 `cycle/N` 로 무시할 만하므로, N 에 따라 상한을
키우면(예 `min(50, N//20)`) 잔차를 더 줄일 수 있다 — 현재 미적용.
