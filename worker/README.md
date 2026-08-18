# CSD Worker

Two workers share the same file-based contract (env in → `records.jsonl` +
`result.json` out; host merges via `cli.py mock-verify`):

- **`mock_worker.py`** — contract adapter (mock records only). Proves the
  master→worker Kubernetes plumbing. See "Mock Worker Quickstart" below.
- **`csd_worker.py`** — **real preprocessing** worker over an assigned shard.
  Resized images are written to the shared `DATASET_DIR/images` (OCFS2 직접 기록,
  파일 이동·결합 없음); partial mean/std is returned in `result.json` for the host to
  aggregate into global statistics.

## 전처리 파이프라인은 어디서 오는가

워크로드마다 스텝을 고르지 않는다. **고정 템플릿** 두 개(stage1/stage2)가 정본이다:

```
config/pipeline_templates/stage1_raw_ingestion.yaml      ← legacy 인프로세스 경로와 공유
config/pipeline_templates/stage2_training_preparation.yaml
   │  (매니저가 해석: op + params)
   ▼
PreprocessingJob spec
   ├ preprocessing_pipeline  = [{op, params}]  ← stage1, 샤드 병렬
   └ stage2_pipeline         = [{op, params}]  ← stage2, 샤드 완료 후 단일 패스
   │  (컨트롤러가 PREPROCESSING_STEPS env 로 전달)
   ▼
[stage1] pj-<id>-cpu / pj-<id>-csd   샤드별 병렬 (CSD 는 실 장비 오프로드)
   │  validate → deduplicate → filter_quality → resize → normalize (+phash)
   ▼  컨트롤러: 부분 통계 집계 + 일관성 검증 → _shards/shard_summary.json
[stage2] pj-<id>-stage2              데이터셋 전체를 한 번에 (기본 CSD)
   │  convert_annotation → augment → split → statistics
   ▼
out_root/{train,val,test}/{images,labels} + data.yaml + statistics.json
```

stage2 는 라벨 의존 + 데이터셋 전역 연산이라 샤드로 쪼갤 수 없어서 단일 패스로
돈다. `stage2Template: none` 이면 stage1 까지만 실행된다.

### 라벨이 아직 없을 때 (임시 라벨)

`LABEL_PATH` 가 비고 `PLACEHOLDER_LABELS=true` 면 워커가 이미지 중앙 50% 를 단일
클래스(`placeholder_object`)로 찍은 COCO 라벨을 생성해 stage2 배선을 통과시킨다.
**정답이 아니므로 학습에 쓸 수 없다** — 다음 위치에 전부 표시된다:

- `result.json.labelSource = "placeholder"` + `labelInfo`
- `data.yaml` 최상단 경고 주석 + `label_source: placeholder`, `names: [placeholder_object]`
- `statistics.json.stage2.warning`
- pj status `stage2.label_source` (`kubectl get pj` 의 LABELS 컬럼)
- 매니저·컨트롤러 로그 WARNING

실 어노테이션이 준비되면 `pw.spec.workload.dataset.labelPath` 를 지정하고 매니저의
`ALLOW_PLACEHOLDER_LABELS=false` 로 바꾼다(그러면 라벨 없는 stage2 는 실패한다).

- 템플릿을 바꾸려면 `pw.spec.workload.pipelineTemplate` 또는 매니저의
  `DEFAULT_PIPELINE_TEMPLATE` (기본 `stage1_raw_ingestion`).
- `PREPROCESSING_STEPS` 는 세 형태를 받는다 — 컨트롤러가 주는 `[{op, params}]`,
  이름만 나열한 `["resize","normalize"]`, 콤마 구분 `resize,normalize`.
  이름만 주면 params 는 워커 기본값(`STEP_PARAMS`)이라 템플릿 설정과
  달라질 수 있으므로, 재현성이 필요하면 템플릿 경로를 쓴다.
- env 미지정 시 기본 `resize → normalize → phash`.

```bash
PREPROCESSING_STEPS='[{"op":"resize","params":{"target_size":[640,640],"method":"letterbox"}}]'
PREPROCESSING_STEPS='["validate","resize","normalize","phash"]'
PREPROCESSING_STEPS=resize,normalize
```

지원 스텝: `validate`, `deduplicate`, `filter_quality`, `convert_annotation`,
`resize`, `normalize`, `augment`, `split`, `statistics`, `tile`, `phash`.
알 수 없는 이름은 워커가 즉시 실패시킨다.

**샤드 단위 실행의 한계** (`result.json.warnings` 에 기록됨)

- `deduplicate`/`split`/`statistics` 는 데이터셋 전역 연산이라 샤드-로컬 결과만
  낸다. 전역 집계(mean/std, 카운트, 일관성)는 컨트롤러 `aggregate_shards()` 책임.
- 어떤 연산이 입력 디렉터리를 재스캔해 샤드 밖 파일을 끌어오면 워커가 경고를
  남기고 샤드 범위로 되돌린다(모든 워커가 전체 데이터셋을 중복 처리하는 것을 방지).
- `convert_annotation`/`augment`/`split` 은 라벨이 전제라 샤드 단계가 아니라
  샤드 완료 후 stage2 단일 패스에서 실행된다(`LABEL_PATH` 로 어노테이션 전달).
  CSD 오프로드 시에는 데이터셋과 어노테이션을 함께 CSD 로 밀어 넣는다.
- 실 CSD 오프로드 시, CSD 측 코드가 구버전이면 전달한 파이프라인이 무시되므로
  워커가 그것을 감지해 실패시킨다(`result.json.preprocessingSteps` 미보고 검사).
  이 경우 `CSD_REMOTE_REPO` 경로의 코드를 갱신해야 한다.

**CSD 오프로드 방식** (`result.json.offload.mode` 에 기록)

| mode | 조건 | 동작 |
|---|---|---|
| `shared-volume` | 입출력·라벨이 모두 공유 OCFS2 파티션(`/mnt/newport_1` ↔ CSD `/home/ngd/storage`) 아래 | 복사 없이 CSD 가 같은 파일을 제자리에서 처리 (30장 기준 push 4ms / pull 0ms) |
| `copy` | 그 외 (노드 로컬 등) | scp 로 밀어넣고 결과 회수 (30장 기준 약 2~3초) |

진입 전에 `ssh test -d` 로 CSD 가 그 경로를 실제로 보는지 확인하고, 아니면 복사
방식으로 폴백한다.

**CSD 접속 설정** (2026-08-18 기준)

| 환경변수 | 기본값 | 비고 |
|---|---|---|
| `CSD_REMOTE_HOST` | (없음, 필수) | 예 `root@10.2.1.2` |
| `CSD_REMOTE_REPO` | `/home/ngd/storage/csd_preprocessing` | CSD 측 코드 경로. 2026-08-14 재구축 때 `csd-based-preprocessing` 에서 바뀌었다 |
| `CSD_REMOTE_PASS` | (없음, **필수**) | sshpass 비밀번호. 없으면 워커가 즉시 실패한다 |
| `CSD_REMOTE_PYTHONPATH` | `/home/ngd/storage/pylibs` | CSD 의 numpy/OpenCV 경로 |

**인증은 비밀번호(sshpass) 전용이다.** 공개키 인증은 `PubkeyAuthentication=no` 로
명시적으로 끈다 — 원격 실행 주체(서버 셸 / 워커 컨테이너 / 다른 노드)마다 키를
심어야 하는 부담이 있고, 켜 두면 키가 있는 자리에서만 조용히 성공해 환경마다
다르게 동작하기 때문이다. k8s 에서는 `csd-credentials` 시크릿으로 넣는다.

## Running the real worker as a Kubernetes Job

k8s 경로 전체(네임스페이스·RBAC·CRD·device-plugin·컨트롤러)는
[k8s/deploy.sh](deploy.sh) 로 세운다 — `CSD_PASS=<비번> ./k8s/deploy.sh all`.
워커 코드를 고쳤으면 `./k8s/deploy.sh image` 로 이미지를 다시 굽는다. 파드는
볼륨이 아니라 **이미지 안의 코드**를 돌기 때문에, 안 구우면 옛 워커가 계속 돈다.

(분산실험 오케스트레이터 `experiments/`는 2026-07-28 삭제 — 백업:
`/root/preprocess-isolation/backup-deleted-experiments-evidence-20260728.tar.gz`)

`worker/csd-worker-job.yaml`의 placeholder(`{{BATCH_ID}}`,
`{{BATCH_MANIFEST_JSON}}`, `{{DATA_HOST_PATH}}`, `{{OUTPUT_HOST_PATH}}`,
`{{DATASET_HOST_PATH}}`)를 채운 뒤 `kubectl apply`로 직접 실행하고, 완료 후
공유 출력 경로의 `result.json`을 읽는다. Job은 `preprocess-csd` 네임스페이스에
생성된다. Build/load the image on the CSD node first
(`docker build -t csd-preprocessor:latest . && docker save csd-preprocessor:latest | ctr -n k8s.io images import -`).

---

# Mock Worker Quickstart

This worker is the minimum contract adapter for the master-to-worker Kubernetes flow.

## Local run

```bash
BATCH_MANIFEST_JSON='{"batchId":"csd-test-001","worker":"CSD","indexes":[0,1,2],"itemCount":3}' \
DATA_PATH=/tmp/mock-input \
OUTPUT_DIR=/tmp/csd-mock-out \
OUTPUT_HOST_DIR=/tmp/csd-mock-out \
WORKER_TYPE=CSD \
BATCH_ID=csd-test-001 \
./run_local_python.sh worker/mock_worker.py
```

Expected outputs:

- `OUTPUT_DIR/records.jsonl`
- `OUTPUT_DIR/result.json`

## Container run

Build on the worker host:

```bash
docker build -t csd-preprocessor:latest .
```

Run directly:

```bash
docker run --rm \
  -e BATCH_MANIFEST_JSON='{"batchId":"csd-test-001","worker":"CSD","indexes":[0,1,2],"itemCount":3}' \
  -e DATA_PATH=/data \
  -e OUTPUT_DIR=/output/csd-test-001 \
  -e OUTPUT_HOST_DIR=/home/ngd/storage/mock-output/csd-test-001 \
  -e WORKER_TYPE=CSD \
  -e BATCH_ID=csd-test-001 \
  -v /home/ngd/storage/mock-input:/data:ro \
  -v /home/ngd/storage/mock-output:/output \
  csd-preprocessor:latest \
  python worker/mock_worker.py
```

## Kubernetes Job

Use [mock-worker-job.yaml](/root/workspace/edge-ai-workspace/csd-based-preprocessing/worker/mock-worker-job.yaml) as the starting point.

Command prefix convention used below:

- `(230)` = cluster control host / master-side validation host
- `(232)` = worker node that runs the Pod and stores mock artifacts

Before applying:

- Confirm the actual node label for the `10.0.4.232` worker:
  `kubectl get nodes -o wide`
  `kubectl get nodes --show-labels`
- Replace `REPLACE_WITH_232_HOSTNAME_LABEL` in the Job YAML with the real `kubernetes.io/hostname` value.
- Confirm the image is visible to the kubelet on that node.
  For a node-local image only, keep `imagePullPolicy: Never`.
  For a registry image, change `image` and `imagePullPolicy` accordingly.
- Confirm these host paths exist on the worker node and have write permission for the container runtime:
  `/home/ngd/storage/mock-input`
  `/home/ngd/storage/mock-output`

Apply it from the cluster control host:

```bash
(230) kubectl delete job csd-mock-worker --ignore-not-found
(230) kubectl apply -f worker/mock-worker-job.yaml
(230) kubectl get pods -o wide
(230) kubectl logs job/csd-mock-worker
```

Describe the Job or Pod if scheduling fails:

```bash
(230) kubectl describe job csd-mock-worker
(230) kubectl describe pod <pod-name>
```

Check output files on the worker storage:

```bash
(232) cat /home/ngd/storage/mock-output/csd-test-001/result.json
(232) cat /home/ngd/storage/mock-output/csd-test-001/records.jsonl
```

Verify and merge on the host side:

```bash
(230) ./run_local_python.sh cli.py mock-verify \
  --batch-dir /home/ngd/storage/mock-output/csd-test-001 \
  --merged-output /home/ngd/storage/mock-output/merged-records.jsonl \
  --exit-code 0
```

If you captured the Pod stdout JSON to a file, validate that as well:

```bash
(230) kubectl logs job/csd-mock-worker -n edge-system > /tmp/csd-mock-worker.stdout.json
(230) ./run_local_python.sh cli.py mock-verify \
  --batch-dir /home/ngd/storage/mock-output/csd-test-001 \
  --merged-output /home/ngd/storage/mock-output/merged-records.jsonl \
  --stdout-json /tmp/csd-mock-worker.stdout.json \
  --exit-code 0
```

## Success criteria

- Pod schedules on the intended worker node for `10.0.4.232`
- Worker receives `BATCH_MANIFEST_JSON`
- `records.jsonl` and `result.json` are written under `OUTPUT_DIR`
- `result.json.outputFile` points to the shared-storage path from `OUTPUT_HOST_DIR`
- stdout prints the same completion JSON payload as `result.json`
- `/dev/termination-log` contains the completion JSON when available
- Job exits with code `0`
- Host-side `cli.py mock-verify` completes and writes the merged JSONL

## Validation order

1. `(230) kubectl apply -f worker/mock-worker-job.yaml`
2. `(230) kubectl get pods -o wide`
3. `(230) kubectl logs job/csd-mock-worker`
4. `(230) kubectl describe job csd-mock-worker`
5. `(232) Check result.json and records.jsonl on the worker storage path`
6. `(230) Run cli.py mock-verify`

## Re-run notes

- Re-applying the same Job name without deleting it first will fail because Jobs are not updated in place.
- Re-using the same `batchId` and output directory will overwrite `records.jsonl` and `result.json`.
- For clean end-to-end verification, use a fresh `batchId` per run or delete the prior output directory before re-running.

## 230 Status Update Template

After each run, report back to the 230-side operator in this format:

```text
[230 update]
- Job apply: success|failed
- Scheduling: edge-gpu-232|failed
- Pod phase: Pending|Running|Succeeded|Failed
- Current blocker: none|<reason>
- Logs: captured|not available
- Artifacts on 232:
  - result.json: present|missing
  - records.jsonl: present|missing
- Host verify: passed|failed|not run
- Next action: <single concrete next step>
```

Example for image failure:

```text
[230 update]
- Job apply: success
- Scheduling: edge-gpu-232
- Pod phase: Pending
- Current blocker: ErrImageNeverPull
- Logs: not available
- Artifacts on 232:
  - result.json: missing
  - records.jsonl: missing
- Host verify: not run
- Next action: import csd-preprocessor:latest into 232 containerd or switch to a registry image
```
