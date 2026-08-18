#!/usr/bin/env bash
#
# preprocess-csd 스택 배포/갱신 스크립트.
#
# 2026-08-18 에 빈 클러스터를 세우며 확인한 절차를 그대로 담았다. 손으로 하면
# 매번 같은 곳에서 걸린다 — device-plugin 을 빼먹어 CSD 샤드가 Pending 에 머물거나,
# 클러스터를 다시 만든 뒤 kubeconfig 토큰이 죽어 컨트롤러가 API 서버에 못 붙는다.
# 보름 사이 클러스터가 두 번 재생성됐으므로 또 필요해질 일이다.
#
# 사용법:
#   ./k8s/deploy.sh all          # 빈 클러스터에서 전체 배포 (아래를 순서대로)
#   ./k8s/deploy.sh image        # 이미지 재빌드 → containerd 반입 → rollout restart
#   ./k8s/deploy.sh kubeconfig   # 컨트롤러용 kubeconfig 재발급 (클러스터 재생성 후)
#   ./k8s/deploy.sh rbac         # 네임스페이스 · 쿼터 · SA · RBAC
#   ./k8s/deploy.sh crd          # CRD 2종
#   ./k8s/deploy.sh secret       # CSD 비밀번호 시크릿 (CSD_PASS 필요)
#   ./k8s/deploy.sh plugin       # csd-device-plugin (확장 자원 광고)
#   ./k8s/deploy.sh controllers  # 컨트롤러 · 매니저
#   ./k8s/deploy.sh csd-sync     # CSD 가 실행할 코드 사본을 공유 볼륨에 동기화
#   ./k8s/deploy.sh verify       # 회귀 3층 (로컬 스모크 / CSD 헬스체크 / k8s 통합)
#   ./k8s/deploy.sh status       # 현재 상태만 확인
#
# all 과 image 는 끝나고 회귀를 자동으로 돌린다 — 배포해 놓고 도는지 확인하지 않는
# 상태가 제일 나쁘기 때문이다. 건너뛰려면 --no-verify 를 붙인다.
#
# 전부 idempotent 하다 — 이미 서 있는 클러스터에 다시 돌려도 안전하다.
# 단 image 와 kubeconfig 는 파드를 재시작하거나 토큰을 새로 발급한다.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
NS="${WATCH_NAMESPACE:-preprocess-csd}"
NODE="${NODE_HOSTNAME:-gpu-npu-server-02}"
IMAGE="${WORKER_IMAGE:-csd-preprocessor:latest}"
PLUGIN_IMAGE="${PLUGIN_IMAGE:-csd-device-plugin:latest}"
# 컨트롤러·매니저 Deployment 가 hostPath 로 마운트하는 경로. 바꾸려면 매니페스트도 같이.
KUBECONFIG_OUT="${PREPROCESS_KUBECONFIG:-/root/preprocess-isolation/preprocess-csd.kubeconfig}"
CSD_RESOURCE="${CSD_RESOURCE_NAME:-keti.re.kr/csd}"
# CSD 가 실행하는 코드 사본이 놓이는 공유 OCFS2 경로 (CSD 에서는 /home/ngd/storage/...).
CSD_CODE_DEST="${CSD_CODE_DEST:-/mnt/newport_1/csd_preprocessing}"
# CSD 에서 실제로 도는 것만 올린다 — 실험·문서·서버 스크립트는 CSD 에서 쓰지 않는다.
# server/csd_healthcheck.py 의 RUNTIME_ITEMS 와 같은 목록이어야 드리프트 검사가 맞는다.
CSD_RUNTIME_ITEMS=(cli.py requirements.txt config controller csd_preprocessor worker)

log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }
die()  { printf '\033[31merror: %s\033[0m\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "$1 이 필요합니다"; }

# 노드가 광고 중인 CSD 확장 자원 수량(없으면 빈 문자열).
# jsonpath 는 키 안의 점을 이스케이프해야 해서 조용히 빈 값을 돌려준다 —
# 실제로 "광고 안 됨"과 구분이 안 돼 한 번 속았다. go-template 은 그 함정이 없다.
allocatable_csd() {
  kubectl get node "$NODE" \
    -o go-template="{{with index .status.allocatable \"$CSD_RESOURCE\"}}{{.}}{{end}}" \
    2>/dev/null
}

# --------------------------------------------------------------------------- #

do_image() {
  log "이미지 재빌드 → containerd 반입"
  need docker; need ctr
  docker build -t "$IMAGE" "$REPO_ROOT"
  local tar; tar="$(mktemp -t csd-image-XXXXXX.tar)"
  # 노드 containerd 의 k8s.io 네임스페이스에 넣어야 kubelet 이 본다
  # (매니페스트가 imagePullPolicy: Never 이므로 레지스트리를 거치지 않는다).
  docker save "$IMAGE" -o "$tar"
  ctr -n k8s.io images import "$tar"
  rm -f "$tar"
  if kubectl -n "$NS" get deploy preprocess-controller >/dev/null 2>&1; then
    info "실행 중인 파드에 반영 (rollout restart)"
    kubectl -n "$NS" rollout restart deploy/preprocess-controller deploy/preprocess-manager
    kubectl -n "$NS" rollout status deploy/preprocess-controller --timeout=180s
    kubectl -n "$NS" rollout status deploy/preprocess-manager --timeout=180s
  else
    info "컨트롤러가 아직 없다 — rollout 생략"
    return 0
  fi
  # 새 이미지가 실제로 도는지 여기서 확인한다. 드리프트를 고치러 온 경로이므로
  # "고쳤다고 생각했는데 안 돌더라"가 가장 흔한 실패 방식이다.
  [ -n "${SKIP_VERIFY:-}" ] || do_verify
}

do_rbac() {
  log "네임스페이스 · 쿼터 · SA · RBAC"
  kubectl apply -f "$SCRIPT_DIR/namespace-rbac.yaml"
}

do_crd() {
  log "CRD"
  kubectl apply -f "$SCRIPT_DIR/preprocessingjob-crd.yaml" \
                -f "$SCRIPT_DIR/preprocessing-workload-crd.yaml"
}

do_kubeconfig() {
  log "컨트롤러용 kubeconfig 발급 → $KUBECONFIG_OUT"
  kubectl -n "$NS" get sa preprocess-admin >/dev/null 2>&1 \
    || die "ServiceAccount 가 없습니다 — 먼저 './k8s/deploy.sh rbac'"

  local token server ca
  token="$(kubectl -n "$NS" create token preprocess-admin --duration=8760h)"
  server="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
  ca="$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
  if [ -z "$ca" ]; then
    local cafile
    cafile="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.certificate-authority}')"
    [ -n "$cafile" ] || die "클러스터 CA 를 찾을 수 없습니다"
    ca="$(base64 -w0 "$cafile")"
  fi

  mkdir -p "$(dirname "$KUBECONFIG_OUT")"
  # 직전 것만 남긴다 — 새 토큰이 잘못 발급됐을 때 되돌릴 한 걸음이면 충분하다.
  [ -f "$KUBECONFIG_OUT" ] && cp -a "$KUBECONFIG_OUT" "$KUBECONFIG_OUT.prev"

  umask 077
  cat > "$KUBECONFIG_OUT" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: kubernetes
  cluster:
    server: ${server}
    certificate-authority-data: ${ca}
contexts:
- name: preprocess-admin@${NS}
  context:
    cluster: kubernetes
    namespace: ${NS}
    user: preprocess-admin
current-context: preprocess-admin@${NS}
users:
- name: preprocess-admin
  user:
    token: ${token}
EOF
  chmod 600 "$KUBECONFIG_OUT"
  info "server: $server"
  KUBECONFIG="$KUBECONFIG_OUT" kubectl auth can-i watch \
    preprocessingworkloads.edgeai.keti.re.kr -n "$NS" >/dev/null \
    || die "발급된 kubeconfig 로 CR 감시 권한이 없습니다"
  info "권한 확인 통과 (preprocessingworkloads watch)"
}

do_secret() {
  log "CSD 비밀번호 시크릿"
  local pass="${CSD_PASS:-${CSD_REMOTE_PASS:-}}"
  [ -n "$pass" ] || die "CSD_PASS(또는 CSD_REMOTE_PASS) 환경변수가 필요합니다 — 비밀번호 인증 전용"
  # apply 로 만들어야 이미 있을 때 갱신된다 (create 는 AlreadyExists 로 실패).
  kubectl -n "$NS" create secret generic csd-credentials \
    --from-literal=password="$pass" --dry-run=client -o yaml | kubectl apply -f -
}

do_plugin() {
  log "csd-device-plugin (확장 자원 $CSD_RESOURCE 광고)"
  if ! ctr -n k8s.io images ls -q 2>/dev/null | grep -q "${PLUGIN_IMAGE##*/}"; then
    info "플러그인 이미지가 containerd 에 없다 — 빌드해서 반입"
    need docker
    docker build -t "$PLUGIN_IMAGE" "$REPO_ROOT/deviceplugin"
    local tar; tar="$(mktemp -t csd-plugin-XXXXXX.tar)"
    docker save "$PLUGIN_IMAGE" -o "$tar"; ctr -n k8s.io images import "$tar"; rm -f "$tar"
  fi
  kubectl apply -f "$SCRIPT_DIR/csd-device-plugin.yaml"

  # 이게 광고되기 전에 워크로드를 넣으면 CSD 샤드 파드가 Pending 에서 나오지 못한다
  # ("0/N nodes are available: 1 Insufficient keti.re.kr/csd"). 그래서 여기서 기다린다.
  info "확장 자원 광고 대기 (최대 90s)"
  local i
  for i in $(seq 1 18); do
    if [ -n "$(allocatable_csd)" ]; then
      info "광고 확인: $CSD_RESOURCE"
      return 0
    fi
    sleep 5
  done
  die "$CSD_RESOURCE 가 광고되지 않습니다 — kubectl -n $NS logs -l app=csd-device-plugin"
}

do_controllers() {
  log "컨트롤러 · 매니저"
  [ -f "$KUBECONFIG_OUT" ] || die "kubeconfig 가 없습니다 — 먼저 './k8s/deploy.sh kubeconfig'"
  kubectl apply -f "$SCRIPT_DIR/preprocess-controller-deployment.yaml" \
                -f "$SCRIPT_DIR/preprocess-manager-deployment.yaml"
  kubectl -n "$NS" rollout status deploy/preprocess-controller --timeout=180s
  kubectl -n "$NS" rollout status deploy/preprocess-manager --timeout=180s
}

do_csd_sync() {
  log "CSD 코드 사본 동기화 → $CSD_CODE_DEST"
  [ -d "$(dirname "$CSD_CODE_DEST")" ] || die "공유 볼륨이 보이지 않습니다: $CSD_CODE_DEST"
  mkdir -p "$CSD_CODE_DEST"
  local item
  for item in "${CSD_RUNTIME_ITEMS[@]}"; do
    rm -rf "${CSD_CODE_DEST:?}/$item"
    cp -r "$REPO_ROOT/$item" "$CSD_CODE_DEST/$item"
  done
  # 서버(x86)에서 만든 캐시가 CSD(aarch64)에서 쓰이면 안 된다.
  find "$CSD_CODE_DEST" -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
  info "동기화: ${CSD_RUNTIME_ITEMS[*]}"
}

do_verify() {
  log "회귀 3층"
  local py="$REPO_ROOT/run_local_python.sh"
  local failed=()

  info "1층: 로컬 스모크"
  if "$REPO_ROOT/run_smoke_tests.sh" >/tmp/csd-verify-smoke.log 2>&1; then
    info "  통과 ($(grep -c '^\[RUN\]' /tmp/csd-verify-smoke.log)종)"
  else
    failed+=("로컬 스모크 — /tmp/csd-verify-smoke.log")
    tail -15 /tmp/csd-verify-smoke.log
  fi

  # 2층은 CSD 비밀번호가 있어야 한다. 없으면 건너뛴다 — 비번 없이 도는 경로는 없다.
  local pass="${CSD_PASS:-${CSD_REMOTE_PASS:-}}"
  if [ -n "$pass" ]; then
    info "2층: CSD 헬스체크"
    if CSD_REMOTE_PASS="$pass" "$py" "$REPO_ROOT/server/csd_healthcheck.py" --offload \
         >/tmp/csd-verify-health.log 2>&1; then
      info "  $(tail -1 /tmp/csd-verify-health.log)"
    else
      failed+=("CSD 헬스체크 — /tmp/csd-verify-health.log")
      grep -a '^\[FAIL\]' /tmp/csd-verify-health.log || tail -10 /tmp/csd-verify-health.log
    fi
  else
    info "2층: SKIP (CSD_PASS 미설정)"
  fi

  info "3층: k8s 통합 회귀"
  if "$py" "$REPO_ROOT/server/test_k8s_integration.py" >/tmp/csd-verify-k8s.log 2>&1; then
    info "  $(grep -aE '^\[(PASS|SKIP)\]' /tmp/csd-verify-k8s.log | head -1)"
    grep -aE '샤드 산출|CSD 오프로드|정리:' /tmp/csd-verify-k8s.log | sed 's/^/   /'
  else
    failed+=("k8s 통합 회귀 — /tmp/csd-verify-k8s.log")
    tail -12 /tmp/csd-verify-k8s.log
  fi

  # 분할 알고리즘 E2E. 통합 회귀는 STATIC 만 밟으므로 MTE/WRR/AUTO 는 따로 본다.
  info "3층: 분할 알고리즘 E2E"
  if "$py" "$REPO_ROOT/server/test_k8s_algorithms.py" >/tmp/csd-verify-algo.log 2>&1; then
    info "  $(grep -aE '^\[(PASS|SKIP)\]' /tmp/csd-verify-algo.log | head -1)"
    grep -aE '^  (MTE|WRR|AUTO) ' /tmp/csd-verify-algo.log | sed 's/^/   /'
  else
    failed+=("분할 알고리즘 E2E — /tmp/csd-verify-algo.log")
    tail -12 /tmp/csd-verify-algo.log
  fi

  if [ ${#failed[@]} -gt 0 ]; then
    printf '\033[31m회귀 실패 %d건:\033[0m\n' "${#failed[@]}"
    printf '   - %s\n' "${failed[@]}"
    return 1
  fi
  info "회귀 전부 통과"
}

do_status() {
  log "상태"
  kubectl -n "$NS" get pods -o wide 2>/dev/null || info "네임스페이스 $NS 없음"
  echo
  kubectl -n "$NS" get pw,pj 2>/dev/null || true
  echo
  info "확장 자원 $CSD_RESOURCE: $(allocatable_csd || true) $([ -n "$(allocatable_csd)" ] || echo '(광고 없음 — ./k8s/deploy.sh plugin)')"
  info "kubeconfig: $([ -f "$KUBECONFIG_OUT" ] && grep -a 'server:' "$KUBECONFIG_OUT" || echo '(없음)')"
  echo
  info "회귀: ./run_local_python.sh server/test_k8s_integration.py"
}

do_all() {
  # image 안의 rollout 은 컨트롤러가 아직 없을 때 건너뛰므로, 여기서는 굽고 반입만
  # 하고 실제 기동은 아래 controllers 가 맡는다. 회귀도 맨 마지막에 한 번만 돈다.
  # ※ `SKIP_VERIFY=1 do_image` 로 쓰면 안 된다 — bash 는 함수 호출 앞의 변수 대입을
  #   호출 뒤에도 남기므로 마지막 do_verify 까지 조용히 건너뛴다.
  local want_verify="${SKIP_VERIFY:-}"
  SKIP_VERIFY=1
  do_image
  do_rbac
  do_crd
  do_kubeconfig
  do_secret
  do_plugin
  do_controllers
  do_csd_sync
  do_status
  SKIP_VERIFY="$want_verify"
  [ -n "$SKIP_VERIFY" ] || do_verify
}

# --no-verify 를 어느 위치에서든 받는다 (환경변수 SKIP_VERIFY 로도 가능).
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --no-verify) SKIP_VERIFY=1 ;;
    *)           ARGS+=("$arg") ;;
  esac
done
export SKIP_VERIFY="${SKIP_VERIFY:-}"

need kubectl
case "${ARGS[0]:-}" in
  all)         do_all ;;
  image)       do_image ;;
  rbac)        do_rbac ;;
  crd)         do_crd ;;
  kubeconfig)  do_kubeconfig ;;
  secret)      do_secret ;;
  plugin)      do_plugin ;;
  controllers) do_controllers ;;
  csd-sync)    do_csd_sync ;;
  verify)      do_verify ;;
  status)      do_status ;;
  *)           # 헤더 주석(첫 줄 shebang 제외)이 곧 사용법이다 — 한 곳만 고치면 된다
               awk 'NR>1 && /^#/ {sub(/^# ?/, ""); print; next} NR>1 {exit}' \
                 "${BASH_SOURCE[0]}"
               exit 1 ;;
esac
