#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_RUNNER="${PYTHON_RUNNER:-$SCRIPT_DIR/run_local_python.sh}"

TESTS=(
  "server/test_trigger_flow.py"
  "server/test_mock_verify.py"
  "server/test_stage1_smoke.py"
  "server/test_stage2_smoke.py"
  "server/test_worker_local_fallback.py"
  "server/test_manager_controller_smoke.py"
  "server/test_stage2_missing_labels_failure.py"
  "server/test_invalid_template_failure.py"
  # CSD 원격 실패 경로. CSD_REMOTE_PASS 가 없으면 실제 CSD 가 필요한 2건은 스스로 SKIP 한다
  # (오프라인에서도 이 묶음은 통째로 통과해야 한다).
  "server/test_csd_remote_failure.py"
)

if [[ ! -x "$PYTHON_RUNNER" ]]; then
  echo "error: python runner is not executable: $PYTHON_RUNNER" >&2
  exit 1
fi

echo "Smoke test runner"
echo "  runner: $PYTHON_RUNNER"
echo "  tests: ${#TESTS[@]}"
echo

for test_path in "${TESTS[@]}"; do
  echo "[RUN] $test_path"
  "$PYTHON_RUNNER" "$SCRIPT_DIR/$test_path"
done

echo
echo "[PASS] all smoke tests passed"
