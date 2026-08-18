#!/bin/bash
##############################################################################
# HOST -> CSD Stage 2 트리거 (via SCP)
#
# HOST가 라벨링(CVAT, Label Studio 등)을 완료한 후
# SCP를 통해 CSD에 어노테이션을 전송하고 Stage 2를 트리거.
#
# 사용법:
#   ./trigger_stage2.sh
##############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_ANN="$SCRIPT_DIR/demo_data/_source/annotations"

# CSD connection
CSD_HOST="root@10.2.1.2"
CSD_BASE="/home/ngd/storage/csd_preprocessing"
CSD_ANN="$CSD_BASE/raw_data/annotations"
CSD_WATCHER="$CSD_BASE/raw_data/_watcher"

# Colors
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m'

# CSD 인증 — sshpass 비밀번호 인증 전용. 공개키는 쓰지 않는다(PubkeyAuthentication=no):
# 실행하는 자리마다 키가 있기도 없기도 해서, 켜 두면 환경에 따라 다르게 동작한다.
CSD_PASS="${CSD_PASS:?CSD_PASS 환경변수를 설정하세요}"
if ! command -v sshpass &>/dev/null; then
    echo "Error: sshpass not installed"
    exit 1
fi
SSH_OPTS=(-o StrictHostKeyChecking=no -o PubkeyAuthentication=no
          -o PreferredAuthentications=password)
SSH_CMD=(sshpass -p "$CSD_PASS" ssh "${SSH_OPTS[@]}")
SCP_CMD=(sshpass -p "$CSD_PASS" scp "${SSH_OPTS[@]}")

echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  HOST: Labeling Complete -> Stage 2 Trigger${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Step 1: Copy annotations to CSD (simulating HOST labeling result)
echo -e "${BLUE}[HOST]${NC} Sending annotation files to CSD (labeling result)..."
if ! "${SSH_CMD[@]}" "$CSD_HOST" "mkdir -p '$CSD_ANN'"; then
    echo "Error: failed to prepare annotation directory on CSD: $CSD_ANN"
    exit 1
fi
if ! "${SCP_CMD[@]}" "$SOURCE_ANN"/*.json "$CSD_HOST:$CSD_ANN/"; then
    echo "Error: failed to copy annotation files to CSD"
    exit 1
fi
echo -e "${BLUE}[HOST]${NC} Annotations sent to CSD"

# Step 2: Write trigger.json locally, then scp to CSD
echo -e "${BLUE}[HOST]${NC} Sending Stage 2 trigger to CSD..."
TRIGGER_TMP=$(mktemp)
cat > "$TRIGGER_TMP" << 'EOF'
{"stage": "stage2", "source": "host", "message": "Labeling complete."}
EOF
if ! "${SSH_CMD[@]}" "$CSD_HOST" "mkdir -p '$CSD_WATCHER'"; then
    rm -f "$TRIGGER_TMP"
    echo "Error: failed to prepare watcher directory on CSD: $CSD_WATCHER"
    exit 1
fi
if ! "${SCP_CMD[@]}" "$TRIGGER_TMP" "$CSD_HOST:$CSD_WATCHER/trigger.json"; then
    rm -f "$TRIGGER_TMP"
    echo "Error: failed to send trigger.json to CSD"
    exit 1
fi
rm -f "$TRIGGER_TMP"

echo -e "${GREEN}[HOST]${NC} Trigger sent! CSD server will start Stage 2."
echo ""
echo "  CSD will: convert -> augment -> split -> statistics"
echo "  Result:   preprocessed/train/ val/ test/ data.yaml"
echo ""
