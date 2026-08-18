#!/bin/bash
##############################################################################
# Stage 2 트리거
#
# 라벨링 완료 후 Stage 2 전처리를 시작하는 스크립트.
# CSD 내부에서 직접 실행.
#
# 사용법:
#   ./server/trigger_stage2.sh
##############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CSD_BASE="/home/ngd/storage/csd_preprocessing"
CSD_ANN="$CSD_BASE/raw_data/annotations"
CSD_WATCHER="$CSD_BASE/raw_data/_watcher"
SOURCE_ANN="$PROJECT_DIR/demo_data/_source/annotations"

# Colors
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m'

echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  Labeling Complete -> Stage 2 Trigger${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Step 1: Copy annotations
echo -e "${BLUE}[CSD]${NC} Copying annotation files..."
mkdir -p "$CSD_ANN"
if ! cp "$SOURCE_ANN"/*.json "$CSD_ANN/"; then
    echo "Error: failed to copy annotation files to $CSD_ANN"
    exit 1
fi
echo -e "${BLUE}[CSD]${NC} Annotations ready: $(ls "$CSD_ANN" | wc -l) files"

# Step 2: Write trigger.json
echo -e "${BLUE}[CSD]${NC} Writing Stage 2 trigger..."
mkdir -p "$CSD_WATCHER"
cat > "$CSD_WATCHER/trigger.json" << 'EOF'
{"stage": "stage2", "source": "host", "message": "Labeling complete."}
EOF

echo -e "${GREEN}[CSD]${NC} Trigger sent! CSD server will start Stage 2."
echo ""
echo "  CSD will: convert -> augment -> split -> statistics"
echo "  Result:   preprocessed/train/ val/ test/ data.yaml"
echo ""
