#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Build configuration
IMAGES=(
  "kafka-image:kafka-local:7.5.0"
  "flink-jobmanager:flink-jobmanager:1.18.1"
  "flink-taskmanager:flink-taskmanager:1.18.1"
  "flink-sql-job:flink-sql-job:1.18.1"
  "flink-kafka:flink-kafka:1.18.1"
)

echo -e "${BLUE}Starting Docker image builds...${NC}\n"

failed_images=()
built_images=()

for image_config in "${IMAGES[@]}"; do
  IFS=':' read -r dir name tag <<< "$image_config"
  image_name="${name}:${tag}"
  image_path="${SCRIPT_DIR}/${dir}"

  echo -e "${BLUE}Building ${image_name}...${NC}"

  if [ ! -d "$image_path" ]; then
    echo -e "${RED}✗ Directory not found: ${image_path}${NC}\n"
    failed_images+=("$image_name")
    continue
  fi

  if [ ! -f "${image_path}/Dockerfile" ]; then
    echo -e "${RED}✗ Dockerfile not found in ${image_path}${NC}\n"
    failed_images+=("$image_name")
    continue
  fi

  if docker build --no-cache -t "$image_name" "$image_path"; then
    echo -e "${GREEN}✓ Successfully built ${image_name}${NC}\n"
    built_images+=("$image_name")
  else
    echo -e "${RED}✗ Failed to build ${image_name}${NC}\n"
    failed_images+=("$image_name")
  fi
done

# Summary
echo -e "${BLUE}========== Build Summary ==========${NC}"
echo -e "${GREEN}Built (${#built_images[@]}):${NC}"
for img in "${built_images[@]}"; do
  echo -e "  ${GREEN}✓${NC} $img"
done

if [ ${#failed_images[@]} -gt 0 ]; then
  echo -e "${RED}Failed (${#failed_images[@]}):${NC}"
  for img in "${failed_images[@]}"; do
    echo -e "  ${RED}✗${NC} $img"
  done
  exit 1
else
  echo -e "${GREEN}All images built successfully!${NC}"
  exit 0
fi
