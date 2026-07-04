#!/usr/bin/env bash
set -euo pipefail

# Colors for display
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Running local-ci for Mnemosyne ===${NC}"

# 1. Format check
echo -e "\n${YELLOW}[1/4] Checking code formatting (cargo fmt)...${NC}"
if ! cargo fmt --all -- --check; then
    echo -e "${RED}✘ Formatting errors detected! Run 'cargo fmt --all' to fix them.${NC}"
    exit 1
fi
echo -e "${GREEN}✔ Formatting is correct.${NC}"

# 2. Static analysis (Clippy)
echo -e "\n${YELLOW}[2/4] Running static analysis (cargo clippy)...${NC}"
if ! cargo clippy --all-targets -- -D warnings; then
    echo -e "${RED}✘ Clippy found warnings or errors!${NC}"
    exit 1
fi
echo -e "${GREEN}✔ Static analysis passed successfully.${NC}"

# 3. Unit tests
echo -e "\n${YELLOW}[3/4] Running test suite (cargo test)...${NC}"
if ! cargo test; then
    echo -e "${RED}✘ Some tests failed!${NC}"
    exit 1
fi
echo -e "${GREEN}✔ All tests passed successfully.${NC}"

# 4. Compilation check (Debug)
echo -e "\n${YELLOW}[4/4] Checking build (cargo build)...${NC}"
if ! cargo build; then
    echo -e "${RED}✘ Compilation failed!${NC}"
    exit 1
fi
echo -e "${GREEN}✔ Compilation successful.${NC}"

echo -e "\n${GREEN}===========================================${NC}"
echo -e "${GREEN}  ✓ ALL LOCAL VERIFICATIONS SUCCESSFUL!     ${NC}"
echo -e "${GREEN}===========================================${NC}"
