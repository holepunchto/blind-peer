#!/usr/bin/env bash
set -euo pipefail

for idx in 1 2 3 4; do
  ip netns del "test-net-$idx" 2>/dev/null || true
done

ip link del test-bridge 2>/dev/null || true
