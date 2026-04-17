#!/usr/bin/env bash
set -euo pipefail

BRIDGE_NAME=test-bridge
BRIDGE_IP=10.200.1.1/24

ip link add name "$BRIDGE_NAME" type bridge
ip addr add "$BRIDGE_IP" dev "$BRIDGE_NAME" 2>/dev/null
ip link set "$BRIDGE_NAME" up

for idx in 1 2 3 4; do
  ns="test-net-$idx"
  host_octet=$((idx + 1))
  veth_br="veth${idx}-br"
  veth_ns="veth${idx}-ns"

  ip netns add "$ns"
  ip link add "$veth_br" type veth peer name "$veth_ns"
  ip link set "$veth_ns" netns "$ns"
  ip link set "$veth_br" master "$BRIDGE_NAME"
  ip link set "$veth_br" up

  ip netns exec "$ns" ip addr add "10.200.1.${host_octet}/24" dev "$veth_ns"
  ip netns exec "$ns" ip link set "$veth_ns" up
  ip netns exec "$ns" ip link set lo up
done

echo "Bridge ${BRIDGE_NAME} configured at ${BRIDGE_IP}"
echo "Namespaces test-net-1..4 configured at 10.200.1.2..5/24"
