## test-netns (Linux-only, advanced)

This suite verifies remote-IP top-k accounting and IP ban-list behavior using
Linux network namespaces.
It only runs on Linux and requires root privileges to create namespaces and to
execute clients inside them.

### What this tests

- `blind_peer_add_cores_top5_by_remote_ip`: verifies that remote IPs are tracked
  independently, using four isolated namespaces with distinct IPs in
  `test-netns/top-k-ip.test.js`.
- Top-k accounting: with namespace request counts `1, 2, 3, 4` and `k = 3`, the
  top-k sum should be `2 + 3 + 4 = 9`.
- Multiple `banIpListKeys`: verifies that separate ban-list feeds can each ban a
  different namespace IP, and that non-banned namespace IPs still complete
  requests normally in `test-netns/ip-ban-list.test.js`.

### How it works

- A bridge interface (`test-bridge`) is created in the root namespace with IP
  `10.200.1.1/24`.
- Four namespaces are created: `test-net-1` through `test-net-4`, with IPs
  `10.200.1.2/24` through `10.200.1.5/24`.
- The testnet bootstrap nodes are bound to the bridge IP.
- Each namespace executes a small Node.js helper via
  `ip netns exec ... node test-netns/make-add-cores.js`.
- The test follows the existing top-k request pattern and checks that the IP
  top-k window keeps the highest three namespace request counts.

### Run locally

1. Setup namespaces and bridge:

```bash
sudo test-netns/scripts/setup.sh
```

2. Run the test suite:

```bash
sudo npm run test:netns
```

3. Teardown and cleanup:

```bash
sudo test-netns/scripts/teardown.sh
```
