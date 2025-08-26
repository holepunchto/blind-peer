# blind-peer

Blind peers help keep hypercores available.

For the client side responsible for requesting cores be kept by Blind Peers, see [blind-peering](https://github.com/holepunchto/blind-peering).

## Installation

Install globally to use the `blind-peer` command:

```
npm install -g blind-peer
```

## Usage

Run a blind peer:

```
blind-peer
```

### Command Line Options

- `--storage|-s [path]` - Storage path, defaults to ./blind-peer
- `--port|-p [int]` - DHT Port to try to bind to. Only relevant when that port is not firewalled. (defaults to a random port)
- `--trusted-peer|-t [trusted-peer]` - Public key of a trusted peer (allowed to set announce: true). Can be specified multiple times.
- `--debug|-d` - Enable debug mode (more logs). Can be specified multiple times.
- `--max-storage|-m [int]` - Max storage usage, in Mb (defaults to 100000)
- `--autodiscovery-rpc-key [autodiscovery-rpc-key]` - Public key where the autodiscovery service is listening. When set, the autodiscovery-seed must also be set. Can be hex or z32.
- `--autodiscovery-seed [autodiscovery-seed]` - 64-byte seed used to authenticate to the autodiscovery service. Can be hex or z32.
- `--autodiscovery-service-name [autodiscovery-service-name]` - Name under which to register the service (default blind-peer)
- `--scraper-public-key [scraper-public-key]` - Public key of a dht-prometheus scraper. Can be hex or z32.
- `--scraper-secret [scraper-secret]` - Secret of the dht-prometheus scraper. Can be hex or z32.
- `--scraper-alias [scraper-alias]` - (optional) Alias with which to register to the scraper

### Output

When started, ndjson (pino) will be emitted for events. An example startup will look like:

```
{"level":30,"time":1751662694931,"pid":96069,"hostname":"L293","msg":"Starting blind peer"}
{"level":30,"time":1751662694932,"pid":96069,"hostname":"L293","msg":"Using storage 'blind-peer'"}
{"level":30,"time":1751662696936,"pid":96069,"hostname":"L293","msg":"Blind peer listening, local address is 10.0.0.214:49741"}
{"level":30,"time":1751662696936,"pid":96069,"hostname":"L293","msg":"Bytes allocated: 0B of 100GB"}
{"level":30,"time":1751662696936,"pid":96069,"hostname":"L293","msg":"Listening at es4n7ty45odd1udfqyi9xz58mrbheuhdnxgdufsn9gz6e5uhsqco"}
{"level":30,"time":1751662696936,"pid":96069,"hostname":"L293","msg":"Encryption public key is ur7d9r7s3zf1ryibixt5139bep67y94s5bg4gckzo1p6qgtwwfyy"}
```

### Using a Blind Peer

To use a blind peer, use [blind-peering](https://github.com/holepunchto/blind-peering)

Here is an example, using the key from above

```
import BlindPeering from 'blind-peering'
import Hyperswarm from 'hyperswarm'
import Corestore from 'corestore'
import Wakeup from 'protomux-wakeup'

const store = new Corestore(Pear.config.storage)
const swarm = new Hyperswarm()
const wakeup = new Wakeup()

const DEFAULT_BLIND_PEER_KEYS = ['es4n7ty45odd1udfqyi9xz58mrbheuhdnxgdufsn9gz6e5uhsqco']
const blind = new BlindPeering(swarm, store, { wakeup, mirrors: DEFAULT_BLIND_PEER_KEYS })

// Add your autobase
blind.addAutobaseBackground(autobase1)

// Add another core
blind.addCore(core1, autobase1.wakeupCapability.key)

```

Related services:

 https://github.com/holepunchto/autobase-discovery
 https://github.com/HDegroote/dht-prometheus


## Programmatic Usage

``` js
const BlindPeer = require('blind-peer')
```

## License

Apache-2.0
