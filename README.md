# blind-peer

Blind peers help keep hypercores available.

For the client side responsible for requesting cores be kept by Blind Peers, see [blind-peering](https://github.com/holepunchto/blind-peering).

To run the server as a CLI, see [blind-peer-cli](https://github.com/holepunchto/blind-peer-cli).

## Installation

```
npm install blind-peer
```

## Usage

to run a blind-peer server, use [blind-peer-cli](https://github.com/holepunchto/blind-peer-cli).

### Using a Blind Peer

To talk to a blind peer, use [blind-peering](https://github.com/holepunchto/blind-peering)

Here is an example:

```js
import BlindPeering from 'blind-peering'
import Hyperswarm from 'hyperswarm'
import Corestore from 'corestore'
import Wakeup from 'protomux-wakeup'

const store = new Corestore(Pear.config.storage)
const swarm = new Hyperswarm()
const wakeup = new Wakeup()

const DEFAULT_BLIND_PEER_KEYS = ['es4n7ty45odd1udfqyi9xz58mrbheuhdnxgdufsn9gz6e5uhsqco'] // replace with your own key
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

```js
const BlindPeer = require('blind-peer')
```

## License

Apache-2.0
