# blind-peer

Blind peers help keep hypercores available.

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

## Programmatic Usage

``` js
const BlindPeer = require('blind-peer')
```

## License

Apache-2.0
