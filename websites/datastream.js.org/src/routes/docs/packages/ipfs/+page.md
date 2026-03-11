---
title: ipfs
description: IPFS get and add streams.
---

IPFS get and add streams.

## Install

```bash
npm install @datastream/ipfs
```

## `ipfsGetStream` <span class="badge">Readable</span> <span class="badge">async</span>

Retrieves content from IPFS by CID.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `node` | `IPFS` | — | IPFS node instance |
| `cid` | `string` | — | Content identifier |

### Example

```javascript
import { pipeline } from '@datastream/core'
import { ipfsGetStream } from '@datastream/ipfs'

await pipeline([
  await ipfsGetStream({ node: ipfsNode, cid: 'QmHash...' }),
])
```

## `ipfsAddStream` <span class="badge">PassThrough</span> <span class="badge">async</span>

Adds content to IPFS and returns the CID in `.result()`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `node` | `IPFS` | — | IPFS node instance |
| `resultKey` | `string` | `"cid"` | Key in pipeline result |

### Result

The CID of the stored content.

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { ipfsAddStream } from '@datastream/ipfs'

const result = await pipeline([
  createReadableStream('Hello IPFS!'),
  await ipfsAddStream({ node: ipfsNode }),
])

console.log(result)
// { cid: 'QmHash...' }
```
