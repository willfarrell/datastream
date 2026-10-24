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
| `node` | `IPFS` | — | IPFS node instance (js-ipfs, kubo-rpc-client, or any client with `get(cid, { signal })` / `add(source, { signal })`) |
| `cid` | `string` | — | Content identifier |

`streamOptions.signal` is passed to the client as `node.get(cid, { signal })`, so aborting the pipeline also cancels the request.

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
| `node` | `IPFS` | — | IPFS node instance (js-ipfs, kubo-rpc-client, or any client with `get(cid, { signal })` / `add(source, { signal })`) |
| `resultKey` | `string` | `"cid"` | Key in pipeline result |

`node.add` receives the incoming chunks as an async iterable, read one chunk at a time, plus `{ signal }` from `streamOptions.signal`, so aborting the pipeline also cancels the upload. In the browser, the stream listens for that signal's `abort` once and removes the listener when `node.add` settles, so one long-lived signal can be reused across many runs.

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
