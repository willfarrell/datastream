---
title: Introduction
slug: /
---

## What is datastream

Datastream is a collection of commonly used **stream patterns** for the **Web Streams API** and **Node.js Streams**.

If you're iterating over an array more than once, it's time to use streams.

## A quick example

Code is better than 10,000 words, so let's jump into an example.

Let's assume you want to read a CSV file, validate the data, and compress it:

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { csvParseStream } from '@datastream/csv'
import { validateStream } from '@datastream/validate'
import { gzipCompressStream } from '@datastream/compress'

const streams = [
  createReadableStream(csvData),
  csvParseStream({ header: true }),
  validateStream(schema),
  gzipCompressStream()
]

await pipeline(streams)
```

## Stream types

- **Readable**: The start of a pipeline that injects data into a stream.
- **PassThrough**: Does not modify the data, but listens and prepares a result that can be retrieved.
- **Transform**: Modifies data as it passes through.
- **Writable**: The end of a pipeline that stores data from the stream.

## Setup

```bash
npm install @datastream/core @datastream/{module}
```

## Why streams?

Streams allow you to process data incrementally, without loading everything into memory at once. This is essential for:

- **Large files**: Process gigabytes of data with constant memory usage
- **Real-time data**: Handle data as it arrives, not after it's all collected
- **Composability**: Chain simple operations into complex data pipelines
- **Performance**: Start processing before all data is available

Datastream provides ready-made stream patterns so you can focus on your data flow instead of stream plumbing.

## Next steps

Ready to dive in? Head to the [Quick Start](/docs/quick-start) guide.
