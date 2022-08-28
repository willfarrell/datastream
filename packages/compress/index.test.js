import test from 'node:test'
import { equal } from 'node:assert'
import {
  pipejoin,
  streamToString,
  createReadableStream
} from '@datastream/core'

import {
  gzipCompressStream,
  gzipDecompressStream
} from '@datastream/compress/gzip'
import {
  deflateCompressStream,
  deflateDecompressStream
} from '@datastream/compress/deflate'

import {
  brotliCompressSync,
  // brotliDecompressSync,
  gzipSync,
  // gunzipSync,
  deflateSync
  // inflateSync
} from 'zlib'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes('--conditions=')) {
    variant = execArgv.replace(flag, '')
  }
}

const compressibleBody = JSON.stringify(new Array(1024).fill(0))

// *** brotli *** //
if (variant === 'node') {
  const { brotliCompressStream, brotliDecompressStream } = await import(
    '@datastream/compress/brotli'
  )

  // unable to test on NodeJS due to esm / fetch(wasm)
  test(`${variant}: brotliCompressStream should compress`, async (t) => {
    const input = compressibleBody
    const streams = [createReadableStream(input), brotliCompressStream()]
    const output = await streamToString(pipejoin(streams))
    equal(output, brotliCompressSync(compressibleBody))
    // equal(brotliDecompressSync(output), compressibleBody)
  })

  test(`${variant}: brotliDecompressStream should decompress`, async (t) => {
    const input = brotliCompressSync(compressibleBody)
    const streams = [createReadableStream(input), brotliDecompressStream()]
    const output = await streamToString(pipejoin(streams))
    equal(output, compressibleBody)
  })

  // *** gzip *** //
  test(`${variant}: gzipCompressStream should compress`, async (t) => {
    const input = compressibleBody
    const streams = [createReadableStream(input), gzipCompressStream()]
    const output = await streamToString(pipejoin(streams))
    equal(output, gzipSync(compressibleBody))
    // equal(gunzipSync(output), compressibleBody)
  })

  test(`${variant}: gzipDecompressStream should decompress`, async (t) => {
    const input = gzipSync(compressibleBody)
    const streams = [createReadableStream(input), gzipDecompressStream()]
    const output = await streamToString(pipejoin(streams))
    equal(output, compressibleBody)
  })

  // *** deflate *** //
  test(`${variant}: deflateCompressStream should compress`, async (t) => {
    const input = compressibleBody
    const streams = [createReadableStream(input), deflateCompressStream()]
    const output = await streamToString(pipejoin(streams))
    equal(output, deflateSync(compressibleBody))
    // equal(inflateSync(output), compressibleBody)
  })

  test(`${variant}: deflateDecompressStream should decompress`, async (t) => {
    const input = deflateSync(compressibleBody)
    const streams = [createReadableStream(input), deflateDecompressStream()]
    const output = await streamToString(pipejoin(streams))
    equal(output, compressibleBody)
  })
}
