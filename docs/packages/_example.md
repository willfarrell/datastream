# @datastream/{package}

// ToC

## Setup

```bash
npm install @datastream/{package}
```

## Support

| Stream                | node:stream | node:stream/web | Chrome | Edge | Firefox | Safari | Comments     |
| --------------------- | ----------- | --------------- | ------ | ---- | ------- | ------ | ------------ |
| {package}{name}Stream | 18.x        | 18.x            | 67     | 79   | 102     | 14.1   | Uses ... API |

### NodeJS

- pipeline: [15.0.0](https://nodejs.org/dist/latest-v18.x/docs/api/stream.html#streams-promises-api)
- fetch: [18.0.0](https://nodejs.org/en/blog/announcements/v18-release-announce/#fetch-experimental)

### NodeJS (Web Stream)

- ReadableStream: [18.0.0](https://nodejs.org/dist/latest-v18.x/docs/api/webstreams.html#new-readablestreamunderlyingsource--strategy)
- TransformStream: [18.0.0](https://nodejs.org/dist/latest-v18.x/docs/api/webstreams.html#new-transformstreamtransformer-writablestrategy-readablestrategy)
- WritableStream: [18.0.0](https://nodejs.org/dist/latest-v18.x/docs/api/webstreams.html#new-writablestreamunderlyingsink-strategy)
- CompressionStream (gzip, deflate): [18.0.0](https://nodejs.org/dist/latest-v18.x/docs/api/webstreams.html#new-compressionstreamformat)
- DecompressionStream (gzip, deflate): [18.0.0](https://nodejs.org/dist/latest-v18.x/docs/api/webstreams.html#new-decompressionstreamformat)
- TextEncoderStream: [18.0.0](https://nodejs.org/dist/latest-v18.x/docs/api/webstreams.html#new-textencoderstream)
- TextDecoderStream: [18.0.0](https://nodejs.org/dist/latest-v18.x/docs/api/webstreams.html#new-textdecoderstreamencoding-options)

### Browser (Web Stream)

- ReadableStream: [caniuse](https://caniuse.com/mdn-api_readablestream)
  - 
- TransformStream: [caniuse](https://caniuse.com/mdn-api_transformstream)
- WritableStream: [caniuse](https://caniuse.com/mdn-api_writablestream)
- CompressionStream (gzip, deflate): [caniuse](https://caniuse.com/mdn-api_compressionstream)
- DecompressionStream (gzip, deflate): [caniuse](https://caniuse.com/mdn-api_decompressionstream)
- TextEncoderStream: [caniuse](https://caniuse.com/mdn-api_textencoderstream)
- TextDecoderStream: [caniuse](https://caniuse.com/mdn-api_textdecoderstream)

## Help make streams better

Feature request(s) to +1

- W3C:
  - webcrypto Streams: https://github.com/w3c/webcrypto/issues/73
  - webcrypto SHA3: https://github.com/w3c/webcrypto/issues/319
- WHATWG:
  - ReadableStream is async iterable: https://github.com/whatwg/streams/issues/778#issuecomment-461341033
- Web Incubator Community Group (WICG):
  - CompressionStream (brotli): https://github.com/WICG/compression/issues/34
  - CompressionStream (zstd): not found
- NodeJS TC39:
- Chrome:
- Firefox:
  - CompressionStream: https://bugzilla.mozilla.org/show_bug.cgi?id=1586639
  - TextDecoderStream: https://bugzilla.mozilla.org/show_bug.cgi?id=1486949
  - TextEncoderStream: https://bugzilla.mozilla.org/show_bug.cgi?id=1486949
- Safari:
  - CompressionStream: not found

## Streams

<a id="{package}{name}Stream"></a>

### {package}{name}Stream ({Readable,PassThrough,Transform,Writable})

#### Options

#### Example(s)
