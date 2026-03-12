<div align="center">
<!--<br/><br/><br/><br/><br/><br/><br/>
<br/><br/><br/><br/><br/><br/><br/>-->
<h1>&lt;datastream&gt;</h1>
<p>Commonly used stream patterns for Web Streams API and NodeJS Stream.</p>
<p>If you're iterating over an array more than once, it's time to use streams.</p>
<br />
<p>
  <a href="https://github.com/willfarrell/datastream/actions/workflows/test-unit.yml"><img src="https://github.com/willfarrell/datastream/actions/workflows/test-unit.yml/badge.svg" alt="GitHub Actions unit test status"></a>
  <a href="https://github.com/willfarrell/datastream/actions/workflows/test-dast.yml"><img src="https://github.com/willfarrell/datastream/actions/workflows/test-dast.yml/badge.svg" alt="GitHub Actions dast test status"></a>
  <a href="https://github.com/willfarrell/datastream/actions/workflows/test-perf.yml"><img src="https://github.com/willfarrell/datastream/actions/workflows/test-perf.yml/badge.svg" alt="GitHub Actions perf test status"></a>
  <a href="https://github.com/willfarrell/datastream/actions/workflows/test-sast.yml"><img src="https://github.com/willfarrell/datastream/actions/workflows/test-sast.yml/badge.svg" alt="GitHub Actions SAST test status"></a>
  <a href="https://github.com/willfarrell/datastream/actions/workflows/test-lint.yml"><img src="https://github.com/willfarrell/datastream/actions/workflows/test-lint.yml/badge.svg" alt="GitHub Actions lint test status"></a>
  <br/>
  <a href="https://www.npmjs.com/package/@datastream/core"><img alt="npm version" src="https://img.shields.io/npm/v/@datastream/core.svg"></a>
  <a href="https://packagephobia.com/result?p=@datastream/core"><img src="https://packagephobia.com/badge?p=@datastream/core" alt="npm install size"></a>
  <a href="https://www.npmjs.com/package/@datastream/core">
  <img alt="npm weekly downloads" src="https://img.shields.io/npm/dw/@datastream/core.svg"></a>
  <a href="https://www.npmjs.com/package/@datastream/core#provenance">
  <img alt="npm provenance" src="https://img.shields.io/badge/provenance-Yes-brightgreen"></a>
  <br/>
  <a href="https://scorecard.dev/viewer/?uri=github.com/willfarrell/datastream"><img src="https://api.scorecard.dev/projects/github.com/willfarrell/datastream/badge" alt="Open Source Security Foundation (OpenSSF) Scorecard"></a>
  <a href="https://slsa.dev"><img src="https://slsa.dev/images/gh-badge-level3.svg" alt="SLSA 3"></a>
  <a href="https://github.com/willfarrell/datastream/blob/main/docs/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg"></a>
  <a href="https://biomejs.dev"><img alt="Checked with Biome" src="https://img.shields.io/badge/Checked_with-Biome-60a5fa?style=flat&logo=biome"></a>
  <a href="https://conventionalcommits.org"><img alt="Conventional Commits" src="https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white"></a>
  <a href="https://github.com/willfarrell/datastream/blob/main/package.json#L32">
  <img alt="code coverage" src="https://img.shields.io/badge/code%20coverage-95%25-brightgreen"></a>
</p>
</div>

- [`@datastream/core`](#core)
  - pipeline
  - pipejoin
  - streamToArray
  - streamToString
  - isReadable
  - isWritable
  - makeOptions
  - createReadableStream
  - createTransformStream
  - createWritableStream

## Streams

- Readable: The start of a pipeline of streams that injects data into a stream.
- PassThrough: Does not modify the data, but listens to the data and prepares a result that can be retrieved.
- Transform: Modifies data as it passes through.
- Writable: The end of a pipeline of streams that stores data from the stream.

### Basics

- [`@datastream/string`](#string)
  - stringReadableStream [Readable]
  - stringLengthStream [PassThrough]
  - stringOutputStream [PassThrough]
- [`@datastream/object`](#object)
  - objectReadableStream [Readable]
  - objectCountStream [PassThrough]
  - objectBatchStream [Transform]
  - objectOutputStream [PassThrough]

### Common

- [`@datastream/fetch`](#fetch)
  - fetchResponseStream [Readable]
- [`@datastream/charset[/{detect,decode,encode}]`](#charset)
  - charsetDetectStream [PassThrough]
  - charsetDecodeStream [Transform]
  - charsetEncodeStream [Transform]
- [`@datastream/compression[/{gzip,deflate}]`](#compression)
  - gzipCompressionStream [Transform]
  - gzipDecompressionStream [Transform]
  - deflateCompressionStream [Transform]
  - deflateDecompressionStream [Transform]
- [`@datastream/digest`](#digest)
  - digestStream [PassThrough]

### Advanced

- [`@datastream/csv[/{parse,format}]`](#csv)
  - csvParseStream [Transform]
  - csvFormatStream [Transform]
- [`@datastream/validate`](#validate)
  - validateStream [Transform]

## Setup

```bash
npm install @datastream/core @datastream/{module}
```

## Flows

```mermaid
stateDiagram-v2

    [*] --> fileRead*: path
    [*] --> fetchResponse: URL
    [*] --> sqlCopyTo*: SQL
    [*] --> stringReadable: string
    [*] --> stringReadable: string[]
    [*] --> objectReadable: object[]
    [*] --> createReadable: blob

    readable --> charsetDetect: binary
    charsetDetect --> [*]

    readable --> decryption
    decryption --> passThroughBuffer: buffer

    readable --> decompression
    decompression --> passThroughBuffer: buffer
    passThroughBuffer --> charsetDecode: buffer
    charsetDecode --> passThroughString: string
    passThroughString --> parse: string
    parse --> validate: object
    validate --> passThroughObject: object
    passThroughObject --> transform: object

    transform --> format: object
    format --> charsetEncode: string
    charsetEncode --> compression: buffer
    compression --> writable: buffer

    charsetEncode --> encryption: buffer
    encryption --> writable: buffer

    state readable {
        fileRead*
        fetchResponse
        sqlCopyTo*
        createReadable
        stringReadable
        objectReadable
        awsS3Get
        awsDynamoDBQuery
        awsDynamoDBScan
        awsDynamoDBGet
    }

    state decompression {
        brotliDeompression
        gzipDeompression
        deflateDeompression
        zstdDecompression*
        protobufDecompression*
    }

    state decryption {
      decryption*
    }

    state parse {
      csvParse
      jsonParse*
      xmlParse*
    }

    state passThroughBuffer {
      digest
    }

    state passThroughString {
      stringLength
      stringOutput
    }

    state passThroughObject {
      objectCount
      objectOutput
    }

    state transform {
      objectBatch
      objectPivotLongToWide
      objectPivotWideToLong
      objectKeyValue
      objectKeyValue
    }

    state format {
      csvFormat
      jsonFormat*
      xmlFormat*
    }

    state compression {
        brotliCompression
        gzipCompression
        deflateCompression
        zstdCompression*
        protobufCompression*
    }

    state encryption {
      encryption*
    }

    state writable {
        fileWrite*
        fetchRequest*
        sqlCopyFrom*
        awsS3Put
        awsDynamoDBPut
        awsDynamoDBDelete
    }
    writable --> [*]
```

\* possible future package

## Write your own

### Readable

#### NodeJS Streams

- [NodeJS](https://nodejs.org/api/stream.html#class-streamreadable)

#### Web Streams API

- [MDN](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream)
- [NodeJS](https://nodejs.org/api/webstreams.html#class-readablestream)

### Transform

#### NodeJS Streams

- [NodeJS](https://nodejs.org/api/stream.html#class-streamtransform)

#### Web Streams API

- [MDN](https://developer.mozilla.org/en-US/docs/Web/API/TransformStream)
- [NodeJS](https://nodejs.org/api/webstreams.html#class-transformstream)

### Writeable

#### NodeJS Streams

- [NodeJS](https://nodejs.org/api/stream.html#class-streamwritable)

#### Web Streams API

- [MDN](https://developer.mozilla.org/en-US/docs/Web/API/WritableStream)
- [NodeJS](https://nodejs.org/api/webstreams.html#class-writablestream)

## End-to-End Examples

### NodeJS: Import CSV into SQL database

Read a CSV file, validate the structure, pivot data, then save compressed.

- fs.creatReadStream
- gzip
- cryptoDigest
- charsetDecode
- csvParse
- countChunks
- validate
- changeCase (pascal to snake)
- parquet?
- csvFormat
- postgesCopyFrom

### WebWorker: Validate and collect metadata about file prior to upload

- <input type="file">
- cryptoDigest
- charsetDetect
- jsonParse?
- validate

### WebWorker: Upload file compressed

Upload file with brotli compression?

### WebWorker: Decompress protobuf compressed JSON requests

Fetch protobuf file, decompress, parse JSON

### streams

- filter

- file (docs only?)

### examples

- fetch
- node:fs
- input type=file
- readable string/array/etc

## License

Licensed under [MIT License](LICENSE). Copyright (c) 2026 [will Farrell](https://github.com/willfarrell) and [contributors](https://github.com/middyjs/middy/graphs/contributors).
