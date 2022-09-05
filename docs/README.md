<div align="center">
<!--<br/><br/><br/><br/><br/><br/><br/>
<br/><br/><br/><br/><br/><br/><br/>-->
<h1>&lt;datastream&gt;</h1>
<p>Commonly used stream patterns for Web Streams API and NodeJS Stream.</p>
<p>If you're iterating over an array more than once, it's time to use streams.</p>
<br />
<p>
  <a href="https://www.npmjs.com/package/@datastream/core?activeTab=versions">
    <img src="https://badge.fury.io/js/@datastream/core.svg" alt="npm version" style="max-width:100%;">
  </a>
  <a href="https://packagephobia.com/result?p=@datastream/core">
    <img src="https://packagephobia.com/badge?p=@datastream/core" alt="npm install size" style="max-width:100%;">
  </a>
  <!--<a href="https://github.com/willfarrell/datastream/actions/workflows/tests.yml">
    <img src="https://github.com/willfarrell/datastream/actions/workflows/tests.yml/badge.svg?branch=main&event=push" alt="GitHub Actions CI status badge" style="max-width:100%;">
  </a>-->
  <br/>
   <a href="https://standardjs.com/">
    <img src="https://img.shields.io/badge/code_style-standard-brightgreen.svg" alt="Standard Code Style"  style="max-width:100%;">
  </a>
  <!--<a href="https://snyk.io/test/github/willfarrell/datastream">
    <img src="https://snyk.io/test/github/willfarrell/datastream/badge.svg" alt="Known Vulnerabilities" data-canonical-src="https://snyk.io/test/github/willfarrell/datastream" style="max-width:100%;">
  </a>
  <a href="https://github.com/willfarrell/datastream/actions/workflows/sast.yml">
    <img src="https://github.com/willfarrell/datastream/actions/workflows/sast.yml/badge.svg?branch=main&event=push" alt="SAST" style="max-width:100%;">
  </a>
  <a href="https://bestpractices.coreinfrastructure.org/projects/0000">
    <img src="https://bestpractices.coreinfrastructure.org/projects/0000/badge" alt="Core Infrastructure Initiative (CII) Best Practices"  style="max-width:100%;">
  </a>-->
</p>
</div>

Warning: This library is in Alpha, and will contain breaking changes as modules mature to have consistent usage patterns.

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
