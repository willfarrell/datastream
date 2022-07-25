<div align="center">
<!--<br/><br/><br/><br/><br/><br/><br/>
<br/><br/><br/><br/><br/><br/><br/>-->
<h1>&lt;datastream&gt;</h1>
<p>Commonly used stream patterns for Web Streams API and NodeJS Stream.</p>
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
    <img src="https://snyk.io/test/github/willfarrell/csv-rex/badge.svg" alt="Known Vulnerabilities" data-canonical-src="https://snyk.io/test/github/willfarrell/csv-rex" style="max-width:100%;">
  </a>
  <a href="https://github.com/willfarrell/datastream/actions/workflows/sast.yml">
    <img src="https://github.com/willfarrell/datastream/actions/workflows/sast.yml/badge.svg?branch=main&event=push" alt="SAST" style="max-width:100%;">
  </a>
  <a href="https://bestpractices.coreinfrastructure.org/projects/0000">
    <img src="https://bestpractices.coreinfrastructure.org/projects/0000/badge" alt="Core Infrastructure Initiative (CII) Best Practices"  style="max-width:100%;">
  </a>-->
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
- `@datastream/string`
  - stringReadableStream [Readable]
  - stringLengthStream [PassThrough]
  - stringOutputStream [PassThrough]
- `@datastream/object`
  - objectReadableStream [Readable]
  - objectCountStream [PassThrough]
  - objectBatchStream [Transform]
  - objectOutputStream [PassThrough]

### Common
- `@datastream/digest`
  - digestStream [PassThrough]

### Advanced
- `@datastream/csv[/{parse,format}]`
  - csvParseStream [Transform]
  - csvFormatStream [Transform]

## Setup
```bash
npm install @datastream/core @datastream/{module}
```

<a id="core"></a>
## Core

- `pipeline(stream[], options)`: Connects streams and awaits until completion. Returns results from stream taps. Will add in a terminating Writable if missing.
- `pipejoin(stream[])`: Connects streams and returns resulting stream for use with async iterators
- `streamToArray(stream)`: Returns array from stream chunks. stream must not end with Writable.
- `streamToString(stream)`: Returns string from stream chunks. stream must not end with Writable.
- `isReadable(stream)`: Return bool is stream is Readable
- `isWritable(stream)`: Return bool is stream is Writable
- `makeOptions(options)`: Make options interoperable between Readable/Writable and Transform
- `creatReadableStream(input, options)`: Create a Readable stream from input (string, array, iterable) with options.
- `creatTransformStream((chunk)=>{}, options)`: Create a Transform stream that allows mutation of chunk before being passed.
- `creatWritableStream((chunk)=>{}, options)`: Create a Writable stream that allows mutation of chunk before being passed.

- `options`:
  - `highWaterMark`
  - `chunkSize`
  - `signal`

### Examples
#### 
```javascript
import {pipejoin, streamToArray, createReadableStream, createTransformStream} from '@datastream/core'
import {csvParseStream} from '@datastream/csv'

let count
const streams = [
  createReadableStream('a,b,c\r\n1,2,3'),
  createTransformStream(() => { count += 1}),
  createTransformStream(console.log),
]

const river = pipejoin(streams)
const output = await streamToArray(river)
```
## Roadmap
- Documentation
- More stream modules
  - charset detection and encoding/decoding 
  - compression
  - JSON Schema
- Possible future stream modules
  - JSON Stream
  - JSON
  - IPFS
  - encrypt/decrypt
  - XML

