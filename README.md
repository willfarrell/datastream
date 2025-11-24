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

## Roadmap

- Documentation
- Extend Modules
  - core
    - merge - connect multiple readable into one stream (see fetch)
- New Modules
  - json
    - parse - https://github.com/jimhigson/oboe.js / https://github.com/dscape/clarinet
    - parseChunk (json-stream notation) - fastify/secure-json-parse
    - format
    - formatChunk (json-stream notation) - fastify/fast-json-stringify
    - https://github.com/uhop/stream-json
    - https://github.com/creationix/jsonparse
    - https://github.com/dominictarr/JSONStream
    - https://www.npmjs.com/package/json-stream - string chunk -> look for \n -> JSON.parse
- Maybe Future

  - compression

    - zstd - simple-zstd (NodeStream)
    - protobuf - protobufjs / pbf - https://buf.build/blog/protobuf-es-the-protocol-buffers-typescript-javascript-runtime-we-all-deserve

  - encryption
    - encrypt [transform]
    - decrypt [transform]
  - ipfs
    - readable - get
    - writable - add https://github.com/ipfs/js-ipfs/blob/master/docs/core-api/FILES.md#ipfsadddata-options
      https://github.com/ipfs/js-datastore-s3/blob/master/examples/full-s3-repo/index.js
      // https://github.com/ipfs-examples/js-ipfs-examples/blob/master/examples/browser-add-readable-stream/src/index.js
  - xml
    - parse - https://github.com/isaacs/sax-js / KeeeX/sax-js
    - format
  - type
    - pg - obj to db types
  - benchmark using `node file.js | pv > /dev/null`
