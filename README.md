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
  <br/>
  <a href="https://gitter.im/willfarrell/Lobby"><img src="https://badges.gitter.im/gitterHQ/gitter.svg" alt="Chat on Gitter" style="max-width:100%;"></a>
  <a href="https://stackoverflow.com/questions/tagged/datastream?sort=Newest&uqlId=35052"><img src="https://img.shields.io/badge/StackOverflow-[datastream]-yellow" alt="Ask questions on StackOverflow" style="max-width:100%;"></a>
</p>
</div>

Warning: This library is in Alpha, and will contain breaking changes as modules mature to have consistent usage patterns.

## Roadmap

- Documentation
- Extend Modules
  - core
    - merge - connect multiple readable into one stream (see fetch)
  - csv
    - break into parts
      - csvDetectDelimitors
      - csvDetectHeaders
      - csvParse w/ hooks for error checks
      - csvSkipEmptyRows
      - csvMapHeaders -> 
      - csvCoerceValues -> validate w/ coerce
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
