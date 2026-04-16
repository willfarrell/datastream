# Contributing

In the spirit of Open Source Software, everyone is very welcome to contribute to this repository. Feel free to [raise issues](https://github.com/willfarrell/datastream/issues) or to [submit Pull Requests](https://github.com/willfarrell/datastream/pulls).

Before contributing to the project, make sure to have a look at our [Code of Conduct](/.github/CODE_OF_CONDUCT.md).


## Development Setup

### Prerequisites

- Node.js >= 24
- npm (comes with Node.js)

### Getting Started

```bash
git clone https://github.com/willfarrell/datastream.git
cd datastream
npm install
```

This installs all dependencies across all workspaces (`packages/*`, `websites/*`, `.github`).


## Project Structure

```
packages/       # npm packages (core, csv, compress, encrypt, etc.)
websites/       # documentation website (datastream.js.org)
.github/        # CI workflows, workflow-specific dependencies
bin/            # build scripts
docs/           # additional documentation
```

Each package in `packages/` has both a Node.js stream implementation (`index.node.js`) and a Web Streams API implementation (`index.web.js`), selected via conditional exports.


## Testing

```bash
# Run all checks (lint, unit, types, sast, perf, dast)
npm test

# Run specific test suites
npm run test:lint          # Biome linting
npm run test:unit          # Unit tests (both Node.js and Web stream variants)
npm run test:unit:node     # Unit tests (Node.js streams only)
npm run test:unit:web      # Unit tests (Web Streams API only)
npm run test:types         # TypeScript type checking (tstyche)
npm run test:perf          # Performance benchmarks (tinybench)
npm run test:dast          # Fuzz tests (fast-check)

# Run tests for a single package
node --test ./packages/core
```

Unit tests use Node.js built-in `node:test`. Both `--conditions=node` and `--conditions=webstream` are tested to cover both stream implementations.


## Building

```bash
npm run build
```

Produces dual ESM builds per package via esbuild: `*.node.mjs` (Node.js) and `*.web.mjs` (Web Streams API), both with external source maps.


## Code Style

- Formatting and linting are handled by [Biome](https://biomejs.dev/) (`biome.json`)
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) enforced by commitlint
- Husky pre-commit hooks run linting and tests automatically


## Licence

Licensed under [MIT Licence](LICENSE). Copyright (c) 2026 [will Farrell](https://github.com/willfarrell), and the [datastream team](https://github.com/willfarrell/datastream/graphs/contributors).
