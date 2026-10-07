# Contributing

In the spirit of Open Source Software, everyone is very welcome to contribute to this repository. Feel free to [raise issues](https://github.com/willfarrell/datastream/issues) or to [submit Pull Requests](https://github.com/willfarrell/datastream/pulls).

Before contributing to the project, make sure to have a look at our [Code of Conduct](/.github/CODE_OF_CONDUCT.md).


## Development Setup

### Prerequisites

- Node.js >= 26
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
```

Each package in `packages/` has both a Node.js stream implementation (`index.node.js`) and a Web Streams API implementation (`index.browser.js`), selected via the `node` and `browser` export conditions. A feature a platform can't support has no export condition for it: `aws` and `kafka` are Node.js only, `indexeddb` is browser only, and `compress` zstd is Node.js only.


## Testing

```bash
# Run all checks (lint, types, unit, dast, sast, bench, mutation)
npm test

# Run specific test suites (build first: test suites run against the built *.mjs)
npm run build
npm run test:lint          # Biome linting
npm run test:unit          # Unit tests, node build (100% coverage gate on *.node.mjs + shared .js)
npm run test:unit:browser  # Unit tests, browser build (100% coverage gate on *.browser.mjs)
npm run test:types         # TypeScript type checking (tstyche)
npm run test:bench         # Performance benchmarks (node:bench, packages/**/*.bench.js)
npm run test:dast          # Fuzz tests (fast-check)
npm run test:mutation      # Mutation tests (Stryker) for every package, one at a time
sh bin/stryker <package>   # Mutation tests for one package (each exported platform)

# Run tests for a single package, against either build
node --test ./packages/core
node --test --conditions=browser ./packages/core
```

Unit tests use Node.js built-in `node:test`. The same test files run twice: once against the node build and once with `--conditions=browser`, which makes every `@datastream/*` import resolve to the browser build. Tests read that flag into `variant`; node-only cases use `nodeTest` (an alias for `test.skip` under the browser run) and browser-only cases sit in `if (variant === "browser")` blocks. `packages/aws` and `packages/kafka` are node-only (no browser build), so they are excluded from the browser run. `packages/indexeddb` is browser-only (no node export condition): its tests run in both passes, and the node pass imports the browser source directly and drives it with mocked IndexedDB objects. Mutation testing follows the same split: `MUTATE_VARIANT=browser` mutates `*.browser.mjs` and runs the suite with `--conditions=browser`; shared plain `.js` files (`helpers.js`, `shared.js`, `guard.node.js`) are mutated by the node run only (the browser-only `compress/native.browser.js` by the browser run), so keep platform-specific code in `index.node.js` / `index.browser.js`. A dependency contract the real module never exercises gets its own `*.test.js` that swaps the module with `module.register()` (loader hooks are per test file) — see `encrypt/libsodium.test.js` and `compress/brotli-partial.test.js`.


## Building

```bash
npm run build
```

Produces dual ESM builds per package via esbuild: `*.node.mjs` (Node.js) and `*.browser.mjs` (Web Streams API), both with external source maps. Packages without a `browser` export condition (`aws`, `kafka`) get the node build only.


## Code Style

- Formatting and linting are handled by [Biome](https://biomejs.dev/) (`biome.json`)
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) enforced by commitlint
- Husky hooks run linting (pre-commit) and tests (pre-push). Install scripts are disabled (`ignore-scripts=true`), so `prepare` never installs them: run `npx husky` once after cloning


## Licence

Licensed under [MIT Licence](../LICENSE). Copyright (c) 2026 [will Farrell](https://github.com/willfarrell), and the [datastream team](https://github.com/willfarrell/datastream/graphs/contributors).
