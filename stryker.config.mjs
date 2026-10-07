// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
const pkg = process.env.MUTATE_PACKAGE;
const base = pkg ? `packages/${pkg}` : "packages";
// MUTATE_VARIANT=browser mutates the browser bundles and runs the suite with
// --conditions=browser. Per package only: the whole-repo glob includes aws and
// azure, which are node-only (no browser build).
const browser = process.env.MUTATE_VARIANT === "browser";
const conditions = browser ? "--conditions=browser " : "";
// Shared plain .js (helpers/shared/guard) is loaded as-is by both bundles and
// fully covered by the node run, so it is only mutated there. native.browser.js
// is browser-only, so the node run never loads it.
const mutate = browser
	? [`${base}/**/*.browser.mjs`, `${base}/**/native.browser.js`]
	: [
			`${base}/**/*.node.mjs`,
			`${base}/**/guard.node.js`,
			`${base}/**/helpers.js`,
			`${base}/**/shared.js`,
			`${base}/**/client.js`,
		];

/** @type {import('@stryker-mutator/api/core').PartialStrykerOptions} */
export default {
	packageManager: "npm",
	testRunner: "command",
	commandRunner: {
		command: `node --test ${conditions}--test-force-exit ./${base}/**/*.test.js`,
	},
	coverageAnalysis: "off",
	mutate: [...mutate, "!**/*.map", "!**/node_modules/**"],
	plugins: ["@stryker-mutator/*"],
	reporters: ["progress", "clear-text"],
	thresholds: { high: 100, low: 100, break: 100 },
	// WebCrypto importKey's `extractable: false` flags are the only booleans in the
	// encrypt browser build and cannot be observed from outside the key object.
	...(browser && pkg === "encrypt"
		? { mutator: { excludedMutations: ["BooleanLiteral"] } }
		: {}),
	tempDirName: `/tmp/stryker/${browser ? "browser" : "node"}/@datastream/${pkg ?? "all"}`,
};
