// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
const pkg = process.env.MUTATE_PACKAGE;
const base = pkg ? `packages/${pkg}` : "packages";
const browser = process.env.MUTATE_VARIANT === "browser";
const conditions = browser ? "--conditions=browser " : "";
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
	...(browser && pkg === "encrypt"
		? { mutator: { excludedMutations: ["BooleanLiteral"] } }
		: {}),
	tempDirName: `/tmp/stryker/${browser ? "browser" : "node"}/@datastream/${pkg ?? "all"}`,
};
