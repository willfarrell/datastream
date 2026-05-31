// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
const pkg = process.env.MUTATE_PACKAGE;
const base = pkg ? `packages/${pkg}` : "packages";

/** @type {import('@stryker-mutator/api/core').PartialStrykerOptions} */
export default {
	packageManager: "npm",
	testRunner: "command",
	commandRunner: {
		command: `node --test --conditions=node --test-force-exit ./${base}/**/*.test.js`,
	},
	coverageAnalysis: "off",
	mutate: [`${base}/**/*.node.mjs`, "!**/*.map", "!**/node_modules/**"],
	plugins: ["@stryker-mutator/*"],
	reporters: ["progress", "clear-text"],
	thresholds: { high: 100, low: 100, break: 100 },
	tempDirName: pkg
		? `/tmp/stryker/@datastream/${pkg}`
		: "/tmp/stryker/@datastream",
};
