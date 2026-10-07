// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Alternative runner: the forked @stryker-mutator/node-test-runner. perTest
// coverage, ~40% faster than the command runner, but it is NOT in
// devDependencies - install the fork before using this config.
//
// Scope it the same way as the default config:
//   MUTATE_PACKAGE=encrypt npx stryker run stryker.node-test.config.mjs
// One package at a time: a whole-repo dry run fails on cross-file
// contamination.
import base from "./stryker.config.mjs";

const pkg = process.env.MUTATE_PACKAGE;
const scope = pkg ? `packages/${pkg}` : "packages";
const browser = process.env.MUTATE_VARIANT === "browser";

// The command runner's `commandRunner` key is meaningless here.
const { commandRunner, ...shared } = base;

export default {
	...shared,
	testRunner: "node-test",
	nodeTest: {
		testFiles: [`${scope}/**/*.test.js`],
		...(browser ? { nodeArgs: ["--conditions=browser"] } : {}),
		concurrency: false,
	},
	coverageAnalysis: "perTest",
	plugins: ["@stryker-mutator/node-test-runner"],
	tempDirName: `/tmp/stryker/node-test/${browser ? "browser" : "node"}/@datastream/${pkg ?? "all"}`,
};
