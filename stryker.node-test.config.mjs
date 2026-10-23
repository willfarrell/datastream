// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import base from "./stryker.config.mjs";

const pkg = process.env.MUTATE_PACKAGE;
const scope = pkg ? `packages/${pkg}` : "packages";
const browser = process.env.MUTATE_VARIANT === "browser";

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
