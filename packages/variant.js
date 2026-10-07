// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Test-only. Which build the suite is exercising, set by `node --test --conditions=browser`.
const flag = "--conditions=";
export const variant =
	process.execArgv.find((arg) => arg.startsWith(flag))?.slice(flag.length) ??
	"node";
