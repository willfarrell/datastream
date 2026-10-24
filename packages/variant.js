// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
const flag = "--conditions=";
export const variant =
	process.execArgv.find((arg) => arg.startsWith(flag))?.slice(flag.length) ??
	"node";
