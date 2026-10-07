// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Resolved via package.json `imports` ("#deepEqual") so the node build keeps
// node:util's full isDeepStrictEqual while the browser build gets the
// structural fallback from helpers.js.
export { isDeepStrictEqual as deepEqual } from "node:util";
