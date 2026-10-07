// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { ipfsGetStream, makeIpfsAddStream } from "./shared.js";

export { ipfsGetStream };
// If the transform errors or is destroyed (aborted, or an upstream pipeline
// error tears it down), neither transform() nor flush() runs again; the
// lifecycle events are the hook that lets a parked node.add settle.
export const ipfsAddStream = makeIpfsAddStream((stream, teardown) => {
	stream.on("error", teardown);
	stream.on("close", () => teardown());
});
