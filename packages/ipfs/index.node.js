// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { ipfsGetStream, makeIpfsAddStream } from "./shared.js";

export { ipfsGetStream };
export const ipfsAddStream = makeIpfsAddStream((stream, teardown) => {
	stream.on("error", teardown);
	stream.on("close", () => teardown());
	return () => {};
});
