// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { ipfsGetStream, makeIpfsAddStream } from "./shared.js";

export { ipfsGetStream };
// Web TransformStreams emit no error/close events, so the abort signal is the
// only hook that can release a parked node.add.
export const ipfsAddStream = makeIpfsAddStream(
	(_stream, teardown, { signal }) => {
		const onAbort = () => teardown(signal.reason);
		signal?.addEventListener("abort", onAbort, { once: true });
		return () => signal?.removeEventListener("abort", onAbort);
	},
);
