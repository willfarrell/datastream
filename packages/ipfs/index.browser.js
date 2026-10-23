// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { ipfsGetStream, makeIpfsAddStream } from "./shared.js";

export { ipfsGetStream };
// ponytail: web TransformStreams emit no error/close events, so the abort
// signal is the only hook that can release a parked node.add; an upstream
// pipeline error still leaves it parked. Upgrade path: have core's
// createPassThroughStream expose its cancel() hook.
// The signal can outlive the stream (one controller for many runs), so the
// listener is removed once node.add settles rather than leaked per run.
export const ipfsAddStream = makeIpfsAddStream(
	(_stream, teardown, { signal }) => {
		const onAbort = () => teardown(signal.reason);
		signal?.addEventListener("abort", onAbort, { once: true });
		return () => signal?.removeEventListener("abort", onAbort);
	},
);
