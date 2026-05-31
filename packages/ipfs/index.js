// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createPassThroughStream,
	createReadableStream,
} from "@datastream/core";

export const ipfsGetStream = async ({ node, cid }, streamOptions = {}) => {
	const source = await node.get(cid);
	return createReadableStream(source, streamOptions);
};

export const ipfsAddStream = async (
	{ node, resultKey } = {},
	streamOptions = {},
) => {
	let cid;
	// Bridge the pass-through's per-chunk callbacks into an async iterable so
	// node.add consumes the stream incrementally instead of buffering it all
	// in memory. A single-slot pull handshake gives real backpressure: each
	// produced chunk is held until source() pulls it, so the transform's
	// transform() callback only completes once node.add has consumed the chunk.
	let slot; // the chunk currently waiting to be pulled, when filled === true
	let filled = false;
	let done = false;
	let error;
	// Set once node.add settles. A client may resolve without draining the
	// source (it took what it needed); once that happens there is no consumer
	// left to pull, so remaining chunks pass straight through without parking.
	let consumerDone = false;
	// Resolves when a produced chunk has been pulled by source() (back-pressure
	// release for the producer).
	let resolvePulled;
	// Resolves when a chunk has been produced (or done/error set) for source().
	let resolveProduced;
	const onProduced = () => {
		resolveProduced?.();
		resolveProduced = undefined;
	};
	const onPulled = () => {
		resolvePulled?.();
		resolvePulled = undefined;
	};

	async function* source() {
		while (true) {
			if (error) throw error;
			if (filled) {
				const chunk = slot;
				slot = undefined;
				filled = false;
				// Release the producer now that this chunk has been pulled.
				onPulled();
				yield chunk;
				continue;
			}
			if (done) return;
			// Wait for the next chunk (or done/error) to be produced.
			await new Promise((resolve) => {
				resolveProduced = resolve;
			});
		}
	}

	// Kick off the add so it pulls from the source as chunks arrive. Wrap in
	// Promise.resolve so a client whose add() returns synchronously (or any
	// non-Promise thenable) is normalized — mirrors ipfsGetStream's `await
	// node.get`, and avoids a cryptic "add(...).then is not a function".
	const addPromise = Promise.resolve(node.add(source())).then(
		(result) => {
			// The consumer is finished; release any parked producer so a client
			// that resolves without draining the source can't deadlock the
			// transform. Do this before validating the shape so the producer is
			// released even on a malformed result.
			consumerDone = true;
			onPulled();
			// Guard against a legacy / differently-shaped client that resolves
			// without { cid }; surface a clear, actionable error instead of an
			// opaque "Cannot read properties of undefined (reading 'cid')".
			if (result == null || typeof result !== "object" || !("cid" in result)) {
				throw new Error("node.add did not return { cid }");
			}
			cid = result.cid;
		},
		(e) => {
			// Record the consumer failure so a still-blocked producer is released
			// with the error instead of hanging.
			error ??= e;
			consumerDone = true;
			onPulled();
			throw e;
		},
	);
	// Swallow the rejection on this reference; the flush() handler awaits and
	// re-throws so the pipeline still sees it, but an unobserved teardown path
	// must not surface an unhandled rejection.
	addPromise.catch(() => {});

	const stream = createPassThroughStream(
		(chunk) =>
			// Park the chunk and resolve only once source() has pulled it. This
			// awaited promise is what applies backpressure to upstream.
			new Promise((resolve, reject) => {
				if (error) {
					reject(error);
					return;
				}
				// No consumer left to pull (node.add already settled): pass
				// through immediately instead of parking forever.
				if (consumerDone) {
					resolve();
					return;
				}
				slot = chunk;
				filled = true;
				onProduced();
				resolvePulled = () => {
					if (error) reject(error);
					else resolve();
				};
			}),
		async () => {
			done = true;
			onProduced();
			await addPromise;
		},
		streamOptions,
	);

	// If the transform errors or is destroyed (e.g. aborted, or an upstream
	// pipeline error tears it down), neither transform() nor flush() will run
	// again. Inject the error into source() so the generator throws and
	// node.add settles instead of stranding forever on a parked promise.
	const teardown = (e) => {
		if (done) return;
		error ??= e ?? new Error("ipfsAddStream destroyed");
		// Wake both sides: source() so it throws, and any parked producer so its
		// transform() callback rejects.
		onProduced();
		onPulled();
	};
	stream.on("error", teardown);
	stream.on("close", () => teardown());

	stream.result = () => ({
		key: resultKey ?? "cid",
		value: cid,
	});
	return stream;
};

export default {
	getStream: ipfsGetStream,
	addStream: ipfsAddStream,
};
