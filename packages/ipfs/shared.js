// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createPassThroughStream,
	createReadableStream,
} from "@datastream/core";

export const ipfsGetStream = async ({ node, cid }, streamOptions = {}) => {
	const source = await node.get(cid, { signal: streamOptions.signal });
	return createReadableStream(source, streamOptions);
};

export const makeIpfsAddStream =
	(wireTeardown) =>
	async ({ node, resultKey } = {}, streamOptions = {}) => {
		let cid;
		let slot;
		let filled = false;
		let done = false;
		let error;
		let consumerDone = false;
		let resolvePulled;
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
					onPulled();
					yield chunk;
					continue;
				}
				if (done) return;
				await new Promise((resolve) => {
					resolveProduced = resolve;
				});
			}
		}

		const addPromise = Promise.resolve(
			node.add(source(), { signal: streamOptions.signal }),
		).then(
			(result) => {
				consumerDone = true;
				onPulled();
				if (
					result === null ||
					typeof result !== "object" ||
					!("cid" in result)
				) {
					throw new Error("node.add did not return { cid }");
				}
				cid = result.cid;
			},
			(e) => {
				error ??= e;
				onPulled();
				throw e;
			},
		);
		addPromise.catch(() => {});

		const stream = createPassThroughStream(
			(chunk) =>
				new Promise((resolve, reject) => {
					if (error) {
						reject(error);
						return;
					}
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

		const teardown = (e) => {
			if (done) return;
			error ??= e ?? new Error("ipfsAddStream destroyed");
			onProduced();
			onPulled();
		};
		const unwire = wireTeardown(stream, teardown, streamOptions);
		addPromise.then(unwire, unwire);

		stream.result = () => ({
			key: resultKey ?? "cid",
			value: cid,
		});
		return stream;
	};
