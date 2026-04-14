// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createPassThroughStream } from "@datastream/core";

export const ipfsGetStream = async ({ node, cid }, _streamOptions = {}) => {
	return node.get(cid);
};

export const ipfsAddStream = async (
	{ node, resultKey } = {},
	streamOptions = {},
) => {
	const chunks = [];
	let cid;
	const stream = createPassThroughStream(
		(chunk) => {
			chunks.push(chunk);
		},
		async () => {
			const result = await node.add(chunks);
			cid = result.cid;
		},
		streamOptions,
	);

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
