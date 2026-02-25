// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createPassThroughStream } from "@datastream/core";

export const ipfsGetStream = async (
	{ node, _repo, cid },
	_streamOptions = {},
) => {
	// node ??= await create({ repo })
	return node.get(cid);
};

export const ipfsAddStream = async (
	{ node, _repo, resultKey } = {},
	streamOptions = {},
) => {
	// node ??= await create({ repo })

	const stream = createPassThroughStream(() => {}, streamOptions);
	const { cid } = node.add(stream);

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
