// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";

export const protobufSerializeStream = ({ Type } = {}, streamOptions = {}) => {
	const transform = (chunk, enqueue) => {
		enqueue(Type.encode(Type.create(chunk)).finish());
	};
	return createTransformStream(transform, streamOptions);
};

export const protobufDeserializeStream = (
	{ Type, maxOutputSize } = {},
	streamOptions = {},
) => {
	let outputSize = 0;
	const transform = (chunk, enqueue) => {
		const decoded = Type.decode(chunk);
		if (maxOutputSize != null) {
			const encoded = JSON.stringify(decoded);
			outputSize += encoded?.length ?? 0;
			if (outputSize > maxOutputSize) {
				throw new Error(
					`Decompression output exceeds maxOutputSize (${maxOutputSize} bytes)`,
				);
			}
		}
		enqueue(decoded);
	};
	return createTransformStream(transform, streamOptions);
};

export default {
	serializeStream: protobufSerializeStream,
	deserializeStream: protobufDeserializeStream,
};
