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
	{ Type } = {},
	streamOptions = {},
) => {
	const transform = (chunk, enqueue) => {
		enqueue(Type.decode(chunk));
	};
	return createTransformStream(transform, streamOptions);
};

export default {
	serializeStream: protobufSerializeStream,
	deserializeStream: protobufDeserializeStream,
};
