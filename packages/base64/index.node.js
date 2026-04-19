// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { Buffer } from "node:buffer";
import { createTransformStream } from "@datastream/core";

const toBuffer = (chunk) =>
	Buffer.isBuffer(chunk)
		? chunk
		: chunk instanceof Uint8Array
			? Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)
			: Buffer.from(chunk);

export const base64EncodeStream = (_options = {}, streamOptions = {}) => {
	let extra; // Buffer | undefined
	const transform = (chunk, enqueue) => {
		let buf = toBuffer(chunk);
		if (extra) {
			buf = Buffer.concat([extra, buf]);
			extra = undefined;
		}
		const remaining = buf.length % 3;
		if (remaining > 0) {
			extra = Buffer.from(buf.subarray(buf.length - remaining));
			buf = buf.subarray(0, buf.length - remaining);
		}
		if (buf.length > 0) enqueue(buf.toString("base64"));
	};
	const flush = (enqueue) => {
		if (extra && extra.length > 0) {
			enqueue(extra.toString("base64"));
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const base64DecodeStream = (_options = {}, streamOptions = {}) => {
	let extra = "";
	const transform = (chunk, enqueue) => {
		const str =
			typeof chunk === "string" ? chunk : toBuffer(chunk).toString("ascii");
		let s = extra.length > 0 ? extra + str : str;
		extra = "";
		const remaining = s.length % 4;
		if (remaining > 0) {
			extra = s.slice(s.length - remaining);
			s = s.slice(0, s.length - remaining);
		}
		if (s.length > 0) enqueue(Buffer.from(s, "base64"));
	};
	const flush = (enqueue) => {
		if (extra.length > 0) {
			enqueue(Buffer.from(extra, "base64"));
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export default {
	encodeStream: base64EncodeStream,
	decodeStream: base64DecodeStream,
};
