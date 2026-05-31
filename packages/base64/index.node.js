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

// Valid base64 requires length to be a multiple of 4 and only valid alphabet
// chars, with at most 2 trailing '=' padding characters.  This rejects short
// fragments like "YQ=" (length 3) and standalone padding like "==" (length 2)
// that Buffer.from leniently accepts but atob() in the Web build rejects.
const VALID_BASE64_RE = /^[A-Za-z0-9+/]*={0,2}$/;
const assertValidBase64 = (s) => {
	if (s.length % 4 !== 0 || !VALID_BASE64_RE.test(s)) {
		throw new Error(`Invalid base64 string: ${JSON.stringify(s)}`);
	}
};

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
		if (s.length > 0) {
			assertValidBase64(s);
			enqueue(Buffer.from(s, "base64"));
		}
	};
	const flush = (enqueue) => {
		if (extra.length > 0) {
			assertValidBase64(extra);
			enqueue(Buffer.from(extra, "base64"));
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export default {
	encodeStream: base64EncodeStream,
	decodeStream: base64DecodeStream,
};
