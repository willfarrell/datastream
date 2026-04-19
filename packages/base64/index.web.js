// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global btoa, atob */
import { createTransformStream } from "@datastream/core";

const utf8Encoder = new TextEncoder();

const toBytes = (chunk) => {
	if (chunk instanceof Uint8Array) return chunk;
	if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
	if (typeof chunk === "string") return utf8Encoder.encode(chunk);
	return new Uint8Array(chunk);
};

const concat = (a, b) => {
	const out = new Uint8Array(a.length + b.length);
	out.set(a, 0);
	out.set(b, a.length);
	return out;
};

const bytesToBinaryString = (bytes) => {
	let s = "";
	for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
	return s;
};

const binaryStringToBytes = (s) => {
	const out = new Uint8Array(s.length);
	for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i) & 0xff;
	return out;
};

export const base64EncodeStream = (_options = {}, streamOptions = {}) => {
	let extra; // Uint8Array | undefined
	const transform = (chunk, enqueue) => {
		let bytes = toBytes(chunk);
		if (extra) {
			bytes = concat(extra, bytes);
			extra = undefined;
		}
		const remaining = bytes.length % 3;
		if (remaining > 0) {
			extra = bytes.slice(bytes.length - remaining);
			bytes = bytes.subarray(0, bytes.length - remaining);
		}
		if (bytes.length > 0) enqueue(btoa(bytesToBinaryString(bytes)));
	};
	const flush = (enqueue) => {
		if (extra && extra.length > 0) {
			enqueue(btoa(bytesToBinaryString(extra)));
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const base64DecodeStream = (_options = {}, streamOptions = {}) => {
	let extra = "";
	const transform = (chunk, enqueue) => {
		const str =
			typeof chunk === "string" ? chunk : bytesToBinaryString(toBytes(chunk));
		let s = extra.length > 0 ? extra + str : str;
		extra = "";
		const remaining = s.length % 4;
		if (remaining > 0) {
			extra = s.slice(s.length - remaining);
			s = s.slice(0, s.length - remaining);
		}
		if (s.length > 0) enqueue(binaryStringToBytes(atob(s)));
	};
	const flush = (enqueue) => {
		if (extra.length > 0) {
			enqueue(binaryStringToBytes(atob(extra)));
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export default {
	encodeStream: base64EncodeStream,
	decodeStream: base64DecodeStream,
};
