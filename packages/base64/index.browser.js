// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { concatBytes, createTransformStream } from "@datastream/core";

const INVALID_BASE64_CHAR_RE = /[^A-Za-z0-9+/=]/;
const quote = (s) => JSON.stringify(s.length > 32 ? `${s.slice(0, 32)}…` : s);
const assertValidBase64 = (s) => {
	if (s.length % 4 !== 0 || INVALID_BASE64_CHAR_RE.test(s)) {
		throw new Error(`Invalid base64 string: ${quote(s)}`);
	}
	const pad = s.indexOf("=");
	if (pad !== -1 && s.length - pad > 2) {
		throw new Error(`Invalid base64 string: ${quote(s)}`);
	}
};
const assertNotAfterPadding = (padded, s) => {
	if (padded) {
		throw new Error(`Invalid base64 string: ${quote(s)} follows "=" padding`);
	}
};

const utf8Encoder = new TextEncoder();

const toBytes = (chunk) =>
	typeof chunk === "string" ? utf8Encoder.encode(chunk) : new Uint8Array(chunk);

// Sliced because the engine's stack caps String.fromCharCode's argument count.
const FROM_CHAR_CODE_SLICE = 8192;
const bytesToBinaryString = (bytes) => {
	let s = "";
	let rest = bytes;
	while (rest.length) {
		s += String.fromCharCode.apply(
			null,
			rest.subarray(0, FROM_CHAR_CODE_SLICE),
		);
		rest = rest.subarray(FROM_CHAR_CODE_SLICE);
	}
	return s;
};

// The latin1 label (windows-1252) maps every byte >= 0x80 to a non-ASCII char
// so validation still rejects it, and unlike the utf-8 default it never strips
// a leading BOM (which would silently accept invalid input).
const latin1Decoder = new TextDecoder("latin1");
const bytesToBase64Text = (bytes) => latin1Decoder.decode(bytes);

const binaryStringToBytes = (s) => {
	const out = new Uint8Array(s.length);
	let i = out.length;
	while (i--) out[i] = s.charCodeAt(i);
	return out;
};

export const base64EncodeStream = (_options = {}, streamOptions = {}) => {
	let extra = new Uint8Array(0);
	const transform = (chunk, enqueue) => {
		const all = concatBytes([extra, toBytes(chunk)]);
		const whole = all.length - (all.length % 3);
		extra = all.slice(whole);
		if (whole > 0) enqueue(btoa(bytesToBinaryString(all.subarray(0, whole))));
	};
	const flush = (enqueue) => {
		if (extra.length > 0) enqueue(btoa(bytesToBinaryString(extra)));
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const base64DecodeStream = (_options = {}, streamOptions = {}) => {
	let extra = "";
	let padded = false;
	const transform = (chunk, enqueue) => {
		const all =
			extra +
			(typeof chunk === "string" ? chunk : bytesToBase64Text(toBytes(chunk)));
		const whole = all.length - (all.length % 4);
		extra = all.slice(whole);
		if (whole > 0) {
			const s = all.slice(0, whole);
			assertNotAfterPadding(padded, s);
			assertValidBase64(s);
			padded = s.endsWith("=");
			enqueue(binaryStringToBytes(atob(s)));
		}
	};
	const flush = () => {
		assertValidBase64(extra);
	};
	return createTransformStream(transform, flush, streamOptions);
};
