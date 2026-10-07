// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global btoa, atob */
import { concatBytes, createTransformStream } from "@datastream/core";

// Valid base64 requires length to be a multiple of 4 and only valid alphabet
// chars, with at most 2 trailing '=' padding characters.  This rejects short
// fragments like "YQ=" (length 3) and standalone padding like "==" (length 2)
// so that the Web build (atob) and the Node build (Buffer.from) behave
// identically on malformed input.
const VALID_BASE64_RE = /^[A-Za-z0-9+/]*={0,2}$/;
// Quote at most 32 chars of the offending input: a multi-megabyte bad chunk
// must not be copied into the error message (and on into logs).
const quote = (s) => JSON.stringify(s.length > 32 ? `${s.slice(0, 32)}…` : s);
const assertValidBase64 = (s) => {
	if (s.length % 4 !== 0 || !VALID_BASE64_RE.test(s)) {
		throw new Error(`Invalid base64 string: ${quote(s)}`);
	}
};
// A padded quartet split from the data after it (["YQ==", "YWJj"]) must fail
// like the same text in one chunk ("YQ==YWJj") does.
const assertNotAfterPadding = (padded, s) => {
	if (padded) {
		throw new Error(`Invalid base64 string: ${quote(s)} follows "=" padding`);
	}
};

const utf8Encoder = new TextEncoder();

// Node parity: string chunks are UTF-8 (Buffer.from(string)), never latin1.
// Everything else (typed array, ArrayBuffer, byte array) is copied into a
// fresh Uint8Array; concatBytes() copies anyway, so skipping it here buys
// nothing.
const toBytes = (chunk) =>
	typeof chunk === "string" ? utf8Encoder.encode(chunk) : new Uint8Array(chunk);

const bytesToBinaryString = (bytes) => {
	let s = "";
	for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
	return s;
};

// atob() output is a binary string: every char code is already a byte.
const binaryStringToBytes = (s) => Uint8Array.from(s, (c) => c.charCodeAt(0));

export const base64EncodeStream = (_options = {}, streamOptions = {}) => {
	// Bytes held back until a whole 3-byte group can be encoded.
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
	// Characters held back until a whole 4-char quartet can be decoded.
	let extra = "";
	// "=" ends a base64 stream; set once a quartet with padding is decoded.
	let padded = false;
	const transform = (chunk, enqueue) => {
		const all =
			extra +
			(typeof chunk === "string" ? chunk : bytesToBinaryString(toBytes(chunk)));
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
		// Any leftover characters form an incomplete quartet (length 1-3) and can
		// never be a valid base64 group, so reject rather than silently drop them.
		// assertValidBase64("") is a no-op, so no length guard is needed here.
		assertValidBase64(extra);
	};
	return createTransformStream(transform, flush, streamOptions);
};
