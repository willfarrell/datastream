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

export const base64EncodeStream = (_options = {}, streamOptions = {}) => {
	let extra; // Buffer | undefined
	const transform = (chunk, enqueue) => {
		let buf = toBuffer(chunk);
		if (extra) {
			buf = Buffer.concat([extra, buf]);
			extra = undefined;
		}
		const remaining = buf.length % 3;
		const whole = buf.length - remaining;
		if (remaining > 0) {
			extra = Buffer.from(buf.subarray(whole));
		}
		if (whole > 0) enqueue(buf.subarray(0, whole).toString("base64"));
	};
	const flush = (enqueue) => {
		if (extra) {
			enqueue(extra.toString("base64"));
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const base64DecodeStream = (_options = {}, streamOptions = {}) => {
	let extra = "";
	// "=" ends a base64 stream; set once a quartet with padding is decoded.
	let padded = false;
	const transform = (chunk, enqueue) => {
		// Base64's alphabet is pure ASCII, so a string chunk and the byte view of
		// a Buffer/Uint8Array chunk are interchangeable. Normalising every chunk
		// through toBuffer().toString("latin1") keeps a single code path; latin1
		// (not "ascii", which strips the high bit: 0xD9 -> "Y") keeps non-ASCII
		// bytes non-ASCII so validation rejects them.
		const str = toBuffer(chunk).toString("latin1");
		const s0 = extra + str;
		const remaining = s0.length % 4;
		const whole = s0.length - remaining;
		extra = s0.slice(whole);
		const s = s0.slice(0, whole);
		if (s.length > 0) {
			assertNotAfterPadding(padded, s);
			assertValidBase64(s);
			padded = s.endsWith("=");
			enqueue(Buffer.from(s, "base64"));
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
