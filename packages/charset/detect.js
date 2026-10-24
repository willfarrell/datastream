// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { concatBytes, createPassThroughStream } from "@datastream/core";
import { analyse } from "chardet";

const charsetKeys = [
	"UTF-8",
	"UTF-16BE",
	"UTF-16LE",
	"UTF-32BE",
	"UTF-32LE",
	"Shift_JIS",
	"ISO-2022-JP",
	"ISO-2022-CN",
	"ISO-2022-KR",
	"GB18030",
	"EUC-JP",
	"EUC-KR",
	"Big5",
	"ISO-8859-1",
	"ISO-8859-2",
	"ISO-8859-5",
	"ISO-8859-6",
	"ISO-8859-7",
	"ISO-8859-8",
	"windows-1251",
	"windows-1256",
	"windows-1252",
	"windows-1254",
	"windows-1250",
	"KOI8-R",
	"ISO-8859-9",
];

// Cap the detection sample so we never buffer an unbounded amount of the
// stream. chardet analyses a representative prefix; 64KB is plenty.
const MAX_DETECTION_SAMPLE = 64 * 1024;

// chardet reports pure-ASCII input as "ASCII" (often at confidence 100). ASCII
// is a strict subset of UTF-8, so fold an ASCII match into the UTF-8 bucket
// rather than discarding the highest-confidence result and reporting a
// spurious ISO-8859-1 winner.
const normaliseMatchName = (name) => (name === "ASCII" ? "UTF-8" : name);

export const charsetDetectStream = ({ resultKey } = {}, streamOptions = {}) => {
	// Accumulate a bounded byte sample and run chardet once on the whole sample
	// in result(). Running analyse() per-chunk and averaging corrupts results at
	// multibyte chunk boundaries (a split sequence mis-detects each fragment).
	const sample = [];
	let sampleLength = 0;
	const encoder = new TextEncoder();
	const passThrough = (chunk) => {
		const remaining = MAX_DETECTION_SAMPLE - sampleLength;
		if (remaining <= 0) return;
		const bytes = typeof chunk === "string" ? encoder.encode(chunk) : chunk;
		// Uint8Array's slice: Buffer#slice is a view, so the sample would change
		// when a later stage reuses the chunk.
		const slice = Uint8Array.prototype.slice.call(bytes, 0, remaining);
		sample.push(slice);
		sampleLength += slice.length;
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	stream.result = () => {
		// No bytes seen: signal "nothing to detect" rather than a phantom guess so
		// callers can distinguish empty input from a low-confidence real result.
		if (!sampleLength) {
			return {
				key: resultKey ?? "charset",
				value: { charset: undefined, confidence: 0 },
			};
		}
		const charsets = Object.fromEntries(charsetKeys.map((k) => [k, undefined]));
		const matches = analyse(concatBytes(sample));
		for (const match of matches) {
			const name = normaliseMatchName(match.name);
			if (name in charsets) {
				charsets[name] = Math.max(charsets[name] ?? 0, match.confidence);
			}
		}
		const values = Object.entries(charsets)
			.map(([charset, confidence]) => ({
				charset,
				confidence: confidence ?? 0,
			}))
			.sort((a, b) => b.confidence - a.confidence);
		return { key: resultKey ?? "charset", value: values[0] };
	};
	return stream;
};
