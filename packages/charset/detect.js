// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createPassThroughStream } from "@datastream/core";
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
	"ISO-8859-8-I",
	"ISO-8859-8",
	"windows-1251",
	"windows-1256",
	"windows-1252",
	"windows-1254",
	"windows-1250",
	"KOIR8-R",
	"ISO-8859-9",
];

export const charsetDetectStream = ({ resultKey } = {}, streamOptions = {}) => {
	const charsets = Object.fromEntries(charsetKeys.map((k) => [k, 0]));
	let chunkCount = 0;
	const passThrough = (chunk) => {
		const matches = analyse(
			typeof chunk === "string" ? Buffer.from(chunk) : chunk,
		);
		chunkCount++;
		if (matches.length) {
			for (const match of matches) {
				if (match.name in charsets) {
					charsets[match.name] += match.confidence;
				}
			}
		}
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	stream.result = () => {
		const divisor = chunkCount || 1;
		const values = Object.entries(charsets)
			.map(([charset, confidence]) => ({
				charset,
				confidence: confidence / divisor,
			}))
			.sort((a, b) => b.confidence - a.confidence);
		return { key: resultKey ?? "charset", value: values[0] };
	};
	return stream;
};

export const getSupportedEncoding = (charset) => {
	if (charset === "ISO-8859-8-I") charset = "ISO-8859-8";
	return charset;
};

export default charsetDetectStream;
