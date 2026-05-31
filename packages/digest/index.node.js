// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createHash } from "node:crypto";
import { createPassThroughStream } from "@datastream/core";

// Canonical datastream algorithm name -> native node crypto name.
const algorithmMap = {
	"SHA2-256": "SHA256",
	"SHA2-384": "SHA384",
	"SHA2-512": "SHA512",
	"SHA3-256": "SHA3-256",
	"SHA3-384": "SHA3-384",
	"SHA3-512": "SHA3-512",
};

// Native aliases accepted for cross-platform parity, normalized to canonical.
const aliasMap = {
	SHA256: "SHA2-256",
	SHA384: "SHA2-384",
	SHA512: "SHA2-512",
};

const normalizeAlgorithm = (algorithm) => {
	if (algorithmMap[algorithm]) return algorithm;
	if (aliasMap[algorithm]) return aliasMap[algorithm];
	throw new Error(`Unsupported algorithm: ${algorithm}`);
};

export const digestStream = ({ algorithm, resultKey }, streamOptions = {}) => {
	const canonical = normalizeAlgorithm(algorithm);
	const hash = createHash(algorithmMap[canonical]);
	const passThrough = (chunk) => {
		hash.update(chunk);
	};
	let finished = false;
	const flush = () => {
		finished = true;
	};
	const stream = createPassThroughStream(passThrough, flush, streamOptions);
	let checksum;
	stream.result = () => {
		if (!finished) {
			throw new Error(
				"digestStream.result() called before the stream finished",
			);
		}
		checksum ??= hash.digest("hex");
		return {
			key: resultKey ?? "digest",
			value: `${canonical}:${checksum}`,
		};
	};
	return stream;
};

export default digestStream;
