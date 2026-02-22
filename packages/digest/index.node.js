// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createHash } from "node:crypto";
import { createPassThroughStream } from "@datastream/core";

const algorithmMap = {
	"SHA2-256": "SHA256",
	"SHA2-384": "SHA384",
	"SHA2-512": "SHA512",
};

export const digestStream = ({ algorithm, resultKey }, streamOptions = {}) => {
	const hash = createHash(algorithmMap[algorithm] ?? algorithm);
	const passThrough = (chunk) => {
		hash.update(chunk);
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	let checksum;
	stream.result = () => {
		checksum ??= hash.digest("hex");
		return {
			key: resultKey ?? "digest",
			value: `${algorithm}:${checksum}`,
		};
	};
	return stream;
};

export default digestStream;
