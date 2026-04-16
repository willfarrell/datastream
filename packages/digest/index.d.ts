// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

export type DigestAlgorithm =
	| "SHA2-256"
	| "SHA2-384"
	| "SHA2-512"
	| "SHA3-256"
	| "SHA3-384"
	| "SHA3-512";

type DigestStreamResult = unknown & {
	result: () => StreamResult<string>;
};

export function digestStream(
	options: {
		algorithm: DigestAlgorithm;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DigestStreamResult | Promise<DigestStreamResult>;

export default digestStream;
