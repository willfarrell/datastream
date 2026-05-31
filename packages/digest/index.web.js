// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createPassThroughStream } from "@datastream/core";
import {
	createSHA3,
	createSHA256,
	createSHA384,
	createSHA512,
} from "hash-wasm";

// Canonical datastream algorithm name -> hash-wasm constructor.
const algorithms = {
	"SHA2-256": createSHA256,
	"SHA2-384": createSHA384,
	"SHA2-512": createSHA512,
	"SHA3-256": () => createSHA3(256),
	"SHA3-384": () => createSHA3(384),
	"SHA3-512": () => createSHA3(512),
};

// Native aliases accepted for cross-platform parity, normalized to canonical.
const aliasMap = {
	SHA256: "SHA2-256",
	SHA384: "SHA2-384",
	SHA512: "SHA2-512",
};

const normalizeAlgorithm = (algorithm) => {
	if (algorithms[algorithm]) return algorithm;
	if (aliasMap[algorithm]) return aliasMap[algorithm];
	throw new Error(`Unsupported algorithm: ${algorithm}`);
};

export const digestStream = ({ algorithm, resultKey }, streamOptions = {}) => {
	const canonical = normalizeAlgorithm(algorithm);
	// hash-wasm constructors are async; kick off WASM init eagerly but resolve it
	// lazily inside passThrough/flush so the factory matches the Node build and
	// returns the stream synchronously (no Promise<stream> for callers to await).
	const hashPromise = algorithms[canonical]();
	let hash;
	const passThrough = async (chunk) => {
		hash ??= await hashPromise;
		hash.update(chunk);
	};
	let finished = false;
	const flush = async () => {
		// Ensure the hash is initialized even when the stream carried no chunks.
		hash ??= await hashPromise;
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
		checksum ??= hash.digest();
		return {
			key: resultKey ?? "digest",
			value: `${canonical}:${checksum}`,
		};
	};
	return stream;
};

export default digestStream;
