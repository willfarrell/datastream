// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Shared by the node and browser builds so both validate identically.

export const DEFAULT_MAX_INPUT_SIZE = 64 * 1024 * 1024; // 64MB

const algorithms = {
	"AES-128-GCM": { ivSize: 12, keySize: 16 },
	"AES-256-GCM": { ivSize: 12, keySize: 32 },
	"AES-128-CTR": { ivSize: 16, keySize: 16 },
	"AES-256-CTR": { ivSize: 16, keySize: 32 },
	"CHACHA20-POLY1305": { ivSize: 12, keySize: 32 },
};

export const authAlgorithms = [
	"AES-128-GCM",
	"AES-256-GCM",
	"CHACHA20-POLY1305",
];

export const getAlgorithm = (algorithm) => {
	// hasOwn: inherited names like "toString" must not resolve to a config.
	if (!Object.hasOwn(algorithms, algorithm)) {
		throw new Error(`Unsupported algorithm: ${algorithm}`);
	}
	return algorithms[algorithm];
};

// Keys must be bytes (Buffer is a Uint8Array). Checking only `.length` let a
// 32-character string through as a raw key.
export const validateKey = (key, keySize) => {
	if (!(key instanceof Uint8Array) || key.byteLength !== keySize) {
		throw new Error(
			`Encryption key must be ${keySize} bytes (${keySize * 8} bits), got ${key?.byteLength ?? 0}`,
		);
	}
};

export const validateIv = (iv, ivSize, algorithm) => {
	if (!(iv instanceof Uint8Array) || iv.byteLength !== ivSize) {
		throw new Error(
			`IV for ${algorithm} must be ${ivSize} bytes, got ${iv?.byteLength ?? 0}`,
		);
	}
};

export const validateAuthTag = (authTag, algorithm) => {
	if (authTag?.byteLength !== 16) {
		throw new Error(
			`authTag for ${algorithm} must be 16 bytes, got ${authTag?.byteLength ?? 0}`,
		);
	}
};

export const validateAad = (aad, algorithm) => {
	if ((aad ?? null) === null) return;
	// Buffer is a Uint8Array; ArrayBuffer is not accepted by either build.
	if (!(aad instanceof Uint8Array)) {
		throw new Error("aad must be a Uint8Array");
	}
	// AAD only has meaning for authenticated modes. Silently dropping it for
	// AES-*-CTR would give the caller a false sense of integrity binding.
	if (!authAlgorithms.includes(algorithm)) {
		throw new Error(
			`aad is not supported for ${algorithm} (not authenticated)`,
		);
	}
};

// undefined → the documented 64MB default; null → unlimited.
export const bufferedInputLimit = (maxInputSize = DEFAULT_MAX_INPUT_SIZE) =>
	maxInputSize ?? Number.POSITIVE_INFINITY;

export const encryptInputError = (max) =>
	new RangeError(
		`Encryption input exceeds maxInputSize (${max} bytes). Raise maxInputSize or encrypt the data as separate smaller messages.`,
	);
export const decryptInputError = (max) =>
	new RangeError(`Decryption input exceeds maxInputSize (${max} bytes)`);
export const decryptOutputError = (max) =>
	new RangeError(`Decryption output exceeds maxOutputSize (${max} bytes)`);

// Running byte counter that throws `error(max)` once the total passes max.
// Plain numbers (exact to 2^53 bytes) so Infinity and fractional limits work;
// BigInt() throws on both.
export const byteLimit = (max, error) => {
	let size = 0;
	return (byteLength) => {
		size += byteLength;
		if (size > max) {
			throw error(max);
		}
	};
};
