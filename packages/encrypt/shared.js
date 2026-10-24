// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

export const DEFAULT_MAX_INPUT_SIZE = 64 * 1024 * 1024;

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
	if (!Object.hasOwn(algorithms, algorithm)) {
		throw new Error(`Unsupported algorithm: ${algorithm}`);
	}
	return algorithms[algorithm];
};

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
	if (!(aad instanceof Uint8Array)) {
		throw new Error("aad must be a Uint8Array");
	}
	if (!authAlgorithms.includes(algorithm)) {
		throw new Error(
			`aad is not supported for ${algorithm} (not authenticated)`,
		);
	}
};

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

export const byteLimit = (max, error) => {
	let size = 0;
	return (byteLength) => {
		size += byteLength;
		if (size > max) {
			throw error(max);
		}
	};
};
