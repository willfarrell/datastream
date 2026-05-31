// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { createTransformStream } from "@datastream/core";

const algorithmMap = {
	"AES-128-GCM": { cipher: "aes-128-gcm", ivSize: 12, keySize: 16 },
	"AES-256-GCM": { cipher: "aes-256-gcm", ivSize: 12, keySize: 32 },
	"AES-128-CTR": { cipher: "aes-128-ctr", ivSize: 16, keySize: 16 },
	"AES-256-CTR": { cipher: "aes-256-ctr", ivSize: 16, keySize: 32 },
	"CHACHA20-POLY1305": { cipher: "chacha20-poly1305", ivSize: 12, keySize: 32 },
};

const authAlgorithms = ["AES-128-GCM", "AES-256-GCM", "CHACHA20-POLY1305"];
const ctrAlgorithms = ["AES-128-CTR", "AES-256-CTR"];

const DEFAULT_MAX_INPUT_SIZE = 64 * 1024 * 1024; // 64MB

// NOTE on nonce/IV uniqueness for the AEAD modes (AES-256-GCM,
// CHACHA20-POLY1305): the default IV is a fresh 96-bit CSPRNG value, which is
// safe for a bounded number of messages per key. With random 96-bit nonces the
// birthday bound makes a collision non-negligible after ~2^32 encryptions under
// a single key, and a single GCM nonce reuse is catastrophic (enables forgery
// and authentication-key recovery). Rotate the key well before 2^32 messages,
// or supply a unique deterministic nonce per message. Never reuse an explicit
// iv with the same key.

const validateKey = (key, keySize = 32) => {
	if (!key || key.length !== keySize) {
		throw new Error(
			`Encryption key must be ${keySize} bytes (${keySize * 8} bits), got ${key?.length ?? 0}`,
		);
	}
};

const validateIv = (iv, expectedSize, algorithm) => {
	if (!iv || iv.length !== expectedSize) {
		throw new Error(
			`IV for ${algorithm} must be ${expectedSize} bytes, got ${iv?.length ?? 0}`,
		);
	}
};

const validateAuthTag = (authTag, algorithm) => {
	if (!authTag || authTag.length !== 16) {
		throw new Error(
			`authTag for ${algorithm} must be 16 bytes, got ${authTag?.length ?? 0}`,
		);
	}
};

const validateAad = (aad, algorithm) => {
	if (aad != null && !Buffer.isBuffer(aad) && !(aad instanceof Uint8Array)) {
		throw new Error("aad must be a Buffer or Uint8Array");
	}
	// AAD only has meaning for authenticated modes. Silently dropping it for
	// AES-256-CTR would give the caller a false sense of integrity binding.
	if (aad != null && !authAlgorithms.includes(algorithm)) {
		throw new Error(
			`aad is not supported for ${algorithm} (not authenticated)`,
		);
	}
};

export const encryptStream = (
	{ algorithm = "AES-256-GCM", key, iv, aad, maxInputSize, resultKey } = {},
	streamOptions = {},
) => {
	const config = algorithmMap[algorithm];
	if (!config) {
		throw new Error(`Unsupported algorithm: ${algorithm}`);
	}
	const { cipher: cipherName, ivSize, keySize } = config;
	validateKey(key, keySize);
	iv ??= randomBytes(ivSize);
	validateIv(iv, ivSize, algorithm);
	validateAad(aad, algorithm);
	const authTagLength = authAlgorithms.includes(algorithm) ? 16 : undefined;
	const stream = createCipheriv(cipherName, key, iv, {
		...streamOptions,
		authTagLength,
	});
	if (aad != null && authAlgorithms.includes(algorithm)) {
		stream.setAAD(aad);
	}
	// Input-size guard:
	//  - When maxInputSize is supplied explicitly, enforce it for all algorithms.
	//  - AEAD modes default to 64MB (DoS guard; web build buffers the whole
	//    ciphertext, so parity is important).
	//  - AES-*-CTR with no explicit maxInputSize: OpenSSL silently wraps its
	//    128-bit counter at 2^128 blocks, but that is so far beyond any practical
	//    workload that no runtime guard is needed. Skip the transform override
	//    to avoid dead-code for an unreachable ceiling.
	const skipInputGuard =
		ctrAlgorithms.includes(algorithm) && maxInputSize == null;
	if (!skipInputGuard) {
		const effectiveMaxInput = maxInputSize ?? DEFAULT_MAX_INPUT_SIZE;
		let inputSize = 0n;
		const limit = BigInt(effectiveMaxInput);
		const originalWrite = stream._transform.bind(stream);
		stream._transform = (chunk, encoding, callback) => {
			inputSize += BigInt(chunk.length);
			if (inputSize > limit) {
				callback(
					new Error(
						`Encryption input exceeds maxInputSize (${effectiveMaxInput} bytes). Use AES-256-CTR for large data.`,
					),
				);
				return;
			}
			originalWrite(chunk, encoding, callback);
		};
	}
	stream.result = () => ({
		key: resultKey ?? "encrypt",
		value: {
			algorithm,
			iv,
			...(authAlgorithms.includes(algorithm)
				? { authTag: stream.getAuthTag() }
				: {}),
		},
	});
	return stream;
};

// AEAD decrypt: buffer the whole ciphertext and only release plaintext after
// final() verifies the auth tag. Node's Decipher is a streaming Transform that
// emits decrypted plaintext incrementally and checks the tag only at final();
// using it directly would release UNAUTHENTICATED plaintext to downstream
// consumers before the tag is ever verified. Buffering-then-verifying matches
// the web implementation's authenticated-before-release guarantee.
const aeadDecryptStream = (
	{ cipherName, key, iv, authTag, aad, maxInputSize, maxOutputSize },
	streamOptions,
) => {
	const decipher = createDecipheriv(cipherName, key, iv, {
		...streamOptions,
		authTagLength: 16,
	});
	decipher.setAuthTag(authTag);
	if (aad != null) {
		decipher.setAAD(aad);
	}
	maxInputSize ??= DEFAULT_MAX_INPUT_SIZE;
	const chunks = [];
	let inputSize = 0;
	const transform = (chunk) => {
		// Bound memory before buffering/verification: we must buffer the whole
		// ciphertext to verify the tag before releasing plaintext, so cap input.
		inputSize += chunk.length;
		if (inputSize > maxInputSize) {
			throw new Error(
				`Decryption input exceeds maxInputSize (${maxInputSize} bytes)`,
			);
		}
		chunks.push(chunk);
	};
	const flush = (enqueue) => {
		const ciphertext = Buffer.concat(chunks);
		// update() may produce plaintext, but we withhold it until final()
		// succeeds; if the tag is wrong, final() throws and nothing is emitted.
		const head = decipher.update(ciphertext);
		const tail = decipher.final();
		const plaintext = Buffer.concat([head, tail]);
		if (maxOutputSize != null && plaintext.length > maxOutputSize) {
			throw new Error(
				`Decryption output exceeds maxOutputSize (${maxOutputSize} bytes)`,
			);
		}
		enqueue(plaintext);
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const decryptStream = (
	{
		algorithm = "AES-256-GCM",
		key,
		iv,
		authTag,
		aad,
		maxInputSize,
		maxOutputSize,
	} = {},
	streamOptions = {},
) => {
	const config = algorithmMap[algorithm];
	if (!config) {
		throw new Error(`Unsupported algorithm: ${algorithm}`);
	}
	const { cipher: cipherName, ivSize, keySize } = config;
	validateKey(key, keySize);
	validateIv(iv, ivSize, algorithm);
	validateAad(aad, algorithm);
	if (authAlgorithms.includes(algorithm)) {
		validateAuthTag(authTag, algorithm);
		return aeadDecryptStream(
			{ cipherName, key, iv, authTag, aad, maxInputSize, maxOutputSize },
			streamOptions,
		);
	}
	// AES-256-CTR: unauthenticated, safe to stream incrementally.
	const stream = createDecipheriv(cipherName, key, iv, streamOptions);
	let outputSize = 0;
	const originalPush = stream.push.bind(stream);
	stream.push = (chunk) => {
		if (chunk !== null) {
			if (maxOutputSize != null) {
				outputSize += chunk.length;
				if (outputSize > maxOutputSize) {
					stream.push = originalPush;
					stream.destroy(
						new Error(
							`Decryption output exceeds maxOutputSize (${maxOutputSize} bytes)`,
						),
					);
					return false;
				}
			}
		}
		return originalPush(chunk);
	};
	stream.on("close", () => {
		stream.push = originalPush;
	});
	return stream;
};

export const generateEncryptionKey = ({ bits = 256 } = {}) => {
	if (![128, 256].includes(bits)) {
		throw new Error(`Unsupported key size: ${bits}. Must be 128 or 256.`);
	}
	return randomBytes(bits / 8);
};

export default {
	encryptStream,
	decryptStream,
	generateEncryptionKey,
};
