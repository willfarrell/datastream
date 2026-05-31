// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global crypto */
import { createTransformStream } from "@datastream/core";

const DEFAULT_MAX_INPUT_SIZE = 64 * 1024 * 1024; // 64MB

const expectedIvSize = {
	"AES-128-GCM": 12,
	"AES-256-GCM": 12,
	"AES-128-CTR": 16,
	"AES-256-CTR": 16,
	"CHACHA20-POLY1305": 12,
};

const expectedKeySize = {
	"AES-128-GCM": 16,
	"AES-256-GCM": 32,
	"AES-128-CTR": 16,
	"AES-256-CTR": 32,
	"CHACHA20-POLY1305": 32,
};

const validateKey = (key, keySize = 32) => {
	if (!key || key.byteLength !== keySize) {
		throw new Error(
			`Encryption key must be ${keySize} bytes (${keySize * 8} bits), got ${key?.byteLength ?? 0}`,
		);
	}
};

const validateIv = (iv, algorithm) => {
	const expected = expectedIvSize[algorithm];
	if (!iv || iv.byteLength !== expected) {
		throw new Error(
			`IV for ${algorithm} must be ${expected} bytes, got ${iv?.byteLength ?? 0}`,
		);
	}
};

const validateAuthTag = (authTag, algorithm) => {
	if (!authTag || authTag.byteLength !== 16) {
		throw new Error(
			`authTag for ${algorithm} must be 16 bytes, got ${authTag?.byteLength ?? 0}`,
		);
	}
};

const authAlgorithms = ["AES-128-GCM", "AES-256-GCM", "CHACHA20-POLY1305"];

const validateAad = (aad, algorithm) => {
	// Match the node implementation's accepted types so identical inputs behave
	// identically on both platforms (Buffer is a Uint8Array; ArrayBuffer is not
	// accepted).
	if (aad != null && !(aad instanceof Uint8Array)) {
		throw new Error("aad must be a Uint8Array");
	}
	// AAD only has meaning for authenticated modes. Silently dropping it for
	// AES-256-CTR would give the caller a false sense of integrity binding.
	if (aad != null && !authAlgorithms.includes(algorithm)) {
		throw new Error(
			`aad is not supported for ${algorithm} (not authenticated)`,
		);
	}
};

const concatBuffers = (chunks) => {
	let totalLength = 0;
	const buffers = chunks.map((chunk) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		totalLength += buf.byteLength;
		return buf;
	});
	const result = new Uint8Array(totalLength);
	let offset = 0;
	for (const buf of buffers) {
		result.set(buf, offset);
		offset += buf.byteLength;
	}
	return result;
};

// libsodium-wrappers is an optional peer dep loaded lazily. After `await
// ready`, the bound functions live on the module's default export, not the
// namespace object, so normalize to that.
const loadSodium = async () => {
	let mod;
	try {
		mod = await import("libsodium-wrappers");
		await mod.ready;
	} catch {
		throw new Error(
			"CHACHA20-POLY1305 requires libsodium-wrappers. Install it: npm install libsodium-wrappers",
		);
	}
	return mod.default ?? mod;
};

// Max safe counter: 2^64 blocks = 2^68 bytes (256 EB).
// Beyond this, keystream reuse breaks confidentiality.
// BigInt so the comparison stays meaningful past 2^53.
const MAX_CTR_BLOCKS = 1n << 64n;

const incrementCounter = (iv, blockOffset) => {
	if (blockOffset >= MAX_CTR_BLOCKS) {
		throw new Error(
			"AES-CTR counter overflow: data exceeds safe limit. Use a new key/IV pair.",
		);
	}
	const counter = new Uint8Array(iv);
	let carry = blockOffset;
	for (let i = counter.length - 1; i >= 0 && carry > 0n; i--) {
		const sum = BigInt(counter[i]) + (carry & 0xffn);
		counter[i] = Number(sum & 0xffn);
		carry = (carry >> 8n) + (sum >> 8n);
	}
	return counter;
};

// AES-GCM: buffer all chunks, encrypt on flush
const aesGcmEncrypt = async (
	{ algorithm = "AES-256-GCM", key, iv, aad, maxInputSize, resultKey },
	streamOptions,
) => {
	validateKey(key, expectedKeySize[algorithm]);
	iv ??= crypto.getRandomValues(new Uint8Array(12));
	validateIv(iv, algorithm);
	validateAad(aad, algorithm);
	maxInputSize ??= DEFAULT_MAX_INPUT_SIZE;
	const chunks = [];
	let inputSize = 0;
	let authTag;
	const transform = (chunk) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		inputSize += buf.byteLength;
		if (inputSize > maxInputSize) {
			throw new Error(
				`Encryption input exceeds maxInputSize (${maxInputSize} bytes). Use AES-256-CTR for large data.`,
			);
		}
		chunks.push(buf);
	};
	const flush = async (enqueue) => {
		const data = concatBuffers(chunks);
		const cryptoKey = await crypto.subtle.importKey(
			"raw",
			key,
			"AES-GCM",
			false,
			["encrypt"],
		);
		const encrypted = await crypto.subtle.encrypt(
			{ name: "AES-GCM", iv, ...(aad != null ? { additionalData: aad } : {}) },
			cryptoKey,
			data,
		);
		const result = new Uint8Array(encrypted);
		// Web Crypto appends 16-byte auth tag to ciphertext
		authTag = result.slice(-16);
		enqueue(result.slice(0, -16));
	};
	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({
		key: resultKey ?? "encrypt",
		value: { algorithm, iv, authTag },
	});
	return stream;
};

const aesGcmDecrypt = async (
	{
		algorithm = "AES-256-GCM",
		key,
		iv,
		authTag,
		aad,
		maxInputSize,
		maxOutputSize,
	},
	streamOptions,
) => {
	validateKey(key, expectedKeySize[algorithm]);
	validateIv(iv, algorithm);
	validateAuthTag(authTag, algorithm);
	validateAad(aad, algorithm);
	maxInputSize ??= DEFAULT_MAX_INPUT_SIZE;
	const chunks = [];
	let inputSize = 0;
	const transform = (chunk) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		// Bound memory before buffering/decryption: maxOutputSize alone is checked
		// only post-decryption, after the full ciphertext is already allocated.
		inputSize += buf.byteLength;
		if (inputSize > maxInputSize) {
			throw new Error(
				`Decryption input exceeds maxInputSize (${maxInputSize} bytes)`,
			);
		}
		chunks.push(buf);
	};
	const flush = async (enqueue) => {
		const ciphertext = concatBuffers(chunks);
		// Re-append auth tag for Web Crypto
		const combined = new Uint8Array(ciphertext.byteLength + authTag.byteLength);
		combined.set(ciphertext);
		combined.set(authTag, ciphertext.byteLength);
		const cryptoKey = await crypto.subtle.importKey(
			"raw",
			key,
			"AES-GCM",
			false,
			["decrypt"],
		);
		const decrypted = await crypto.subtle.decrypt(
			{ name: "AES-GCM", iv, ...(aad != null ? { additionalData: aad } : {}) },
			cryptoKey,
			combined,
		);
		const result = new Uint8Array(decrypted);
		if (maxOutputSize != null && result.byteLength > maxOutputSize) {
			throw new Error(
				`Decryption output exceeds maxOutputSize (${maxOutputSize} bytes)`,
			);
		}
		enqueue(result);
	};
	return createTransformStream(transform, flush, streamOptions);
};

// AES-CTR: true streaming (counter mode is chunk-friendly)
const aesCtrEncrypt = async (
	{ algorithm = "AES-256-CTR", key, iv, aad, maxInputSize, resultKey },
	streamOptions,
) => {
	validateKey(key, expectedKeySize[algorithm]);
	validateAad(aad, algorithm);
	iv ??= crypto.getRandomValues(new Uint8Array(16));
	validateIv(iv, algorithm);
	maxInputSize ??= DEFAULT_MAX_INPUT_SIZE;
	const cryptoKey = await crypto.subtle.importKey(
		"raw",
		key,
		"AES-CTR",
		false,
		["encrypt"],
	);
	let blockOffset = 0n;
	let inputSize = 0;
	const transform = async (chunk, enqueue) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		inputSize += buf.byteLength;
		if (inputSize > maxInputSize) {
			throw new Error(
				`Encryption input exceeds maxInputSize (${maxInputSize} bytes). Use AES-256-CTR for large data.`,
			);
		}
		const counter = incrementCounter(iv, blockOffset);
		// length:128 makes WebCrypto treat the FULL 128-bit block as the counter,
		// matching node's aes-256-ctr and the manual incrementCounter, so ciphertext
		// is chunking-independent and node<->web identical across the counter wrap.
		const encrypted = await crypto.subtle.encrypt(
			{ name: "AES-CTR", counter, length: 128 },
			cryptoKey,
			buf,
		);
		blockOffset += BigInt(Math.ceil(buf.byteLength / 16));
		enqueue(new Uint8Array(encrypted));
	};
	const stream = createTransformStream(transform, streamOptions);
	stream.result = () => ({
		key: resultKey ?? "encrypt",
		value: { algorithm, iv },
	});
	return stream;
};

const aesCtrDecrypt = async (
	{ algorithm = "AES-256-CTR", key, iv, aad, maxOutputSize },
	streamOptions,
) => {
	validateKey(key, expectedKeySize[algorithm]);
	validateAad(aad, algorithm);
	validateIv(iv, algorithm);
	const cryptoKey = await crypto.subtle.importKey(
		"raw",
		key,
		"AES-CTR",
		false,
		["decrypt"],
	);
	let blockOffset = 0n;
	let outputSize = 0;
	const transform = async (chunk, enqueue) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		const counter = incrementCounter(iv, blockOffset);
		// length:128 — see aesCtrEncrypt; the full 128-bit counter must match.
		const decrypted = await crypto.subtle.decrypt(
			{ name: "AES-CTR", counter, length: 128 },
			cryptoKey,
			buf,
		);
		blockOffset += BigInt(Math.ceil(buf.byteLength / 16));
		const result = new Uint8Array(decrypted);
		if (maxOutputSize != null) {
			outputSize += result.byteLength;
			if (outputSize > maxOutputSize) {
				throw new Error(
					`Decryption output exceeds maxOutputSize (${maxOutputSize} bytes)`,
				);
			}
		}
		enqueue(result);
	};
	return createTransformStream(transform, streamOptions);
};

// ChaCha20-Poly1305: requires optional peer dep
const chacha20Encrypt = async (
	{ key, iv, aad, maxInputSize, resultKey },
	streamOptions,
) => {
	validateKey(key);
	validateAad(aad, "CHACHA20-POLY1305");
	const sodium = await loadSodium();
	iv ??= sodium.randombytes_buf(12);
	validateIv(iv, "CHACHA20-POLY1305");
	maxInputSize ??= DEFAULT_MAX_INPUT_SIZE;
	const chunks = [];
	let inputSize = 0;
	let authTag;
	const transform = (chunk) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		inputSize += buf.byteLength;
		if (inputSize > maxInputSize) {
			throw new Error(
				`Encryption input exceeds maxInputSize (${maxInputSize} bytes). Use AES-256-CTR for large data.`,
			);
		}
		chunks.push(buf);
	};
	const flush = (enqueue) => {
		const data = concatBuffers(chunks);
		const encrypted = sodium.crypto_aead_chacha20poly1305_ietf_encrypt(
			data,
			aad ?? null,
			null,
			iv,
			key,
		);
		// Last 16 bytes are the auth tag
		authTag = encrypted.slice(-16);
		enqueue(encrypted.slice(0, -16));
	};
	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({
		key: resultKey ?? "encrypt",
		value: { algorithm: "CHACHA20-POLY1305", iv, authTag },
	});
	return stream;
};

const chacha20Decrypt = async (
	{ key, iv, authTag, aad, maxInputSize, maxOutputSize },
	streamOptions,
) => {
	validateKey(key);
	validateAuthTag(authTag, "CHACHA20-POLY1305");
	validateAad(aad, "CHACHA20-POLY1305");
	const sodium = await loadSodium();
	validateIv(iv, "CHACHA20-POLY1305");
	maxInputSize ??= DEFAULT_MAX_INPUT_SIZE;
	const chunks = [];
	let inputSize = 0;
	const transform = (chunk) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		// Bound memory before buffering/decryption (maxOutputSize is post-decrypt).
		inputSize += buf.byteLength;
		if (inputSize > maxInputSize) {
			throw new Error(
				`Decryption input exceeds maxInputSize (${maxInputSize} bytes)`,
			);
		}
		chunks.push(buf);
	};
	const flush = (enqueue) => {
		const ciphertext = concatBuffers(chunks);
		// Re-append auth tag
		const combined = new Uint8Array(ciphertext.byteLength + authTag.byteLength);
		combined.set(ciphertext);
		combined.set(authTag, ciphertext.byteLength);
		const decrypted = sodium.crypto_aead_chacha20poly1305_ietf_decrypt(
			null,
			combined,
			aad ?? null,
			iv,
			key,
		);
		if (maxOutputSize != null && decrypted.byteLength > maxOutputSize) {
			throw new Error(
				`Decryption output exceeds maxOutputSize (${maxOutputSize} bytes)`,
			);
		}
		enqueue(decrypted);
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const encryptStream = async (
	{ algorithm = "AES-256-GCM", key, iv, aad, maxInputSize, resultKey } = {},
	streamOptions = {},
) => {
	if (algorithm === "AES-256-GCM" || algorithm === "AES-128-GCM") {
		return aesGcmEncrypt(
			{ algorithm, key, iv, aad, maxInputSize, resultKey },
			streamOptions,
		);
	}
	if (algorithm === "AES-256-CTR" || algorithm === "AES-128-CTR") {
		return aesCtrEncrypt(
			{ algorithm, key, iv, aad, maxInputSize, resultKey },
			streamOptions,
		);
	}
	if (algorithm === "CHACHA20-POLY1305") {
		return chacha20Encrypt(
			{ key, iv, aad, maxInputSize, resultKey },
			streamOptions,
		);
	}
	throw new Error(`Unsupported algorithm: ${algorithm}`);
};

export const decryptStream = async (
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
	if (algorithm === "AES-256-GCM" || algorithm === "AES-128-GCM") {
		return aesGcmDecrypt(
			{ algorithm, key, iv, authTag, aad, maxInputSize, maxOutputSize },
			streamOptions,
		);
	}
	if (algorithm === "AES-256-CTR" || algorithm === "AES-128-CTR") {
		return aesCtrDecrypt(
			{ algorithm, key, iv, aad, maxOutputSize },
			streamOptions,
		);
	}
	if (algorithm === "CHACHA20-POLY1305") {
		return chacha20Decrypt(
			{ key, iv, authTag, aad, maxInputSize, maxOutputSize },
			streamOptions,
		);
	}
	throw new Error(`Unsupported algorithm: ${algorithm}`);
};

export const generateEncryptionKey = ({ bits = 256 } = {}) => {
	if (![128, 256].includes(bits)) {
		throw new Error(`Unsupported key size: ${bits}. Must be 128 or 256.`);
	}
	return crypto.getRandomValues(new Uint8Array(bits / 8));
};

export default {
	encryptStream,
	decryptStream,
	generateEncryptionKey,
};
