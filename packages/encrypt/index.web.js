// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global crypto */
import { createTransformStream } from "@datastream/core";

const DEFAULT_MAX_INPUT_SIZE = 64 * 1024 * 1024; // 64MB

const expectedIvSize = {
	"AES-256-GCM": 12,
	"AES-256-CTR": 16,
	"CHACHA20-POLY1305": 12,
};

const validateKey = (key) => {
	if (!key || key.byteLength !== 32) {
		throw new Error(
			`Encryption key must be 32 bytes (256 bits), got ${key?.byteLength ?? 0}`,
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

const validateAad = (aad) => {
	if (
		aad != null &&
		!(aad instanceof Uint8Array) &&
		!(aad instanceof ArrayBuffer)
	) {
		throw new Error("aad must be a Uint8Array or ArrayBuffer");
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

// Max safe counter: 2^64 blocks = 2^68 bytes (256 EB).
// Beyond this, keystream reuse breaks confidentiality.
const MAX_CTR_BLOCKS = 2 ** 64;

const incrementCounter = (iv, blockOffset) => {
	if (blockOffset >= MAX_CTR_BLOCKS) {
		throw new Error(
			"AES-CTR counter overflow: data exceeds safe limit. Use a new key/IV pair.",
		);
	}
	const counter = new Uint8Array(iv);
	let carry = blockOffset;
	for (let i = counter.length - 1; i >= 0 && carry > 0; i--) {
		const sum = counter[i] + (carry & 0xff);
		counter[i] = sum & 0xff;
		carry = (carry >> 8) + (sum >> 8);
	}
	return counter;
};

// AES-GCM: buffer all chunks, encrypt on flush
const aesGcmEncrypt = async ({ key, iv, aad, maxInputSize }, streamOptions) => {
	validateKey(key);
	iv ??= crypto.getRandomValues(new Uint8Array(12));
	validateIv(iv, "AES-256-GCM");
	validateAad(aad);
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
			{ name: "AES-GCM", iv, ...(aad ? { additionalData: aad } : {}) },
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
		key: "encrypt",
		value: { algorithm: "AES-256-GCM", iv, authTag },
	});
	return stream;
};

const aesGcmDecrypt = async (
	{ key, iv, authTag, aad, maxOutputSize },
	streamOptions,
) => {
	validateKey(key);
	validateIv(iv, "AES-256-GCM");
	validateAuthTag(authTag, "AES-256-GCM");
	validateAad(aad);
	const chunks = [];
	const transform = (chunk) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
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
			{ name: "AES-GCM", iv, ...(aad ? { additionalData: aad } : {}) },
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
const aesCtrEncrypt = async ({ key, iv }, streamOptions) => {
	validateKey(key);
	iv ??= crypto.getRandomValues(new Uint8Array(16));
	validateIv(iv, "AES-256-CTR");
	const cryptoKey = await crypto.subtle.importKey(
		"raw",
		key,
		"AES-CTR",
		false,
		["encrypt"],
	);
	let blockOffset = 0;
	const transform = async (chunk, enqueue) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		const counter = incrementCounter(iv, blockOffset);
		const encrypted = await crypto.subtle.encrypt(
			{ name: "AES-CTR", counter, length: 64 },
			cryptoKey,
			buf,
		);
		blockOffset += Math.ceil(buf.byteLength / 16);
		enqueue(new Uint8Array(encrypted));
	};
	const stream = createTransformStream(transform, streamOptions);
	stream.result = () => ({
		key: "encrypt",
		value: { algorithm: "AES-256-CTR", iv },
	});
	return stream;
};

const aesCtrDecrypt = async ({ key, iv, maxOutputSize }, streamOptions) => {
	validateKey(key);
	validateIv(iv, "AES-256-CTR");
	const cryptoKey = await crypto.subtle.importKey(
		"raw",
		key,
		"AES-CTR",
		false,
		["decrypt"],
	);
	let blockOffset = 0;
	let outputSize = 0;
	const transform = async (chunk, enqueue) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
		const counter = incrementCounter(iv, blockOffset);
		const decrypted = await crypto.subtle.decrypt(
			{ name: "AES-CTR", counter, length: 64 },
			cryptoKey,
			buf,
		);
		blockOffset += Math.ceil(buf.byteLength / 16);
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
const chacha20Encrypt = async ({ key, iv, aad }, streamOptions) => {
	validateKey(key);
	validateAad(aad);
	let sodium;
	try {
		sodium = await import("libsodium-wrappers");
		await sodium.ready;
	} catch {
		throw new Error(
			"CHACHA20-POLY1305 requires libsodium-wrappers. Install it: npm install libsodium-wrappers",
		);
	}
	iv ??= sodium.randombytes_buf(12);
	validateIv(iv, "CHACHA20-POLY1305");
	const chunks = [];
	let authTag;
	const transform = (chunk) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
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
		key: "encrypt",
		value: { algorithm: "CHACHA20-POLY1305", iv, authTag },
	});
	return stream;
};

const chacha20Decrypt = async (
	{ key, iv, authTag, aad, maxOutputSize },
	streamOptions,
) => {
	validateKey(key);
	validateAuthTag(authTag, "CHACHA20-POLY1305");
	validateAad(aad);
	let sodium;
	try {
		sodium = await import("libsodium-wrappers");
		await sodium.ready;
	} catch {
		throw new Error(
			"CHACHA20-POLY1305 requires libsodium-wrappers. Install it: npm install libsodium-wrappers",
		);
	}
	validateIv(iv, "CHACHA20-POLY1305");
	const chunks = [];
	const transform = (chunk) => {
		const buf =
			chunk instanceof Uint8Array ? chunk : new TextEncoder().encode(chunk);
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
	{ algorithm = "AES-256-GCM", key, iv, aad, maxInputSize } = {},
	streamOptions = {},
) => {
	if (algorithm === "AES-256-GCM") {
		return aesGcmEncrypt({ key, iv, aad, maxInputSize }, streamOptions);
	}
	if (algorithm === "AES-256-CTR") {
		return aesCtrEncrypt({ key, iv }, streamOptions);
	}
	if (algorithm === "CHACHA20-POLY1305") {
		return chacha20Encrypt({ key, iv, aad }, streamOptions);
	}
	throw new Error(`Unsupported algorithm: ${algorithm}`);
};

export const decryptStream = async (
	{ algorithm = "AES-256-GCM", key, iv, authTag, aad, maxOutputSize } = {},
	streamOptions = {},
) => {
	if (algorithm === "AES-256-GCM") {
		return aesGcmDecrypt(
			{ key, iv, authTag, aad, maxOutputSize },
			streamOptions,
		);
	}
	if (algorithm === "AES-256-CTR") {
		return aesCtrDecrypt({ key, iv, maxOutputSize }, streamOptions);
	}
	if (algorithm === "CHACHA20-POLY1305") {
		return chacha20Decrypt(
			{ key, iv, authTag, aad, maxOutputSize },
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
