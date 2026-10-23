// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { concatBytes, createTransformStream } from "@datastream/core";
import {
	authAlgorithms,
	bufferedInputLimit,
	byteLimit,
	decryptInputError,
	decryptOutputError,
	encryptInputError,
	getAlgorithm,
	validateAad,
	validateAuthTag,
	validateIv,
	validateKey,
} from "./shared.js";

const textEncoder = new TextEncoder();

// After `await ready`, libsodium-wrappers' bound functions live on the
// module's default export, not the namespace object.
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

// 128-bit big-endian add, wrapping modulo 2^128 like node:crypto's CTR.
const incrementCounter = (iv, blockOffset) => {
	let n = 0n;
	for (const byte of iv) n = (n << 8n) | BigInt(byte);
	n += blockOffset;
	const counter = new Uint8Array(16);
	for (let i = 15; i >= 0; i--) {
		counter[i] = Number(n & 0xffn);
		n >>= 8n;
	}
	return counter;
};

const toBytes = (chunk) =>
	chunk instanceof Uint8Array ? chunk : textEncoder.encode(chunk);

const bufferInput = (max, error) => {
	const guard = byteLimit(max, error);
	const chunks = [];
	const transform = (chunk) => {
		const buf = toBytes(chunk);
		guard(buf.byteLength);
		chunks.push(buf);
	};
	return { transform, data: () => concatBytes(chunks) };
};

const withAuthTag = (ciphertext, authTag) => concatBytes([ciphertext, authTag]);

const checkOutputSize = (plaintext, maxOutputSize) => {
	byteLimit(
		maxOutputSize ?? Number.POSITIVE_INFINITY,
		decryptOutputError,
	)(plaintext.byteLength);
};

const aesGcmEncrypt = async ({ key, iv, aad, maxInputSize }) => {
	const input = bufferInput(
		bufferedInputLimit(maxInputSize),
		encryptInputError,
	);
	let authTag;
	const flush = async (enqueue) => {
		const cryptoKey = await crypto.subtle.importKey(
			"raw",
			key,
			"AES-GCM",
			false,
			["encrypt"],
		);
		const encrypted = await crypto.subtle.encrypt(
			{ name: "AES-GCM", iv, additionalData: aad ?? undefined },
			cryptoKey,
			input.data(),
		);
		const result = new Uint8Array(encrypted);
		// Web Crypto appends 16-byte auth tag to ciphertext
		authTag = result.slice(-16);
		enqueue(result.slice(0, -16));
	};
	return { transform: input.transform, flush, authTag: () => authTag };
};

const aesGcmDecrypt = async ({
	key,
	iv,
	authTag,
	aad,
	maxInputSize,
	maxOutputSize,
}) => {
	const input = bufferInput(
		bufferedInputLimit(maxInputSize),
		decryptInputError,
	);
	const flush = async (enqueue) => {
		const cryptoKey = await crypto.subtle.importKey(
			"raw",
			key,
			"AES-GCM",
			false,
			["decrypt"],
		);
		const decrypted = await crypto.subtle.decrypt(
			{ name: "AES-GCM", iv, additionalData: aad ?? undefined },
			cryptoKey,
			withAuthTag(input.data(), authTag),
		);
		const result = new Uint8Array(decrypted);
		checkOutputSize(result, maxOutputSize);
		enqueue(result);
	};
	return { transform: input.transform, flush };
};

const aesCtr = async ({ key, iv }, usage, guard) => {
	const cryptoKey = await crypto.subtle.importKey(
		"raw",
		key,
		"AES-CTR",
		false,
		[usage],
	);
	let blockOffset = 0n;
	let pending = new Uint8Array(0);
	const run = async (data) => {
		// length:128 makes WebCrypto treat the FULL 128-bit block as the counter,
		// matching node's aes-256-ctr and incrementCounter across the counter wrap.
		const counter = incrementCounter(iv, blockOffset);
		const out = await crypto.subtle[usage](
			{ name: "AES-CTR", counter, length: 128 },
			cryptoKey,
			data,
		);
		return new Uint8Array(out);
	};
	const transform = async (chunk, enqueue) => {
		const buf = toBytes(chunk);
		guard(buf.byteLength);
		const all = concatBytes([pending, buf]);
		const whole = all.byteLength - (all.byteLength % 16);
		pending = all.slice(whole);
		enqueue(await run(all.subarray(0, whole)));
		blockOffset += BigInt(whole / 16);
	};
	const flush = async (enqueue) => {
		enqueue(await run(pending));
	};
	return { transform, flush };
};

const aesCtrEncrypt = (options) =>
	aesCtr(
		options,
		"encrypt",
		byteLimit(
			options.maxInputSize ?? Number.POSITIVE_INFINITY,
			encryptInputError,
		),
	);

const aesCtrDecrypt = (options) =>
	aesCtr(
		options,
		"decrypt",
		byteLimit(
			options.maxOutputSize ?? Number.POSITIVE_INFINITY,
			decryptOutputError,
		),
	);

const chacha20Encrypt = async ({ iv, key, aad, maxInputSize }) => {
	const sodium = await loadSodium();
	const input = bufferInput(
		bufferedInputLimit(maxInputSize),
		encryptInputError,
	);
	let authTag;
	const flush = (enqueue) => {
		const encrypted = sodium.crypto_aead_chacha20poly1305_ietf_encrypt(
			input.data(),
			aad ?? null,
			null,
			iv,
			key,
		);
		// Last 16 bytes are the auth tag
		authTag = encrypted.slice(-16);
		enqueue(encrypted.slice(0, -16));
	};
	return { transform: input.transform, flush, authTag: () => authTag };
};

const chacha20Decrypt = async ({
	key,
	iv,
	authTag,
	aad,
	maxInputSize,
	maxOutputSize,
}) => {
	const sodium = await loadSodium();
	const input = bufferInput(
		bufferedInputLimit(maxInputSize),
		decryptInputError,
	);
	const flush = (enqueue) => {
		const decrypted = sodium.crypto_aead_chacha20poly1305_ietf_decrypt(
			null,
			withAuthTag(input.data(), authTag),
			aad ?? null,
			iv,
			key,
		);
		checkOutputSize(decrypted, maxOutputSize);
		enqueue(decrypted);
	};
	return { transform: input.transform, flush };
};

const modes = {
	"AES-128-GCM": { encrypt: aesGcmEncrypt, decrypt: aesGcmDecrypt },
	"AES-256-GCM": { encrypt: aesGcmEncrypt, decrypt: aesGcmDecrypt },
	"AES-128-CTR": { encrypt: aesCtrEncrypt, decrypt: aesCtrDecrypt },
	"AES-256-CTR": { encrypt: aesCtrEncrypt, decrypt: aesCtrDecrypt },
	"CHACHA20-POLY1305": { encrypt: chacha20Encrypt, decrypt: chacha20Decrypt },
};

export const encryptStream = async (
	{ algorithm = "AES-256-GCM", key, iv, aad, maxInputSize, resultKey } = {},
	streamOptions = {},
) => {
	const { ivSize, keySize } = getAlgorithm(algorithm);
	validateKey(key, keySize);
	validateAad(aad, algorithm);
	iv ??= crypto.getRandomValues(new Uint8Array(ivSize));
	validateIv(iv, ivSize, algorithm);
	const { transform, flush, authTag } = await modes[algorithm].encrypt({
		key,
		iv,
		aad,
		maxInputSize,
	});
	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({
		key: resultKey ?? "encrypt",
		value: {
			algorithm,
			iv,
			...(authAlgorithms.includes(algorithm) ? { authTag: authTag() } : {}),
		},
	});
	return stream;
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
	const { ivSize, keySize } = getAlgorithm(algorithm);
	validateKey(key, keySize);
	validateIv(iv, ivSize, algorithm);
	validateAad(aad, algorithm);
	if (authAlgorithms.includes(algorithm)) {
		validateAuthTag(authTag, algorithm);
	}
	const { transform, flush } = await modes[algorithm].decrypt({
		key,
		iv,
		authTag,
		aad,
		maxInputSize,
		maxOutputSize,
	});
	return createTransformStream(transform, flush, streamOptions);
};

export const generateEncryptionKey = ({ bits = 256 } = {}) => {
	if (![128, 256].includes(bits)) {
		throw new Error(`Unsupported key size: ${bits}. Must be 128 or 256.`);
	}
	return crypto.getRandomValues(new Uint8Array(bits / 8));
};
