// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { createTransformStream } from "@datastream/core";
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

// NOTE on nonce/IV uniqueness for the AEAD modes (AES-256-GCM,
// CHACHA20-POLY1305): the default IV is a fresh 96-bit CSPRNG value, which is
// safe for a bounded number of messages per key. With random 96-bit nonces the
// birthday bound makes a collision non-negligible after ~2^32 encryptions under
// a single key, and a single GCM nonce reuse is catastrophic (enables forgery
// and authentication-key recovery). Rotate the key well before 2^32 messages,
// or supply a unique deterministic nonce per message. Never reuse an explicit
// iv with the same key.

export const encryptStream = async (
	{ algorithm = "AES-256-GCM", key, iv, aad, maxInputSize, resultKey } = {},
	streamOptions = {},
) => {
	const { ivSize, keySize } = getAlgorithm(algorithm);
	validateKey(key, keySize);
	iv ??= randomBytes(ivSize);
	validateIv(iv, ivSize, algorithm);
	validateAad(aad, algorithm);
	const aead = authAlgorithms.includes(algorithm);
	const stream = createCipheriv(algorithm, key, iv, {
		...streamOptions,
		authTagLength: aead ? 16 : undefined,
	});
	if ((aad ?? null) !== null) {
		stream.setAAD(aad);
	}
	const guard = byteLimit(
		aead
			? bufferedInputLimit(maxInputSize)
			: (maxInputSize ?? Number.POSITIVE_INFINITY),
		encryptInputError,
	);
	const originalTransform = stream._transform.bind(stream);
	stream._transform = (chunk, encoding, callback) => {
		try {
			// Cipher passes string writes through un-decoded: count bytes, not code units.
			guard(Buffer.byteLength(chunk, encoding));
		} catch (error) {
			callback(error);
			return;
		}
		originalTransform(chunk, encoding, callback);
	};
	stream.result = () => ({
		key: resultKey ?? "encrypt",
		value: {
			algorithm,
			iv,
			...(aead ? { authTag: stream.getAuthTag() } : {}),
		},
	});
	return stream;
};

// AEAD decrypt: buffer the whole ciphertext and only release plaintext after
// final() verifies the auth tag. Node's Decipher is a streaming Transform that
// emits decrypted plaintext incrementally and checks the tag only at final();
// using it directly would release UNAUTHENTICATED plaintext to downstream
// consumers before the tag is ever verified.
const aeadDecryptStream = (
	{ algorithm, key, iv, authTag, aad, maxInputSize, maxOutputSize },
	streamOptions,
) => {
	// The decipher here is driven manually via update()/final() and is never
	// exposed as a stream, so forwarding streamOptions to it has no effect. The
	// default auth-tag length is already 16 bytes for every AEAD cipher we use,
	// so no options object is needed.
	const decipher = createDecipheriv(algorithm, key, iv);
	decipher.setAuthTag(authTag);
	if ((aad ?? null) !== null) {
		decipher.setAAD(aad);
	}
	const guard = byteLimit(bufferedInputLimit(maxInputSize), decryptInputError);
	const chunks = [];
	const transform = (chunk) => {
		guard(chunk.length);
		chunks.push(chunk);
	};
	const flush = (enqueue) => {
		const ciphertext = Buffer.concat(chunks);
		// update() may produce plaintext, but we withhold it until final()
		// succeeds; if the tag is wrong, final() throws and nothing is emitted.
		// The AEAD modes are stream ciphers (no padding), so final() only
		// verifies the tag and returns no bytes: update() holds all plaintext.
		const plaintext = decipher.update(ciphertext);
		decipher.final();
		byteLimit(
			maxOutputSize ?? Number.POSITIVE_INFINITY,
			decryptOutputError,
		)(plaintext.length);
		enqueue(plaintext);
	};
	return createTransformStream(transform, flush, streamOptions);
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
		return aeadDecryptStream(
			{ algorithm, key, iv, authTag, aad, maxInputSize, maxOutputSize },
			streamOptions,
		);
	}
	const stream = createDecipheriv(algorithm, key, iv, streamOptions);
	if ((maxOutputSize ?? null) !== null) {
		const guard = byteLimit(maxOutputSize, decryptOutputError);
		const originalPush = stream.push.bind(stream);
		stream.push = (chunk) => {
			// EOF marker (null) carries no bytes and must always pass through.
			if (chunk === null) return originalPush(chunk);
			try {
				guard(chunk.length);
			} catch (error) {
				stream.destroy(error);
				return;
			}
			return originalPush(chunk);
		};
	}
	return stream;
};

export const generateEncryptionKey = ({ bits = 256 } = {}) => {
	if (![128, 256].includes(bits)) {
		throw new Error(`Unsupported key size: ${bits}. Must be 128 or 256.`);
	}
	return randomBytes(bits / 8);
};
