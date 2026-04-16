// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

const algorithmMap = {
	"AES-256-GCM": { cipher: "aes-256-gcm", ivSize: 12 },
	"AES-256-CTR": { cipher: "aes-256-ctr", ivSize: 16 },
	"CHACHA20-POLY1305": { cipher: "chacha20-poly1305", ivSize: 12 },
};

const authAlgorithms = ["AES-256-GCM", "CHACHA20-POLY1305"];

const validateKey = (key) => {
	if (!key || key.length !== 32) {
		throw new Error(
			`Encryption key must be 32 bytes (256 bits), got ${key?.length ?? 0}`,
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

const validateAad = (aad) => {
	if (aad != null && !Buffer.isBuffer(aad) && !(aad instanceof Uint8Array)) {
		throw new Error("aad must be a Buffer or Uint8Array");
	}
};

export const encryptStream = (
	{ algorithm = "AES-256-GCM", key, iv, aad, maxInputSize } = {},
	streamOptions = {},
) => {
	const config = algorithmMap[algorithm];
	if (!config) {
		throw new Error(`Unsupported algorithm: ${algorithm}`);
	}
	const { cipher: cipherName, ivSize } = config;
	validateKey(key);
	iv ??= randomBytes(ivSize);
	validateIv(iv, ivSize, algorithm);
	validateAad(aad);
	const authTagLength = authAlgorithms.includes(algorithm) ? 16 : undefined;
	const stream = createCipheriv(cipherName, key, iv, {
		...streamOptions,
		authTagLength,
	});
	if (aad && authAlgorithms.includes(algorithm)) {
		stream.setAAD(aad);
	}
	if (maxInputSize !== null && maxInputSize !== undefined) {
		let inputSize = 0;
		const originalWrite = stream._transform.bind(stream);
		stream._transform = (chunk, encoding, callback) => {
			inputSize += chunk.length;
			if (inputSize > maxInputSize) {
				callback(
					new Error(
						`Encryption input exceeds maxInputSize (${maxInputSize} bytes). Use AES-256-CTR for large data.`,
					),
				);
				return;
			}
			originalWrite(chunk, encoding, callback);
		};
	}
	stream.result = () => ({
		key: "encrypt",
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

export const decryptStream = (
	{ algorithm = "AES-256-GCM", key, iv, authTag, aad, maxOutputSize } = {},
	streamOptions = {},
) => {
	const config = algorithmMap[algorithm];
	if (!config) {
		throw new Error(`Unsupported algorithm: ${algorithm}`);
	}
	const { cipher: cipherName, ivSize } = config;
	validateKey(key);
	validateIv(iv, ivSize, algorithm);
	validateAad(aad);
	const authTagLength = authAlgorithms.includes(algorithm) ? 16 : undefined;
	if (authAlgorithms.includes(algorithm)) {
		validateAuthTag(authTag, algorithm);
	}
	const stream = createDecipheriv(cipherName, key, iv, {
		...streamOptions,
		authTagLength,
	});
	if (authTag) {
		stream.setAuthTag(authTag);
	}
	if (aad && authAlgorithms.includes(algorithm)) {
		stream.setAAD(aad);
	}
	if (maxOutputSize != null) {
		let outputSize = 0;
		const originalPush = stream.push.bind(stream);
		stream.push = (chunk) => {
			if (chunk !== null) {
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
			return originalPush(chunk);
		};
		stream.on("close", () => {
			stream.push = originalPush;
		});
	}
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
