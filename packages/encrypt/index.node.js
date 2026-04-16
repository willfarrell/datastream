// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

const algorithmMap = {
	"AES-256-GCM": { cipher: "aes-256-gcm", ivSize: 12 },
	"AES-256-CTR": { cipher: "aes-256-ctr", ivSize: 16 },
	"CHACHA20-POLY1305": { cipher: "chacha20-poly1305", ivSize: 12 },
};

const authAlgorithms = ["AES-256-GCM", "CHACHA20-POLY1305"];

export const encryptStream = (
	{ algorithm = "AES-256-GCM", key, iv, aad } = {},
	streamOptions = {},
) => {
	const config = algorithmMap[algorithm];
	if (!config) {
		throw new Error(`Unsupported algorithm: ${algorithm}`);
	}
	const { cipher: cipherName, ivSize } = config;
	iv ??= randomBytes(ivSize);
	const authTagLength = authAlgorithms.includes(algorithm) ? 16 : undefined;
	const stream = createCipheriv(cipherName, key, iv, {
		...streamOptions,
		authTagLength,
	});
	if (aad && authAlgorithms.includes(algorithm)) {
		stream.setAAD(aad);
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
	const { cipher: cipherName } = config;
	const authTagLength = authAlgorithms.includes(algorithm) ? 16 : undefined;
	const stream = createDecipheriv(cipherName, key, iv, {
		...streamOptions,
		authTagLength,
	});
	if (authTag && authAlgorithms.includes(algorithm)) {
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
	}
	return stream;
};

export const generateEncryptionKey = ({ bits = 256 } = {}) => {
	return randomBytes(bits / 8);
};

export default {
	encryptStream,
	decryptStream,
	generateEncryptionKey,
};
