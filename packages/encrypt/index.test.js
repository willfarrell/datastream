// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, strictEqual, throws } from "node:assert";
import { randomBytes } from "node:crypto";
import test from "node:test";

import { createReadableStream, pipeline } from "@datastream/core";

import encryptDefault, {
	decryptStream,
	encryptStream,
	generateEncryptionKey,
} from "@datastream/encrypt";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

const key = randomBytes(32);

// *** generateEncryptionKey *** //
test(`${variant}: generateEncryptionKey should generate 32-byte key by default`, (_t) => {
	const k = generateEncryptionKey();
	strictEqual(k.byteLength, 32);
});

test(`${variant}: generateEncryptionKey should generate 16-byte key for 128 bits`, (_t) => {
	const k = generateEncryptionKey({ bits: 128 });
	strictEqual(k.byteLength, 16);
});

// *** AES-256-GCM (default) *** //
test(`${variant}: encryptStream should encrypt and decrypt with AES-256-GCM`, async (_t) => {
	const input = "hello, world!";
	const enc = encryptStream({ key });
	const streams = [createReadableStream(input), enc];
	await pipeline(streams);

	const { key: resultKey, value } = enc.result();
	strictEqual(resultKey, "encrypt");
	strictEqual(value.algorithm, "AES-256-GCM");
	strictEqual(value.iv.byteLength, 12);
	strictEqual(value.authTag.byteLength, 16);
});

test(`${variant}: encryptStream roundtrip with AES-256-GCM`, async (_t) => {
	const input = "secret data to encrypt";

	// Encrypt
	const enc = encryptStream({ key });
	const encStreams = [createReadableStream(input), enc];
	const encryptedChunks = [];
	const encStream = encStreams[0].pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;

	// Decrypt
	const dec = decryptStream({ key, iv, authTag });
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	const decryptedChunks = [];
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
	strictEqual(decrypted, input);
});

test(`${variant}: encryptStream should use custom IV`, async (_t) => {
	const iv = randomBytes(12);
	const enc = encryptStream({ key, iv });
	const streams = [createReadableStream("test"), enc];
	await pipeline(streams);

	const { value } = enc.result();
	deepStrictEqual(value.iv, iv);
});

test(`${variant}: encryptStream should support AAD`, async (_t) => {
	const aad = Buffer.from("metadata");

	// Encrypt with AAD
	const enc = encryptStream({ key, aad });
	const encStreams = [createReadableStream("aad test"), enc];
	const encryptedChunks = [];
	const encStream = encStreams[0].pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;

	// Decrypt with matching AAD
	const dec = decryptStream({ key, iv, authTag, aad });
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	const decryptedChunks = [];
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
	strictEqual(decrypted, "aad test");
});

test(`${variant}: encryptStream should collect result via pipeline`, async (_t) => {
	const enc = encryptStream({ key });
	const streams = [createReadableStream("pipeline test"), enc];
	const result = await pipeline(streams);

	strictEqual(typeof result.encrypt, "object");
	strictEqual(result.encrypt.algorithm, "AES-256-GCM");
});

// *** AES-256-CTR *** //
test(`${variant}: encryptStream roundtrip with AES-256-CTR`, async (_t) => {
	const input = "ctr mode test data";

	const enc = encryptStream({ key, algorithm: "AES-256-CTR" });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv } = enc.result().value;
	strictEqual(iv.byteLength, 16);
	strictEqual(enc.result().value.authTag, undefined);

	const dec = decryptStream({ key, iv, algorithm: "AES-256-CTR" });
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	const decryptedChunks = [];
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
	strictEqual(decrypted, input);
});

// *** CHACHA20-POLY1305 *** //
test(`${variant}: encryptStream roundtrip with CHACHA20-POLY1305`, async (_t) => {
	const input = "chacha20 test data";

	const enc = encryptStream({ key, algorithm: "CHACHA20-POLY1305" });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;
	strictEqual(iv.byteLength, 12);
	strictEqual(authTag.byteLength, 16);

	const dec = decryptStream({
		key,
		iv,
		authTag,
		algorithm: "CHACHA20-POLY1305",
	});
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	const decryptedChunks = [];
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
	strictEqual(decrypted, input);
});

// *** Error cases *** //
test(`${variant}: encryptStream should throw for unsupported algorithm`, (_t) => {
	throws(
		() => encryptStream({ key, algorithm: "INVALID" }),
		/Unsupported algorithm/,
	);
});

test(`${variant}: decryptStream should throw for unsupported algorithm`, (_t) => {
	throws(
		() => decryptStream({ key, iv: randomBytes(12), algorithm: "INVALID" }),
		/Unsupported algorithm/,
	);
});

test(`${variant}: decryptStream should fail with wrong key`, async (_t) => {
	const enc = encryptStream({ key });
	const encryptedChunks = [];
	const encStream = createReadableStream("wrong key test").pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;

	const wrongKey = randomBytes(32);
	const dec = decryptStream({ key: wrongKey, iv, authTag });
	try {
		const decStream = createReadableStream(encryptedChunks).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail
		}
		throw new Error("Expected decryption to fail");
	} catch (e) {
		strictEqual(e.message !== "Expected decryption to fail", true);
	}
});

test(`${variant}: decryptStream maxOutputSize should limit output`, async (_t) => {
	const input = "a".repeat(1000);
	const enc = encryptStream({ key, algorithm: "AES-256-CTR" });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv } = enc.result().value;

	const dec = decryptStream({
		key,
		iv,
		algorithm: "AES-256-CTR",
		maxOutputSize: 100,
	});
	try {
		const decStream = createReadableStream(encryptedChunks).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail
		}
		throw new Error("Expected maxOutputSize error");
	} catch (e) {
		strictEqual(e.message.includes("maxOutputSize"), true);
	}
});

// *** Chunked input *** //
test(`${variant}: encryptStream should handle chunked input`, async (_t) => {
	const chunks = ["hello, ", "world", "!"];

	const enc = encryptStream({ key });
	const encryptedChunks = [];
	const encStream = createReadableStream(chunks).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;

	const dec = decryptStream({ key, iv, authTag });
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	const decryptedChunks = [];
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
	strictEqual(decrypted, "hello, world!");
});

// *** Empty input *** //
test(`${variant}: encryptStream should handle empty input`, async (_t) => {
	const enc = encryptStream({ key });
	const encryptedChunks = [];
	const encStream = createReadableStream("").pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;

	const dec = decryptStream({ key, iv, authTag });
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	const decryptedChunks = [];
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
	strictEqual(decrypted, "");
});

// *** maxInputSize *** //
if (variant === "webstream") {
	test(`${variant}: encryptStream AES-256-GCM should enforce maxInputSize`, async (_t) => {
		const input = "a".repeat(200);
		const enc = await encryptStream({ key, maxInputSize: 100 });
		try {
			const encStream = createReadableStream(input).pipeThrough(enc);
			for await (const _chunk of encStream.readable) {
				// should fail
			}
			throw new Error("Expected maxInputSize error");
		} catch (e) {
			strictEqual(e.message.includes("maxInputSize"), true);
		}
	});

	test(`${variant}: encryptStream CHACHA20-POLY1305 should enforce maxInputSize`, async (_t) => {
		const input = "a".repeat(200);
		const enc = await encryptStream({
			key,
			algorithm: "CHACHA20-POLY1305",
			maxInputSize: 100,
		});
		try {
			const encStream = createReadableStream(input).pipeThrough(enc);
			for await (const _chunk of encStream.readable) {
				// should fail
			}
			throw new Error("Expected maxInputSize error");
		} catch (e) {
			strictEqual(e.message.includes("maxInputSize"), true);
		}
	});
}

// *** generateEncryptionKey error cases *** //
test(`${variant}: generateEncryptionKey should throw for unsupported bits`, (_t) => {
	throws(() => generateEncryptionKey({ bits: 512 }), /Unsupported key size/);
});

// *** default export *** //
test(`${variant}: default export should include all functions`, (_t) => {
	deepStrictEqual(Object.keys(encryptDefault).sort(), [
		"decryptStream",
		"encryptStream",
		"generateEncryptionKey",
	]);
});
