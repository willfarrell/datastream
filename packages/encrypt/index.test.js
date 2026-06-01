// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, rejects, strictEqual, throws } from "node:assert";
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

// *** AEAD: no unauthenticated plaintext released on tamper *** //
const tamperedAeadEmitsNothing = async (algorithm) => {
	// Large enough that a streaming Decipher would emit plaintext mid-stream
	// before final() ever checks the auth tag.
	const input = "a".repeat(64 * 1024);
	const enc = await encryptStream({ key, algorithm });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;

	// Tamper: flip the last byte of the ciphertext.
	const ciphertext = Buffer.concat(encryptedChunks.map((c) => Buffer.from(c)));
	ciphertext[ciphertext.length - 1] ^= 0xff;
	// Re-chunk so a streaming decipher would have multiple transform calls.
	const tamperedChunks = [];
	for (let i = 0; i < ciphertext.length; i += 4096) {
		tamperedChunks.push(ciphertext.subarray(i, i + 4096));
	}

	const dec = await decryptStream({ key, iv, authTag, algorithm });
	const emitted = [];
	let threw = false;
	try {
		const decStream = createReadableStream(tamperedChunks).pipe(dec);
		for await (const chunk of decStream) {
			emitted.push(chunk);
		}
	} catch (_e) {
		threw = true;
	}
	// Must error AND must NOT have released any plaintext before verification.
	strictEqual(threw, true);
	strictEqual(
		emitted.reduce((n, c) => n + c.length, 0),
		0,
	);
};

test(`${variant}: AES-256-GCM decrypt releases NO plaintext on tampered ciphertext`, async (_t) => {
	await tamperedAeadEmitsNothing("AES-256-GCM");
});

test(`${variant}: CHACHA20-POLY1305 decrypt releases NO plaintext on tampered ciphertext`, async (_t) => {
	await tamperedAeadEmitsNothing("CHACHA20-POLY1305");
});

test(`${variant}: AES-256-GCM decrypt enforces maxInputSize`, async (_t) => {
	const input = "a".repeat(1000);
	const enc = await encryptStream({ key });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;
	const ciphertext = Buffer.concat(encryptedChunks.map((c) => Buffer.from(c)));
	const chunked = [];
	for (let i = 0; i < ciphertext.length; i += 100) {
		chunked.push(ciphertext.subarray(i, i + 100));
	}
	const dec = await decryptStream({ key, iv, authTag, maxInputSize: 100 });
	let threw = false;
	let message = "";
	try {
		const decStream = createReadableStream(chunked).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxInputSize"), true);
});

test(`${variant}: AES-256-GCM decrypt enforces maxOutputSize`, async (_t) => {
	const input = "a".repeat(1000);
	const enc = await encryptStream({ key });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;
	const dec = await decryptStream({ key, iv, authTag, maxOutputSize: 100 });
	let threw = false;
	let message = "";
	try {
		const decStream = createReadableStream(encryptedChunks).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxOutputSize"), true);
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
test(`${variant}: encryptStream AES-256-GCM should enforce maxInputSize`, async (_t) => {
	const input = "a".repeat(200);
	const enc = await encryptStream({ key, maxInputSize: 100 });
	try {
		const streams = [createReadableStream(input), enc];
		await pipeline(streams);
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
		const streams = [createReadableStream(input), enc];
		await pipeline(streams);
		throw new Error("Expected maxInputSize error");
	} catch (e) {
		strictEqual(e.message.includes("maxInputSize"), true);
	}
});

// *** generateEncryptionKey error cases *** //
test(`${variant}: generateEncryptionKey should throw for unsupported bits`, (_t) => {
	throws(() => generateEncryptionKey({ bits: 512 }), /Unsupported key size/);
});

// *** validation error cases *** //
test(`${variant}: encryptStream should throw for invalid key length`, (_t) => {
	throws(() => encryptStream({ key: randomBytes(16) }), /32 bytes/);
	throws(() => encryptStream({}), /32 bytes/);
});

test(`${variant}: encryptStream should throw for invalid IV length`, (_t) => {
	throws(
		() => encryptStream({ key, iv: randomBytes(8) }),
		/IV for AES-256-GCM/,
	);
});

test(`${variant}: encryptStream should throw for invalid aad`, (_t) => {
	throws(() => encryptStream({ key, aad: "not-a-buffer" }), /aad must be/);
});

test(`${variant}: decryptStream should throw for invalid key length`, (_t) => {
	throws(
		() => decryptStream({ key: randomBytes(16), iv: randomBytes(12) }),
		/32 bytes/,
	);
});

test(`${variant}: decryptStream should throw for invalid IV length`, (_t) => {
	throws(
		() => decryptStream({ key, iv: randomBytes(8) }),
		/IV for AES-256-GCM/,
	);
});

test(`${variant}: decryptStream should report 0 for missing IV`, (_t) => {
	throws(() => decryptStream({ key, authTag: randomBytes(16) }), /got 0/);
});

test(`${variant}: decryptStream should report 0 for missing authTag`, (_t) => {
	throws(() => decryptStream({ key, iv: randomBytes(12) }), /got 0/);
});

test(`${variant}: decryptStream should throw for missing authTag on GCM`, (_t) => {
	throws(
		() => decryptStream({ key, iv: randomBytes(12) }),
		/authTag for AES-256-GCM/,
	);
});

// *** within-limit paths *** //
test(`${variant}: encryptStream maxInputSize within limit should succeed`, async (_t) => {
	const input = "a".repeat(50);
	const enc = encryptStream({ key, maxInputSize: 100 });
	const streams = [createReadableStream(input), enc];
	await pipeline(streams);
	strictEqual(enc.result().value.algorithm, "AES-256-GCM");
});

test(`${variant}: decryptStream maxOutputSize within limit should succeed`, async (_t) => {
	const input = "small";
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
		maxOutputSize: 1000,
	});
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	const decryptedChunks = [];
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	strictEqual(Buffer.concat(decryptedChunks).toString("utf8"), input);
});

// *** default export *** //
test(`${variant}: default export should include all functions`, (_t) => {
	deepStrictEqual(Object.keys(encryptDefault).sort(), [
		"decryptStream",
		"encryptStream",
		"generateEncryptionKey",
	]);
});

// *** Web source exercised directly *** //
// Node's export resolution always picks index.node.mjs (the "node" condition
// matches first under every --conditions value), so the web implementation can
// only be exercised via a direct file import. crypto.subtle, CompressionStream,
// TextEncoder, and the WHATWG stream globals are all available in Node 24.
const importWeb = () =>
	import(`file://${new URL("./index.web.js", import.meta.url).pathname}`);

const webRoundtrip = async (algorithm, aad) => {
	const web = await importWeb();
	const input = "the web implementation must round-trip correctly";
	const enc = await web.encryptStream({ key, algorithm, aad });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;
	const dec = await web.decryptStream({ key, iv, authTag, algorithm, aad });
	const decryptedChunks = [];
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	strictEqual(Buffer.concat(decryptedChunks).toString("utf8"), input);
};

test(`${variant}: web roundtrip AES-256-GCM`, async (_t) => {
	await webRoundtrip("AES-256-GCM");
});

test(`${variant}: web roundtrip AES-256-GCM with AAD`, async (_t) => {
	await webRoundtrip("AES-256-GCM", Buffer.from("metadata"));
});

test(`${variant}: web roundtrip AES-256-CTR`, async (_t) => {
	await webRoundtrip("AES-256-CTR");
});

test(`${variant}: web roundtrip CHACHA20-POLY1305`, async (_t) => {
	await webRoundtrip("CHACHA20-POLY1305");
});

test(`${variant}: web generateEncryptionKey default and 128-bit`, async (_t) => {
	const web = await importWeb();
	strictEqual(web.generateEncryptionKey().byteLength, 32);
	strictEqual(web.generateEncryptionKey({ bits: 128 }).byteLength, 16);
});

test(`${variant}: web generateEncryptionKey throws for unsupported bits`, async (_t) => {
	const web = await importWeb();
	throws(
		() => web.generateEncryptionKey({ bits: 512 }),
		/Unsupported key size/,
	);
});

test(`${variant}: web default export includes all functions`, async (_t) => {
	const web = await importWeb();
	deepStrictEqual(Object.keys(web.default).sort(), [
		"decryptStream",
		"encryptStream",
		"generateEncryptionKey",
	]);
});

test(`${variant}: web encryptStream throws for invalid key`, async (_t) => {
	const web = await importWeb();
	await rejects(() => web.encryptStream({ key: randomBytes(16) }), /32 bytes/);
});

test(`${variant}: web encryptStream reports 0 for missing key`, async (_t) => {
	const web = await importWeb();
	await rejects(() => web.encryptStream({}), /got 0/);
});

test(`${variant}: web encryptStream throws for invalid IV`, async (_t) => {
	const web = await importWeb();
	await rejects(
		() => web.encryptStream({ key, iv: randomBytes(8) }),
		/IV for AES-256-GCM/,
	);
});

test(`${variant}: web decryptStream throws for missing authTag`, async (_t) => {
	const web = await importWeb();
	await rejects(
		() => web.decryptStream({ key, iv: randomBytes(12) }),
		/authTag for AES-256-GCM/,
	);
});

test(`${variant}: web encryptStream throws for unsupported algorithm`, async (_t) => {
	const web = await importWeb();
	await rejects(
		() => web.encryptStream({ key, algorithm: "INVALID" }),
		/Unsupported algorithm/,
	);
});

test(`${variant}: web decryptStream throws for unsupported algorithm`, async (_t) => {
	const web = await importWeb();
	await rejects(
		() =>
			web.decryptStream({
				key,
				iv: randomBytes(12),
				authTag: randomBytes(16),
				algorithm: "INVALID",
			}),
		/Unsupported algorithm/,
	);
});

const webEncryptMaxInput = async (algorithm) => {
	const web = await importWeb();
	const enc = await web.encryptStream({ key, algorithm, maxInputSize: 100 });
	let threw = false;
	let message = "";
	try {
		const stream = createReadableStream("a".repeat(200)).pipe(enc);
		for await (const _chunk of stream) {
			// should fail
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxInputSize"), true);
};

test(`${variant}: web AES-256-GCM encrypt enforces maxInputSize`, async (_t) => {
	await webEncryptMaxInput("AES-256-GCM");
});

test(`${variant}: web CHACHA20-POLY1305 encrypt enforces maxInputSize`, async (_t) => {
	await webEncryptMaxInput("CHACHA20-POLY1305");
});

const webDecryptMaxOutput = async (algorithm) => {
	const web = await importWeb();
	const input = "a".repeat(1000);
	const enc = await web.encryptStream({ key, algorithm });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;
	const dec = await web.decryptStream({
		key,
		iv,
		authTag,
		algorithm,
		maxOutputSize: 100,
	});
	let threw = false;
	let message = "";
	try {
		const decStream = createReadableStream(encryptedChunks).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxOutputSize"), true);
};

test(`${variant}: web AES-256-GCM decrypt enforces maxOutputSize`, async (_t) => {
	await webDecryptMaxOutput("AES-256-GCM");
});

test(`${variant}: web AES-256-CTR decrypt enforces maxOutputSize`, async (_t) => {
	await webDecryptMaxOutput("AES-256-CTR");
});

test(`${variant}: web CHACHA20-POLY1305 decrypt enforces maxOutputSize`, async (_t) => {
	await webDecryptMaxOutput("CHACHA20-POLY1305");
});

test(`${variant}: web AES-256-CTR multi-chunk roundtrip advances counter`, async (_t) => {
	const web = await importWeb();
	// Many chunks larger than one AES block so blockOffset advances and the
	// incrementCounter carry path runs.
	const chunks = Array.from({ length: 8 }, (_, i) => "z".repeat(64) + i);
	const enc = await web.encryptStream({ key, algorithm: "AES-256-CTR" });
	const encryptedChunks = [];
	const encStream = createReadableStream(chunks).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv } = enc.result().value;
	const dec = await web.decryptStream({ key, iv, algorithm: "AES-256-CTR" });
	const decryptedChunks = [];
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	for await (const chunk of decStream) {
		decryptedChunks.push(chunk);
	}
	strictEqual(Buffer.concat(decryptedChunks).toString("utf8"), chunks.join(""));
});

test(`${variant}: web AES-256-GCM decrypt releases NO plaintext on tamper`, async (_t) => {
	const web = await importWeb();
	const input = "a".repeat(64 * 1024);
	const enc = await web.encryptStream({ key });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;
	const ciphertext = Buffer.concat(encryptedChunks.map((c) => Buffer.from(c)));
	ciphertext[ciphertext.length - 1] ^= 0xff;

	const dec = await web.decryptStream({ key, iv, authTag });
	const emitted = [];
	let threw = false;
	try {
		const decStream = createReadableStream([ciphertext]).pipe(dec);
		for await (const chunk of decStream) {
			emitted.push(chunk);
		}
	} catch (_e) {
		threw = true;
	}
	strictEqual(threw, true);
	strictEqual(
		emitted.reduce((n, c) => n + c.length, 0),
		0,
	);
});

test(`${variant}: web AES-256-GCM decrypt enforces maxInputSize before buffering`, async (_t) => {
	const web = await importWeb();
	const input = "a".repeat(1000);
	const enc = await web.encryptStream({ key });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;
	// Re-chunk so the input-size cap can trip mid-stream during transform.
	const ciphertext = Buffer.concat(encryptedChunks.map((c) => Buffer.from(c)));
	const chunked = [];
	for (let i = 0; i < ciphertext.length; i += 100) {
		chunked.push(ciphertext.subarray(i, i + 100));
	}

	const dec = await web.decryptStream({ key, iv, authTag, maxInputSize: 100 });
	let threw = false;
	let message = "";
	try {
		const decStream = createReadableStream(chunked).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail before any plaintext
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxInputSize"), true);
});

test(`${variant}: web CHACHA20-POLY1305 decrypt enforces maxInputSize`, async (_t) => {
	const web = await importWeb();
	const input = "a".repeat(1000);
	const enc = await web.encryptStream({ key, algorithm: "CHACHA20-POLY1305" });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(chunk);
	}
	const { iv, authTag } = enc.result().value;
	const ciphertext = Buffer.concat(encryptedChunks.map((c) => Buffer.from(c)));
	const chunked = [];
	for (let i = 0; i < ciphertext.length; i += 100) {
		chunked.push(ciphertext.subarray(i, i + 100));
	}
	const dec = await web.decryptStream({
		key,
		iv,
		authTag,
		algorithm: "CHACHA20-POLY1305",
		maxInputSize: 100,
	});
	let threw = false;
	let message = "";
	try {
		const decStream = createReadableStream(chunked).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxInputSize"), true);
});

test(`${variant}: web rejects aad supplied with non-AEAD AES-256-CTR`, async (_t) => {
	const web = await importWeb();
	await rejects(
		() =>
			web.encryptStream({
				key,
				algorithm: "AES-256-CTR",
				iv: randomBytes(16),
				aad: Buffer.from("x"),
			}),
		/aad is not supported/,
	);
});

test(`${variant}: node rejects aad supplied with non-AEAD AES-256-CTR`, (_t) => {
	throws(
		() =>
			encryptStream({
				key,
				algorithm: "AES-256-CTR",
				iv: randomBytes(16),
				aad: Buffer.from("x"),
			}),
		/aad is not supported/,
	);
});

test(`${variant}: AES-256-CTR encrypt honors explicit maxInputSize`, async (_t) => {
	const input = "a".repeat(200);
	const enc = await encryptStream({
		key,
		algorithm: "AES-256-CTR",
		maxInputSize: 100,
	});
	let threw = false;
	let message = "";
	try {
		await pipeline([createReadableStream(input), enc]);
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxInputSize"), true);
});

test(`${variant}: web validateAad rejects ArrayBuffer like node (type parity)`, async (_t) => {
	const web = await importWeb();
	await rejects(
		() => web.encryptStream({ key, aad: new ArrayBuffer(8) }),
		/aad must be/,
	);
});

// *** AES-256-CTR counter-wrap determinism & node/web parity *** //
// IV whose low 64 bits are set so the per-block counter wraps across the
// 2^64 boundary within a few blocks. With length:64 the web build wraps only
// the low half while incrementCounter advances the full 128-bit IV, so
// (a) one-chunk vs split-chunk web ciphertext diverges past the wrap and
// (b) node and web ciphertext diverge. length:128 fixes both.
const ctrWrapIv = () => {
	const iv = new Uint8Array(16);
	// high 64 bits = 0, low 64 bits = 0xFFFFFFFFFFFFFFFE
	for (let i = 8; i < 16; i++) {
		iv[i] = 0xff;
	}
	iv[15] = 0xfe;
	return iv;
};

const ctrEncryptChunks = async (mod, iv, chunks) => {
	const enc = await mod.encryptStream({ key, algorithm: "AES-256-CTR", iv });
	const out = [];
	const encStream = createReadableStream(chunks).pipe(enc);
	for await (const chunk of encStream) {
		out.push(Buffer.from(chunk));
	}
	return Buffer.concat(out);
};

test(`${variant}: web AES-256-CTR one-chunk == split-chunk across counter wrap`, async (_t) => {
	const web = await importWeb();
	const iv = ctrWrapIv();
	// 4 blocks of data so the counter advances across the 2^64 low-half wrap.
	const plaintext = Buffer.alloc(64, 0x41);
	const oneChunk = await ctrEncryptChunks(web, new Uint8Array(iv), [plaintext]);
	const splitChunks = [];
	for (let i = 0; i < plaintext.length; i += 16) {
		splitChunks.push(plaintext.subarray(i, i + 16));
	}
	const split = await ctrEncryptChunks(web, new Uint8Array(iv), splitChunks);
	deepStrictEqual(oneChunk, split);
});

test(`${variant}: web AES-256-CTR encrypt-then-rechunk-decrypt round-trips across wrap`, async (_t) => {
	const web = await importWeb();
	const iv = ctrWrapIv();
	const plaintext = Buffer.alloc(64, 0x42);
	const ciphertext = await ctrEncryptChunks(web, new Uint8Array(iv), [
		plaintext,
	]);
	// Re-chunk the ciphertext differently from how it was produced.
	const rechunked = [];
	for (let i = 0; i < ciphertext.length; i += 16) {
		rechunked.push(ciphertext.subarray(i, i + 16));
	}
	const dec = await web.decryptStream({
		key,
		iv: new Uint8Array(iv),
		algorithm: "AES-256-CTR",
	});
	const out = [];
	const decStream = createReadableStream(rechunked).pipe(dec);
	for await (const chunk of decStream) {
		out.push(Buffer.from(chunk));
	}
	deepStrictEqual(Buffer.concat(out), plaintext);
});

test(`${variant}: node and web AES-256-CTR produce identical ciphertext across counter wrap`, async (_t) => {
	const web = await importWeb();
	const iv = ctrWrapIv();
	const plaintext = Buffer.alloc(64, 0x43);
	const nodeCipher = await ctrEncryptChunks(
		{ encryptStream },
		Buffer.from(iv),
		[plaintext],
	);
	const webCipher = await ctrEncryptChunks(web, new Uint8Array(iv), [
		plaintext,
	]);
	deepStrictEqual(webCipher, nodeCipher);
});

// *** Web AES-256-CTR encrypt maxInputSize parity *** //
test(`${variant}: web AES-256-CTR encrypt honors explicit maxInputSize`, async (_t) => {
	const web = await importWeb();
	const enc = await web.encryptStream({
		key,
		algorithm: "AES-256-CTR",
		iv: randomBytes(16),
		maxInputSize: 100,
	});
	let threw = false;
	let message = "";
	try {
		const stream = createReadableStream("a".repeat(200)).pipe(enc);
		for await (const _chunk of stream) {
			// should fail
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxInputSize"), true);
});

// *** resultKey honored in result() (node + web, all encrypt modes) *** //
const resultKeyHonored = async (mod, algorithm) => {
	const enc = await mod.encryptStream({ key, algorithm, resultKey: "cipher" });
	const encStream = createReadableStream("resultKey payload").pipe(enc);
	for await (const _chunk of encStream) {
		// drain
	}
	const { key: resultKey, value } = enc.result();
	strictEqual(resultKey, "cipher");
	strictEqual(value.algorithm, algorithm);
};

test(`${variant}: node encryptStream result() honors resultKey (AES-256-GCM)`, async (_t) => {
	await resultKeyHonored({ encryptStream }, "AES-256-GCM");
});

test(`${variant}: node encryptStream result() honors resultKey (AES-256-CTR)`, async (_t) => {
	await resultKeyHonored({ encryptStream }, "AES-256-CTR");
});

test(`${variant}: node encryptStream result() honors resultKey (CHACHA20-POLY1305)`, async (_t) => {
	await resultKeyHonored({ encryptStream }, "CHACHA20-POLY1305");
});

test(`${variant}: web encryptStream result() honors resultKey (AES-256-GCM)`, async (_t) => {
	const web = await importWeb();
	await resultKeyHonored(web, "AES-256-GCM");
});

test(`${variant}: web encryptStream result() honors resultKey (AES-256-CTR)`, async (_t) => {
	const web = await importWeb();
	await resultKeyHonored(web, "AES-256-CTR");
});

test(`${variant}: web encryptStream result() honors resultKey (CHACHA20-POLY1305)`, async (_t) => {
	const web = await importWeb();
	await resultKeyHonored(web, "CHACHA20-POLY1305");
});

test(`${variant}: encryptStream result() defaults resultKey to 'encrypt'`, async (_t) => {
	const enc = encryptStream({ key });
	await pipeline([createReadableStream("default key"), enc]);
	strictEqual(enc.result().key, "encrypt");
});

// *** generateEncryptionKey({bits:128}) must produce a usable key *** //
const key128Usable = async (mod, algorithm) => {
	const k = mod.generateEncryptionKey({ bits: 128 });
	strictEqual(k.byteLength, 16);
	const input = "128-bit key round-trip";
	const enc = await mod.encryptStream({ key: k, algorithm });
	const encryptedChunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		encryptedChunks.push(Buffer.from(chunk));
	}
	const { iv, authTag } = enc.result().value;
	const dec = await mod.decryptStream({ key: k, iv, authTag, algorithm });
	const decryptedChunks = [];
	const decStream = createReadableStream(encryptedChunks).pipe(dec);
	for await (const chunk of decStream) {
		decryptedChunks.push(Buffer.from(chunk));
	}
	strictEqual(Buffer.concat(decryptedChunks).toString("utf8"), input);
};

test(`${variant}: node 128-bit key usable with AES-128-GCM`, async (_t) => {
	await key128Usable(
		{ encryptStream, decryptStream, generateEncryptionKey },
		"AES-128-GCM",
	);
});

test(`${variant}: node 128-bit key usable with AES-128-CTR`, async (_t) => {
	await key128Usable(
		{ encryptStream, decryptStream, generateEncryptionKey },
		"AES-128-CTR",
	);
});

test(`${variant}: web 128-bit key usable with AES-128-GCM`, async (_t) => {
	const web = await importWeb();
	await key128Usable(web, "AES-128-GCM");
});

test(`${variant}: web 128-bit key usable with AES-128-CTR`, async (_t) => {
	const web = await importWeb();
	await key128Usable(web, "AES-128-CTR");
});

// *** Mutation-kill: exact validation error messages *** //
test(`${variant}: validateKey emits exact bits/got in 256-bit message`, (_t) => {
	throws(
		() => encryptStream({ key: randomBytes(16) }),
		/Encryption key must be 32 bytes \(256 bits\), got 16/,
	);
});

test(`${variant}: validateKey emits exact bits/got in 128-bit message`, (_t) => {
	throws(
		() => encryptStream({ key: randomBytes(8), algorithm: "AES-128-GCM" }),
		/Encryption key must be 16 bytes \(128 bits\), got 8/,
	);
});

test(`${variant}: validateKey reports "got 0" (not undefined) for missing key`, (_t) => {
	// Kills `key?.length ?? 0` -> `key?.length && 0` (would yield "got undefined").
	throws(
		() => encryptStream({}),
		/Encryption key must be 32 bytes \(256 bits\), got 0/,
	);
});

test(`${variant}: validateAuthTag rejects 15-byte authTag with exact message`, (_t) => {
	// Kills `authTag.length !== 16` -> `false`; a non-falsy wrong-length tag
	// must still be rejected with the validation message (not a deeper error).
	throws(
		() => decryptStream({ key, iv: randomBytes(12), authTag: randomBytes(15) }),
		/authTag for AES-256-GCM must be 16 bytes, got 15/,
	);
});

// *** Mutation-kill: encrypt maxInputSize message + boundary *** //
test(`${variant}: encrypt maxInputSize message reports the explicit limit`, async (_t) => {
	// Kills `maxInputSize ?? DEFAULT` -> `maxInputSize && DEFAULT` (reportedLimit
	// would become 67108864 instead of 100).
	const enc = encryptStream({ key, maxInputSize: 100 });
	let message = "";
	try {
		await pipeline([createReadableStream("a".repeat(200)), enc]);
	} catch (e) {
		message = e.message;
	}
	strictEqual(message.includes("(100 bytes)"), true);
});

test(`${variant}: encrypt maxInputSize boundary: exactly the limit succeeds`, async (_t) => {
	// Kills `inputSize > limit` -> `inputSize >= limit` on the encrypt path.
	const enc = encryptStream({ key, maxInputSize: 100 });
	await pipeline([createReadableStream("a".repeat(100)), enc]);
	strictEqual(enc.result().value.algorithm, "AES-256-GCM");
});

test(`${variant}: encrypt maxInputSize boundary: one over the limit fails`, async (_t) => {
	const enc = encryptStream({ key, maxInputSize: 100 });
	let threw = false;
	try {
		await pipeline([createReadableStream("a".repeat(101)), enc]);
	} catch (_e) {
		threw = true;
	}
	strictEqual(threw, true);
});

// *** Mutation-kill: encrypt default ceilings (CTR huge vs AEAD 64MB) *** //
// AES-CTR with no maxInputSize uses the keystream-reuse ceiling (2^68), so a
// >64MB stream succeeds; AEAD modes default to the 64MB DoS guard, so >64MB is
// rejected with the maxInputSize message. 65MB distinguishes the two ceilings.
// Drain without retaining ciphertext to keep peak memory bounded.
const drain = async (stream) => {
	for await (const _chunk of stream) {
		// discard
	}
};
const big65 = () => Buffer.alloc(65 * 1024 * 1024, 0x41);

test(`${variant}: AES-256-CTR encrypt allows >64MB with no maxInputSize`, async (_t) => {
	// Kills ctrAlgorithms mutations and `usingCtrDefaultCeiling = false`: any of
	// those drops AES-256-CTR back to the 64MB default and would reject this.
	const enc = encryptStream({ key, algorithm: "AES-256-CTR" });
	await drain(createReadableStream([big65()]).pipe(enc));
	strictEqual(enc.result().value.algorithm, "AES-256-CTR");
});

test(`${variant}: AES-128-CTR encrypt allows >64MB with no maxInputSize`, async (_t) => {
	// Kills the "AES-128-CTR" element of ctrAlgorithms (empty/blanked array).
	const k16 = randomBytes(16);
	const enc = encryptStream({ key: k16, algorithm: "AES-128-CTR" });
	await drain(createReadableStream([big65()]).pipe(enc));
	strictEqual(enc.result().value.algorithm, "AES-128-CTR");
});

test(`${variant}: AES-256-GCM encrypt rejects >64MB at the 64MB default`, async (_t) => {
	// Kills `if (maxInputSize != null) -> true`, `else if (...) -> true`, removal
	// of the DEFAULT_MAX_INPUT_SIZE assignment: each would let >64MB AEAD pass.
	const enc = encryptStream({ key });
	let threw = false;
	let message = "";
	try {
		await drain(createReadableStream([big65()]).pipe(enc));
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxInputSize"), true);
	strictEqual(message.includes("67108864"), true);
});

// *** Mutation-kill: AEAD decrypt maxInputSize / maxOutputSize boundaries *** //
const gcmCiphertext = async (input) => {
	const enc = encryptStream({ key });
	const chunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		chunks.push(Buffer.from(chunk));
	}
	const { iv, authTag } = enc.result().value;
	return { ciphertext: Buffer.concat(chunks), iv, authTag };
};

test(`${variant}: AEAD decrypt maxInputSize boundary: exactly the limit succeeds`, async (_t) => {
	// Kills `inputSize > maxInputSize` -> `>=` in aeadDecryptStream.
	const { ciphertext, iv, authTag } = await gcmCiphertext("boundary in");
	const dec = decryptStream({
		key,
		iv,
		authTag,
		maxInputSize: ciphertext.length,
	});
	const out = [];
	const decStream = createReadableStream([ciphertext]).pipe(dec);
	for await (const chunk of decStream) {
		out.push(Buffer.from(chunk));
	}
	strictEqual(Buffer.concat(out).toString("utf8"), "boundary in");
});

test(`${variant}: AEAD decrypt maxInputSize boundary: one over the limit fails`, async (_t) => {
	const { ciphertext, iv, authTag } = await gcmCiphertext("boundary in");
	const dec = decryptStream({
		key,
		iv,
		authTag,
		maxInputSize: ciphertext.length - 1,
	});
	let threw = false;
	try {
		const decStream = createReadableStream([ciphertext]).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail
		}
	} catch (_e) {
		threw = true;
	}
	strictEqual(threw, true);
});

test(`${variant}: AEAD decrypt within maxOutputSize succeeds (no false error)`, async (_t) => {
	// Kills `maxOutputSize != null && X` -> `|| X` and `&& true`: those would
	// reject a plaintext that is within the configured limit.
	const input = "exact output";
	const { ciphertext, iv, authTag } = await gcmCiphertext(input);
	const dec = decryptStream({
		key,
		iv,
		authTag,
		maxOutputSize: input.length,
	});
	const out = [];
	const decStream = createReadableStream([ciphertext]).pipe(dec);
	for await (const chunk of decStream) {
		out.push(Buffer.from(chunk));
	}
	strictEqual(Buffer.concat(out).toString("utf8"), input);
});

test(`${variant}: AEAD decrypt maxOutputSize boundary: one under the limit fails`, async (_t) => {
	// Kills `plaintext.length > maxOutputSize` -> `>=` in aeadDecryptStream.
	const input = "exact output";
	const { ciphertext, iv, authTag } = await gcmCiphertext(input);
	const dec = decryptStream({
		key,
		iv,
		authTag,
		maxOutputSize: input.length - 1,
	});
	let threw = false;
	let message = "";
	try {
		const decStream = createReadableStream([ciphertext]).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxOutputSize"), true);
});

// *** Mutation-kill: AES-256-CTR decrypt maxOutputSize push override *** //
const ctrCiphertext = async (input) => {
	const enc = encryptStream({ key, algorithm: "AES-256-CTR" });
	const chunks = [];
	const encStream = createReadableStream(input).pipe(enc);
	for await (const chunk of encStream) {
		chunks.push(Buffer.from(chunk));
	}
	const { iv } = enc.result().value;
	return { chunks, iv };
};

test(`${variant}: CTR decrypt maxOutputSize over-limit fails with message`, async (_t) => {
	// Kills `if (chunk !== null) -> false`, `if (maxOutputSize != null) -> false`,
	// `outputSize += -> -=`, and `outputSize > maxOutputSize -> false`: each would
	// suppress the over-limit error.
	const { chunks, iv } = await ctrCiphertext("a".repeat(500));
	const dec = decryptStream({
		key,
		iv,
		algorithm: "AES-256-CTR",
		maxOutputSize: 100,
	});
	let threw = false;
	let message = "";
	let emitted = 0;
	try {
		const decStream = createReadableStream(chunks).pipe(dec);
		for await (const chunk of decStream) {
			emitted += chunk.length;
		}
	} catch (e) {
		threw = true;
		message = e.message;
	}
	strictEqual(threw, true);
	strictEqual(message.includes("maxOutputSize"), true);
	strictEqual(message.includes("100"), true);
	strictEqual(emitted <= 100, true);
});

test(`${variant}: CTR decrypt maxOutputSize boundary: exactly the limit succeeds`, async (_t) => {
	// Kills `outputSize > maxOutputSize` -> `>=` on the CTR decrypt path:
	// output length equal to the limit must NOT error.
	const input = "a".repeat(100);
	const { chunks, iv } = await ctrCiphertext(input);
	const dec = decryptStream({
		key,
		iv,
		algorithm: "AES-256-CTR",
		maxOutputSize: 100,
	});
	const out = [];
	const decStream = createReadableStream(chunks).pipe(dec);
	for await (const chunk of decStream) {
		out.push(Buffer.from(chunk));
	}
	strictEqual(Buffer.concat(out).toString("utf8"), input);
});

// *** Mutation-kill: createCipheriv options object forwards streamOptions ***
// The 4th argument `{ ...streamOptions, authTagLength }` is the cipher's
// Transform options. Replacing it with `{}` (ObjectLiteral mutant) would drop
// the forwarded highWaterMark, so the returned cipher stream would fall back to
// the default highWaterMark instead of the caller-supplied one.
test(`${variant}: encryptStream forwards streamOptions.highWaterMark to the cipher stream`, (_t) => {
	const enc = encryptStream({ key }, { highWaterMark: 7 });
	strictEqual(
		enc.readableHighWaterMark,
		7,
		"highWaterMark from streamOptions must reach createCipheriv",
	);
	strictEqual(enc.writableHighWaterMark, 7);
});

test(`${variant}: encryptStream cipher uses the default highWaterMark when none supplied`, (_t) => {
	// Guards the above: the 7 above is genuinely from streamOptions, not a default.
	const enc = encryptStream({ key });
	strictEqual(enc.readableHighWaterMark !== 7, true);
});

// *** Mutation-kill: maxOutputSize == null means NO ceiling (explicit null) ***
// `maxOutputSize != null && ...` must short-circuit to false when null. The
// `true && ...` mutant degrades to `length > null` (=> length > 0) and would
// wrongly reject any non-empty plaintext.
test(`${variant}: AEAD decrypt with maxOutputSize: null imposes no output ceiling`, async (_t) => {
	const input = "no ceiling please";
	const { ciphertext, iv, authTag } = await gcmCiphertext(input);
	const dec = decryptStream({ key, iv, authTag, maxOutputSize: null });
	const out = [];
	const decStream = createReadableStream([ciphertext]).pipe(dec);
	for await (const chunk of decStream) {
		out.push(Buffer.from(chunk));
	}
	strictEqual(Buffer.concat(out).toString("utf8"), input);
});

test(`${variant}: CTR decrypt with maxOutputSize: null imposes no output ceiling`, async (_t) => {
	// Kills `if (maxOutputSize != null) -> if (true)` on the CTR push override:
	// with null and the mutant, `outputSize > null` (=> > 0) would destroy the
	// stream on the first non-empty chunk.
	const input = "a".repeat(500);
	const { chunks, iv } = await ctrCiphertext(input);
	const dec = decryptStream({
		key,
		iv,
		algorithm: "AES-256-CTR",
		maxOutputSize: null,
	});
	const out = [];
	const decStream = createReadableStream(chunks).pipe(dec);
	for await (const chunk of decStream) {
		out.push(Buffer.from(chunk));
	}
	strictEqual(Buffer.concat(out).toString("utf8"), input);
});

test(`${variant}: CTR decrypt maxOutputSize accumulates across chunks`, async (_t) => {
	// Reinforces the `outputSize += chunk.length` accumulation (kills `-=`):
	// many small chunks whose sum exceeds the limit must error.
	const { chunks, iv } = await ctrCiphertext("a".repeat(64));
	const rechunked = [];
	const ct = Buffer.concat(chunks);
	for (let i = 0; i < ct.length; i += 8) {
		rechunked.push(ct.subarray(i, i + 8));
	}
	const dec = decryptStream({
		key,
		iv,
		algorithm: "AES-256-CTR",
		maxOutputSize: 32,
	});
	let threw = false;
	try {
		const decStream = createReadableStream(rechunked).pipe(dec);
		for await (const _chunk of decStream) {
			// should fail once cumulative output passes 32
		}
	} catch (_e) {
		threw = true;
	}
	strictEqual(threw, true);
});
