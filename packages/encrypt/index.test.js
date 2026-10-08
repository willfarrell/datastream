// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, rejects, strictEqual, throws } from "node:assert";
import { createCipheriv, randomBytes } from "node:crypto";
import test, { describe } from "node:test";
import { createReadableStream, pipejoin, pipeline } from "@datastream/core";
import {
	decryptStream,
	encryptStream,
	generateEncryptionKey,
} from "@datastream/encrypt";
import { variant } from "../variant.js";

describe(`@datastream/encrypt (${variant})`, () => {
	// Sync validation throws and node:crypto specifics; the browser build is async
	// WebCrypto and is exercised by the "web" tests below.
	const nodeTest = variant === "node" ? test : test.skip;
	// Errors are consumed through the async iterator below; a no-op onError stops
	// the node build's pipejoin from also re-throwing them on nextTick.
	const join = (streams) => pipejoin(streams, () => {});

	const key = randomBytes(32);

	// *** generateEncryptionKey *** //
	test(`generateEncryptionKey should generate 32-byte key by default`, (_t) => {
		const k = generateEncryptionKey();
		strictEqual(k.byteLength, 32);
	});

	test(`generateEncryptionKey should generate 16-byte key for 128 bits`, (_t) => {
		const k = generateEncryptionKey({ bits: 128 });
		strictEqual(k.byteLength, 16);
	});

	// *** AES-256-GCM (default) *** //
	test(`encryptStream should encrypt and decrypt with AES-256-GCM`, async (_t) => {
		const input = "hello, world!";
		const enc = await encryptStream({ key });
		const streams = [createReadableStream(input), enc];
		await pipeline(streams);

		const { key: resultKey, value } = enc.result();
		strictEqual(resultKey, "encrypt");
		strictEqual(value.algorithm, "AES-256-GCM");
		strictEqual(value.iv.byteLength, 12);
		strictEqual(value.authTag.byteLength, 16);
	});

	test(`encryptStream roundtrip with AES-256-GCM`, async (_t) => {
		const input = "secret data to encrypt";

		// Encrypt
		const enc = await encryptStream({ key });
		const encStreams = [createReadableStream(input), enc];
		const encryptedChunks = [];
		const encStream = join(encStreams);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;

		// Decrypt
		const dec = await decryptStream({ key, iv, authTag });
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		const decryptedChunks = [];
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
		strictEqual(decrypted, input);
	});

	test(`encryptStream should use custom IV`, async (_t) => {
		const iv = randomBytes(12);
		const enc = await encryptStream({ key, iv });
		const streams = [createReadableStream("test"), enc];
		await pipeline(streams);

		const { value } = enc.result();
		deepStrictEqual(value.iv, iv);
	});

	test(`encryptStream should support AAD`, async (_t) => {
		const aad = Buffer.from("metadata");

		// Encrypt with AAD
		const enc = await encryptStream({ key, aad });
		const encStreams = [createReadableStream("aad test"), enc];
		const encryptedChunks = [];
		const encStream = join(encStreams);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;

		// Decrypt with matching AAD
		const dec = await decryptStream({ key, iv, authTag, aad });
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		const decryptedChunks = [];
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
		strictEqual(decrypted, "aad test");
	});

	test(`encryptStream should collect result via pipeline`, async (_t) => {
		const enc = await encryptStream({ key });
		const streams = [createReadableStream("pipeline test"), enc];
		const result = await pipeline(streams);

		strictEqual(typeof result.encrypt, "object");
		strictEqual(result.encrypt.algorithm, "AES-256-GCM");
	});

	// *** AES-256-CTR *** //
	test(`encryptStream roundtrip with AES-256-CTR`, async (_t) => {
		const input = "ctr mode test data";

		const enc = await encryptStream({ key, algorithm: "AES-256-CTR" });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv } = enc.result().value;
		strictEqual(iv.byteLength, 16);
		strictEqual(enc.result().value.authTag, undefined);

		const dec = await decryptStream({ key, iv, algorithm: "AES-256-CTR" });
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		const decryptedChunks = [];
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
		strictEqual(decrypted, input);
	});

	// *** CHACHA20-POLY1305 *** //
	test(`encryptStream roundtrip with CHACHA20-POLY1305`, async (_t) => {
		const input = "chacha20 test data";

		const enc = await encryptStream({ key, algorithm: "CHACHA20-POLY1305" });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;
		strictEqual(iv.byteLength, 12);
		strictEqual(authTag.byteLength, 16);

		const dec = await decryptStream({
			key,
			iv,
			authTag,
			algorithm: "CHACHA20-POLY1305",
		});
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		const decryptedChunks = [];
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
		strictEqual(decrypted, input);
	});

	// *** Error cases *** //
	test(`encryptStream should throw for unsupported algorithm`, async (_t) => {
		await rejects(
			() => encryptStream({ key, algorithm: "INVALID" }),
			/Unsupported algorithm/,
		);
	});

	test(`decryptStream should throw for unsupported algorithm`, async (_t) => {
		await rejects(
			() => decryptStream({ key, iv: randomBytes(12), algorithm: "INVALID" }),
			/Unsupported algorithm/,
		);
	});

	test(`decryptStream should fail with wrong key`, async (_t) => {
		const enc = await encryptStream({ key });
		const encryptedChunks = [];
		const encStream = join([createReadableStream("wrong key test"), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;

		const wrongKey = randomBytes(32);
		const dec = await decryptStream({ key: wrongKey, iv, authTag });
		try {
			const decStream = join([createReadableStream(encryptedChunks), dec]);
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
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;

		// Tamper: flip the last byte of the ciphertext.
		const ciphertext = Buffer.concat(
			encryptedChunks.map((c) => Buffer.from(c)),
		);
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
			const decStream = join([createReadableStream(tamperedChunks), dec]);
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

	test(`AES-256-GCM decrypt releases NO plaintext on tampered ciphertext`, async (_t) => {
		await tamperedAeadEmitsNothing("AES-256-GCM");
	});

	test(`CHACHA20-POLY1305 decrypt releases NO plaintext on tampered ciphertext`, async (_t) => {
		await tamperedAeadEmitsNothing("CHACHA20-POLY1305");
	});

	test(`AES-256-GCM decrypt enforces maxInputSize`, async (_t) => {
		const input = "a".repeat(1000);
		const enc = await encryptStream({ key });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;
		const ciphertext = Buffer.concat(
			encryptedChunks.map((c) => Buffer.from(c)),
		);
		const chunked = [];
		for (let i = 0; i < ciphertext.length; i += 100) {
			chunked.push(ciphertext.subarray(i, i + 100));
		}
		const dec = await decryptStream({ key, iv, authTag, maxInputSize: 100 });
		let threw = false;
		let message = "";
		try {
			const decStream = join([createReadableStream(chunked), dec]);
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

	test(`AES-256-GCM decrypt enforces maxOutputSize`, async (_t) => {
		const input = "a".repeat(1000);
		const enc = await encryptStream({ key });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;
		const dec = await decryptStream({ key, iv, authTag, maxOutputSize: 100 });
		let threw = false;
		let message = "";
		try {
			const decStream = join([createReadableStream(encryptedChunks), dec]);
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

	test(`decryptStream maxOutputSize should limit output`, async (_t) => {
		const input = "a".repeat(1000);
		const enc = await encryptStream({ key, algorithm: "AES-256-CTR" });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv } = enc.result().value;

		const dec = await decryptStream({
			key,
			iv,
			algorithm: "AES-256-CTR",
			maxOutputSize: 100,
		});
		try {
			const decStream = join([createReadableStream(encryptedChunks), dec]);
			for await (const _chunk of decStream) {
				// should fail
			}
			throw new Error("Expected maxOutputSize error");
		} catch (e) {
			strictEqual(e.message.includes("maxOutputSize"), true);
		}
	});

	// *** Chunked input *** //
	test(`encryptStream should handle chunked input`, async (_t) => {
		const chunks = ["hello, ", "world", "!"];

		const enc = await encryptStream({ key });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(chunks), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;

		const dec = await decryptStream({ key, iv, authTag });
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		const decryptedChunks = [];
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
		strictEqual(decrypted, "hello, world!");
	});

	// *** Empty input *** //
	test(`encryptStream should handle empty input`, async (_t) => {
		const enc = await encryptStream({ key });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(""), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;

		const dec = await decryptStream({ key, iv, authTag });
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		const decryptedChunks = [];
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		const decrypted = Buffer.concat(decryptedChunks).toString("utf8");
		strictEqual(decrypted, "");
	});

	// *** maxInputSize *** //
	test(`encryptStream AES-256-GCM should enforce maxInputSize`, async (_t) => {
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

	// maxInputSize counts bytes, not UTF-16 code units: "🙂🙂" is 4 code units but
	// 8 UTF-8 bytes, so it must exceed a 4-byte limit.
	test(`encryptStream should count string chunks in bytes for maxInputSize`, async (_t) => {
		const enc = await encryptStream({ key, maxInputSize: 4 });
		await rejects(pipeline([createReadableStream("🙂🙂"), enc]), (e) =>
			e.message.includes("maxInputSize"),
		);
	});

	// The byte count honours the write encoding: 8 hex chars are 4 bytes.
	nodeTest(
		`encryptStream should count string chunks in their write encoding`,
		async (_t) => {
			const enc = await encryptStream({ key, maxInputSize: 4 });
			enc.end("00000000", "hex");
			enc.resume();
			await new Promise((resolve, reject) => {
				enc.on("end", resolve);
				enc.on("error", reject);
			});
		},
	);

	test(`encryptStream CHACHA20-POLY1305 should enforce maxInputSize`, async (_t) => {
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
	test(`generateEncryptionKey should throw for unsupported bits`, (_t) => {
		throws(() => generateEncryptionKey({ bits: 512 }), /Unsupported key size/);
	});

	// *** validation error cases *** //
	test(`encryptStream should throw for invalid key length`, async (_t) => {
		await rejects(() => encryptStream({ key: randomBytes(16) }), /32 bytes/);
		await rejects(() => encryptStream({}), /32 bytes/);
	});

	test(`encryptStream should throw for invalid IV length`, async (_t) => {
		await rejects(
			() => encryptStream({ key, iv: randomBytes(8) }),
			/IV for AES-256-GCM/,
		);
	});

	test(`encryptStream should throw for invalid aad`, async (_t) => {
		await rejects(
			() => encryptStream({ key, aad: "not-a-buffer" }),
			/aad must be/,
		);
	});

	test(`decryptStream should throw for invalid key length`, async (_t) => {
		await rejects(
			() => decryptStream({ key: randomBytes(16), iv: randomBytes(12) }),
			/32 bytes/,
		);
	});

	test(`decryptStream should throw for invalid IV length`, async (_t) => {
		await rejects(
			() => decryptStream({ key, iv: randomBytes(8) }),
			/IV for AES-256-GCM/,
		);
	});

	test(`decryptStream should report 0 for missing IV`, async (_t) => {
		await rejects(
			() => decryptStream({ key, authTag: randomBytes(16) }),
			/got 0/,
		);
	});

	test(`decryptStream should report 0 for missing authTag`, async (_t) => {
		await rejects(() => decryptStream({ key, iv: randomBytes(12) }), /got 0/);
	});

	test(`decryptStream should throw for missing authTag on GCM`, async (_t) => {
		await rejects(
			() => decryptStream({ key, iv: randomBytes(12) }),
			/authTag for AES-256-GCM/,
		);
	});

	// *** within-limit paths *** //
	test(`encryptStream maxInputSize within limit should succeed`, async (_t) => {
		const input = "a".repeat(50);
		const enc = await encryptStream({ key, maxInputSize: 100 });
		const streams = [createReadableStream(input), enc];
		await pipeline(streams);
		strictEqual(enc.result().value.algorithm, "AES-256-GCM");
	});

	test(`decryptStream maxOutputSize within limit should succeed`, async (_t) => {
		const input = "small";
		const enc = await encryptStream({ key, algorithm: "AES-256-CTR" });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv } = enc.result().value;
		const dec = await decryptStream({
			key,
			iv,
			algorithm: "AES-256-CTR",
			maxOutputSize: 1000,
		});
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		const decryptedChunks = [];
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		strictEqual(Buffer.concat(decryptedChunks).toString("utf8"), input);
	});

	// *** Browser implementation *** //
	// Under the browser run `@datastream/encrypt` is the built browser bundle; the
	// node run imports the source directly so WebCrypto paths are pinned there too.
	const importWeb = () =>
		variant === "browser"
			? import("@datastream/encrypt")
			: import(
					`file://${new URL("./index.browser.js", import.meta.url).pathname}`
				);

	const webRoundtrip = async (algorithm, aad) => {
		const web = await importWeb();
		const input = "the web implementation must round-trip correctly";
		const enc = await web.encryptStream({ key, algorithm, aad });
		const encryptedChunks = [];
		// Mixed byte/string chunks in, and an empty string chunk among the
		// ciphertext bytes out: both builds accept either chunk type on both sides.
		const encStream = join([
			createReadableStream([
				new TextEncoder().encode(input.slice(0, 7)),
				input.slice(7),
			]),
			enc,
		]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;
		const dec = await web.decryptStream({ key, iv, authTag, algorithm, aad });
		const decryptedChunks = [];
		const decStream = join([
			createReadableStream(["", ...encryptedChunks]),
			dec,
		]);
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		strictEqual(Buffer.concat(decryptedChunks).toString("utf8"), input);
	};

	test(`web roundtrip AES-256-GCM`, async (_t) => {
		await webRoundtrip("AES-256-GCM");
	});

	test(`web roundtrip AES-256-GCM with AAD`, async (_t) => {
		await webRoundtrip("AES-256-GCM", Buffer.from("metadata"));
	});

	test(`web roundtrip AES-256-CTR`, async (_t) => {
		await webRoundtrip("AES-256-CTR");
	});

	test(`web roundtrip CHACHA20-POLY1305`, async (_t) => {
		await webRoundtrip("CHACHA20-POLY1305");
	});

	test(`web generateEncryptionKey default and 128-bit`, async (_t) => {
		const web = await importWeb();
		strictEqual(web.generateEncryptionKey().byteLength, 32);
		strictEqual(web.generateEncryptionKey({ bits: 128 }).byteLength, 16);
	});

	test(`web generateEncryptionKey throws for unsupported bits`, async (_t) => {
		const web = await importWeb();
		throws(
			() => web.generateEncryptionKey({ bits: 512 }),
			/Unsupported key size/,
		);
	});

	test(`web encryptStream throws for invalid key`, async (_t) => {
		const web = await importWeb();
		await rejects(
			() => web.encryptStream({ key: randomBytes(16) }),
			/32 bytes/,
		);
	});

	test(`web encryptStream reports 0 for missing key`, async (_t) => {
		const web = await importWeb();
		await rejects(() => web.encryptStream({}), /got 0/);
	});

	test(`web encryptStream throws for invalid IV`, async (_t) => {
		const web = await importWeb();
		await rejects(
			() => web.encryptStream({ key, iv: randomBytes(8) }),
			/IV for AES-256-GCM/,
		);
	});

	test(`web decryptStream throws for missing authTag`, async (_t) => {
		const web = await importWeb();
		await rejects(
			() => web.decryptStream({ key, iv: randomBytes(12) }),
			/authTag for AES-256-GCM/,
		);
	});

	test(`web encryptStream throws for unsupported algorithm`, async (_t) => {
		const web = await importWeb();
		await rejects(
			() => web.encryptStream({ key, algorithm: "INVALID" }),
			/Unsupported algorithm/,
		);
	});

	test(`web decryptStream throws for unsupported algorithm`, async (_t) => {
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
			const stream = join([createReadableStream("a".repeat(200)), enc]);
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

	test(`web AES-256-GCM encrypt enforces maxInputSize`, async (_t) => {
		await webEncryptMaxInput("AES-256-GCM");
	});

	test(`web CHACHA20-POLY1305 encrypt enforces maxInputSize`, async (_t) => {
		await webEncryptMaxInput("CHACHA20-POLY1305");
	});

	const webDecryptMaxOutput = async (algorithm) => {
		const web = await importWeb();
		const input = "a".repeat(1000);
		const enc = await web.encryptStream({ key, algorithm });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
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
			const decStream = join([createReadableStream(encryptedChunks), dec]);
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

	test(`web AES-256-GCM decrypt enforces maxOutputSize`, async (_t) => {
		await webDecryptMaxOutput("AES-256-GCM");
	});

	test(`web AES-256-CTR decrypt enforces maxOutputSize`, async (_t) => {
		await webDecryptMaxOutput("AES-256-CTR");
	});

	test(`web CHACHA20-POLY1305 decrypt enforces maxOutputSize`, async (_t) => {
		await webDecryptMaxOutput("CHACHA20-POLY1305");
	});

	test(`web AES-256-CTR multi-chunk roundtrip advances counter`, async (_t) => {
		const web = await importWeb();
		// Many chunks larger than one AES block so blockOffset advances and the
		// incrementCounter carry path runs.
		const chunks = Array.from({ length: 8 }, (_, i) => "z".repeat(64) + i);
		const enc = await web.encryptStream({ key, algorithm: "AES-256-CTR" });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(chunks), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv } = enc.result().value;
		const dec = await web.decryptStream({ key, iv, algorithm: "AES-256-CTR" });
		const decryptedChunks = [];
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		for await (const chunk of decStream) {
			decryptedChunks.push(chunk);
		}
		strictEqual(
			Buffer.concat(decryptedChunks).toString("utf8"),
			chunks.join(""),
		);
	});

	test(`web AES-256-GCM decrypt releases NO plaintext on tamper`, async (_t) => {
		const web = await importWeb();
		const input = "a".repeat(64 * 1024);
		const enc = await web.encryptStream({ key });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;
		const ciphertext = Buffer.concat(
			encryptedChunks.map((c) => Buffer.from(c)),
		);
		ciphertext[ciphertext.length - 1] ^= 0xff;

		const dec = await web.decryptStream({ key, iv, authTag });
		const emitted = [];
		let threw = false;
		try {
			const decStream = join([createReadableStream([ciphertext]), dec]);
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

	test(`web AES-256-GCM decrypt enforces maxInputSize before buffering`, async (_t) => {
		const web = await importWeb();
		const input = "a".repeat(1000);
		const enc = await web.encryptStream({ key });
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;
		// Re-chunk so the input-size cap can trip mid-stream during transform.
		const ciphertext = Buffer.concat(
			encryptedChunks.map((c) => Buffer.from(c)),
		);
		const chunked = [];
		for (let i = 0; i < ciphertext.length; i += 100) {
			chunked.push(ciphertext.subarray(i, i + 100));
		}

		const dec = await web.decryptStream({
			key,
			iv,
			authTag,
			maxInputSize: 100,
		});
		let threw = false;
		let message = "";
		try {
			const decStream = join([createReadableStream(chunked), dec]);
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

	test(`web CHACHA20-POLY1305 decrypt enforces maxInputSize`, async (_t) => {
		const web = await importWeb();
		const input = "a".repeat(1000);
		const enc = await web.encryptStream({
			key,
			algorithm: "CHACHA20-POLY1305",
		});
		const encryptedChunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(chunk);
		}
		const { iv, authTag } = enc.result().value;
		const ciphertext = Buffer.concat(
			encryptedChunks.map((c) => Buffer.from(c)),
		);
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
			const decStream = join([createReadableStream(chunked), dec]);
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

	test(`web rejects aad supplied with non-AEAD AES-256-CTR`, async (_t) => {
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

	test(`node rejects aad supplied with non-AEAD AES-256-CTR`, async (_t) => {
		await rejects(
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

	test(`AES-256-CTR encrypt honors explicit maxInputSize`, async (_t) => {
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

	test(`web validateAad rejects ArrayBuffer like node (type parity)`, async (_t) => {
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
		const encStream = join([createReadableStream(chunks), enc]);
		for await (const chunk of encStream) {
			out.push(Buffer.from(chunk));
		}
		return Buffer.concat(out);
	};

	test(`web AES-256-CTR one-chunk == split-chunk across counter wrap`, async (_t) => {
		const web = await importWeb();
		const iv = ctrWrapIv();
		// 4 blocks of data so the counter advances across the 2^64 low-half wrap.
		const plaintext = Buffer.alloc(64, 0x41);
		const oneChunk = await ctrEncryptChunks(web, new Uint8Array(iv), [
			plaintext,
		]);
		const splitChunks = [];
		for (let i = 0; i < plaintext.length; i += 16) {
			splitChunks.push(plaintext.subarray(i, i + 16));
		}
		const split = await ctrEncryptChunks(web, new Uint8Array(iv), splitChunks);
		deepStrictEqual(oneChunk, split);
	});

	test(`web AES-256-CTR encrypt-then-rechunk-decrypt round-trips across wrap`, async (_t) => {
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
		const decStream = join([createReadableStream(rechunked), dec]);
		for await (const chunk of decStream) {
			out.push(Buffer.from(chunk));
		}
		deepStrictEqual(Buffer.concat(out), plaintext);
	});

	test(`node and web AES-256-CTR produce identical ciphertext across counter wrap`, async (_t) => {
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
	test(`web AES-256-CTR encrypt honors explicit maxInputSize`, async (_t) => {
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
			const stream = join([createReadableStream("a".repeat(200)), enc]);
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

	// Node parity: CTR has no default input limit (only AEAD modes default to
	// 64MB), so one byte past 64MB must encrypt when maxInputSize is unset.
	test(`web AES-256-CTR encrypt has no default maxInputSize`, async (_t) => {
		const web = await importWeb();
		const enc = await web.encryptStream({ key, algorithm: "AES-256-CTR" });
		let outputSize = 0;
		const stream = join([
			createReadableStream([new Uint8Array(64 * 1024 * 1024 + 1)]),
			enc,
		]);
		for await (const chunk of stream) {
			outputSize += chunk.byteLength;
		}
		strictEqual(outputSize, 64 * 1024 * 1024 + 1);
	});

	// maxInputSize is a plain number: Infinity means "no limit" and fractional
	// limits compare numerically (BigInt() throws on both).
	test(`encryptStream accepts maxInputSize: Infinity`, async (_t) => {
		const enc = await encryptStream({ key, maxInputSize: Infinity });
		let outputSize = 0;
		for await (const chunk of join([createReadableStream(["abc"]), enc])) {
			outputSize += chunk.byteLength;
		}
		strictEqual(outputSize, 3);
	});

	test(`encryptStream compares a fractional maxInputSize numerically`, async (_t) => {
		const enc = await encryptStream({ key, maxInputSize: 2.5 });
		await rejects(
			pipeline([createReadableStream(["abc"]), enc]),
			/Encryption input exceeds maxInputSize \(2\.5 bytes\)/,
		);
	});

	// *** resultKey honored in result() (node + web, all encrypt modes) *** //
	const resultKeyHonored = async (mod, algorithm) => {
		const enc = await mod.encryptStream({
			key,
			algorithm,
			resultKey: "cipher",
		});
		const encStream = join([createReadableStream("resultKey payload"), enc]);
		for await (const _chunk of encStream) {
			// drain
		}
		const { key: resultKey, value } = enc.result();
		strictEqual(resultKey, "cipher");
		strictEqual(value.algorithm, algorithm);
	};

	test(`node encryptStream result() honors resultKey (AES-256-GCM)`, async (_t) => {
		await resultKeyHonored({ encryptStream }, "AES-256-GCM");
	});

	test(`node encryptStream result() honors resultKey (AES-256-CTR)`, async (_t) => {
		await resultKeyHonored({ encryptStream }, "AES-256-CTR");
	});

	test(`node encryptStream result() honors resultKey (CHACHA20-POLY1305)`, async (_t) => {
		await resultKeyHonored({ encryptStream }, "CHACHA20-POLY1305");
	});

	test(`web encryptStream result() honors resultKey (AES-256-GCM)`, async (_t) => {
		const web = await importWeb();
		await resultKeyHonored(web, "AES-256-GCM");
	});

	test(`web encryptStream result() honors resultKey (AES-256-CTR)`, async (_t) => {
		const web = await importWeb();
		await resultKeyHonored(web, "AES-256-CTR");
	});

	test(`web encryptStream result() honors resultKey (CHACHA20-POLY1305)`, async (_t) => {
		const web = await importWeb();
		await resultKeyHonored(web, "CHACHA20-POLY1305");
	});

	test(`encryptStream result() defaults resultKey to 'encrypt'`, async (_t) => {
		const enc = await encryptStream({ key });
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
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			encryptedChunks.push(Buffer.from(chunk));
		}
		const { iv, authTag } = enc.result().value;
		const dec = await mod.decryptStream({ key: k, iv, authTag, algorithm });
		const decryptedChunks = [];
		const decStream = join([createReadableStream(encryptedChunks), dec]);
		for await (const chunk of decStream) {
			decryptedChunks.push(Buffer.from(chunk));
		}
		strictEqual(Buffer.concat(decryptedChunks).toString("utf8"), input);
	};

	test(`node 128-bit key usable with AES-128-GCM`, async (_t) => {
		await key128Usable(
			{ encryptStream, decryptStream, generateEncryptionKey },
			"AES-128-GCM",
		);
	});

	test(`node 128-bit key usable with AES-128-CTR`, async (_t) => {
		await key128Usable(
			{ encryptStream, decryptStream, generateEncryptionKey },
			"AES-128-CTR",
		);
	});

	test(`web 128-bit key usable with AES-128-GCM`, async (_t) => {
		const web = await importWeb();
		await key128Usable(web, "AES-128-GCM");
	});

	test(`web 128-bit key usable with AES-128-CTR`, async (_t) => {
		const web = await importWeb();
		await key128Usable(web, "AES-128-CTR");
	});

	// *** Mutation-kill: exact validation error messages *** //
	test(`validateKey emits exact bits/got in 256-bit message`, async (_t) => {
		await rejects(
			() => encryptStream({ key: randomBytes(16) }),
			/Encryption key must be 32 bytes \(256 bits\), got 16/,
		);
	});

	test(`validateKey emits exact bits/got in 128-bit message`, async (_t) => {
		await rejects(
			() => encryptStream({ key: randomBytes(8), algorithm: "AES-128-GCM" }),
			/Encryption key must be 16 bytes \(128 bits\), got 8/,
		);
	});

	test(`validateKey reports "got 0" (not undefined) for missing key`, async (_t) => {
		// Kills `key?.length ?? 0` -> `key?.length && 0` (would yield "got undefined").
		await rejects(
			() => encryptStream({}),
			/Encryption key must be 32 bytes \(256 bits\), got 0/,
		);
	});

	test(`validateAuthTag rejects 15-byte authTag with exact message`, async (_t) => {
		// Kills `authTag.length !== 16` -> `false`; a non-falsy wrong-length tag
		// must still be rejected with the validation message (not a deeper error).
		await rejects(
			() =>
				decryptStream({ key, iv: randomBytes(12), authTag: randomBytes(15) }),
			/authTag for AES-256-GCM must be 16 bytes, got 15/,
		);
	});

	// *** Mutation-kill: encrypt maxInputSize message + boundary *** //
	test(`encrypt maxInputSize message reports the explicit limit`, async (_t) => {
		// Kills `maxInputSize ?? DEFAULT` -> `maxInputSize && DEFAULT` (reportedLimit
		// would become 67108864 instead of 100).
		const enc = await encryptStream({ key, maxInputSize: 100 });
		let message = "";
		try {
			await pipeline([createReadableStream("a".repeat(200)), enc]);
		} catch (e) {
			message = e.message;
		}
		strictEqual(message.includes("(100 bytes)"), true);
	});

	test(`encrypt maxInputSize boundary: exactly the limit succeeds`, async (_t) => {
		// Kills `inputSize > limit` -> `inputSize >= limit` on the encrypt path.
		const enc = await encryptStream({ key, maxInputSize: 100 });
		await pipeline([createReadableStream("a".repeat(100)), enc]);
		strictEqual(enc.result().value.algorithm, "AES-256-GCM");
	});

	test(`encrypt maxInputSize boundary: one over the limit fails`, async (_t) => {
		const enc = await encryptStream({ key, maxInputSize: 100 });
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

	nodeTest(
		`AES-256-CTR encrypt allows >64MB with no maxInputSize`,
		async (_t) => {
			// Kills ctrAlgorithms mutations and `usingCtrDefaultCeiling = false`: any of
			// those drops AES-256-CTR back to the 64MB default and would reject this.
			const enc = await encryptStream({ key, algorithm: "AES-256-CTR" });
			await drain(join([createReadableStream([big65()]), enc]));
			strictEqual(enc.result().value.algorithm, "AES-256-CTR");
		},
	);

	nodeTest(
		`AES-128-CTR encrypt allows >64MB with no maxInputSize`,
		async (_t) => {
			// Kills the "AES-128-CTR" element of ctrAlgorithms (empty/blanked array).
			const k16 = randomBytes(16);
			const enc = await encryptStream({ key: k16, algorithm: "AES-128-CTR" });
			await drain(join([createReadableStream([big65()]), enc]));
			strictEqual(enc.result().value.algorithm, "AES-128-CTR");
		},
	);

	test(`AES-256-GCM encrypt rejects >64MB at the 64MB default`, async (_t) => {
		// Kills `if (maxInputSize != null) -> true`, `else if (...) -> true`, removal
		// of the DEFAULT_MAX_INPUT_SIZE assignment: each would let >64MB AEAD pass.
		const enc = await encryptStream({ key });
		let threw = false;
		let message = "";
		try {
			await drain(join([createReadableStream([big65()]), enc]));
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
		const enc = await encryptStream({ key });
		const chunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			chunks.push(Buffer.from(chunk));
		}
		const { iv, authTag } = enc.result().value;
		return { ciphertext: Buffer.concat(chunks), iv, authTag };
	};

	test(`AEAD decrypt maxInputSize boundary: exactly the limit succeeds`, async (_t) => {
		// Kills `inputSize > maxInputSize` -> `>=` in aeadDecryptStream.
		const { ciphertext, iv, authTag } = await gcmCiphertext("boundary in");
		const dec = await decryptStream({
			key,
			iv,
			authTag,
			maxInputSize: ciphertext.length,
		});
		const out = [];
		const decStream = join([createReadableStream([ciphertext]), dec]);
		for await (const chunk of decStream) {
			out.push(Buffer.from(chunk));
		}
		strictEqual(Buffer.concat(out).toString("utf8"), "boundary in");
	});

	test(`AEAD decrypt maxInputSize boundary: one over the limit fails`, async (_t) => {
		const { ciphertext, iv, authTag } = await gcmCiphertext("boundary in");
		const dec = await decryptStream({
			key,
			iv,
			authTag,
			maxInputSize: ciphertext.length - 1,
		});
		let threw = false;
		try {
			const decStream = join([createReadableStream([ciphertext]), dec]);
			for await (const _chunk of decStream) {
				// should fail
			}
		} catch (_e) {
			threw = true;
		}
		strictEqual(threw, true);
	});

	test(`AEAD decrypt within maxOutputSize succeeds (no false error)`, async (_t) => {
		// Kills `maxOutputSize != null && X` -> `|| X` and `&& true`: those would
		// reject a plaintext that is within the configured limit.
		const input = "exact output";
		const { ciphertext, iv, authTag } = await gcmCiphertext(input);
		const dec = await decryptStream({
			key,
			iv,
			authTag,
			maxOutputSize: input.length,
		});
		const out = [];
		const decStream = join([createReadableStream([ciphertext]), dec]);
		for await (const chunk of decStream) {
			out.push(Buffer.from(chunk));
		}
		strictEqual(Buffer.concat(out).toString("utf8"), input);
	});

	test(`AEAD decrypt maxOutputSize boundary: one under the limit fails`, async (_t) => {
		// Kills `plaintext.length > maxOutputSize` -> `>=` in aeadDecryptStream.
		const input = "exact output";
		const { ciphertext, iv, authTag } = await gcmCiphertext(input);
		const dec = await decryptStream({
			key,
			iv,
			authTag,
			maxOutputSize: input.length - 1,
		});
		let threw = false;
		let message = "";
		try {
			const decStream = join([createReadableStream([ciphertext]), dec]);
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
		const enc = await encryptStream({ key, algorithm: "AES-256-CTR" });
		const chunks = [];
		const encStream = join([createReadableStream(input), enc]);
		for await (const chunk of encStream) {
			chunks.push(Buffer.from(chunk));
		}
		const { iv } = enc.result().value;
		return { chunks, iv };
	};

	test(`CTR decrypt maxOutputSize over-limit fails with message`, async (_t) => {
		// Kills `if (chunk !== null) -> false`, `if (maxOutputSize != null) -> false`,
		// `outputSize += -> -=`, and `outputSize > maxOutputSize -> false`: each would
		// suppress the over-limit error.
		const { chunks, iv } = await ctrCiphertext("a".repeat(500));
		const dec = await decryptStream({
			key,
			iv,
			algorithm: "AES-256-CTR",
			maxOutputSize: 100,
		});
		let threw = false;
		let message = "";
		let emitted = 0;
		try {
			const decStream = join([createReadableStream(chunks), dec]);
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

	test(`CTR decrypt maxOutputSize boundary: exactly the limit succeeds`, async (_t) => {
		// Kills `outputSize > maxOutputSize` -> `>=` on the CTR decrypt path:
		// output length equal to the limit must NOT error.
		const input = "a".repeat(100);
		const { chunks, iv } = await ctrCiphertext(input);
		const dec = await decryptStream({
			key,
			iv,
			algorithm: "AES-256-CTR",
			maxOutputSize: 100,
		});
		const out = [];
		const decStream = join([createReadableStream(chunks), dec]);
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
	nodeTest(
		`encryptStream forwards streamOptions.highWaterMark to the cipher stream`,
		async (_t) => {
			const enc = await encryptStream({ key }, { highWaterMark: 7 });
			strictEqual(
				enc.readableHighWaterMark,
				7,
				"highWaterMark from streamOptions must reach createCipheriv",
			);
			strictEqual(enc.writableHighWaterMark, 7);
		},
	);

	test(`encryptStream cipher uses the default highWaterMark when none supplied`, async (_t) => {
		// Guards the above: the 7 above is genuinely from streamOptions, not a default.
		const enc = await encryptStream({ key });
		strictEqual(enc.readableHighWaterMark !== 7, true);
	});

	// *** Mutation-kill: maxOutputSize == null means NO ceiling (explicit null) ***
	// `maxOutputSize != null && ...` must short-circuit to false when null. The
	// `true && ...` mutant degrades to `length > null` (=> length > 0) and would
	// wrongly reject any non-empty plaintext.
	test(`AEAD decrypt with maxOutputSize: null imposes no output ceiling`, async (_t) => {
		const input = "no ceiling please";
		const { ciphertext, iv, authTag } = await gcmCiphertext(input);
		const dec = await decryptStream({ key, iv, authTag, maxOutputSize: null });
		const out = [];
		const decStream = join([createReadableStream([ciphertext]), dec]);
		for await (const chunk of decStream) {
			out.push(Buffer.from(chunk));
		}
		strictEqual(Buffer.concat(out).toString("utf8"), input);
	});

	test(`CTR decrypt with maxOutputSize: null imposes no output ceiling`, async (_t) => {
		// Kills `if (maxOutputSize != null) -> if (true)` on the CTR push override:
		// with null and the mutant, `outputSize > null` (=> > 0) would destroy the
		// stream on the first non-empty chunk.
		const input = "a".repeat(500);
		const { chunks, iv } = await ctrCiphertext(input);
		const dec = await decryptStream({
			key,
			iv,
			algorithm: "AES-256-CTR",
			maxOutputSize: null,
		});
		const out = [];
		const decStream = join([createReadableStream(chunks), dec]);
		for await (const chunk of decStream) {
			out.push(Buffer.from(chunk));
		}
		strictEqual(Buffer.concat(out).toString("utf8"), input);
	});

	test(`CTR decrypt maxOutputSize accumulates across chunks`, async (_t) => {
		// Reinforces the `outputSize += chunk.length` accumulation (kills `-=`):
		// many small chunks whose sum exceeds the limit must error.
		const { chunks, iv } = await ctrCiphertext("a".repeat(64));
		const rechunked = [];
		const ct = Buffer.concat(chunks);
		for (let i = 0; i < ct.length; i += 8) {
			rechunked.push(ct.subarray(i, i + 8));
		}
		const dec = await decryptStream({
			key,
			iv,
			algorithm: "AES-256-CTR",
			maxOutputSize: 32,
		});
		let threw = false;
		try {
			const decStream = join([createReadableStream(rechunked), dec]);
			for await (const _chunk of decStream) {
				// should fail once cumulative output passes 32
			}
		} catch (_e) {
			threw = true;
		}
		strictEqual(threw, true);
	});

	test(`web decryptStream reports "got 0" for a missing IV or authTag`, async () => {
		const web = await importWeb();
		await rejects(
			web.decryptStream({ key, authTag: new Uint8Array(16) }),
			/IV for AES-256-GCM must be 12 bytes, got 0/,
		);
		await rejects(
			web.decryptStream({ key, iv: new Uint8Array(12) }),
			/authTag for AES-256-GCM must be 16 bytes, got 0/,
		);
		await rejects(
			web.decryptStream({
				key,
				iv: new Uint8Array(12),
				authTag: new Uint8Array(15),
			}),
			/authTag for AES-256-GCM must be 16 bytes, got 15/,
		);
	});

	// *** browser build: defaults, AAD binding, messages, boundaries, counters *** //
	const collect = async (streams) => {
		const chunks = [];
		for await (const chunk of join(streams)) chunks.push(chunk);
		return chunks;
	};

	test(`web encryptStream defaults to AES-256-GCM under the "encrypt" result key`, async () => {
		const web = await importWeb();
		const enc = await web.encryptStream({ key });
		await pipeline([createReadableStream("x"), enc]);
		const { key: resultKey, value } = enc.result();
		strictEqual(resultKey, "encrypt");
		strictEqual(value.algorithm, "AES-256-GCM");
		strictEqual(value.iv.byteLength, 12);
		strictEqual(value.authTag.byteLength, 16);
		const ctr = await web.encryptStream({ key, algorithm: "AES-256-CTR" });
		await pipeline([createReadableStream("x"), ctr]);
		strictEqual(ctr.result().key, "encrypt");
		strictEqual(ctr.result().value.algorithm, "AES-256-CTR");
	});

	test(`web AEAD modes bind the AAD (AES-128-GCM, CHACHA20-POLY1305)`, async () => {
		const web = await importWeb();
		const aad = new TextEncoder().encode("hdr");
		for (const [algorithm, size] of [
			["AES-128-GCM", 16],
			["CHACHA20-POLY1305", 32],
		]) {
			const k = key.subarray(0, size);
			const enc = await web.encryptStream({ key: k, algorithm, aad });
			const chunks = await collect([createReadableStream("bound"), enc]);
			const { iv, authTag } = enc.result().value;
			strictEqual(enc.result().value.algorithm, algorithm);
			strictEqual(enc.result().key, "encrypt");
			const dec = await web.decryptStream({
				key: k,
				iv,
				authTag,
				algorithm,
				aad,
			});
			strictEqual(
				Buffer.concat(
					await collect([createReadableStream(chunks), dec]),
				).toString(),
				"bound",
			);
			// Dropping the AAD must fail authentication.
			const bad = await web.decryptStream({ key: k, iv, authTag, algorithm });
			await rejects(collect([createReadableStream(chunks), bad]));
		}
	});

	test(`web validation messages name the algorithm, size and bits`, async () => {
		const web = await importWeb();
		await rejects(
			web.encryptStream({ key: new Uint8Array(16) }),
			/Encryption key must be 32 bytes \(256 bits\), got 16/,
		);
		await rejects(
			web.decryptStream({
				key,
				iv: new Uint8Array(12),
				algorithm: "CHACHA20-POLY1305",
			}),
			/authTag for CHACHA20-POLY1305 must be 16 bytes, got 0/,
		);
		await rejects(
			web.encryptStream({ key, algorithm: "CHACHA20-POLY1305", aad: "no" }),
			/aad must be a Uint8Array/,
		);
		await rejects(
			web.encryptStream({
				key,
				algorithm: "AES-256-CTR",
				aad: new Uint8Array(1),
			}),
			/aad is not supported for AES-256-CTR/,
		);
	});

	test(`web maxInputSize / maxOutputSize hold at the exact boundary`, async () => {
		const web = await importWeb();
		for (const algorithm of [
			"AES-256-CTR",
			"CHACHA20-POLY1305",
			"AES-256-GCM",
		]) {
			const ok8 = await web.encryptStream({ key, algorithm, maxInputSize: 8 });
			const chunks = await collect([createReadableStream("12345678"), ok8]);
			const { iv, authTag } = ok8.result().value;
			const over = await web.encryptStream({ key, algorithm, maxInputSize: 8 });
			await rejects(
				collect([createReadableStream("123456789"), over]),
				/maxInputSize/,
			);
			const dec8 = await web.decryptStream({
				key,
				algorithm,
				iv,
				authTag,
				maxOutputSize: 8,
			});
			strictEqual(
				Buffer.concat(
					await collect([createReadableStream(chunks), dec8]),
				).toString(),
				"12345678",
			);
			const dec7 = await web.decryptStream({
				key,
				algorithm,
				iv,
				authTag,
				maxOutputSize: 7,
			});
			await rejects(
				collect([createReadableStream(chunks), dec7]),
				/maxOutputSize \(7 bytes\)/,
			);
			// AEAD decryption buffers the ciphertext, so its input bound is exact too.
			if (algorithm !== "AES-256-CTR") {
				const inputBytes = chunks.reduce((n, c) => n + c.byteLength, 0);
				const in8 = await web.decryptStream({
					key,
					algorithm,
					iv,
					authTag,
					maxInputSize: inputBytes,
				});
				strictEqual(
					Buffer.concat(
						await collect([createReadableStream(chunks), in8]),
					).toString(),
					"12345678",
				);
				const in7 = await web.decryptStream({
					key,
					algorithm,
					iv,
					authTag,
					maxInputSize: inputBytes - 1,
				});
				await rejects(
					collect([createReadableStream(chunks), in7]),
					/maxInputSize/,
				);
			}
		}
	});

	test(`web AES-256-CTR ciphertext matches node:crypto across a counter wrap`, async () => {
		const web = await importWeb();
		const iv = new Uint8Array(16).fill(0xff); // the very next block wraps the counter
		const input = ["abcdefghij", "klmnopqrstuvwxyz0123", "456789"]; // 36 bytes, 3 blocks
		const enc = await web.encryptStream({ key, algorithm: "AES-256-CTR", iv });
		const chunks = await collect([createReadableStream(input), enc]);
		const cipher = createCipheriv("aes-256-ctr", key, iv);
		const expected = Buffer.concat([
			cipher.update(input.join("")),
			cipher.final(),
		]);
		deepStrictEqual(Buffer.concat(chunks), expected);
	});
	// *** Keys must be bytes: a 32-char string has length 32 but is not a raw
	// 256-bit key (node used to accept it; the browser build never did). *** //
	test(`encryptStream rejects a 32-character string key`, async (_t) => {
		await rejects(async () => encryptStream({ key: "k".repeat(32) }), {
			message: "Encryption key must be 32 bytes (256 bits), got 0",
		});
	});

	test(`decryptStream rejects a 32-character string key`, async (_t) => {
		await rejects(
			async () =>
				decryptStream({
					key: "k".repeat(32),
					iv: randomBytes(12),
					authTag: randomBytes(16),
				}),
			{ message: "Encryption key must be 32 bytes (256 bits), got 0" },
		);
	});

	// *** Inherited Object.prototype names are not algorithms *** //
	for (const algorithm of ["toString", "__proto__"]) {
		test(`encryptStream rejects inherited name ${algorithm} as an algorithm`, async (_t) => {
			await rejects(async () => encryptStream({ key, algorithm }), {
				message: `Unsupported algorithm: ${algorithm}`,
			});
		});

		test(`decryptStream rejects inherited name ${algorithm} as an algorithm`, async (_t) => {
			await rejects(
				async () => decryptStream({ key, iv: randomBytes(12), algorithm }),
				{ message: `Unsupported algorithm: ${algorithm}` },
			);
		});
	}

	// *** The limit error must not steer users to unauthenticated CTR *** //
	for (const algorithm of ["AES-256-GCM", "AES-256-CTR", "CHACHA20-POLY1305"]) {
		test(`${algorithm} maxInputSize error suggests raising the limit or chunking`, async (_t) => {
			const enc = await encryptStream({ key, algorithm, maxInputSize: 2 });
			await rejects(pipeline([createReadableStream(["abc"]), enc]), {
				message:
					"Encryption input exceeds maxInputSize (2 bytes). Raise maxInputSize or encrypt the data as separate smaller messages.",
			});
		});
	}
	// *** aad: null means "no aad", exactly like omitting it *** //
	test(`aad: null round-trips like no aad`, async (_t) => {
		const enc = await encryptStream({ key, aad: null });
		const ciphertext = [];
		for await (const chunk of join([createReadableStream(["abc"]), enc])) {
			ciphertext.push(chunk);
		}
		const { iv, authTag } = enc.result().value;
		const dec = await decryptStream({ key, iv, authTag, aad: null });
		const plaintext = [];
		for await (const chunk of join([createReadableStream(ciphertext), dec])) {
			plaintext.push(chunk);
		}
		strictEqual(Buffer.concat(plaintext).toString(), "abc");
	});

	test(`aad: null is accepted for AES-256-CTR`, async (_t) => {
		const enc = await encryptStream({
			key,
			algorithm: "AES-256-CTR",
			aad: null,
		});
		const { iv } = enc.result().value;
		const dec = await decryptStream({
			key,
			algorithm: "AES-256-CTR",
			iv,
			aad: null,
		});
		strictEqual(typeof dec, "object");
	});

	// *** Major: both builds return a Promise; validation rejects, never throws *** //
	test(`encryptStream and decryptStream return a Promise`, async (_t) => {
		const enc = encryptStream({ key });
		strictEqual(enc instanceof Promise, true);
		await enc;
		const dec = decryptStream({
			key,
			iv: randomBytes(12),
			authTag: randomBytes(16),
		});
		strictEqual(dec instanceof Promise, true);
		await dec;
	});

	test(`encryptStream and decryptStream reject (not throw) on invalid options`, async (_t) => {
		await rejects(encryptStream({ key, algorithm: "INVALID" }), {
			message: "Unsupported algorithm: INVALID",
		});
		await rejects(decryptStream({ key, iv: randomBytes(12) }), {
			message: "authTag for AES-256-GCM must be 16 bytes, got 0",
		});
	});

	// *** Major: named exports only *** //
	test(`module has only named exports`, async (_t) => {
		const mod = await import("@datastream/encrypt");
		deepStrictEqual(Object.keys(mod).sort(), [
			"decryptStream",
			"encryptStream",
			"generateEncryptionKey",
		]);
	});

	// *** Major: limit errors are RangeError *** //
	const isRangeError = (message) => (e) =>
		e instanceof RangeError && e.message === message;

	for (const algorithm of ["AES-256-GCM", "AES-256-CTR", "CHACHA20-POLY1305"]) {
		test(`${algorithm} encrypt maxInputSize error is a RangeError`, async (_t) => {
			const enc = await encryptStream({ key, algorithm, maxInputSize: 2 });
			await rejects(
				pipeline([createReadableStream(["abc"]), enc]),
				isRangeError(
					"Encryption input exceeds maxInputSize (2 bytes). Raise maxInputSize or encrypt the data as separate smaller messages.",
				),
			);
		});
	}

	for (const algorithm of ["AES-256-GCM", "CHACHA20-POLY1305"]) {
		test(`${algorithm} decrypt maxInputSize error is a RangeError`, async (_t) => {
			const dec = await decryptStream({
				key,
				algorithm,
				iv: randomBytes(12),
				authTag: randomBytes(16),
				maxInputSize: 2,
			});
			await rejects(
				pipeline([createReadableStream([new Uint8Array(3)]), dec]),
				isRangeError("Decryption input exceeds maxInputSize (2 bytes)"),
			);
		});

		test(`${algorithm} decrypt maxOutputSize error is a RangeError`, async (_t) => {
			const enc = await encryptStream({ key, algorithm });
			const ciphertext = [];
			for await (const chunk of join([createReadableStream(["abc"]), enc])) {
				ciphertext.push(chunk);
			}
			const { iv, authTag } = enc.result().value;
			const dec = await decryptStream({
				key,
				algorithm,
				iv,
				authTag,
				maxOutputSize: 2,
			});
			await rejects(
				pipeline([createReadableStream(ciphertext), dec]),
				isRangeError("Decryption output exceeds maxOutputSize (2 bytes)"),
			);
		});
	}

	test(`AES-256-CTR decrypt maxOutputSize error is a RangeError`, async (_t) => {
		const dec = await decryptStream({
			key,
			algorithm: "AES-256-CTR",
			iv: randomBytes(16),
			maxOutputSize: 2,
		});
		await rejects(
			pipeline([createReadableStream([new Uint8Array(3)]), dec]),
			isRangeError("Decryption output exceeds maxOutputSize (2 bytes)"),
		);
	});

	// *** Major: maxInputSize null means unlimited (undefined keeps the 64MB default) *** //
	test(`AES-256-GCM encrypt maxInputSize: null lifts the 64MB default`, async (_t) => {
		const enc = await encryptStream({ key, maxInputSize: null });
		let outputSize = 0;
		for await (const chunk of join([createReadableStream([big65()]), enc])) {
			outputSize += chunk.byteLength;
		}
		strictEqual(outputSize, 65 * 1024 * 1024);
	});

	for (const algorithm of ["AES-256-GCM", "CHACHA20-POLY1305"]) {
		test(`${algorithm} decrypt maxInputSize: null lifts the 64MB default`, async (_t) => {
			// A wrong authTag reaches verification (not the input limit) only when
			// the whole 65MB was buffered.
			const dec = await decryptStream({
				key,
				algorithm,
				iv: randomBytes(12),
				authTag: randomBytes(16),
				maxInputSize: null,
			});
			await rejects(
				pipeline([createReadableStream([big65()]), dec]),
				(e) => !(e instanceof RangeError),
			);
		});
	}

	// *** IVs must be bytes in both builds (node used to accept a string IV) *** //
	test(`encryptStream rejects a 12-character string IV`, async (_t) => {
		await rejects(encryptStream({ key, iv: "i".repeat(12) }), {
			message: "IV for AES-256-GCM must be 12 bytes, got 0",
		});
	});

	// ArrayBuffer has the right byteLength but is not a Uint8Array.
	test(`encryptStream rejects an ArrayBuffer key and IV`, async (_t) => {
		await rejects(encryptStream({ key: new ArrayBuffer(32) }), {
			message: "Encryption key must be 32 bytes (256 bits), got 32",
		});
		await rejects(encryptStream({ key, iv: new ArrayBuffer(12) }), {
			message: "IV for AES-256-GCM must be 12 bytes, got 12",
		});
	});
});
