import { randomBytes } from "node:crypto";
import test from "node:test";
import { createReadableStream } from "@datastream/core";
import { decryptStream, encryptStream } from "@datastream/encrypt";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"Unsupported state or unable to authenticate data",
		"Unsupported algorithm",
	];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

const key = randomBytes(32);

// *** encryptStream roundtrip with random data *** //
test("fuzz encryptStream AES-256-GCM roundtrip", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 10_000 }),
			async (input) => {
				try {
					const enc = encryptStream({ key });
					const encryptedChunks = [];
					const encStream = createReadableStream(input).pipe(enc);
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
					const decrypted = Buffer.concat(decryptedChunks);
					if (!Buffer.from(input).equals(decrypted)) {
						throw new Error("Roundtrip mismatch");
					}
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

test("fuzz encryptStream AES-256-CTR roundtrip", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 10_000 }),
			async (input) => {
				try {
					const enc = encryptStream({
						key,
						algorithm: "AES-256-CTR",
					});
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
					});
					const decStream = createReadableStream(encryptedChunks).pipe(dec);
					const decryptedChunks = [];
					for await (const chunk of decStream) {
						decryptedChunks.push(chunk);
					}
					const decrypted = Buffer.concat(decryptedChunks);
					if (!Buffer.from(input).equals(decrypted)) {
						throw new Error("Roundtrip mismatch");
					}
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

test("fuzz encryptStream CHACHA20-POLY1305 roundtrip", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 0, maxLength: 10_000 }),
			async (input) => {
				try {
					const enc = encryptStream({
						key,
						algorithm: "CHACHA20-POLY1305",
					});
					const encryptedChunks = [];
					const encStream = createReadableStream(input).pipe(enc);
					for await (const chunk of encStream) {
						encryptedChunks.push(chunk);
					}
					const { iv, authTag } = enc.result().value;

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
					const decrypted = Buffer.concat(decryptedChunks);
					if (!Buffer.from(input).equals(decrypted)) {
						throw new Error("Roundtrip mismatch");
					}
				} catch (e) {
					catchError(input, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

test("fuzz decryptStream with wrong key should fail", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.uint8Array({ minLength: 1, maxLength: 1_000 }),
			async (input) => {
				const enc = encryptStream({ key });
				const encryptedChunks = [];
				const encStream = createReadableStream(input).pipe(enc);
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
					if (e.message === "Expected decryption to fail") {
						throw e;
					}
					// Expected: authentication failure
				}
			},
		),
		{
			numRuns: 50,
			verbose: 2,
			examples: [],
		},
	);
});
