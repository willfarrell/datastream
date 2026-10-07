import { bench, suite } from "node:bench";
import { randomBytes } from "node:crypto";
import { createReadableStream, pipeline } from "@datastream/core";
import { decryptStream, encryptStream } from "@datastream/encrypt";

// -- Data generators --

const OPS = 10;
const options = { warmup: 2, samples: 30 };

const smallBuffer = randomBytes(1_024); // 1KB
const bigBuffer = randomBytes(1_024 * 1_024); // 1MB
const key = randomBytes(32);

// -- Tests --

suite("encryptStream AES-256-GCM", () => {
	bench("1KB", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const enc = await encryptStream({ key });
			const streams = [createReadableStream(smallBuffer), enc];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench("1MB", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const enc = await encryptStream({ key });
			const streams = [createReadableStream(bigBuffer), enc];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("encryptStream AES-256-CTR", () => {
	bench("1KB", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const enc = await encryptStream({ key, algorithm: "AES-256-CTR" });
			const streams = [createReadableStream(smallBuffer), enc];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench("1MB", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const enc = await encryptStream({ key, algorithm: "AES-256-CTR" });
			const streams = [createReadableStream(bigBuffer), enc];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("encryptStream CHACHA20-POLY1305", () => {
	bench("1KB", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const enc = await encryptStream({ key, algorithm: "CHACHA20-POLY1305" });
			const streams = [createReadableStream(smallBuffer), enc];
			await pipeline(streams);
		}
		b.end(OPS);
	});

	bench("1MB", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const enc = await encryptStream({ key, algorithm: "CHACHA20-POLY1305" });
			const streams = [createReadableStream(bigBuffer), enc];
			await pipeline(streams);
		}
		b.end(OPS);
	});
});

suite("roundtrip comparison 1MB", () => {
	for (const algorithm of ["AES-256-GCM", "AES-256-CTR", "CHACHA20-POLY1305"]) {
		bench(algorithm, options, async (b) => {
			b.start();
			for (let op = 0; op < OPS; op++) {
				const enc = await encryptStream({ key, algorithm });
				const encryptedChunks = [];
				const encStream = createReadableStream(bigBuffer).pipe(enc);
				for await (const chunk of encStream) {
					encryptedChunks.push(chunk);
				}
				const { iv, authTag } = enc.result().value;

				const dec = await decryptStream({ key, iv, authTag, algorithm });
				const decStream = createReadableStream(encryptedChunks).pipe(dec);
				for await (const _chunk of decStream) {
					// consume
				}
			}
			b.end(OPS);
		});
	}
});
