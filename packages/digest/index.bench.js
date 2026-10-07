import { bench, suite } from "node:bench";
import { createReadableStream, pipeline } from "@datastream/core";
import { digestStream } from "@datastream/digest";

// -- Data generators --

const OPS = 10;
const options = { warmup: 2, samples: 30 };

const generateString = (size) => {
	const chars =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	let result = "";
	for (let i = 0; i < size; i++) {
		result += chars[i % chars.length];
	}
	return result;
};

const bigString = generateString(1_024 * 1_024); // 1MB

// -- Tests --

suite("digestStream SHA256", () => {
	bench("1MB string", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const digest = digestStream({ algorithm: "SHA256" });
			const streams = [createReadableStream(bigString), digest];
			await pipeline(streams);
			digest.result();
		}
		b.end(OPS);
	});
});

suite("digestStream SHA384", () => {
	bench("1MB string", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const digest = digestStream({ algorithm: "SHA384" });
			const streams = [createReadableStream(bigString), digest];
			await pipeline(streams);
			digest.result();
		}
		b.end(OPS);
	});
});

suite("digestStream SHA512", () => {
	bench("1MB string", options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			const digest = digestStream({ algorithm: "SHA512" });
			const streams = [createReadableStream(bigString), digest];
			await pipeline(streams);
			digest.result();
		}
		b.end(OPS);
	});
});

suite("digestStream comparison", () => {
	for (const algorithm of ["SHA256", "SHA384", "SHA512"]) {
		bench(`1MB ${algorithm}`, options, async (b) => {
			b.start();
			for (let op = 0; op < OPS; op++) {
				const digest = digestStream({ algorithm });
				const streams = [createReadableStream(bigString), digest];
				await pipeline(streams);
				digest.result();
			}
			b.end(OPS);
		});
	}
});
