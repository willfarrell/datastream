import test from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import { digestStream } from "@datastream/digest";
import { Bench } from "tinybench";

// -- Data generators --

const time = 5_000;

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

test("perf: digestStream SHA256", async () => {
	const bench = new Bench({ name: "digestStream SHA256", time });

	bench.add("1MB string", async () => {
		const digest = digestStream({ algorithm: "SHA256" });
		const streams = [createReadableStream(bigString), digest];
		await pipeline(streams);
		digest.result();
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: digestStream SHA384", async () => {
	const bench = new Bench({ name: "digestStream SHA384", time });

	bench.add("1MB string", async () => {
		const digest = digestStream({ algorithm: "SHA384" });
		const streams = [createReadableStream(bigString), digest];
		await pipeline(streams);
		digest.result();
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: digestStream SHA512", async () => {
	const bench = new Bench({ name: "digestStream SHA512", time });

	bench.add("1MB string", async () => {
		const digest = digestStream({ algorithm: "SHA512" });
		const streams = [createReadableStream(bigString), digest];
		await pipeline(streams);
		digest.result();
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: digestStream algorithm comparison", async () => {
	const bench = new Bench({ name: "digestStream comparison", time });

	for (const algorithm of ["SHA256", "SHA384", "SHA512"]) {
		bench.add(`1MB ${algorithm}`, async () => {
			const digest = digestStream({ algorithm });
			const streams = [createReadableStream(bigString), digest];
			await pipeline(streams);
			digest.result();
		});
	}

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
