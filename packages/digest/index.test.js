import { strictEqual } from "node:assert";
import test from "node:test";

import { createReadableStream, pipeline } from "@datastream/core";

import digestDefault, { digestStream } from "@datastream/digest";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: digestStream should calculate digest`, async (_t) => {
	const streams = [
		createReadableStream("1,2,3,4"),
		await digestStream({ algorithm: "SHA2-256" }),
	];
	const result = await pipeline(streams);

	const { key, value } = streams[1].result();

	strictEqual(key, "digest");
	strictEqual(
		value,
		"SHA2-256:37db36876b9ccaaa88394679f019c3435af9320dea117e867003840317870e25",
	);
	strictEqual(
		result.digest,
		"SHA2-256:37db36876b9ccaaa88394679f019c3435af9320dea117e867003840317870e25",
	);
});
test(`${variant}: digestStream should calculate digest from chunks`, async (_t) => {
	const streams = [
		createReadableStream(["1,", "2,", "3,", "4"]),
		await digestStream({ algorithm: "SHA2-256" }),
	];
	const result = await pipeline(streams);

	const { key, value } = streams[1].result();

	strictEqual(key, "digest");
	strictEqual(
		value,
		"SHA2-256:37db36876b9ccaaa88394679f019c3435af9320dea117e867003840317870e25",
	);
	strictEqual(
		result.digest,
		"SHA2-256:37db36876b9ccaaa88394679f019c3435af9320dea117e867003840317870e25",
	);
});

test(`${variant}: digestStream should use native algorithm name`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA256" }),
	];
	const result = await pipeline(streams);

	strictEqual(result.digest.startsWith("SHA256:"), true);
});

test(`${variant}: digestStream should use custom resultKey`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA256", resultKey: "checksum" }),
	];
	const result = await pipeline(streams);

	const { key } = streams[1].result();
	strictEqual(key, "checksum");
	strictEqual(typeof result.checksum, "string");
});

// *** algorithm variants *** //
test(`${variant}: digestStream should calculate SHA2-384`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA2-384" }),
	];
	const result = await pipeline(streams);
	strictEqual(result.digest.startsWith("SHA2-384:"), true);
});

test(`${variant}: digestStream should calculate SHA2-512`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA2-512" }),
	];
	const result = await pipeline(streams);
	strictEqual(result.digest.startsWith("SHA2-512:"), true);
});

test(`${variant}: digestStream should calculate SHA3-256`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA3-256" }),
	];
	const result = await pipeline(streams);
	strictEqual(result.digest.startsWith("SHA3-256:"), true);
});

test(`${variant}: digestStream should calculate SHA3-384`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA3-384" }),
	];
	const result = await pipeline(streams);
	strictEqual(result.digest.startsWith("SHA3-384:"), true);
});

test(`${variant}: digestStream should calculate SHA3-512`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA3-512" }),
	];
	const result = await pipeline(streams);
	strictEqual(result.digest.startsWith("SHA3-512:"), true);
});

// *** default export *** //
test(`${variant}: default export should be digestStream`, (_t) => {
	strictEqual(digestDefault, digestStream);
});
