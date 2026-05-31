import { rejects, strictEqual, throws } from "node:assert";
import test from "node:test";

import { createReadableStream, pipeline } from "@datastream/core";

import digestDefault, { digestStream } from "@datastream/digest";

const webDigest = await import(
	`file://${new URL("./index.web.js", import.meta.url).pathname}`
);

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

test(`${variant}: digestStream should accept native algorithm name and normalize prefix`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA256" }),
	];
	const result = await pipeline(streams);

	// Native alias "SHA256" must normalize to the canonical "SHA2-256" label so
	// the emitted digest carries the same prefix cross-platform.
	strictEqual(result.digest.startsWith("SHA2-256:"), true);
});

test(`${variant}: digestStream should accept native SHA384 alias and normalize prefix`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA384" }),
	];
	const result = await pipeline(streams);
	// Native alias "SHA384" must normalize to the canonical "SHA2-384" label.
	strictEqual(result.digest.startsWith("SHA2-384:"), true);
});

test(`${variant}: digestStream should accept native SHA512 alias and normalize prefix`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA512" }),
	];
	const result = await pipeline(streams);
	// Native alias "SHA512" must normalize to the canonical "SHA2-512" label.
	strictEqual(result.digest.startsWith("SHA2-512:"), true);
});

test(`${variant}: digestStream native alias matches canonical name (node)`, async (_t) => {
	const aliasStreams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA256" }),
	];
	const canonicalStreams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA2-256" }),
	];
	const aliasResult = await pipeline(aliasStreams);
	const canonicalResult = await pipeline(canonicalStreams);
	strictEqual(aliasResult.digest, canonicalResult.digest);
});

// *** node/web parity: native algorithm names *** //
test(`${variant}: web digestStream should accept native algorithm name (parity)`, async (_t) => {
	const streams = [
		createReadableStream("test"),
		await webDigest.digestStream({ algorithm: "SHA256" }),
	];
	const result = await pipeline(streams);
	// Web must accept the native alias (not throw) and emit the canonical prefix.
	strictEqual(result.digest.startsWith("SHA2-256:"), true);
});

test(`${variant}: web digestStream native alias matches node digest`, async (_t) => {
	const webStreams = [
		createReadableStream("test"),
		await webDigest.digestStream({ algorithm: "SHA256" }),
	];
	const nodeStreams = [
		createReadableStream("test"),
		await digestStream({ algorithm: "SHA256" }),
	];
	const webResult = await pipeline(webStreams);
	const nodeResult = await pipeline(nodeStreams);
	strictEqual(webResult.digest, nodeResult.digest);
});

// *** unsupported algorithm rejected consistently *** //
test(`${variant}: digestStream should reject unsupported algorithm (node)`, async (_t) => {
	throws(() => digestStream({ algorithm: "MD5" }), {
		message: "Unsupported algorithm: MD5",
	});
});

test(`${variant}: web digestStream should reject unsupported algorithm`, (_t) => {
	// Unified contract: the web factory is now synchronous (matching node), so it
	// throws synchronously rather than rejecting a Promise.
	throws(() => webDigest.digestStream({ algorithm: "MD5" }), {
		message: "Unsupported algorithm: MD5",
	});
});

// *** result() premature finalization guard *** //
test(`${variant}: digestStream.result() throws before stream finishes (node)`, async (_t) => {
	const stream = await digestStream({ algorithm: "SHA2-256" });
	throws(() => stream.result(), {
		message: "digestStream.result() called before the stream finished",
	});
	// After consuming the stream, result() works.
	const streams = [createReadableStream("test"), stream];
	const result = await pipeline(streams);
	strictEqual(result.digest.startsWith("SHA2-256:"), true);
});

test(`${variant}: web digestStream.result() throws before stream finishes`, async (_t) => {
	const stream = await webDigest.digestStream({ algorithm: "SHA2-256" });
	throws(() => stream.result(), {
		message: "digestStream.result() called before the stream finished",
	});
	const streams = [createReadableStream("test"), stream];
	const result = await pipeline(streams);
	strictEqual(result.digest.startsWith("SHA2-256:"), true);
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

// *** web build: all algorithm variants (exercise web algorithm map) *** //
for (const algorithm of [
	"SHA2-256",
	"SHA2-384",
	"SHA2-512",
	"SHA3-256",
	"SHA3-384",
	"SHA3-512",
]) {
	test(`${variant}: web digestStream should calculate ${algorithm}`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await webDigest.digestStream({ algorithm }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith(`${algorithm}:`), true);
	});
}

// *** unified sync factory contract: both builds return a stream synchronously *** //
test(`${variant}: digestStream returns a stream synchronously (node)`, (_t) => {
	const stream = digestStream({ algorithm: "SHA2-256" });
	// Must be a stream, not a Promise, so callers do not need to await.
	strictEqual(typeof stream?.then, "undefined");
	strictEqual(typeof stream?.result, "function");
});

test(`${variant}: web digestStream returns a stream synchronously (parity)`, (_t) => {
	const stream = webDigest.digestStream({ algorithm: "SHA2-256" });
	// Web must match node: synchronous return, no Promise wrapper.
	strictEqual(typeof stream?.then, "undefined");
	strictEqual(typeof stream?.result, "function");
});

test(`${variant}: web digestStream works without await (sync contract)`, async (_t) => {
	const streams = [
		createReadableStream("1,2,3,4"),
		webDigest.digestStream({ algorithm: "SHA2-256" }),
	];
	const result = await pipeline(streams);
	strictEqual(
		result.digest,
		"SHA2-256:37db36876b9ccaaa88394679f019c3435af9320dea117e867003840317870e25",
	);
});

// *** full-value cross-build parity for every algorithm *** //
for (const algorithm of [
	"SHA2-256",
	"SHA2-384",
	"SHA2-512",
	"SHA3-256",
	"SHA3-384",
	"SHA3-512",
]) {
	test(`${variant}: ${algorithm} node and web digests are identical`, async (_t) => {
		const nodeResult = await pipeline([
			createReadableStream("The quick brown fox"),
			digestStream({ algorithm }),
		]);
		const webResult = await pipeline([
			createReadableStream("The quick brown fox"),
			webDigest.digestStream({ algorithm }),
		]);
		strictEqual(nodeResult.digest, webResult.digest);
		strictEqual(nodeResult.digest.startsWith(`${algorithm}:`), true);
	});
}

// *** error propagation: a non-hashable chunk must reject the pipeline *** //
test(`${variant}: digestStream rejects on a non-hashable chunk (node)`, async (_t) => {
	await rejects(
		pipeline([
			createReadableStream([42], { objectMode: true }),
			digestStream({ algorithm: "SHA2-256" }),
		]),
	);
});

test(`${variant}: web digestStream rejects on a non-hashable chunk`, async (_t) => {
	await rejects(
		pipeline([
			createReadableStream([42], { objectMode: true }),
			webDigest.digestStream({ algorithm: "SHA2-256" }),
		]),
	);
});

// *** default export *** //
test(`${variant}: default export should be digestStream`, (_t) => {
	strictEqual(digestDefault, digestStream);
});

test(`${variant}: web default export should be digestStream`, (_t) => {
	strictEqual(webDigest.default, webDigest.digestStream);
});
