import { rejects, strictEqual, throws } from "node:assert";
import test, { describe } from "node:test";
import { createReadableStream, pipeline } from "@datastream/core";
import { digestStream } from "@datastream/digest";
import { variant } from "../variant.js";

describe(`@datastream/digest (${variant})`, async () => {
	const webDigest = await import(
		variant === "browser"
			? "@datastream/digest"
			: `file://${new URL("./index.browser.js", import.meta.url).pathname}`
	);

	test(`digestStream should calculate digest`, async (_t) => {
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
	test(`digestStream should calculate digest from chunks`, async (_t) => {
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

	test(`digestStream should accept native algorithm name and normalize prefix`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA256" }),
		];
		const result = await pipeline(streams);

		strictEqual(result.digest.startsWith("SHA2-256:"), true);
	});

	test(`digestStream should accept native SHA384 alias and normalize prefix`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA384" }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA2-384:"), true);
	});

	test(`digestStream should accept native SHA512 alias and normalize prefix`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA512" }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA2-512:"), true);
	});

	test(`digestStream native alias matches canonical name (node)`, async (_t) => {
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

	test(`web digestStream should accept native algorithm name (parity)`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await webDigest.digestStream({ algorithm: "SHA256" }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA2-256:"), true);
	});

	test(`web digestStream native alias matches node digest`, async (_t) => {
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

	test(`digestStream should reject unsupported algorithm (node)`, async (_t) => {
		throws(() => digestStream({ algorithm: "MD5" }), {
			message: "Unsupported algorithm: MD5",
		});
	});

	test(`web digestStream should reject unsupported algorithm`, (_t) => {
		throws(() => webDigest.digestStream({ algorithm: "MD5" }), {
			message: "Unsupported algorithm: MD5",
		});
	});

	for (const algorithm of ["constructor", "__proto__", "toString"]) {
		test(`digestStream should reject inherited property name ${algorithm}`, (_t) => {
			throws(() => digestStream({ algorithm }), {
				message: `Unsupported algorithm: ${algorithm}`,
			});
		});

		test(`web digestStream should reject inherited property name ${algorithm}`, (_t) => {
			throws(() => webDigest.digestStream({ algorithm }), {
				message: `Unsupported algorithm: ${algorithm}`,
			});
		});
	}

	test(`digestStream.result() throws before stream finishes (node)`, async (_t) => {
		const stream = await digestStream({ algorithm: "SHA2-256" });
		throws(() => stream.result(), {
			message: "digestStream.result() called before the stream finished",
		});
		const streams = [createReadableStream("test"), stream];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA2-256:"), true);
	});

	test(`web digestStream.result() throws before stream finishes`, async (_t) => {
		const stream = await webDigest.digestStream({ algorithm: "SHA2-256" });
		throws(() => stream.result(), {
			message: "digestStream.result() called before the stream finished",
		});
		const streams = [createReadableStream("test"), stream];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA2-256:"), true);
	});

	test(`digestStream should use custom resultKey`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA256", resultKey: "checksum" }),
		];
		const result = await pipeline(streams);

		const { key } = streams[1].result();
		strictEqual(key, "checksum");
		strictEqual(typeof result.checksum, "string");
	});

	test(`digestStream should calculate SHA2-384`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA2-384" }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA2-384:"), true);
	});

	test(`digestStream should calculate SHA2-512`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA2-512" }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA2-512:"), true);
	});

	test(`digestStream should calculate SHA3-256`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA3-256" }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA3-256:"), true);
	});

	test(`digestStream should calculate SHA3-384`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA3-384" }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA3-384:"), true);
	});

	test(`digestStream should calculate SHA3-512`, async (_t) => {
		const streams = [
			createReadableStream("test"),
			await digestStream({ algorithm: "SHA3-512" }),
		];
		const result = await pipeline(streams);
		strictEqual(result.digest.startsWith("SHA3-512:"), true);
	});

	for (const algorithm of [
		"SHA2-256",
		"SHA2-384",
		"SHA2-512",
		"SHA3-256",
		"SHA3-384",
		"SHA3-512",
	]) {
		test(`web digestStream should calculate ${algorithm}`, async (_t) => {
			const streams = [
				createReadableStream("test"),
				await webDigest.digestStream({ algorithm }),
			];
			const result = await pipeline(streams);
			strictEqual(result.digest.startsWith(`${algorithm}:`), true);
		});
	}

	test(`digestStream returns a stream synchronously (node)`, (_t) => {
		const stream = digestStream({ algorithm: "SHA2-256" });
		strictEqual(typeof stream?.then, "undefined");
		strictEqual(typeof stream?.result, "function");
	});

	test(`web digestStream returns a stream synchronously (parity)`, (_t) => {
		const stream = webDigest.digestStream({ algorithm: "SHA2-256" });
		strictEqual(typeof stream?.then, "undefined");
		strictEqual(typeof stream?.result, "function");
	});

	test(`web digestStream works without await (sync contract)`, async (_t) => {
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

	for (const algorithm of [
		"SHA2-256",
		"SHA2-384",
		"SHA2-512",
		"SHA3-256",
		"SHA3-384",
		"SHA3-512",
	]) {
		test(`${algorithm} node and web digests are identical`, async (_t) => {
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

	test(`digestStream rejects on a non-hashable chunk (node)`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream([42], { objectMode: true }),
				digestStream({ algorithm: "SHA2-256" }),
			]),
		);
	});

	test(`web digestStream rejects on a non-hashable chunk`, async (_t) => {
		await rejects(
			pipeline([
				createReadableStream([42], { objectMode: true }),
				webDigest.digestStream({ algorithm: "SHA2-256" }),
			]),
		);
	});

	test(`exports only digestStream (no default export)`, async (_t) => {
		const mod = await import("@datastream/digest");
		strictEqual(Object.keys(mod).join(","), "digestStream");
	});

	test(`web exports only digestStream (no default export)`, (_t) => {
		strictEqual(Object.keys(webDigest).join(","), "digestStream");
	});

	test(`web digestStream hashes an empty stream (hash initialised in flush)`, async (_t) => {
		const stream = webDigest.digestStream({ algorithm: "SHA2-256" });
		await pipeline([createReadableStream([]), stream]);
		strictEqual(
			stream.result().value,
			"SHA2-256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		);
	});
});
