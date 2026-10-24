import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import { randomBytes } from "node:crypto";
import { getEventListeners } from "node:events";
import { realpathSync } from "node:fs";
import { register } from "node:module";
import test, { describe } from "node:test";
import { pathToFileURL } from "node:url";
import {
	brotliCompressSync,
	brotliDecompressSync,
	deflateSync,
	gzipSync,
	constants as zlibConstants,
	zstdCompressSync,
	zstdDecompressSync,
} from "node:zlib";
import {
	deflateCompressStream,
	deflateDecompressStream,
} from "@datastream/compress/deflate";
import {
	gzipCompressStream,
	gzipDecompressStream,
} from "@datastream/compress/gzip";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToBuffer,
	streamToString,
} from "@datastream/core";
import { variant } from "../variant.js";

describe(`@datastream/compress (${variant})`, async () => {
	const nodeTest = variant === "node" ? test : test.skip;

	const compressibleBody = JSON.stringify(new Array(1024).fill(0));

	// brotli-wasm's `browser`/`import` entries fetch their wasm, which fails for
	// file:// under Node, so every run remaps it to its synchronous node build.
	// Resolve through realpathSync: under a Stryker sandbox `${repo}node_modules`
	// is a symlink, and importing the CJS module through the symlinked path yields
	// an empty namespace (DecompressStream undefined). Static imports are hoisted
	// past register(), hence the dynamic import of the brotli module.
	const repo = new URL("../../", import.meta.url).pathname;
	const brotliWasmNodeUrl = pathToFileURL(
		realpathSync(`${repo}node_modules/brotli-wasm/index.node.js`),
	).href;
	register(
		`data:text/javascript,${encodeURIComponent(`
			export async function resolve(specifier, context, nextResolve) {
				if (specifier === "brotli-wasm") {
					return { url: ${JSON.stringify(brotliWasmNodeUrl)}, shortCircuit: true };
				}
				return nextResolve(specifier, context);
			}
		`)}`,
		import.meta.url,
	);
	const { brotliCompressStream, brotliDecompressStream } = await import(
		"@datastream/compress/brotli"
	);

	let zstdCompressStream;
	let zstdDecompressStream;
	if (variant === "node") {
		({ zstdCompressStream, zstdDecompressStream } = await import(
			"@datastream/compress/zstd"
		));

		test(`zstdCompressStream should compress`, async (_t) => {
			const input = compressibleBody;
			const streams = [createReadableStream(input), zstdCompressStream()];
			const output = await streamToBuffer(pipejoin(streams));
			// strictEqual(output, zstdCompressSync(compressibleBody)) // fails, see https://github.com/nodejs/node/issues/58392
			strictEqual(zstdDecompressSync(output).toString(), compressibleBody);
		});

		test(`zstdDecompressStream should decompress`, async (_t) => {
			const input = zstdCompressSync(compressibleBody);
			const streams = [createReadableStream(input), zstdDecompressStream()];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});
	}

	test(`brotliCompressStream should compress`, async (_t) => {
		const input = compressibleBody;
		const streams = [createReadableStream(input), brotliCompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, brotliCompressSync(compressibleBody).toString());
	});

	test(`brotliDecompressStream should decompress`, async (_t) => {
		const input = brotliCompressSync(compressibleBody);
		const streams = [createReadableStream(input), brotliDecompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	test(`brotliDecompressStream should reject truncated input`, async (_t) => {
		const input = brotliCompressSync(compressibleBody);
		const streams = [
			createReadableStream(input.subarray(0, input.byteLength - 1)),
			brotliDecompressStream(),
		];
		await rejects(pipeline(streams), /unexpected end of file/);
	});

	test(`brotliDecompressStream should reject empty input`, async (_t) => {
		const streams = [createReadableStream([]), brotliDecompressStream()];
		await rejects(pipeline(streams), /unexpected end of file/);
	});

	test(`gzipCompressStream should compress`, async (_t) => {
		const input = compressibleBody;
		const streams = [createReadableStream(input), gzipCompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, gzipSync(compressibleBody).toString());
	});

	test(`gzipDecompressStream should decompress`, async (_t) => {
		const input = gzipSync(compressibleBody);
		const streams = [createReadableStream(input), gzipDecompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	test(`gzip streams accept null streamOptions`, async (_t) => {
		const streams = [
			createReadableStream(compressibleBody),
			gzipCompressStream({}, null),
			gzipDecompressStream({}, null),
		];
		strictEqual(await streamToString(pipejoin(streams)), compressibleBody);
	});

	test(`deflateCompressStream should compress`, async (_t) => {
		const input = compressibleBody;
		const streams = [createReadableStream(input), deflateCompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, deflateSync(compressibleBody).toString());
	});

	test(`deflateDecompressStream should decompress`, async (_t) => {
		const input = deflateSync(compressibleBody);
		const streams = [createReadableStream(input), deflateDecompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	if (variant === "node") {
		test(`gzipDecompressStream should enforce maxOutputSize`, async (_t) => {
			const input = gzipSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				gzipDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`deflateDecompressStream should enforce maxOutputSize`, async (_t) => {
			const input = deflateSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				deflateDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`brotliDecompressStream should enforce maxOutputSize`, async (_t) => {
			const input = brotliCompressSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				brotliDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`gzipDecompressStream should work without maxOutputSize`, async (_t) => {
			const input = gzipSync(compressibleBody);
			const streams = [createReadableStream(input), gzipDecompressStream()];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});

		test(`gzipDecompressStream maxOutputSize:null disables the limit`, async (_t) => {
			const input = gzipSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				gzipDecompressStream({ maxOutputSize: null }),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});

		test(`gzipDecompressStream should pass through within maxOutputSize`, async (_t) => {
			const input = gzipSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				gzipDecompressStream({ maxOutputSize: 1024 * 1024 }),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});

		test(`deflateDecompressStream should pass through within maxOutputSize`, async (_t) => {
			const input = deflateSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				deflateDecompressStream({ maxOutputSize: 1024 * 1024 }),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});

		test(`brotliDecompressStream should pass through within maxOutputSize`, async (_t) => {
			const input = brotliCompressSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				brotliDecompressStream({ maxOutputSize: 1024 * 1024 }),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});

		test(`brotliDecompressStream should not promote first-arg keys to Brotli params`, async (_t) => {
			const input = brotliCompressSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				brotliDecompressStream({ chunkSize: 1024 }),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});

		test(`zstdDecompressStream should enforce maxOutputSize`, async (_t) => {
			const input = zstdCompressSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				zstdDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`zstdDecompressStream should pass through within maxOutputSize`, async (_t) => {
			const input = zstdCompressSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				zstdDecompressStream({ maxOutputSize: 1024 * 1024 }),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});

		test(`zstdDecompressStream maxOutputSize:null should NOT throw`, async (_t) => {
			const input = zstdCompressSync(compressibleBody);
			const output = await streamToString(
				pipejoin([
					createReadableStream(input),
					zstdDecompressStream({ maxOutputSize: null }),
				]),
			);
			strictEqual(output, compressibleBody);
		});

		test(`zstdDecompressStream guardOutput sizes string chunks via Buffer.byteLength`, (_t) => {
			const stream = zstdDecompressStream({ maxOutputSize: 1024 });
			ok(stream.push("abc") === true);
			stream.destroy();
		});

		test(`gzipCompressStream should enforce maxOutputSize`, async (_t) => {
			const input = compressibleBody;
			const streams = [
				createReadableStream(input),
				gzipCompressStream({ maxOutputSize: 5 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`gzipCompressStream should pass through within maxOutputSize`, async (_t) => {
			const input = compressibleBody;
			const streams = [
				createReadableStream(input),
				gzipCompressStream({ maxOutputSize: 1024 * 1024 }),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, gzipSync(compressibleBody).toString());
		});

		test(`deflateCompressStream should enforce maxOutputSize`, async (_t) => {
			const input = compressibleBody;
			const streams = [
				createReadableStream(input),
				deflateCompressStream({ maxOutputSize: 5 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`deflateCompressStream should pass through within maxOutputSize`, async (_t) => {
			const input = compressibleBody;
			const streams = [
				createReadableStream(input),
				deflateCompressStream({ maxOutputSize: 1024 * 1024 }),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, deflateSync(compressibleBody).toString());
		});

		test(`deflateCompressStream should honor quality and second-arg stream options`, async (_t) => {
			const streams = [
				createReadableStream(compressibleBody),
				deflateCompressStream({ quality: 9 }, { chunkSize: 1024 }),
				deflateDecompressStream(),
			];
			const output = await streamToString(pipejoin(streams));
			strictEqual(output, compressibleBody);
		});

		test(`brotliCompressStream should enforce maxOutputSize`, async (_t) => {
			const streams = [
				createReadableStream(compressibleBody),
				brotliCompressStream({ maxOutputSize: 5 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`brotliCompressStream should pass through within maxOutputSize`, async (_t) => {
			const streams = [
				createReadableStream(compressibleBody),
				brotliCompressStream({ maxOutputSize: 1024 * 1024 }),
			];
			const output = await streamToBuffer(pipejoin(streams));
			strictEqual(brotliDecompressSync(output).toString(), compressibleBody);
		});

		test(`zstdCompressStream should enforce maxOutputSize`, async (_t) => {
			const streams = [
				createReadableStream(compressibleBody),
				zstdCompressStream({ maxOutputSize: 5 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`zstdCompressStream should pass through within maxOutputSize`, async (_t) => {
			const streams = [
				createReadableStream(compressibleBody),
				zstdCompressStream({ maxOutputSize: 1024 * 1024 }),
			];
			const output = await streamToBuffer(pipejoin(streams));
			strictEqual(zstdDecompressSync(output).toString(), compressibleBody);
		});
	}

	if (variant === "node") {
		test(`brotliCompressStream quality:0 differs from quality:11`, async (_t) => {
			const out0 = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					brotliCompressStream({ quality: 0 }),
				]),
			);
			const out11 = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					brotliCompressStream({ quality: 11 }),
				]),
			);
			ok(
				out0.byteLength !== out11.byteLength,
				"different quality => different size",
			);
			strictEqual(brotliDecompressSync(out0).toString(), compressibleBody);
			strictEqual(brotliDecompressSync(out11).toString(), compressibleBody);
		});

		test(`brotliCompressStream default quality equals explicit BROTLI_DEFAULT_QUALITY`, async (_t) => {
			const { constants: zlibConst } = await import("node:zlib");
			const outDef = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					brotliCompressStream(),
				]),
			);
			const outExp = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					brotliCompressStream({ quality: zlibConst.BROTLI_DEFAULT_QUALITY }),
				]),
			);
			strictEqual(outDef.toString("hex"), outExp.toString("hex"));
		});

		test(`gzipCompressStream quality:1 and quality:9 both round-trip`, async (_t) => {
			const out1 = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					gzipCompressStream({ quality: 1 }),
				]),
			);
			const out9 = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					gzipCompressStream({ quality: 9 }),
				]),
			);
			ok(
				out1.byteLength !== out9.byteLength,
				"quality 1 vs 9 => different size",
			);
			strictEqual(
				await streamToString(
					pipejoin([createReadableStream(out1), gzipDecompressStream()]),
				),
				compressibleBody,
			);
			strictEqual(
				await streamToString(
					pipejoin([createReadableStream(out9), gzipDecompressStream()]),
				),
				compressibleBody,
			);
		});

		test(`gzipCompressStream level overrides quality`, async (_t) => {
			const output = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					gzipCompressStream({ level: 9, quality: 1 }),
				]),
			);
			strictEqual(
				output.toString("hex"),
				gzipSync(compressibleBody, { level: 9 }).toString("hex"),
			);
		});

		test(`gzipCompressStream level:1 output matches gzipSync level:1`, async (_t) => {
			const output = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					gzipCompressStream({ level: 1 }),
				]),
			);
			strictEqual(
				output.toString("hex"),
				gzipSync(compressibleBody, { level: 1 }).toString("hex"),
			);
		});

		test(`deflateCompressStream level overrides quality`, async (_t) => {
			const compressed = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					deflateCompressStream({ level: 9, quality: 1 }),
				]),
			);
			const output = await streamToString(
				pipejoin([createReadableStream(compressed), deflateDecompressStream()]),
			);
			strictEqual(output, compressibleBody);
		});

		test(`deflateCompressStream quality:9 output matches deflateSync level:9`, async (_t) => {
			const { deflateSync: defSync } = await import("node:zlib");
			const output = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					deflateCompressStream({ quality: 9 }),
				]),
			);
			strictEqual(
				output.toString("hex"),
				defSync(compressibleBody, { level: 9 }).toString("hex"),
			);
		});

		test(`deflateCompressStream level:1 output matches deflateSync level:1`, async (_t) => {
			const { deflateSync: defSync } = await import("node:zlib");
			const output = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					deflateCompressStream({ level: 1 }),
				]),
			);
			strictEqual(
				output.toString("hex"),
				defSync(compressibleBody, { level: 1 }).toString("hex"),
			);
		});

		test(`zstdCompressStream quality:1 and quality:19 both round-trip`, async (_t) => {
			const out1 = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					zstdCompressStream({ quality: 1 }),
				]),
			);
			const out19 = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					zstdCompressStream({ quality: 19 }),
				]),
			);
			strictEqual(zstdDecompressSync(out1).toString(), compressibleBody);
			strictEqual(zstdDecompressSync(out19).toString(), compressibleBody);
		});

		test(`zstdCompressStream with explicit params round-trips`, async (_t) => {
			const { constants: zlibConst } = await import("node:zlib");
			const out = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					zstdCompressStream({
						params: { [zlibConst.ZSTD_c_compressionLevel]: 3 },
					}),
				]),
			);
			strictEqual(zstdDecompressSync(out).toString(), compressibleBody);
		});

		test(`zstdDecompressStream with explicit params round-trips`, async (_t) => {
			const { constants: zlibConst } = await import("node:zlib");
			const input = zstdCompressSync(compressibleBody);
			const output = await streamToString(
				pipejoin([
					createReadableStream(input),
					zstdDecompressStream({
						params: { [zlibConst.ZSTD_d_windowLogMax]: 27 },
					}),
				]),
			);
			strictEqual(output, compressibleBody);
		});

		test(`brotliDecompressStream with params option round-trips`, async (_t) => {
			const { constants: zlibConst } = await import("node:zlib");
			const input = brotliCompressSync(compressibleBody);
			const output = await streamToString(
				pipejoin([
					createReadableStream(input),
					brotliDecompressStream({
						params: { [zlibConst.BROTLI_PARAM_MODE]: 0 },
					}),
				]),
			);
			strictEqual(output, compressibleBody);
		});
	}

	if (variant === "node") {
		test(`gzipDecompressStream exact-boundary maxOutputSize should NOT throw`, async (_t) => {
			const input = gzipSync(compressibleBody);
			const exactSize = Buffer.byteLength(compressibleBody);
			const output = await streamToString(
				pipejoin([
					createReadableStream(input),
					gzipDecompressStream({ maxOutputSize: exactSize }),
				]),
			);
			strictEqual(output, compressibleBody);
		});

		test(`deflateDecompressStream exact-boundary maxOutputSize should NOT throw`, async (_t) => {
			const input = deflateSync(compressibleBody);
			const exactSize = Buffer.byteLength(compressibleBody);
			const output = await streamToString(
				pipejoin([
					createReadableStream(input),
					deflateDecompressStream({ maxOutputSize: exactSize }),
				]),
			);
			strictEqual(output, compressibleBody);
		});

		test(`brotliDecompressStream exact-boundary maxOutputSize should NOT throw`, async (_t) => {
			const input = brotliCompressSync(compressibleBody);
			const exactSize = Buffer.byteLength(compressibleBody);
			const output = await streamToString(
				pipejoin([
					createReadableStream(input),
					brotliDecompressStream({ maxOutputSize: exactSize }),
				]),
			);
			strictEqual(output, compressibleBody);
		});

		test(`zstdDecompressStream exact-boundary maxOutputSize should NOT throw`, async (_t) => {
			const input = zstdCompressSync(compressibleBody);
			const exactSize = Buffer.byteLength(compressibleBody);
			const output = await streamToString(
				pipejoin([
					createReadableStream(input),
					zstdDecompressStream({ maxOutputSize: exactSize }),
				]),
			);
			strictEqual(output, compressibleBody);
		});

		test(`gzipDecompressStream one-byte-under maxOutputSize should throw`, async (_t) => {
			const input = gzipSync(compressibleBody);
			const exactSize = Buffer.byteLength(compressibleBody);
			try {
				await pipeline([
					createReadableStream(input),
					gzipDecompressStream({ maxOutputSize: exactSize - 1 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"), e.message);
			}
		});
	}

	if (variant === "node") {
		test(`gzipDecompressStream error message contains "Decompression"`, async (_t) => {
			const input = gzipSync(compressibleBody);
			try {
				await pipeline([
					createReadableStream(input),
					gzipDecompressStream({ maxOutputSize: 10 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("Decompression"), `msg: ${e.message}`);
			}
		});

		test(`gzipCompressStream error message contains "Compression"`, async (_t) => {
			try {
				await pipeline([
					createReadableStream(compressibleBody),
					gzipCompressStream({ maxOutputSize: 5 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("Compression"), `msg: ${e.message}`);
			}
		});

		test(`deflateDecompressStream error message contains "Decompression"`, async (_t) => {
			const input = deflateSync(compressibleBody);
			try {
				await pipeline([
					createReadableStream(input),
					deflateDecompressStream({ maxOutputSize: 10 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("Decompression"), `msg: ${e.message}`);
			}
		});

		test(`deflateCompressStream error message contains "Compression"`, async (_t) => {
			try {
				await pipeline([
					createReadableStream(compressibleBody),
					deflateCompressStream({ maxOutputSize: 5 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("Compression"), `msg: ${e.message}`);
			}
		});

		test(`brotliDecompressStream error message contains "Decompression"`, async (_t) => {
			const input = brotliCompressSync(compressibleBody);
			try {
				await pipeline([
					createReadableStream(input),
					brotliDecompressStream({ maxOutputSize: 10 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("Decompression"), `msg: ${e.message}`);
			}
		});

		test(`brotliCompressStream error message contains "Compression"`, async (_t) => {
			try {
				await pipeline([
					createReadableStream(compressibleBody),
					brotliCompressStream({ maxOutputSize: 5 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("Compression"), `msg: ${e.message}`);
			}
		});

		test(`zstdDecompressStream error message contains "Decompression"`, async (_t) => {
			const input = zstdCompressSync(compressibleBody);
			try {
				await pipeline([
					createReadableStream(input),
					zstdDecompressStream({ maxOutputSize: 10 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("Decompression"), `msg: ${e.message}`);
			}
		});

		test(`zstdCompressStream error message contains "Compression"`, async (_t) => {
			try {
				await pipeline([
					createReadableStream(compressibleBody),
					zstdCompressStream({ maxOutputSize: 5 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("Compression"), `msg: ${e.message}`);
			}
		});

		test(`gzipDecompressStream error message includes the byte limit value`, async (_t) => {
			const input = gzipSync(compressibleBody);
			try {
				await pipeline([
					createReadableStream(input),
					gzipDecompressStream({ maxOutputSize: 42 }),
				]);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("42"), `msg: ${e.message}`);
			}
		});
	}

	if (variant === "node") {
		test(`gzipDecompressStream second invocation works after first closes`, async (_t) => {
			const input = gzipSync(compressibleBody);
			const out1 = await streamToString(
				pipejoin([createReadableStream(input), gzipDecompressStream()]),
			);
			strictEqual(out1, compressibleBody);
			const out2 = await streamToString(
				pipejoin([createReadableStream(input), gzipDecompressStream()]),
			);
			strictEqual(out2, compressibleBody);
		});

		test(`gzipDecompressStream works correctly after previous maxOutputSize error`, async (_t) => {
			const input = gzipSync(compressibleBody);
			try {
				await pipeline([
					createReadableStream(input),
					gzipDecompressStream({ maxOutputSize: 10 }),
				]);
			} catch (_e) {}
			const out = await streamToString(
				pipejoin([createReadableStream(input), gzipDecompressStream()]),
			);
			strictEqual(out, compressibleBody);
		});
	}

	if (variant === "node") {
		test(`gzipCompressStream maxOutputSize:null should NOT throw`, async (_t) => {
			const output = await streamToString(
				pipejoin([
					createReadableStream(compressibleBody),
					gzipCompressStream({ maxOutputSize: null }),
				]),
			);
			strictEqual(output, gzipSync(compressibleBody).toString());
		});

		test(`deflateCompressStream maxOutputSize:null should NOT throw`, async (_t) => {
			const output = await streamToString(
				pipejoin([
					createReadableStream(compressibleBody),
					deflateCompressStream({ maxOutputSize: null }),
				]),
			);
			strictEqual(output, deflateSync(compressibleBody).toString());
		});

		test(`brotliCompressStream maxOutputSize:null should NOT throw`, async (_t) => {
			const out = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					brotliCompressStream({ maxOutputSize: null }),
				]),
			);
			strictEqual(brotliDecompressSync(out).toString(), compressibleBody);
		});

		test(`zstdCompressStream maxOutputSize:null should NOT throw`, async (_t) => {
			const out = await streamToBuffer(
				pipejoin([
					createReadableStream(compressibleBody),
					zstdCompressStream({ maxOutputSize: null }),
				]),
			);
			strictEqual(zstdDecompressSync(out).toString(), compressibleBody);
		});
	}

	if (variant === "node") {
		const bigRepeat = "x".repeat(100_000);
		test(`zstdCompressStream quality:1 differs from quality:19 on large body`, async (_t) => {
			const out1 = await streamToBuffer(
				pipejoin([
					createReadableStream(bigRepeat),
					zstdCompressStream({ quality: 1 }),
				]),
			);
			const out19 = await streamToBuffer(
				pipejoin([
					createReadableStream(bigRepeat),
					zstdCompressStream({ quality: 19 }),
				]),
			);
			ok(
				out1.byteLength !== out19.byteLength,
				`quality 1 (${out1.byteLength}B) must differ from quality 19 (${out19.byteLength}B)`,
			);
		});
	}

	const signalHonored = (e) =>
		e.name === "AbortError" ||
		e.code === "ABORT_ERR" ||
		/abort/i.test(e.message);
	const bigBody = "x".repeat(2_000_000);
	if (variant === "node") {
		test(`gzipCompressStream should reject when signal aborts`, async (_t) => {
			const controller = new AbortController();
			const streams = [
				createReadableStream(bigBody, { chunkSize: 1024 }),
				gzipCompressStream({}, { signal: controller.signal }),
			];
			const promise = pipeline(streams);
			queueMicrotask(() => controller.abort());
			try {
				await promise;
				throw new Error("Should have thrown");
			} catch (e) {
				ok(signalHonored(e), `${e.name}: ${e.message}`);
			}
		});

		test(`deflateDecompressStream should reject when signal aborts`, async (_t) => {
			const controller = new AbortController();
			const input = deflateSync(bigBody);
			const streams = [
				createReadableStream(input, { chunkSize: 1024 }),
				deflateDecompressStream({}, { signal: controller.signal }),
			];
			const promise = pipeline(streams);
			queueMicrotask(() => controller.abort());
			try {
				await promise;
				throw new Error("Should have thrown");
			} catch (e) {
				ok(signalHonored(e), `${e.name}: ${e.message}`);
			}
		});
	}

	test(`gzipDecompressStream maxOutputSize:null should NOT throw`, async (_t) => {
		const input = gzipSync(compressibleBody);
		const output = await streamToString(
			pipejoin([
				createReadableStream(input),
				gzipDecompressStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(output, compressibleBody);
	});

	test(`deflateDecompressStream maxOutputSize:null should NOT throw`, async (_t) => {
		const input = deflateSync(compressibleBody);
		const output = await streamToString(
			pipejoin([
				createReadableStream(input),
				deflateDecompressStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(output, compressibleBody);
	});

	test(`brotliDecompressStream maxOutputSize:null should NOT throw`, async (_t) => {
		const input = brotliCompressSync(compressibleBody);
		const output = await streamToString(
			pipejoin([
				createReadableStream(input),
				brotliDecompressStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(output, compressibleBody);
	});

	nodeTest(
		`gzipDecompressStream guardOutput sizes string chunks via Buffer.byteLength`,
		(_t) => {
			const stream = gzipDecompressStream({ maxOutputSize: 1024 });
			ok(stream.push("abc") === true);
			stream.destroy();
		},
	);

	nodeTest(
		`deflateDecompressStream guardOutput sizes string chunks via Buffer.byteLength`,
		(_t) => {
			const stream = deflateDecompressStream({ maxOutputSize: 1024 });
			ok(stream.push("abc") === true);
			stream.destroy();
		},
	);

	nodeTest(
		`brotliDecompressStream guardOutput sizes string chunks via Buffer.byteLength`,
		(_t) => {
			const stream = brotliDecompressStream({ maxOutputSize: 1024 });
			ok(stream.push("abc") === true);
			stream.destroy();
		},
	);

	nodeTest(
		`gzipDecompressStream guardOutput rejects oversized string chunk`,
		(_t) => {
			const stream = gzipDecompressStream({ maxOutputSize: 2 });
			stream.on("error", () => {});
			strictEqual(stream.push("abcdef"), false);
			stream.destroy();
		},
	);

	if (variant === "browser") {
		test(`gzipDecompressStream should enforce maxOutputSize`, async (_t) => {
			const input = gzipSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				gzipDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`deflateDecompressStream should enforce maxOutputSize`, async (_t) => {
			const input = deflateSync(compressibleBody);
			const streams = [
				createReadableStream(input),
				deflateDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`gzipCompressStream should enforce maxOutputSize`, async (_t) => {
			const streams = [
				createReadableStream(compressibleBody),
				gzipCompressStream({ maxOutputSize: 10 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`deflateCompressStream should enforce maxOutputSize`, async (_t) => {
			const streams = [
				createReadableStream(compressibleBody),
				deflateCompressStream({ maxOutputSize: 10 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`brotliCompressStream should enforce maxOutputSize`, async (_t) => {
			const streams = [
				createReadableStream(compressibleBody),
				brotliCompressStream({ maxOutputSize: 5 }),
			];
			try {
				await pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test(`brotliDecompressStream with params should round-trip`, async (_t) => {
			const { constants: zlibConst } = await import("node:zlib");
			const input = brotliCompressSync(compressibleBody);
			const output = await streamToString(
				pipejoin([
					createReadableStream(input),
					brotliDecompressStream({
						params: { [zlibConst.BROTLI_PARAM_MODE]: 0 },
					}),
				]),
			);
			strictEqual(output, compressibleBody);
		});
	}

	{
		const coreWebUrl = pathToFileURL(
			`${repo}packages/core/index.browser.mjs`,
		).href;
		const loaderSource = `
			export async function resolve(specifier, context, nextResolve) {
				if (specifier === "@datastream/core") {
					return { url: ${JSON.stringify(coreWebUrl)}, shortCircuit: true };
				}
				return nextResolve(specifier, context);
			}
		`;
		register(
			`data:text/javascript,${encodeURIComponent(loaderSource)}`,
			import.meta.url,
		);

		const webModule = (file) =>
			variant === "browser"
				? import(`@datastream/compress/${file.replace(".browser.js", "")}`)
				: import(pathToFileURL(`${repo}packages/compress/${file}`).href);

		const webCore = await import(coreWebUrl);
		const textDecoder = new TextDecoder();
		const toText = (stream) =>
			webCore
				.streamToBuffer(stream)
				.then((buffer) => textDecoder.decode(buffer));
		const webInput = (buffer) => Uint8Array.from(buffer);

		const brotliWeb = await webModule("brotli.browser.js");

		test("web-direct: brotliCompressStream should round-trip", async (_t) => {
			const streams = [
				webCore.createReadableStream(compressibleBody),
				brotliWeb.brotliCompressStream(),
				brotliWeb.brotliDecompressStream(),
			];
			strictEqual(await toText(webCore.pipejoin(streams)), compressibleBody);
		});

		test("web-direct: brotliCompressStream output decompresses with node:zlib", async (_t) => {
			const streams = [
				webCore.createReadableStream(compressibleBody),
				brotliWeb.brotliCompressStream(),
			];
			const output = await webCore.streamToBuffer(webCore.pipejoin(streams));
			ok(output.byteLength > 0);
			strictEqual(
				brotliDecompressSync(Buffer.from(output)).toString(),
				compressibleBody,
			);
		});

		test("web-direct: brotliDecompressStream should decompress node:zlib output", async (_t) => {
			const input = webInput(brotliCompressSync(compressibleBody));
			const streams = [
				webCore.createReadableStream(input),
				brotliWeb.brotliDecompressStream(),
			];
			strictEqual(await toText(webCore.pipejoin(streams)), compressibleBody);
		});

		test("web-direct: brotliCompressStream should accept quality", async (_t) => {
			const streams = [
				webCore.createReadableStream(compressibleBody),
				brotliWeb.brotliCompressStream({ quality: 5 }),
				brotliWeb.brotliDecompressStream(),
			];
			strictEqual(await toText(webCore.pipejoin(streams)), compressibleBody);
		});

		test("web-direct: brotli should round-trip data larger than the output buffer", async (_t) => {
			const big = "x".repeat(200_000);
			const streams = [
				webCore.createReadableStream(big),
				brotliWeb.brotliCompressStream(),
				brotliWeb.brotliDecompressStream(),
			];
			strictEqual(await toText(webCore.pipejoin(streams)), big);
		});

		test("web-direct: brotliCompressStream should enforce maxOutputSize", async (_t) => {
			const streams = [
				webCore.createReadableStream(compressibleBody),
				brotliWeb.brotliCompressStream({ maxOutputSize: 5 }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test("web-direct: brotliDecompressStream should enforce maxOutputSize", async (_t) => {
			const input = webInput(brotliCompressSync(compressibleBody));
			const streams = [
				webCore.createReadableStream(input),
				brotliWeb.brotliDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test("web-direct: brotliDecompressStream should reject trailing bytes after stream end", async (_t) => {
			const compressed = brotliCompressSync(compressibleBody);
			const withTrailing = new Uint8Array(compressed.byteLength + 4);
			withTrailing.set(compressed, 0);
			withTrailing.set([0x01, 0x02, 0x03, 0x04], compressed.byteLength);
			const streams = [
				webCore.createReadableStream(withTrailing),
				brotliWeb.brotliDecompressStream(),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(/trailing/i.test(e.message), e.message);
			}
		});

		const gzipWeb = await webModule("gzip.browser.js");

		test("web-direct: gzipCompressStream should round-trip", async (_t) => {
			const streams = [
				webCore.createReadableStream(compressibleBody),
				gzipWeb.gzipCompressStream(),
				gzipWeb.gzipDecompressStream(),
			];
			strictEqual(await toText(webCore.pipejoin(streams)), compressibleBody);
		});

		test("web-direct: gzipDecompressStream should decompress node:zlib output", async (_t) => {
			const input = webInput(gzipSync(compressibleBody));
			const streams = [
				webCore.createReadableStream(input),
				gzipWeb.gzipDecompressStream(),
			];
			strictEqual(await toText(webCore.pipejoin(streams)), compressibleBody);
		});

		test("web-direct: gzipCompressStream should enforce maxOutputSize", async (_t) => {
			const streams = [
				webCore.createReadableStream(compressibleBody),
				gzipWeb.gzipCompressStream({ maxOutputSize: 10 }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test("web-direct: gzipDecompressStream should enforce maxOutputSize", async (_t) => {
			const input = webInput(gzipSync(compressibleBody));
			const streams = [
				webCore.createReadableStream(input),
				gzipWeb.gzipDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		// A spec-strict CompressionStream rejects non-BufferSource chunks (browsers
		// throw `TypeError: ... not a BufferSource`). Node's CompressionStream is
		// lenient and accepts strings, hiding the bug. We swap in a strict identity
		// CompressionStream that throws on any string/non-BufferSource chunk so that
		// driving the web compress factory with raw string chunks fails unless the
		// factory converts strings to bytes first. Identity output keeps the test
		// independent of real gzip framing.
		const NativeCompressionStream = globalThis.CompressionStream;
		const isBufferSource = (chunk) =>
			chunk instanceof ArrayBuffer || ArrayBuffer.isView(chunk);
		const installStrictCompression = () => {
			globalThis.CompressionStream = class {
				constructor(_format) {
					const ts = new TransformStream({
						transform(chunk, controller) {
							if (!isBufferSource(chunk)) {
								controller.error(
									new TypeError(
										"CompressionStream chunk is not a BufferSource",
									),
								);
								return;
							}
							controller.enqueue(chunk);
						},
					});
					this.readable = ts.readable;
					this.writable = ts.writable;
				}
			};
			return () => {
				globalThis.CompressionStream = NativeCompressionStream;
			};
		};

		test("web-direct: gzipCompressStream should accept string chunks (BufferSource conversion)", async (_t) => {
			const restore = installStrictCompression();
			try {
				const gzipWebStrict = await import(
					`${pathToFileURL(`${repo}packages/compress/gzip.browser.js`).href}?strict=gzip`
				);
				const out = await webCore.streamToBuffer(
					webCore.pipejoin([
						webCore.createReadableStream(compressibleBody),
						gzipWebStrict.gzipCompressStream(),
					]),
				);
				strictEqual(new TextDecoder().decode(out), compressibleBody);
			} finally {
				restore();
			}
		});

		test("web-direct: gzipCompressStream should reject when signal aborts", async (_t) => {
			const controller = new AbortController();
			controller.abort();
			const streams = [
				webCore.createReadableStream(compressibleBody),
				gzipWeb.gzipCompressStream({}, { signal: controller.signal }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.name === "AbortError" || /abort/i.test(e.message), e.message);
			}
		});

		test("web-direct: gzipDecompressStream should reject when signal aborts", async (_t) => {
			const controller = new AbortController();
			controller.abort();
			const input = webInput(gzipSync(compressibleBody));
			const streams = [
				webCore.createReadableStream(input),
				gzipWeb.gzipDecompressStream({}, { signal: controller.signal }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.name === "AbortError" || /abort/i.test(e.message), e.message);
			}
		});

		const deflateWeb = await webModule("deflate.browser.js");

		test("web-direct: deflateCompressStream should round-trip", async (_t) => {
			const streams = [
				webCore.createReadableStream(compressibleBody),
				deflateWeb.deflateCompressStream(),
				deflateWeb.deflateDecompressStream(),
			];
			strictEqual(await toText(webCore.pipejoin(streams)), compressibleBody);
		});

		test("web-direct: deflateDecompressStream should decompress node:zlib output", async (_t) => {
			const input = webInput(deflateSync(compressibleBody));
			const streams = [
				webCore.createReadableStream(input),
				deflateWeb.deflateDecompressStream(),
			];
			strictEqual(await toText(webCore.pipejoin(streams)), compressibleBody);
		});

		test("web-direct: deflateCompressStream should enforce maxOutputSize", async (_t) => {
			const streams = [
				webCore.createReadableStream(compressibleBody),
				deflateWeb.deflateCompressStream({ maxOutputSize: 10 }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test("web-direct: deflateDecompressStream should enforce maxOutputSize", async (_t) => {
			const input = webInput(deflateSync(compressibleBody));
			const streams = [
				webCore.createReadableStream(input),
				deflateWeb.deflateDecompressStream({ maxOutputSize: 100 }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes("maxOutputSize"));
			}
		});

		test("web-direct: deflateCompressStream should accept string chunks (BufferSource conversion)", async (_t) => {
			const restore = installStrictCompression();
			try {
				const deflateWebStrict = await import(
					`${pathToFileURL(`${repo}packages/compress/deflate.browser.js`).href}?strict=deflate`
				);
				const out = await webCore.streamToBuffer(
					webCore.pipejoin([
						webCore.createReadableStream(compressibleBody),
						deflateWebStrict.deflateCompressStream(),
					]),
				);
				strictEqual(new TextDecoder().decode(out), compressibleBody);
			} finally {
				restore();
			}
		});

		const withFakeDecompressionStream = async (ratio, fn) => {
			const original = globalThis.DecompressionStream;
			const writes = [];
			globalThis.DecompressionStream = class extends TransformStream {
				constructor(_format) {
					super({
						transform(chunk, controller) {
							writes.push(chunk.byteLength);
							controller.enqueue(
								ratio === 1
									? chunk.slice()
									: new Uint8Array(chunk.byteLength * ratio),
							);
						},
					});
				}
			};
			try {
				await fn(writes);
			} finally {
				globalThis.DecompressionStream = original;
			}
		};
		const sequence = (length) => new Uint8Array(length).map((_, i) => i % 251);

		test("web-direct: deflateDecompressStream feeds the decompressor 16KiB slices", async (_t) => {
			await withFakeDecompressionStream(1, async (writes) => {
				const input = sequence(40_000);
				const out = await webCore.streamToBuffer(
					webCore.pipejoin([
						webCore.createReadableStream([input]),
						deflateWeb.deflateDecompressStream(),
					]),
				);
				deepStrictEqual(writes, [16_384, 16_384, 7_232]);
				deepStrictEqual(new Uint8Array(out), input);
			});
		});

		test("web-direct: deflateDecompressStream feeds no empty slice for an exact multiple", async (_t) => {
			await withFakeDecompressionStream(1, async (writes) => {
				const input = sequence(32_768);
				const out = await webCore.streamToBuffer(
					webCore.pipejoin([
						webCore.createReadableStream([input]),
						deflateWeb.deflateDecompressStream(),
					]),
				);
				deepStrictEqual(writes, [16_384, 16_384]);
				deepStrictEqual(new Uint8Array(out), input);
			});
		});

		test("web-direct: deflateDecompressStream input stage aborts without the codec", async (_t) => {
			const original = globalThis.DecompressionStream;
			globalThis.DecompressionStream = class {
				constructor(_format) {
					this.writable = new WritableStream({
						write: () => new Promise(() => {}),
					});
					this.readable = new ReadableStream();
				}
			};
			try {
				const controller = new AbortController();
				const reason = new Error("stop");
				const { writable } = deflateWeb.deflateDecompressStream(
					{},
					{ signal: controller.signal },
				);
				const writer = writable.getWriter();
				controller.abort(reason);
				strictEqual(
					await Promise.race([
						writer.closed.catch((e) => e),
						new Promise((resolve) => setTimeout(() => resolve("pending"), 10)),
					]),
					reason,
				);
			} finally {
				globalThis.DecompressionStream = original;
			}
		});

		test("web-direct: deflateDecompressStream slices ArrayBuffer and offset views", async (_t) => {
			await withFakeDecompressionStream(1, async (writes) => {
				const backing = sequence(20_000);
				const out = await webCore.streamToBuffer(
					webCore.pipejoin([
						webCore.createReadableStream([
							backing.buffer.slice(0, 3),
							new DataView(backing.buffer, 3, 5),
							new Uint16Array(backing.buffer, 8, 9_996),
						]),
						deflateWeb.deflateDecompressStream(),
					]),
				);
				deepStrictEqual(writes, [3, 5, 16_384, 3_608]);
				deepStrictEqual(new Uint8Array(out), backing);
			});
		});

		test("web-direct: deflateDecompressStream rejects non-BufferSource chunks", async (_t) => {
			await rejects(
				webCore.pipeline([
					webCore.createReadableStream([5]),
					deflateWeb.deflateDecompressStream(),
				]),
				{
					name: "TypeError",
					message: "chunk must be a string or BufferSource",
				},
			);
		});

		test("web-direct: deflateDecompressStream stops feeding a bomb at maxOutputSize", async (_t) => {
			await withFakeDecompressionStream(1000, async (writes) => {
				await rejects(
					webCore.pipeline([
						webCore.createReadableStream([new Uint8Array(1024 * 1024)]),
						deflateWeb.deflateDecompressStream({ maxOutputSize: 1024 * 1024 }),
					]),
					(e) => e instanceof RangeError && e.message.includes("maxOutputSize"),
				);
				ok(writes.length <= 4, `wrote ${writes.length} slices`);
			});
		});

		test("web-direct: deflateCompressStream should reject when signal aborts", async (_t) => {
			const controller = new AbortController();
			controller.abort();
			const streams = [
				webCore.createReadableStream(compressibleBody),
				deflateWeb.deflateCompressStream({}, { signal: controller.signal }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.name === "AbortError" || /abort/i.test(e.message), e.message);
			}
		});

		test("web-direct: deflateDecompressStream should reject when signal aborts", async (_t) => {
			const controller = new AbortController();
			controller.abort();
			const input = webInput(deflateSync(compressibleBody));
			const streams = [
				webCore.createReadableStream(input),
				deflateWeb.deflateDecompressStream({}, { signal: controller.signal }),
			];
			try {
				await webCore.pipeline(streams);
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.name === "AbortError" || /abort/i.test(e.message), e.message);
			}
		});
	}

	if (variant === "node") {
		const compressFactories = {
			gzip: gzipCompressStream,
			deflate: deflateCompressStream,
			brotli: brotliCompressStream,
			zstd: zstdCompressStream,
		};
		const decompressFactories = {
			gzip: gzipDecompressStream,
			deflate: deflateDecompressStream,
			brotli: brotliDecompressStream,
			zstd: zstdDecompressStream,
		};

		for (const [name, factory] of Object.entries(decompressFactories)) {
			test(`${name}DecompressStream guardOutput sizes string chunks and trips limit`, (_t) => {
				const stream = factory({ maxOutputSize: 2 });
				stream.on("error", () => {});
				strictEqual(stream.push("abc"), false);
				stream.destroy();
			});

			test(`${name}DecompressStream guardOutput passes string chunks within limit`, (_t) => {
				const stream = factory({ maxOutputSize: 1024 });
				strictEqual(stream.push("abc"), true);
				stream.destroy();
			});

			test(`${name}DecompressStream restores push on error`, (_t) => {
				const stream = factory({ maxOutputSize: 100 });
				const wrapped = stream.push;
				stream.on("error", () => {});
				stream.emit("error", new Error("boom"));
				ok(stream.push !== wrapped, "push must be restored after error");
				stream.destroy();
			});

			test(`${name}DecompressStream restores push on close`, (_t) => {
				const stream = factory({ maxOutputSize: 100 });
				const wrapped = stream.push;
				stream.emit("close");
				ok(stream.push !== wrapped, "push must be restored after close");
				stream.destroy();
			});

			test(`${name}DecompressStream maxOutputSize:null installs no guard`, (_t) => {
				const stream = factory({ maxOutputSize: null });
				strictEqual(stream.listenerCount("close"), 0);
				strictEqual(stream.listenerCount("error"), 0);
				stream.destroy();
			});

			test(`${name}DecompressStream default installs the guard`, (_t) => {
				const stream = factory();
				strictEqual(stream.listenerCount("close"), 1);
				strictEqual(stream.listenerCount("error"), 1);
				stream.destroy();
			});
		}

		for (const [name, factory] of Object.entries(compressFactories)) {
			test(`${name}CompressStream without maxOutputSize installs no guard`, (_t) => {
				const stream = factory();
				strictEqual(stream.listenerCount("close"), 0);
				strictEqual(stream.listenerCount("error"), 0);
				stream.destroy();
			});

			test(`${name}CompressStream maxOutputSize:null installs no guard`, (_t) => {
				const stream = factory({ maxOutputSize: null });
				strictEqual(stream.listenerCount("close"), 0);
				strictEqual(stream.listenerCount("error"), 0);
				stream.destroy();
			});

			test(`${name}CompressStream with maxOutputSize installs the guard`, (_t) => {
				const stream = factory({ maxOutputSize: 1024 });
				strictEqual(stream.listenerCount("close"), 1);
				strictEqual(stream.listenerCount("error"), 1);
				stream.destroy();
			});
		}

		test(`brotliDecompressStream forwards streamOptions when params given`, (_t) => {
			const stream = brotliDecompressStream(
				{ params: { [zlibConstants.BROTLI_DECODER_PARAM_LARGE_WINDOW]: 0 } },
				{ chunkSize: 8192 },
			);
			strictEqual(stream._chunkSize, 8192);
			stream.destroy();
		});

		test(`zstdDecompressStream forwards streamOptions when params given`, (_t) => {
			const stream = zstdDecompressStream(
				{ params: { [zlibConstants.ZSTD_d_windowLogMax]: 27 } },
				{ chunkSize: 8192 },
			);
			strictEqual(stream._chunkSize, 8192);
			stream.destroy();
		});
	}

	if (variant === "browser") {
		const bigBody = "0123456789".repeat(20_000);
		const noise = Uint8Array.from(randomBytes(128 * 1024));
		const roundtrip = async (streams) => streamToString(pipejoin(streams));

		test(`brotli streams drain NeedsMoreOutput across many output buffers`, async () => {
			const bytes = async (streams) =>
				Uint8Array.from(await streamToBuffer(pipejoin(streams)));
			deepStrictEqual(
				await bytes([
					createReadableStream([noise]),
					brotliCompressStream(),
					brotliDecompressStream(),
				]),
				noise,
			);
			deepStrictEqual(
				await bytes([
					createReadableStream([Uint8Array.from(brotliCompressSync(noise))]),
					brotliDecompressStream(),
				]),
				noise,
			);
			deepStrictEqual(
				Uint8Array.from(
					brotliDecompressSync(
						await bytes([
							createReadableStream([noise]),
							brotliCompressStream(),
						]),
					),
				),
				noise,
			);
			strictEqual(
				await roundtrip([
					createReadableStream(bigBody, { chunkSize: 50_000 }),
					brotliCompressStream(),
					brotliDecompressStream(),
				]),
				bigBody,
			);
			strictEqual(
				await roundtrip([
					createReadableStream(Uint8Array.from(brotliCompressSync(bigBody))),
					brotliDecompressStream(),
				]),
				bigBody,
			);
		});

		test(`brotliCompressStream quality changes the output size`, async () => {
			const sizeAt = async (options) =>
				(
					await streamToBuffer(
						pipejoin([
							createReadableStream(bigBody),
							brotliCompressStream(options),
						]),
					)
				).byteLength;
			ok((await sizeAt({ quality: 0 })) > (await sizeAt({ quality: 11 })));
			strictEqual(await sizeAt({}), await sizeAt({ quality: 11 }));
		});

		test(`brotli streams enforce maxOutputSize at the exact boundary`, async () => {
			const compressed = Uint8Array.from(brotliCompressSync(compressibleBody));
			await pipeline([
				createReadableStream(compressed),
				brotliDecompressStream({ maxOutputSize: compressibleBody.length }),
			]);
			await rejects(
				pipeline([
					createReadableStream(compressed),
					brotliDecompressStream({
						maxOutputSize: compressibleBody.length - 1,
					}),
				]),
				new RegExp(
					`Decompression output exceeds maxOutputSize \\(${compressibleBody.length - 1} bytes\\)`,
				),
			);
			await pipeline([
				createReadableStream(compressibleBody),
				brotliDecompressStream({ maxOutputSize: null }),
			]).catch(() => {});
			const size = (
				await streamToBuffer(
					pipejoin([createReadableStream(bigBody), brotliCompressStream()]),
				)
			).byteLength;
			await pipeline([
				createReadableStream(bigBody),
				brotliCompressStream({ maxOutputSize: size }),
			]);
			await rejects(
				pipeline([
					createReadableStream(bigBody),
					brotliCompressStream({ maxOutputSize: size - 1 }),
				]),
				new RegExp(
					`Compression output exceeds maxOutputSize \\(${size - 1} bytes\\)`,
				),
			);
		});

		test(`brotliDecompressStream rejects trailing bytes after the end of stream`, async () => {
			const compressed = brotliCompressSync(compressibleBody);
			const withTrailer = new Uint8Array(compressed.length + 3);
			withTrailer.set(compressed);
			await rejects(
				pipeline([createReadableStream(withTrailer), brotliDecompressStream()]),
				/trailing bytes after end of brotli stream/,
			);
		});

		test(`native streams enforce maxOutputSize at the exact boundary and null disables it`, async () => {
			const compressed = Uint8Array.from(gzipSync(compressibleBody));
			await pipeline([
				createReadableStream(compressed),
				gzipDecompressStream({ maxOutputSize: compressibleBody.length }),
			]);
			await rejects(
				pipeline([
					createReadableStream(compressed),
					gzipDecompressStream({ maxOutputSize: compressibleBody.length - 1 }),
				]),
				/Decompression output exceeds maxOutputSize/,
			);
			strictEqual(
				await roundtrip([
					createReadableStream(compressed),
					gzipDecompressStream({ maxOutputSize: null }),
				]),
				compressibleBody,
			);
			const size = (
				await streamToBuffer(
					pipejoin([
						createReadableStream(compressibleBody),
						deflateCompressStream(),
					]),
				)
			).byteLength;
			await pipeline([
				createReadableStream(compressibleBody),
				deflateCompressStream({ maxOutputSize: size }),
			]);
			await rejects(
				pipeline([
					createReadableStream(compressibleBody),
					deflateCompressStream({ maxOutputSize: size - 1 }),
				]),
				/Compression output exceeds maxOutputSize/,
			);
		});

		test(`native streams honour a live AbortSignal and detach its listeners`, async () => {
			const { signal } = new AbortController();
			const calls = { add: 0, remove: 0 };
			const add = signal.addEventListener.bind(signal);
			const remove = signal.removeEventListener.bind(signal);
			signal.addEventListener = (...args) => {
				calls.add++;
				add(...args);
			};
			signal.removeEventListener = (...args) => {
				calls.remove++;
				remove(...args);
			};
			strictEqual(
				await roundtrip([
					createReadableStream(Uint8Array.from(gzipSync(compressibleBody))),
					gzipDecompressStream({}, { signal }),
				]),
				compressibleBody,
			);
			deepStrictEqual(calls, { add: 2, remove: 2 });
		});

		const unended = (chunk) => {
			const source = createReadableStream();
			source.push(chunk);
			return source;
		};
		for (const [name, source, maxOutputSize] of [
			[
				"exceeding maxOutputSize",
				() => createReadableStream(Uint8Array.from(gzipSync(compressibleBody))),
				10,
			],
			[
				"corrupt input",
				() => createReadableStream(Uint8Array.from(Buffer.from("not gzip"))),
			],
			[
				"corrupt input before the end",
				() => unended(Uint8Array.from(Buffer.from("not gzip"))),
			],
		]) {
			test(`native streams detach their abort listeners on ${name}`, async () => {
				const { signal } = new AbortController();
				await rejects(
					pipeline([
						source(),
						gzipDecompressStream({ maxOutputSize }, { signal }),
					]),
				);
				await new Promise((resolve) => setImmediate(resolve));
				strictEqual(getEventListeners(signal, "abort").length, 0);
			});
		}

		test(`native streams reject with the signal reason when aborted mid-flight`, async () => {
			const controller = new AbortController();
			const reason = new Error("stop");
			const promise = pipeline([
				createReadableStream(bigBody, { chunkSize: 1024 }),
				gzipCompressStream({}, { signal: controller.signal }),
			]);
			queueMicrotask(() => controller.abort(reason));
			await rejects(promise, (e) => e === reason);
		});

		const settled = (promise) =>
			Promise.race([
				promise.then(
					() => "resolved",
					(e) => e,
				),
				new Promise((resolve) => setTimeout(() => resolve("pending"), 10)),
			]);

		test(`native streams reject with the signal reason when aborted while idle`, async () => {
			const controller = new AbortController();
			const reason = new Error("stop");
			const promise = pipeline([
				unended(Uint8Array.from(gzipSync(compressibleBody))),
				gzipDecompressStream({}, { signal: controller.signal }),
			]);
			strictEqual(await settled(promise), "pending");
			controller.abort(reason);
			strictEqual(await settled(promise), reason);
		});

		test(`native output stage honours the signal after the input has ended`, async () => {
			const controller = new AbortController();
			const { signal } = controller;
			const reason = new Error("stop");
			const { readable, writable } = gzipCompressStream({}, { signal });
			strictEqual(getEventListeners(signal, "abort").length, 2);
			const writer = writable.getWriter();
			await writer.write(compressibleBody);
			await writer.close();
			strictEqual(getEventListeners(signal, "abort").length, 1);
			controller.abort(reason);
			await new Promise((resolve) => setTimeout(resolve, 0));
			strictEqual(await settled(readable.getReader().read()), reason);
		});

		test(`native decompress limit: null is unlimited, undefined is the default`, async () => {
			const { resolveDecompressLimit, DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE } =
				await import("./native.browser.js");
			strictEqual(resolveDecompressLimit(null), Number.POSITIVE_INFINITY);
			strictEqual(
				resolveDecompressLimit(undefined),
				DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE,
			);
		});

		for (const [name, factory] of [
			["compress", gzipCompressStream],
			["decompress", gzipDecompressStream],
		]) {
			test(`native ${name} with a pre-aborted signal rejects and adds no listener`, async () => {
				const controller = new AbortController();
				const reason = new Error("stop");
				controller.abort(reason);
				const { signal } = controller;
				let adds = 0;
				const add = signal.addEventListener.bind(signal);
				signal.addEventListener = (...args) => {
					adds++;
					add(...args);
				};
				await rejects(
					pipeline([createReadableStream("x"), factory({}, { signal })]),
					(e) => e === reason,
				);
				strictEqual(adds, 0);
			});
		}
	}

	const streamNames = (algo) => [
		`${algo}CompressStream`,
		`${algo}DecompressStream`,
	];

	test(`subpaths have only named exports`, async () => {
		const algos =
			variant === "node"
				? ["brotli", "deflate", "gzip", "zstd"]
				: ["brotli", "deflate", "gzip"];
		for (const algo of algos) {
			const mod = await import(`@datastream/compress/${algo}`);
			deepStrictEqual(Object.keys(mod).sort(), streamNames(algo));
		}
	});

	test(`index exports every algorithm; zstd only on node`, async () => {
		const mod = await import("@datastream/compress");
		deepStrictEqual(
			Object.keys(mod).sort(),
			["brotli", "deflate", "gzip", ...(variant === "node" ? ["zstd"] : [])]
				.flatMap(streamNames)
				.sort(),
		);
	});

	test(`zstd subpath has no browser condition (resolves to the node build)`, () => {
		strictEqual(
			import.meta
				.resolve("@datastream/compress/zstd")
				.endsWith("/zstd.node.mjs"),
			true,
		);
	});

	const limitFactories = {
		gzip: [gzipCompressStream, gzipDecompressStream, gzipSync],
		deflate: [deflateCompressStream, deflateDecompressStream, deflateSync],
		brotli: [brotliCompressStream, brotliDecompressStream, brotliCompressSync],
	};
	if (variant === "node") {
		limitFactories.zstd = [
			zstdCompressStream,
			zstdDecompressStream,
			zstdCompressSync,
		];
	}
	for (const [algo, [compress, decompress, compressSync]] of Object.entries(
		limitFactories,
	)) {
		test(`${algo} maxOutputSize errors are RangeErrors`, async () => {
			await rejects(
				pipeline([
					createReadableStream(compressibleBody),
					compress({ maxOutputSize: 5 }),
				]),
				(e) =>
					e instanceof RangeError &&
					e.message === "Compression output exceeds maxOutputSize (5 bytes)",
			);
			await rejects(
				pipeline([
					createReadableStream(Uint8Array.from(compressSync(compressibleBody))),
					decompress({ maxOutputSize: 10 }),
				]),
				(e) =>
					e instanceof RangeError &&
					e.message === "Decompression output exceeds maxOutputSize (10 bytes)",
			);
		});
	}
});
