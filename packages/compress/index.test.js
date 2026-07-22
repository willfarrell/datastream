import { ok, strictEqual } from "node:assert";
import { realpathSync } from "node:fs";
import { register } from "node:module";
import test from "node:test";
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
	brotliCompressStream,
	brotliDecompressStream,
} from "@datastream/compress/brotli";
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

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

const compressibleBody = JSON.stringify(new Array(1024).fill(0));

let zstdCompressStream;
let zstdDecompressStream;
if (variant === "node") {
	({ zstdCompressStream, zstdDecompressStream } = await import(
		"@datastream/compress/zstd"
	));

	// *** zstd *** //
	test(`${variant}: zstdCompressStream should compress`, async (_t) => {
		const input = compressibleBody;
		const streams = [createReadableStream(input), zstdCompressStream()];
		const output = await streamToBuffer(pipejoin(streams));
		// strictEqual(output, zstdCompressSync(compressibleBody)) // fails, see https://github.com/nodejs/node/issues/58392
		strictEqual(zstdDecompressSync(output).toString(), compressibleBody);
	});

	test(`${variant}: zstdDecompressStream should decompress`, async (_t) => {
		const input = zstdCompressSync(compressibleBody);
		const streams = [createReadableStream(input), zstdDecompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});
}

// *** brotli *** //
test(`${variant}: brotliCompressStream should compress`, async (_t) => {
	const input = compressibleBody;
	const streams = [createReadableStream(input), brotliCompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, brotliCompressSync(compressibleBody).toString());
});

test(`${variant}: brotliDecompressStream should decompress`, async (_t) => {
	const input = brotliCompressSync(compressibleBody);
	const streams = [createReadableStream(input), brotliDecompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, compressibleBody);
});

// *** gzip *** //
test(`${variant}: gzipCompressStream should compress`, async (_t) => {
	const input = compressibleBody;
	const streams = [createReadableStream(input), gzipCompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, gzipSync(compressibleBody).toString());
});

test(`${variant}: gzipDecompressStream should decompress`, async (_t) => {
	const input = gzipSync(compressibleBody);
	const streams = [createReadableStream(input), gzipDecompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, compressibleBody);
});

// *** deflate *** //
test(`${variant}: deflateCompressStream should compress`, async (_t) => {
	const input = compressibleBody;
	const streams = [createReadableStream(input), deflateCompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, deflateSync(compressibleBody).toString());
});

test(`${variant}: deflateDecompressStream should decompress`, async (_t) => {
	const input = deflateSync(compressibleBody);
	const streams = [createReadableStream(input), deflateDecompressStream()];
	const output = await streamToString(pipejoin(streams));
	strictEqual(output, compressibleBody);
});

// *** decompression bomb protection *** //
if (variant === "node") {
	test(`${variant}: gzipDecompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: deflateDecompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: brotliDecompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: gzipDecompressStream should work without maxOutputSize`, async (_t) => {
		const input = gzipSync(compressibleBody);
		const streams = [createReadableStream(input), gzipDecompressStream()];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	// A default decompression ceiling now applies; `maxOutputSize: null` opts
	// out of any limit (unbounded), so normal payloads still round-trip.
	test(`${variant}: gzipDecompressStream maxOutputSize:null disables the limit`, async (_t) => {
		const input = gzipSync(compressibleBody);
		const streams = [
			createReadableStream(input),
			gzipDecompressStream({ maxOutputSize: null }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	// *** maxOutputSize within limit (covers normal-path push) *** //
	test(`${variant}: gzipDecompressStream should pass through within maxOutputSize`, async (_t) => {
		const input = gzipSync(compressibleBody);
		const streams = [
			createReadableStream(input),
			gzipDecompressStream({ maxOutputSize: 1024 * 1024 }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	test(`${variant}: deflateDecompressStream should pass through within maxOutputSize`, async (_t) => {
		const input = deflateSync(compressibleBody);
		const streams = [
			createReadableStream(input),
			deflateDecompressStream({ maxOutputSize: 1024 * 1024 }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	test(`${variant}: brotliDecompressStream should pass through within maxOutputSize`, async (_t) => {
		const input = brotliCompressSync(compressibleBody);
		const streams = [
			createReadableStream(input),
			brotliDecompressStream({ maxOutputSize: 1024 * 1024 }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	// Arbitrary keys on the first (options) argument must NOT be promoted into
	// zlib Brotli `params` (the old behavior threw "chunkSize is not a valid
	// Brotli parameter"). Only an explicit `params` field is forwarded; real
	// stream options go on the second argument.
	test(`${variant}: brotliDecompressStream should not promote first-arg keys to Brotli params`, async (_t) => {
		const input = brotliCompressSync(compressibleBody);
		const streams = [
			createReadableStream(input),
			brotliDecompressStream({ chunkSize: 1024 }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	// *** zstd decompress maxOutputSize *** //
	test(`${variant}: zstdDecompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: zstdDecompressStream should pass through within maxOutputSize`, async (_t) => {
		const input = zstdCompressSync(compressibleBody);
		const streams = [
			createReadableStream(input),
			zstdDecompressStream({ maxOutputSize: 1024 * 1024 }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	// Covers `maxOutputSize === null ? void 0 :` true-branch in zstdDecompressStream.
	test(`${variant}: zstdDecompressStream maxOutputSize:null should NOT throw`, async (_t) => {
		const input = zstdCompressSync(compressibleBody);
		const output = await streamToString(
			pipejoin([
				createReadableStream(input),
				zstdDecompressStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(output, compressibleBody);
	});

	// guardOutput Buffer.byteLength(chunk) fallback for zstd (node-only): push a
	// string directly into the guarded stream so .byteLength is undefined.
	test(`${variant}: zstdDecompressStream guardOutput sizes string chunks via Buffer.byteLength`, (_t) => {
		const stream = zstdDecompressStream({ maxOutputSize: 1024 });
		ok(stream.push("abc") === true);
		stream.destroy();
	});

	// *** compress maxOutputSize *** //
	test(`${variant}: gzipCompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: gzipCompressStream should pass through within maxOutputSize`, async (_t) => {
		const input = compressibleBody;
		const streams = [
			createReadableStream(input),
			gzipCompressStream({ maxOutputSize: 1024 * 1024 }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, gzipSync(compressibleBody).toString());
	});

	test(`${variant}: deflateCompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: deflateCompressStream should pass through within maxOutputSize`, async (_t) => {
		const input = compressibleBody;
		const streams = [
			createReadableStream(input),
			deflateCompressStream({ maxOutputSize: 1024 * 1024 }),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, deflateSync(compressibleBody).toString());
	});

	// deflateCompressStream reads quality/level/maxOutputSize from the first
	// argument and forwards real stream options from the second argument
	// (matching gzipCompressStream), rather than spreading the first arg.
	test(`${variant}: deflateCompressStream should honor quality and second-arg stream options`, async (_t) => {
		const streams = [
			createReadableStream(compressibleBody),
			deflateCompressStream({ quality: 9 }, { chunkSize: 1024 }),
			deflateDecompressStream(),
		];
		const output = await streamToString(pipejoin(streams));
		strictEqual(output, compressibleBody);
	});

	test(`${variant}: brotliCompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: brotliCompressStream should pass through within maxOutputSize`, async (_t) => {
		const streams = [
			createReadableStream(compressibleBody),
			brotliCompressStream({ maxOutputSize: 1024 * 1024 }),
		];
		const output = await streamToBuffer(pipejoin(streams));
		strictEqual(brotliDecompressSync(output).toString(), compressibleBody);
	});

	test(`${variant}: zstdCompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: zstdCompressStream should pass through within maxOutputSize`, async (_t) => {
		const streams = [
			createReadableStream(compressibleBody),
			zstdCompressStream({ maxOutputSize: 1024 * 1024 }),
		];
		const output = await streamToBuffer(pipejoin(streams));
		strictEqual(zstdDecompressSync(output).toString(), compressibleBody);
	});
}

// *** quality / level parameter routing *** //
if (variant === "node") {
	// brotliCompressStream: quality=0 vs quality=11 produce different sizes;
	// ensures params object is forwarded (ObjectLiteral survivor).
	test(`${variant}: brotliCompressStream quality:0 differs from quality:11`, async (_t) => {
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

	// default quality matches BROTLI_DEFAULT_QUALITY constant
	// (covers quality ?? BROTLI_DEFAULT_QUALITY LogicalOperator survivor).
	test(`${variant}: brotliCompressStream default quality equals explicit BROTLI_DEFAULT_QUALITY`, async (_t) => {
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

	// gzipCompressStream: quality maps to zlib level; levels 1 and 9 differ.
	test(`${variant}: gzipCompressStream quality:1 and quality:9 both round-trip`, async (_t) => {
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
		ok(out1.byteLength !== out9.byteLength, "quality 1 vs 9 => different size");
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

	// deflateCompressStream: `level` takes precedence over `quality`
	// (covers `level ?? quality` LogicalOperator survivor).
	test(`${variant}: deflateCompressStream level overrides quality`, async (_t) => {
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

	// deflateCompressStream with quality:9 must match deflateSync at level:9.
	// `level && quality` mutation: when level is undefined, `undefined && quality`
	// = `undefined` → quality ignored → output would match default level, not 9.
	// Also catches `createDeflate({})` mutation (drops level entirely).
	test(`${variant}: deflateCompressStream quality:9 output matches deflateSync level:9`, async (_t) => {
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

	// deflateCompressStream with level:1 must match deflateSync at level:1.
	test(`${variant}: deflateCompressStream level:1 output matches deflateSync level:1`, async (_t) => {
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

	// zstdCompressStream: different quality levels both round-trip.
	test(`${variant}: zstdCompressStream quality:1 and quality:19 both round-trip`, async (_t) => {
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

	// zstdCompressStream with explicit params (covers `params ?? { ... }` ObjectLiteral survivor).
	test(`${variant}: zstdCompressStream with explicit params round-trips`, async (_t) => {
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

	// zstdDecompressStream with explicit params
	// (covers `params ? {..., params} : streamOptions` ConditionalExpression survivor).
	test(`${variant}: zstdDecompressStream with explicit params round-trips`, async (_t) => {
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

	// brotliDecompressStream with params option
	// (covers `params ? {..., params} : streamOptions` ConditionalExpression survivor).
	test(`${variant}: brotliDecompressStream with params option round-trips`, async (_t) => {
		const { constants: zlibConst } = await import("node:zlib");
		const input = brotliCompressSync(compressibleBody);
		// BROTLI_PARAM_MODE is a valid decompress-time param (mode 0 = generic)
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

// *** maxOutputSize exact boundary (> vs >=) *** //
// Payload whose decompressed size exactly equals maxOutputSize must PASS.
// `outputSize > maxOutputSize` allows equal; `>=` would reject it.
if (variant === "node") {
	test(`${variant}: gzipDecompressStream exact-boundary maxOutputSize should NOT throw`, async (_t) => {
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

	test(`${variant}: deflateDecompressStream exact-boundary maxOutputSize should NOT throw`, async (_t) => {
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

	test(`${variant}: brotliDecompressStream exact-boundary maxOutputSize should NOT throw`, async (_t) => {
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

	test(`${variant}: zstdDecompressStream exact-boundary maxOutputSize should NOT throw`, async (_t) => {
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

	// One byte under the limit MUST throw.
	test(`${variant}: gzipDecompressStream one-byte-under maxOutputSize should throw`, async (_t) => {
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

// *** error message label strings (StringLiteral survivors) *** //
// Error message must contain "Compression"/"Decompression" (not "").
if (variant === "node") {
	test(`${variant}: gzipDecompressStream error message contains "Decompression"`, async (_t) => {
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

	test(`${variant}: gzipCompressStream error message contains "Compression"`, async (_t) => {
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

	test(`${variant}: deflateDecompressStream error message contains "Decompression"`, async (_t) => {
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

	test(`${variant}: deflateCompressStream error message contains "Compression"`, async (_t) => {
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

	test(`${variant}: brotliDecompressStream error message contains "Decompression"`, async (_t) => {
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

	test(`${variant}: brotliCompressStream error message contains "Compression"`, async (_t) => {
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

	test(`${variant}: zstdDecompressStream error message contains "Decompression"`, async (_t) => {
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

	test(`${variant}: zstdCompressStream error message contains "Compression"`, async (_t) => {
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

	// Error message includes the maxOutputSize number (covers StringLiteral survivor).
	test(`${variant}: gzipDecompressStream error message includes the byte limit value`, async (_t) => {
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

// *** restore after "close" event (StringLiteral "close" → "") *** //
// After a stream closes normally, a second independent stream must still work.
if (variant === "node") {
	test(`${variant}: gzipDecompressStream second invocation works after first closes`, async (_t) => {
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

	// After an error event fires the push override must be restored
	// (StringLiteral "error" → "").
	test(`${variant}: gzipDecompressStream works correctly after previous maxOutputSize error`, async (_t) => {
		const input = gzipSync(compressibleBody);
		try {
			await pipeline([
				createReadableStream(input),
				gzipDecompressStream({ maxOutputSize: 10 }),
			]);
		} catch (_e) {
			// expected
		}
		const out = await streamToString(
			pipejoin([createReadableStream(input), gzipDecompressStream()]),
		);
		strictEqual(out, compressibleBody);
	});
}

// *** maxOutputSize: null on compress streams (LogicalOperator || survivor) *** //
// `||` mutation: `null !== null || null !== void 0` = true → guardOutput(null) → throws.
// These tests verify null disables the guard on compress streams.
if (variant === "node") {
	test(`${variant}: gzipCompressStream maxOutputSize:null should NOT throw`, async (_t) => {
		const output = await streamToString(
			pipejoin([
				createReadableStream(compressibleBody),
				gzipCompressStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(output, gzipSync(compressibleBody).toString());
	});

	test(`${variant}: deflateCompressStream maxOutputSize:null should NOT throw`, async (_t) => {
		const output = await streamToString(
			pipejoin([
				createReadableStream(compressibleBody),
				deflateCompressStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(output, deflateSync(compressibleBody).toString());
	});

	test(`${variant}: brotliCompressStream maxOutputSize:null should NOT throw`, async (_t) => {
		const out = await streamToBuffer(
			pipejoin([
				createReadableStream(compressibleBody),
				brotliCompressStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(brotliDecompressSync(out).toString(), compressibleBody);
	});

	test(`${variant}: zstdCompressStream maxOutputSize:null should NOT throw`, async (_t) => {
		const out = await streamToBuffer(
			pipejoin([
				createReadableStream(compressibleBody),
				zstdCompressStream({ maxOutputSize: null }),
			]),
		);
		strictEqual(zstdDecompressSync(out).toString(), compressibleBody);
	});
}

// *** zstd quality output size differs (kills ObjectLiteral `params ?? {}`) *** //
// With a 100KB repetitive body, quality:1 and quality:19 DO produce different sizes.
// `params ?? {}` loses the quality → both use default level → same output size.
if (variant === "node") {
	const bigRepeat = "x".repeat(100_000);
	test(`${variant}: zstdCompressStream quality:1 differs from quality:19 on large body`, async (_t) => {
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

// *** default export usability (ObjectLiteral `default = {}` survivor) *** //
// The default export must expose compressStream/decompressStream functions;
// if the object is `{}` those calls would throw TypeError.
if (variant === "node") {
	test(`${variant}: gzip default export compressStream/decompressStream round-trips`, async (_t) => {
		const gzipMod = await import("@datastream/compress/gzip");
		const out = await streamToString(
			pipejoin([
				createReadableStream(compressibleBody),
				gzipMod.default.compressStream(),
				gzipMod.default.decompressStream(),
			]),
		);
		strictEqual(out, compressibleBody);
	});

	test(`${variant}: deflate default export compressStream/decompressStream round-trips`, async (_t) => {
		const defMod = await import("@datastream/compress/deflate");
		const out = await streamToString(
			pipejoin([
				createReadableStream(compressibleBody),
				defMod.default.compressStream(),
				defMod.default.decompressStream(),
			]),
		);
		strictEqual(out, compressibleBody);
	});

	test(`${variant}: brotli default export compressStream/decompressStream round-trips`, async (_t) => {
		const brotliMod = await import("@datastream/compress/brotli");
		const out = await streamToString(
			pipejoin([
				createReadableStream(compressibleBody),
				brotliMod.default.compressStream(),
				brotliMod.default.decompressStream(),
			]),
		);
		strictEqual(out, compressibleBody);
	});

	test(`${variant}: zstd default export compressStream/decompressStream round-trips`, async (_t) => {
		const zstdMod = await import("@datastream/compress/zstd");
		const out = await streamToString(
			pipejoin([
				createReadableStream(compressibleBody),
				zstdMod.default.compressStream(),
				zstdMod.default.decompressStream(),
			]),
		);
		strictEqual(out, compressibleBody);
	});
}

// *** AbortSignal honored on node compress/decompress streams *** //
// Aborting mid-flight must cause the pipeline to reject with an AbortError (the
// signal is forwarded into the underlying zlib stream); without signal support
// the pipeline would resolve successfully. A large payload + small chunkSize
// guarantees the stream is still in-flight when the signal fires.
const signalHonored = (e) =>
	e.name === "AbortError" || e.code === "ABORT_ERR" || /abort/i.test(e.message);
const bigBody = "x".repeat(2_000_000);
if (variant === "node") {
	test(`${variant}: gzipCompressStream should reject when signal aborts`, async (_t) => {
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

	test(`${variant}: deflateDecompressStream should reject when signal aborts`, async (_t) => {
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

// *** maxOutputSize:null on decompress streams (ungated — works under both runs) *** //
// Covers the `maxOutputSize === null ? void 0 :` true-branch in each decompressor.
// The gzip variant is also covered inside the node-only block; these ungated
// copies ensure the branch fires in the webstream run too.
test(`${variant}: gzipDecompressStream maxOutputSize:null should NOT throw`, async (_t) => {
	const input = gzipSync(compressibleBody);
	const output = await streamToString(
		pipejoin([
			createReadableStream(input),
			gzipDecompressStream({ maxOutputSize: null }),
		]),
	);
	strictEqual(output, compressibleBody);
});

test(`${variant}: deflateDecompressStream maxOutputSize:null should NOT throw`, async (_t) => {
	const input = deflateSync(compressibleBody);
	const output = await streamToString(
		pipejoin([
			createReadableStream(input),
			deflateDecompressStream({ maxOutputSize: null }),
		]),
	);
	strictEqual(output, compressibleBody);
});

test(`${variant}: brotliDecompressStream maxOutputSize:null should NOT throw`, async (_t) => {
	const input = brotliCompressSync(compressibleBody);
	const output = await streamToString(
		pipejoin([
			createReadableStream(input),
			brotliDecompressStream({ maxOutputSize: null }),
		]),
	);
	strictEqual(output, compressibleBody);
});

// *** guardOutput Buffer.byteLength(chunk) fallback (ungated — both runs) *** //
// guardOutput overrides stream.push and sizes each chunk via
// `chunk.byteLength ?? Buffer.byteLength(chunk)`. zlib only ever pushes Buffers
// (which have .byteLength), so the Buffer.byteLength fallback is reached only
// when a non-BufferSource (a string) is pushed. We grab the guarded stream and
// push a string directly: byteLength is undefined, so the fallback runs.
test(`${variant}: gzipDecompressStream guardOutput sizes string chunks via Buffer.byteLength`, (_t) => {
	const stream = gzipDecompressStream({ maxOutputSize: 1024 });
	ok(stream.push("abc") === true);
	stream.destroy();
});

test(`${variant}: deflateDecompressStream guardOutput sizes string chunks via Buffer.byteLength`, (_t) => {
	const stream = deflateDecompressStream({ maxOutputSize: 1024 });
	ok(stream.push("abc") === true);
	stream.destroy();
});

test(`${variant}: brotliDecompressStream guardOutput sizes string chunks via Buffer.byteLength`, (_t) => {
	const stream = brotliDecompressStream({ maxOutputSize: 1024 });
	ok(stream.push("abc") === true);
	stream.destroy();
});

// String chunk larger than maxOutputSize must trip the guard: this exercises
// Buffer.byteLength(chunk) on the string-sizing path together with the
// `outputSize > maxOutputSize` true branch. push() returns false synchronously.
test(`${variant}: gzipDecompressStream guardOutput rejects oversized string chunk`, (_t) => {
	const stream = gzipDecompressStream({ maxOutputSize: 2 });
	// Swallow the destroy() error so it does not surface as an unhandled error.
	stream.on("error", () => {});
	strictEqual(stream.push("abcdef"), false);
	stream.destroy();
});

if (variant === "webstream") {
	test(`${variant}: gzipDecompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: deflateDecompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: gzipCompressStream should enforce maxOutputSize`, async (_t) => {
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

	test(`${variant}: deflateCompressStream should enforce maxOutputSize`, async (_t) => {
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

	// Brotli compress maxOutputSize — covers guardOutput invocation on the
	// compress path (lines 39-41 in brotli.node.mjs) which is gated to node-only
	// in the decompression-bomb block but must also fire in the webstream run.
	test(`${variant}: brotliCompressStream should enforce maxOutputSize`, async (_t) => {
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

	// brotliDecompressStream with params — covers the `params ?` branch in
	// brotliDecompressStream (line 46 in brotli.node.mjs).
	test(`${variant}: brotliDecompressStream with params should round-trip`, async (_t) => {
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

// *** web (*.web.js) sources, exercised directly *** //
// Node's package export resolution always matches the built-in `node`
// condition first, so `@datastream/compress/<algo>` (and even the
// `/<algo>/webstream` subpath under `node --test`) never loads the *.web.js
// implementations. To actually run the browser code paths we import the
// *.web.js sources by file URL (the same pattern used in packages/kafka),
// drive them through the web build of @datastream/core, and -- because
// brotli-wasm's ESM entry loads its wasm via fetch (unavailable for file://
// URLs under Node) -- remap the `brotli-wasm` specifier to its synchronous
// node build via a module customization hook. The streaming class API is
// identical across brotli-wasm builds; only wasm loading differs.
{
	const repo = new URL("../../", import.meta.url).pathname;
	const coreWebUrl = pathToFileURL(`${repo}packages/core/index.web.mjs`).href;
	// Resolve through realpathSync so the brotli-wasm CJS node build is loaded via
	// its canonical path. Under a Stryker sandbox `${repo}node_modules` is a
	// symlink, and importing the CJS module through the symlinked path yields an
	// empty namespace (DecompressStream undefined); the canonical path avoids that.
	const brotliWasmNodeUrl = pathToFileURL(
		realpathSync(`${repo}node_modules/brotli-wasm/index.node.js`),
	).href;
	const loaderSource = `
		export async function resolve(specifier, context, nextResolve) {
			if (specifier === "brotli-wasm") {
				return { url: ${JSON.stringify(brotliWasmNodeUrl)}, shortCircuit: true };
			}
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
		import(pathToFileURL(`${repo}packages/compress/${file}`).href);

	const webCore = await import(coreWebUrl);
	const textDecoder = new TextDecoder();
	const toText = (stream) =>
		webCore.streamToBuffer(stream).then((buffer) => textDecoder.decode(buffer));
	// node:zlib *Sync helpers return a Buffer that is a view into Node's shared
	// allocation pool; copy it into a tightly-sized Uint8Array so the web
	// ReadableStream feeds the exact compressed bytes (and nothing trailing).
	const webInput = (buffer) => Uint8Array.from(buffer);

	// *** brotli web *** //
	const brotliWeb = await webModule("brotli.web.js");

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
		// 200KB > OUTPUT_SIZE (16KB) exercises the NeedsMoreOutput loop.
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
		// Append garbage after a complete brotli stream; a strict decoder must
		// not silently drop it.
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

	// *** gzip web *** //
	const gzipWeb = await webModule("gzip.web.js");

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
			constructor() {
				const ts = new TransformStream({
					transform(chunk, controller) {
						if (!isBufferSource(chunk)) {
							controller.error(
								new TypeError("CompressionStream chunk is not a BufferSource"),
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
				`${pathToFileURL(`${repo}packages/compress/gzip.web.js`).href}?strict=gzip`
			);
			// Identity compressor => round-trips through the (real) decompress on
			// the bytes the strict compressor accepted; the test fails with a
			// TypeError if the factory forwards string chunks to CompressionStream.
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

	// *** deflate web *** //
	const deflateWeb = await webModule("deflate.web.js");

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
				`${pathToFileURL(`${repo}packages/compress/deflate.web.js`).href}?strict=deflate`
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

	// *** zstd web (unsupported in browsers) *** //
	const zstdWeb = await webModule("zstd.web.js");

	test("web-direct: zstdCompressStream should throw Not supported", (_t) => {
		try {
			zstdWeb.zstdCompressStream();
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("Not supported"));
		}
	});

	test("web-direct: zstdDecompressStream should throw Not supported", (_t) => {
		try {
			zstdWeb.zstdDecompressStream();
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("Not supported"));
		}
	});
}

// *** guardOutput internals (node *.node.mjs sources) *** //
// These tests pin the monkey-patched push guard installed by guardOutput and the
// limit-resolution branches in each <algo>{Compress,Decompress}Stream. They run
// only under --conditions=node because they import the node builds directly.
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
		// guardOutput must size string chunks via Buffer.byteLength (the `??`
		// fallback). With a tiny limit, pushing a string longer than the limit must
		// trip the guard: push() returns false. The `&&` mutant makes outputSize
		// NaN so the guard never trips and push() returns true.
		test(`${variant}: ${name}DecompressStream guardOutput sizes string chunks and trips limit`, (_t) => {
			const stream = factory({ maxOutputSize: 2 });
			stream.on("error", () => {});
			strictEqual(stream.push("abc"), false); // 3 bytes > 2 -> destroyed
			stream.destroy();
		});

		// Within the limit, push() returns true and the guard does not trip.
		test(`${variant}: ${name}DecompressStream guardOutput passes string chunks within limit`, (_t) => {
			const stream = factory({ maxOutputSize: 1024 });
			strictEqual(stream.push("abc"), true);
			stream.destroy();
		});

		// restore() resets stream.push on 'error'. If restore is a no-op, or the
		// 'error' listener is registered under the wrong event name, push stays the
		// wrapped function.
		test(`${variant}: ${name}DecompressStream restores push on error`, (_t) => {
			const stream = factory({ maxOutputSize: 100 });
			const wrapped = stream.push;
			stream.on("error", () => {});
			stream.emit("error", new Error("boom"));
			ok(stream.push !== wrapped, "push must be restored after error");
			stream.destroy();
		});

		// restore() resets stream.push on 'close' too (separate listener).
		test(`${variant}: ${name}DecompressStream restores push on close`, (_t) => {
			const stream = factory({ maxOutputSize: 100 });
			const wrapped = stream.push;
			stream.emit("close");
			ok(stream.push !== wrapped, "push must be restored after close");
			stream.destroy();
		});

		// maxOutputSize:null disables the limit entirely: NO guard is installed, so
		// the stream carries no extra close/error listeners. The `false ? ...` and
		// `if (limit !== undefined) -> if (true)` mutants would install the guard.
		test(`${variant}: ${name}DecompressStream maxOutputSize:null installs no guard`, (_t) => {
			const stream = factory({ maxOutputSize: null });
			strictEqual(stream.listenerCount("close"), 0);
			strictEqual(stream.listenerCount("error"), 0);
			stream.destroy();
		});

		// The default (no maxOutputSize) DOES install the guard (DEFAULT ceiling).
		test(`${variant}: ${name}DecompressStream default installs the guard`, (_t) => {
			const stream = factory();
			strictEqual(stream.listenerCount("close"), 1);
			strictEqual(stream.listenerCount("error"), 1);
			stream.destroy();
		});
	}

	for (const [name, factory] of Object.entries(compressFactories)) {
		// No maxOutputSize on a compress stream => no guard => no extra listeners.
		// The `maxOutputSize !== undefined -> true` mutant would install the guard.
		test(`${variant}: ${name}CompressStream without maxOutputSize installs no guard`, (_t) => {
			const stream = factory();
			strictEqual(stream.listenerCount("close"), 0);
			strictEqual(stream.listenerCount("error"), 0);
			stream.destroy();
		});

		// maxOutputSize:null on a compress stream also installs no guard.
		test(`${variant}: ${name}CompressStream maxOutputSize:null installs no guard`, (_t) => {
			const stream = factory({ maxOutputSize: null });
			strictEqual(stream.listenerCount("close"), 0);
			strictEqual(stream.listenerCount("error"), 0);
			stream.destroy();
		});

		// A finite maxOutputSize installs the guard.
		test(`${variant}: ${name}CompressStream with maxOutputSize installs the guard`, (_t) => {
			const stream = factory({ maxOutputSize: 1024 });
			strictEqual(stream.listenerCount("close"), 1);
			strictEqual(stream.listenerCount("error"), 1);
			stream.destroy();
		});
	}

	// ObjectLiteral: brotli/zstd decompress forward `{ ...streamOptions, params }`
	// when params are supplied. The `{}` mutant drops streamOptions, so chunkSize
	// reverts to the zlib default (16384) instead of the supplied value.
	test(`${variant}: brotliDecompressStream forwards streamOptions when params given`, (_t) => {
		const stream = brotliDecompressStream(
			{ params: { [zlibConstants.BROTLI_DECODER_PARAM_LARGE_WINDOW]: 0 } },
			{ chunkSize: 8192 },
		);
		strictEqual(stream._chunkSize, 8192);
		stream.destroy();
	});

	test(`${variant}: zstdDecompressStream forwards streamOptions when params given`, (_t) => {
		const stream = zstdDecompressStream(
			{ params: { [zlibConstants.ZSTD_d_windowLogMax]: 27 } },
			{ chunkSize: 8192 },
		);
		strictEqual(stream._chunkSize, 8192);
		stream.destroy();
	});
}
