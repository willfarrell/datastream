import { ok, strictEqual } from "node:assert";
import test from "node:test";
import {
	brotliCompressSync,
	deflateSync,
	gzipSync,
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

if (variant === "node") {
	const { zstdCompressStream, zstdDecompressStream } = await import(
		"@datastream/compress/zstd"
	);

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
}

// *** web variant decompression bomb protection *** //
// *** protobuf *** //
let hasProtobuf = false;
try {
	const protobuf = await import("protobufjs");
	hasProtobuf = !!protobuf;
} catch {
	// protobufjs not installed
}

if (hasProtobuf) {
	const protobuf = await import("protobufjs");
	const { protobufSerializeStream, protobufDeserializeStream } = await import(
		"@datastream/compress/protobuf"
	);

	const TestType = new protobuf.Type("TestMessage")
		.add(new protobuf.Field("name", 1, "string"))
		.add(new protobuf.Field("value", 2, "int32"));
	new protobuf.Root().define("test").add(TestType);

	test(`${variant}: protobuf roundtrip serialize/deserialize`, async (_t) => {
		const input = [
			{ name: "a", value: 1 },
			{ name: "b", value: 2 },
		];
		const serialize = protobufSerializeStream({ Type: TestType });
		const deserialize = protobufDeserializeStream({ Type: TestType });
		const streams = [createReadableStream(input), serialize, deserialize];
		const output = await streamToString(pipejoin(streams));
		ok(output.includes("a"));
	});
}

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
}
