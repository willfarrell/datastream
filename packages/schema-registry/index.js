// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global CompressionStream, DecompressionStream */
import { createTransformStream } from "@datastream/core";

// Each frame/unframe transform treats one input chunk as one Schema Registry
// envelope. This matches the Kafka use case (one Kafka message = one chunk =
// one Schema Registry-framed record). Callers piping concatenated framed
// buffers must split them upstream (e.g. via a length-prefix transform).
//
// Unframe streams emit { schemaId | schemaVersionId, payload } envelopes
// downstream so downstream decoders (e.g. protobufDecodeStream) can pick the
// right schema per chunk without sharing mutable state via `.result()`. The
// `.result()` accessor is still exposed for parity with the csvDetect pattern,
// but reflects the *most recently seen* envelope and is racy under
// backpressure — prefer the per-chunk envelope when wiring a decoder.

const GLUE_COMPRESSION_NONE = 0x00;
const GLUE_COMPRESSION_ZLIB = 0x05;
const UUID_HEX_RE = /^[0-9a-fA-F]{32}$/;

// Shared no-op for swallowing secondary promise rejections in error-handling
// catch blocks (see inflate/deflate). Using a named reference avoids creating
// uncalled anonymous functions that inflate function-coverage miss counts.
const noop = () => {};

const asBytes = (chunk) => {
	if (typeof chunk === "string") return new TextEncoder().encode(chunk);
	// ArrayBuffer.isView covers typed arrays / DataView with possible byteOffset
	// (including Uint8Array — the resulting view is byte-identical to the input).
	if (ArrayBuffer.isView(chunk)) {
		return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
	}
	if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
	// Reject anything else (numbers, null, plain objects). The previous
	// `new Uint8Array(chunk.buffer ?? chunk)` silently turned a number N into an
	// N-byte zero-filled buffer instead of erroring.
	throw new TypeError(
		"schema-registry: chunk must be a Uint8Array, ArrayBuffer view, ArrayBuffer, or string",
	);
};

const concat = (parts) => {
	let total = 0;
	for (const p of parts) total += p.byteLength;
	const out = new Uint8Array(total);
	let offset = 0;
	for (const p of parts) {
		out.set(p, offset);
		offset += p.byteLength;
	}
	return out;
};

// Reassemble a single logical frame that createReadableStream may have
// auto-chunked into multiple sub-chunks. Schema Registry envelopes carry no
// embedded length, so we use the magic byte + minimum header length as the
// frame-boundary signal: a chunk that begins with `magic` and is at least
// `headerSize` bytes long starts a NEW frame; any other chunk is a
// continuation of the frame currently being buffered. This keeps the
// "one complete framed record per chunk" contract working (each such chunk
// starts with the magic byte) while correctly reassembling a >chunkSize frame
// that createReadableStream sliced into payload-only continuation chunks.
const createFrameBuffer = (magic, headerSize) => {
	let parts = null; // null => no frame in progress
	return {
		// Returns the completed previous frame (or undefined) and starts buffering
		// the incoming chunk.
		push(bytes) {
			let completed;
			const startsNewFrame =
				bytes.byteLength >= headerSize && bytes[0] === magic;
			if (startsNewFrame) {
				// Emit the previously buffered frame (if any) before starting fresh.
				// concat handles the common single-part case correctly too.
				if (parts !== null) completed = concat(parts);
				parts = [bytes];
			} else {
				// Continuation chunk. If nothing is in progress this is a malformed
				// lead chunk; surface it to the caller as `undefined` start + push so
				// the caller's header validation throws the right error.
				if (parts === null) parts = [bytes];
				else parts.push(bytes);
			}
			return completed;
		},
		flush() {
			if (parts === null) return undefined;
			const completed = concat(parts);
			parts = null;
			return completed;
		},
	};
};

const collectStream = async (readable, maxOutputSize, limitName) => {
	const reader = readable.getReader();
	const chunks = [];
	let total = 0;
	while (true) {
		const { value, done } = await reader.read();
		if (done) break;
		total += value.byteLength;
		if (maxOutputSize != null && total > maxOutputSize) {
			await reader.cancel();
			throw new Error(
				`schema-registry: ${limitName} exceeded (${maxOutputSize})`,
			);
		}
		chunks.push(value);
	}
	return concat(chunks);
};

const inflate = async (bytes, maxOutputSize) => {
	const ds = new DecompressionStream("deflate");
	const writer = ds.writable.getWriter();
	// Errors on the write side surface via the read side; we await both so
	// failures (e.g. malformed zlib, backpressure aborts) propagate cleanly.
	const writeP = writer.write(bytes);
	const closeP = writer.close();
	try {
		const result = await collectStream(
			ds.readable,
			maxOutputSize,
			"maxDecompressedBytes",
		);
		await writeP;
		await closeP;
		return result;
	} catch (err) {
		// Swallow writeP/closeP rejections after collect throws — read-side error
		// is the underlying cause.
		writeP.catch(noop);
		closeP.catch(noop);
		throw err;
	}
};

const deflate = async (bytes, maxOutputSize) => {
	const cs = new CompressionStream("deflate");
	const writer = cs.writable.getWriter();
	const writeP = writer.write(bytes);
	const closeP = writer.close();
	try {
		const result = await collectStream(
			cs.readable,
			maxOutputSize,
			"maxFrameBytes",
		);
		await writeP;
		await closeP;
		return result;
	} catch (err) {
		writeP.catch(noop);
		closeP.catch(noop);
		throw err;
	}
};

// *** Confluent (5-byte: 0x00 magic + uint32 BE schema id) *** //

export const confluentFrameStream = (
	{ schemaId, resultKey } = {},
	streamOptions = {},
) => {
	if (
		// Number.isInteger is false for every non-number, so it already rejects
		// strings/bigints/etc.; an explicit typeof check would be redundant.
		!Number.isInteger(schemaId) ||
		schemaId < 0 ||
		schemaId > 0xffffffff
	) {
		throw new TypeError(
			"confluentFrameStream: schemaId must be an unsigned 32-bit integer",
		);
	}
	const header = new Uint8Array(5);
	header[0] = 0x00;
	new DataView(header.buffer).setUint32(1, schemaId, false);
	const value = { schemaId };
	const transform = (chunk, enqueue) => {
		enqueue(concat([header, asBytes(chunk)]));
	};
	const stream = createTransformStream(transform, streamOptions);
	// For frame streams `.result()` echoes the schemaId the caller configured
	// (not detected). Listed under "confluentSchemaId" for symmetry with
	// unframe — see file header note.
	stream.result = () => ({
		key: resultKey ?? "confluentSchemaId",
		value,
	});
	return stream;
};

export const confluentUnframeStream = (
	{ resultKey } = {},
	streamOptions = {},
) => {
	const value = { schemaId: null };
	let distinctIds = 0;
	const frameBuffer = createFrameBuffer(0x00, 5);
	const parseAndEmit = (bytes, enqueue) => {
		if (bytes.byteLength < 5 || bytes[0] !== 0x00) {
			throw new Error(
				"confluentUnframeStream: missing 0x00 magic byte / frame is too short",
			);
		}
		const schemaId = new DataView(
			bytes.buffer,
			bytes.byteOffset,
			bytes.byteLength,
		).getUint32(1, false);
		if (value.schemaId !== schemaId) distinctIds += 1;
		value.schemaId = schemaId;
		// subarray (zero-copy) — the source bytes outlive the chunk.
		enqueue({ schemaId, payload: bytes.subarray(5) });
	};
	const transform = (chunk, enqueue) => {
		const completed = frameBuffer.push(asBytes(chunk));
		if (completed !== undefined) parseAndEmit(completed, enqueue);
	};
	const flush = (enqueue) => {
		const completed = frameBuffer.flush();
		if (completed !== undefined) parseAndEmit(completed, enqueue);
	};
	const stream = createTransformStream(transform, flush, streamOptions);
	// `.result()` reflects the most-recently-seen envelope and is racy under
	// backpressure (see file header). If more than one DISTINCT schema id was
	// observed, the single shared value is meaningless, so throw rather than
	// silently report a stale id — the per-chunk envelope is the correct API.
	stream.result = () => {
		if (distinctIds > 1) {
			throw new Error(
				"confluentUnframeStream.result(): stream carried multiple distinct schemaIds; use the per-chunk envelope { schemaId, payload } instead",
			);
		}
		return { key: resultKey ?? "confluentSchemaId", value };
	};
	return stream;
};

// *** Glue (18-byte: 0x03 magic + 1 byte compression + 16 bytes UUID) *** //

const uuidToBytes = (uuid) => {
	const hex = uuid.replaceAll("-", "");
	if (!UUID_HEX_RE.test(hex)) {
		throw new TypeError(
			`glueFrameStream: schemaVersionId must be a valid UUID (got ${uuid})`,
		);
	}
	// hex is exactly 32 chars (UUID_HEX_RE), i.e. 16 byte-pairs. Collect into a
	// plain array first so an over-long loop produces a >16-byte result (caught
	// downstream by header.set) instead of a silently-ignored out-of-bounds write
	// on a fixed-size typed array.
	const out = [];
	for (let i = 0; i < 16; i++) {
		out.push(Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16));
	}
	return new Uint8Array(out);
};

// Precomputed byte -> 2-char hex so bytesToUuid (called per unframed Glue
// message) avoids a per-byte toString(16)+padStart and the 5 trailing slices:
// index the table and concatenate the canonical 8-4-4-4-12 layout directly.
const HEX_BYTE = Array.from({ length: 256 }, (_, i) =>
	i.toString(16).padStart(2, "0"),
);
const bytesToUuid = (bytes, offset) => {
	const h = HEX_BYTE;
	return (
		`${h[bytes[offset]]}${h[bytes[offset + 1]]}${h[bytes[offset + 2]]}${h[bytes[offset + 3]]}-` +
		`${h[bytes[offset + 4]]}${h[bytes[offset + 5]]}-` +
		`${h[bytes[offset + 6]]}${h[bytes[offset + 7]]}-` +
		`${h[bytes[offset + 8]]}${h[bytes[offset + 9]]}-` +
		`${h[bytes[offset + 10]]}${h[bytes[offset + 11]]}${h[bytes[offset + 12]]}${h[bytes[offset + 13]]}${h[bytes[offset + 14]]}${h[bytes[offset + 15]]}`
	);
};

export const glueFrameStream = (
	{ schemaVersionId, compression = "none", maxFrameBytes, resultKey } = {},
	streamOptions = {},
) => {
	if (typeof schemaVersionId !== "string") {
		throw new TypeError("glueFrameStream: schemaVersionId required");
	}
	if (compression !== "none" && compression !== "zlib") {
		throw new TypeError(
			`glueFrameStream: unsupported compression "${compression}" (expected "none" or "zlib")`,
		);
	}
	const uuid = uuidToBytes(schemaVersionId);
	const compressionByte =
		compression === "zlib" ? GLUE_COMPRESSION_ZLIB : GLUE_COMPRESSION_NONE;
	const value = { schemaVersionId };
	// Pre-built 18-byte header — reused across chunks (only payload differs).
	const header = new Uint8Array(18);
	header[0] = 0x03;
	header[1] = compressionByte;
	header.set(uuid, 2);

	const transform = async (chunk, enqueue) => {
		const bytes = asBytes(chunk);
		// deflate() buffers the whole compressed result; bound it with the
		// optional maxFrameBytes ceiling, mirroring glueUnframeStream's
		// maxDecompressedBytes (no other buffering path here is unbounded).
		const payload =
			compression === "zlib" ? await deflate(bytes, maxFrameBytes) : bytes;
		enqueue(concat([header, payload]));
	};
	const stream = createTransformStream(transform, streamOptions);
	stream.result = () => ({
		key: resultKey ?? "glueSchemaVersionId",
		value,
	});
	return stream;
};

export const glueUnframeStream = (
	{ maxDecompressedBytes = 10 * 1024 * 1024, resultKey } = {},
	streamOptions = {},
) => {
	const value = { schemaVersionId: null, compression: null };
	let distinctIds = 0;
	const frameBuffer = createFrameBuffer(0x03, 18);
	const parseAndEmit = async (bytes, enqueue) => {
		if (bytes.byteLength < 18 || bytes[0] !== 0x03) {
			throw new Error(
				"glueUnframeStream: missing 0x03 magic byte / frame is too short",
			);
		}
		const compressionByte = bytes[1];
		const schemaVersionId = bytesToUuid(bytes, 2);
		if (value.schemaVersionId !== schemaVersionId) distinctIds += 1;
		value.schemaVersionId = schemaVersionId;
		const framedPayload = bytes.subarray(18);
		let payload;
		let kind;
		if (compressionByte === GLUE_COMPRESSION_NONE) {
			kind = "none";
			payload = framedPayload;
		} else if (compressionByte === GLUE_COMPRESSION_ZLIB) {
			kind = "zlib";
			payload = await inflate(framedPayload, maxDecompressedBytes);
		} else {
			throw new Error(
				`glueUnframeStream: unsupported compression byte 0x${compressionByte.toString(16).padStart(2, "0")}`,
			);
		}
		value.compression = kind;
		enqueue({ schemaVersionId, compression: kind, payload });
	};
	const transform = async (chunk, enqueue) => {
		const completed = frameBuffer.push(asBytes(chunk));
		if (completed !== undefined) await parseAndEmit(completed, enqueue);
	};
	const flush = async (enqueue) => {
		const completed = frameBuffer.flush();
		if (completed !== undefined) await parseAndEmit(completed, enqueue);
	};
	const stream = createTransformStream(transform, flush, streamOptions);
	// See confluentUnframeStream: throw rather than report a stale id when the
	// stream carried multiple distinct schemaVersionIds.
	stream.result = () => {
		if (distinctIds > 1) {
			throw new Error(
				"glueUnframeStream.result(): stream carried multiple distinct schemaVersionIds; use the per-chunk envelope { schemaVersionId, compression, payload } instead",
			);
		}
		return { key: resultKey ?? "glueSchemaVersionId", value };
	};
	return stream;
};

export default {
	confluentFrameStream,
	confluentUnframeStream,
	glueFrameStream,
	glueUnframeStream,
};
