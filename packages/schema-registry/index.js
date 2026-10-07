// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global CompressionStream, DecompressionStream */
import { concatBytes, createTransformStream } from "@datastream/core";

// Each frame/unframe transform treats one input chunk as one WHOLE Schema
// Registry envelope. This matches the Kafka use case (one Kafka message = one
// chunk = one Schema Registry-framed record). Envelopes carry no embedded
// length, so a frame split across chunks cannot be reassembled reliably (a
// continuation slice may itself start with the magic byte); callers piping
// concatenated or sliced framed buffers must re-frame them upstream (e.g. via
// a length-prefix transform). Each chunk is decoded and emitted immediately,
// so a live consumer's latest message is never held back waiting for the next.
//
// Unframe streams emit { schemaId | schemaVersionId, payload } envelopes
// downstream so downstream decoders (e.g. protobufDecodeStream) can pick the
// right schema per chunk without sharing mutable state via `.result()`. The
// `.result()` accessor is still exposed for parity with the csvDetect pattern:
// it lists every distinct id seen plus the *most recently seen* one, which is
// racy under backpressure — prefer the per-chunk envelope when wiring a decoder.

const GLUE_COMPRESSION_NONE = 0x00;
const GLUE_COMPRESSION_ZLIB = 0x05;
const UUID_HEX_RE = /^[0-9a-fA-F]{32}$/;
// Cap on distinct ids an unframe stream records for .result(): a long-lived
// consumer must not grow its id list forever. null opts out.
const DEFAULT_MAX_SCHEMA_IDS = 1000;
// Cap on bytes a zlib (de)compression buffers per frame, matching
// @datastream/compress. null opts out.
const DEFAULT_MAX_OUTPUT_SIZE = 268_435_456; // 256MiB

// Shared no-op for swallowing secondary promise rejections in error-handling
// catch blocks (see transcode). Using a named reference avoids creating
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

// `outputLimit` is maxOutputSize with null already mapped to Infinity.
const collectStream = async (readable, maxOutputSize, outputLimit) => {
	const reader = readable.getReader();
	const chunks = [];
	let total = 0;
	while (true) {
		const { value, done } = await reader.read();
		if (done) break;
		total += value.byteLength;
		if (total > outputLimit) {
			await reader.cancel();
			throw new RangeError(
				`schema-registry: maxOutputSize exceeded (${maxOutputSize})`,
			);
		}
		chunks.push(value);
	}
	return concatBytes(chunks);
};

// Pipe bytes through a (De)CompressionStream and collect the output, bounded
// by maxOutputSize. inflate and deflate differ only in the stream, so they
// share this helper.
const transcode = async (transform, bytes, maxOutputSize, outputLimit) => {
	const writer = transform.writable.getWriter();
	// Errors on the write side surface via the read side; we await both so
	// failures (e.g. malformed zlib, backpressure aborts) propagate cleanly.
	const writeP = writer.write(bytes);
	const closeP = writer.close();
	try {
		const result = await collectStream(
			transform.readable,
			maxOutputSize,
			outputLimit,
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
		enqueue(concatBytes([header, asBytes(chunk)]));
	};
	const stream = createTransformStream(transform, streamOptions);
	// For frame streams `.result()` echoes the schemaId the caller configured
	// (not detected). Its default key differs from confluentUnframeStream's so
	// both can share one pipeline result.
	stream.result = () => ({
		key: resultKey ?? "confluentFrameSchemaId",
		value,
	});
	return stream;
};

export const confluentUnframeStream = (
	{ maxSchemaIds = DEFAULT_MAX_SCHEMA_IDS, resultKey } = {},
	streamOptions = {},
) => {
	// schemaId: most recently seen; schemaIds: distinct ids, first-seen order,
	// up to maxSchemaIds; untrackedSchemaIds: frames whose new id was past the cap.
	const value = { schemaId: null, schemaIds: [], untrackedSchemaIds: 0 };
	const seenIds = new Set();
	const idLimit = maxSchemaIds ?? Number.POSITIVE_INFINITY;
	const transform = (chunk, enqueue) => {
		const bytes = asBytes(chunk);
		if (bytes.byteLength < 5 || bytes[0] !== 0x00) {
			throw new Error(
				"confluentUnframeStream: missing 0x00 magic byte / frame is too short (each chunk must be one whole frame)",
			);
		}
		const schemaId = new DataView(
			bytes.buffer,
			bytes.byteOffset,
			bytes.byteLength,
		).getUint32(1, false);
		if (!seenIds.has(schemaId)) {
			if (seenIds.size < idLimit) {
				seenIds.add(schemaId);
				value.schemaIds.push(schemaId);
			} else {
				value.untrackedSchemaIds += 1;
			}
		}
		value.schemaId = schemaId;
		// subarray (zero-copy) — the source bytes outlive the chunk.
		enqueue({ schemaId, payload: bytes.subarray(5) });
	};
	const stream = createTransformStream(transform, streamOptions);
	// `.result()` reports every distinct id seen rather than throwing on mixed
	// ids: pipeline() always calls result(), so a throw would reject a run
	// after all of its data had already been processed. Per-chunk schema
	// selection still belongs on the envelope { schemaId, payload }.
	stream.result = () => ({ key: resultKey ?? "confluentSchemaId", value });
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
	{
		schemaVersionId,
		compression = "none",
		maxOutputSize = DEFAULT_MAX_OUTPUT_SIZE,
		resultKey,
	} = {},
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
	const outputLimit = maxOutputSize ?? Number.POSITIVE_INFINITY;
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
		// transcode() buffers the whole compressed result; bound it with
		// maxOutputSize, mirroring glueUnframeStream (no other buffering path
		// here is unbounded).
		const payload =
			compression === "zlib"
				? await transcode(
						new CompressionStream("deflate"),
						bytes,
						maxOutputSize,
						outputLimit,
					)
				: bytes;
		enqueue(concatBytes([header, payload]));
	};
	const stream = createTransformStream(transform, streamOptions);
	// Default key differs from glueUnframeStream's so both can share one
	// pipeline result.
	stream.result = () => ({
		key: resultKey ?? "glueFrameSchemaVersionId",
		value,
	});
	return stream;
};

export const glueUnframeStream = (
	{
		maxOutputSize = DEFAULT_MAX_OUTPUT_SIZE,
		maxSchemaIds = DEFAULT_MAX_SCHEMA_IDS,
		resultKey,
	} = {},
	streamOptions = {},
) => {
	// schemaVersionId/compression: most recently seen; schemaVersionIds: distinct
	// ids, first-seen order, up to maxSchemaIds; untrackedSchemaVersionIds:
	// frames whose new id was past the cap.
	const value = {
		schemaVersionId: null,
		compression: null,
		schemaVersionIds: [],
		untrackedSchemaVersionIds: 0,
	};
	const seenIds = new Set();
	const idLimit = maxSchemaIds ?? Number.POSITIVE_INFINITY;
	const outputLimit = maxOutputSize ?? Number.POSITIVE_INFINITY;
	const transform = async (chunk, enqueue) => {
		const bytes = asBytes(chunk);
		if (bytes.byteLength < 18 || bytes[0] !== 0x03) {
			throw new Error(
				"glueUnframeStream: missing 0x03 magic byte / frame is too short (each chunk must be one whole frame)",
			);
		}
		const compressionByte = bytes[1];
		const schemaVersionId = bytesToUuid(bytes, 2);
		if (!seenIds.has(schemaVersionId)) {
			if (seenIds.size < idLimit) {
				seenIds.add(schemaVersionId);
				value.schemaVersionIds.push(schemaVersionId);
			} else {
				value.untrackedSchemaVersionIds += 1;
			}
		}
		value.schemaVersionId = schemaVersionId;
		const framedPayload = bytes.subarray(18);
		let payload;
		let kind;
		if (compressionByte === GLUE_COMPRESSION_NONE) {
			kind = "none";
			payload = framedPayload;
		} else if (compressionByte === GLUE_COMPRESSION_ZLIB) {
			kind = "zlib";
			payload = await transcode(
				new DecompressionStream("deflate"),
				framedPayload,
				maxOutputSize,
				outputLimit,
			);
		} else {
			throw new Error(
				`glueUnframeStream: unsupported compression byte 0x${compressionByte.toString(16).padStart(2, "0")}`,
			);
		}
		value.compression = kind;
		enqueue({ schemaVersionId, compression: kind, payload });
	};
	const stream = createTransformStream(transform, streamOptions);
	// See confluentUnframeStream: report every distinct id rather than throw.
	stream.result = () => ({ key: resultKey ?? "glueSchemaVersionId", value });
	return stream;
};
