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
// A chunk may also be a message envelope carrying the frame in `value`, e.g.
// kafkaConsumeStream's { topic, partition, offset, key, value, ... }. Its other
// fields are copied onto the emitted envelope, so kafkaCommitStream can still
// commit the message after it has been unframed and decoded.
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

// A chunk with a `value` field is a message envelope (see the top of this
// file). Bytes, ArrayBuffers and strings have no `value`, so they stay frames.
// A null value (a Kafka tombstone) still counts, and asBytes rejects it. A
// stream chunk is never null/undefined, so no optional chaining.
const asEnvelope = (chunk) => (chunk.value === undefined ? undefined : chunk);

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

// `outputLimit` is the byte cap with null already mapped to Infinity;
// `message` is the RangeError message for exceeding it.
const collectStream = async (readable, outputLimit, message) => {
	const reader = readable.getReader();
	const chunks = [];
	let total = 0;
	while (true) {
		const { value, done } = await reader.read();
		if (done) break;
		total += value.byteLength;
		if (total > outputLimit) {
			await reader.cancel();
			throw new RangeError(message);
		}
		chunks.push(value);
	}
	return concatBytes(chunks);
};

// Pipe bytes through a (De)CompressionStream and collect the output, bounded
// by maxOutputSize. Input is written in small slices, each write awaiting the
// previous one: a browser DecompressionStream inflates a whole written chunk
// synchronously, so writing the frame in one go would let a ~1MB zlib bomb
// allocate ~1GB before collectStream could check the limit. Once collectStream
// cancels the readable, the next write rejects and the loop stops.
const WRITE_SLICE_SIZE = 16_384;
const writeSlices = async (writer, bytes) => {
	for (let offset = 0; offset < bytes.byteLength; offset += WRITE_SLICE_SIZE) {
		await writer.write(bytes.subarray(offset, offset + WRITE_SLICE_SIZE));
	}
	await writer.close();
};
const transcode = async (transform, bytes, maxOutputSize, outputLimit) => {
	const writer = transform.writable.getWriter();
	// Errors on the write side surface via the read side; we await both so
	// failures (e.g. malformed zlib, backpressure aborts) propagate cleanly.
	const writeP = writeSlices(writer, bytes);
	try {
		const result = await collectStream(
			transform.readable,
			outputLimit,
			`schema-registry: maxOutputSize exceeded (${maxOutputSize})`,
		);
		await writeP;
		return result;
	} catch (err) {
		// Swallow the writeP rejection after collect throws — read-side error
		// is the underlying cause.
		writeP.catch(noop);
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
		const envelope = asEnvelope(chunk);
		const bytes = asBytes(envelope ? envelope.value : chunk);
		if (bytes.byteLength < 5 || bytes[0] !== 0x00) {
			throw new Error(
				"confluentUnframeStream: missing 0x00 magic byte / frame is too short (each chunk must be one whole frame)",
			);
		}
		// uint32 BE by hand: no DataView per frame. The top byte is scaled
		// arithmetically (not shifted) so ids >= 2^31 stay positive.
		const schemaId =
			bytes[1] * 0x1000000 + ((bytes[2] << 16) | (bytes[3] << 8) | bytes[4]);
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
		// Spreading undefined adds nothing, so a bare frame emits only these keys.
		enqueue({ ...envelope, schemaId, payload: bytes.subarray(5) });
	};
	const stream = createTransformStream(transform, streamOptions);
	// `.result()` reports every distinct id seen rather than throwing on mixed
	// ids: pipeline() always calls result(), so a throw would reject a run
	// after all of its data had already been processed. Per-chunk schema
	// selection still belongs on the envelope { schemaId, payload }.
	stream.result = () => ({ key: resultKey ?? "confluentSchemaId", value });
	return stream;
};

// Looks schema ids up in a Confluent Schema Registry (GET /schemas/ids/{id}).
// Same cache/eviction/dedup semantics as @datastream/aws
// awsGlueSchemaRegistryResolver, and both results carry schemaDefinition and
// dataFormat, but this one is keyed by schemaId (Glue: schemaVersionId) and
// adds the registry's references.
export const confluentSchemaRegistryResolver = ({
	url,
	headers,
	cacheExpiry = -1,
	maxCacheSize = 1000,
	timeout = 30_000,
	signal,
	maxBodySize = 1_048_576, // 1MiB
	fetch = globalThis.fetch,
} = {}) => {
	if (typeof url !== "string" || !URL.canParse(url)) {
		throw new TypeError(
			"confluentSchemaRegistryResolver: url must be an absolute URL",
		);
	}
	// Append to the path, not the string: a base with a query string
	// (?tenant=a) must keep it after /schemas/ids/{id}.
	const baseUrl = new URL(url);
	const basePath = baseUrl.pathname.replace(/\/$/, "");
	// null = no body cap
	const bodyLimit = maxBodySize ?? Number.POSITIVE_INFINITY;
	// null = unbounded cache
	const cacheCap = maxCacheSize ?? Number.POSITIVE_INFINITY;
	// Map preserves insertion order, so we get O(1) FIFO eviction by deleting
	// the oldest key when we hit the cap.
	const cache = new Map(); // schemaId -> { value, expires }
	// Dedup in-flight lookups so parallel resolves for the same id don't fan
	// out to N registry calls.
	const inflight = new Map(); // schemaId -> Promise<value>

	const lookup = async (schemaId) => {
		const requestUrl = new URL(baseUrl);
		requestUrl.pathname = `${basePath}/schemas/ids/${schemaId}`;
		// The signal also aborts reading the body, so a registry that stalls
		// mid-response is bounded by the same timeout.
		const timeoutSignal = AbortSignal.timeout(timeout);
		const response = await fetch(requestUrl.href, {
			headers,
			signal: signal ? AbortSignal.any([timeoutSignal, signal]) : timeoutSignal,
		});
		if (!response.ok) {
			// Release the connection; the error body is not needed.
			await response.body?.cancel();
			throw new Error(
				`confluentSchemaRegistryResolver: schema ${schemaId} lookup failed (HTTP ${response.status})`,
				{ cause: { status: response.status } },
			);
		}
		if (!response.body) {
			throw new Error(
				`confluentSchemaRegistryResolver: schema ${schemaId} lookup returned no body`,
			);
		}
		// response.json() would buffer a body of any size; the registry is a
		// trust boundary, so cap it.
		const bytes = await collectStream(
			response.body,
			bodyLimit,
			`confluentSchemaRegistryResolver: schema ${schemaId} response exceeds maxBodySize (${maxBodySize} bytes)`,
		);
		const body = JSON.parse(new TextDecoder().decode(bytes));
		// The registry omits schemaType for Avro, its original (default) format,
		// and references when the schema has none.
		return {
			schemaId,
			schemaDefinition: body.schema,
			dataFormat: body.schemaType ?? "AVRO",
			references: body.references ?? [],
		};
	};

	return async (schemaId) => {
		// The id goes into the request path, so it must be the same unsigned
		// 32-bit integer the wire format carries (confluentFrameStream's check).
		if (!Number.isInteger(schemaId) || schemaId < 0 || schemaId > 0xffffffff) {
			throw new TypeError(
				"confluentSchemaRegistryResolver: schemaId must be an unsigned 32-bit integer",
			);
		}
		const hit = cache.get(schemaId);
		// expires === -1 is the never-expires sentinel; otherwise it is an absolute
		// timestamp and the entry is fresh while it is still in the future.
		if (hit && (hit.expires === -1 || hit.expires > Date.now())) {
			return hit.value;
		}
		// Drop the stale entry so the refreshed one is set() at the Map's tail,
		// keeping eviction true-FIFO.
		cache.delete(schemaId);
		const pending = inflight.get(schemaId);
		if (pending) return pending;

		const promise = (async () => {
			try {
				const value = await lookup(schemaId);
				// maxCacheSize <= 0 disables caching; evicting down to a non-positive
				// cap would otherwise spin forever on an empty Map.
				if (cacheCap > 0) {
					while (cache.size >= cacheCap) {
						cache.delete(cache.keys().next().value);
					}
					cache.set(schemaId, {
						value,
						expires: cacheExpiry < 0 ? -1 : Date.now() + cacheExpiry,
					});
				}
				return value;
			} finally {
				inflight.delete(schemaId);
			}
		})();
		inflight.set(schemaId, promise);
		return promise;
	};
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
	const emit = (envelope, schemaVersionId, kind, payload, enqueue) => {
		value.compression = kind;
		enqueue({ ...envelope, schemaVersionId, compression: kind, payload });
	};
	const transform = (chunk, enqueue) => {
		const envelope = asEnvelope(chunk);
		const bytes = asBytes(envelope ? envelope.value : chunk);
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
		if (compressionByte === GLUE_COMPRESSION_NONE) {
			emit(envelope, schemaVersionId, "none", framedPayload, enqueue);
			return;
		}
		if (compressionByte === GLUE_COMPRESSION_ZLIB) {
			return transcode(
				new DecompressionStream("deflate"),
				framedPayload,
				maxOutputSize,
				outputLimit,
			).then((payload) =>
				emit(envelope, schemaVersionId, "zlib", payload, enqueue),
			);
		}
		throw new Error(
			`glueUnframeStream: unsupported compression byte 0x${compressionByte.toString(16).padStart(2, "0")}`,
		);
	};
	const stream = createTransformStream(transform, streamOptions);
	// See confluentUnframeStream: report every distinct id rather than throw.
	stream.result = () => ({ key: resultKey ?? "glueSchemaVersionId", value });
	return stream;
};
