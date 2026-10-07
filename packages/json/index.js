// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createChunkDecoder, createTransformStream } from "@datastream/core";

// A per-stream chunk decoder (streaming TextDecoder) keeps a multibyte char
// split across byte chunks intact; `buffer += chunk` decoded (node) or
// comma-joined (browser) each chunk on its own. String chunks pass through.

// Returns a per-stream function that drops a leading byte-order mark from the
// first non-empty text only. TextDecoder already strips it from byte input,
// but string chunks (e.g. a file read as text) keep it, and JSON.parse / the
// array scanner reject U+FEFF. Empty chunks don't count as "first", or a
// leading "" would let the BOM through. Later U+FEFF chars are data.
const createBomStripper = () => {
	let pending = true;
	return (text) => {
		if (!pending || text === "") return text;
		pending = false;
		return text.charCodeAt(0) === 0xfeff ? text.substring(1) : text;
	};
};

// Error map entry per id (csv parity): `idx` keeps at most maxErrorRows row
// indexes so a stream of bad rows can't grow memory without bound, while
// `count` records the true total. null = unlimited.
const createErrorTracker = (errors, maxErrorRows) => {
	const limit = maxErrorRows ?? Number.POSITIVE_INFINITY;
	return (id, message, idx) => {
		errors[id] ??= { id, message, idx: [], count: 0 };
		const error = errors[id];
		error.count += 1;
		if (error.idx.length < limit) error.idx.push(idx);
	};
};

const isWhitespace = (ch) =>
	ch === 0x20 || ch === 0x09 || ch === 0x0a || ch === 0x0d;

// --- NDJSON ---

export const ndjsonParseStream = (options = {}, streamOptions = {}) => {
	const {
		maxBufferSize = 16_777_216,
		maxErrorRows = 1_000,
		resultKey,
	} = options;
	const bufferLimit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	let buffer = "";
	let idx = 0;
	const errors = {};
	const decoder = createChunkDecoder();
	const stripBom = createBomStripper();
	const track = createErrorTracker(errors, maxErrorRows);
	const trackError = (id, message) => track(id, message, idx);

	const transform = (chunk, enqueue) => {
		if (buffer.length + chunk.length > bufferLimit) {
			throw new RangeError(
				`ndjsonParseStream buffer (${buffer.length + chunk.length}) exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		buffer += stripBom(decoder.decode(chunk));
		let pos = 0;
		while (true) {
			const nlIdx = buffer.indexOf("\n", pos);
			if (nlIdx === -1) {
				buffer = buffer.substring(pos);
				break;
			}
			const line = buffer.substring(pos, nlIdx);
			pos = nlIdx + 1;
			// Skip blank/whitespace-only lines. JSON.parse already tolerates
			// surrounding whitespace (incl. a trailing \r from \r\n), so the raw
			// line is parsed directly.
			if (!/\S/.test(line)) continue;
			try {
				enqueue(JSON.parse(line));
			} catch {
				trackError("ParseError", "Invalid JSON");
			}
			idx++;
		}
	};

	const flush = (enqueue) => {
		// Flush bytes held by the streaming decoder (an incomplete trailing
		// sequence becomes U+FFFD rather than being silently dropped).
		buffer += decoder.flush();
		// Parse the trailing (unterminated) line if it has any content. JSON.parse
		// tolerates surrounding whitespace, so the raw buffer is parsed directly.
		if (/\S/.test(buffer)) {
			try {
				enqueue(JSON.parse(buffer));
			} catch {
				trackError("ParseError", "Invalid JSON");
			}
			// No idx++ here: flush is terminal, so a post-increment would be dead.
		}
		// No buffer reset here: flush is terminal and the buffer is never read
		// again, so a reset would be dead code.
	};

	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({ key: resultKey ?? "ndjsonErrors", value: errors });
	return stream;
};

export const ndjsonFormatStream = (options = {}, streamOptions = {}) => {
	const { space } = options;
	const batch = [];

	const transform = (chunk, enqueue) => {
		batch.push(JSON.stringify(chunk, null, space));
		if (batch.length >= 64) {
			enqueue(`${batch.join("\n")}\n`);
			batch.length = 0;
		}
	};

	const flush = (enqueue) => {
		if (batch.length > 0) {
			enqueue(`${batch.join("\n")}\n`);
			batch.length = 0;
		}
	};

	return createTransformStream(transform, flush, streamOptions);
};

// --- JSON Array ---

export const jsonParseStream = (options = {}, streamOptions = {}) => {
	const {
		maxBufferSize = 16_777_216,
		maxValueSize = 16_777_216,
		maxErrorRows = 1_000,
		resultKey,
	} = options;
	const bufferLimit = maxBufferSize ?? Number.POSITIVE_INFINITY;
	const valueLimit = maxValueSize ?? Number.POSITIVE_INFINITY;

	let buffer = "";
	let scanPos = 0;
	let depth = 0;
	let inString = false;
	let escaped = false;
	let started = false;
	let rejected = false;
	let elementStart = -1;
	let idx = 0;
	const errors = {};
	const decoder = createChunkDecoder();
	const stripBom = createBomStripper();

	const track = createErrorTracker(errors, maxErrorRows);
	const trackError = (id, message) => track(id, message, idx);

	const emitElement = (text, enqueue) => {
		if (text.length > valueLimit) {
			throw new RangeError(
				`jsonParseStream value size (${text.length}) exceeds maxValueSize (${maxValueSize})`,
			);
		}
		// JSON.parse tolerates surrounding whitespace, so the raw element text is
		// parsed directly; callers only reach here with non-whitespace content.
		try {
			enqueue(JSON.parse(text));
		} catch {
			trackError("ParseError", "Invalid JSON");
		}
		idx++;
	};

	const scan = (enqueue) => {
		const len = buffer.length;

		while (scanPos < len) {
			const ch = buffer.charCodeAt(scanPos);

			// Only whitespace may precede the top-level `[`. Skipping ahead to the
			// first `[` anywhere would stream a nested array out of e.g.
			// {"a":[1,2]}; anything else means the input is not a JSON array, so
			// stop scanning for good.
			if (!started) {
				if (ch === 0x5b) {
					started = true;
				} else if (!isWhitespace(ch)) {
					rejected = true;
					return;
				}
				scanPos++;
				continue;
			}

			if (escaped) {
				escaped = false;
				scanPos++;
				continue;
			}

			if (inString) {
				if (ch === 0x5c) {
					escaped = true;
				} else if (ch === 0x22) {
					inString = false;
				}
				scanPos++;
				continue;
			}

			if (ch === 0x22) {
				inString = true;
				if (elementStart === -1) {
					elementStart = scanPos;
				}
				scanPos++;
				continue;
			}

			if (ch === 0x5b || ch === 0x7b) {
				if (elementStart === -1) elementStart = scanPos;
				depth++;
				scanPos++;
				continue;
			}

			if (ch === 0x5d || ch === 0x7d) {
				if (depth > 0) {
					depth--;
					// depth was > 0, so the matching open brace already set
					// elementStart; it is provably !== -1 here.
					if (depth === 0) {
						emitElement(buffer.substring(elementStart, scanPos + 1), enqueue);
						elementStart = -1;
					}
				} else {
					// Closing ] of top-level array
					if (elementStart !== -1) {
						emitElement(buffer.substring(elementStart, scanPos), enqueue);
						elementStart = -1;
					}
				}
				scanPos++;
				continue;
			}

			if (ch === 0x2c && depth === 0) {
				if (elementStart !== -1) {
					emitElement(buffer.substring(elementStart, scanPos), enqueue);
					elementStart = -1;
				}
				scanPos++;
				continue;
			}

			if (elementStart === -1 && !isWhitespace(ch)) {
				elementStart = scanPos;
			}
			scanPos++;
		}

		// Trim the processed portion from the buffer. trimFrom is always >= 0:
		// it is either elementStart (>= 0 when an element is in progress) or
		// scanPos (>= 0). A trimFrom of 0 makes the substring/offset updates
		// no-ops, so the trim runs unconditionally.
		const trimFrom = elementStart !== -1 ? elementStart : scanPos;
		buffer = buffer.substring(trimFrom);
		scanPos -= trimFrom;
		if (elementStart !== -1) {
			// The in-progress element now begins at the start of the buffer.
			elementStart = 0;
		}
	};

	const transform = (chunk, enqueue) => {
		// Not a JSON array: the rest of the input is ignored.
		if (rejected) return;
		if (buffer.length + chunk.length > bufferLimit) {
			throw new RangeError(
				`jsonParseStream buffer (${buffer.length + chunk.length}) exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		buffer += stripBom(decoder.decode(chunk));
		scan(enqueue);
	};

	const flush = (enqueue) => {
		// Flush bytes held by the streaming decoder (a truncated trailing
		// sequence becomes U+FFFD, i.e. invalid JSON, not silently dropped) and
		// scan them, so a lone U+FFFD before any `[` is rejected too. Rescanning
		// an already-rejected buffer just rejects again.
		buffer += decoder.flush();
		scan(enqueue);
		if (rejected) {
			trackError("NoArrayStart", "Input did not contain a top-level array");
			return;
		}
		// After scan(), the buffer holds exactly the trailing in-progress element
		// (starting at its first non-whitespace char) or only whitespace when no
		// element is pending. Emit the raw buffer as an element when it contains
		// any non-whitespace; emitElement/JSON.parse tolerate surrounding
		// whitespace. The scanner never leaves a structural `]` pending, so no
		// special closing-bracket guard is needed.
		if (/\S/.test(buffer)) {
			emitElement(buffer, enqueue);
		}
		// flush is terminal; the buffer is never read again, so no reset is needed.
	};

	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({ key: resultKey ?? "jsonErrors", value: errors });
	return stream;
};

export const jsonFormatStream = (options = {}, streamOptions = {}) => {
	const { space } = options;
	let first = true;

	const transform = (chunk, enqueue) => {
		const json = JSON.stringify(chunk, null, space);
		enqueue(first ? `[${json}` : `,\n${json}`);
		first = false;
	};

	const flush = (enqueue) => {
		enqueue(first ? "[]" : "\n]");
	};

	return createTransformStream(transform, flush, streamOptions);
};
