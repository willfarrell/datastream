// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createChunkDecoder, createTransformStream } from "@datastream/core";

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

const createErrorTracker = (errors, maxErrorRows) => {
	const limit = maxErrorRows ?? Number.POSITIVE_INFINITY;
	return (id, message, idx) => {
		errors[id] ??= { id, message, idx: [], count: 0 };
		const error = errors[id];
		error.count += 1;
		if (error.idx.length < limit) error.idx.push(idx);
	};
};

// JSON.stringify returns undefined (not a string) for undefined, functions
// and symbols; writing that verbatim would emit the invalid token
// `undefined`. Map it to `null`, matching JSON.stringify array semantics.
const stringify = (value, space) =>
	JSON.stringify(value, null, space) ?? "null";

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
		buffer += stripBom(decoder.decode(chunk));
		let pos = 0;
		while (true) {
			const nlIdx = buffer.indexOf("\n", pos);
			if (nlIdx === -1) {
				buffer = buffer.substring(pos);
				if (buffer.length > bufferLimit) {
					throw new RangeError(
						`ndjsonParseStream buffer (${buffer.length}) exceeds maxBufferSize (${maxBufferSize})`,
					);
				}
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

export const ndjsonFormatStream = (_options = {}, streamOptions = {}) => {
	const batch = [];

	const transform = (chunk, enqueue) => {
		batch.push(stringify(chunk));
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
	let ended = false;
	let trailing = false;
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
		// Hot loop state lives in locals: a closure variable written per char
		// is a context-slot store V8 cannot keep in a register across the
		// emitElement call; everything is written back before the function
		// returns, so cross-chunk behaviour is unchanged.
		const text = buffer;
		const len = text.length;
		let pos = scanPos;
		let elStart = elementStart;
		let level = depth;
		let inStr = inString;
		let esc = escaped;

		while (pos < len) {
			const ch = text.charCodeAt(pos);

			if (!started) {
				if (ch === 0x5b) {
					started = true;
				} else if (!isWhitespace(ch)) {
					rejected = true;
					break;
				}
				pos++;
				continue;
			}

			if (ended) {
				if (!isWhitespace(ch)) {
					trailing = true;
					trackError(
						"TrailingData",
						"Unexpected content after top-level array",
					);
					break;
				}
				pos++;
				continue;
			}

			if (esc) {
				esc = false;
				pos++;
				continue;
			}

			if (inStr) {
				while (pos < len) {
					const next = text.charCodeAt(pos++);
					if (next === 0x5c) {
						esc = true;
						break;
					}
					if (next === 0x22) {
						inStr = false;
						break;
					}
				}
				continue;
			}

			if (ch === 0x22) {
				inStr = true;
				if (elStart === -1) {
					elStart = pos;
				}
				pos++;
				continue;
			}

			if (ch === 0x5b || ch === 0x7b) {
				if (elStart === -1) elStart = pos;
				level++;
				pos++;
				continue;
			}

			if (ch === 0x5d || ch === 0x7d) {
				if (level > 0) {
					level--;
					if (level === 0) {
						emitElement(text.substring(elStart, pos + 1), enqueue);
						elStart = -1;
					}
				} else {
					// Closing ] of top-level array
					if (elStart !== -1) {
						emitElement(text.substring(elStart, pos), enqueue);
						elStart = -1;
					}
					ended = true;
				}
				pos++;
				continue;
			}

			if (ch === 0x2c && level === 0) {
				if (elStart !== -1) {
					emitElement(text.substring(elStart, pos), enqueue);
					elStart = -1;
				}
				pos++;
				continue;
			}

			if (elStart === -1 && !isWhitespace(ch)) {
				elStart = pos;
			}
			pos++;
		}

		depth = level;
		inString = inStr;
		escaped = esc;
		const trimFrom = elStart !== -1 ? elStart : pos;
		buffer = text.substring(trimFrom);
		scanPos = pos - trimFrom;
		elementStart = elStart !== -1 ? 0 : -1;
	};

	const transform = (chunk, enqueue) => {
		if (rejected || trailing) return;
		buffer += stripBom(decoder.decode(chunk));
		scan(enqueue);
		if (rejected || trailing) return;
		if (buffer.length > bufferLimit) {
			throw new RangeError(
				`jsonParseStream buffer (${buffer.length}) exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
	};

	const flush = (enqueue) => {
		if (trailing) return;
		buffer += decoder.flush();
		scan(enqueue);
		if (rejected) {
			trackError("NoArrayStart", "Input did not contain a top-level array");
			return;
		}
		if (!started || ended) return;
		// After scan(), the buffer holds exactly the trailing in-progress element
		// (starting at its first non-whitespace char) or only whitespace when no
		// element is pending. Emit the raw buffer as an element when it contains
		// any non-whitespace; emitElement/JSON.parse tolerate surrounding
		// whitespace. The scanner never leaves a structural `]` pending, so no
		// special closing-bracket guard is needed.
		if (/\S/.test(buffer)) {
			emitElement(buffer, enqueue);
		}
		trackError(
			"UnexpectedEnd",
			"Input ended before the top-level array closed",
		);
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
		const json = stringify(chunk, space);
		enqueue(first ? `[${json}` : `,\n${json}`);
		first = false;
	};

	const flush = (enqueue) => {
		enqueue(first ? "[]" : "\n]");
	};

	return createTransformStream(transform, flush, streamOptions);
};
