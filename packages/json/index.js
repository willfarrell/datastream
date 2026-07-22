// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";

// --- NDJSON ---

export const ndjsonParseStream = (options = {}, streamOptions = {}) => {
	const { maxBufferSize = 16_777_216, resultKey } = options;
	let buffer = "";
	let idx = 0;
	const errors = {};

	const trackError = (id, message) => {
		if (!errors[id]) errors[id] = { id, message, idx: [] };
		errors[id].idx.push(idx);
	};

	const transform = (chunk, enqueue) => {
		if (buffer.length + chunk.length > maxBufferSize) {
			throw new Error(
				`ndjsonParseStream buffer (${buffer.length + chunk.length}) exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		buffer += chunk;
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
	stream.result = () => ({ key: resultKey ?? "jsonErrors", value: errors });
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
		resultKey,
	} = options;

	let buffer = "";
	let scanPos = 0;
	let depth = 0;
	let inString = false;
	let escaped = false;
	let started = false;
	let sawNonWhitespace = false;
	let elementStart = -1;
	let idx = 0;
	const errors = {};

	const trackError = (id, message) => {
		if (!errors[id]) errors[id] = { id, message, idx: [] };
		errors[id].idx.push(idx);
	};

	const emitElement = (text, enqueue) => {
		if (text.length > maxValueSize) {
			throw new Error(
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
				if (started && elementStart === -1) {
					elementStart = scanPos;
				}
				scanPos++;
				continue;
			}

			if (!started) {
				if (ch === 0x5b) {
					started = true;
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

			if (
				elementStart === -1 &&
				ch !== 0x20 &&
				ch !== 0x09 &&
				ch !== 0x0a &&
				ch !== 0x0d
			) {
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
		if (buffer.length + chunk.length > maxBufferSize) {
			throw new Error(
				`jsonParseStream buffer (${buffer.length + chunk.length}) exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		if (!sawNonWhitespace && /\S/.test(chunk)) sawNonWhitespace = true;
		buffer += chunk;
		scan(enqueue);
	};

	const flush = (enqueue) => {
		// After scan(), the buffer holds exactly the trailing in-progress element
		// (starting at its first non-whitespace char) or only whitespace when no
		// element is pending. Emit the raw buffer as an element when it contains
		// any non-whitespace; emitElement/JSON.parse tolerate surrounding
		// whitespace. The scanner never leaves a structural `]` pending, so no
		// special closing-bracket guard is needed.
		if (/\S/.test(buffer)) {
			emitElement(buffer, enqueue);
		}
		if (!started && sawNonWhitespace) {
			trackError("NoArrayStart", "Input did not contain a top-level array");
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
