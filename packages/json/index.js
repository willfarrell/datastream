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
			const line = buffer.substring(pos, nlIdx).trimEnd();
			pos = nlIdx + 1;
			if (line.length === 0) continue;
			try {
				enqueue(JSON.parse(line));
			} catch {
				trackError("ParseError", "Invalid JSON");
			}
			idx++;
		}
	};

	const flush = (enqueue) => {
		const line = buffer.trimEnd();
		if (line.length > 0) {
			try {
				enqueue(JSON.parse(line));
			} catch {
				trackError("ParseError", "Invalid JSON");
			}
			idx++;
		}
		buffer = "";
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
		const trimmed = text.trim();
		if (trimmed.length === 0) return;
		try {
			enqueue(JSON.parse(trimmed));
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
					if (depth === 0 && elementStart !== -1) {
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

		// Trim processed portion from buffer
		const trimFrom = elementStart !== -1 ? elementStart : scanPos;
		if (trimFrom > 0) {
			buffer = buffer.substring(trimFrom);
			scanPos -= trimFrom;
			if (elementStart !== -1) {
				elementStart = 0;
			}
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
		if (elementStart !== -1 && buffer.length > 0) {
			const trimmed = buffer.substring(elementStart).trim();
			if (trimmed.length > 0 && trimmed !== "]") {
				emitElement(trimmed, enqueue);
			}
		}
		if (!started && sawNonWhitespace) {
			trackError("NoArrayStart", "Input did not contain a top-level array");
		}
		buffer = "";
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
