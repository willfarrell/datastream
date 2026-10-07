// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createPassThroughStream,
	createTransformStream,
} from "@datastream/core";

export const stringLengthStream = ({ resultKey } = {}, streamOptions = {}) => {
	let value = 0;
	const passThrough = (chunk) => {
		value += chunk.length;
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	stream.result = () => ({ key: resultKey ?? "length", value });
	return stream;
};
export const stringCountStream = (
	{ substr, resultKey } = {},
	streamOptions = {},
) => {
	if (typeof substr !== "string" || substr.length === 0) {
		throw new Error("stringCountStream requires a non-empty substr");
	}
	let value = 0;
	let carry = "";
	const passThrough = (chunk) => {
		const combined = carry + chunk;
		let cursor = -1;
		while (true) {
			cursor = combined.indexOf(substr, cursor + 1);
			if (cursor === -1) {
				break;
			}
			value += 1;
		}
		// slice(-0) would keep everything, so a 1-char substr carries nothing
		carry = combined.slice(Math.max(0, combined.length - substr.length + 1));
	};
	const stream = createPassThroughStream(passThrough, streamOptions);
	stream.result = () => ({ key: resultKey ?? "stringCount", value });
	return stream;
};

export const stringMinimumFirstChunkSizeStream = (
	options = {},
	streamOptions = {},
) => {
	const { chunkSize = 1024 } = options;
	let buffer = "";
	let done = false;
	const transform = (chunk, enqueue) => {
		if (done) {
			enqueue(chunk);
			return;
		}
		buffer += chunk;
		if (buffer.length >= chunkSize) {
			enqueue(buffer);
			done = true;
		}
	};
	const flush = (enqueue) => {
		if (!done && buffer.length > 0) {
			enqueue(buffer);
		}
	};
	const stream = createTransformStream(transform, flush, streamOptions);
	return stream;
};

export const stringMinimumChunkSizeStream = (
	options = {},
	streamOptions = {},
) => {
	const { chunkSize = 1024 } = options;
	let buffer = "";
	const transform = (chunk, enqueue) => {
		buffer += chunk;
		if (buffer.length >= chunkSize) {
			enqueue(buffer);
			buffer = "";
		}
	};
	const flush = (enqueue) => {
		if (buffer.length > 0) {
			enqueue(buffer);
		}
	};
	const stream = createTransformStream(transform, flush, streamOptions);
	return stream;
};

export const stringSkipConsecutiveDuplicatesStream = (
	_options = {},
	streamOptions = {},
) => {
	let previousChunk;
	const transform = (chunk, enqueue) => {
		if (chunk !== previousChunk) {
			enqueue(chunk);
			previousChunk = chunk;
		}
	};
	return createTransformStream(transform, streamOptions);
};

export const stringReplaceStream = (options, streamOptions = {}) => {
	const {
		pattern,
		replacement,
		maxMatchLength,
		lookbehind = 16,
		maxBufferSize = 16_777_216, // 16MB
	} = options;
	const bufferLimit = maxBufferSize ?? Number.POSITIVE_INFINITY; // null = unlimited
	const isString = typeof pattern === "string";
	if (
		!isString &&
		!pattern.flags.includes("g") &&
		!pattern.flags.includes("y")
	) {
		throw new Error(
			"RegExp pattern must include the global (g) or sticky (y) flag",
		);
	}
	if (pattern === "") {
		throw new Error("stringReplaceStream requires a non-empty pattern");
	}
	// Scan with a private global copy so the caller's lastIndex is untouched;
	// a string pattern becomes an escaped RegExp so both share one scanner
	const regexp = isString
		? new RegExp(RegExp.escape(pattern), "g")
		: new RegExp(pattern, `${pattern.flags.replace("g", "")}g`);
	const unicode = regexp.unicode || regexp.unicodeSets;
	// Longest possible match; unknown for a RegExp unless given, in which case
	// the latest chunk is held back instead
	const holdLength = isString ? pattern.length : maxMatchLength;
	// $` and $' need the whole stream, so such a template is applied at flush
	const wholeStream =
		typeof replacement === "string" && /\$[`']/.test(replacement);
	// String.prototype.replace's template expansion ($$, $&, $n, $nn, $<name>),
	// done per match so the RegExp can be run over the whole buffer
	const expand = (template, match) =>
		template.replace(/\$(?:[$&]|(\d\d?)|<([^>]*)>)/g, (token, digits, name) => {
			if (token === "$$") {
				return "$";
			}
			if (token === "$&") {
				return match[0];
			}
			if (name !== undefined) {
				// Without named groups "$<" is literal and the rest still expands
				return match.groups === undefined
					? `$<${expand(`${name}>`, match)}`
					: (match.groups[name] ?? "");
			}
			const index = Number(digits);
			if (index > 0 && index < match.length) {
				return match[index] ?? "";
			}
			// "$nn" that isn't a group falls back to "$n" and a literal digit
			const first = Number(digits[0]);
			if (first > 0 && first < match.length) {
				return (match[first] ?? "") + digits.slice(1);
			}
			return token;
		});
	const substitute =
		typeof replacement === "function"
			? (match, position, text) =>
					replacement(...match, position, text, match.groups)
			: (match) => expand(replacement, match);
	// Raw (not yet replaced) text; replacing twice would corrupt output
	let buffer = "";
	// Tail of already-emitted raw text, so lookbehind, ^ and \b see what
	// preceded the buffer instead of treating the buffer start as stream start
	let context = "";
	// Stream position of buffer[0], for function replacement offsets
	let offset = 0;
	let stopped = false;
	// Why: replacing each emitted slice on its own let the RegExp see neither
	// side of the slice (lookaround, ^/$, sticky, greedy matches split at the
	// buffer end). Instead run one exec pass over context + buffer, emitting
	// matches that start before `safe` and don't end at `holdEnd` (they could
	// still grow); the scan always resumes where emitting stopped.
	const scan = (safe, holdEnd) => {
		const text = context + buffer;
		const base = context.length;
		let output = "";
		let cut = 0;
		regexp.lastIndex = base;
		while (true) {
			const start = regexp.lastIndex - base;
			const match = stopped ? null : regexp.exec(text);
			const index = match === null ? Infinity : match.index - base;
			if (index >= safe || index + match[0].length >= holdEnd) {
				const emitTo = Math.max(start, Math.min(index, safe));
				// A sticky miss before `safe` can't succeed later, so matching ends
				if (match === null && regexp.sticky && start < safe) {
					stopped = true;
				}
				output += buffer.slice(cut, emitTo);
				context = text.slice(
					Math.max(0, base + emitTo - lookbehind),
					base + emitTo,
				);
				buffer = buffer.slice(emitTo);
				offset += emitTo;
				return output;
			}
			output +=
				buffer.slice(cut, index) + substitute(match, offset + index, text);
			cut = index + match[0].length;
			if (cut === index) {
				regexp.lastIndex +=
					unicode && text.codePointAt(match.index) > 0xffff ? 2 : 1;
			}
		}
	};
	const transform = (chunk, enqueue) => {
		buffer += chunk;
		if (buffer.length > bufferLimit) {
			throw new RangeError(
				`stringReplaceStream buffer (${buffer.length}) exceeds maxBufferSize (${maxBufferSize})`,
			);
		}
		if (wholeStream) {
			return;
		}
		const output = scan(
			holdLength === undefined
				? buffer.length - chunk.length
				: buffer.length - holdLength + 1,
			buffer.length,
		);
		if (output !== "") {
			enqueue(output);
		}
	};
	const flush = (enqueue) => {
		const output = wholeStream
			? buffer.replace(regexp, replacement)
			: scan(Infinity, Infinity);
		if (output !== "") {
			enqueue(output);
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export const stringSplitStream = (options, streamOptions = {}) => {
	const {
		separator,
		maxBufferSize = 16_777_216, // 16MB
	} = options;
	const bufferLimit = maxBufferSize ?? Number.POSITIVE_INFINITY; // null = unlimited
	if (typeof separator !== "string" || separator.length === 0) {
		throw new Error("stringSplitStream requires a non-empty separator");
	}
	let previousChunk = "";
	const transform = (chunk, enqueue) => {
		chunk = previousChunk + chunk;
		let pos = 0;
		while (true) {
			const idx = chunk.indexOf(separator, pos);
			if (idx > -1) {
				enqueue(chunk.substring(pos, idx));
				pos = idx + separator.length;
			} else {
				previousChunk = chunk.substring(pos);
				break;
			}
		}
		if (previousChunk.length > bufferLimit) {
			throw new RangeError(
				`stringSplitStream buffer (${previousChunk.length}) exceeds maxBufferSize (${maxBufferSize}), separator not found`,
			);
		}
	};
	const flush = (enqueue) => {
		enqueue(previousChunk);
	};
	return createTransformStream(transform, flush, streamOptions);
};
