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
	const bufferLimit = maxBufferSize ?? Number.POSITIVE_INFINITY;
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
	const regexp = isString
		? {
				lastIndex: 0,
				exec(text) {
					const index = text.indexOf(pattern, this.lastIndex);
					if (index === -1) {
						this.lastIndex = 0;
						return null;
					}
					this.lastIndex = index + pattern.length;
					const match = [pattern];
					match.index = index;
					match.input = text;
					return match;
				},
			}
		: pattern;
	const unicode = regexp.unicode || regexp.unicodeSets;
	const holdLength = isString ? pattern.length : maxMatchLength;
	const wholeStream =
		typeof replacement === "string" && /\$[`']/.test(replacement);
	const compile = (template, match) => {
		const parts = [];
		// Group names are found with indexOf, so an unclosed "$<" never rescans
		const lastClose = template.lastIndexOf(">");
		const tokens = /\$(?:[$&`']|(\d\d?)|<)/g;
		let last = 0;
		for (
			let token = tokens.exec(template);
			token !== null;
			token = tokens.exec(template)
		) {
			parts.push(template.slice(last, token.index));
			last = tokens.lastIndex;
			const [text, digits] = token;
			if (text === "$<") {
				// Without named groups or a closing ">", "$<" is literal
				if (match.groups === undefined || lastClose < last) {
					parts.push(text);
				} else {
					const close = template.indexOf(">", last);
					const name = template.slice(last, close);
					parts.push((match) => match.groups[name] ?? "");
					last = close + 1;
					tokens.lastIndex = last;
				}
			} else if (text === "$$") {
				parts.push("$");
			} else if (text === "$&") {
				parts.push((match) => match[0]);
			} else if (text === "$`") {
				parts.push((match, text) => text.slice(0, match.index));
			} else if (text === "$'") {
				parts.push((match, text) => text.slice(match.index + match[0].length));
			} else {
				const index = Number(digits);
				const first = Number(digits[0]);
				if (index > 0 && index < match.length) {
					parts.push((match) => match[index] ?? "");
				} else if (first > 0 && first < match.length) {
					// "$nn" that isn't a group falls back to "$n" and a literal digit
					parts.push((match) => (match[first] ?? "") + digits.slice(1));
				} else {
					parts.push(text);
				}
			}
		}
		parts.push(template.slice(last));
		return parts;
	};
	let parts;
	const substitute =
		typeof replacement === "function"
			? (match, position, text) =>
					replacement(...match, position, text, match.groups)
			: (match, _position, text) => {
					parts ??= compile(replacement, match);
					let output = "";
					for (const part of parts) {
						output += typeof part === "string" ? part : part(match, text);
					}
					return output;
				};
	let buffer = "";
	let context = "";
	let offset = 0;
	let stopped = false;
	const scan = (safe, holdEnd) => {
		const text = context + buffer;
		const base = context.length;
		let output = "";
		let cut = 0;
		const callerLastIndex = regexp.lastIndex;
		regexp.lastIndex = base;
		try {
			while (true) {
				const start = regexp.lastIndex - base;
				const match = stopped ? null : regexp.exec(text);
				const index = match === null ? Infinity : match.index - base;
				if (index >= safe || index + match[0].length >= holdEnd) {
					const emitTo = Math.max(start, Math.min(index, safe));
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
		} finally {
			regexp.lastIndex = callerLastIndex;
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
		const output = scan(Infinity, Infinity);
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
	const bufferLimit = maxBufferSize ?? Number.POSITIVE_INFINITY;
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
