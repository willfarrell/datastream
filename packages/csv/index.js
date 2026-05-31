// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	// createPassThroughStream,
	createTransformStream,
	resolveLazy,
} from "@datastream/core";
import { objectToEntriesStream } from "@datastream/object";

const comma = ",";
const quote = "'";
const doubleQuote = '"';
const tab = "\t";
const pipe = "|";
const semiColon = ";";
// const colon = ":";
// const space = " ";
const carageReturn = "\r";
const lineFeed = "\n";
const newline = `${carageReturn}${lineFeed}`;

const detectDelimiterChars = [tab, pipe, semiColon, comma];
const detectNewlineChars = [newline, carageReturn, lineFeed];
const detectQuoteChars = [doubleQuote, quote];

const defaultDelimiterChar = comma;
const defaultNewlineChar = newline;
const defaultQuoteChar = doubleQuote;

const stripBOM = (str) => {
	return str.charCodeAt(0) === 0xfeff ? str.slice(1) : str;
};

// Quote/escape-aware scan for the end of the first record (row). Returns the
// index of the newline that terminates row 0, or -1 if no complete row is
// present. Newlines inside quoted fields are skipped so a quoted newline does
// not split the row. Mirrors the field-start quoting rules of the parser.
const findRowEnd = (
	text,
	delimiterChar,
	newlineChar,
	quoteChar,
	escapeChar,
) => {
	const len = text.length;
	const quoteCode = quoteChar.charCodeAt(0);
	const escapeCode = escapeChar.charCodeAt(0);
	const escapeIsQuote = escapeChar === quoteChar;
	const delimiterLength = delimiterChar.length;
	let pos = 0;
	let fieldStart = 0;
	while (pos < len) {
		if (text.charCodeAt(pos) === quoteCode && pos === fieldStart) {
			// Quoted field: scan to the matching closing quote.
			pos++;
			while (pos < len) {
				if (text.charCodeAt(pos) === quoteCode) {
					if (escapeIsQuote) {
						// "" is an escaped quote; a single " closes the field.
						if (pos + 1 < len && text.charCodeAt(pos + 1) === quoteCode) {
							pos += 2;
							continue;
						}
						pos++;
						break;
					}
					// Custom escape: odd run of escapeChar before quote => escaped.
					let run = 0;
					let k = pos - 1;
					while (k >= 0 && text.charCodeAt(k) === escapeCode) {
						run++;
						k--;
					}
					if ((run & 1) === 1) {
						pos++;
						continue;
					}
					pos++;
					break;
				}
				pos++;
			}
			fieldStart = pos;
			continue;
		}
		if (text.startsWith(newlineChar, pos)) {
			return pos;
		}
		if (text.startsWith(delimiterChar, pos)) {
			pos += delimiterLength;
			fieldStart = pos;
			continue;
		}
		pos++;
	}
	return -1;
};

export const csvDetectDelimitersStream = (options = {}, streamOptions = {}) => {
	const {
		chunkSize = 1024, // 1KB
		resultKey,
	} = options;

	const value = {
		delimiterChar: undefined,
		newlineChar: undefined,
		quoteChar: undefined,
		escapeChar: undefined,
	};

	const headerRegExp = new RegExp(
		`^([^${detectNewlineChars.join("")}]*)(${detectNewlineChars.join("|")})`,
	);

	let buffer = "";
	let detected = false;

	const detect = (text) => {
		text = stripBOM(text);
		const headerMatch = text.match(headerRegExp);
		if (!headerMatch) return false;
		value.newlineChar = headerMatch[2];
		const headerString = headerMatch[1];

		value.delimiterChar =
			detectDelimiterChars.find(
				(delimiter) => headerString.indexOf(delimiter) > -1,
			) ?? defaultDelimiterChar;

		// A char is only the quote char when it actually BRACKETS a field: it
		// opens at a field-start (text start, or right after a delimiter or a
		// newline) AND a matching quote closes the field right before the next
		// delimiter/newline/end. A bare apostrophe/quote inside ordinary data
		// (e.g. "'twas the night", which opens but never closes a field) must
		// NOT be mistaken for the quote char.
		const delimiterChar = value.delimiterChar;
		const cr = carageReturn.charCodeAt(0);
		const lf = lineFeed.charCodeAt(0);
		const isFieldStart = (i) =>
			i === 0 ||
			text.startsWith(delimiterChar, i - delimiterChar.length) ||
			text.charCodeAt(i - 1) === cr ||
			text.charCodeAt(i - 1) === lf;
		const isFieldEnd = (i) =>
			i >= text.length ||
			text.startsWith(delimiterChar, i) ||
			text.charCodeAt(i) === cr ||
			text.charCodeAt(i) === lf;
		value.quoteChar =
			detectQuoteChars.find((candidate) => {
				let i = text.indexOf(candidate);
				while (i > -1) {
					if (isFieldStart(i)) {
						// Look for a closing quote that ends the field.
						let close = text.indexOf(candidate, i + 1);
						while (close > -1) {
							if (isFieldEnd(close + 1)) return true;
							close = text.indexOf(candidate, close + 1);
						}
					}
					i = text.indexOf(candidate, i + 1);
				}
				return false;
			}) ?? defaultQuoteChar;
		value.escapeChar = value.quoteChar;
		return true;
	};

	const transform = (chunk, enqueue) => {
		if (detected) {
			enqueue(chunk);
			return;
		}
		buffer += chunk;
		if (buffer.length >= chunkSize && detect(buffer)) {
			detected = true;
			enqueue(buffer);
			buffer = "";
		}
	};

	const flush = (enqueue) => {
		if (!detected && buffer.length > 0) {
			detect(buffer);
			enqueue(buffer);
			buffer = "";
		}
	};

	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({ key: resultKey ?? "csvDetectDelimiters", value });
	return stream;
};

export const csvDetectHeaderStream = (options = {}, streamOptions = {}) => {
	let {
		chunkSize = 1024, // 1KB
		parser,
		delimiterChar,
		newlineChar,
		quoteChar,
		escapeChar,
		resultKey,
	} = options;

	const value = {
		header: [],
	};

	let buffer = "";
	let headerDetected = false;

	const processBuffer = (enqueue) => {
		delimiterChar = resolveLazy(delimiterChar) ?? defaultDelimiterChar;
		newlineChar = resolveLazy(newlineChar) ?? defaultNewlineChar;
		quoteChar = resolveLazy(quoteChar) ?? defaultQuoteChar;
		escapeChar = resolveLazy(escapeChar) ?? quoteChar;

		const text = stripBOM(buffer);
		buffer = "";
		headerDetected = true;

		// Find the end of the first row with a quote/escape-aware scan so a
		// newline inside a quoted header field does NOT split the header.
		const headerEndOfRow = findRowEnd(
			text,
			delimiterChar,
			newlineChar,
			quoteChar,
			escapeChar,
		);

		const parserFn = parser ?? csvQuotedParser;
		const headerChunk =
			headerEndOfRow === -1 ? text : text.slice(0, headerEndOfRow);
		const result = parserFn(
			headerChunk,
			{
				delimiterChar,
				newlineChar,
				quoteChar,
				escapeChar,
				numCols: 0,
				idx: 0,
			},
			true,
		);
		value.header = result.rows[0] ?? [];

		if (headerEndOfRow === -1) {
			// Entire input is header, no data rows
			return;
		}

		const rest = text.slice(headerEndOfRow + newlineChar.length);
		if (rest.length > 0) {
			enqueue(rest);
		}
	};

	const transform = (chunk, enqueue) => {
		if (headerDetected) {
			enqueue(chunk);
			return;
		}
		buffer += chunk;
		if (buffer.length >= chunkSize) {
			processBuffer(enqueue);
		}
	};

	const flush = (enqueue) => {
		if (!headerDetected && buffer.length > 0) {
			processBuffer(enqueue);
		}
	};

	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({ key: resultKey ?? "csvDetectHeader", value });
	return stream;
};

// --- Parsers ---
// Both return { rows: string[][], tail: string, numCols: number, idx: number, errors?: {} }
// Options can include pre-computed char codes (from csvSteamifyParser) or raw config strings.

// Inverse of csvFormatStream's custom-escape encoding (escapeChar !== quoteChar):
// the formatter escapes escapeChar -> escapeChar+escapeChar and quoteChar ->
// escapeChar+quoteChar. Reverse both in a single left-to-right pass so an
// escapeChar consumes the following char literally (handling escaped escapes
// and escaped quotes together), keeping format/parse a faithful round-trip.
const unescapeCustom = (text, escapeCharCode) => {
	const len = text.length;
	let out = "";
	let i = 0;
	let start = 0;
	while (i < len) {
		if (text.charCodeAt(i) === escapeCharCode && i + 1 < len) {
			out += text.substring(start, i);
			out += text[i + 1];
			i += 2;
			start = i;
		} else {
			i++;
		}
	}
	return out + text.substring(start);
};

// Internal hot-path parser. Writes results directly to ctx and calls enqueue(fields) per row.
// ctx must have all pre-computed char codes + numCols, idx, tail, errors fields.
const csvParseInline = (text, ctx, isFlushing, enqueue) => {
	const delimiterCharCode = ctx.delimiterCharCode;
	const delimiterChar = ctx.delimiterChar;
	const delimiterCharLength = ctx.delimiterCharLength;
	const delimiterCharSingle = ctx.delimiterCharSingle;
	const newlineCharCode = ctx.newlineCharCode;
	const newlineCharSingle = ctx.newlineCharSingle;
	const newlineChar = ctx.newlineChar;
	const newlineCharLength = ctx.newlineCharLength;
	const quoteCharCode = ctx.quoteCharCode;
	const quoteChar = ctx.quoteChar;
	const escapeCharCode = ctx.escapeCharCode;
	const escapeIsQuote = ctx.escapeIsQuote;
	const escapedQuote = ctx.escapedQuote;
	const fieldMaxSize = ctx.fieldMaxSize;

	const len = text.length;
	let numCols = ctx.numCols;
	let idx = ctx.idx;
	let errors = null;

	let rowStart = 0;
	let fieldStart = 0;
	let rowTpl = numCols > 0 ? Array(numCols).fill("") : null;
	let fields = rowTpl ? rowTpl.slice() : [];
	let fi = 0;
	let pos = 0;
	let lastWasDelimiter = false;

	const trackError = (id, message) => {
		if (errors === null) errors = {};
		if (!errors[id]) errors[id] = { id, message, idx: [] };
		errors[id].idx.push(idx);
	};

	outer: while (pos < len) {
		if (text.charCodeAt(pos) === quoteCharCode && pos === fieldStart) {
			// === QUOTED FIELD ===
			lastWasDelimiter = false;
			pos++;
			const contentStart = pos;

			if (escapeIsQuote) {
				// Find closing quote using indexOf, skipping escaped "" pairs
				let closeQ = text.indexOf(quoteChar, pos);
				let hasEscapes = false;
				while (
					closeQ !== -1 &&
					closeQ + 1 < len &&
					text.charCodeAt(closeQ + 1) === quoteCharCode
				) {
					hasEscapes = true;
					closeQ = text.indexOf(quoteChar, closeQ + 2);
				}

				if (closeQ === -1) {
					// Unterminated quote
					if (isFlushing) {
						trackError("UnterminatedQuote", "Unterminated quoted field");
						const raw = text.substring(contentStart);
						fields[fi++] = hasEscapes
							? raw.replaceAll(escapedQuote, quoteChar)
							: raw;
						if (numCols === 0) numCols = fi;
						else if (fi < numCols) fields.length = fi;
						enqueue(fields);
						idx++;
					}
					ctx.tail = isFlushing ? "" : text.substring(rowStart);
					ctx.numCols = numCols;
					ctx.idx = idx;
					ctx.errors = errors;
					return;
				}

				// Extract field value: single slice + conditional replaceAll
				const field = hasEscapes
					? text
							.substring(contentStart, closeQ)
							.replaceAll(escapedQuote, quoteChar)
					: text.substring(contentStart, closeQ);
				if (field.length > fieldMaxSize) {
					throw new Error(
						`CSV field size (${field.length}) exceeds fieldMaxSize (${fieldMaxSize} bytes)`,
					);
				}
				pos = closeQ + 1;

				// Post-quote dispatch: delimiter, newline, or end-of-input
				if (pos >= len) {
					fields[fi++] = field;
					fieldStart = pos;
					break;
				}
				const nc = text.charCodeAt(pos);
				if (
					delimiterCharSingle
						? nc === delimiterCharCode
						: text.startsWith(delimiterChar, pos)
				) {
					fields[fi++] = field;
					pos += delimiterCharLength;
					fieldStart = pos;
					lastWasDelimiter = true;
					continue;
				}
				if (
					nc === newlineCharCode &&
					(newlineCharLength === 1 ||
						(newlineCharLength === 2 &&
							pos + 1 < len &&
							text.charCodeAt(pos + 1) === newlineCharSingle) ||
						(newlineCharLength > 2 && text.startsWith(newlineChar, pos)))
				) {
					fields[fi++] = field;
					if (numCols === 0) {
						numCols = fi;
						rowTpl = Array(numCols).fill("");
					} else if (fi < numCols) fields.length = fi;
					enqueue(fields);
					idx++;
					fi = 0;
					fields = rowTpl.slice();
					pos += newlineCharLength;
					rowStart = pos;
					fieldStart = pos;
					lastWasDelimiter = false;
					continue;
				}
				// Garbage after closing quote
				fields[fi++] = field;
				fieldStart = pos;
				continue;
			}

			// escapeChar !== quoteChar — use indexOf with run-length lookback.
			// A quote is only escaped when the run of escapeChar immediately
			// before it is ODD; an even run (e.g. "\\" => an escaped escape)
			// leaves the following quote as a real closing quote.
			let closeQ = text.indexOf(quoteChar, pos);
			let hasEscape = false;
			while (closeQ !== -1) {
				let run = 0;
				let k = closeQ - 1;
				while (k >= contentStart && text.charCodeAt(k) === escapeCharCode) {
					run++;
					k--;
				}
				if (run > 0) hasEscape = true;
				if ((run & 1) === 0) break; // even run => real closing quote
				closeQ = text.indexOf(quoteChar, closeQ + 1);
			}

			if (closeQ === -1) {
				// Unterminated quote
				const raw = text.substring(contentStart);
				const field = hasEscape ? unescapeCustom(raw, escapeCharCode) : raw;
				if (isFlushing) {
					trackError("UnterminatedQuote", "Unterminated quoted field");
					fields[fi++] = field;
					if (numCols === 0) numCols = fi;
					else if (fi < numCols) fields.length = fi;
					enqueue(fields);
					idx++;
				}
				ctx.tail = isFlushing ? "" : text.substring(rowStart);
				ctx.numCols = numCols;
				ctx.idx = idx;
				ctx.errors = errors;
				return;
			}

			// Extract field value: single slice + conditional unescape
			{
				const field = hasEscape
					? unescapeCustom(text.substring(contentStart, closeQ), escapeCharCode)
					: text.substring(contentStart, closeQ);
				if (field.length > fieldMaxSize) {
					throw new Error(
						`CSV field size (${field.length}) exceeds fieldMaxSize (${fieldMaxSize} bytes)`,
					);
				}
				pos = closeQ + 1;

				// Post-quote dispatch: delimiter, newline, or end-of-input
				if (pos >= len) {
					fields[fi++] = field;
					fieldStart = pos;
					break;
				}
				const nc = text.charCodeAt(pos);
				if (
					delimiterCharSingle
						? nc === delimiterCharCode
						: text.startsWith(delimiterChar, pos)
				) {
					fields[fi++] = field;
					pos += delimiterCharLength;
					fieldStart = pos;
					lastWasDelimiter = true;
					continue;
				}
				if (
					nc === newlineCharCode &&
					(newlineCharLength === 1 ||
						(newlineCharLength === 2 &&
							pos + 1 < len &&
							text.charCodeAt(pos + 1) === newlineCharSingle) ||
						(newlineCharLength > 2 && text.startsWith(newlineChar, pos)))
				) {
					fields[fi++] = field;
					if (numCols === 0) {
						numCols = fi;
						rowTpl = Array(numCols).fill("");
					} else if (fi < numCols) fields.length = fi;
					enqueue(fields);
					idx++;
					fi = 0;
					fields = rowTpl.slice();
					pos += newlineCharLength;
					rowStart = pos;
					fieldStart = pos;
					lastWasDelimiter = false;
					continue;
				}
				// Garbage after closing quote
				fields[fi++] = field;
				fieldStart = pos;
				continue;
			}
		}

		// === UNQUOTED FIELDS — indexOf scan ===
		lastWasDelimiter = false;
		{
			let nextNl = text.indexOf(newlineChar, pos);

			// Fast path: no quotes — column-aware indexOf loop
			// Finds row boundary first, then processes columns within
			// bounds. Fewer allocations than split (no intermediate
			// line strings). Handles short/long rows via split fallback.
			if (
				fi === 0 &&
				numCols > 0 &&
				text.indexOf(quoteChar, fieldStart) === -1
			) {
				const lastFi = numCols - 1;
				let rowEnd = nextNl;
				while (rowEnd !== -1) {
					for (fi = 0; fi < lastFi; fi++) {
						const d = text.indexOf(delimiterChar, fieldStart);
						if (d === -1 || d > rowEnd) {
							// Malformed row (too few columns): split fallback
							fields = text.substring(pos, rowEnd).split(delimiterChar);
							fi = numCols; // sentinel: skip lastFi assign
							break;
						}
						fields[fi] = text.substring(fieldStart, d);
						fieldStart = d + delimiterCharLength;
					}
					if (fi === lastFi) {
						// Surplus columns: a delimiter still remains before rowEnd, so
						// the last field would otherwise absorb the extra columns.
						// Fall back to split so over-long rows keep every field
						// separate (matching the slow path and keeping malformed rows
						// detectable).
						const extra = text.indexOf(delimiterChar, fieldStart);
						if (extra !== -1 && extra < rowEnd) {
							fields = text.substring(pos, rowEnd).split(delimiterChar);
						} else {
							fields[lastFi] = text.substring(fieldStart, rowEnd);
						}
					}
					enqueue(fields);
					idx++;
					fi = 0;
					pos = rowEnd + newlineCharLength;
					rowStart = pos;
					fieldStart = pos;
					fields = rowTpl.slice();
					rowEnd = text.indexOf(newlineChar, pos);
				}
				if (pos >= len) {
					break;
				}
				// Partial row without newline: fall through to regular path
				nextNl = -1;
			}

			// First-row detection: use split to establish numCols
			if (
				fi === 0 &&
				numCols === 0 &&
				nextNl !== -1 &&
				text.indexOf(quoteChar, fieldStart) === -1
			) {
				const lineFields = text
					.substring(fieldStart, nextNl)
					.split(delimiterChar);
				numCols = lineFields.length;
				rowTpl = Array(numCols).fill("");
				enqueue(lineFields);
				idx++;
				pos = nextNl + newlineCharLength;
				rowStart = pos;
				fieldStart = pos;
				fields = rowTpl.slice();
				if (pos >= len) {
					break;
				}
				// Re-enter the fast path via continue outer
				continue;
			}

			// Regular indexOf path
			while (pos < len) {
				const nextDelim = text.indexOf(delimiterChar, pos);

				if (nextNl !== -1 && (nextDelim === -1 || nextNl < nextDelim)) {
					fields[fi++] = text.substring(fieldStart, nextNl);
					if (numCols === 0) {
						numCols = fi;
						rowTpl = Array(numCols).fill("");
					} else if (fi < numCols) fields.length = fi;
					enqueue(fields);
					idx++;
					fi = 0;
					fields = rowTpl.slice();
					pos = nextNl + newlineCharLength;
					rowStart = pos;
					fieldStart = pos;
					lastWasDelimiter = false;
					nextNl = text.indexOf(newlineChar, pos);
					if (pos >= len) break;
					if (text.charCodeAt(pos) === quoteCharCode) continue outer;
					continue;
				}

				if (nextDelim !== -1) {
					fields[fi++] = text.substring(fieldStart, nextDelim);
					pos = nextDelim + delimiterCharLength;
					fieldStart = pos;
					lastWasDelimiter = true;
					if (pos >= len) continue outer;
					if (text.charCodeAt(pos) === quoteCharCode) continue outer;
					continue;
				}

				break;
			}
		}

		break;
	}

	// Cleanup: partial row at end
	if (fieldStart < len || lastWasDelimiter || fi > 0) {
		if (isFlushing) {
			if (fieldStart < len) {
				fields[fi++] = text.substring(fieldStart);
			} else if (lastWasDelimiter) {
				fields[fi++] = "";
			}
			if (fi > 0) {
				if (numCols === 0) numCols = fi;
				else if (fi < numCols) fields.length = fi;
				enqueue(fields);
				idx++;
			}
		} else {
			ctx.tail = text.substring(rowStart);
			ctx.numCols = numCols;
			ctx.idx = idx;
			ctx.errors = errors;
			return;
		}
	}
	ctx.tail = "";
	ctx.numCols = numCols;
	ctx.idx = idx;
	ctx.errors = errors;
};

export const csvQuotedParser = (text, options = {}, isFlushing = false) => {
	const delimiterChar = options.delimiterChar ?? defaultDelimiterChar;
	const newlineChar = options.newlineChar ?? defaultNewlineChar;
	const quoteChar = options.quoteChar ?? defaultQuoteChar;
	const escapeChar = options.escapeChar ?? quoteChar;

	const ctx = {
		delimiterChar,
		delimiterCharCode: options.delimiterCharCode ?? delimiterChar.charCodeAt(0),
		delimiterCharLength: options.delimiterCharLength ?? delimiterChar.length,
		delimiterCharSingle:
			options.delimiterCharSingle ?? delimiterChar.length === 1,
		newlineChar,
		newlineCharCode: options.newlineCharCode ?? newlineChar.charCodeAt(0),
		newlineCharSingle:
			options.newlineCharSingle ??
			(newlineChar.length > 1 ? newlineChar.charCodeAt(1) : -1),
		newlineCharLength: options.newlineCharLength ?? newlineChar.length,
		quoteChar,
		quoteCharCode: options.quoteCharCode ?? quoteChar.charCodeAt(0),
		escapeChar,
		escapeCharCode: options.escapeCharCode ?? escapeChar.charCodeAt(0),
		escapeIsQuote: options.escapeIsQuote ?? escapeChar === quoteChar,
		escapedQuote: options.escapedQuote ?? escapeChar + quoteChar,
		numCols: options.numCols ?? 0,
		idx: options.idx ?? 0,
		tail: "",
		errors: null,
	};
	const rows = [];
	csvParseInline(text, ctx, isFlushing, (row) => rows.push(row));
	return {
		rows,
		tail: ctx.tail,
		numCols: ctx.numCols,
		idx: ctx.idx,
		errors: ctx.errors ?? {},
	};
};

export const csvUnquotedParser = (text, options = {}, isFlushing = false) => {
	const delimiterChar = options.delimiterChar ?? defaultDelimiterChar;
	const newlineChar = options.newlineChar ?? defaultNewlineChar;
	const newlineCharLength = options.newlineCharLength ?? newlineChar.length;

	const len = text.length;
	const rows = [];
	let numCols = options.numCols ?? 0;
	let idx = options.idx ?? 0;

	let pos = 0;
	let nlIdx = text.indexOf(newlineChar, pos);
	while (nlIdx !== -1) {
		const fields = text.substring(pos, nlIdx).split(delimiterChar);
		if (numCols === 0) numCols = fields.length;
		rows.push(fields);
		idx++;
		pos = nlIdx + newlineCharLength;
		nlIdx = text.indexOf(newlineChar, pos);
	}
	if (pos < len) {
		if (isFlushing) {
			const fields = text.substring(pos).split(delimiterChar);
			if (numCols === 0) numCols = fields.length;
			rows.push(fields);
			idx++;
		} else {
			return { rows, tail: text.substring(pos), numCols, idx };
		}
	}
	return { rows, tail: "", numCols, idx };
};

// --- Streaming wrapper ---

const csvSteamifyParser = (options = {}) => {
	let {
		parser,
		delimiterChar,
		newlineChar,
		quoteChar,
		escapeChar,
		fieldMaxSize,
	} = options;
	const useCustomParser = parser != null;
	parser ??= csvQuotedParser;

	let resolved = false;
	const ctx = { numCols: 0, idx: 0, tail: "", errors: null };
	let buffer = "";
	const errors = {};

	const mergeErrors = (incoming) => {
		for (const id in incoming) {
			if (errors[id]) {
				errors[id].idx.push(...incoming[id].idx);
			} else {
				errors[id] = {
					id: incoming[id].id,
					message: incoming[id].message,
					idx: [...incoming[id].idx],
				};
			}
		}
	};

	const resolveOptions = () => {
		delimiterChar = resolveLazy(delimiterChar) ?? defaultDelimiterChar;
		newlineChar = resolveLazy(newlineChar) ?? defaultNewlineChar;
		quoteChar = resolveLazy(quoteChar) ?? defaultQuoteChar;
		escapeChar = resolveLazy(escapeChar) ?? quoteChar;

		ctx.delimiterChar = delimiterChar;
		ctx.delimiterCharCode = delimiterChar.charCodeAt(0);
		ctx.delimiterCharLength = delimiterChar.length;
		ctx.delimiterCharSingle = delimiterChar.length === 1;
		ctx.newlineChar = newlineChar;
		ctx.newlineCharCode = newlineChar.charCodeAt(0);
		ctx.newlineCharSingle =
			newlineChar.length > 1 ? newlineChar.charCodeAt(1) : -1;
		ctx.newlineCharLength = newlineChar.length;
		ctx.quoteChar = quoteChar;
		ctx.quoteCharCode = quoteChar.charCodeAt(0);
		ctx.escapeChar = escapeChar;
		ctx.escapeCharCode = escapeChar.charCodeAt(0);
		ctx.escapeIsQuote = escapeChar === quoteChar;
		ctx.escapedQuote = escapeChar + quoteChar;
		ctx.fieldMaxSize = fieldMaxSize;
		resolved = true;
	};

	const streamFn = (chunk, enqueue) => {
		if (!resolved) resolveOptions();
		const str = typeof chunk === "string" ? chunk : chunk.toString();
		const text = buffer.length > 0 ? buffer + str : str;
		buffer = "";
		if (text.length > ctx.fieldMaxSize * 2) {
			throw new Error(
				`CSV buffer size (${text.length}) exceeds safety limit, likely unterminated quoted field`,
			);
		}

		if (useCustomParser) {
			const result = parser(text, ctx, false);
			ctx.numCols = result.numCols;
			ctx.idx = result.idx;
			buffer = result.tail;
			if (result.errors) mergeErrors(result.errors);
			const rows = result.rows;
			for (let i = 0; i < rows.length; i++) enqueue(rows[i]);
		} else {
			ctx.tail = "";
			ctx.errors = null;
			csvParseInline(text, ctx, false, enqueue);
			buffer = ctx.tail;
		}
	};

	streamFn.flush = (enqueue) => {
		if (!resolved) resolveOptions();
		if (buffer.length > 0) {
			const remaining = buffer;
			buffer = "";
			if (useCustomParser) {
				const result = parser(remaining, ctx, true);
				ctx.numCols = result.numCols;
				ctx.idx = result.idx;
				if (result.errors) mergeErrors(result.errors);
				const rows = result.rows;
				for (let i = 0; i < rows.length; i++) enqueue(rows[i]);
			} else {
				ctx.tail = "";
				ctx.errors = null;
				csvParseInline(remaining, ctx, true, enqueue);
				if (ctx.errors !== null) mergeErrors(ctx.errors);
			}
		}
	};

	streamFn.errors = errors;
	return streamFn;
};

// --- Stream exports ---

export const csvParseStream = (options = {}, streamOptions = {}) => {
	const {
		chunkSize = 2_097_152, // 2MB
		fieldMaxSize = 16_777_216, // 16MB
		resultKey,
		...parserOptions
	} = options;
	parserOptions.fieldMaxSize = fieldMaxSize;
	streamOptions = { highWaterMark: 16384, ...streamOptions };

	const streamParse = csvSteamifyParser(parserOptions);

	let inputChunks = [];
	let inputLen = 0;
	let ready = false;

	const transform = (chunk, enqueue) => {
		if (!ready) {
			inputChunks.push(chunk);
			inputLen += chunk.length;
			if (inputLen < chunkSize) return;
			ready = true;
			const text =
				inputChunks.length === 1 ? inputChunks[0] : inputChunks.join("");
			inputChunks = null;
			streamParse(text, enqueue);
		} else {
			streamParse(chunk, enqueue);
		}
	};

	const flush = (enqueue) => {
		if (!ready && inputLen > 0) {
			const text =
				inputChunks.length === 1 ? inputChunks[0] : inputChunks.join("");
			inputChunks = null;
			streamParse(text, enqueue);
		}
		streamParse.flush(enqueue);
	};

	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({
		key: resultKey ?? "csvErrors",
		value: streamParse.errors,
	});
	return stream;
};

export const csvRemoveMalformedRowsStream = (
	options = {},
	streamOptions = {},
) => {
	let { headers, onErrorEnqueue, resultKey } = options;
	onErrorEnqueue ??= false;

	const value = {};
	let expectedColumns;
	let idx = -1;

	const transform = (chunk, enqueue) => {
		idx++;
		if (expectedColumns === undefined) {
			expectedColumns = resolveLazy(headers)?.length ?? chunk.length;
		}
		if (chunk.length !== expectedColumns) {
			if (!value.MalformedRow) {
				value.MalformedRow = {
					id: "MalformedRow",
					message: "Row has incorrect number of fields",
					idx: [],
				};
			}
			value.MalformedRow.idx.push(idx);
			if (onErrorEnqueue) {
				enqueue(chunk);
			}
			return;
		}
		enqueue(chunk);
	};

	const stream = createTransformStream(transform, streamOptions);
	stream.result = () => ({
		key: resultKey ?? "csvRemoveMalformedRows",
		value,
	});
	return stream;
};

export const csvRemoveEmptyRowsStream = (options = {}, streamOptions = {}) => {
	let { onErrorEnqueue, resultKey } = options;
	onErrorEnqueue ??= false;

	const value = {};
	let idx = -1;

	const isEmpty = (chunk) => {
		const l = chunk.length;
		if (l === 0) return true;
		for (let i = 0; i < l; i++) {
			if (chunk[i] !== "") return false;
		}
		return true;
	};

	const transform = (chunk, enqueue) => {
		idx++;
		if (isEmpty(chunk)) {
			if (!value.EmptyRow) {
				value.EmptyRow = {
					id: "EmptyRow",
					message: "Row is empty",
					idx: [],
				};
			}
			value.EmptyRow.idx.push(idx);
			if (onErrorEnqueue) {
				enqueue(chunk);
			}
			return;
		}
		enqueue(chunk);
	};

	const stream = createTransformStream(transform, streamOptions);
	stream.result = () => ({ key: resultKey ?? "csvRemoveEmptyRows", value });
	return stream;
};

const numberRe = /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/;
const iso8601Re =
	/^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$/;

const autoCoerce = (val) => {
	if (typeof val !== "string") return val;
	const len = val.length;
	if (len === 0) return null;
	const c0 = val.charCodeAt(0);
	// Fast boolean check: avoid toLowerCase() for non-boolean strings
	if (len === 4 && (c0 === 116 || c0 === 84)) {
		// 't' or 'T'
		const lower = val.toLowerCase();
		if (lower === "true") return true;
	} else if (len === 5 && (c0 === 102 || c0 === 70)) {
		// 'f' or 'F'
		const lower = val.toLowerCase();
		if (lower === "false") return false;
	}
	// Number: starts with digit or minus sign
	if (
		(c0 >= 48 && c0 <= 57) || // '0'-'9'
		c0 === 45 // '-'
	) {
		if (numberRe.test(val)) return Number(val);
		if (iso8601Re.test(val)) {
			const d = new Date(val);
			if (!Number.isNaN(d.getTime())) return d;
		}
		return val;
	}
	// ISO date: starts with digit (already handled above)
	// JSON: starts with '{' or '['
	if (c0 === 123 || c0 === 91) {
		try {
			return JSON.parse(val);
		} catch {
			return val;
		}
	}
	return val;
};

const coerceToType = (val, type) => {
	switch (type) {
		case "number": {
			if (val === "") return null;
			const n = Number(val);
			return Number.isNaN(n) ? val : n;
		}
		case "boolean":
			return typeof val === "string"
				? val.toLowerCase() === "true"
				: Boolean(val);
		case "null":
			return null;
		case "date": {
			const d = new Date(val);
			return Number.isNaN(d.getTime()) ? val : d;
		}
		case "json":
			try {
				return JSON.parse(val);
			} catch {
				return val;
			}
		default:
			return val;
	}
};

export const csvCoerceValuesStream = (options = {}, streamOptions = {}) => {
	const { columns, resultKey } = options;
	const value = {};

	const transform = columns
		? (chunk, enqueue) => {
				const coerced = {};
				for (const key in chunk) {
					const type = columns[key];
					coerced[key] = type
						? coerceToType(chunk[key], type)
						: autoCoerce(chunk[key]);
				}
				enqueue(coerced);
			}
		: (chunk, enqueue) => {
				const coerced = {};
				for (const key in chunk) {
					coerced[key] = autoCoerce(chunk[key]);
				}
				enqueue(coerced);
			};

	const stream = createTransformStream(transform, streamOptions);
	stream.result = () => ({ key: resultKey ?? "csvCoerceValues", value });
	return stream;
};

// --- Formatting ---

export const csvInjectHeaderStream = ({ header }, streamOptions = {}) => {
	let injected = false;
	const transform = (chunk, enqueue) => {
		if (!injected) {
			injected = true;
			enqueue(header);
		}
		enqueue(chunk);
	};
	return createTransformStream(transform, streamOptions);
};

export const csvFormatStream = (options = {}, streamOptions = {}) => {
	const delimiterChar = options.delimiterChar ?? defaultDelimiterChar;
	const newlineChar = options.newlineChar ?? defaultNewlineChar;
	const quoteChar = options.quoteChar ?? defaultQuoteChar;
	const escapeChar = options.escapeChar ?? quoteChar;

	// Pre-compute char codes and flags once at stream creation
	const delimiterCode = delimiterChar.charCodeAt(0);
	const delimiterSingle = delimiterChar.length === 1;
	const quoteCode = quoteChar.charCodeAt(0);
	const escapeIsQuote = escapeChar === quoteChar;
	const escapedQuote = escapeChar + quoteChar;
	const escapedEscape = escapeChar + escapeChar;

	// Single-pass charCode scan for single-char delimiter (common case),
	// includes fallback for multi-char
	const scanNeedsQuote = delimiterSingle
		? (value) => {
				const len = value.length;
				const first = value.charCodeAt(0);
				// = (61) + (43) - (45) @ (64) space (32) BOM (FEFF)
				if (
					first === 61 ||
					first === 43 ||
					first === 45 ||
					first === 64 ||
					first === 32 ||
					first === 0xfeff
				)
					return true;
				if (value.charCodeAt(len - 1) === 32) return true;
				for (let i = 0; i < len; i++) {
					const c = value.charCodeAt(i);
					if (c === delimiterCode || c === quoteCode || c === 13 || c === 10)
						return true;
				}
				return false;
			}
		: (value) => {
				const len = value.length;
				const first = value.charCodeAt(0);
				if (
					first === 61 ||
					first === 43 ||
					first === 45 ||
					first === 64 ||
					first === 32 ||
					first === 0xfeff
				)
					return true;
				if (value.charCodeAt(len - 1) === 32) return true;
				if (value.includes(delimiterChar)) return true;
				for (let i = 0; i < len; i++) {
					const c = value.charCodeAt(i);
					if (c === quoteCode || c === 13 || c === 10) return true;
				}
				return false;
			};

	// Skip replaceAll when value has no chars that need escaping (common:
	// field quoted because of delimiter/newline, but contains no quote chars)
	const wrapQuote = escapeIsQuote
		? (value) =>
				value.includes(quoteChar)
					? quoteChar + value.replaceAll(quoteChar, escapedQuote) + quoteChar
					: quoteChar + value + quoteChar
		: (value) => {
				const v = value.includes(escapeChar)
					? value.replaceAll(escapeChar, escapedEscape)
					: value;
				return v.includes(quoteChar)
					? quoteChar + v.replaceAll(quoteChar, escapedQuote) + quoteChar
					: quoteChar + v + quoteChar;
			};

	// Fast path: all fields are strings (or null/undefined) and none need
	// quoting → use Array.join (single native allocation + memcpy) instead
	// of per-field ConsString concatenation. join converts null/undefined
	// to "" which matches empty-field CSV semantics.
	const isSimpleRow = (chunk) => {
		for (let i = 0; i < chunk.length; i++) {
			const val = chunk[i];
			if (val == null) continue;
			if (typeof val !== "string") return false;
			if (val.length > 0 && scanNeedsQuote(val)) return false;
		}
		return true;
	};

	// Slow path: pre-allocated parts array + join (produces flat string
	// directly, avoids ~2n ConsString nodes from per-field concatenation)
	const formatRowSlow = (chunk) => {
		const len = chunk.length;
		const parts = new Array(len);
		for (let i = 0; i < len; i++) {
			let val = chunk[i];
			if (val == null) {
				parts[i] = "";
				continue;
			}
			if (typeof val !== "string") {
				val = val instanceof Date ? val.toISOString() : String(val);
			}
			if (val.length === 0) {
				parts[i] = "";
				continue;
			}
			parts[i] = scanNeedsQuote(val) ? wrapQuote(val) : val;
		}
		return parts.join(delimiterChar);
	};

	// Array batch: collect row strings, then join with newlineChar as
	// separator → one flat string per batch (vs ~128-node ConsString tree
	// from repeated buffer += concatenation)
	const batch = [];

	const transform = (chunk, enqueue) => {
		batch.push(
			isSimpleRow(chunk) ? chunk.join(delimiterChar) : formatRowSlow(chunk),
		);
		if (batch.length >= 64) {
			enqueue(batch.join(newlineChar) + newlineChar);
			batch.length = 0;
		}
	};

	const flush = (enqueue) => {
		if (batch.length > 0) {
			enqueue(batch.join(newlineChar) + newlineChar);
			batch.length = 0;
		}
	};

	return createTransformStream(transform, flush, streamOptions);
};

export const csvArrayToObject = ({ headers }, streamOptions = {}) => {
	let resolvedKeys;
	const transform = (chunk, enqueue) => {
		resolvedKeys ??= resolveLazy(headers);
		const value = {};
		for (let i = 0; i < resolvedKeys.length; i++) {
			const key = resolvedKeys[i];
			// Reserved keys like "__proto__" must be stored as own data
			// properties: a plain `value[key] = ...` assignment would set the
			// object's prototype instead, silently dropping the column. Use
			// defineProperty so the column is always an enumerable own property.
			if (key === "__proto__" || key === "constructor") {
				Object.defineProperty(value, key, {
					value: chunk[i],
					writable: true,
					enumerable: true,
					configurable: true,
				});
			} else {
				value[key] = chunk[i];
			}
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};
export const csvObjectToArray = ({ headers }, streamOptions) =>
	objectToEntriesStream({ keys: headers }, streamOptions);
