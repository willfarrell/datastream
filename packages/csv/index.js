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

// True when the quote at `idx` is escaped — i.e. preceded by an ODD run of
// escapeChar (scanning no further back than `lowerBound`). A "not found" index
// of -1 yields false (the lookback inspects nothing), so callers can use this
// directly as a loop condition without a separate -1 check.
const quoteIsEscaped = (text, idx, lowerBound, escapeCode) => {
	let escaped = false;
	let k = idx - 1;
	while (k >= lowerBound && text.charCodeAt(k) === escapeCode) {
		escaped = !escaped;
		k--;
	}
	return escaped;
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
	const quoteCode = quoteChar.charCodeAt(0);
	const escapeCode = escapeChar.charCodeAt(0);
	const delimiterLength = delimiterChar.length;
	let pos = 0;
	for (;;) {
		// `pos` is always a field start here (it advances only past a closing quote
		// or a delimiter), so a quote at `pos` always opens a quoted field.
		if (text.charCodeAt(pos) === quoteCode) {
			// Quoted field: find the matching closing quote with indexOf. A quote is
			// escaped (does not close the field) only when the run of escapeChar
			// immediately before it is odd. When escapeChar === quoteChar this run
			// counts the doubled "" quotes, so one algorithm covers both conventions.
			// Only the parity of quotes matters for locating the row-terminating
			// newline, so this matches the parser's field-close position.
			const contentStart = pos + 1;
			let closeQ = text.indexOf(quoteChar, contentStart);
			// Skip escaped quotes; a "not found" (-1) is treated as not-escaped and
			// ends the loop.
			while (quoteIsEscaped(text, closeQ, contentStart, escapeCode)) {
				closeQ = text.indexOf(quoteChar, closeQ + 1);
			}
			// Unterminated quote → no complete row in this buffer.
			if (closeQ === -1) return -1;
			pos = closeQ + 1;
			continue;
		}
		const nextNl = text.indexOf(newlineChar, pos);
		const nextDelim = text.indexOf(delimiterChar, pos);
		// Advance past a delimiter that falls at or before the row's newline (a
		// delimiter that is a prefix of the newline wins the tie). When there is no
		// newline, nextNl is -1 and `nextDelim <= -1` is false, so the loop returns
		// -1 (an incomplete row). Otherwise the newline ends row 0.
		if (nextDelim !== -1 && nextDelim <= nextNl) {
			pos = nextDelim + delimiterLength;
			continue;
		}
		return nextNl;
	}
};

export const csvDetectDelimitersStream = (options = {}, streamOptions = {}) => {
	const {
		// chunkSize is accepted for compatibility; detect() already waits for a
		// complete first line, so no byte threshold is needed.
		chunkSize: _chunkSize,
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
		// detect() returns false until the buffer holds a complete first line, so
		// it can be attempted on every chunk without a size threshold.
		if (detect(buffer)) {
			detected = true;
			enqueue(buffer);
			// `buffer` is not read again once detected is set.
		}
	};

	const flush = (enqueue) => {
		if (!detected && buffer.length > 0) {
			// Detect from whatever was buffered (may be a partial line) and emit it.
			detect(buffer);
			enqueue(buffer);
			// End of stream; `buffer` is not read again.
		}
	};

	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({ key: resultKey ?? "csvDetectDelimiters", value });
	return stream;
};

export const csvDetectHeaderStream = (options = {}, streamOptions = {}) => {
	let {
		// chunkSize is accepted for compatibility; the header is processed as soon
		// as a complete first row is buffered.
		chunkSize: _chunkSize,
		parser,
		delimiterChar,
		newlineChar,
		quoteChar,
		escapeChar,
		resultKey,
	} = options;

	// `header` is always assigned by processBuffer (which runs at the latest on
	// flush) before the stream's result() is read.
	const value = {};

	let buffer = "";
	let headerDetected = false;

	const resolveOptions = () => {
		delimiterChar = resolveLazy(delimiterChar) ?? defaultDelimiterChar;
		newlineChar = resolveLazy(newlineChar) ?? defaultNewlineChar;
		quoteChar = resolveLazy(quoteChar) ?? defaultQuoteChar;
		escapeChar = resolveLazy(escapeChar) ?? quoteChar;
	};

	// Returns the offset of the row-0 terminator within the (BOM-stripped) buffer,
	// or -1 if a complete header row is not yet present.
	const headerRowEnd = () =>
		findRowEnd(
			stripBOM(buffer),
			delimiterChar,
			newlineChar,
			quoteChar,
			escapeChar,
		);

	const processBuffer = (enqueue, headerEndOfRow) => {
		const text = stripBOM(buffer);
		// `buffer` is not read again once headerDetected is set, so it is left as-is.
		headerDetected = true;

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
		resolveOptions();
		// Process as soon as a complete header row is buffered (a quoted newline in
		// the header does not count); otherwise keep buffering.
		const headerEndOfRow = headerRowEnd();
		if (headerEndOfRow !== -1) {
			processBuffer(enqueue, headerEndOfRow);
		}
	};

	const flush = (enqueue) => {
		// Whatever remains (possibly a partial header row, or nothing) is finalized
		// here. On empty input this yields an empty header and emits nothing.
		if (!headerDetected) {
			resolveOptions();
			processBuffer(enqueue, headerRowEnd());
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
const unescapeCustom = (text, escapeChar) => {
	let out = "";
	let start = 0;
	let i = text.indexOf(escapeChar, start);
	// Each escapeChar consumes the following character literally. A trailing
	// escapeChar with no following character (i + 1 === length) is kept as-is.
	while (i !== -1 && i + 1 < text.length) {
		out += text.substring(start, i) + text[i + 1];
		start = i + 2;
		i = text.indexOf(escapeChar, start);
	}
	return out + text.substring(start);
};

// Internal hot-path parser. Writes results directly to ctx and calls enqueue(fields) per row.
// ctx must have all pre-computed char codes + numCols, idx, tail, errors fields.
const csvParseInline = (text, ctx, isFlushing, enqueue) => {
	const delimiterChar = ctx.delimiterChar;
	const delimiterCharLength = ctx.delimiterCharLength;
	const newlineChar = ctx.newlineChar;
	const newlineCharLength = ctx.newlineCharLength;
	const quoteCharCode = ctx.quoteCharCode;
	const quoteChar = ctx.quoteChar;
	const escapeChar = ctx.escapeChar;
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
	// Each row is built by pushing fields in order, so fields.length always
	// equals the field count — short rows simply have fewer entries.
	let fields = [];
	let pos = 0;
	let lastWasDelimiter = false;

	// Called at most once per invocation (each unterminated-quote branch returns
	// immediately afterwards), so the error map and entry are created fresh here.
	const trackError = (id, message) => {
		errors = { [id]: { id, message, idx: [idx] } };
	};

	while (pos < len) {
		// The outer loop is only (re)entered at a field start, so a quote here
		// always opens a quoted field (mid-field quotes are consumed by the
		// unquoted scan below and never reach this check).
		if (text.charCodeAt(pos) === quoteCharCode) {
			// === QUOTED FIELD ===
			lastWasDelimiter = false;
			pos++;
			const contentStart = pos;

			if (escapeIsQuote) {
				// Find the closing quote with indexOf, skipping escaped "" pairs.
				// replaceAll collapses any doubled quotes; on a field without "" it
				// is a no-op, so it is applied unconditionally.
				let closeQ = text.indexOf(quoteChar, pos);
				while (closeQ !== -1 && text.charCodeAt(closeQ + 1) === quoteCharCode) {
					closeQ = text.indexOf(quoteChar, closeQ + 2);
				}

				if (closeQ === -1) {
					// Unterminated quote
					if (isFlushing) {
						trackError("UnterminatedQuote", "Unterminated quoted field");
						const raw = text.substring(contentStart);
						fields.push(raw.replaceAll(escapedQuote, quoteChar));
						if (numCols === 0) numCols = fields.length;
						enqueue(fields);
						idx++;
					}
					ctx.tail = isFlushing ? "" : text.substring(rowStart);
					ctx.numCols = numCols;
					ctx.idx = idx;
					ctx.errors = errors;
					return;
				}

				// Extract field value: single slice + replaceAll (no-op without "")
				const field = text
					.substring(contentStart, closeQ)
					.replaceAll(escapedQuote, quoteChar);
				if (field.length > fieldMaxSize) {
					throw new Error(
						`CSV field size (${field.length}) exceeds fieldMaxSize (${fieldMaxSize} bytes)`,
					);
				}
				pos = closeQ + 1;

				// Post-quote dispatch: delimiter, newline, or end-of-input.
				// At end-of-input charCodeAt(pos) is NaN, so neither the delimiter
				// nor the newline branch matches and the field falls through to the
				// "garbage after closing quote" branch, which records the field and
				// lets the outer loop terminate — no explicit end guard needed.
				if (text.startsWith(delimiterChar, pos)) {
					fields.push(field);
					pos += delimiterCharLength;
					fieldStart = pos;
					lastWasDelimiter = true;
					continue;
				}
				if (text.startsWith(newlineChar, pos)) {
					fields.push(field);
					if (numCols === 0) numCols = fields.length;
					enqueue(fields);
					idx++;
					fields = [];
					pos += newlineCharLength;
					rowStart = pos;
					fieldStart = pos;
					lastWasDelimiter = false;
					continue;
				}
				// Garbage after closing quote (also the end-of-input case)
				fields.push(field);
				fieldStart = pos;
				continue;
			}

			// escapeChar !== quoteChar — find the closing quote with indexOf and a
			// run-length lookback. A quote is escaped (does not close the field) only
			// when the run of escapeChar immediately before it is odd; an even run
			// (e.g. "\\" => an escaped escape) leaves a real closing quote. The
			// opening quote (which differs from escapeChar here) terminates the
			// lookback, so no explicit lower bound is needed. unescapeCustom reverses
			// the escaping and is a no-op on a field with no escapeChar, so it is
			// applied unconditionally.
			let closeQ = text.indexOf(quoteChar, pos);
			// Skip escaped quotes; a "not found" (-1) is treated as not-escaped and
			// ends the loop.
			while (quoteIsEscaped(text, closeQ, contentStart, escapeCharCode)) {
				closeQ = text.indexOf(quoteChar, closeQ + 1);
			}

			if (closeQ === -1) {
				// Unterminated quote
				const field = unescapeCustom(text.substring(contentStart), escapeChar);
				if (isFlushing) {
					trackError("UnterminatedQuote", "Unterminated quoted field");
					fields.push(field);
					if (numCols === 0) numCols = fields.length;
					enqueue(fields);
					idx++;
				}
				ctx.tail = isFlushing ? "" : text.substring(rowStart);
				ctx.numCols = numCols;
				ctx.idx = idx;
				ctx.errors = errors;
				return;
			}

			// Extract field value: single slice + unescape (no-op without escapes)
			{
				const field = unescapeCustom(
					text.substring(contentStart, closeQ),
					escapeChar,
				);
				if (field.length > fieldMaxSize) {
					throw new Error(
						`CSV field size (${field.length}) exceeds fieldMaxSize (${fieldMaxSize} bytes)`,
					);
				}
				pos = closeQ + 1;

				// Post-quote dispatch: delimiter, newline, or end-of-input (see the
				// escapeIsQuote branch above — the garbage branch also covers EOI).
				if (text.startsWith(delimiterChar, pos)) {
					fields.push(field);
					pos += delimiterCharLength;
					fieldStart = pos;
					lastWasDelimiter = true;
					continue;
				}
				if (text.startsWith(newlineChar, pos)) {
					fields.push(field);
					if (numCols === 0) numCols = fields.length;
					enqueue(fields);
					idx++;
					fields = [];
					pos += newlineCharLength;
					rowStart = pos;
					fieldStart = pos;
					lastWasDelimiter = false;
					continue;
				}
				// Garbage after closing quote
				fields.push(field);
				fieldStart = pos;
				continue;
			}
		}

		// === UNQUOTED FIELD — one field per outer iteration ===
		// The next quote/newline/delimiter is resolved, the field emitted, and the
		// outer loop re-entered so the following field-start is re-dispatched
		// (handling a quote that opens the next field).
		lastWasDelimiter = false;
		{
			const nextNl = text.indexOf(newlineChar, pos);
			const nextDelim = text.indexOf(delimiterChar, pos);

			if (nextDelim !== -1 && (nextNl === -1 || nextDelim <= nextNl)) {
				// Field terminated by a delimiter (which wins a tie with the newline,
				// e.g. when the delimiter is a prefix of the newline) → more fields.
				fields.push(text.substring(fieldStart, nextDelim));
				pos = nextDelim + delimiterCharLength;
				fieldStart = pos;
				lastWasDelimiter = true;
				continue;
			}

			if (nextNl !== -1) {
				// Field terminated by a newline → end of row.
				fields.push(text.substring(fieldStart, nextNl));
				if (numCols === 0) numCols = fields.length;
				enqueue(fields);
				idx++;
				fields = [];
				pos = nextNl + newlineCharLength;
				rowStart = pos;
				fieldStart = pos;
				continue;
			}
		}

		break;
	}

	// Cleanup: a partial row may remain at the end of the chunk.
	if (!isFlushing) {
		// rowStart marks the start of the unconsumed (incomplete) row; for a fully
		// consumed input it equals len and yields an empty tail.
		ctx.tail = text.substring(rowStart);
		ctx.numCols = numCols;
		ctx.idx = idx;
		ctx.errors = errors;
		return;
	}
	// Flushing: emit any trailing field or the empty field of a dangling delimiter.
	if (fieldStart < len) {
		fields.push(text.substring(fieldStart));
	} else if (lastWasDelimiter) {
		fields.push("");
	}
	if (fields.length > 0) {
		if (numCols === 0) numCols = fields.length;
		enqueue(fields);
		idx++;
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
		delimiterCharLength: options.delimiterCharLength ?? delimiterChar.length,
		newlineChar,
		newlineCharLength: options.newlineCharLength ?? newlineChar.length,
		quoteChar,
		quoteCharCode: options.quoteCharCode ?? quoteChar.charCodeAt(0),
		escapeChar,
		escapeCharCode: options.escapeCharCode ?? escapeChar.charCodeAt(0),
		escapeIsQuote: options.escapeIsQuote ?? escapeChar === quoteChar,
		escapedQuote: options.escapedQuote ?? escapeChar + quoteChar,
		fieldMaxSize: options.fieldMaxSize ?? Number.POSITIVE_INFINITY,
		numCols: options.numCols ?? 0,
		idx: options.idx ?? 0,
		// `tail`/`errors` are always assigned by csvParseInline before being read.
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
	parser ??= csvQuotedParser;

	// Per-chunk parser context; every field is (re)assigned by resolveOptions and
	// the parser result before it is read (numCols/idx default via `?? 0`).
	const ctx = {};
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
		ctx.delimiterCharLength = delimiterChar.length;
		ctx.newlineChar = newlineChar;
		ctx.newlineCharLength = newlineChar.length;
		ctx.quoteChar = quoteChar;
		ctx.quoteCharCode = quoteChar.charCodeAt(0);
		ctx.escapeChar = escapeChar;
		ctx.escapeCharCode = escapeChar.charCodeAt(0);
		ctx.escapeIsQuote = escapeChar === quoteChar;
		ctx.escapedQuote = escapeChar + quoteChar;
		ctx.fieldMaxSize = fieldMaxSize;
	};

	const streamFn = (chunk, enqueue) => {
		// resolveLazy is idempotent on already-resolved values, so re-resolving on
		// every chunk is safe and keeps lazy options deferred until upstream runs.
		resolveOptions();
		// String#toString returns the string itself, so this also handles string
		// chunks; an empty buffer concatenates to just the chunk.
		const text = buffer + chunk.toString();
		// `buffer` is reassigned from the parse tail below before it is read again.
		if (text.length > ctx.fieldMaxSize * 2) {
			throw new Error(
				`CSV buffer size (${text.length}) exceeds safety limit, likely unterminated quoted field`,
			);
		}

		const result = parser(text, ctx, false);
		ctx.numCols = result.numCols;
		ctx.idx = result.idx;
		buffer = result.tail;
		mergeErrors(result.errors);
		const rows = result.rows;
		for (let i = 0; i < rows.length; i++) enqueue(rows[i]);
	};

	streamFn.flush = (enqueue) => {
		resolveOptions();
		if (buffer.length > 0) {
			const remaining = buffer;
			const result = parser(remaining, ctx, true);
			ctx.numCols = result.numCols;
			ctx.idx = result.idx;
			mergeErrors(result.errors);
			const rows = result.rows;
			for (let i = 0; i < rows.length; i++) enqueue(rows[i]);
		}
	};

	streamFn.errors = errors;
	return streamFn;
};

// --- Stream exports ---

export const csvParseStream = (options = {}, streamOptions = {}) => {
	const {
		// chunkSize is accepted for backwards compatibility; the streaming parser
		// buffers partial rows itself, so chunks are parsed as they arrive.
		chunkSize: _chunkSize,
		fieldMaxSize = 16_777_216, // 16MB
		resultKey,
		...parserOptions
	} = options;
	parserOptions.fieldMaxSize = fieldMaxSize;

	const streamParse = csvSteamifyParser(parserOptions);

	const transform = (chunk, enqueue) => {
		streamParse(chunk, enqueue);
	};

	const flush = (enqueue) => {
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
		// A zero-length row falls through the loop and returns true as well.
		for (let i = 0; i < chunk.length; i++) {
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
	const lower = val.toLowerCase();
	if (lower === "true") return true;
	if (lower === "false") return false;
	// Number then ISO date — both regexes are anchored and only match
	// digit/minus-prefixed strings, so non-numeric input falls through.
	if (numberRe.test(val)) return Number(val);
	if (iso8601Re.test(val)) {
		const d = new Date(val);
		if (!Number.isNaN(d.getTime())) return d;
	}
	// JSON: only attempt for '{' or '[' so values like "null" are not parsed.
	// On a parse error fall through to the final `return val`.
	if (c0 === 123 || c0 === 91) {
		try {
			return JSON.parse(val);
		} catch {}
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
		case "json": {
			try {
				return JSON.parse(val);
			} catch {}
			// On a JSON parse error, keep the original string.
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

	// Pre-compute escaping flags/strings once at stream creation
	const escapeIsQuote = escapeChar === quoteChar;
	const escapedQuote = escapeChar + quoteChar;
	const escapedEscape = escapeChar + escapeChar;

	// A field must be quoted when it starts with a formula/whitespace/BOM
	// trigger, ends with a space, or contains the delimiter, the quote char,
	// or a CR/LF. The leading-char trigger is checked by code; the rest are
	// substring containment checks (delimiter may be multi-char).
	const startsWithTrigger = (value) => {
		// = (61) + (43) - (45) @ (64) space (32) BOM (FEFF)
		const first = value.charCodeAt(0);
		return (
			first === 61 ||
			first === 43 ||
			first === 45 ||
			first === 64 ||
			first === 32 ||
			first === 0xfeff
		);
	};
	const scanNeedsQuote = (value) =>
		startsWithTrigger(value) ||
		value.charCodeAt(value.length - 1) === 32 ||
		value.includes(delimiterChar) ||
		value.includes(quoteChar) ||
		value.includes("\r") ||
		value.includes("\n");

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

	// Build one row string: coerce each field, quote/escape where required,
	// then join with the delimiter (one flat string per row).
	const formatRow = (chunk) => {
		const parts = [];
		for (let i = 0; i < chunk.length; i++) {
			const raw = chunk[i];
			if (raw == null) {
				// null/undefined → empty field
				parts.push("");
				continue;
			}
			// Strings pass through String() unchanged; Dates use ISO 8601. An empty
			// string is never a quoting trigger, so scanNeedsQuote handles it
			// directly without a special case.
			const val = raw instanceof Date ? raw.toISOString() : String(raw);
			parts.push(scanNeedsQuote(val) ? wrapQuote(val) : val);
		}
		return parts.join(delimiterChar);
	};

	// Array batch: collect row strings, then join with newlineChar as
	// separator → one flat string per batch (vs ~128-node ConsString tree
	// from repeated buffer += concatenation)
	const batch = [];

	const transform = (chunk, enqueue) => {
		batch.push(formatRow(chunk));
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
			// defineProperty is used for every column so reserved keys such as
			// "__proto__" become own enumerable data properties instead of mutating
			// the object's prototype (which a plain `value[key] = ...` would do,
			// silently dropping the column). For ordinary keys this is equivalent
			// to a normal assignment.
			Object.defineProperty(value, resolvedKeys[i], {
				value: chunk[i],
				writable: true,
				enumerable: true,
				configurable: true,
			});
		}
		enqueue(value);
	};
	return createTransformStream(transform, streamOptions);
};
export const csvObjectToArray = ({ headers }, streamOptions) =>
	objectToEntriesStream({ keys: headers }, streamOptions);
