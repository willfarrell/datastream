// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	createChunkDecoder,
	// createPassThroughStream,
	createTransformStream,
	resolveLazy,
} from "@datastream/core";
import {
	objectFromEntriesStream,
	objectToEntriesStream,
} from "@datastream/object";

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

// Turns a stream chunk into text. Byte chunks (Buffer/Uint8Array) go through
// a streaming TextDecoder so a multi-byte UTF-8 character split across chunks
// is reassembled instead of being corrupted (per-chunk toString() would emit
// replacement chars). On flush, `decoder.flush()` emits any incomplete
// trailing byte sequence as U+FFFD ("" otherwise). The decoder keeps a leading
// BOM (ignoreBOM) so byte and string input share one BOM-stripping rule;
// otherwise a second BOM in byte input would also be stripped.
const decoderOptions = { ignoreBOM: true };

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
	let nextNl = text.indexOf(newlineChar, 0);
	for (;;) {
		// `pos` is a field start here, or (escapeChar === quoteChar only) just past
		// a quote where a second quote continues the field as an escaped "" pair.
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
			// When escapeChar === quoteChar a quote right after this one is the second
			// half of an escaped "" pair (the lookback only sees the run BEFORE the
			// quote), so re-enter the quoted-field scan. Otherwise fall through: text
			// after a closing quote (up to the next delimiter/newline) is literal, as
			// in the parser, so a quote there does not open another quoted field.
			if (escapeCode === quoteCode) continue;
		}
		// After a quoted-field skip pos can jump past the cached newline, so refresh
		// nextNl to the first newline at/after pos. A `while` (not `if`) guard keeps
		// this mutation-clean: any mutant that makes it over-resync re-finds the same
		// index and spins → timeout-killed; the `if` form's "always resync" mutant is
		// output-equivalent and would survive.
		while (nextNl !== -1 && nextNl < pos) {
			nextNl = text.indexOf(newlineChar, pos);
		}
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
		maxBufferSize = 16_777_216, // 16MB
		resultKey,
	} = options;
	const bufferLimit = maxBufferSize ?? Number.POSITIVE_INFINITY;

	const value = {
		delimiterChar: undefined,
		newlineChar: undefined,
		quoteChar: undefined,
		escapeChar: undefined,
	};

	const newlineCharRegExp = /[\r\n]/;
	const headerRegExp = new RegExp(
		`^([^${detectNewlineChars.join("")}]*)(${detectNewlineChars.join("|")})`,
	);

	let buffer = "";
	let detected = false;
	// detect() cannot succeed before the first CR/LF arrives, so it is skipped
	// until one has been seen in a chunk. Running it (and flattening the growing
	// `buffer` rope) on every chunk of a long first line was O(n^2).
	let newlineSeen = false;
	const decoder = createChunkDecoder(decoderOptions);

	const detect = (text, isFlushing) => {
		text = stripBOM(text);
		// No newline only happens on flush (transform waits for one): the whole
		// text is the header line and newlineChar stays undefined (default).
		const headerMatch = text.match(headerRegExp) ?? [text, text];
		// A bare CR at the very end of the buffer may be the first half of a CRLF
		// split across chunks, so wait for more data unless the stream is ending.
		if (
			!isFlushing &&
			headerMatch[2] === carageReturn &&
			headerMatch[0].length === text.length
		) {
			return false;
		}
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
		// Only the FIRST field-start opener needs checking: any closer that pairs
		// with a later opener also follows (and so pairs with) the first one. One
		// forward pass per candidate keeps this O(n); retrying every opener against
		// every later quote was O(n^2) (a CPU DoS on a long crafted first line).
		value.quoteChar =
			detectQuoteChars.find((candidate) => {
				let open = text.indexOf(candidate);
				while (open > -1 && !isFieldStart(open)) {
					open = text.indexOf(candidate, open + 1);
				}
				if (open === -1) return false;
				// Look for a closing quote that ends the field.
				let close = text.indexOf(candidate, open + 1);
				while (close > -1 && !isFieldEnd(close + 1)) {
					close = text.indexOf(candidate, close + 1);
				}
				return close > -1;
			}) ?? defaultQuoteChar;
		value.escapeChar = value.quoteChar;
		return true;
	};

	const transform = (chunk, enqueue) => {
		// Downstream always receives decoded text.
		const text = decoder.decode(chunk);
		if (detected) {
			enqueue(text);
			return;
		}
		buffer += text;
		newlineSeen ||= newlineCharRegExp.test(text);
		// detect() returns false until the buffer holds a complete first line, so
		// no size threshold is needed.
		if (newlineSeen && detect(buffer, false)) {
			detected = true;
			enqueue(buffer);
			// `buffer` is not read again once detected is set.
		} else if (buffer.length > bufferLimit) {
			throw new RangeError(
				`csvDetectDelimitersStream buffer (${buffer.length}) exceeds maxBufferSize (${maxBufferSize}), newline not found`,
			);
		}
	};

	const flush = (enqueue) => {
		const rest = decoder.flush();
		if (detected) {
			if (rest.length > 0) enqueue(rest);
			return;
		}
		buffer += rest;
		if (buffer.length > 0) {
			// Detect from whatever was buffered (may be a partial line) and emit it.
			detect(buffer, true);
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
		maxBufferSize = 16_777_216, // 16MB
		parser,
		delimiterChar,
		newlineChar,
		quoteChar,
		escapeChar,
		resultKey,
	} = options;
	const bufferLimit = maxBufferSize ?? Number.POSITIVE_INFINITY;

	// `header` is always assigned by processBuffer (which runs at the latest on
	// flush) before the stream's result() is read.
	const value = {};

	let buffer = "";
	let headerDetected = false;
	// Buffer length at which to retry locating the header row end. Re-scanning
	// (and flattening the growing `buffer` rope) on every chunk of a long header
	// row was O(n^2); retrying only once the buffer has doubled since the last
	// failed attempt is amortised O(n). Capped just past maxBufferSize so the
	// limit is still enforced as soon as it is exceeded.
	let nextAttempt = 0;
	const decoder = createChunkDecoder(decoderOptions);

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
		// Downstream always receives decoded text.
		const text = decoder.decode(chunk);
		if (headerDetected) {
			enqueue(text);
			return;
		}
		buffer += text;
		if (buffer.length < nextAttempt) return;
		resolveOptions();
		// Process once a complete header row is buffered (a quoted newline in the
		// header does not count); otherwise keep buffering.
		const headerEndOfRow = headerRowEnd();
		if (headerEndOfRow !== -1) {
			processBuffer(enqueue, headerEndOfRow);
		} else if (buffer.length > bufferLimit) {
			throw new RangeError(
				`csvDetectHeaderStream buffer (${buffer.length}) exceeds maxBufferSize (${maxBufferSize}), header row not found`,
			);
		} else {
			nextAttempt = Math.min(buffer.length * 2, bufferLimit + 1);
		}
	};

	const flush = (enqueue) => {
		const rest = decoder.flush();
		if (headerDetected) {
			if (rest.length > 0) enqueue(rest);
			return;
		}
		buffer += rest;
		// Whatever remains (possibly a partial header row, or nothing) is finalized
		// here. On empty input this yields an empty header and emits nothing.
		resolveOptions();
		processBuffer(enqueue, headerRowEnd());
	};

	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({ key: resultKey ?? "csvDetectHeader", value });
	return stream;
};

// --- Parsers ---
// Both return { rows: string[][], tail: string, numCols: number, idx: number, errors?: {} }
// Options can include pre-computed char codes (from csvStreamifyParser) or raw config strings.

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
	const maxFieldSize = ctx.maxFieldSize;
	// Every field (quoted or not) is size-checked; unquoted fields used to rely on
	// the 2x buffer safety limit only.
	const sized = (field) => {
		if (field.length > maxFieldSize) {
			throw new RangeError(
				`CSV field size (${field.length}) exceeds maxFieldSize (${maxFieldSize} bytes)`,
			);
		}
		return field;
	};

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

	let nextNl = text.indexOf(newlineChar, 0);
	// Text after a closing quote (e.g. `"a"x,b`) is malformed. It is kept in the
	// same field (appended to the quoted value, up to the next delimiter/newline)
	// so the row's field count stays correct. `quotedPrefix` holds the quoted
	// value while that trailing text is scanned as an unquoted field; null
	// otherwise.
	let quotedPrefix = null;
	// Set when the current row contains text after a closing quote. The error is
	// only recorded when the row is emitted, because an incomplete row is handed
	// back as the tail and re-parsed with the next chunk (recording it here would
	// count it twice).
	let rowHasUnexpectedQuote = false;

	const trackError = (id, message) => {
		errors ??= {};
		errors[id] ??= { id, message, idx: [] };
		errors[id].idx.push(idx);
	};

	const emit = (row) => {
		if (rowHasUnexpectedQuote) {
			rowHasUnexpectedQuote = false;
			trackError("UnexpectedQuote", "Unexpected text after closing quote");
		}
		enqueue(row);
	};

	while (pos < len) {
		// The outer loop is only (re)entered at a field start, so a quote here
		// always opens a quoted field (mid-field quotes are consumed by the
		// unquoted scan below and never reach this check) — unless it directly
		// follows a closing quote, in which case it is part of the malformed text.
		if (quotedPrefix === null && text.charCodeAt(pos) === quoteCharCode) {
			// === QUOTED FIELD ===
			lastWasDelimiter = false;
			pos++;
			const contentStart = pos;

			if (escapeIsQuote) {
				// Find the closing quote with indexOf, skipping escaped "" pairs.
				let closeQ = text.indexOf(quoteChar, pos);
				while (closeQ !== -1 && text.charCodeAt(closeQ + 1) === quoteCharCode) {
					closeQ = text.indexOf(quoteChar, closeQ + 2);
				}

				if (closeQ === -1) {
					// Unterminated quote
					if (isFlushing) {
						trackError("UnterminatedQuote", "Unterminated quoted field");
						const raw = text.substring(contentStart);
						// replaceAll is a no-op when the field carries no "" pair, so it is
						// applied unconditionally (a hadEscaped guard is an equivalent mutant).
						fields.push(raw.replaceAll(escapedQuote, quoteChar));
						if (numCols === 0) numCols = fields.length;
						emit(fields);
						idx++;
					}
					ctx.tail = isFlushing ? "" : text.substring(rowStart);
					ctx.numCols = numCols;
					ctx.idx = idx;
					ctx.errors = errors;
					return;
				}

				const slice = text.substring(contentStart, closeQ);
				const field = sized(slice.replaceAll(escapedQuote, quoteChar));
				pos = closeQ + 1;

				// Post-quote dispatch: delimiter, newline, end-of-input, or
				// unexpected text after the closing quote.
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
					emit(fields);
					idx++;
					fields = [];
					pos += newlineCharLength;
					rowStart = pos;
					fieldStart = pos;
					lastWasDelimiter = false;
					continue;
				}
				if (pos === len) {
					// End of input: record the field and let the outer loop terminate.
					fields.push(field);
					fieldStart = pos;
					continue;
				}
				// Unexpected text after closing quote: scan it as an unquoted field.
				quotedPrefix = field;
				rowHasUnexpectedQuote = true;
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
					emit(fields);
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
				const field = sized(
					unescapeCustom(text.substring(contentStart, closeQ), escapeChar),
				);
				pos = closeQ + 1;

				// Post-quote dispatch: see the escapeIsQuote branch above.
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
					emit(fields);
					idx++;
					fields = [];
					pos += newlineCharLength;
					rowStart = pos;
					fieldStart = pos;
					lastWasDelimiter = false;
					continue;
				}
				if (pos === len) {
					fields.push(field);
					fieldStart = pos;
					continue;
				}
				quotedPrefix = field;
				rowHasUnexpectedQuote = true;
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
			// nextNl is the first newline at/after pos, cached across a row's unquoted
			// fields so the row terminator is found once (O(n)). Refreshed with a
			// `while` (not `if`) guard when pos has advanced past it (new row, or a
			// quoted field that swallowed an embedded newline): a mutant that makes the
			// guard over-resync re-finds the same index and spins → timeout-killed,
			// whereas an `if` guard's "always resync" mutant is output-equivalent.
			while (nextNl !== -1 && nextNl < pos) {
				nextNl = text.indexOf(newlineChar, pos);
			}
			const nextDelim = text.indexOf(delimiterChar, pos);

			if (nextDelim !== -1 && (nextNl === -1 || nextDelim <= nextNl)) {
				// Field terminated by a delimiter (which wins a tie with the newline,
				// e.g. when the delimiter is a prefix of the newline) → more fields.
				fields.push(
					sized((quotedPrefix ?? "") + text.substring(fieldStart, nextDelim)),
				);
				quotedPrefix = null;
				pos = nextDelim + delimiterCharLength;
				fieldStart = pos;
				lastWasDelimiter = true;
				continue;
			}

			if (nextNl !== -1) {
				// Field terminated by a newline → end of row.
				fields.push(
					sized((quotedPrefix ?? "") + text.substring(fieldStart, nextNl)),
				);
				quotedPrefix = null;
				if (numCols === 0) numCols = fields.length;
				emit(fields);
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
		fields.push(sized((quotedPrefix ?? "") + text.substring(fieldStart)));
	} else if (lastWasDelimiter) {
		fields.push("");
	}
	if (fields.length > 0) {
		if (numCols === 0) numCols = fields.length;
		emit(fields);
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
		maxFieldSize: options.maxFieldSize ?? Number.POSITIVE_INFINITY,
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

const csvStreamifyParser = (options = {}) => {
	let {
		parser,
		delimiterChar,
		newlineChar,
		quoteChar,
		escapeChar,
		maxFieldSize,
		maxErrorRows,
	} = options;
	parser ??= csvQuotedParser;

	// Per-chunk parser context; every field is (re)assigned by resolveOptions and
	// the parser result before it is read (numCols/idx default via `?? 0`).
	const ctx = {};
	let buffer = "";
	const errors = {};
	// Byte chunks are decoded with a streaming decoder (see decoderOptions).
	const decoder = createChunkDecoder(decoderOptions);
	// Buffer length at which to re-parse an incomplete row (the parse tail).
	// Re-parsing the tail from its start on every chunk of a long row was
	// O(n^2); retrying only once the buffer has doubled is amortised O(n). A
	// row can only complete once more data arrives, so skipping is safe; with
	// no pending tail (0) every chunk is parsed straight away.
	let nextParse = 0;
	// A UTF-8 BOM is only meaningful at the very start of the stream (when
	// csvDetectHeaderStream is not used upstream to strip it).
	let isFirstChunk = true;

	// Keeps at most maxErrorRows row indexes per error id (so the result stays
	// bounded on large/untrusted input) while `count` tracks the true total.
	const mergeErrors = (incoming) => {
		for (const id in incoming) {
			const src = incoming[id].idx;
			errors[id] ??= {
				id: incoming[id].id,
				message: incoming[id].message,
				idx: [],
				count: 0,
			};
			const error = errors[id];
			error.count += src.length;
			const take = Math.min(src.length, maxErrorRows - error.idx.length);
			for (let i = 0; i < take; i++) error.idx.push(src[i]);
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
		ctx.maxFieldSize = maxFieldSize;
	};

	const streamFn = (chunk, enqueue) => {
		// resolveLazy is idempotent on already-resolved values, so re-resolving on
		// every chunk is safe and keeps lazy options deferred until upstream runs.
		resolveOptions();
		let chunkText = decoder.decode(chunk);
		// An empty chunk must not consume the BOM check: the BOM is the first
		// character of the first NON-EMPTY text.
		if (isFirstChunk && chunkText.length > 0) {
			isFirstChunk = false;
			chunkText = stripBOM(chunkText);
		}
		// An empty buffer concatenates to just the chunk.
		buffer += chunkText;
		if (buffer.length < nextParse) return;

		const result = parser(buffer, ctx, false);
		ctx.numCols = result.numCols;
		ctx.idx = result.idx;
		buffer = result.tail;
		// Only the unparsed tail (an incomplete row) can grow without bound, e.g. an
		// unterminated quote. Checking the whole chunk rejected large inputs made of
		// small rows. Between parses growth stays bounded: the gate above re-parses
		// once the buffer doubles past the last tail.
		if (buffer.length > ctx.maxFieldSize * 2) {
			throw new RangeError(
				`CSV buffer size (${buffer.length}) exceeds safety limit, likely unterminated quoted field`,
			);
		}
		nextParse = buffer.length * 2;
		mergeErrors(result.errors);
		const rows = result.rows;
		for (let i = 0; i < rows.length; i++) enqueue(rows[i]);
	};

	streamFn.flush = (enqueue) => {
		resolveOptions();
		// Emit any incomplete trailing byte sequence (as U+FFFD); "" otherwise.
		buffer += decoder.flush();
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
		maxFieldSize = 16_777_216, // 16MB
		maxErrorRows = 1_000,
		resultKey,
		...parserOptions
	} = options;
	parserOptions.maxFieldSize = maxFieldSize ?? Number.POSITIVE_INFINITY;
	parserOptions.maxErrorRows = maxErrorRows ?? Number.POSITIVE_INFINITY;

	const streamParse = csvStreamifyParser(parserOptions);

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

// Records a failing row index, keeping at most maxErrorRows indexes (so the
// result stays bounded on large/untrusted input); `count` is the true total.
const recordErrorRow = (error, idx, maxErrorRows) => {
	error.count++;
	if (error.idx.length < maxErrorRows) error.idx.push(idx);
};

export const csvRemoveMalformedRowsStream = (
	options = {},
	streamOptions = {},
) => {
	let { headers, onErrorEnqueue, maxErrorRows = 1_000, resultKey } = options;
	onErrorEnqueue ??= false;
	maxErrorRows ??= Number.POSITIVE_INFINITY;

	const value = {};
	let expectedColumns;
	let idx = -1;

	const transform = (chunk, enqueue) => {
		idx++;
		if (expectedColumns === undefined) {
			expectedColumns = resolveLazy(headers)?.length ?? chunk.length;
		}
		if (chunk.length !== expectedColumns) {
			value.MalformedRow ??= {
				id: "MalformedRow",
				message: "Row has incorrect number of fields",
				idx: [],
				count: 0,
			};
			recordErrorRow(value.MalformedRow, idx, maxErrorRows);
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
	let { onErrorEnqueue, maxErrorRows = 1_000, resultKey } = options;
	onErrorEnqueue ??= false;
	maxErrorRows ??= Number.POSITIVE_INFINITY;

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
			value.EmptyRow ??= {
				id: "EmptyRow",
				message: "Row is empty",
				idx: [],
				count: 0,
			};
			recordErrorRow(value.EmptyRow, idx, maxErrorRows);
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
// Only consulted for values starting with a formula trigger, so the sign is
// required (an optional sign would be an untestable no-op).
const signedNumberRe = /^[+-]\d+(\.\d+)?([eE][+-]?\d+)?$/;
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
			// Number("  ") is 0: treat whitespace-only like empty.
			if (typeof val === "string" && val.trim() === "") return null;
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

// Spreading copies a "__proto__" column as an own data property (spread uses
// CreateDataProperty), and assigning to an existing own "__proto__" data
// property updates it rather than replacing the prototype — so reserved keys
// stay own columns. Measured ~2.5x faster than per-key defineProperty.
export const csvCoerceValuesStream = (options = {}, streamOptions = {}) => {
	const { columns } = options;

	const transform = columns
		? (chunk, enqueue) => {
				const coerced = { ...chunk };
				for (const key in coerced) {
					// hasOwn: a column named "constructor"/"toString" must not pick up an
					// inherited Object.prototype member as its type.
					const type = Object.hasOwn(columns, key) ? columns[key] : undefined;
					coerced[key] = type
						? coerceToType(coerced[key], type)
						: autoCoerce(coerced[key]);
				}
				enqueue(coerced);
			}
		: (chunk, enqueue) => {
				const coerced = { ...chunk };
				for (const key in coerced) {
					coerced[key] = autoCoerce(coerced[key]);
				}
				enqueue(coerced);
			};

	return createTransformStream(transform, streamOptions);
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
	const escapeFormulae = options.escapeFormulae ?? true;

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
		value.includes("\n") ||
		value.includes(newlineChar);

	// CSV/formula injection: spreadsheets evaluate a cell starting with = + - @
	// (and TAB/CR, which some strip first) even when the field is quoted, so
	// prefix a ' to make it text. Plain signed numbers are left untouched.
	const isFormula = (value) => {
		// = (61) + (43) - (45) @ (64) TAB (9) CR (13)
		const first = value.charCodeAt(0);
		return (
			(first === 61 ||
				first === 43 ||
				first === 45 ||
				first === 64 ||
				first === 9 ||
				first === 13) &&
			!signedNumberRe.test(value)
		);
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

	// Build one row string: coerce each field, quote/escape where required,
	// then join with the delimiter (one flat string per row).
	const formatRow = (chunk) => {
		const parts = [];
		for (let i = 0; i < chunk.length; i++) {
			const raw = chunk[i];
			if (raw === null || raw === undefined) {
				// null/undefined → empty field
				parts.push("");
				continue;
			}
			// Strings pass through String() unchanged; Dates use ISO 8601. An empty
			// string is never a quoting trigger, so scanNeedsQuote handles it
			// directly without a special case.
			let val = raw instanceof Date ? raw.toISOString() : String(raw);
			if (escapeFormulae && isFormula(val)) val = `'${val}`;
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

// objectFromEntriesStream defines each column with defineProperty, so reserved
// keys such as "__proto__" stay own data properties instead of replacing the
// row's prototype.
export const csvArrayToObjectStream = ({ headers }, streamOptions) =>
	objectFromEntriesStream({ keys: headers }, streamOptions);
export const csvObjectToArrayStream = ({ headers }, streamOptions) =>
	objectToEntriesStream({ keys: headers }, streamOptions);
