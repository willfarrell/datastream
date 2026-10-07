// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

import { createTransformStream } from "@datastream/core";
import iconv from "iconv-lite";

export const charsetEncodeStream = ({ charset } = {}, streamOptions = {}) => {
	// A missing/null charset means UTF-8; a given but unknown one is an error
	// (like the web build), not a silent UTF-8 fallback. iconv-lite resolves
	// labels itself (including ISO-8859-8-I), so no label mapping is needed.
	const encoding = charset ?? "UTF-8";
	if (!iconv.encodingExists(encoding)) {
		throw new Error(`charsetEncodeStream: Unsupported encoding "${charset}"`);
	}

	const conv = iconv.getEncoder(encoding);
	const transform = (chunk, enqueue) => {
		// conv.write() always returns a Buffer (never nullish), so a plain
		// .length check is enough here.
		const res = conv.write(chunk);
		if (res.length) {
			enqueue(res);
		}
	};
	const flush = (enqueue) => {
		// Stateful encoders (e.g. UTF-7-IMAP) buffer multibyte content and emit
		// their trailing shift-out sequence only from end(); mirror decode.node.js
		// and enqueue that tail so no data is dropped.
		const res = conv.end();
		if (res?.length) {
			enqueue(res);
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};
