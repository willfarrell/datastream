// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

import { getSupportedEncoding } from "@datastream/charset/detect";
import { createTransformStream } from "@datastream/core";
import iconv from "iconv-lite";

export const charsetEncodeStream = ({ charset } = {}, streamOptions = {}) => {
	charset = getSupportedEncoding(charset);
	if (!iconv.encodingExists(charset)) charset = "UTF-8";

	const conv = iconv.getEncoder(charset);
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

export default charsetEncodeStream;
