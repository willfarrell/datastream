// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

import { getSupportedEncoding } from "@datastream/charset/detect";
import { createTransformStream } from "@datastream/core";
import iconv from "iconv-lite";

export const charsetDecodeStream = ({ charset } = {}, streamOptions = {}) => {
	charset = getSupportedEncoding(charset);
	if (!iconv.encodingExists(charset)) charset = "UTF-8";

	const conv = iconv.getDecoder(charset);

	const transform = (chunk, enqueue) => {
		// conv.write() always returns a string (never nullish), so a plain
		// .length check is enough. The stream is objectMode, so enqueue ignores
		// any encoding argument; pass only the chunk.
		const res = conv.write(chunk);
		if (res.length) {
			enqueue(res);
		}
	};
	const flush = (enqueue) => {
		// conv.end() can return undefined for some decoders (e.g. ISO-8859-1),
		// so guard the length read with optional chaining.
		const res = conv.end();
		if (res?.length) {
			enqueue(res);
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export default charsetDecodeStream;
