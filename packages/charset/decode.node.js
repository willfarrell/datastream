// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

import { createTransformStream } from "@datastream/core";
import iconv from "iconv-lite";

export const charsetDecodeStream = ({ charset } = {}, streamOptions = {}) => {
	// A missing/null charset means UTF-8; a given but unknown one is an error
	// (like the web build), not a silent UTF-8 fallback. iconv-lite resolves
	// labels itself (including ISO-8859-8-I), so no label mapping is needed.
	const encoding = charset ?? "UTF-8";
	if (!iconv.encodingExists(encoding)) {
		throw new Error(`charsetDecodeStream: Unsupported encoding "${charset}"`);
	}

	const conv = iconv.getDecoder(encoding);

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
