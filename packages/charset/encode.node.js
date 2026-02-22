// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";
import iconv from "iconv-lite";

export const charsetEncodeStream = ({ charset } = {}, streamOptions = {}) => {
	charset = getSupportedEncoding(charset);

	const conv = iconv.getEncoder(charset);
	const transform = (chunk, enqueue) => {
		const res = conv.write(chunk);
		if (res?.length) {
			enqueue(res);
		}
	};
	const flush = () => {
		// iconv-lite encoder.end() always returns undefined, so no flush needed
		conv.end();
	};
	return createTransformStream(transform, flush, streamOptions);
};

const getSupportedEncoding = (charset) => {
	if (charset === "ISO-8859-8-I") charset = "ISO-8859-8";
	if (!iconv.encodingExists(charset)) charset = "UTF-8";
	return charset;
};
export default charsetEncodeStream;
