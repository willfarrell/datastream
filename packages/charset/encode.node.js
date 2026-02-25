// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";
import iconv from "iconv-lite";
import { getSupportedEncoding } from "./detect.js";

export const charsetEncodeStream = ({ charset } = {}, streamOptions = {}) => {
	charset = getSupportedEncoding(charset);
	if (!iconv.encodingExists(charset)) charset = "UTF-8";

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

export default charsetEncodeStream;
