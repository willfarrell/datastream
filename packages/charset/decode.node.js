// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";
import iconv from "iconv-lite";
import { getSupportedEncoding } from "./detect.js";

export const charsetDecodeStream = ({ charset } = {}, streamOptions = {}) => {
	charset = getSupportedEncoding(charset);
	if (!iconv.encodingExists(charset)) charset = "UTF-8";

	const conv = iconv.getDecoder(charset);

	const transform = (chunk, enqueue) => {
		const res = conv.write(chunk);
		if (res?.length) {
			enqueue(res, "utf8");
		}
	};
	const flush = (enqueue) => {
		const res = conv.end();
		if (res?.length) {
			enqueue(res, "utf8");
		}
	};
	return createTransformStream(transform, flush, streamOptions);
};

export default charsetDecodeStream;
