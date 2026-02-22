// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream } from "@datastream/core";
import iconv from "iconv-lite";

export const charsetDecodeStream = ({ charset } = {}, streamOptions = {}) => {
	charset = getSupportedEncoding(charset);

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

// TODO to peek ahead and determin charset from that
/*
export const charsetDetectDecodeStream = (
  { charset, peekAhead } = { peekAhead: 0 },
  streamOptions
) => {
  charset = getSupportedEncoding(charset)

  let conv,
    peekAheadChunk = ''

  const transform = (chunk, enqueue) => {
    if (!conv && peekahead && peekahead < peekAheadChunk.length) {
      peekAheadChunk += chunk
      return
    }
    if (!conv) {
      conv = iconv.getDecoder(charset)
      chunk = peekAheadChunk + chunk
    }

    const res = conv.write(chunk)
    if (res?.length) {
      enqueue(res, 'utf8')
    }
  }
  const flush = (enqueue) => {
    const res = conv.end()
    if (res?.length) {
      enqueue(res, 'utf8')
    }
  }
  return createTransformStream(transform, flush, streamOptions)
} */

const getSupportedEncoding = (charset) => {
	if (charset === "ISO-8859-8-I") charset = "ISO-8859-8";
	if (!iconv.encodingExists(charset)) charset = "UTF-8";
	return charset;
};
export default charsetDecodeStream;
