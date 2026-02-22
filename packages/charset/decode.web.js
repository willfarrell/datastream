// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextDecoderStream */

export const charsetDecodeStream = ({ charset } = {}, _streamOptions = {}) => {
	// doesn't support signal?
	return new TextDecoderStream(charset);
};

export default charsetDecodeStream;
