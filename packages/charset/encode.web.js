// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextEncoderStream */

export const charsetDecodeStream = ({ charset } = {}, _streamOptions = {}) => {
	// doesn't support signal?
	return new TextEncoderStream(charset);
};

export default charsetDecodeStream;
