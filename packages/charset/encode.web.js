// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextEncoderStream */

export const charsetEncodeStream = ({ charset } = {}, _streamOptions = {}) => {
	if (charset !== null && charset.toUpperCase() !== "UTF-8") {
		throw new Error(
			`charsetEncodeStream: Web only supports UTF-8 encoding, got "${charset}"`,
		);
	}
	return new TextEncoderStream();
};

export default charsetEncodeStream;
