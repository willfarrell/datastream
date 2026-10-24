// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextDecoderStream */

export const charsetDecodeStream = ({ charset } = {}, _streamOptions = {}) => {
	try {
		return new TextDecoderStream(charset ?? "utf-8");
	} catch {
		throw new Error(
			`charsetDecodeStream: Unsupported web encoding "${charset}"`,
		);
	}
};
