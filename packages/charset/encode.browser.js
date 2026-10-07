// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextEncoderStream */

export const charsetEncodeStream = ({ charset } = {}, _streamOptions = {}) => {
	// A missing/null charset means UTF-8, matching the node implementation. Only
	// a non-UTF-8 charset is rejected; calling with no charset must not throw.
	// Labels match case-insensitively with an optional hyphen ("utf8", "utf-8"),
	// as iconv-lite in the node build does.
	if (!/^utf-?8$/i.test(charset ?? "UTF-8")) {
		throw new Error(
			`charsetEncodeStream: Web only supports UTF-8 encoding, got "${charset}"`,
		);
	}
	return new TextEncoderStream();
};
