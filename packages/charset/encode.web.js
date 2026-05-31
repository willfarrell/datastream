// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextEncoderStream */

// NOTE: web vs node parity caveat. The browser TextEncoder genuinely only
// supports UTF-8, so the web encoder is UTF-8-only and throws for any other
// charset. This is intentionally asymmetric with charsetDecodeStream (web),
// which accepts every label TextDecoder supports (UTF-16, ISO-8859-1, ...), and
// with the node encoder, which supports the full iconv set. A detect->encode
// pipeline that selects a non-UTF-8 charset works under node but throws under
// web; callers targeting the browser must encode to UTF-8.
export const charsetEncodeStream = ({ charset } = {}, _streamOptions = {}) => {
	// Default/null charset means UTF-8, matching the node implementation. Only a
	// non-UTF-8 charset is rejected; calling with no charset must not throw.
	if (
		charset !== null &&
		charset !== undefined &&
		String(charset).toUpperCase() !== "UTF-8"
	) {
		throw new Error(
			`charsetEncodeStream: Web only supports UTF-8 encoding, got "${charset}"`,
		);
	}
	return new TextEncoderStream();
};

export default charsetEncodeStream;
