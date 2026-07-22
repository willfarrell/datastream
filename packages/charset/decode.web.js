// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextDecoderStream */

export const charsetDecodeStream = ({ charset } = {}, _streamOptions = {}) => {
	// Default/null charset means UTF-8, matching the node implementation which
	// falls back to UTF-8 for unknown/missing encodings instead of crashing.
	// `new TextDecoderStream(undefined)` already defaults to UTF-8, so normalise
	// null to undefined.
	if (charset === null || charset === undefined) {
		return new TextDecoderStream();
	}
	// Let TextDecoderStream validate the label rather than maintaining a manual
	// allowlist that is stricter than the platform (it rejected valid labels such
	// as ISO-8859-1 / ISO-8859-9 that TextDecoder accepts). The constructor
	// throws a RangeError for genuinely unknown labels; rethrow with the
	// package-branded message.
	try {
		return new TextDecoderStream(charset);
	} catch {
		throw new Error(
			`charsetDecodeStream: Unsupported web encoding "${charset}"`,
		);
	}
};

export default charsetDecodeStream;
