// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextDecoderStream */

export const charsetDecodeStream = ({ charset } = {}, _streamOptions = {}) => {
	// Let TextDecoderStream validate the label rather than maintaining a manual
	// allowlist that is stricter than the platform (it rejected valid labels such
	// as ISO-8859-1 / ISO-8859-9 that TextDecoder accepts). The constructor
	// throws a RangeError for genuinely unknown labels; rethrow with the
	// package-branded message. A missing/null charset means UTF-8, matching the
	// node implementation.
	try {
		return new TextDecoderStream(charset ?? "utf-8");
	} catch {
		throw new Error(
			`charsetDecodeStream: Unsupported web encoding "${charset}"`,
		);
	}
};
