// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

export const charsetEncodeStream = ({ charset } = {}, _streamOptions = {}) => {
	if (!/^utf-?8$/i.test(charset ?? "UTF-8")) {
		throw new Error(
			`charsetEncodeStream: Web only supports UTF-8 encoding, got "${charset}"`,
		);
	}
	return new TextEncoderStream();
};
