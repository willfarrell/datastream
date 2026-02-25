// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
export const indexedDBReadStream = async (
	_options = {},
	_streamOptions = {},
) => {
	throw new Error("indexedDBReadStream: Not supported");
};

export const indexedDBWriteStream = async (
	_options = {},
	_streamOptions = {},
) => {
	throw new Error("indexedDBWriteStream: Not supported");
};

export default {
	readStream: indexedDBReadStream,
	writeStream: indexedDBWriteStream,
};
