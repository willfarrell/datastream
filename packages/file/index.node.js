// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadStream, createWriteStream } from "node:fs";
import { extname } from "node:path";
import { makeOptions } from "@datastream/core";

export const fileReadStream = ({ path, types }, streamOptions = {}) => {
	enforceType(path, types);
	return createReadStream(path, makeOptions(streamOptions));
};

export const fileWriteStream = ({ path, types }, streamOptions = {}) => {
	enforceType(path, types);
	return createWriteStream(path, makeOptions(streamOptions));
};

const enforceType = (path, types = []) => {
	const pathExt = extname(path);
	for (const type of types) {
		for (const mime in type.accept) {
			for (const ext of type.accept[mime]) {
				if (pathExt === ext) {
					return;
				}
			}
		}
	}
	if (types.length) {
		throw new Error("invalid extension");
	}
};

export default {
	readStream: fileReadStream,
	writeStream: fileWriteStream,
};
