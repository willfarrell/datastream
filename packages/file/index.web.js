// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream } from "@datastream/core";

const enforceType = (name, types = []) => {
	if (!types.length) return;
	const dotIdx = name.lastIndexOf(".");
	const pathExt = dotIdx >= 0 ? name.slice(dotIdx) : "";
	for (const type of types) {
		for (const mime in type.accept) {
			for (const ext of type.accept[mime]) {
				if (pathExt === ext) {
					return;
				}
			}
		}
	}
	throw new Error("Invalid extension");
};

export const fileReadStream = async ({ types }, _streamOptions = {}) => {
	const [fileHandle] = await window.showOpenFilePicker({ types });
	const fileData = await fileHandle.getFile();
	enforceType(fileData.name, types);
	return createReadableStream(fileData);
};

export const fileWriteStream = async ({ path, types }, _streamOptions = {}) => {
	const fileHandle = await window.showSaveFilePicker({
		suggestedName: path,
		types,
	});
	enforceType(fileHandle.name, types);
	return fileHandle.createWritable();
};

export default {
	readStream: fileReadStream,
	writeStream: fileWriteStream,
};
