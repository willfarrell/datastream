// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { enforceType } from "./shared.js";

// A picked file has only a bare name, so the extension is everything from the
// last dot (a dotfile like ".csv" is its own extension, unlike path.extname).
const extension = (name) => {
	const dotIdx = name.lastIndexOf(".");
	return dotIdx >= 0 ? name.slice(dotIdx) : "";
};

export const fileReadStream = async ({ types }, _streamOptions = {}) => {
	const [fileHandle] = await window.showOpenFilePicker({ types });
	const fileData = await fileHandle.getFile();
	enforceType(extension(fileData.name), types);
	// A File is a Blob: not iterable, so createReadableStream can't wrap it.
	return fileData.stream();
};

export const fileWriteStream = async ({ path, types }, _streamOptions = {}) => {
	const fileHandle = await window.showSaveFilePicker({
		suggestedName: path,
		types,
	});
	enforceType(extension(fileHandle.name), types);
	return fileHandle.createWritable();
};
