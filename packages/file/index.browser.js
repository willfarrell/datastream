// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { enforceType } from "./shared.js";

const extension = (name) => {
	const dotIdx = name.lastIndexOf(".");
	return dotIdx >= 0 ? name.slice(dotIdx) : "";
};

export const fileReadStream = async ({ types }, _streamOptions = {}) => {
	const [fileHandle] = await window.showOpenFilePicker({ types });
	const fileData = await fileHandle.getFile();
	enforceType(extension(fileData.name), types);
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
