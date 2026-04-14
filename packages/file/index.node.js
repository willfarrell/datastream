// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadStream, createWriteStream, lstatSync } from "node:fs";
import { extname, resolve } from "node:path";
import { makeOptions } from "@datastream/core";

export const fileReadStream = (
	{ path, basePath, types },
	streamOptions = {},
) => {
	enforcePath(path, basePath);
	enforceType(path, types);
	return createReadStream(path, makeOptions(streamOptions));
};

export const fileWriteStream = (
	{ path, basePath, types },
	streamOptions = {},
) => {
	enforcePath(path, basePath);
	enforceType(path, types);
	return createWriteStream(path, makeOptions(streamOptions));
};

const enforcePath = (path, basePath) => {
	if (basePath != null) {
		const resolvedPath = resolve(path);
		const resolvedBase = resolve(basePath);
		if (!resolvedPath.startsWith(resolvedBase)) {
			throw new Error("Path traversal detected");
		}
		try {
			const stat = lstatSync(resolvedPath);
			if (stat.isSymbolicLink()) {
				throw new Error("Symbolic links are not allowed");
			}
		} catch (e) {
			if (e.message === "Symbolic links are not allowed") throw e;
			throw new Error("Path not found", { cause: e });
		}
	}
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
		throw new Error("Invalid extension");
	}
};

export default {
	readStream: fileReadStream,
	writeStream: fileWriteStream,
};
