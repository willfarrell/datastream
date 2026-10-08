// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	closeSync,
	constants,
	createReadStream,
	createWriteStream,
	ftruncateSync,
	lstatSync,
	openSync,
	realpathSync,
} from "node:fs";
import {
	dirname,
	extname,
	isAbsolute,
	relative,
	resolve,
	sep,
} from "node:path";
import { enforceType } from "./shared.js";

// fs streams are byte streams: datastream's objectMode/chunkSize options make
// fs throw uncatchably on string chunks, so strip only those and forward the
// rest (flags, encoding, start, mode, ...) untouched.
const fsOptions = ({
	objectMode,
	readableObjectMode,
	writableObjectMode,
	chunkSize,
	...options
}) => options;

export const fileReadStream = async (
	{ path, basePath, types },
	streamOptions = {},
) => {
	enforcePath(path, basePath);
	enforceType(extname(path), types);
	if ((basePath ?? null) !== null) {
		// Open with O_NOFOLLOW to prevent TOCTOU symlink race
		const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
		try {
			return createReadStream(null, { ...fsOptions(streamOptions), fd });
		} catch (e) {
			// Don't leak the descriptor when the stream never took ownership of it.
			closeSync(fd);
			throw e;
		}
	}
	return createReadStream(path, fsOptions(streamOptions));
};

export const fileWriteStream = async (
	{ path, basePath, types },
	streamOptions = {},
) => {
	enforcePath(path, basePath);
	enforceType(extname(path), types);
	if ((basePath ?? null) !== null) {
		// createWriteStream ignores `flags`/`mode` when an `fd` is supplied, so
		// they are applied to the O_NOFOLLOW open here instead.
		const { flags, mode, ...options } = fsOptions(streamOptions);
		const append = flags?.includes("a");
		// "r+"/"rs+" update an existing file in place: no create, no truncate.
		const update = flags?.startsWith("r");
		const fd = openSync(
			path,
			constants.O_WRONLY |
				(update ? 0 : constants.O_CREAT) |
				constants.O_NOFOLLOW |
				(append ? constants.O_APPEND : 0) |
				(flags?.includes("x") ? constants.O_EXCL : 0),
			mode,
		);
		try {
			const stream = createWriteStream(null, { ...options, fd });
			// Truncate only once the stream exists (rather than O_TRUNC on open) so
			// an invalid-options throw above leaves the existing file intact.
			if (!append && !update) ftruncateSync(fd);
			return stream;
		} catch (e) {
			// Don't leak the descriptor when the stream never took ownership of it.
			closeSync(fd);
			throw e;
		}
	}
	return createWriteStream(path, fsOptions(streamOptions));
};

// `..cache` is a valid name inside basePath; only a whole `..` segment escapes.
// isAbsolute covers Windows, where relative() across drives is absolute.
const escapes = (rel) =>
	rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel);

const enforcePath = (path, basePath) => {
	if ((basePath ?? null) !== null) {
		const resolvedPath = resolve(path);
		const resolvedBase = resolve(basePath);
		const rel = relative(resolvedBase, resolvedPath);
		if (rel === "" || escapes(rel)) {
			throw new Error("Path traversal detected");
		}
		let stat;
		try {
			stat = lstatSync(resolvedPath);
		} catch (e) {
			// File may not exist yet (for writes), that's ok
			if (e.code !== "ENOENT") {
				throw new Error("Path not found", { cause: e });
			}
		}
		if (stat?.isSymbolicLink()) {
			throw new Error("Symbolic links are not allowed");
		}
		// The checks above are lexical and only look at the final component, so a
		// symlinked directory (basePath/link -> /elsewhere) would still escape.
		// Compare the real parent directory against the real basePath.
		// Limit: a local attacker swapping a parent directory between this check
		// and the open (TOCTOU) is out of scope; O_NOFOLLOW only guards the leaf.
		let realRel;
		try {
			realRel = relative(
				realpathSync(resolvedBase),
				realpathSync(dirname(resolvedPath)),
			);
		} catch (e) {
			throw new Error("Path not found", { cause: e });
		}
		if (escapes(realRel)) {
			throw new Error("Path traversal detected");
		}
	}
};
