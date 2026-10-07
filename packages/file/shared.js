// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

// `ext` is computed by the caller: node uses path.extname (directory- and
// dotfile-aware), the browser only has a bare file name.
export const enforceType = (ext, types = []) => {
	if (!types.length) return;
	for (const type of types) {
		for (const mime in type.accept) {
			for (const accepted of type.accept[mime]) {
				if (ext === accepted) {
					return;
				}
			}
		}
	}
	throw new Error("Invalid extension");
};
