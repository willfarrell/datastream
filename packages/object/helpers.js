// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// Clone / equality helpers used by the object streams (moved from
// @datastream/core, whose only user was this package). Imported relatively so
// esbuild leaves the import in place; shipped via package.json `files`.
export const shallowClone = (obj) => ({ ...obj });

export const deepClone = (obj) => {
	try {
		return structuredClone(obj);
	} catch (e) {
		throw new Error("Failed to clone chunk, possibly circular reference", {
			cause: e,
		});
	}
};

export const shallowEqual = (a, b) => {
	if (a === b) return true;
	if (a === null || a === undefined || b === null || b === undefined) {
		return false;
	}
	const keysA = Object.keys(a);
	if (keysA.length !== Object.keys(b).length) return false;
	for (const key of keysA) {
		if (a[key] !== b[key]) return false;
	}
	return true;
};

// Structural compare standing in for node:util's isDeepStrictEqual (which the
// node build uses via the `#deepEqual` import): key-order insensitive,
// Object.is for primitives (NaN), Date/RegExp/Map/Set/typed arrays, cycle-safe.
// Lives here (not in deepEqual.browser.js) so the node test run covers it too.
// ponytail: Set members and Map keys compare by identity, not structure;
// upgrade to pairwise deep matching if a caller ever needs deep Set/Map members.
const deepEqualInner = (a, b, seen) => {
	if (Object.is(a, b)) return true;
	if (
		typeof a !== "object" ||
		typeof b !== "object" ||
		a === null ||
		b === null
	) {
		return false;
	}
	if (Object.getPrototypeOf(a) !== Object.getPrototypeOf(b)) return false;
	if (seen.get(a) === b) return true;
	seen.set(a, b);
	if (a instanceof Date) return a.getTime() === b.getTime();
	if (a instanceof RegExp) return a.source === b.source && a.flags === b.flags;
	if (a instanceof Set) {
		if (a.size !== b.size) return false;
		for (const v of a) if (!b.has(v)) return false;
		return true;
	}
	if (a instanceof Map) {
		if (a.size !== b.size) return false;
		for (const [k, v] of a) {
			if (!b.has(k) || !deepEqualInner(v, b.get(k), seen)) return false;
		}
		return true;
	}
	// Typed arrays fall through: their own enumerable keys are the indices.
	const keys = Object.keys(a);
	if (keys.length !== Object.keys(b).length) return false;
	for (const key of keys) {
		if (!Object.hasOwn(b, key) || !deepEqualInner(a[key], b[key], seen)) {
			return false;
		}
	}
	return true;
};
export const deepEqual = (a, b) => deepEqualInner(a, b, new Map());
