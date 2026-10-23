// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
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
		if (!Object.hasOwn(b, key) || a[key] !== b[key]) return false;
	}
	return true;
};

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
