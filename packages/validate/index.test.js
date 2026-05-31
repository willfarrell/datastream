import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import validateDefault, {
	transpileSchema,
	validateStream,
} from "@datastream/validate";

import Ajv from "ajv";

const ajv = new Ajv();

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** validateStream *** //
test(`${variant}: validateStream should validate using json schema`, async (_t) => {
	const input = [{ a: "1" }, { a: "2" }, { a: "3" }];
	const schema = {
		type: "object",
		properties: {
			a: {
				type: "number",
			},
		},
		required: ["a"],
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { a: 2 }, { a: 3 }]);
});

test(`${variant}: validateStream should validate using compiled json schema`, async (_t) => {
	const input = [{ a: "1" }, { a: "2" }, { a: "3" }];
	const schema = ajv.compile({
		type: "object",
		properties: {
			a: {
				type: "string",
			},
		},
		required: ["a"],
	});

	const streams = [createReadableStream(input), validateStream({ schema })];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: "1" }, { a: "2" }, { a: "3" }]);
});

test(`${variant}: validateStream should have errors in result`, async (_t) => {
	const input = [{ a: "1" }, { a: "a" }, { a: "3" }];
	const schema = {
		type: "object",
		properties: {
			a: {
				type: "number",
			},
		},
		required: ["a"],
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	deepStrictEqual(result, {
		validate: {
			"#/properties/a/type": {
				id: "#/properties/a/type",
				idx: [1],
				keys: ["a"],
				message: "must be number",
			},
		},
	});
});

test(`${variant}: validateStream should enqueue invalid items when onErrorEnqueue is true`, async (_t) => {
	const input = [{ a: "1" }, { a: "a" }, { a: "3" }];
	const schema = {
		type: "object",
		properties: {
			a: {
				type: "number",
			},
		},
		required: ["a"],
	};

	const streams = [
		createReadableStream(input),
		validateStream({ schema, onErrorEnqueue: true }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: 1 }, { a: "a" }, { a: 3 }]);
});

test(`${variant}: validateStream should use original uncoerced data when allowCoerceTypes is false`, async (_t) => {
	const input = [{ a: "1" }, { a: "2" }, { a: "3" }];
	const schema = {
		type: "object",
		properties: {
			a: {
				type: "number",
			},
		},
		required: ["a"],
	};

	const streams = [
		createReadableStream(input),
		validateStream({ schema, allowCoerceTypes: false }),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [{ a: "1" }, { a: "2" }, { a: "3" }]);
});

test(`${variant}: validateStream should use custom idxStart for error indices`, async (_t) => {
	const input = [{ a: "1" }, { a: "a" }, { a: "3" }];
	const schema = {
		type: "object",
		properties: {
			a: {
				type: "number",
			},
		},
		required: ["a"],
	};

	const streams = [
		createReadableStream(input),
		validateStream({ schema, idxStart: 10 }),
	];
	const result = await pipeline(streams);

	deepStrictEqual(result, {
		validate: {
			"#/properties/a/type": {
				id: "#/properties/a/type",
				idx: [11],
				keys: ["a"],
				message: "must be number",
			},
		},
	});
});

test(`${variant}: validateStream should handle missing required property`, async (_t) => {
	const input = [{ b: 1 }];
	const schema = {
		type: "object",
		properties: {
			a: { type: "number" },
		},
		required: ["a"],
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	deepStrictEqual(result.validate["#/required/a"].keys, ["a"]);
});

test(`${variant}: validateStream should handle additional properties`, async (_t) => {
	const input = [{ a: 1, extra: 2 }];
	const schema = {
		type: "object",
		properties: {
			a: { type: "number" },
		},
		additionalProperties: false,
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	deepStrictEqual(result.validate["#/additionalProperties/extra"].keys, [
		"extra",
	]);
});

test(`${variant}: validateStream should handle custom resultKey`, async (_t) => {
	const input = [{ a: 1 }];
	const schema = {
		type: "object",
		properties: {
			a: { type: "number" },
		},
		required: ["a"],
	};

	const streams = [
		createReadableStream(input),
		validateStream({ schema, resultKey: "custom" }),
	];
	const result = await pipeline(streams);

	deepStrictEqual(result.custom, {});
});

test(`${variant}: validateStream should handle streamOptions`, async (_t) => {
	const input = [{ a: 1 }];
	const schema = {
		type: "object",
		properties: {
			a: { type: "number" },
		},
		required: ["a"],
	};

	const streams = [
		createReadableStream(input),
		validateStream({ schema }, { objectMode: true }),
	];
	const result = await pipeline(streams);

	deepStrictEqual(result.validate, {});
});

test(`${variant}: validateStream should handle errorMessage keyword`, async (_t) => {
	const input = [{ a: "string" }];
	const schema = {
		type: "object",
		properties: {
			a: { type: "number" },
		},
		required: ["a"],
		errorMessage: {
			properties: {
				a: "Property a must be a number",
			},
		},
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	deepStrictEqual(
		result.validate["#/errorMessage"].message,
		"Property a must be a number",
	);
});

test(`${variant}: validateStream should handle nested object validation`, async (_t) => {
	const input = [{ nested: { value: "wrong" } }];
	const schema = {
		type: "object",
		properties: {
			nested: {
				type: "object",
				properties: {
					value: { type: "number" },
				},
			},
		},
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	// Should have error with instancePath set (nested/value)
	deepStrictEqual(typeof result.validate, "object");
	strictEqual(Object.keys(result.validate).length >= 1, true);
	// Check that instancePath was used (keys contains "nested/value")
	const errorKey = Object.keys(result.validate)[0];
	strictEqual(result.validate[errorKey].keys.includes("nested/value"), true);
});

test(`${variant}: validateStream should handle multiple validation errors`, async (_t) => {
	const input = [{ a: "wrong", b: "wrong" }];
	const schema = {
		type: "object",
		properties: {
			a: { type: "number" },
			b: { type: "number" },
		},
		required: ["a", "b"],
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	// Should have multiple error types
	strictEqual(Object.keys(result.validate).length >= 1, true);
});

test(`${variant}: validateStream should handle error without message property`, async (_t) => {
	// This tests the `error.message || ""` branch
	// We need a schema that produces an error with empty/no message
	const input = [{ a: 1 }];
	const schema = {
		type: "object",
		properties: {
			a: { type: "integer", maximum: 0 },
		},
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	// Should have maximum error
	strictEqual(Object.keys(result.validate).length >= 1, true);
});

test(`${variant}: validateStream should handle additionalProperty with instancePath`, async (_t) => {
	const input = [{ nested: { extra: "value" } }];
	const schema = {
		type: "object",
		properties: {
			nested: {
				type: "object",
				additionalProperties: false,
			},
		},
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	strictEqual(Object.keys(result.validate).length >= 1, true);
});

test(`${variant}: validateStream should handle missingProperty with empty instancePath`, async (_t) => {
	const input = [{}];
	const schema = {
		type: "object",
		properties: {
			a: { type: "string" },
		},
		required: ["a"],
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	strictEqual(Object.keys(result.validate).length >= 1, true);
	// The error should have keys populated
	strictEqual(
		result.validate[Object.keys(result.validate)[0]].keys.length >= 1,
		true,
	);
});

test(`${variant}: validateStream should handle root-level type error`, async (_t) => {
	const schema = { type: "object" };
	const input = ["not-an-object"];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length > 0);
	const errorKey = Object.keys(result.validate)[0];
	// root-level errors produce an empty makeKeys result; empty keys are filtered out
	deepStrictEqual(result.validate[errorKey].keys, []);
});

test(`${variant}: validateStream should handle errorMessage with root-level error`, async (_t) => {
	const schema = {
		type: "object",
		properties: {
			a: { type: "string" },
			b: { type: "string" },
		},
		required: ["a", "b"],
		errorMessage: {
			required: "Missing required fields",
		},
	};
	const input = [{}];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length > 0);
});

test(`${variant}: validateStream should handle errorMessage with falsy makeKeys`, async (_t) => {
	const schema = {
		type: "object",
		errorMessage: {
			type: "Must be an object",
		},
	};
	const input = ["not-an-object"];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length > 0);
	const errorKey = Object.keys(result.validate)[0];
	strictEqual(result.validate[errorKey].message, "Must be an object");
});

test(`${variant}: validateStream should handle pre-compiled schema with no messages`, async (_t) => {
	const schema = transpileSchema(
		{
			type: "object",
			properties: {
				a: { type: "number" },
			},
		},
		{ messages: false },
	);
	const input = [{ a: "not-a-number" }];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length > 0);
	const errorKey = Object.keys(result.validate)[0];
	strictEqual(result.validate[errorKey].message, "");
});

// *** maxErrorRows *** //
test(`${variant}: validateStream should cap error indices with maxErrorRows`, async (_t) => {
	const input = Array.from({ length: 100 }, (_, i) => ({ a: `bad${i}` }));
	const schema = {
		type: "object",
		properties: {
			a: { type: "number" },
		},
	};

	const streams = [
		createReadableStream(input),
		validateStream({ schema, maxErrorRows: 10 }),
	];
	const result = await pipeline(streams);

	const errorKey = Object.keys(result.validate)[0];
	ok(result.validate[errorKey].idx.length <= 10);
});

// *** maxErrorKeys *** //
test(`${variant}: validateStream should cap distinct error ids with maxErrorKeys`, async (_t) => {
	// Feed 500 rows, each with a unique extra property name → would produce 500 distinct ids
	const schema = {
		type: "object",
		additionalProperties: false,
		properties: { a: { type: "number" } },
	};
	const input = Array.from({ length: 500 }, (_, i) => ({
		a: 1,
		[`extra_${i}`]: i,
	}));
	const streams = [
		createReadableStream(input),
		validateStream({ schema, maxErrorKeys: 50 }),
	];
	const result = await pipeline(streams);

	// Should never exceed maxErrorKeys distinct entries
	ok(Object.keys(result.validate).length <= 50);
});

test(`${variant}: validateStream should default maxErrorKeys to 1000`, async (_t) => {
	// 1500 rows each with a unique extra property → default cap of 1000 should kick in
	const schema = {
		type: "object",
		additionalProperties: false,
		properties: { a: { type: "number" } },
	};
	const input = Array.from({ length: 1500 }, (_, i) => ({
		a: 1,
		[`extra_${i}`]: i,
	}));
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length <= 1000);
});

// *** allowCoerceTypes:false with non-cloneable chunks *** //
test(`${variant}: validateStream should not throw when allowCoerceTypes:false and chunk contains non-cloneable value`, async (_t) => {
	const schema = {
		type: "object",
		properties: { a: { type: "number" } },
		required: ["a"],
	};
	// Chunk with a function (not structuredClone-able)
	const input = [{ a: 1, fn: () => {} }];

	const streams = [
		createReadableStream(input),
		validateStream({ schema, allowCoerceTypes: false }),
	];
	// Should not throw — should fall back to original chunk
	const result = await pipeline(streams);
	ok(result, "pipeline should complete without error");
});

// *** root-level errors: no trailing-slash id, no empty-string key *** //
test(`${variant}: validateStream root-level error should not have trailing-slash id or empty-string key`, async (_t) => {
	const schema = { type: "object" };
	const input = ["not-an-object"];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length > 0);
	for (const [id, entry] of Object.entries(result.validate)) {
		// id must not end with '/'
		strictEqual(id.endsWith("/"), false, `id '${id}' must not end with '/'`);
		// keys must not include ''
		strictEqual(
			entry.keys.includes(""),
			false,
			`keys must not include empty string`,
		);
	}
});

// *** default path should not mutate caller's input *** //
test(`${variant}: validateStream should emit a clone so caller input is not mutated`, async (_t) => {
	// coerceTypes:true would mutate {a:'1'} → {a:1} on the original object
	const original = { a: "1" };
	const input = [original];
	const schema = {
		type: "object",
		properties: { a: { type: "number" } },
		required: ["a"],
	};

	const streams = [createReadableStream(input), validateStream({ schema })];
	const output = await streamToArray(pipejoin(streams));

	// The emitted chunk should be coerced
	strictEqual(output[0].a, 1);
	// But the original reference must NOT be mutated
	strictEqual(
		original.a,
		"1",
		"caller's original object must not be mutated in place",
	);
});

// *** no-schema branded error *** //
test(`${variant}: validateStream should throw a branded error when schema is missing`, (_t) => {
	try {
		validateStream({});
		ok(false, "should have thrown");
	} catch (err) {
		ok(
			err.message.includes("validateStream") && err.message.includes("schema"),
			`expected branded error mentioning validateStream and schema, got: ${err.message}`,
		);
	}
});

// *** default export *** //
test(`${variant}: default export should be validateStream`, (_t) => {
	strictEqual(validateDefault, validateStream);
});

// *** transpileSchema strict:true mode *** //
test(`${variant}: transpileSchema should use strict:true by default and reject schemas with unknown keywords`, (_t) => {
	// In strict mode, AJV throws on unknown keywords in schemas
	let threw = false;
	try {
		transpileSchema({ type: "object", unknownKeyword: true });
	} catch {
		threw = true;
	}
	// strict:false would silently accept unknown keywords; strict:true throws
	strictEqual(
		threw,
		true,
		"transpileSchema should throw on unknown keywords in strict mode",
	);
});

// *** transpileSchema useDefaults:"empty" *** //
test(`${variant}: transpileSchema should apply default values for empty strings (useDefaults:"empty")`, async (_t) => {
	// useDefaults:"empty" fills defaults even when value is an empty string
	const schema = {
		type: "object",
		properties: {
			name: { type: "string", default: "fallback" },
		},
	};
	const input = [{ name: "" }];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const output = await streamToArray(pipejoin(streams));
	// useDefaults:"empty" should replace empty string with the default
	deepStrictEqual(output[0].name, "fallback");
});

// *** catch block fallback: non-cloneable in allowCoerceTypes:false path *** //
test(`${variant}: validateStream allowCoerceTypes:false with non-cloneable chunk emits original chunk`, async (_t) => {
	const schema = {
		type: "object",
		properties: { a: { type: "string" } },
	};
	const original = { a: "hello", fn: () => {} };
	const input = [original];
	const streams = [
		createReadableStream(input),
		validateStream({ schema, allowCoerceTypes: false }),
	];
	const output = await streamToArray(pipejoin(streams));
	// Should emit the original object reference (the chunk itself, not a clone)
	strictEqual(
		output[0],
		original,
		"non-cloneable chunk should emit original reference when allowCoerceTypes:false",
	);
	strictEqual(output[0].a, "hello");
});

// *** catch block fallback: non-cloneable in default (coerce) path *** //
test(`${variant}: validateStream default coerce path with non-cloneable chunk still emits chunk`, async (_t) => {
	const schema = {
		type: "object",
		properties: { a: { type: "string" } },
	};
	const original = { a: "hello", fn: () => {} };
	const input = [original];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const output = await streamToArray(pipejoin(streams));
	// Should emit something (the original chunk when clone fails)
	strictEqual(output[0].a, "hello");
});

// *** if (!value[id]) guard: same error should accumulate idx, not reset *** //
test(`${variant}: validateStream should accumulate idx for repeated same-key errors`, async (_t) => {
	// Multiple rows with same type error → same id → idx should accumulate
	const input = [{ a: "bad" }, { a: "bad" }, { a: "bad" }];
	const schema = {
		type: "object",
		properties: { a: { type: "number" } },
	};
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);
	const errorKey = Object.keys(result.validate)[0];
	// All three rows should appear in idx
	deepStrictEqual(result.validate[errorKey].idx, [0, 1, 2]);
});

// *** if (chunkValid || onErrorEnqueue): invalid items NOT enqueued by default *** //
test(`${variant}: validateStream should not enqueue invalid items when onErrorEnqueue is not set`, async (_t) => {
	const input = [
		{ a: "valid_number_string" },
		{ a: "definitely-not-a-number" },
	];
	const schema = {
		type: "object",
		properties: { a: { type: "number" } },
	};
	const streams = [createReadableStream(input), validateStream({ schema })];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	// Only valid (coerced) items should be emitted — "definitely-not-a-number" can't coerce
	// "valid_number_string" also can't coerce; both invalid → output empty
	deepStrictEqual(output.length, 0);
});

// *** errorMessage keyword: branch must populate keys from sub-errors *** //
test(`${variant}: validateStream errorMessage should collect keys from sub-errors`, async (_t) => {
	const schema = {
		type: "object",
		properties: {
			age: { type: "number" },
		},
		required: ["age"],
		errorMessage: {
			properties: {
				age: "Age must be a number",
			},
		},
	};
	const input = [{ age: "not-a-number" }];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length > 0);
	const errorEntry = result.validate["#/errorMessage"];
	ok(errorEntry, "errorMessage error entry should exist");
	// Keys should be populated from the sub-error (age)
	ok(
		errorEntry.keys.includes("age"),
		`keys should include 'age', got: ${JSON.stringify(errorEntry.keys)}`,
	);
	strictEqual(errorEntry.message, "Age must be a number");
});

// *** errorMessage keys sorted and deduped *** //
test(`${variant}: validateStream errorMessage should sort keys alphabetically (z_field before a_field in required → sorted as a_field|z_field)`, async (_t) => {
	// Required array has z_field FIRST so AJV returns sub-errors as [z_field, a_field].
	// The sort() must reorder them to [a_field, z_field] for the id to be correct.
	const schema = {
		type: "object",
		properties: {
			z_field: { type: "string" },
			a_field: { type: "string" },
		},
		required: ["z_field", "a_field"],
		errorMessage: {
			required: "Both fields are required",
		},
	};
	const input = [{}];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length > 0);
	// Without sort: id would be "#/errorMessage/z_field|a_field"
	// With sort: id must be "#/errorMessage/a_field|z_field"
	const errorEntry = result.validate["#/errorMessage/a_field|z_field"];
	ok(
		errorEntry,
		"error entry id '#/errorMessage/a_field|z_field' must exist (keys sorted)",
	);
	deepStrictEqual(errorEntry.keys, ["a_field", "z_field"]);
	strictEqual(errorEntry.message, "Both fields are required");
});

// *** errorMessage: falsy makeKeys values should NOT be pushed into keys *** //
test(`${variant}: validateStream errorMessage sub-errors with empty instancePath produce no empty string keys`, async (_t) => {
	const schema = {
		type: "object",
		errorMessage: "Must be an object",
	};
	const input = ["not-an-object"];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	ok(Object.keys(result.validate).length > 0);
	for (const entry of Object.values(result.validate)) {
		strictEqual(
			entry.keys.includes(""),
			false,
			"keys must not include empty string from errorMessage sub-errors",
		);
	}
});

// *** id suffix: multiple keys joined with "|" not "" *** //
test(`${variant}: validateStream error id should join multiple keys with pipe separator`, async (_t) => {
	// Use required errorMessage with two required fields. Both missing → two sub-errors
	// → keys = ["a_field", "b_field"] → id = "#/errorMessage/a_field|b_field"
	const schema = {
		type: "object",
		properties: {
			b_field: { type: "string" },
			a_field: { type: "string" },
		},
		required: ["a_field", "b_field"],
		errorMessage: {
			required: "Fields a_field and b_field are required",
		},
	};
	const input = [{}];
	const streams = [createReadableStream(input), validateStream({ schema })];
	const result = await pipeline(streams);

	// The entry id MUST be "#/errorMessage/a_field|b_field" — pipe not empty-string join
	const errorEntry = result.validate["#/errorMessage/a_field|b_field"];
	ok(errorEntry, "error entry id '#/errorMessage/a_field|b_field' must exist");
	deepStrictEqual(errorEntry.keys, ["a_field", "b_field"]);
	strictEqual(errorEntry.message, "Fields a_field and b_field are required");
});
