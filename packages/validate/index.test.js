import { deepStrictEqual, notStrictEqual, ok, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { transpileSchema, validateStream } from "@datastream/validate";
import Ajv from "ajv";
import { variant } from "../variant.js";

const ajv = new Ajv();

describe(`@datastream/validate (${variant})`, () => {
	test(`validateStream should validate using json schema`, async (_t) => {
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

	test(`validateStream should validate using compiled json schema`, async (_t) => {
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

	test(`validateStream should have errors in result`, async (_t) => {
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

	test(`validateStream should enqueue invalid items when onErrorEnqueue is true`, async (_t) => {
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

	test(`validateStream should use original uncoerced data when allowCoerceTypes is false`, async (_t) => {
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

	test(`validateStream should use custom idxStart for error indices`, async (_t) => {
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

	test(`validateStream should handle missing required property`, async (_t) => {
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

	test(`validateStream should handle additional properties`, async (_t) => {
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

	test(`validateStream should handle custom resultKey`, async (_t) => {
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

	test(`validateStream should handle streamOptions`, async (_t) => {
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

	test(`validateStream should handle errorMessage keyword`, async (_t) => {
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

	test(`validateStream should handle nested object validation`, async (_t) => {
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

		deepStrictEqual(typeof result.validate, "object");
		strictEqual(Object.keys(result.validate).length >= 1, true);
		const errorKey = Object.keys(result.validate)[0];
		strictEqual(result.validate[errorKey].keys.includes("nested/value"), true);
	});

	test(`validateStream should handle multiple validation errors`, async (_t) => {
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

		strictEqual(Object.keys(result.validate).length >= 1, true);
	});

	test(`validateStream should handle error without message property`, async (_t) => {
		const input = [{ a: 1 }];
		const schema = {
			type: "object",
			properties: {
				a: { type: "integer", maximum: 0 },
			},
		};

		const streams = [createReadableStream(input), validateStream({ schema })];
		const result = await pipeline(streams);

		strictEqual(Object.keys(result.validate).length >= 1, true);
	});

	test(`validateStream should handle additionalProperty with instancePath`, async (_t) => {
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

	test(`validateStream should handle missingProperty with empty instancePath`, async (_t) => {
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
		strictEqual(
			result.validate[Object.keys(result.validate)[0]].keys.length >= 1,
			true,
		);
	});

	test(`validateStream should handle root-level type error`, async (_t) => {
		const schema = { type: "object" };
		const input = ["not-an-object"];
		const streams = [createReadableStream(input), validateStream({ schema })];
		const result = await pipeline(streams);

		ok(Object.keys(result.validate).length > 0);
		const errorKey = Object.keys(result.validate)[0];
		deepStrictEqual(result.validate[errorKey].keys, []);
	});

	test(`validateStream should handle errorMessage with root-level error`, async (_t) => {
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

	test(`validateStream should handle errorMessage with falsy makeKeys`, async (_t) => {
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

	test(`validateStream should handle pre-compiled schema with no messages`, async (_t) => {
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

	test(`validateStream should cap error indices with maxErrorRows`, async (_t) => {
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

	test(`validateStream should cap distinct error ids with maxErrorKeys`, async (_t) => {
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

		ok(Object.keys(result.validate).length <= 50);
	});

	test(`validateStream should default maxErrorKeys to 1000`, async (_t) => {
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

	test(`validateStream should not throw when allowCoerceTypes:false and chunk contains non-cloneable value`, async (_t) => {
		const schema = {
			type: "object",
			properties: { a: { type: "number" } },
			required: ["a"],
		};
		const input = [{ a: 1, fn: () => {} }];

		const streams = [
			createReadableStream(input),
			validateStream({ schema, allowCoerceTypes: false }),
		];
		const result = await pipeline(streams);
		ok(result, "pipeline should complete without error");
	});

	test(`validateStream root-level error should not have trailing-slash id or empty-string key`, async (_t) => {
		const schema = { type: "object" };
		const input = ["not-an-object"];
		const streams = [createReadableStream(input), validateStream({ schema })];
		const result = await pipeline(streams);

		ok(Object.keys(result.validate).length > 0);
		for (const [id, entry] of Object.entries(result.validate)) {
			strictEqual(id.endsWith("/"), false, `id '${id}' must not end with '/'`);
			strictEqual(
				entry.keys.includes(""),
				false,
				`keys must not include empty string`,
			);
		}
	});

	test(`validateStream should emit a clone so caller input is not mutated`, async (_t) => {
		const original = { a: "1" };
		const input = [original];
		const schema = {
			type: "object",
			properties: { a: { type: "number" } },
			required: ["a"],
		};

		const streams = [createReadableStream(input), validateStream({ schema })];
		const output = await streamToArray(pipejoin(streams));

		strictEqual(output[0].a, 1);
		strictEqual(
			original.a,
			"1",
			"caller's original object must not be mutated in place",
		);
	});

	test(`validateStream should throw a branded error when schema is missing`, (_t) => {
		try {
			validateStream({});
			ok(false, "should have thrown");
		} catch (err) {
			ok(
				err.message.includes("validateStream") &&
					err.message.includes("schema"),
				`expected branded error mentioning validateStream and schema, got: ${err.message}`,
			);
		}
	});

	test(`exports no default`, async (_t) => {
		const mod = await import("@datastream/validate");
		strictEqual(mod.default, undefined);
	});

	const numberSchema = {
		type: "object",
		properties: { a: { type: "number" } },
	};
	const badRows = (length) =>
		Array.from({ length }, (_, i) => ({ a: `bad${i}` }));

	test(`validateStream maxErrorRows defaults to 1000 and null is unlimited`, async (_t) => {
		const capped = await pipeline([
			createReadableStream(badRows(1001)),
			validateStream({ schema: numberSchema }),
		]);
		const [cappedError] = Object.values(capped.validate);
		deepStrictEqual(
			[cappedError.idx.length, cappedError.idx.at(-1)],
			[1000, 999],
		);
		const unlimited = await pipeline([
			createReadableStream(badRows(1001)),
			validateStream({ schema: numberSchema, maxErrorRows: null }),
		]);
		strictEqual(Object.values(unlimited.validate)[0].idx.length, 1001);
	});

	test(`validateStream maxErrorKeys null is unlimited`, async (_t) => {
		const schema = {
			type: "object",
			additionalProperties: false,
			properties: { a: { type: "number" } },
		};
		const input = Array.from({ length: 1001 }, (_, i) => ({
			a: 1,
			[`extra_${i}`]: i,
		}));
		const result = await pipeline([
			createReadableStream(input),
			validateStream({ schema, maxErrorKeys: null }),
		]);
		strictEqual(Object.keys(result.validate).length, 1001);
	});

	test(`transpileSchema should use strict:true by default and reject schemas with unknown keywords`, (_t) => {
		let threw = false;
		try {
			transpileSchema({ type: "object", unknownKeyword: true });
		} catch {
			threw = true;
		}
		strictEqual(
			threw,
			true,
			"transpileSchema should throw on unknown keywords in strict mode",
		);
	});

	test(`transpileSchema should apply default values for empty strings (useDefaults:"empty")`, async (_t) => {
		const schema = {
			type: "object",
			properties: {
				name: { type: "string", default: "fallback" },
			},
		};
		const input = [{ name: "" }];
		const streams = [createReadableStream(input), validateStream({ schema })];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output[0].name, "fallback");
	});

	test(`validateStream allowCoerceTypes:false with non-cloneable chunk emits original chunk`, async (_t) => {
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
		strictEqual(
			output[0],
			original,
			"non-cloneable chunk should emit original reference when allowCoerceTypes:false",
		);
		strictEqual(output[0].a, "hello");
	});

	test(`validateStream default coerce path with non-cloneable chunk still emits chunk`, async (_t) => {
		const schema = {
			type: "object",
			properties: { a: { type: "string" } },
		};
		const original = { a: "hello", fn: () => {} };
		const input = [original];
		const streams = [createReadableStream(input), validateStream({ schema })];
		const output = await streamToArray(pipejoin(streams));
		strictEqual(output[0].a, "hello");
	});

	test(`validateStream should accumulate idx for repeated same-key errors`, async (_t) => {
		const input = [{ a: "bad" }, { a: "bad" }, { a: "bad" }];
		const schema = {
			type: "object",
			properties: { a: { type: "number" } },
		};
		const streams = [createReadableStream(input), validateStream({ schema })];
		const result = await pipeline(streams);
		const errorKey = Object.keys(result.validate)[0];
		deepStrictEqual(result.validate[errorKey].idx, [0, 1, 2]);
	});

	test(`validateStream should not enqueue invalid items when onErrorEnqueue is not set`, async (_t) => {
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
		deepStrictEqual(output.length, 0);
	});

	test(`validateStream errorMessage should collect keys from sub-errors`, async (_t) => {
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
		ok(
			errorEntry.keys.includes("age"),
			`keys should include 'age', got: ${JSON.stringify(errorEntry.keys)}`,
		);
		strictEqual(errorEntry.message, "Age must be a number");
	});

	test(`validateStream errorMessage should sort keys alphabetically (z_field before a_field in required → sorted as a_field|z_field)`, async (_t) => {
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
		const errorEntry = result.validate["#/errorMessage/a_field|z_field"];
		ok(
			errorEntry,
			"error entry id '#/errorMessage/a_field|z_field' must exist (keys sorted)",
		);
		deepStrictEqual(errorEntry.keys, ["a_field", "z_field"]);
		strictEqual(errorEntry.message, "Both fields are required");
	});

	test(`validateStream errorMessage sub-errors with empty instancePath produce no empty string keys`, async (_t) => {
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

	test(`validateStream error id should join multiple keys with pipe separator`, async (_t) => {
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

		const errorEntry = result.validate["#/errorMessage/a_field|b_field"];
		ok(
			errorEntry,
			"error entry id '#/errorMessage/a_field|b_field' must exist",
		);
		deepStrictEqual(errorEntry.keys, ["a_field", "b_field"]);
		strictEqual(errorEntry.message, "Fields a_field and b_field are required");
	});

	test(`transpileSchema should use the JSON Schema 2020-12 dialect`, (_t) => {
		// prefixItems is 2020-12 only; the draft-07 Ajv class rejects it in strict mode
		const validate = transpileSchema({
			type: "array",
			prefixItems: [{ type: "string" }],
			items: false,
			minItems: 1,
		});
		strictEqual(validate(["a"]), true);
		strictEqual(validate([{}]), false);
		strictEqual(validate.errors[0].keyword, "type");
		strictEqual(validate.errors[0].instancePath, "/0");
	});

	test(`transpileSchema should support ajv-formats formats`, (_t) => {
		const validate = transpileSchema({ type: "string", format: "email" });
		strictEqual(validate("a@example.com"), true);
		strictEqual(validate("not an email"), false);
		strictEqual(validate.errors[0].keyword, "format");
	});

	test(`transpileSchema should support draft 2019 formats`, (_t) => {
		// idn-email comes from @silverbucket/ajv-formats-draft2019, not ajv-formats
		const validate = transpileSchema({ type: "string", format: "idn-email" });
		strictEqual(validate("用户@例子.广告"), true);
		strictEqual(validate("not an email"), false);
		strictEqual(validate.errors[0].keyword, "format");
	});

	test(`transpileSchema should support ajv-keywords keywords`, (_t) => {
		const validate = transpileSchema({
			type: "object",
			properties: { a: { type: "string", transform: ["trim", "toLowerCase"] } },
		});
		const data = { a: "  ABC " };
		strictEqual(validate(data), true);
		deepStrictEqual(data, { a: "abc" });
	});

	test(`transpileSchema should skip ajv-errors when allErrors is false`, (_t) => {
		// ajv-errors throws at registration without allErrors
		const validate = transpileSchema(
			{
				type: "object",
				properties: { a: { type: "string" }, b: { type: "string" } },
				required: ["a", "b"],
			},
			{ allErrors: false },
		);
		strictEqual(validate({}), false);
		strictEqual(validate.errors.length, 1);
		let message;
		try {
			transpileSchema(
				{ type: "object", errorMessage: "nope" },
				{ allErrors: false },
			);
		} catch (e) {
			message = e.message;
		}
		strictEqual(message, 'strict mode: unknown keyword: "errorMessage"');
	});

	const numberA = { type: "object", properties: { a: { type: "number" } } };
	const validateOne = async (row, schema = numberA) =>
		(
			await streamToArray(
				pipejoin([createReadableStream([row]), validateStream({ schema })]),
			)
		)[0];

	test(`validateStream copies a function value by reference and never coerces the original`, async (_t) => {
		const f = () => {};
		const original = { a: "1", f };
		const output = await validateOne(original);
		strictEqual(output.a, 1);
		strictEqual(output.f, f);
		strictEqual(original.a, "1");
	});

	test(`validateStream clones a null-prototype row (node:sqlite rows) as a plain object`, async (_t) => {
		const f = () => {};
		const original = Object.assign(Object.create(null), { a: "1", f });
		const output = await validateOne(original);
		strictEqual(output.a, 1);
		strictEqual(output.f, f);
		strictEqual(Object.getPrototypeOf(output), Object.prototype);
		strictEqual(original.a, "1");
	});

	test(`validateStream clones a Date value through structuredClone`, async (_t) => {
		const original = { a: "1", d: new Date(0) };
		const output = await validateOne(original);
		ok(output.d instanceof Date);
		notStrictEqual(output.d, original.d);
		strictEqual(output.d.getTime(), 0);
	});

	test(`validateStream keeps cycles and shared references in the clone`, async (_t) => {
		const original = { a: "1", sub: { b: "2" } };
		original.self = original;
		original.again = original.sub;
		const output = await validateOne(original);
		strictEqual(output.a, 1);
		strictEqual(output.self, output);
		strictEqual(output.again, output.sub);
		notStrictEqual(output.sub, original.sub);
		strictEqual(original.a, "1");
	});

	test(`validateStream clones nested arrays`, async (_t) => {
		const original = { list: ["1", "2"] };
		const schema = {
			type: "object",
			properties: { list: { type: "array", items: { type: "number" } } },
		};
		const output = await validateOne(original, schema);
		ok(Array.isArray(output.list));
		deepStrictEqual(output.list, [1, 2]);
		strictEqual(original.list[0], "1");
	});

	test(`validateStream keeps an own "__proto__" key as a data property`, async (_t) => {
		const original = JSON.parse('{"__proto__":{"polluted":true},"a":"1"}');
		const output = await validateOne(original);
		strictEqual(output.a, 1);
		deepStrictEqual(Object.getOwnPropertyDescriptor(output, "__proto__"), {
			value: { polluted: true },
			writable: true,
			enumerable: true,
			configurable: true,
		});
		strictEqual(Object.getPrototypeOf(output), Object.prototype);
		strictEqual(output.polluted, undefined);
	});

	test(`validateStream clones a row holding a null value`, async (_t) => {
		const original = { a: "1", b: null };
		const output = await validateOne(original);
		strictEqual(output.a, 1);
		strictEqual(output.b, null);
		strictEqual(original.a, "1");
	});

	test(`validateStream validates the original in place when a value is not cloneable`, async (_t) => {
		// WeakMap has no structuredClone representation, so clone() throws and
		// the fallback validates (and coerces) the caller's object itself.
		const original = { a: "1", w: new WeakMap() };
		const output = await validateOne(original);
		strictEqual(output, original);
		strictEqual(original.a, 1);
	});

	test(`validateStream clones an empty array`, async (_t) => {
		const output = await validateOne({ a: "1", list: [] });
		deepStrictEqual(output.list, []);
	});

	test(`validateStream copies an array holding a function without the structuredClone fallback`, async (_t) => {
		const f = () => {};
		const original = { a: "1", list: [f] };
		const output = await validateOne(original);
		notStrictEqual(output, original);
		ok(Array.isArray(output.list));
		strictEqual(output.list[0], f);
		strictEqual(original.a, "1");
	});
});
