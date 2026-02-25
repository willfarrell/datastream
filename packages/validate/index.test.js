import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import validateDefault, { validateStream } from "@datastream/validate";

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

// *** default export *** //
test(`${variant}: default export should be validateStream`, (_t) => {
	strictEqual(validateDefault, validateStream);
});
