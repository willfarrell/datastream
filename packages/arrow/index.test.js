import { deepStrictEqual, ok, rejects, strictEqual, throws } from "node:assert";
import test, { describe } from "node:test";
import {
	arrowBatchFromArrayStream,
	arrowBatchFromObjectStream,
	arrowDetectSchemaStream,
	arrowToArrayStream,
	arrowToObjectStream,
} from "@datastream/arrow";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	Bool,
	Field,
	Float64,
	Int8,
	Int32,
	Int64,
	RecordBatch,
	Schema,
	TimestampMillisecond,
	Type,
	Uint8,
	Uint32,
	Utf8,
} from "apache-arrow";
import { variant } from "../variant.js";

describe(`@datastream/arrow (${variant})`, () => {
	const usersSchema = new Schema([
		new Field("id", new Int32(), true),
		new Field("name", new Utf8(), true),
	]);

	test(`arrowDetectSchemaStream should infer schema from object rows`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([
				{ id: 1, name: "alice" },
				{ id: 2, name: "bob" },
				{ id: 3, name: "carol" },
			]),
			detect,
		]);
		const output = await streamToArray(stream);

		strictEqual(output.length, 3);
		const { value } = detect.result();
		ok(value.schema);
		deepStrictEqual(value.fields, ["id", "name"]);
		strictEqual(value.schema.fields.length, 2);
		strictEqual(value.schema.fields[0].name, "id");
		strictEqual(value.schema.fields[1].name, "name");
	});

	test(`arrowDetectSchemaStream should infer column names for array rows`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([
				[1, "alice"],
				[2, "bob"],
			]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		deepStrictEqual(value.fields, ["column0", "column1"]);
	});

	test(`arrowDetectSchemaStream widens out-of-range integers to Float64 (no Int32 overflow)`, async () => {
		const big = 3_000_000_000;
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ id: big, small: 5 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Float64, true);
		strictEqual(value.schema.fields[1].type instanceof Int32, true);
	});

	test(`detect -> batch -> rows preserves a large integer exactly (no Int32 wraparound)`, async () => {
		const input = [{ id: 3_000_000_000, name: "alice" }];
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream(input),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
				batchSize: 10,
			}),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		strictEqual(rows[0].id, 3_000_000_000);
		strictEqual(rows[0].name, "alice");
	});

	test(`arrowDetectSchemaStream should seal on flush when fewer rows than sampleSize`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 100 });
		const stream = pipejoin([
			createReadableStream([{ id: 1, name: "alice" }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		ok(value.schema);
		deepStrictEqual(value.fields, ["id", "name"]);
	});

	test(`arrowDetectSchemaStream seals mid-stream at sampleSize, ignoring wider later rows`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([[1], [2], [3, 4, 5]]),
			detect,
		]);
		const rows = await streamToArray(stream);
		const { value } = detect.result();
		deepStrictEqual(value.fields, ["column0"]);
		strictEqual(value.schema.fields.length, 1);
		strictEqual(rows.length, 3);
		deepStrictEqual(rows[2], [3, 4, 5]);
	});

	test(`arrowBatchFromArrayStream should emit a RecordBatch per batchSize rows`, async () => {
		const stream = pipejoin([
			createReadableStream([
				[1, "a"],
				[2, "b"],
				[3, "c"],
			]),
			arrowBatchFromArrayStream({ schema: usersSchema, batchSize: 2 }),
		]);
		const batches = await streamToArray(stream);

		strictEqual(batches.length, 2);
		ok(batches[0] instanceof RecordBatch);
		strictEqual(batches[0].numRows, 2);
		strictEqual(batches[1].numRows, 1);
		strictEqual(batches[0].getChildAt(0).get(0), 1);
		strictEqual(batches[0].getChildAt(1).get(1), "b");
	});

	test(`arrowBatchFromArrayStream should resolve a lazy schema`, async () => {
		const stream = pipejoin([
			createReadableStream([[1, "a"]]),
			arrowBatchFromArrayStream({
				schema: () => usersSchema,
				batchSize: 10,
			}),
		]);
		const batches = await streamToArray(stream);
		strictEqual(batches.length, 1);
		strictEqual(batches[0].numRows, 1);
	});

	test(`arrowBatchFromObjectStream should batch object rows`, async () => {
		const stream = pipejoin([
			createReadableStream([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
			]),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 10 }),
		]);
		const batches = await streamToArray(stream);
		strictEqual(batches.length, 1);
		strictEqual(batches[0].numRows, 2);
		strictEqual(batches[0].getChildAt(0).get(1), 2);
		strictEqual(batches[0].getChildAt(1).get(0), "a");
	});

	test(`arrowToArrayStream should emit one array row per record-batch row`, async () => {
		const buildStream = pipejoin([
			createReadableStream([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
			]),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 10 }),
			arrowToArrayStream(),
		]);
		const rows = await streamToArray(buildStream);
		deepStrictEqual(rows, [
			[1, "a"],
			[2, "b"],
		]);
	});

	test(`round-trip rows -> arrow -> rows preserves data`, async () => {
		const input = [
			{ id: 1, name: "alice" },
			{ id: 2, name: "bob" },
			{ id: 3, name: "carol" },
		];
		const stream = pipejoin([
			createReadableStream(input),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 2 }),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		deepStrictEqual(rows, input);
	});

	test(`detect + batchFromObject end-to-end with lazy schema`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
				{ id: 3, name: "c" },
			]),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
				batchSize: 10,
			}),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		deepStrictEqual(rows, [
			{ id: 1, name: "a" },
			{ id: 2, name: "b" },
			{ id: 3, name: "c" },
		]);
	});

	test(`detect + batchFromObject writes "" as null in non-Utf8 columns`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 3 });
		const stream = pipejoin([
			createReadableStream([
				{ a: 1, b: true, d: new Date(0), s: "x" },
				{ a: "", b: "", d: "", s: "" },
				{ a: 3, b: null, d: null, s: "z" },
			]),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
			}),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		deepStrictEqual(rows[1], { a: null, b: null, d: null, s: "" });
	});

	test(`arrowDetectSchemaStream unions object keys across all sampled rows`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ id: 1 }, { id: 2, name: "bob" }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		deepStrictEqual(value.fields, ["id", "name"]);
		strictEqual(value.schema.fields.length, 2);
		strictEqual(value.schema.fields[1].name, "name");
		strictEqual(value.schema.fields[1].type instanceof Utf8, true);
	});

	test(`arrowDetectSchemaStream uses max array width across sampled rows`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([[1], [2, "bob", true]]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		deepStrictEqual(value.fields, ["column0", "column1", "column2"]);
		strictEqual(value.schema.fields.length, 3);
	});

	test(`detect -> batch -> rows preserves a column present only in a later row`, async () => {
		const input = [{ id: 1 }, { id: 2, name: "bob" }];
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream(input),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
				batchSize: 10,
			}),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		strictEqual(rows.length, 2);
		strictEqual(rows[1].name, "bob");
	});

	test(`detect -> batch -> rows round-trips Date values as Date`, async () => {
		const when = new Date("2020-06-01T00:00:00.000Z");
		const input = [{ id: 1, at: when }];
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream(input),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
				batchSize: 10,
			}),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		ok(rows[0].at instanceof Date, "at should come back as a Date");
		strictEqual(rows[0].at.getTime(), when.getTime());
	});

	test(`arrowToArrayStream round-trips Date values as Date`, async () => {
		const when = new Date("2020-06-01T00:00:00.000Z");
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ at: when }]),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
				batchSize: 10,
			}),
			arrowToArrayStream(),
		]);
		const rows = await streamToArray(stream);
		ok(rows[0][0] instanceof Date, "at should come back as a Date");
		strictEqual(rows[0][0].getTime(), when.getTime());
	});

	test(`arrowBatchFromObjectStream rejects a zero-field schema instead of losing rows`, () => {
		throws(
			() =>
				arrowBatchFromObjectStream({ schema: new Schema([]), batchSize: 10 }),
			/at least one field/,
		);
	});

	test(`arrowBatchFromArrayStream rejects a zero-field schema instead of losing rows`, () => {
		throws(
			() =>
				arrowBatchFromArrayStream({ schema: new Schema([]), batchSize: 10 }),
			/at least one field/,
		);
	});

	test(`arrowBatchFromObjectStream throws eagerly when schema is missing`, () => {
		throws(() => arrowBatchFromObjectStream({}), /schema is required/);
		throws(() => arrowBatchFromObjectStream(), /schema is required/);
	});

	test(`arrowBatchFromArrayStream throws eagerly when schema is missing`, () => {
		throws(() => arrowBatchFromArrayStream({}), /schema is required/);
		throws(() => arrowBatchFromArrayStream(), /schema is required/);
	});

	const errorFromEmptyLazy = async (build) => {
		try {
			await pipeline([createReadableStream([]), build()]);
		} catch (e) {
			return e;
		}
		throw new Error("expected an error, got end");
	};

	test(`arrowBatchFromObjectStream errors for a lazy missing schema even on empty input`, async () => {
		const error = await errorFromEmptyLazy(() =>
			arrowBatchFromObjectStream({ schema: () => undefined }),
		);
		ok(/schema is required/.test(error.message));
	});

	test(`arrowBatchFromArrayStream errors for a lazy missing schema even on empty input`, async () => {
		const error = await errorFromEmptyLazy(() =>
			arrowBatchFromArrayStream({ schema: () => undefined }),
		);
		ok(/schema is required/.test(error.message));
	});

	test(`inferType treats null as no-type (first non-null row wins)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ x: null }, { x: 42 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
	});

	test(`inferType treats undefined as no-type (first non-null row wins)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ x: undefined }, { x: 7 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
	});

	test(`inferType treats empty string as no-type (first non-empty row wins)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ x: "" }, { x: "hello" }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Utf8, true);
	});

	test(`inferType returns Bool typeId for boolean true`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([createReadableStream([{ flag: true }]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type.typeId, Type.Bool);
	});

	test(`inferType returns Bool typeId for boolean false`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ flag: false }, { flag: true }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type.typeId, Type.Bool);
	});

	test(`inferType returns Float64 for non-integer number`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([createReadableStream([{ x: 3.14 }]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Float64, true);
	});

	test(`inferType returns Int32 for integer at INT32_MAX (2147483647)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ x: 2147483647 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
	});

	test(`inferType returns Float64 for integer one above INT32_MAX (2147483648)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ x: 2147483648 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Float64, true);
	});

	test(`inferType returns Int32 for integer at INT32_MIN (-2147483648)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ x: -2147483648 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
	});

	test(`inferType returns Float64 for integer one below INT32_MIN (-2147483649)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ x: -2147483649 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Float64, true);
	});

	test(`inferType returns TimestampMillisecond typeId for Date values`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ ts: new Date() }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type.typeId, Type.Timestamp);
	});

	test(`fieldsFromSamples skips null rows and uses first non-null type`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ x: null }, { x: null }, { x: 99 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
		strictEqual(value.schema.fields[0].nullable, true);
	});

	test(`fieldsFromSamples falls back to Utf8 when all sampled values are null`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ x: null }, { x: null }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Utf8, true);
		strictEqual(value.schema.fields[0].nullable, true);
	});

	test(`arrowDetectSchemaStream result value has schema and fields properties`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([createReadableStream([{ id: 1 }]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		ok("schema" in value, "value must have schema key");
		ok("fields" in value, "value must have fields key");
	});

	test(`arrowDetectSchemaStream leaves schema null for empty stream`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([createReadableStream([]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema, null);
	});

	test(`arrowDetectSchemaStream sealed flag prevents re-sealing`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		ok(value.schema !== null);
		strictEqual(value.schema.fields.length, 1);
	});

	test(`arrowDetectSchemaStream seals at exactly sampleSize rows`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([{ id: 1 }, { id: 2 }]),
			detect,
		]);
		const rows = await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(rows.length, 2);
		ok(value.schema !== null);
	});

	test(`arrowDetectSchemaStream emits all rows when sampleSize is hit mid-stream`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
			detect,
		]);
		const rows = await streamToArray(stream);
		strictEqual(rows.length, 4);
	});

	test(`arrowDetectSchemaStream flush path emits the buffered row`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 100 });
		const stream = pipejoin([createReadableStream([{ id: 42 }]), detect]);
		const rows = await streamToArray(stream);
		strictEqual(rows.length, 1);
		strictEqual(rows[0].id, 42);
	});

	test(`arrowDetectSchemaStream result key defaults to "arrowDetectSchema"`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([createReadableStream([{ id: 1 }]), detect]);
		await streamToArray(stream);
		strictEqual(detect.result().key, "arrowDetectSchema");
	});

	test(`arrowDetectSchemaStream result key uses provided resultKey`, async () => {
		const detect = arrowDetectSchemaStream({
			sampleSize: 1,
			resultKey: "myKey",
		});
		const stream = pipejoin([createReadableStream([{ id: 1 }]), detect]);
		await streamToArray(stream);
		strictEqual(detect.result().key, "myKey");
	});

	test(`null field values survive object stream round-trip as null`, async () => {
		const stream = pipejoin([
			createReadableStream([
				{ id: 1, name: "alice" },
				{ id: 2, name: null },
			]),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 10 }),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		strictEqual(rows[1].name, null);
	});

	test(`undefined field values survive array stream round-trip as null`, async () => {
		const stream = pipejoin([
			createReadableStream([
				[1, "alice"],
				[2, undefined],
			]),
			arrowBatchFromArrayStream({ schema: usersSchema, batchSize: 10 }),
			arrowToArrayStream(),
		]);
		const rows = await streamToArray(stream);
		strictEqual(rows[1][1], null);
	});

	test(`arrowBatchFromArrayStream error message includes function name`, () => {
		throws(
			() => arrowBatchFromArrayStream({}),
			(err) => {
				ok(
					/arrowBatchFromArrayStream/.test(err.message),
					`got: ${err.message}`,
				);
				return true;
			},
		);
	});

	test(`arrowBatchFromObjectStream error message includes function name`, () => {
		throws(
			() => arrowBatchFromObjectStream({}),
			(err) => {
				ok(
					/arrowBatchFromObjectStream/.test(err.message),
					`got: ${err.message}`,
				);
				return true;
			},
		);
	});

	test(`arrowBatchFromArrayStream flush emits no extra batch when rowCount is 0`, async () => {
		const stream = pipejoin([
			createReadableStream([
				[1, "a"],
				[2, "b"],
			]),
			arrowBatchFromArrayStream({ schema: usersSchema, batchSize: 2 }),
		]);
		const batches = await streamToArray(stream);
		strictEqual(batches.length, 1);
		strictEqual(batches[0].numRows, 2);
	});

	test(`arrowBatchFromObjectStream emits batch at exactly batchSize rows`, async () => {
		const stream = pipejoin([
			createReadableStream([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
				{ id: 3, name: "c" },
			]),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 2 }),
		]);
		const batches = await streamToArray(stream);
		strictEqual(batches.length, 2);
		strictEqual(batches[0].numRows, 2);
		strictEqual(batches[1].numRows, 1);
	});

	test(`arrowBatchFromObjectStream flush emits no extra batch when rowCount is 0`, async () => {
		const stream = pipejoin([
			createReadableStream([
				{ id: 1, name: "a" },
				{ id: 2, name: "b" },
			]),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 2 }),
		]);
		const batches = await streamToArray(stream);
		strictEqual(batches.length, 1);
		strictEqual(batches[0].numRows, 2);
	});

	test(`readCell does not wrap null timestamp cell in Date`, async () => {
		const when = new Date("2021-01-01T00:00:00.000Z");
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ ts: when }, { ts: null }]),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
				batchSize: 10,
			}),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		ok(rows[0].ts instanceof Date);
		strictEqual(rows[1].ts, null);
	});

	test(`arrowToArrayStream row length equals schema column count`, async () => {
		const stream = pipejoin([
			createReadableStream([{ id: 1, name: "alice" }]),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 10 }),
			arrowToArrayStream(),
		]);
		const rows = await streamToArray(stream);
		strictEqual(rows[0].length, 2);
	});

	test(`exports only the five named stream factories`, async () => {
		const mod = await import("@datastream/arrow");
		deepStrictEqual(Object.keys(mod).sort(), [
			"arrowBatchFromArrayStream",
			"arrowBatchFromObjectStream",
			"arrowDetectSchemaStream",
			"arrowToArrayStream",
			"arrowToObjectStream",
		]);
	});
	test(`fieldsFromSamples widens mixed types across all samples`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([
				{
					intStr: 42,
					strInt: "hello",
					floatInt: 2.7,
					boolInt: true,
					intInt: 1,
				},
				{ intStr: "hello", strInt: 42, floatInt: 1, boolInt: 1, intInt: null },
				{
					intStr: null,
					strInt: null,
					floatInt: null,
					boolInt: null,
					intInt: 2,
				},
			]),
			detect,
		]);
		await streamToArray(stream);
		const types = detect
			.result()
			.value.schema.fields.map((field) => field.type.toString());
		deepStrictEqual(types, ["Utf8", "Utf8", "Float64", "Utf8", "Int32"]);
	});

	test(`arrowDetectSchemaStream uses max width across descending-width array rows`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([createReadableStream([[1, 2, 3], [4]]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		deepStrictEqual(value.fields, ["column0", "column1", "column2"]);
		strictEqual(value.schema.fields.length, 3);
	});

	test(`arrowBatchFromArrayStream lazy schema error includes function name`, async () => {
		const err = await errorFromEmptyLazy(() =>
			arrowBatchFromArrayStream({ schema: () => undefined }),
		);
		ok(/arrowBatchFromArrayStream/.test(err.message), `got: ${err.message}`);
	});

	test(`arrowBatchFromObjectStream lazy schema error includes function name`, async () => {
		const err = await errorFromEmptyLazy(() =>
			arrowBatchFromObjectStream({ schema: () => undefined }),
		);
		ok(/arrowBatchFromObjectStream/.test(err.message), `got: ${err.message}`);
	});
	test(`inferType skips empty string before a number (type from number row wins)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ x: "" }, { x: 42 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
	});
	test(`arrowDetectSchemaStream schema is frozen after first sampleSize rows`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([{ x: 1 }, { x: 2 }, { x: 3.14 }, { x: 2.72 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
	});

	test(`arrowBatchFrom*Stream do not resolve a lazy schema at construction`, () => {
		let calls = 0;
		const schema = () => {
			calls++;
			return usersSchema;
		};
		arrowBatchFromArrayStream({ schema });
		arrowBatchFromObjectStream({ schema });
		strictEqual(calls, 0);
	});

	test(`arrowDetectSchemaStream widens mixed int/float samples to Float64 and preserves 2.7`, async () => {
		const input = [{ a: 1 }, { a: 2.7 }];
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream(input),
			detect,
			arrowBatchFromObjectStream({
				schema: () => detect.result().value.schema,
				batchSize: 10,
			}),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		strictEqual(
			detect.result().value.schema.fields[0].type instanceof Float64,
			true,
		);
		deepStrictEqual(rows, [{ a: 1 }, { a: 2.7 }]);
	});

	test(`arrowToObjectStream keeps a "__proto__" column as an own property`, async () => {
		const schema = new Schema([
			new Field("__proto__", new Utf8(), true),
			new Field("x", new Int32(), true),
		]);
		const stream = pipejoin([
			createReadableStream([["polluted", 2]]),
			arrowBatchFromArrayStream({ schema, batchSize: 10 }),
			arrowToObjectStream(),
		]);
		const rows = await streamToArray(stream);
		deepStrictEqual(rows, [JSON.parse('{"__proto__":"polluted","x":2}')]);
		deepStrictEqual(Object.getOwnPropertyDescriptor(rows[0], "x"), {
			value: 2,
			writable: true,
			enumerable: true,
			configurable: true,
		});
	});
	const mixedCells = [new Date(0), "x", { b: 1 }, null];
	const mixedStrings = ["1970-01-01T00:00:00.000Z", "x", '{"b":1}', null];
	const mixedSchema = new Schema([new Field("a", new Utf8(), true)]);

	test(`arrowBatchFromObjectStream stringifies Date and object cells in Utf8 columns`, async () => {
		const rows = await streamToArray(
			pipejoin([
				createReadableStream(mixedCells.map((a) => ({ a }))),
				arrowBatchFromObjectStream({ schema: mixedSchema }),
				arrowToObjectStream(),
			]),
		);
		deepStrictEqual(
			rows.map((row) => row.a),
			mixedStrings,
		);
	});

	test(`arrowBatchFromArrayStream stringifies Date and object cells in Utf8 columns`, async () => {
		const rows = await streamToArray(
			pipejoin([
				createReadableStream(mixedCells.map((a) => [a])),
				arrowBatchFromArrayStream({ schema: mixedSchema }),
				arrowToArrayStream(),
			]),
		);
		deepStrictEqual(
			rows,
			mixedStrings.map((a) => [a]),
		);
	});
	test(`detect + batchFromObject rejects a later row that overflows the sampled Int32`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		await rejects(
			pipeline([
				createReadableStream([{ id: 1 }, { id: 2 }, { id: 3_000_000_000 }]),
				detect,
				arrowBatchFromObjectStream({
					schema: () => detect.result().value.schema,
				}),
			]),
			{
				name: "RangeError",
				message:
					'arrowBatchFromObjectStream: row 2, field "id" expects Int32, got 3000000000',
			},
		);
	});

	test(`arrowBatchFromArrayStream names the stream, row and column of a bad value`, async () => {
		await rejects(
			pipeline([
				createReadableStream([[1], [2], [2.7]]),
				arrowBatchFromArrayStream({
					schema: new Schema([new Field("column0", new Int32(), true)]),
				}),
			]),
			{
				name: "TypeError",
				message:
					'arrowBatchFromArrayStream: row 2, field "column0" expects Int32, got 2.7',
			},
		);
	});

	const batchOne = (type, value, nullable = true) =>
		streamToArray(
			pipejoin([
				createReadableStream([{ v: value }]),
				arrowBatchFromObjectStream({
					schema: new Schema([new Field("v", type, nullable)]),
				}),
				arrowToObjectStream(),
			]),
		).then((rows) => rows[0].v);
	const prefix = 'arrowBatchFromObjectStream: row 0, field "v" expects';

	test(`arrowBatchFromObjectStream accepts values that fit their column type`, async () => {
		const date = new Date(1_000);
		const cases = [
			[new Int8(), 127, 127],
			[new Int8(), -128, -128],
			[new Uint8(), 0, 0],
			[new Uint8(), 255, 255],
			[new Int32(), 2_147_483_647, 2_147_483_647],
			[new Int32(), -2_147_483_648, -2_147_483_648],
			[new Uint32(), 3_000_000_000, 3_000_000_000],
			[new Int64(), 5n, 5n],
			[new Float64(), 2.7, 2.7],
			[new Bool(), false, false],
			[new TimestampMillisecond(), date, date],
			[new TimestampMillisecond(), 2_000, new Date(2_000)],
			[new Utf8(), 5, "5"],
		];
		for (const [type, value, expected] of cases) {
			deepStrictEqual(
				await batchOne(type, value),
				expected,
				`${type} ${value}`,
			);
		}
	});

	test(`arrowBatchFromObjectStream rejects values that do not fit their column type`, async () => {
		const cases = [
			[new Int8(), 128, "RangeError", "Int8, got 128"],
			[new Int8(), -129, "RangeError", "Int8, got -129"],
			[new Uint8(), 256, "RangeError", "Uint8, got 256"],
			[new Uint8(), -1, "RangeError", "Uint8, got -1"],
			[new Int32(), 2_147_483_648, "RangeError", "Int32, got 2147483648"],
			[new Int32(), -2_147_483_649, "RangeError", "Int32, got -2147483649"],
			[new Int32(), "oops", "TypeError", 'Int32, got "oops"'],
			[new Int32(), true, "TypeError", "Int32, got true"],
			[new Int32(), {}, "TypeError", "Int32, got [object Object]"],
			[new Float64(), "1", "TypeError", 'Float64, got "1"'],
			[new Bool(), 1, "TypeError", "Bool, got 1"],
			[
				new TimestampMillisecond(),
				new Date(Number.NaN),
				"TypeError",
				"Timestamp<MILLISECOND>, got [object Date]",
			],
			[
				new TimestampMillisecond(),
				1.5,
				"TypeError",
				"Timestamp<MILLISECOND>, got 1.5",
			],
		];
		for (const [type, value, name, message] of cases) {
			await rejects(batchOne(type, value), {
				name,
				message: `${prefix} ${message}`,
			});
		}
	});

	test(`arrowBatchFromObjectStream allows nulls only in nullable fields`, async () => {
		for (const value of [null, undefined, ""]) {
			strictEqual(await batchOne(new Int32(), value), null);
		}
		strictEqual(await batchOne(new Utf8(), ""), "");
		strictEqual(await batchOne(new Utf8(), "", false), "");
		strictEqual(await batchOne(new Int32(), 1, false), 1);
		const cases = [
			[new Int32(), null, "non-null Int32, got null"],
			[new Int32(), undefined, "non-null Int32, got undefined"],
			[new Int32(), "", 'non-null Int32, got ""'],
			[new Utf8(), null, "non-null Utf8, got null"],
		];
		for (const [type, value, message] of cases) {
			await rejects(batchOne(type, value, false), {
				name: "TypeError",
				message: `${prefix} ${message}`,
			});
		}
	});
});
