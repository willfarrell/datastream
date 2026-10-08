import { deepStrictEqual, ok, strictEqual, throws } from "node:assert";
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
import { Field, Float64, Int32, RecordBatch, Schema, Utf8 } from "apache-arrow";
import { variant } from "../variant.js";

describe(`@datastream/arrow (${variant})`, () => {
	const usersSchema = new Schema([
		new Field("id", new Int32(), true),
		new Field("name", new Utf8(), true),
	]);

	// *** arrowDetectSchemaStream *** //
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
		const big = 3_000_000_000; // > 2^31, overflows Int32
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ id: big, small: 5 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		// Out-of-range integer must not be inferred as Int32 (which silently wraps).
		strictEqual(value.schema.fields[0].type instanceof Float64, true);
		// In-range integer stays Int32.
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

	// The schema must be sealed mid-stream exactly when `samples.length` REACHES
	// sampleSize, using only the rows seen so far. With sampleSize 2 the first two
	// 1-element array rows seal a single-column schema; the wider later row
	// [3, 4, 5] arrives after sealing and must NOT widen the schema. A mutant that
	// defers sealing (>=  ->  >, the guard removed, or `if (false)`) would instead
	// sample the wider row and infer three columns.
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
		// All input rows still flow through unchanged (the wider row is passed as-is).
		strictEqual(rows.length, 3);
		deepStrictEqual(rows[2], [3, 4, 5]);
	});

	// *** arrowBatchFromArrayStream *** //
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

	// *** arrowBatchFromObjectStream *** //
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

	// *** arrowToArrayStream / arrowToObjectStream *** //
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

	// *** lazy schema wired to detect *** //
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
		// inferType skips "" as null, so an empty cell in an Int32/Bool/Timestamp
		// column must not become 0/false/epoch. Utf8 keeps "" as a real value.
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

	// *** heterogeneous keys: schema detection must union across all sampled rows *** //
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
		// 'name' should be a Utf8 column inferred from the second row.
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

	// *** Date round-trip *** //
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

	// *** empty / zero-field schema must error rather than silently lose rows *** //
	// apache-arrow derives a RecordBatch's length from its child columns, so a
	// zero-field schema can never carry rows; rejecting it prevents corrupt
	// numRows:0 batches that silently discard data.
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

	// *** missing-schema validation, including empty input *** //
	// A concrete missing schema is rejected eagerly at construction.
	test(`arrowBatchFromObjectStream throws eagerly when schema is missing`, () => {
		throws(() => arrowBatchFromObjectStream({}), /schema is required/);
		throws(() => arrowBatchFromObjectStream(), /schema is required/);
	});

	test(`arrowBatchFromArrayStream throws eagerly when schema is missing`, () => {
		throws(() => arrowBatchFromArrayStream({}), /schema is required/);
		throws(() => arrowBatchFromArrayStream(), /schema is required/);
	});

	// A lazy schema that resolves to nothing must still error even for an empty
	// stream: init runs on flush, so the config bug is surfaced (as a stream error)
	// rather than silently swallowed.
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

	// *** inferType: null/undefined/empty-string branch *** //
	// Kills: LogicalOperator (|| -> &&), ConditionalExpression short-circuits,
	//        StringLiteral ("" -> "Stryker was here!")
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

	// *** inferType: boolean branch *** //
	// Kills: ConditionalExpression (false), StringLiteral ("" for "boolean")
	test(`inferType returns Bool typeId for boolean true`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([createReadableStream([{ flag: true }]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type.typeId, 6); // Bool typeId
	});

	test(`inferType returns Bool typeId for boolean false`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ flag: false }, { flag: true }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type.typeId, 6); // Bool typeId
	});

	// *** inferType: number/Float64/Int32 boundary checks *** //
	// Kills: ConditionalExpression (!isInteger false), EqualityOperator (>= vs >),
	//        lower-bound ConditionalExpression and EqualityOperator.
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

	// *** inferType: Date branch *** //
	// Kills: ConditionalExpression (false for instanceof Date)
	test(`inferType returns TimestampMillisecond typeId for Date values`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 1 });
		const stream = pipejoin([
			createReadableStream([{ ts: new Date() }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema.fields[0].type.typeId, 10); // Timestamp typeId
	});

	// *** fieldsFromSamples: type !== null break + nullable Field *** //
	// Kills: ConditionalExpression (true/false on break), EqualityOperator (=== null),
	//        BooleanLiteral (false for nullable).
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

	// *** fieldsFromSamples: type ?? new Utf8() fallback when ALL sampled values are null *** //
	// Kills: branch where every sampled value for a field is null/undefined/empty-string,
	//        so type stays null and the ?? fallback produces a Utf8 Field.
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

	// *** arrowDetectSchemaStream: value object must have schema + fields keys *** //
	// Kills: ObjectLiteral ({} for {schema:null, fields:null})
	test(`arrowDetectSchemaStream result value has schema and fields properties`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([createReadableStream([{ id: 1 }]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		ok("schema" in value, "value must have schema key");
		ok("fields" in value, "value must have fields key");
	});

	// *** seal() guarding: empty stream leaves schema null *** //
	// Kills: ConditionalExpression (false for sealed || !samples.length),
	//        LogicalOperator (|| -> &&).
	test(`arrowDetectSchemaStream leaves schema null for empty stream`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([createReadableStream([]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		strictEqual(value.schema, null);
	});

	// *** seal(): sealed = true flag *** //
	// Kills: BooleanLiteral (false for sealed = true)
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

	// *** transform: samples.length >= sampleSize threshold *** //
	// Kills: ConditionalExpression (false), EqualityOperator (> vs >=).
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

	// *** transform: BlockStatement kill (buffered rows emitted after seal) *** //
	// Kills: BlockStatement (empty block when sampleSize reached).
	test(`arrowDetectSchemaStream emits all rows when sampleSize is hit mid-stream`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
			detect,
		]);
		const rows = await streamToArray(stream);
		strictEqual(rows.length, 4);
	});

	// *** flush(): !sealed branch *** //
	// Kills: ConditionalExpression (true for !sealed), BlockStatement (empty).
	test(`arrowDetectSchemaStream flush path emits the buffered row`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 100 });
		const stream = pipejoin([createReadableStream([{ id: 42 }]), detect]);
		const rows = await streamToArray(stream);
		strictEqual(rows.length, 1);
		strictEqual(rows[0].id, 42);
	});

	// *** stream.result: ?? vs && and default key string *** //
	// Kills: LogicalOperator (?? -> &&), StringLiteral ("" for "arrowDetectSchema").
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

	// *** makeBuilders: nullValues must include null and undefined *** //
	// Kills: ArrayDeclaration ([] for [null, undefined]).
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

	// *** assertSchema error messages include function name *** //
	// Kills: StringLiteral ("" for "arrowBatchFromArrayStream" / "arrowBatchFromObjectStream").
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

	// *** arrowBatchFromArrayStream flush: rowCount > 0 guards *** //
	// Kills: ConditionalExpression (true), EqualityOperator (>= 0 vs > 0).
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

	// *** arrowBatchFromObjectStream transform: rowCount >= batchSize + BlockStatement *** //
	// Kills: EqualityOperator (> vs >=), ConditionalExpression (false), BlockStatement.
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

	// *** arrowBatchFromObjectStream flush: rowCount > 0 guards *** //
	// Kills: ConditionalExpression (true), EqualityOperator (>= 0 vs > 0).
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

	// *** readCell: isTimestamp && typeof value === "number" *** //
	// Kills: ConditionalExpression (isTimestamp && true).
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

	// *** arrowToArrayStream: new Array(colCount) pre-allocates with colCount *** //
	// Kills: ArrayDeclaration (new Array() instead of new Array(colCount)).
	test(`arrowToArrayStream row length equals schema column count`, async () => {
		const stream = pipejoin([
			createReadableStream([{ id: 1, name: "alice" }]),
			arrowBatchFromObjectStream({ schema: usersSchema, batchSize: 10 }),
			arrowToArrayStream(),
		]);
		const rows = await streamToArray(stream);
		strictEqual(rows[0].length, 2);
	});

	// *** named exports only (no default export) *** //
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
	// *** fieldsFromSamples: widening across all samples (order-independent) *** //
	// Previously asserted first-non-null-wins (42 then "hello" -> Int32), which
	// made "hello" unrepresentable. Each column pins one widening rule.
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

	// *** fieldsFromSamples: max array width uses > not >= or always-true *** //
	// Kills: ConditionalExpression (if (true)) - always updates width to last sample's length.
	test(`arrowDetectSchemaStream uses max width across descending-width array rows`, async () => {
		// First row has 3 cols, second has 1. Real: width=3 (never shrinks).
		// Mutant (if true): width=1 (last row wins), giving only column0.
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([createReadableStream([[1, 2, 3], [4]]), detect]);
		await streamToArray(stream);
		const { value } = detect.result();
		deepStrictEqual(value.fields, ["column0", "column1", "column2"]);
		strictEqual(value.schema.fields.length, 3);
	});

	// *** init() in arrowBatchFromArrayStream: error message from lazy schema must include function name *** //
	// Kills: StringLiteral ( for arrowBatchFromArrayStream in init()).
	test(`arrowBatchFromArrayStream lazy schema error includes function name`, async () => {
		const err = await errorFromEmptyLazy(() =>
			arrowBatchFromArrayStream({ schema: () => undefined }),
		);
		ok(/arrowBatchFromArrayStream/.test(err.message), `got: ${err.message}`);
	});

	// *** init() in arrowBatchFromObjectStream: error message from lazy schema must include function name *** //
	// Kills: StringLiteral ( for arrowBatchFromObjectStream in init()).
	test(`arrowBatchFromObjectStream lazy schema error includes function name`, async () => {
		const err = await errorFromEmptyLazy(() =>
			arrowBatchFromObjectStream({ schema: () => undefined }),
		);
		ok(/arrowBatchFromObjectStream/.test(err.message), `got: ${err.message}`);
	});
	// *** Kill empty-string mutants by testing against a non-string next value *** //
	// Kills: ConditionalExpression (false for value === ""),
	//        StringLiteral ("Stryker was here!" for "").
	// If empty-string is NOT skipped, it types as Utf8 and breaks. Next value (42) is
	// never seen. Result would be Utf8 instead of Int32.
	test(`inferType skips empty string before a number (type from number row wins)`, async () => {
		const detect = arrowDetectSchemaStream({ sampleSize: 10 });
		const stream = pipejoin([
			createReadableStream([{ x: "" }, { x: 42 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		// Real: "" skipped, 42 wins → Int32.
		// Mutant: "" not skipped, Utf8 typed, breaks → Utf8 (wrong).
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
	});
	// *** Kill sealed = false mutant: schema must not be re-detected from later rows *** //
	// Kills: BooleanLiteral (false for sealed = true in seal()).
	// With sealed=false: seal() re-runs for every subsequent sampleSize rows, overwriting
	// the schema with types from the new batch. With sealed=true: schema is frozen after
	// the first sampleSize rows.
	test(`arrowDetectSchemaStream schema is frozen after first sampleSize rows`, async () => {
		// sampleSize=2: rows 1-2 produce Int32 schema. Rows 3-4 produce Float64 schema.
		// Real (sealed=true): schema stays Int32.
		// Mutant (sealed=false): seal() re-runs on rows 3-4, overwriting to Float64.
		const detect = arrowDetectSchemaStream({ sampleSize: 2 });
		const stream = pipejoin([
			createReadableStream([{ x: 1 }, { x: 2 }, { x: 3.14 }, { x: 2.72 }]),
			detect,
		]);
		await streamToArray(stream);
		const { value } = detect.result();
		// Schema must reflect the FIRST two rows (Int32), not the last two (Float64).
		strictEqual(value.schema.fields[0].type instanceof Int32, true);
	});

	// A lazy schema is resolved on first row, never at construction: it usually
	// reads a detect stream's result, which does not exist yet.
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

	// *** fieldsFromSamples: widen across every sampled value, not just the first *** //
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

	// *** arrowToObjectStream reserved column name *** //
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
	// *** Utf8 cells: Date -> ISO string (not a local-timezone String(date)),
	// object -> JSON (not "[object Object]"); null stays null. *** //
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
});
