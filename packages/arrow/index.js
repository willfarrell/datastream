// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream, resolveLazy } from "@datastream/core";
import {
	Bool,
	DataType,
	Field,
	Float64,
	Int32,
	makeBuilder,
	makeData,
	RecordBatch,
	Schema,
	Struct,
	TimestampMillisecond,
	Utf8,
} from "apache-arrow";

const inferType = (value) => {
	if (value === null || value === undefined || value === "") return null;
	if (typeof value === "boolean") return new Bool();
	if (typeof value === "number") {
		if (!Number.isInteger(value)) return new Float64();
		// Integers outside the signed 32-bit range silently wrap inside an Int32
		// builder, so widen them to Float64 (exact up to 2^53).
		return value > 2147483647 || value < -2147483648
			? new Float64()
			: new Int32();
	}
	if (value instanceof Date) return new TimestampMillisecond();
	return new Utf8();
};

const isNumberType = (type) => type instanceof Int32 || type instanceof Float64;

const fieldsFromSamples = (samples, fieldNames, isArray) => {
	return fieldNames.map((name, idx) => {
		let type = null;
		for (const sample of samples) {
			const next = inferType(isArray ? sample[idx] : sample[name]);
			if (next === null) continue;
			if (type === null || type.typeId === next.typeId) {
				type = next;
			} else if (isNumberType(type) && isNumberType(next)) {
				type = new Float64();
			} else {
				type = new Utf8();
			}
		}
		return new Field(name, type ?? new Utf8(), true);
	});
};

const isTimestampType = (type) => type instanceof TimestampMillisecond;

export const arrowDetectSchemaStream = (
	{ sampleSize = 100, resultKey } = {},
	streamOptions = {},
) => {
	const value = { schema: null, fields: null };
	const buffered = [];
	let sealed = false;

	const seal = () => {
		if (!buffered.length) return;
		const isArray = Array.isArray(buffered[0]);
		let fieldNames;
		if (isArray) {
			// Column count is the widest array seen across all sampled rows.
			let width = 0;
			for (const sample of buffered) {
				width = Math.max(width, sample.length);
			}
			fieldNames = [];
			for (let i = 0; i < width; i++) fieldNames.push(`column${i}`);
		} else {
			// Union the keys across every sampled row (preserving first-seen order),
			// so columns that only appear in later rows are not dropped.
			const seen = new Set();
			fieldNames = [];
			for (const sample of buffered) {
				for (const key of Object.keys(sample)) {
					if (!seen.has(key)) {
						seen.add(key);
						fieldNames.push(key);
					}
				}
			}
		}
		const fields = fieldsFromSamples(buffered, fieldNames, isArray);
		value.schema = new Schema(fields);
		value.fields = fieldNames;
		sealed = true;
	};

	const transform = (chunk, enqueue) => {
		if (sealed) {
			enqueue(chunk);
			return;
		}
		buffered.push(chunk);
		if (buffered.length >= sampleSize) {
			seal();
			while (buffered.length) enqueue(buffered.shift());
		}
	};
	const flush = (enqueue) => {
		seal();
		while (buffered.length) enqueue(buffered.shift());
	};

	const stream = createTransformStream(transform, flush, streamOptions);
	stream.result = () => ({ key: resultKey ?? "arrowDetectSchema", value });
	return stream;
};

const buildRecordBatch = (schema, builders, length) => {
	const childrenData = builders.map((b) => b.flush());
	const structData = makeData({
		type: new Struct(schema.fields),
		length,
		children: childrenData,
	});
	return new RecordBatch(schema, structData);
};

const makeBuilders = (schema) =>
	schema.fields.map((field) =>
		makeBuilder({
			type: field.type,
			nullValues: DataType.isUtf8(field.type)
				? [null, undefined]
				: [null, undefined, ""],
		}),
	);

// The Utf8 builder coerces with String(v): a Date becomes a local-timezone
// string and an object "[object Object]". Hand it ISO / JSON text instead;
// null stays null and other primitives keep String() semantics.
const toUtf8 = (v) =>
	v instanceof Date
		? v.toISOString()
		: typeof v === "object" && v !== null
			? JSON.stringify(v)
			: v;
const identity = (v) => v;

// Value checks for the column types arrowDetectSchemaStream seals (and their
// kin from user/IPC schemas). The Arrow builders coerce silently: an Int32
// builder wraps 3000000000 to -1294967296, truncates 2.7 to 2 and turns "oops"
// into 0, so a schema inferred from the first sampleSize rows would corrupt any
// later row that does not fit it. Each check returns the Error class to throw,
// or undefined for a valid value. Other types (Utf8, Int64, ...) are unchecked.
const pass = () => {};
const intCheck = (type) => {
	const max = 2 ** (type.isSigned ? type.bitWidth - 1 : type.bitWidth) - 1;
	const min = type.isSigned ? -max - 1 : 0;
	return (v) =>
		!Number.isInteger(v) ? TypeError : v < min || v > max ? RangeError : pass();
};
const numberCheck = (v) => (typeof v === "number" ? pass() : TypeError);
const booleanCheck = (v) => (typeof v === "boolean" ? pass() : TypeError);
const timestampCheck = (v) =>
	Number.isInteger(v instanceof Date ? v.getTime() : v) ? pass() : TypeError;
const valueCheck = (type) =>
	DataType.isInt(type) && type.bitWidth < 64
		? intCheck(type)
		: DataType.isFloat(type)
			? numberCheck
			: DataType.isBool(type)
				? booleanCheck
				: DataType.isTimestamp(type)
					? timestampCheck
					: pass;

const describeValue = (v) =>
	typeof v === "string"
		? JSON.stringify(v)
		: typeof v === "object" && v !== null
			? Object.prototype.toString.call(v)
			: String(v);

const makeConverter = (name, field) => {
	const { type, nullable } = field;
	// typeId check (not instanceof Utf8) so schemas built with another copy of
	// apache-arrow still match.
	const utf8 = DataType.isUtf8(type);
	const convert = utf8 ? toUtf8 : identity;
	const check = valueCheck(type);
	return (v, row) => {
		const isNull = v === null || v === undefined || (!utf8 && v === "");
		const ErrorType = isNull ? (nullable ? pass() : TypeError) : check(v);
		if (ErrorType) {
			throw new ErrorType(
				`${name}: row ${row}, field "${field.name}" expects ${nullable ? "" : "non-null "}${type}, got ${describeValue(v)}`,
			);
		}
		return convert(v);
	};
};
const makeConverters = (name, schema) =>
	schema.fields.map((field) => makeConverter(name, field));

// A zero-field schema cannot represent rows: apache-arrow derives a
// RecordBatch's length from its child columns, so with no columns every batch
// reports numRows 0 and silently discards every row. Reject it explicitly
// instead of emitting corrupt empty batches.
const assertSchema = (name, schema) => {
	if (!schema) throw new Error(`${name}: schema is required`);
	if (schema.fields.length === 0)
		throw new Error(`${name}: schema must have at least one field`);
	return schema;
};

export const arrowBatchFromArrayStream = (
	{ schema, batchSize = 10_000 } = {},
	streamOptions = {},
) => {
	let resolvedSchema;
	let builders;
	let converters;
	let rowCount = 0;
	let rowIndex = 0;

	const init = () => {
		if (builders) return;
		resolvedSchema = assertSchema(
			"arrowBatchFromArrayStream",
			resolveLazy(schema),
		);
		builders = makeBuilders(resolvedSchema);
		converters = makeConverters("arrowBatchFromArrayStream", resolvedSchema);
	};

	// Eagerly validate a concrete schema at construction so a missing/misconfigured
	// schema surfaces at the call site rather than only once rows happen to flow.
	if (typeof schema !== "function")
		assertSchema("arrowBatchFromArrayStream", resolveLazy(schema));

	const transform = (row, enqueue) => {
		init();
		for (let i = 0, l = builders.length; i < l; i++) {
			builders[i].append(converters[i](row[i], rowIndex));
		}
		rowIndex++;
		rowCount++;
		if (rowCount >= batchSize) {
			enqueue(buildRecordBatch(resolvedSchema, builders, rowCount));
			rowCount = 0;
		}
	};
	const flush = (enqueue) => {
		// Always run init so a missing lazy schema throws even for an empty stream.
		init();
		if (rowCount > 0) {
			enqueue(buildRecordBatch(resolvedSchema, builders, rowCount));
			rowCount = 0;
		}
	};

	return createTransformStream(transform, flush, streamOptions);
};

export const arrowBatchFromObjectStream = (
	{ schema, batchSize = 10_000 } = {},
	streamOptions = {},
) => {
	let resolvedSchema;
	let fieldNames;
	let builders;
	let converters;
	let rowCount = 0;
	let rowIndex = 0;

	const init = () => {
		if (builders) return;
		resolvedSchema = assertSchema(
			"arrowBatchFromObjectStream",
			resolveLazy(schema),
		);
		fieldNames = resolvedSchema.fields.map((f) => f.name);
		builders = makeBuilders(resolvedSchema);
		converters = makeConverters("arrowBatchFromObjectStream", resolvedSchema);
	};

	// Eagerly validate a concrete schema at construction so a missing/misconfigured
	// schema surfaces at the call site rather than only once rows happen to flow.
	if (typeof schema !== "function")
		assertSchema("arrowBatchFromObjectStream", resolveLazy(schema));

	const transform = (row, enqueue) => {
		init();
		for (let i = 0, l = builders.length; i < l; i++) {
			builders[i].append(converters[i](row[fieldNames[i]], rowIndex));
		}
		rowIndex++;
		rowCount++;
		if (rowCount >= batchSize) {
			enqueue(buildRecordBatch(resolvedSchema, builders, rowCount));
			rowCount = 0;
		}
	};
	const flush = (enqueue) => {
		// Always run init so a missing lazy schema throws even for an empty stream.
		init();
		if (rowCount > 0) {
			enqueue(buildRecordBatch(resolvedSchema, builders, rowCount));
			rowCount = 0;
		}
	};

	return createTransformStream(transform, flush, streamOptions);
};

// Read one cell, restoring Date for timestamp columns (which Arrow vectors
// otherwise return as raw epoch-millisecond numbers) so round-trips preserve type.
const readCell = (col, isTimestamp, r) => {
	const value = col.get(r);
	if (isTimestamp && typeof value === "number") return new Date(value);
	return value;
};

export const arrowToArrayStream = (_options = {}, streamOptions = {}) => {
	const transform = (batch, enqueue) => {
		const colCount = batch.schema.fields.length;
		const cols = [];
		const isTimestamp = [];
		for (let i = 0; i < colCount; i++) {
			cols.push(batch.getChildAt(i));
			isTimestamp.push(isTimestampType(batch.schema.fields[i].type));
		}
		const rowCount = batch.numRows;
		for (let r = 0; r < rowCount; r++) {
			// The row is fully populated by the loop below, so build it by pushing
			// each cell rather than pre-sizing (which is observationally identical).
			const row = [];
			for (let i = 0; i < colCount; i++)
				row.push(readCell(cols[i], isTimestamp[i], r));
			enqueue(row);
		}
	};
	return createTransformStream(transform, streamOptions);
};

export const arrowToObjectStream = (_options = {}, streamOptions = {}) => {
	const transform = (batch, enqueue) => {
		const fields = batch.schema.fields;
		const colCount = fields.length;
		// One [name, value] pair per column, reused across rows: Object.fromEntries
		// copies the values out, and makes a column named "__proto__" an own
		// enumerable data property instead of replacing the row's prototype (which
		// a plain `row[name] = ...` would do, silently dropping it).
		const entries = [];
		const cols = [];
		const isTimestamp = [];
		for (let i = 0; i < colCount; i++) {
			entries.push([fields[i].name, undefined]);
			cols.push(batch.getChildAt(i));
			isTimestamp.push(isTimestampType(fields[i].type));
		}
		const rowCount = batch.numRows;
		for (let r = 0; r < rowCount; r++) {
			for (let i = 0; i < colCount; i++) {
				entries[i][1] = readCell(cols[i], isTimestamp[i], r);
			}
			enqueue(Object.fromEntries(entries));
		}
	};
	return createTransformStream(transform, streamOptions);
};
