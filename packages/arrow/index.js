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

// Widen across every sampled value rather than trusting the first one, so a
// column that starts with 1 and later holds 2.7 is not truncated by an Int32
// builder. Int32 + Float64 widen to Float64; any other mix falls back to Utf8.
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
	// Rows held back until the schema is sealed; they are also the sample the
	// schema is inferred from.
	const buffered = [];
	let sealed = false;

	const seal = () => {
		// Nothing to seal until at least one row has been sampled. Once sealed,
		// `buffered` is drained right after, so this same guard makes a repeat
		// call a no-op without needing a separate sealed flag here.
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
		// Seal a short stream that never reached sampleSize, then flush whatever is
		// still buffered. If the stream already sealed mid-stream, `buffered` is
		// empty so seal() is a no-op.
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
		// inferType treats "" as null, so an empty cell in a non-Utf8 column must
		// be null too (not 0/false/epoch); only Utf8 can hold a real "".
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
// typeId check (not instanceof Utf8) so schemas built with another copy of
// apache-arrow still match.
const makeConverters = (schema) =>
	schema.fields.map((field) =>
		DataType.isUtf8(field.type) ? toUtf8 : identity,
	);

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

	const init = () => {
		if (builders) return;
		resolvedSchema = assertSchema(
			"arrowBatchFromArrayStream",
			resolveLazy(schema),
		);
		builders = makeBuilders(resolvedSchema);
		converters = makeConverters(resolvedSchema);
	};

	// Eagerly validate a concrete schema at construction so a missing/misconfigured
	// schema surfaces at the call site rather than only once rows happen to flow.
	if (typeof schema !== "function")
		assertSchema("arrowBatchFromArrayStream", resolveLazy(schema));

	const transform = (row, enqueue) => {
		init();
		for (let i = 0, l = builders.length; i < l; i++) {
			builders[i].append(converters[i](row[i]));
		}
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

	const init = () => {
		if (builders) return;
		resolvedSchema = assertSchema(
			"arrowBatchFromObjectStream",
			resolveLazy(schema),
		);
		fieldNames = resolvedSchema.fields.map((f) => f.name);
		builders = makeBuilders(resolvedSchema);
		converters = makeConverters(resolvedSchema);
	};

	// Eagerly validate a concrete schema at construction so a missing/misconfigured
	// schema surfaces at the call site rather than only once rows happen to flow.
	if (typeof schema !== "function")
		assertSchema("arrowBatchFromObjectStream", resolveLazy(schema));

	const transform = (row, enqueue) => {
		init();
		for (let i = 0, l = builders.length; i < l; i++) {
			builders[i].append(converters[i](row[fieldNames[i]]));
		}
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
		const names = [];
		const cols = [];
		const isTimestamp = [];
		for (let i = 0; i < colCount; i++) {
			names.push(fields[i].name);
			cols.push(batch.getChildAt(i));
			isTimestamp.push(isTimestampType(fields[i].type));
		}
		const rowCount = batch.numRows;
		for (let r = 0; r < rowCount; r++) {
			const row = {};
			for (let i = 0; i < colCount; i++) {
				// defineProperty so a column named "__proto__" becomes an own
				// enumerable data property instead of replacing the row's prototype
				// (which a plain `row[name] = ...` would do, silently dropping it).
				Object.defineProperty(row, names[i], {
					value: readCell(cols[i], isTimestamp[i], r),
					writable: true,
					enumerable: true,
					configurable: true,
				});
			}
			enqueue(row);
		}
	};
	return createTransformStream(transform, streamOptions);
};
