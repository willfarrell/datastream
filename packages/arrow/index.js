// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createTransformStream, resolveLazy } from "@datastream/core";
import {
	Bool,
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

const fieldsFromSamples = (samples, fieldNames, isArray) => {
	return fieldNames.map((name, idx) => {
		let type = null;
		for (const sample of samples) {
			const v = isArray ? sample[idx] : sample[name];
			type = inferType(v);
			if (type !== null) break;
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
	const samples = [];
	let sealed = false;

	const seal = () => {
		// Nothing to seal until at least one row has been sampled. Once sealed,
		// `samples` is cleared (below), so this same guard makes a repeat call a
		// no-op without needing a separate sealed flag here.
		if (!samples.length) return;
		const isArray = Array.isArray(samples[0]);
		let fieldNames;
		if (isArray) {
			// Column count is the widest array seen across all sampled rows.
			let width = 0;
			for (const sample of samples) {
				width = Math.max(width, sample.length);
			}
			fieldNames = [];
			for (let i = 0; i < width; i++) fieldNames.push(`column${i}`);
		} else {
			// Union the keys across every sampled row (preserving first-seen order),
			// so columns that only appear in later rows are not dropped.
			const seen = new Set();
			fieldNames = [];
			for (const sample of samples) {
				for (const key of Object.keys(sample)) {
					if (!seen.has(key)) {
						seen.add(key);
						fieldNames.push(key);
					}
				}
			}
		}
		const fields = fieldsFromSamples(samples, fieldNames, isArray);
		value.schema = new Schema(fields);
		value.fields = fieldNames;
		sealed = true;
		// The sampled rows are no longer needed once the schema is computed;
		// release them so they are not retained for the stream's lifetime.
		samples.length = 0;
	};

	const buffered = [];
	const transform = (chunk, enqueue) => {
		if (sealed) {
			enqueue(chunk);
			return;
		}
		samples.push(chunk);
		buffered.push(chunk);
		if (samples.length >= sampleSize) {
			seal();
			while (buffered.length) enqueue(buffered.shift());
		}
	};
	const flush = (enqueue) => {
		// Seal a short stream that never reached sampleSize, then flush whatever is
		// still buffered. If the stream already sealed mid-stream, `samples` is
		// empty so seal() is a no-op and `buffered` has already been drained.
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
		makeBuilder({ type: field.type, nullValues: [null, undefined] }),
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
	let rowCount = 0;

	const init = () => {
		if (builders) return;
		resolvedSchema = assertSchema(
			"arrowBatchFromArrayStream",
			resolveLazy(schema),
		);
		builders = makeBuilders(resolvedSchema);
	};

	// Eagerly validate a concrete schema at construction so a missing/misconfigured
	// schema surfaces at the call site rather than only once rows happen to flow.
	if (typeof schema !== "function")
		assertSchema("arrowBatchFromArrayStream", resolveLazy(schema));

	const transform = (row, enqueue) => {
		init();
		for (let i = 0, l = builders.length; i < l; i++) {
			builders[i].append(row[i]);
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
	let rowCount = 0;

	const init = () => {
		if (builders) return;
		resolvedSchema = assertSchema(
			"arrowBatchFromObjectStream",
			resolveLazy(schema),
		);
		fieldNames = resolvedSchema.fields.map((f) => f.name);
		builders = makeBuilders(resolvedSchema);
	};

	// Eagerly validate a concrete schema at construction so a missing/misconfigured
	// schema surfaces at the call site rather than only once rows happen to flow.
	if (typeof schema !== "function")
		assertSchema("arrowBatchFromObjectStream", resolveLazy(schema));

	const transform = (row, enqueue) => {
		init();
		for (let i = 0, l = builders.length; i < l; i++) {
			builders[i].append(row[fieldNames[i]]);
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
			for (let i = 0; i < colCount; i++)
				row[names[i]] = readCell(cols[i], isTimestamp[i], r);
			enqueue(row);
		}
	};
	return createTransformStream(transform, streamOptions);
};

export default {
	detectSchemaStream: arrowDetectSchemaStream,
	batchFromArrayStream: arrowBatchFromArrayStream,
	batchFromObjectStream: arrowBatchFromObjectStream,
	toArrayStream: arrowToArrayStream,
	toObjectStream: arrowToObjectStream,
};
