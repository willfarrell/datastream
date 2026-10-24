/// <reference types="node" />

import { PassThrough, Writable } from "node:stream";
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamResult,
} from "@datastream/core";
import {
	type GcpBigQueryLoadResult,
	type GcpBigQueryRow,
	gcpBigQueryInsertStream,
	gcpBigQueryLoadStream,
	gcpBigQueryQueryStream,
} from "@datastream/gcp/bigquery";
import { describe, expect, test } from "tstyche";

const bigquery = {
	createQueryStream: (_query: Record<string, unknown>) =>
		new PassThrough({ objectMode: true }),
};
const table = {
	createWriteStream: (_metadata: Record<string, unknown>) => new Writable(),
	insert: async (
		_rows: GcpBigQueryRow[],
		_options: Record<string, unknown>,
	) => [{}],
};

describe("gcpBigQueryQueryStream", () => {
	test("resolves to a readable of rows", () => {
		expect(
			gcpBigQueryQueryStream({
				bigquery,
				query: "SELECT 1",
				params: { a: 1 },
				wrapIntegers: true,
			}),
		).type.toBe<Promise<DatastreamReadable<GcpBigQueryRow>>>();
	});
	test("requires bigquery and query", () => {
		expect(gcpBigQueryQueryStream).type.not.toBeCallableWith({ bigquery });
		expect(gcpBigQueryQueryStream).type.not.toBeCallableWith({
			query: "SELECT 1",
		});
	});
});

describe("gcpBigQueryLoadStream", () => {
	test("returns a writable of bytes with a load result", () => {
		const stream = gcpBigQueryLoadStream({
			table,
			sourceFormat: "CSV",
			skipLeadingRows: 1,
		});
		expect(stream).type.toBeAssignableTo<DatastreamWritable>();
		expect<ReturnType<typeof stream.result>>().type.toBe<
			StreamResult<GcpBigQueryLoadResult>
		>();
	});
	test("requires a table", () => {
		expect(gcpBigQueryLoadStream).type.not.toBeCallableWith({});
	});
});

describe("gcpBigQueryInsertStream", () => {
	test("returns a writable of rows", () => {
		expect(
			gcpBigQueryInsertStream({
				table,
				batchSize: 100,
				maxBatchBytes: 1024,
				skipInvalidRows: true,
			}),
		).type.toBe<DatastreamWritable<GcpBigQueryRow>>();
	});
	test("batchSize is a number", () => {
		expect(gcpBigQueryInsertStream).type.not.toBeCallableWith({
			table,
			batchSize: "500",
		});
	});
	test("requires a table", () => {
		expect(gcpBigQueryInsertStream).type.not.toBeCallableWith({});
	});
});
