/// <reference lib="dom" />
/// <reference types="node" />

import type { DatastreamReadable } from "@datastream/core";
import {
	duckdbAppenderStream,
	duckdbArrowInsertStream,
	duckdbConnect,
	duckdbQueryStream,
} from "@datastream/duckdb";
import type { Schema } from "apache-arrow";
import { describe, expect, test } from "tstyche";

const db: unknown = {};
const schema = {} as Schema;

describe("duckdbConnect", () => {
	test("accepts no args", () => {
		expect(duckdbConnect()).type.not.toBeAssignableTo<never>();
	});

	test("accepts path and options", () => {
		expect(
			duckdbConnect(":memory:", { access_mode: "READ_WRITE" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts browser bundles", () => {
		expect(duckdbConnect).type.toBeCallableWith(":memory:", {
			bundles: { mvp: { mainModule: "/m.wasm", mainWorker: "/w.js" } },
		});
	});

	test("returns a Promise", () => {
		expect(duckdbConnect()).type.toBe<Promise<unknown>>();
	});
});

describe("duckdbAppenderStream", () => {
	test("requires db and table", () => {
		expect(
			duckdbAppenderStream({ db, table: "t" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts schema", () => {
		expect(
			duckdbAppenderStream({ db, table: "t", schema }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("duckdbArrowInsertStream", () => {
	test("requires db and table", () => {
		expect(
			duckdbArrowInsertStream({ db, table: "t" }),
		).type.not.toBeAssignableTo<never>();
	});
	test("accepts batchRows", () => {
		expect(duckdbArrowInsertStream).type.toBeCallableWith({
			db,
			table: "t",
			batchRows: 1000,
		});
	});
});

describe("duckdbQueryStream", () => {
	test("requires db and sql", () => {
		expect(duckdbQueryStream).type.not.toBeCallableWith({ db });
		expect(duckdbQueryStream).type.not.toBeCallableWith({ sql: "SELECT 1" });
	});

	test("accepts positional params and stream options", () => {
		expect(duckdbQueryStream).type.toBeCallableWith(
			{ db, sql: "SELECT ?", params: [1, "a", null] },
			{ signal: new AbortController().signal },
		);
	});

	test("rejects non-array params", () => {
		expect(duckdbQueryStream).type.not.toBeCallableWith({
			db,
			sql: "SELECT ?",
			params: 1,
		});
	});

	test("returns a readable of row objects, not a Promise", () => {
		expect(duckdbQueryStream({ db, sql: "SELECT 1" })).type.toBe<
			DatastreamReadable<Record<string, unknown>>
		>();
	});
});
