/// <reference types="node" />
import type { SQLOutputValue } from "node:sqlite";
import { DatabaseSync } from "node:sqlite";
import type { DatastreamReadable, DatastreamWritable } from "@datastream/core";
import { sqliteInsertStream, sqliteQueryStream } from "@datastream/sqlite";
import { describe, expect, test } from "tstyche";

const db = new DatabaseSync(":memory:");

describe("sqliteQueryStream", () => {
	test("requires db and sql", () => {
		expect(sqliteQueryStream).type.not.toBeCallableWith({ db });
		expect(sqliteQueryStream).type.not.toBeCallableWith({ sql: "SELECT 1" });
	});

	test("accepts positional and named params", () => {
		expect(
			sqliteQueryStream({ db, sql: "SELECT ?", params: [1, "a", null] }),
		).type.not.toBeAssignableTo<never>();
		expect(
			sqliteQueryStream({ db, sql: "SELECT $id", params: { id: 1 } }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts null params", () => {
		expect(
			sqliteQueryStream({ db, sql: "SELECT 1", params: null }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects non-bindable params", () => {
		expect(sqliteQueryStream).type.not.toBeCallableWith({
			db,
			sql: "SELECT ?",
			params: [{}],
		});
	});

	test("returns a readable of row objects", () => {
		expect(sqliteQueryStream({ db, sql: "SELECT 1" })).type.toBe<
			DatastreamReadable<Record<string, SQLOutputValue>>
		>();
	});

	test("accepts a signal in streamOptions", () => {
		expect(
			sqliteQueryStream(
				{ db, sql: "SELECT 1" },
				{ signal: new AbortController().signal },
			),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("sqliteInsertStream", () => {
	test("requires db and table", () => {
		expect(sqliteInsertStream).type.not.toBeCallableWith({ db });
		expect(sqliteInsertStream).type.not.toBeCallableWith({ table: "t" });
	});

	test("accepts columns", () => {
		expect(
			sqliteInsertStream({ db, table: "t", columns: ["id", "name"] }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects non-string columns", () => {
		expect(sqliteInsertStream).type.not.toBeCallableWith({
			db,
			table: "t",
			columns: [1],
		});
	});

	test("returns a writable", () => {
		expect(
			sqliteInsertStream({ db, table: "t" }),
		).type.toBeAssignableTo<DatastreamWritable>();
	});
});
