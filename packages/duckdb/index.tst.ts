/// <reference lib="dom" />
/// <reference types="node" />
import {
	duckdbAppenderStream,
	duckdbArrowInsertStream,
	duckdbConnect,
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
});
