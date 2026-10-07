/// <reference lib="dom" />
/// <reference types="node" />
import type { DatastreamReadable, DatastreamWritable } from "@datastream/core";
import { fileReadStream, fileWriteStream } from "@datastream/file";
import { describe, expect, test } from "tstyche";

describe("fileReadStream", () => {
	test("accepts options", () => {
		expect(
			fileReadStream({ types: [{ accept: { "text/csv": [".csv"] } }] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("fileReadStream (node)", () => {
	test("accepts path and basePath", () => {
		expect(fileReadStream).type.toBeCallableWith({
			path: "data/in.csv",
			basePath: "data",
		});
	});

	test("returns a Promise of a stream", () => {
		expect(fileReadStream({ path: "in.csv" })).type.toBe<
			Promise<DatastreamReadable>
		>();
	});
});

describe("fileWriteStream (node)", () => {
	test("accepts basePath", () => {
		expect(fileWriteStream).type.toBeCallableWith({
			path: "data/out.csv",
			basePath: "data",
		});
	});

	test("returns a Promise of a stream", () => {
		expect(fileWriteStream({ path: "out.csv" })).type.toBe<
			Promise<DatastreamWritable>
		>();
	});
});

describe("fileWriteStream", () => {
	test("accepts options", () => {
		expect(
			fileWriteStream({ path: "test.csv" }),
		).type.not.toBeAssignableTo<never>();
	});
});
