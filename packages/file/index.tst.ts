import _default, { fileReadStream, fileWriteStream } from "@datastream/file";
import { describe, expect, test } from "tstyche";

describe("fileReadStream", () => {
	test("accepts options", () => {
		expect(
			fileReadStream({ types: [{ accept: { "text/csv": [".csv"] } }] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("fileWriteStream", () => {
	test("accepts options", () => {
		expect(
			fileWriteStream({ path: "test.csv" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("default export", () => {
	test("has readStream", () => {
		expect(_default.readStream).type.toBe<typeof fileReadStream>();
	});

	test("has writeStream", () => {
		expect(_default.writeStream).type.toBe<typeof fileWriteStream>();
	});
});
