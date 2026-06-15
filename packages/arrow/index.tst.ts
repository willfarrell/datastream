/// <reference lib="dom" />
/// <reference types="node" />
import type { ArrowDetectSchemaResult } from "@datastream/arrow";
import {
	arrowBatchFromArrayStream,
	arrowBatchFromObjectStream,
	arrowDetectSchemaStream,
	arrowToArrayStream,
	arrowToObjectStream,
} from "@datastream/arrow";
import type { Schema } from "apache-arrow";
import { describe, expect, test } from "tstyche";

const schema = {} as Schema;

describe("ArrowDetectSchemaResult", () => {
	test("has schema and fields", () => {
		expect<ArrowDetectSchemaResult>().type.toBeAssignableTo<{
			schema: Schema | null;
			fields: string[] | null;
		}>();
	});
});

describe("arrowDetectSchemaStream", () => {
	test("accepts no options", () => {
		expect(arrowDetectSchemaStream()).type.not.toBeAssignableTo<never>();
	});

	test("accepts sampleSize", () => {
		expect(
			arrowDetectSchemaStream({ sampleSize: 100 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("exposes result", () => {
		const stream = arrowDetectSchemaStream();
		expect(stream.result).type.not.toBeAssignableTo<never>();
	});
});

describe("arrowBatchFromArrayStream", () => {
	test("requires schema", () => {
		expect(
			arrowBatchFromArrayStream({ schema }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts lazy schema and batchSize", () => {
		expect(
			arrowBatchFromArrayStream({ schema: () => schema, batchSize: 512 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("arrowBatchFromObjectStream", () => {
	test("requires schema", () => {
		expect(
			arrowBatchFromObjectStream({ schema }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("arrowToArrayStream", () => {
	test("accepts no options", () => {
		expect(arrowToArrayStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("arrowToObjectStream", () => {
	test("accepts no options", () => {
		expect(arrowToObjectStream()).type.not.toBeAssignableTo<never>();
	});
});
