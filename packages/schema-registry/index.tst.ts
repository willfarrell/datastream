/// <reference lib="dom" />
/// <reference types="node" />
import type { ResultStream } from "@datastream/core";
import type {
	ConfluentEnvelope,
	ConfluentUnframeResult,
	GlueEnvelope,
	GlueUnframeResult,
} from "@datastream/schema-registry";
import {
	confluentFrameStream,
	confluentUnframeStream,
	glueFrameStream,
	glueUnframeStream,
} from "@datastream/schema-registry";
import { describe, expect, test } from "tstyche";

describe("ConfluentEnvelope", () => {
	test("has schemaId and payload", () => {
		expect<ConfluentEnvelope>().type.toBeAssignableTo<{
			schemaId: number;
			payload: Uint8Array;
		}>();
	});
});

describe("GlueEnvelope", () => {
	test("has schemaVersionId, compression, and payload", () => {
		expect<GlueEnvelope>().type.toBeAssignableTo<{
			schemaVersionId: string;
			compression: "none" | "zlib";
			payload: Uint8Array;
		}>();
	});
});

describe("confluentFrameStream", () => {
	test("requires schemaId", () => {
		expect(
			confluentFrameStream({ schemaId: 1 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("exposes result", () => {
		const stream = confluentFrameStream({ schemaId: 1 });
		expect(stream.result).type.not.toBeAssignableTo<never>();
	});
});

describe("confluentUnframeStream", () => {
	test("accepts no options", () => {
		expect(confluentUnframeStream()).type.not.toBeAssignableTo<never>();
	});

	test("result lists distinct schemaIds", () => {
		expect(confluentUnframeStream()).type.toBeAssignableTo<
			ResultStream<ConfluentUnframeResult>
		>();
		expect<ConfluentUnframeResult["schemaIds"]>().type.toBe<number[]>();
		expect<ConfluentUnframeResult["untrackedSchemaIds"]>().type.toBe<number>();
	});

	test("accepts maxSchemaIds (number or null)", () => {
		expect(
			confluentUnframeStream({ maxSchemaIds: 10 }),
		).type.not.toBeAssignableTo<never>();
		expect(
			confluentUnframeStream({ maxSchemaIds: null }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("glueFrameStream", () => {
	test("requires schemaVersionId", () => {
		expect(
			glueFrameStream({ schemaVersionId: "abc" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts compression", () => {
		expect(
			glueFrameStream({ schemaVersionId: "abc", compression: "zlib" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxOutputSize (number or null)", () => {
		expect(
			glueFrameStream({ schemaVersionId: "abc", maxOutputSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
		expect(
			glueFrameStream({ schemaVersionId: "abc", maxOutputSize: null }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects renamed maxFrameBytes", () => {
		expect(glueFrameStream).type.not.toBeCallableWith({
			schemaVersionId: "abc",
			maxFrameBytes: 1024,
		});
	});
});

describe("glueUnframeStream", () => {
	test("accepts maxOutputSize", () => {
		expect(
			glueUnframeStream({ maxOutputSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxOutputSize: null (no limit)", () => {
		expect(
			glueUnframeStream({ maxOutputSize: null }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects renamed maxDecompressedBytes", () => {
		expect(glueUnframeStream).type.not.toBeCallableWith({
			maxDecompressedBytes: 1024,
		});
	});

	test("result lists distinct schemaVersionIds", () => {
		expect(glueUnframeStream()).type.toBeAssignableTo<
			ResultStream<GlueUnframeResult>
		>();
		expect<GlueUnframeResult["schemaVersionIds"]>().type.toBe<string[]>();
		expect<
			GlueUnframeResult["untrackedSchemaVersionIds"]
		>().type.toBe<number>();
	});

	test("accepts maxSchemaIds (number or null)", () => {
		expect(
			glueUnframeStream({ maxSchemaIds: 10 }),
		).type.not.toBeAssignableTo<never>();
	});
});
