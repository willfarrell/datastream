/// <reference lib="dom" />
/// <reference types="node" />
import type {
	ConfluentEnvelope,
	GlueEnvelope,
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
});

describe("glueUnframeStream", () => {
	test("accepts maxDecompressedBytes", () => {
		expect(
			glueUnframeStream({ maxDecompressedBytes: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});
});
