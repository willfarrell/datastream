/// <reference lib="dom" />
/// <reference types="node" />
import type { DatastreamTransform, ResultStream } from "@datastream/core";
import type {
	ConfluentEnvelope,
	ConfluentSchema,
	ConfluentSchemaReference,
	ConfluentUnframeResult,
	FramedMessage,
	GlueEnvelope,
	GlueUnframeResult,
} from "@datastream/schema-registry";
import {
	confluentFrameStream,
	confluentSchemaRegistryResolver,
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

	test("unframes a message envelope, keeping its fields", () => {
		type Message = {
			topic: string;
			offset: string;
			value: Uint8Array | null;
		};
		expect(confluentUnframeStream<Message>()).type.toBeAssignableTo<
			DatastreamTransform<
				Message,
				{ topic: string; offset: string; schemaId: number; payload: Uint8Array }
			>
		>();
		expect(confluentUnframeStream()).type.toBeAssignableTo<
			DatastreamTransform<Uint8Array, ConfluentEnvelope>
		>();
	});

	test("an envelope needs a bytes value", () => {
		expect<{ topic: string }>().type.not.toBeAssignableTo<
			Uint8Array | FramedMessage
		>();
		expect<{ value: string }>().type.not.toBeAssignableTo<
			Uint8Array | FramedMessage
		>();
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

	test("unframes a message envelope, keeping its fields", () => {
		type Message = { partition: number; value: Uint8Array | null };
		expect(glueUnframeStream<Message>()).type.toBeAssignableTo<
			DatastreamTransform<Message, { partition: number } & GlueEnvelope>
		>();
		expect(glueUnframeStream()).type.toBeAssignableTo<
			DatastreamTransform<Uint8Array, GlueEnvelope>
		>();
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

describe("confluentSchemaRegistryResolver", () => {
	test("requires url", () => {
		expect(confluentSchemaRegistryResolver).type.not.toBeCallableWith({});
	});

	test("resolves a numeric id to a ConfluentSchema", () => {
		const resolve = confluentSchemaRegistryResolver({
			url: "https://registry.example.com",
			headers: { Authorization: "Basic abc" },
			cacheExpiry: 60_000,
			maxCacheSize: null,
			timeout: 5_000,
			signal: new AbortController().signal,
			maxBodySize: null,
			fetch: globalThis.fetch,
		});
		expect(resolve(1)).type.toBe<Promise<ConfluentSchema>>();
		expect(resolve).type.not.toBeCallableWith("1");
		expect<ConfluentSchema["references"]>().type.toBe<
			ConfluentSchemaReference[]
		>();
		expect<ConfluentSchema["dataFormat"]>().type.toBe<
			"AVRO" | "PROTOBUF" | "JSON"
		>();
	});
});
