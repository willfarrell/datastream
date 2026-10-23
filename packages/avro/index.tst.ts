/// <reference lib="dom" />
/// <reference types="node" />
import type {
	AvroEnvelope,
	AvroSchema,
	AvroSchemaInput,
} from "@datastream/avro";
import { avroDecodeStream, avroEncodeStream } from "@datastream/avro";
import type { DatastreamTransform } from "@datastream/core";
import type { KafkaConsumedMessage } from "@datastream/kafka";
import {
	confluentSchemaRegistryResolver,
	confluentUnframeStream,
} from "@datastream/schema-registry";
import { describe, expect, test } from "tstyche";

const schema = {
	type: "record",
	name: "Msg",
	fields: [{ name: "id", type: "int" }],
};

describe("AvroSchemaInput", () => {
	test("accepts a schema object, JSON string, or lazy function", () => {
		expect<typeof schema>().type.toBeAssignableTo<AvroSchemaInput>();
		expect<string>().type.toBeAssignableTo<AvroSchemaInput>();
		expect<() => AvroSchema>().type.toBeAssignableTo<AvroSchemaInput>();
		expect<
			() => Promise<AvroSchema>
		>().type.toBeAssignableTo<AvroSchemaInput>();
	});

	test("rejects a number", () => {
		expect<number>().type.not.toBeAssignableTo<AvroSchemaInput>();
	});
});

describe("avroEncodeStream", () => {
	test("requires schema", () => {
		expect(avroEncodeStream).type.not.toBeCallableWith({});
	});

	test("emits bytes", () => {
		expect(avroEncodeStream({ schema })).type.toBe<
			DatastreamTransform<unknown, Uint8Array>
		>();
	});
});

describe("avroDecodeStream", () => {
	test("accepts a lazy schema and a record type", () => {
		expect(
			avroDecodeStream<{ id: number }>({ schema: () => schema }),
		).type.toBe<
			DatastreamTransform<ArrayBufferView | ArrayBuffer, { id: number }>
		>();
	});

	test("accepts maxMessageSize (number or null)", () => {
		expect(avroDecodeStream).type.toBeCallableWith({
			schema,
			maxMessageSize: 1024,
		});
		expect(avroDecodeStream).type.toBeCallableWith({
			schema,
			maxMessageSize: null,
		});
	});

	test("requires schema or resolveSchema", () => {
		expect(avroDecodeStream).type.not.toBeCallableWith({});
		expect(avroDecodeStream).type.not.toBeCallableWith({
			schema,
			resolveSchema: () => schema,
		});
	});

	test("resolveSchema takes a registry resolver and keeps envelope fields", () => {
		const resolveSchema = confluentSchemaRegistryResolver({
			url: "https://registry.example.com",
		});
		const unframe = confluentUnframeStream<KafkaConsumedMessage>();
		type Envelope =
			typeof unframe extends DatastreamTransform<unknown, infer O> ? O : never;
		const decode = avroDecodeStream<{ id: number }, Envelope>({
			resolveSchema,
		});
		expect(decode).type.toBeAssignableTo<
			DatastreamTransform<
				Envelope,
				{
					topic: string;
					offset: string;
					schemaId: number;
					value: { id: number };
				}
			>
		>();
		expect<Envelope>().type.toBeAssignableTo<AvroEnvelope>();
	});

	test("resolveSchema accepts schemas and a Glue-style resolver", () => {
		expect(avroDecodeStream).type.toBeCallableWith({
			resolveSchema: async (id: string) => ({ schemaDefinition: id }),
		});
		expect(avroDecodeStream).type.toBeCallableWith({
			resolveSchema: () => schema,
		});
		expect(avroDecodeStream).type.not.toBeCallableWith({
			resolveSchema: () => 1,
		});
	});
});
