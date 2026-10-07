/// <reference lib="dom" />
/// <reference types="node" />
import type { ProtobufType, ProtobufTypeInput } from "@datastream/protobuf";
import {
	protobufDecodeStream,
	protobufEncodeStream,
	protobufLengthPrefixFrameStream,
	protobufLengthPrefixUnframeStream,
} from "@datastream/protobuf";
import { describe, expect, test } from "tstyche";

const Type: ProtobufType = {
	encode: () => ({ finish: () => new Uint8Array() }),
	decode: () => ({}),
	create: () => ({}),
};

describe("ProtobufTypeInput", () => {
	test("accepts a static Type", () => {
		expect<ProtobufType>().type.toBeAssignableTo<ProtobufTypeInput>();
	});

	test("accepts a resolver function", () => {
		expect<
			(chunk: unknown) => ProtobufType
		>().type.toBeAssignableTo<ProtobufTypeInput>();
	});
});

describe("protobufEncodeStream", () => {
	test("requires Type", () => {
		expect(protobufEncodeStream({ Type })).type.not.toBeAssignableTo<never>();
	});
});

describe("protobufDecodeStream", () => {
	test("requires Type", () => {
		expect(protobufDecodeStream({ Type })).type.not.toBeAssignableTo<never>();
	});

	test("accepts payload extractor and maxMessageSize", () => {
		expect(
			protobufDecodeStream<{ data: Uint8Array }>({
				Type,
				payload: (chunk) => chunk.data,
				maxMessageSize: 1024,
			}),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts null maxMessageSize (unlimited)", () => {
		expect(
			protobufDecodeStream({ Type, maxMessageSize: null }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects removed maxOutputSize", () => {
		expect(protobufDecodeStream).type.not.toBeCallableWith({
			Type,
			maxOutputSize: 1024,
		});
	});
});

describe("protobufLengthPrefixFrameStream", () => {
	test("accepts no options", () => {
		expect(
			protobufLengthPrefixFrameStream(),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("protobufLengthPrefixUnframeStream", () => {
	test("accepts maxMessageSize", () => {
		expect(
			protobufLengthPrefixUnframeStream({ maxMessageSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});
});
