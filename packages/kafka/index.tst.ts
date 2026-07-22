/// <reference lib="dom" />
/// <reference types="node" />
import type { KafkaConnection } from "@datastream/kafka";
import {
	kafkaConnect,
	kafkaConsumeStream,
	kafkaProduceStream,
} from "@datastream/kafka";
import { describe, expect, test } from "tstyche";

const producer: unknown = {};
const consumer: unknown = {};

describe("kafkaConnect", () => {
	test("accepts no args", () => {
		expect(kafkaConnect()).type.not.toBeAssignableTo<never>();
	});

	test("accepts broker config", () => {
		expect(
			kafkaConnect({
				brokers: ["localhost:9092"],
				clientId: "c",
				groupId: "g",
			}),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts producer: false", () => {
		expect(
			kafkaConnect({ producer: false }),
		).type.not.toBeAssignableTo<never>();
	});

	test("resolves a KafkaConnection", () => {
		expect(kafkaConnect()).type.toBe<Promise<KafkaConnection>>();
	});
});

describe("kafkaProduceStream", () => {
	test("requires producer and topic", () => {
		expect(
			kafkaProduceStream({ producer, topic: "t" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts batching options", () => {
		expect(
			kafkaProduceStream({ producer, topic: "t", batchSize: 50, acks: -1 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("kafkaConsumeStream", () => {
	test("requires consumer and topics", () => {
		expect(
			kafkaConsumeStream({ consumer, topics: "t" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts an array of topics and a signal", () => {
		expect(
			kafkaConsumeStream({
				consumer,
				topics: ["a", "b"],
				fromBeginning: true,
				signal: new AbortController().signal,
			}),
		).type.not.toBeAssignableTo<never>();
	});
});
