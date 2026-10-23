/// <reference types="node" />

import { EventEmitter } from "node:events";
import type { DatastreamReadable, DatastreamWritable } from "@datastream/core";
import {
	type GcpPubSubMessage,
	type GcpPubSubPublishMessage,
	gcpPubSubAckStream,
	gcpPubSubNackStream,
	gcpPubSubPublishStream,
	gcpPubSubSubscribeStream,
} from "@datastream/gcp/pubsub";
import { describe, expect, test } from "tstyche";

const topic = {
	publishMessage: async (_message: GcpPubSubPublishMessage) => "id",
	flush: async () => {},
};
// An EventEmitter-based Subscription (like the SDK's) is accepted.
const subscription = Object.assign(new EventEmitter(), {
	close: async () => {},
});

describe("gcpPubSubPublishStream", () => {
	test("returns a writable of publish messages", () => {
		expect(
			gcpPubSubPublishStream({ topic, maxInFlight: 10 }),
		).type.toBeAssignableTo<DatastreamWritable<GcpPubSubPublishMessage>>();
	});
	test("accepts string, bytes or json data", () => {
		expect<GcpPubSubPublishMessage>().type.toBeAssignableFrom({
			data: "a",
			attributes: { k: "v" },
			orderingKey: "o",
		});
		expect<GcpPubSubPublishMessage>().type.toBeAssignableFrom({
			data: new Uint8Array(),
		});
		expect<GcpPubSubPublishMessage>().type.toBeAssignableFrom({
			json: { x: 1 },
		});
	});
	test("requires a topic", () => {
		expect(gcpPubSubPublishStream).type.not.toBeCallableWith({});
	});
	test("rejects non-string attributes", () => {
		expect<GcpPubSubPublishMessage>().type.not.toBeAssignableFrom({
			attributes: { k: 1 },
		});
	});
});

describe("gcpPubSubSubscribeStream", () => {
	test("returns a readable of messages", () => {
		expect(gcpPubSubSubscribeStream({ subscription })).type.toBeAssignableTo<
			DatastreamReadable<GcpPubSubMessage>
		>();
	});
	test("stop() returns Promise<void>", () => {
		expect(gcpPubSubSubscribeStream({ subscription }).stop()).type.toBe<
			Promise<void>
		>();
	});
	test("requires a subscription with close()", () => {
		expect(gcpPubSubSubscribeStream).type.not.toBeCallableWith({});
		expect(gcpPubSubSubscribeStream).type.not.toBeCallableWith({
			subscription: new EventEmitter(),
		});
	});
});

describe("gcpPubSubAckStream / gcpPubSubNackStream", () => {
	test("return writables of messages", () => {
		expect(gcpPubSubAckStream({ subscription })).type.toBeAssignableTo<
			DatastreamWritable<GcpPubSubMessage>
		>();
		expect(
			gcpPubSubNackStream({ subscription, withResponse: true }),
		).type.toBeAssignableTo<DatastreamWritable<GcpPubSubMessage>>();
	});
	test("withResponse is a boolean", () => {
		expect(gcpPubSubAckStream).type.not.toBeCallableWith({
			subscription,
			withResponse: "yes",
		});
	});
});
