/// <reference types="node" />

import {
	azureBlobDownloadStream,
	azureBlobListStream,
	azureBlobUploadStream,
	azureCosmosDeleteItemStream,
	azureCosmosQueryStream,
	azureCosmosUpsertItemStream,
	azureEventHubsKafkaMechanism,
	azureEventHubsReceiveEventsStream,
	azureEventHubsSendEventsStream,
	azureQueueDeleteMessageStream,
	azureQueueReceiveMessagesStream,
	azureQueueSendMessageStream,
	azureServiceBusAbandonMessageStream,
	azureServiceBusCompleteMessageStream,
	azureServiceBusDeadLetterMessageStream,
	azureServiceBusReceiveMessagesStream,
	azureServiceBusSendMessagesStream,
} from "@datastream/azure";
import type {
	DatastreamPassThrough,
	DatastreamReadable,
} from "@datastream/core";
import { describe, expect, test } from "tstyche";

const client = {};

describe("Blob", () => {
	test("azureBlobDownloadStream returns promise", () => {
		expect(azureBlobDownloadStream({ client })).type.toBeAssignableTo<
			Promise<unknown>
		>();
	});
	test("azureBlobUploadStream has result", () => {
		expect(azureBlobUploadStream({ client }).result).type.toBeAssignableTo<
			() => Promise<unknown>
		>();
	});
	test("azureBlobUploadStream returns a pass-through with key-less result", () => {
		const stream = azureBlobUploadStream({ client });
		expect(stream).type.toBeAssignableTo<DatastreamPassThrough>();
		expect<ReturnType<typeof stream.result>>().type.toBe<
			Promise<Record<string, never>>
		>();
	});
	test("azureBlobListStream returns a readable promise", () => {
		expect(azureBlobListStream({ client, prefix: "a/" })).type.toBe<
			Promise<DatastreamReadable>
		>();
	});
	test("azureBlobListStream prefix is a string", () => {
		expect(azureBlobListStream).type.not.toBeCallableWith({
			client,
			prefix: 1,
		});
	});
	test("client is required", () => {
		expect(azureBlobDownloadStream).type.not.toBeCallableWith({});
		expect(azureBlobListStream).type.not.toBeCallableWith({});
	});
});

describe("Queue", () => {
	test("azureQueueReceiveMessagesStream returns promise", () => {
		expect(
			azureQueueReceiveMessagesStream({ client, pollingActive: true }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});
	test("send/delete return writables", () => {
		expect(azureQueueSendMessageStream({ client })).type.not.toBe<void>();
		expect(azureQueueDeleteMessageStream({ client })).type.not.toBe<void>();
	});
});

describe("Service Bus", () => {
	test("azureServiceBusReceiveMessagesStream returns promise", () => {
		expect(
			azureServiceBusReceiveMessagesStream({ client, maxMessageCount: 10 }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});
	test("send/complete return writables", () => {
		expect(azureServiceBusSendMessagesStream({ client })).type.not.toBe<void>();
		expect(
			azureServiceBusCompleteMessageStream({ client }),
		).type.not.toBe<void>();
	});
	test("abandon/dead-letter return writables", () => {
		expect(
			azureServiceBusAbandonMessageStream({
				client,
				propertiesToModify: { attempt: 2 },
			}),
		).type.not.toBe<void>();
		expect(
			azureServiceBusDeadLetterMessageStream({
				client,
				deadLetterReason: "invalid",
				deadLetterErrorDescription: "schema mismatch",
			}),
		).type.not.toBe<void>();
		expect(azureServiceBusAbandonMessageStream).type.not.toBeCallableWith({
			client,
			propertiesToModify: { attempt: {} },
		});
		expect(azureServiceBusDeadLetterMessageStream).type.not.toBeCallableWith({
			client,
			deadLetterReason: 1,
		});
	});
});

describe("Event Hubs", () => {
	test("azureEventHubsReceiveEventsStream returns promise", () => {
		expect(
			azureEventHubsReceiveEventsStream({ client, partitionId: "0" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});
	test("azureEventHubsSendEventsStream returns writable", () => {
		expect(
			azureEventHubsSendEventsStream({ client, partitionKey: "k" }),
		).type.not.toBe<void>();
	});
	test("azureEventHubsKafkaMechanism returns oauthbearer", () => {
		expect(
			azureEventHubsKafkaMechanism({
				credential: {
					getToken: async () => ({ token: "t", expiresOnTimestamp: 0 }),
				},
				namespace: "ns",
			}).mechanism,
		).type.toBe<"oauthbearer">();
	});
});

describe("Cosmos DB", () => {
	test("azureCosmosQueryStream returns promise", () => {
		expect(
			azureCosmosQueryStream({ client, query: "SELECT * FROM c" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});
	test("upsert/delete accept retryMaxCount null", () => {
		expect(
			azureCosmosUpsertItemStream({ client, retryMaxCount: null }),
		).type.not.toBe<void>();
		expect(azureCosmosDeleteItemStream({ client })).type.not.toBe<void>();
	});
});
