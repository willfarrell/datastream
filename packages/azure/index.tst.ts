/// <reference types="node" />
import {
	azureBlobDownloadStream,
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
	azureServiceBusCompleteMessageStream,
	azureServiceBusReceiveMessagesStream,
	azureServiceBusSendMessagesStream,
} from "@datastream/azure";
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
	test("client is required", () => {
		expect(azureBlobDownloadStream).type.not.toBeCallableWith({});
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
