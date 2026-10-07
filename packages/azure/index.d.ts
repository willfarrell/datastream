// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
export {
	azureBlobDownloadStream,
	azureBlobUploadStream,
} from "@datastream/azure/blob";
export {
	azureCosmosDeleteItemStream,
	azureCosmosQueryStream,
	azureCosmosUpsertItemStream,
} from "@datastream/azure/cosmos";
export {
	azureEventHubsReceiveEventsStream,
	azureEventHubsSendEventsStream,
} from "@datastream/azure/event-hubs";
export { azureEventHubsKafkaMechanism } from "@datastream/azure/event-hubs-kafka";
export {
	azureQueueDeleteMessageStream,
	azureQueueReceiveMessagesStream,
	azureQueueSendMessageStream,
} from "@datastream/azure/queue";
export {
	azureServiceBusCompleteMessageStream,
	azureServiceBusReceiveMessagesStream,
	azureServiceBusSendMessagesStream,
} from "@datastream/azure/service-bus";
