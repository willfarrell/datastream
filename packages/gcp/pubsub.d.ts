// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export interface GcpPubSubPublishMessage {
	/** Strings are sent as UTF-8. */
	data?: Uint8Array | string;
	/** Convenience: published as JSON (the SDK serializes it). */
	json?: unknown;
	attributes?: Record<string, string>;
	orderingKey?: string;
	[key: string]: unknown;
}

/** The parts of an @google-cloud/pubsub `Topic` the stream uses. */
export interface GcpPubSubTopic {
	publishMessage(message: GcpPubSubPublishMessage): Promise<string>;
	flush(): Promise<void>;
}

/** The parts of an @google-cloud/pubsub `Message` the streams use. */
export interface GcpPubSubMessage {
	id: string;
	data: Uint8Array;
	attributes: Record<string, string>;
	ack(): void;
	nack(): void;
	ackWithResponse(): Promise<unknown>;
	nackWithResponse(): Promise<unknown>;
}

/** The parts of an @google-cloud/pubsub `Subscription` the streams use. */
export interface GcpPubSubSubscription {
	on(event: string, listener: (...args: unknown[]) => void): unknown;
	off(event: string, listener: (...args: unknown[]) => void): unknown;
	close(): Promise<void>;
}

export function gcpPubSubPublishStream(
	options: {
		topic: GcpPubSubTopic;
		/** Publishes kept outstanding (the SDK batches them). Defaults to 100. */
		maxInFlight?: number;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable<GcpPubSubPublishMessage>;

export function gcpPubSubSubscribeStream(
	options: { subscription: GcpPubSubSubscription },
	streamOptions?: StreamOptions,
): DatastreamReadable<GcpPubSubMessage> & {
	/**
	 * Graceful shutdown: stops delivering (later messages are nacked), ends the
	 * readable, waits until every open gcpPubSubAckStream / gcpPubSubNackStream
	 * on the same subscription has closed, then calls `subscription.close()`.
	 * Aborting the `signal` or destroying the stream closes it immediately.
	 */
	stop: () => Promise<void>;
};

export function gcpPubSubAckStream(
	options: {
		subscription: GcpPubSubSubscription;
		/** Await `ackWithResponse()` and error if it rejects (exactly-once). */
		withResponse?: boolean;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable<GcpPubSubMessage>;

export function gcpPubSubNackStream(
	options: {
		subscription: GcpPubSubSubscription;
		/** Await `nackWithResponse()` and error if it rejects (exactly-once). */
		withResponse?: boolean;
	},
	streamOptions?: StreamOptions,
): DatastreamWritable<GcpPubSubMessage>;
