// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamReadable,
	DatastreamWritable,
	StreamOptions,
} from "@datastream/core";

export interface KafkaConnection {
	kafka: unknown;
	producer: unknown | null;
	consumer: unknown | null;
	disconnect: () => Promise<void>;
}

export function kafkaConnect(options?: {
	brokers?: string[];
	clientId?: string;
	ssl?: unknown;
	sasl?: unknown;
	groupId?: string;
	/** Pass `false` to skip opening a producer. */
	producer?: Record<string, unknown> | false;
	consumer?: Record<string, unknown>;
	/** Injectable Kafka constructor; defaults to a lazy `import("kafkajs")`. */
	Kafka?: new (
		options: Record<string, unknown>,
	) => unknown;
	[key: string]: unknown;
}): Promise<KafkaConnection>;

export interface KafkaMessage {
	value: Uint8Array | string;
	key?: Uint8Array | string;
	partition?: number;
	headers?: Record<string, unknown>;
	timestamp?: string;
	[key: string]: unknown;
}

export function kafkaProduceStream(
	options: {
		producer: unknown;
		topic: string;
		batchSize?: number;
		acks?: number;
		compression?: number;
		timeout?: number;
	},
	streamOptions?: StreamOptions,
): Promise<DatastreamWritable<Uint8Array | string | KafkaMessage>>;

export interface KafkaConsumedMessage {
	topic: string;
	partition: number;
	offset: string;
	key: Uint8Array | null;
	value: Uint8Array | null;
	headers: Record<string, unknown>;
	timestamp: string;
}

export function kafkaConsumeStream(
	options: {
		consumer: unknown;
		topics: string | string[];
		fromBeginning?: boolean;
		/**
		 * Let kafkajs auto-commit offsets. Defaults to `false` (was `true`): an
		 * auto-commit happens once a message is buffered in the stream, not
		 * processed, so a crash loses buffered messages. Commit `offset + 1` via
		 * `consumer.commitOffsets` after processing, or pass `true` to opt back in.
		 */
		autoCommit?: boolean;
		partitionsConsumedConcurrently?: number;
	},
	streamOptions?: StreamOptions,
): Promise<
	DatastreamReadable<KafkaConsumedMessage> & { stop: () => Promise<void> }
>;
