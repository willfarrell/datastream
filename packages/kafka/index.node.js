// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { Readable } from "node:stream";
import { createWritableStream } from "@datastream/core";

export const kafkaConnect = async ({
	brokers,
	clientId,
	ssl,
	sasl,
	groupId,
	producer: producerOptions,
	consumer: consumerOptions,
	// Injectable Kafka constructor (defaults to kafkajs). Lets callers/tests
	// supply a stub without reaching for a real broker; falls back to the lazy
	// dynamic import so the production path is unchanged.
	Kafka,
	...kafkaOptions
} = {}) => {
	Kafka ??= (await import("kafkajs")).Kafka;
	const kafka = new Kafka({ brokers, clientId, ssl, sasl, ...kafkaOptions });
	// Open a producer only if it wasn't explicitly disabled. Open a consumer
	// only when a groupId is provided. Either or both may be null.
	const producer =
		producerOptions === false ? null : kafka.producer(producerOptions ?? {});
	const consumer = groupId
		? kafka.consumer({ groupId, ...(consumerOptions ?? {}) })
		: null;
	// Connect under a guard: if the second connect fails, the first is already
	// connected (open socket + background metadata/heartbeat timers) with no
	// disconnect handle escaping to the caller. Tear down whatever connected
	// before rethrowing so the error path leaks nothing.
	try {
		if (producer) await producer.connect();
		if (consumer) await consumer.connect();
	} catch (err) {
		await producer?.disconnect().catch(() => {});
		await consumer?.disconnect().catch(() => {});
		throw err;
	}
	return {
		kafka,
		producer,
		consumer,
		disconnect: async () => {
			if (producer) await producer.disconnect();
			if (consumer) await consumer.disconnect();
		},
	};
};

const normalizeMessage = (chunk) => {
	if (chunk instanceof Uint8Array || typeof chunk === "string") {
		return { value: chunk };
	}
	if (chunk && typeof chunk === "object" && "value" in chunk) {
		return chunk;
	}
	// Note: `chunk` is never null here — Node.js Writable streams reject null
	// with ERR_STREAM_NULL_VALUES before the write callback fires — so we report
	// `typeof chunk` directly without a dead `chunk === null` branch.
	throw new TypeError(
		`kafkaProduceStream: chunk must be Uint8Array, string, or { value, ... } object (got ${typeof chunk})`,
	);
};

export const kafkaProduceStream = async (
	{
		producer,
		topic,
		batchSize = 100,
		acks = -1,
		compression,
		timeout: requestTimeout,
	} = {},
	streamOptions = {},
) => {
	if (!producer) throw new TypeError("kafkaProduceStream: producer required");
	if (!topic) throw new TypeError("kafkaProduceStream: topic required");
	if (!(batchSize >= 1)) {
		throw new TypeError("kafkaProduceStream: batchSize must be >= 1");
	}

	let batch = [];
	const flush = async () => {
		if (!batch.length) return;
		// Snapshot then start a fresh batch *before* the send. Two reasons:
		//   1. The snapshot stays addressable on the error path so callers can
		//      re-queue via `err.failedMessages` instead of losing the batch.
		//   2. If kafkajs mutates `messages` (or holds a reference), we don't want
		//      our next write() to interleave with what's in flight.
		const sending = batch;
		batch = [];
		try {
			await producer.send({
				topic,
				messages: sending,
				acks,
				compression,
				timeout: requestTimeout,
			});
		} catch (err) {
			err.failedMessages = sending;
			throw err;
		}
	};
	const write = async (chunk) => {
		batch.push(normalizeMessage(chunk));
		if (batch.length >= batchSize) await flush();
	};
	const final = async () => {
		await flush();
	};
	const stream = createWritableStream(write, final, streamOptions);
	// Node's final() runs only on a graceful end(), never on destroy() or an
	// AbortSignal-triggered teardown. Without this, a partial batch buffered
	// below batchSize would be silently dropped on abort — no send, no error.
	// Surface the un-sent batch on the teardown error (mirroring flush()'s
	// err.failedMessages contract) so callers can recover/retry it instead of
	// losing it. We wrap the default Writable destroy rather than replacing it.
	const originalDestroy = stream._destroy.bind(stream);
	stream._destroy = (err, cb) => {
		if (batch.length) {
			// The stream is being torn down: no further write()/final() can run, so
			// `batch` is never read again. Hand the buffered messages straight to the
			// error (mirroring flush()'s err.failedMessages contract) without a dead
			// reassignment.
			err ??= new Error("kafkaProduceStream: destroyed with a buffered batch");
			err.failedMessages = batch;
		}
		originalDestroy(err, cb);
	};
	return stream;
};

export const kafkaConsumeStream = async (
	{
		consumer,
		topics,
		fromBeginning = false,
		autoCommit = true,
		partitionsConsumedConcurrently = 1,
		signal,
	},
	streamOptions = {},
) => {
	if (!consumer) throw new TypeError("kafkaConsumeStream: consumer required");
	if (!topics || (Array.isArray(topics) && !topics.length)) {
		throw new TypeError("kafkaConsumeStream: topics required");
	}

	const topicList = Array.isArray(topics) ? topics : [topics];
	for (const topic of topicList) {
		await consumer.subscribe({ topic, fromBeginning });
	}

	// Backpressure: kafkajs awaits each eachMessage callback before fetching the
	// next message on the same partition (and per-partition concurrency caps
	// fan-in). We construct a Node Readable and let `.push` return false when
	// the buffer is full; we then await the next `_read` (consumer pulled) before
	// returning from eachMessage. That suspends kafkajs's loop end-to-end so
	// backpressure reaches the broker instead of bursting through.
	// With partitionsConsumedConcurrently > 1, several eachMessage callbacks can
	// be parked on backpressure at the same time. Track every waiting resolver
	// (not just the latest) and release them all on the next `_read`; a single
	// slot would drop all but the last waiter and hang those partition loops.
	const readResolvers = new Set();
	const waitForRead = () =>
		new Promise((resolve) => {
			readResolvers.add(resolve);
		});
	const releaseReaders = () => {
		// Snapshot then clear before resolving so a resolver that synchronously
		// re-parks lands in a fresh Set. Iterating an empty Set is already a no-op,
		// so no explicit size guard is needed.
		const pending = [...readResolvers];
		readResolvers.clear();
		for (const resolve of pending) resolve();
	};

	let stopped = false;
	let runPromise;
	// Memoize consumer.stop() so every teardown path (explicit stop(), destroy(),
	// abort) shares ONE invocation. kafkajs's consumer.stop() is not guaranteed
	// safe to call twice concurrently, and stop()+later-destroy() previously
	// called it twice. Best-effort: swallow errors, run at most once.
	let consumerStopPromise;
	const stopConsumerOnce = () => {
		consumerStopPromise ??= Promise.resolve()
			.then(() => consumer.stop())
			.catch(() => {});
		return consumerStopPromise;
	};

	const stream = new Readable({
		objectMode: true,
		highWaterMark: streamOptions.highWaterMark ?? 100,
		read() {
			releaseReaders();
		},
		destroy(err, cb) {
			stopped = true;
			// Drop the abort listener: a destroyed stream never needs to react to
			// the signal again, and on long-lived/shared signals an un-removed
			// listener accumulates per stream.
			if (signal) signal.removeEventListener("abort", stop);
			// Release any eachMessage callbacks parked on backpressure first, so
			// they observe `stopped` and return. consumer.stop() waits for in-flight
			// callbacks to settle; without this release it would wait on a parked
			// callback that itself can only resume once `_read` fires — a circular
			// wait that deadlocks. Symmetric with stop().
			releaseReaders();
			// Best-effort consumer.stop on destroy, memoized so a prior stop() (or
			// a concurrent abort) does not trigger a second consumer.stop().
			stopConsumerOnce().then(() => cb(err));
		},
	});

	runPromise = consumer.run({
		autoCommit,
		partitionsConsumedConcurrently,
		eachMessage: async ({ topic, partition, message }) => {
			if (stopped) return;
			const wantsMore = stream.push({
				topic,
				partition,
				offset: message.offset,
				key: message.key,
				value: message.value,
				headers: message.headers,
				timestamp: message.timestamp,
			});
			if (!wantsMore) await waitForRead();
		},
	});

	const stop = async () => {
		// No early-out on a repeat call is needed: every step below is idempotent —
		// consumer.stop() is memoized via stopConsumerOnce(), removeEventListener is
		// a no-op once removed, stream.push(null) is a no-op after EOF, and
		// releaseReaders() is a no-op with no parked readers. Setting `stopped`
		// first still makes parked eachMessage callbacks observe it and return.
		stopped = true;
		// Detach from the signal on every normal termination. `{ once: true }`
		// only fires-and-removes when the signal actually aborts; for a normal
		// stop()/drain the listener would otherwise linger forever on a
		// long-lived/shared signal, accumulating one listener per consume stream.
		// `stop` is the exact handler reference, so this removes cleanly.
		if (signal) signal.removeEventListener("abort", stop);
		await stopConsumerOnce();
		stream.push(null);
		await runPromise.catch(() => {});
		// Unblock any pending eachMessage awaits so they can observe `stopped`
		// and return.
		releaseReaders();
	};

	if (signal) {
		if (signal.aborted) await stop();
		else signal.addEventListener("abort", stop);
	}

	stream.stop = stop;
	return stream;
};

export default {
	connect: kafkaConnect,
	produceStream: kafkaProduceStream,
	consumeStream: kafkaConsumeStream,
};
