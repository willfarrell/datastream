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
			const results = await Promise.allSettled([
				producer?.disconnect(),
				consumer?.disconnect(),
			]);
			const failed = results.find(({ status }) => status === "rejected");
			if (failed) throw failed.reason;
		},
	};
};

// kafkaCommitStreams still open, per consumer. A graceful kafkaConsumeStream
// stop() waits for them before consumer.stop(): kafkajs refuses commitOffsets
// once it has left the group, so stopping first would drop every final commit.
const openCommitStreams = new WeakMap();

// What kafkajs's commitOffsets throws while no consumer group exists: after a
// STOP (stop(), or a crash before its restart) until run()/restart rejoins.
const notInitialized = (err) =>
	err.name === "KafkaJSNonRetriableError" &&
	err.message ===
		"Consumer group was not initialized, consumer#run must be called first";

// kafkajs awaits eachMessage; the shared resolved promise keeps that contract
// (and `.then`) without allocating a promise per message.
const delivered = Promise.resolve();

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
	const write = (chunk) => {
		batch.push(normalizeMessage(chunk));
		if (batch.length >= batchSize) return flush();
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
		autoCommit = false,
		partitionsConsumedConcurrently = 1,
	},
	streamOptions = {},
) => {
	const { signal } = streamOptions;
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
	let readResolvers = [];
	const waitForRead = () =>
		new Promise((resolve) => {
			readResolvers.push(resolve);
		});
	const releaseReaders = () => {
		const pending = readResolvers;
		readResolvers = [];
		for (const resolve of pending) resolve();
	};

	let stopped = false;
	let runPromise;
	// Messages kafkajs hands over after stop() (already fetched, or still
	// streaming in while the pipeline drains) wait here until consumer.stop().
	// Resolving would mark them handled; rejecting while the group is live would
	// make kafkajs crash and restart the consumer mid-drain.
	const leaving = Promise.withResolvers();
	// Memoize consumer.stop() so every teardown path (explicit stop(), destroy(),
	// abort) shares ONE invocation. kafkajs's consumer.stop() is not guaranteed
	// safe to call twice concurrently, and stop()+later-destroy() previously
	// called it twice. Best-effort: swallow errors, run at most once.
	let consumerStopPromise;
	const stopConsumerOnce = () => {
		// Release the held messages first: runner.stop() waits for in-flight
		// eachMessage callbacks, so holding them across it would deadlock.
		leaving.resolve();
		consumerStopPromise ??= Promise.resolve()
			.then(() => consumer.stop())
			.catch(() => {});
		return consumerStopPromise;
	};
	const halt = () => {
		stopped = true;
		// Release any eachMessage callbacks parked on backpressure: kafkajs's
		// runner.stop() waits for in-flight eachMessage to settle, and a parked
		// callback can only resume via releaseReaders() — a circular wait that
		// deadlocks when the buffer is full.
		releaseReaders();
		// Detach from the signal on every termination. `{ once: true }` only
		// fires-and-removes when the signal actually aborts; for a normal
		// stop()/drain the listener would otherwise linger forever on a
		// long-lived/shared signal, accumulating one listener per consume stream.
		if (signal) signal.removeEventListener("abort", abort);
	};

	const stream = new Readable({
		objectMode: true,
		highWaterMark: streamOptions.highWaterMark ?? 100,
		read() {
			releaseReaders();
		},
		destroy(err, cb) {
			halt();
			// A gracefully stopped readable auto-destroys as soon as it ends, before
			// downstream final() commits; stop() leaves the group once that is done.
			if (stream.readableEnded) return cb(err);
			stopConsumerOnce().then(() => cb(err));
		},
	});

	runPromise = consumer.run({
		autoCommit,
		partitionsConsumedConcurrently,
		eachMessage: ({ topic, partition, message }) => {
			// Never resolve for a message we did not push: kafkajs would commit its
			// offset (autoCommit) or treat it as handled, silently losing it. Throw
			// (once the group is being left) so it stays uncommitted and is
			// redelivered on the next run.
			if (stopped) {
				return leaving.promise.then(() => {
					throw new Error(
						`kafkaConsumeStream: stream stopped; message at ${topic}[${partition}]@${message.offset} not delivered`,
					);
				});
			}
			const wantsMore = stream.push({
				topic,
				partition,
				offset: message.offset,
				key: message.key,
				value: message.value,
				headers: message.headers,
				timestamp: message.timestamp,
			});
			return wantsMore ? delivered : waitForRead();
		},
	});
	// run() can reject on its own (not connected, already running); surface it
	// as a stream error instead of an unhandled rejection on a stream that never
	// ends. Once stopped, stop() owns and swallows the rejection.
	runPromise.catch((err) => {
		if (!stopped) stream.destroy(err);
	});

	const stop = async () => {
		halt();
		stream.push(null);
		await Promise.all(
			[...(openCommitStreams.get(consumer) ?? [])].map(
				(commitStream) =>
					new Promise((resolve) => commitStream.once("close", resolve)),
			),
		);
		await stopConsumerOnce();
		await runPromise.catch(() => {});
	};
	const abort = async () => {
		halt();
		await stopConsumerOnce();
		stream.push(null);
		await runPromise.catch(() => {});
	};

	if (signal) {
		if (signal.aborted) await abort();
		else signal.addEventListener("abort", abort);
	}

	stream.stop = stop;
	return stream;
};

export const kafkaCommitStream = async (
	{ consumer, batchSize = 100 } = {},
	streamOptions = {},
) => {
	if (!consumer) throw new TypeError("kafkaCommitStream: consumer required");
	if (!(batchSize >= 1)) {
		throw new TypeError("kafkaCommitStream: batchSize must be >= 1");
	}

	// Next offset to commit (`offset + 1`, as BigInt: Kafka offsets are int64
	// strings) per topic-partition. Only ever raised within a group generation,
	// so a redelivered or out-of-order message never moves a commit backwards.
	const highest = new Map();
	const pending = new Map();
	let count = 0;
	// A (re)join starts a new generation: kafkajs refetches from the committed
	// offsets (which an operator may have reset backwards), and partitions may
	// now belong to another member, so committing what was pending could move
	// its offsets. Start over; the dropped tail is redelivered (at-least-once).
	const offJoin = consumer.on(consumer.events.GROUP_JOIN, () => {
		highest.clear();
		pending.clear();
	});

	const commit = async () => {
		count = 0;
		if (!pending.size) return;
		try {
			await consumer.commitOffsets([...pending.values()]);
		} catch (err) {
			// Refused while kafkajs has no group (between a crash and its restart,
			// or after stop()): keep the offsets for the next attempt. Anything
			// else errors the stream.
			if (notInitialized(err)) return;
			throw err;
		}
		pending.clear();
	};
	const write = ({ topic, partition, offset }) => {
		const key = `${topic}:${partition}`;
		const next = BigInt(offset) + 1n;
		if (next > (highest.get(key) ?? 0n)) {
			highest.set(key, next);
			pending.set(key, { topic, partition, offset: next.toString() });
		}
		if (++count >= batchSize) return commit();
	};
	const stream = createWritableStream(write, commit, streamOptions);
	const open = openCommitStreams.getOrInsertComputed(consumer, () => new Set());
	open.add(stream);
	stream.once("close", () => {
		open.delete(stream);
		offJoin();
	});
	return stream;
};
