// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream, createWritableStream } from "@datastream/core";
import { gcpDestroyWithFailedMessages } from "./client.js";

// gcpPubSubAckStream / gcpPubSubNackStream still open, per subscription. A
// graceful gcpPubSubSubscribeStream stop() waits for them before
// subscription.close(): close() nacks every message not yet acked (the SDK's
// default closeOptions), so closing first would redeliver what was processed.
const openAckStreams = new WeakMap();

// topic = @google-cloud/pubsub Topic. chunk = { data, attributes, orderingKey }
// (or { json }); string data is sent as UTF-8 (the SDK takes only a Buffer /
// Uint8Array).
export const gcpPubSubPublishStream = (
	{ topic, maxInFlight = 100 } = {},
	streamOptions,
) => {
	if (!topic) throw new TypeError("gcpPubSubPublishStream: topic required");
	if (!(maxInFlight >= 1)) {
		throw new TypeError("gcpPubSubPublishStream: maxInFlight must be >= 1");
	}
	// Unconfirmed publishes (publish promise -> chunk), oldest first. The SDK
	// batches internally (100 messages / 10ms by default), so awaiting each
	// publish would send one-message batches; keep up to maxInFlight outstanding.
	const inFlight = new Map();
	let stream;
	const publish = (chunk) => {
		const message =
			typeof chunk?.data === "string"
				? { ...chunk, data: Buffer.from(chunk.data) }
				: chunk;
		// Promise.try: publishMessage throws synchronously on invalid data.
		const promise = Promise.try(() => topic.publishMessage(message));
		inFlight.set(promise, chunk);
		promise.then(
			() => inFlight.delete(promise),
			(err) => stream.destroy(err),
		);
	};
	const write = (chunk) => {
		publish(chunk);
		if (inFlight.size >= maxInFlight) return inFlight.keys().next().value;
	};
	const final = () => Promise.all([topic.flush(), ...inFlight.keys()]);
	// In-flight publishes cannot be cancelled: on destroy/abort they are reported
	// as failed but may still be delivered, so a retry can duplicate them.
	stream = gcpDestroyWithFailedMessages(
		createWritableStream(write, final, streamOptions),
		() => [...inFlight.values()],
		"gcpPubSubPublishStream: destroyed with unconfirmed publishes",
	);
	return stream;
};

// subscription = @google-cloud/pubsub Subscription. Pub/Sub pushes, so the
// buffer is bounded only by the subscription's flowControl: set
// flowControl.maxMessages <= highWaterMark (and allowExcessMessages: false), and
// ack/nack downstream. An overflow errors the stream.
export const gcpPubSubSubscribeStream = (
	{ subscription } = {},
	streamOptions,
) => {
	if (!subscription) {
		throw new TypeError("gcpPubSubSubscribeStream: subscription required");
	}
	const stream = createReadableStream(undefined, streamOptions);
	// An already-aborted signal destroys it in the constructor: adding a
	// 'message' listener would open the subscription.
	if (stream.destroyed) return stream;

	let stopped = false;
	const onMessage = (message) => {
		if (stopped) return message.nack();
		try {
			stream.push(message);
		} catch (err) {
			// Buffer overflow: would otherwise throw inside the SDK's emit.
			stream.destroy(err);
		}
	};
	// Not after destroy(): push(null) would still mark the readable ended, so
	// finished()/pipeline would take the teardown for a clean end and hang.
	const end = () => {
		stopped = true;
		if (!stream.destroyed) stream.push(null);
	};
	const onError = (err) => stream.destroy(err);
	const onClose = () => {
		end();
		close().catch(() => {});
	};
	subscription.on("message", onMessage);
	subscription.on("error", onError);
	subscription.on("close", onClose);

	// Memoized; listeners are removed only once closed: removing the last
	// 'message' listener makes the SDK close (and nack) on its own, and an
	// 'error' during close must not go unhandled.
	let closing;
	const close = () => {
		closing ??= subscription.close().finally(() => {
			subscription.off("message", onMessage);
			subscription.off("error", onError);
			subscription.off("close", onClose);
		});
		return closing;
	};

	const destroy = stream._destroy.bind(stream);
	stream._destroy = (err, callback) => {
		stopped = true;
		if (stream.readableEnded) return destroy(err, callback);
		close()
			.catch((closeErr) => {
				err ??= closeErr;
			})
			// After stop(), let the ended readable emit 'end' first: finished()
			// takes an ended, empty readable that closes for a clean finish, but
			// pipe ends the destination only on 'end', so the pipeline would hang.
			.then(() => setImmediate(() => destroy(err, callback)));
	};

	stream.stop = async () => {
		end();
		await Promise.all(
			[...(openAckStreams.get(subscription) ?? [])].map(
				(ackStream) =>
					new Promise((resolve) => ackStream.once("close", resolve)),
			),
		);
		await close();
	};
	return stream;
};

const ackStream =
	(name, method) =>
	({ subscription, withResponse = false } = {}, streamOptions) => {
		if (!subscription) throw new TypeError(`${name}: subscription required`);
		const write = withResponse
			? (message) => message[`${method}WithResponse`]()
			: (message) => message[method]();
		const stream = createWritableStream(write, streamOptions);
		const open = openAckStreams.getOrInsertComputed(
			subscription,
			() => new Set(),
		);
		open.add(stream);
		stream.once("close", () => open.delete(stream));
		return stream;
	};

export const gcpPubSubAckStream = ackStream("gcpPubSubAckStream", "ack");
export const gcpPubSubNackStream = ackStream("gcpPubSubNackStream", "nack");
