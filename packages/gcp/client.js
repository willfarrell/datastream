// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { createReadableStream } from "@datastream/core";

// Readable over a paging/polling generator. Destroying a Readable.from stream
// only reaches the generator's return() after its next yield, so an idle poll
// (empty pages, no items) would keep running and the stream would never emit
// 'close'. Destroy also aborts the in-flight request / idle wait via the
// signal handed to `command(signal)` (combined with streamOptions.signal).
export const gcpReadableStream = (command, streamOptions) => {
	const controller = new AbortController();
	const signal = streamOptions.signal
		? AbortSignal.any([streamOptions.signal, controller.signal])
		: controller.signal;
	const stream = createReadableStream(command(signal), streamOptions);
	const destroy = stream.destroy;
	stream.destroy = function (...args) {
		if (!this.readableEnded) controller.abort();
		return destroy.apply(this, args);
	};
	return stream;
};

// Node's final() runs only on a graceful end(), never on destroy() or an
// AbortSignal teardown, so a partly filled batch would be dropped silently.
export const gcpDestroyWithFailedMessages = (stream, pending, message) => {
	const destroy = stream._destroy.bind(stream);
	stream._destroy = (err, callback) => {
		const failedMessages = pending();
		if (failedMessages.length) {
			err ??= new Error(message);
			err.failedMessages = failedMessages;
		}
		destroy(err, callback);
	};
	return stream;
};
