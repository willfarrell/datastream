import { strictEqual } from "node:assert";
import test, { describe } from "node:test";
import { streamToArray } from "@datastream/core";
import { gcpReadableStream } from "./client.js";

describe("gcp client helpers", () => {
	// destroy() after a natural end must not abort the signal the command saw:
	// it may be shared with (or forwarded to) work that outlives the stream.
	test(`gcpReadableStream does not abort the command signal after a natural end`, async () => {
		let seen;
		const stream = gcpReadableStream(async function* (signal) {
			seen = signal;
			yield 1;
		}, {});
		strictEqual((await streamToArray(stream)).length, 1);
		stream.destroy();
		strictEqual(seen.aborted, false);
	});

	test(`gcpReadableStream aborts the command signal when destroyed early`, async () => {
		let seen;
		const stream = gcpReadableStream(async function* (signal) {
			seen = signal;
			yield 1;
			yield 2;
		}, {});
		for await (const _ of stream) break;
		strictEqual(seen.aborted, true);
	});
});
