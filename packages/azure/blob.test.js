import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import { getEventListeners } from "node:events";
import { Readable } from "node:stream";
import test, { describe } from "node:test";
import * as azureModule from "@datastream/azure/blob";
import {
	azureBlobDownloadStream,
	azureBlobListStream,
	azureBlobUploadStream,
} from "@datastream/azure/blob";
import {
	createReadableStream,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import { variant } from "../variant.js";

describe(`@datastream/azure/blob (${variant})`, () => {
	test(`azureBlobDownloadStream streams the body`, async () => {
		let args;
		const client = {
			download: async (...a) => {
				args = a;
				return { readableStreamBody: Readable.from(["ab", "c"]) };
			},
		};
		const stream = await azureBlobDownloadStream({
			client,
			offset: 1,
			count: 2,
			conditions: {},
		});
		strictEqual(await streamToString(stream), "abc");
		deepStrictEqual(args, [1, 2, { conditions: {}, abortSignal: undefined }]);
	});

	test(`azureBlobDownloadStream throws without a body`, async () => {
		const client = { name: "blob", download: async () => ({}) };
		await rejects(azureBlobDownloadStream({ client }), (e) => {
			strictEqual(e.message, "Blob.download has no body");
			deepStrictEqual(e.cause, { name: "blob" });
			return true;
		});
	});

	test(`azureBlobDownloadStream destroys body on stream error`, async () => {
		const body = Readable.from(["a"]);
		const client = { download: async () => ({ readableStreamBody: body }) };
		const stream = await azureBlobDownloadStream({ client });
		stream.on("error", () => {});
		stream.destroy(new Error("x"));
		await new Promise((r) => setImmediate(r));
		ok(body.destroyed);
	});

	test(`azureBlobDownloadStream tolerates teardown throwing`, async () => {
		const body = Readable.from(["a"]);
		body.destroy = () => {
			throw new Error("nope");
		};
		const client = { download: async () => ({ readableStreamBody: body }) };
		const controller = new AbortController();
		await azureBlobDownloadStream(
			{ client },
			{ signal: controller.signal },
		).catch(() => {});
		controller.abort();
	});

	test(`azureBlobDownloadStream destroys body on abort`, async () => {
		const body = Readable.from(["a"]);
		const client = { download: async () => ({ readableStreamBody: body }) };
		const controller = new AbortController();
		const stream = await azureBlobDownloadStream(
			{ client },
			{ signal: controller.signal },
		);
		stream.on("error", () => {});
		controller.abort();
		ok(body.destroyed);
	});

	test(`azureBlobDownloadStream removes its abort listener once the stream closes`, async () => {
		const client = {
			download: async () => ({ readableStreamBody: Readable.from(["ab"]) }),
		};
		const controller = new AbortController();
		for (let i = 0; i < 3; i++) {
			const stream = await azureBlobDownloadStream(
				{ client },
				{ signal: controller.signal },
			);
			strictEqual(await streamToString(stream), "ab");
		}
		await new Promise((resolve) => setImmediate(resolve));
		strictEqual(getEventListeners(controller.signal, "abort").length, 0);
	});

	test(`azureBlobDownloadStream destroys body when already aborted`, async () => {
		const body = Readable.from(["a"]);
		const controller = new AbortController();
		const client = {
			download: async () => {
				controller.abort();
				return { readableStreamBody: body };
			},
		};
		const stream = await azureBlobDownloadStream(
			{ client },
			{ signal: controller.signal },
		);
		stream.on("error", () => {});
		ok(body.destroyed);
	});

	test(`azureBlobUploadStream uploads the piped body`, async () => {
		let args;
		let uploaded = "";
		const client = {
			uploadStream: async (stream, ...rest) => {
				args = rest;
				for await (const chunk of stream) uploaded += chunk;
				return {};
			},
		};
		const result = await pipeline([
			createReadableStream(["a", "b"]),
			azureBlobUploadStream({
				client,
				bufferSize: 8,
				maxConcurrency: 2,
				tags: { a: "b" },
			}),
		]);
		strictEqual(uploaded, "ab");
		deepStrictEqual(args, [8, 2, { tags: { a: "b" }, abortSignal: undefined }]);
		deepStrictEqual(result, {});
	});

	test(`azureBlobUploadStream surfaces the SDK error`, async () => {
		const client = {
			uploadStream: async () => {
				throw new Error("AuthorizationFailure");
			},
		};
		await rejects(
			pipeline([
				createReadableStream(["a"]),
				azureBlobUploadStream({ client }),
			]),
			{ message: "AuthorizationFailure" },
		);
	});

	// A failed upload stops reading the body; without forwarding the error the
	// writer would sit on backpressure forever instead of failing.
	test(`azureBlobUploadStream destroys the stream when the upload fails`, {
		timeout: 1000,
	}, async () => {
		const client = {
			uploadStream: async () => {
				throw new Error("AuthorizationFailure");
			},
		};
		const stream = azureBlobUploadStream({ client });
		const errored = new Promise((resolve) => stream.once("error", resolve));
		strictEqual((await errored).message, "AuthorizationFailure");
		strictEqual(stream.destroyed, true);
		await rejects(stream.result(), { message: "AuthorizationFailure" });
	});

	test(`azureBlobListStream yields every blob item`, async () => {
		let args;
		const client = {
			listBlobsFlat: async function* (...a) {
				args = a;
				yield { name: "a/1" };
				yield { name: "a/2" };
			},
		};
		const stream = await azureBlobListStream({
			client,
			prefix: "a/",
			includeMetadata: true,
		});
		deepStrictEqual(await streamToArray(stream), [
			{ name: "a/1" },
			{ name: "a/2" },
		]);
		// An internal signal, aborted only if the stream is destroyed early.
		const { abortSignal } = args[0];
		strictEqual(abortSignal instanceof AbortSignal, true);
		strictEqual(abortSignal.aborted, false);
		deepStrictEqual(args, [
			{ prefix: "a/", includeMetadata: true, abortSignal },
		]);
	});

	test(`azureBlobListStream requires a client`, async () => {
		await rejects(azureBlobListStream({ prefix: "a/" }), (e) => {
			strictEqual(e instanceof TypeError, true);
			strictEqual(e.message, "azureBlobListStream: client required");
			return true;
		});
	});

	// Backpressure parks the generator at a yield: destroy must reach the SDK
	// iterator's return() so it stops paging.
	test(`azureBlobListStream destroy calls the iterator's return()`, {
		timeout: 1000,
	}, async () => {
		let returned = false;
		let abortSignal;
		let i = 0;
		const iterator = {
			[Symbol.asyncIterator]() {
				return this;
			},
			next: async () => ({ value: { name: `${i++}` }, done: false }),
			return: async () => {
				returned = true;
				return { done: true };
			},
		};
		const client = {
			listBlobsFlat: (options) => {
				abortSignal = options.abortSignal;
				return iterator;
			},
		};
		const stream = await azureBlobListStream({ client });
		const first = await new Promise((resolve) => stream.once("data", resolve));
		stream.pause();
		deepStrictEqual(first, { name: "0" });
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(returned, true);
		strictEqual(abortSignal.aborted, true);
	});

	// A destroy while a page request is in flight aborts that request so the
	// stream can close instead of waiting on the network.
	test(`azureBlobListStream destroy aborts an in-flight page request`, {
		timeout: 1000,
	}, async () => {
		let abortSignal;
		const client = {
			listBlobsFlat: async function* (options) {
				abortSignal = options.abortSignal;
				await new Promise((_resolve, reject) =>
					abortSignal.addEventListener("abort", () =>
						reject(abortSignal.reason),
					),
				);
			},
		};
		const stream = await azureBlobListStream({ client });
		stream.resume();
		await new Promise((resolve) => setImmediate(resolve));
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(abortSignal.aborted, true);
	});

	test(`azureBlobListStream forwards streamOptions.signal`, async () => {
		let abortSignal;
		const client = {
			listBlobsFlat: async function* (options) {
				abortSignal = options.abortSignal;
				yield { name: "a" };
			},
		};
		const controller = new AbortController();
		const stream = await azureBlobListStream(
			{ client },
			{ signal: controller.signal },
		);
		deepStrictEqual(await streamToArray(stream), [{ name: "a" }]);
		strictEqual(abortSignal.aborted, false);
		controller.abort();
		strictEqual(abortSignal.aborted, true);
	});

	test(`blob has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
