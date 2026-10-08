import { deepStrictEqual, ok, rejects, strictEqual } from "node:assert";
import { getEventListeners } from "node:events";
import { Readable } from "node:stream";
import test, { describe } from "node:test";
import * as azureModule from "@datastream/azure/blob";
import {
	azureBlobDownloadStream,
	azureBlobUploadStream,
} from "@datastream/azure/blob";
import {
	createReadableStream,
	pipeline,
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

	test(`blob has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
