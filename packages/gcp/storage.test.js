import { deepStrictEqual, rejects, strictEqual, throws } from "node:assert";
import { getEventListeners } from "node:events";
import { Readable, Writable } from "node:stream";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import * as gcpModule from "@datastream/gcp/storage";
import {
	gcpStorageListStream,
	gcpStorageReadStream,
	gcpStorageWriteStream,
} from "@datastream/gcp/storage";
import { variant } from "../variant.js";

const settle = async () => {
	for (let i = 0; i < 10; i++) {
		await new Promise((resolve) => setImmediate(resolve));
	}
};

// Bucket.file(name) -> File with createReadStream/createWriteStream.
const fakeBucket = ({ readable, writable } = {}) => {
	const bucket = {
		calls: [],
		file(...args) {
			bucket.calls.push(["file", ...args]);
			return {
				createReadStream(...a) {
					bucket.calls.push(["createReadStream", ...a]);
					return readable;
				},
				createWriteStream(...a) {
					bucket.calls.push(["createWriteStream", ...a]);
					return writable;
				},
			};
		},
	};
	return bucket;
};

// Mirrors File.createWriteStream(): a byte Writable whose final() holds
// 'finish' until the upload is committed (object metadata received and the
// checksum validated), or fails with the SDK error (e.g. FILE_NO_UPLOAD).
const fakeUpload = ({ error, highWaterMark } = {}) => {
	const upload = new Writable({
		highWaterMark,
		write(chunk, _encoding, callback) {
			upload.chunks.push(chunk.toString());
			upload.maxBuffered = Math.max(upload.maxBuffered, upload.writableLength);
			setImmediate(callback);
		},
		final(callback) {
			upload.finalCalled = true;
			setTimeout(() => {
				if (error) return callback(error);
				upload.committed = true;
				callback();
			}, 5);
		},
	});
	upload.chunks = [];
	upload.maxBuffered = 0;
	upload.finalCalled = false;
	upload.committed = false;
	return upload;
};

const assertTypeError = (fn, message) =>
	throws(fn, (e) => {
		strictEqual(e instanceof TypeError, true);
		strictEqual(e.message, message);
		return true;
	});

describe(`@datastream/gcp/storage (${variant})`, () => {
	// *** gcpStorageReadStream *** //
	test(`gcpStorageReadStream streams the object bytes`, async () => {
		const readable = Readable.from(["ab", "c"]);
		const bucket = fakeBucket({ readable });
		const stream = gcpStorageReadStream({
			bucket,
			name: "a/b.csv",
			start: 1,
			end: 2,
			validation: false,
		});
		strictEqual(stream, readable);
		strictEqual(await streamToString(stream), "abc");
		deepStrictEqual(bucket.calls, [
			["file", "a/b.csv"],
			["createReadStream", { start: 1, end: 2, validation: false }],
		]);
	});

	test(`gcpStorageReadStream requires a bucket`, () => {
		assertTypeError(
			() => gcpStorageReadStream({ name: "a" }),
			"gcpStorageReadStream: bucket required",
		);
	});

	test(`gcpStorageReadStream requires a name`, () => {
		const bucket = fakeBucket({ readable: Readable.from([]) });
		for (const name of [undefined, "", 1]) {
			assertTypeError(
				() => gcpStorageReadStream({ bucket, name }),
				"gcpStorageReadStream: name required",
			);
		}
		deepStrictEqual(bucket.calls, []);
	});

	test(`gcpStorageReadStream destroys the SDK stream on abort`, async () => {
		const readable = new Readable({ read() {} });
		const controller = new AbortController();
		const stream = gcpStorageReadStream(
			{ bucket: fakeBucket({ readable }), name: "a" },
			{ signal: controller.signal },
		);
		const errored = new Promise((resolve) => stream.once("error", resolve));
		controller.abort();
		strictEqual(readable.destroyed, true);
		strictEqual((await errored).name, "AbortError");
	});

	test(`gcpStorageReadStream destroys the SDK stream when already aborted`, async () => {
		const readable = new Readable({ read() {} });
		const stream = gcpStorageReadStream(
			{ bucket: fakeBucket({ readable }), name: "a" },
			{ signal: AbortSignal.abort() },
		);
		stream.on("error", () => {});
		strictEqual(readable.destroyed, true);
	});

	test(`gcpStorageReadStream removes its abort listener once the stream closes`, async () => {
		const controller = new AbortController();
		for (let i = 0; i < 3; i++) {
			const stream = gcpStorageReadStream(
				{ bucket: fakeBucket({ readable: Readable.from(["ab"]) }), name: "a" },
				{ signal: controller.signal },
			);
			strictEqual(await streamToString(stream), "ab");
		}
		await settle();
		strictEqual(getEventListeners(controller.signal, "abort").length, 0);
	});

	// *** gcpStorageWriteStream *** //
	test(`gcpStorageWriteStream resolves only once the upload is committed`, async () => {
		const writable = fakeUpload();
		const bucket = fakeBucket({ writable });
		const result = await pipeline([
			createReadableStream(["a", "b", "c"]),
			gcpStorageWriteStream({
				bucket,
				name: "out.csv",
				resumable: false,
				contentType: "text/csv",
				metadata: { metadata: { k: "v" } },
			}),
		]);
		deepStrictEqual(result, {});
		strictEqual(writable.committed, true);
		deepStrictEqual(writable.chunks, ["a", "b", "c"]);
		deepStrictEqual(bucket.calls, [
			["file", "out.csv"],
			[
				"createWriteStream",
				{
					resumable: false,
					contentType: "text/csv",
					metadata: { metadata: { k: "v" } },
				},
			],
		]);
	});

	test(`gcpStorageWriteStream surfaces the SDK validation error`, async () => {
		const error = Object.assign(new Error("upload mismatch"), {
			code: "FILE_NO_UPLOAD",
		});
		const writable = fakeUpload({ error });
		await rejects(
			pipeline([
				createReadableStream(["a"]),
				gcpStorageWriteStream({ bucket: fakeBucket({ writable }), name: "a" }),
			]),
			(e) => e === error,
		);
		strictEqual(writable.committed, false);
	});

	// e.g. the resumable session could not be created: the SDK destroys its
	// write stream on its own while the wrapper sits idle.
	test(`gcpStorageWriteStream fails when the SDK stream errors on its own`, async () => {
		const writable = fakeUpload();
		const stream = gcpStorageWriteStream({
			bucket: fakeBucket({ writable }),
			name: "a",
		});
		const errored = new Promise((resolve) => stream.once("error", resolve));
		const error = new Error("401 Unauthorized");
		writable.destroy(error);
		strictEqual(await errored, error);
		strictEqual(stream.destroyed, true);
	});

	test(`gcpStorageWriteStream fails when an SDK write fails`, async () => {
		const error = new Error("write failed");
		const writable = new Writable({
			write(_chunk, _encoding, callback) {
				callback(error);
			},
		});
		await rejects(
			pipeline([
				createReadableStream(["a", "b"]),
				gcpStorageWriteStream({ bucket: fakeBucket({ writable }), name: "a" }),
			]),
			(e) => e === error,
		);
	});

	test(`gcpStorageWriteStream honours the SDK stream's backpressure`, async () => {
		const writable = fakeUpload({ highWaterMark: 1 });
		// One byte each, so a highWaterMark of 1 refuses every write until 'drain'.
		const chunks = Array.from({ length: 20 }, (_, i) =>
			String.fromCharCode(97 + i),
		);
		await pipeline([
			createReadableStream(chunks),
			gcpStorageWriteStream({ bucket: fakeBucket({ writable }), name: "a" }),
		]);
		deepStrictEqual(writable.chunks, chunks);
		strictEqual(writable.maxBuffered, 1);
	});

	test(`gcpStorageWriteStream abort destroys the upload without committing`, async () => {
		const writable = fakeUpload();
		const controller = new AbortController();
		const stream = gcpStorageWriteStream(
			{ bucket: fakeBucket({ writable }), name: "a" },
			{ signal: controller.signal },
		);
		// A source that never ends, so the upload is still open on abort.
		const source = createReadableStream();
		source.push("a");
		const done = rejects(
			pipeline([source, stream]),
			(e) => e.name === "AbortError",
		);
		await settle();
		controller.abort();
		await done;
		await settle();
		deepStrictEqual(writable.chunks, ["a"]);
		strictEqual(writable.destroyed, true);
		strictEqual(writable.finalCalled, false);
		strictEqual(writable.committed, false);
	});

	test(`gcpStorageWriteStream destroy() destroys the upload without committing`, async () => {
		const writable = fakeUpload();
		const stream = gcpStorageWriteStream({
			bucket: fakeBucket({ writable }),
			name: "a",
		});
		stream.write("a");
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(writable.destroyed, true);
		strictEqual(writable.errored, null);
		strictEqual(writable.finalCalled, false);
	});

	test(`gcpStorageWriteStream requires a bucket and a name`, () => {
		assertTypeError(
			() => gcpStorageWriteStream({ name: "a" }),
			"gcpStorageWriteStream: bucket required",
		);
		const bucket = fakeBucket({ writable: fakeUpload() });
		for (const name of [undefined, "", 1]) {
			assertTypeError(
				() => gcpStorageWriteStream({ bucket, name }),
				"gcpStorageWriteStream: name required",
			);
		}
		deepStrictEqual(bucket.calls, []);
	});

	// *** gcpStorageListStream *** //
	// Mirrors Bucket.getFiles({ autoPaginate: false }) (promisified): resolves
	// [files, nextQuery, apiResponse]; nextQuery is a copy of the query plus
	// the next pageToken, or null on the last page. Each file is a File with
	// .metadata = the API item, unless `fields` is set (then the raw item).
	const pagedBucket = (pages) => {
		const bucket = {
			queries: [],
			getFiles: async (query) => {
				bucket.queries.push(query);
				const index = query.pageToken ?? 0;
				const items = pages[index];
				const files = query.fields
					? items
					: items.map((item) => ({ name: item.name, metadata: item }));
				const next = index + 1 < pages.length;
				const nextQuery = next ? { ...query, pageToken: index + 1 } : null;
				return [files, nextQuery, { items }];
			},
		};
		return bucket;
	};

	test(`gcpStorageListStream yields each object's metadata across pages`, async () => {
		const bucket = pagedBucket([
			[{ name: "a/1" }, { name: "a/2" }],
			[],
			[{ name: "a/3" }],
		]);
		const options = { bucket, prefix: "a/", maxResults: 2 };
		const stream = gcpStorageListStream(options);
		deepStrictEqual(await streamToArray(stream), [
			{ name: "a/1" },
			{ name: "a/2" },
			{ name: "a/3" },
		]);
		const query = { prefix: "a/", maxResults: 2, autoPaginate: false };
		deepStrictEqual(bucket.queries, [
			query,
			{ ...query, pageToken: 1 },
			{ ...query, pageToken: 2 },
		]);
		deepStrictEqual(options, { bucket, prefix: "a/", maxResults: 2 });
	});

	test(`gcpStorageListStream forces autoPaginate off`, async () => {
		const bucket = pagedBucket([[{ name: "a" }]]);
		await streamToArray(gcpStorageListStream({ bucket, autoPaginate: true }));
		deepStrictEqual(bucket.queries, [{ autoPaginate: false }]);
	});

	test(`gcpStorageListStream yields the raw items when fields is set`, async () => {
		const bucket = pagedBucket([[{ name: "a", metadata: { k: "v" } }]]);
		const stream = gcpStorageListStream({ bucket, fields: "items(name)" });
		deepStrictEqual(await streamToArray(stream), [
			{ name: "a", metadata: { k: "v" } },
		]);
	});

	test(`gcpStorageListStream requires a bucket`, () => {
		assertTypeError(
			() => gcpStorageListStream({ prefix: "a/" }),
			"gcpStorageListStream: bucket required",
		);
	});

	// Backpressure parks the generator at a yield: destroy must stop paging.
	test(`gcpStorageListStream destroy stops paging`, {
		timeout: 1000,
	}, async () => {
		let calls = 0;
		const bucket = {
			getFiles: async (query) => {
				calls++;
				const files = Array.from({ length: 100 }, (_, i) => ({
					metadata: { name: `${calls}-${i}` },
				}));
				return [files, { ...query, pageToken: calls }, {}];
			},
		};
		const stream = gcpStorageListStream({ bucket });
		const first = await new Promise((resolve) => stream.once("data", resolve));
		stream.pause();
		deepStrictEqual(first, { name: "1-0" });
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		await settle();
		strictEqual(calls, 1);
	});

	// getFiles takes no AbortSignal: a destroy while a page request is in
	// flight must still close the stream instead of waiting on the network.
	test(`gcpStorageListStream destroy does not wait on an in-flight page`, {
		timeout: 1000,
	}, async () => {
		const bucket = { getFiles: () => new Promise(() => {}) };
		const stream = gcpStorageListStream({ bucket });
		stream.resume();
		await settle();
		const closed = new Promise((resolve) => stream.once("close", resolve));
		stream.destroy();
		await closed;
		strictEqual(stream.errored, null);
	});

	test(`gcpStorageListStream abort does not wait on an in-flight page`, {
		timeout: 1000,
	}, async () => {
		const bucket = { getFiles: () => new Promise(() => {}) };
		const controller = new AbortController();
		const stream = gcpStorageListStream(
			{ bucket },
			{ signal: controller.signal },
		);
		await rejects(
			(async () => {
				const done = streamToArray(stream);
				await settle();
				controller.abort();
				await done;
			})(),
			(e) => e.name === "AbortError",
		);
	});

	test(`storage has no default export`, () => {
		strictEqual(Object.hasOwn(gcpModule, "default"), false);
	});
});
