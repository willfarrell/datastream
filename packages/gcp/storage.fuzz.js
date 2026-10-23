// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, strictEqual } from "node:assert";
import { Readable, Writable } from "node:stream";
import test from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
	streamToString,
} from "@datastream/core";
import {
	gcpStorageListStream,
	gcpStorageReadStream,
	gcpStorageWriteStream,
} from "@datastream/gcp/storage";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"gcpStorageReadStream: name required",
		"gcpStorageWriteStream: name required",
		"upload mismatch",
	];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

const fakeBucket = ({ readable, writable }) => {
	const bucket = {
		calls: [],
		file: (name) => {
			bucket.calls.push(["file", name]);
			return {
				createReadStream: (options) => {
					bucket.calls.push(["createReadStream", options]);
					return readable;
				},
				createWriteStream: (options) => {
					bucket.calls.push(["createWriteStream", options]);
					return writable;
				},
			};
		},
	};
	return bucket;
};

// Like File.createWriteStream(): 'finish' only after the commit, or the SDK
// error (e.g. FILE_NO_UPLOAD on a checksum mismatch).
const fakeUpload = ({ highWaterMark, fail }) => {
	const upload = new Writable({
		highWaterMark,
		write(chunk, _encoding, callback) {
			upload.chunks.push(chunk.toString());
			setImmediate(callback);
		},
		final(callback) {
			setImmediate(() => {
				if (fail) return callback(new Error("upload mismatch"));
				upload.committed = true;
				callback();
			});
		},
	});
	upload.chunks = [];
	upload.committed = false;
	return upload;
};

const name = fc.oneof(
	fc.string({ maxLength: 40 }),
	fc.string({ unit: "binary", maxLength: 40 }),
	fc.anything(),
);
// Read/write options; bucket and name are taken out before they reach the SDK.
const extraOptions = fc.dictionary(
	fc.string({ maxLength: 10 }).filter((k) => k !== "bucket" && k !== "name"),
	fc.anything(),
	{ maxKeys: 5 },
);

// ---------------------------------------------------------------------------
// fuzz: gcpStorageReadStream / gcpStorageWriteStream name + options
// ---------------------------------------------------------------------------

// Any non-empty string name reaches bucket.file() unchanged (object names are
// arbitrary UTF-8; the service validates them), everything else is rejected
// before the SDK is touched. The remaining options are handed over as is.
test("fuzz gcpStorageReadStream w/ random name and options", async () => {
	await fc.assert(
		fc.asyncProperty(name, extraOptions, async (name, options) => {
			const bucket = fakeBucket({ readable: Readable.from(["ab"]) });
			try {
				const stream = gcpStorageReadStream({ ...options, bucket, name });
				strictEqual(await streamToString(stream), "ab");
				deepStrictEqual(bucket.calls, [
					["file", name],
					["createReadStream", { ...options }],
				]);
			} catch (e) {
				deepStrictEqual(bucket.calls, []);
				catchError({ name, options }, e);
			}
		}),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [
				["", {}],
				["../a", {}],
			],
		},
	);
});

test("fuzz gcpStorageWriteStream w/ random name and options", async () => {
	await fc.assert(
		fc.asyncProperty(name, extraOptions, async (name, options) => {
			const writable = fakeUpload({});
			const bucket = fakeBucket({ writable });
			try {
				await pipeline([
					createReadableStream(["a"]),
					gcpStorageWriteStream({ ...options, bucket, name }),
				]);
				strictEqual(writable.committed, true);
				deepStrictEqual(bucket.calls, [
					["file", name],
					["createWriteStream", { ...options }],
				]);
			} catch (e) {
				deepStrictEqual(bucket.calls, []);
				catchError({ name, options }, e);
			}
		}),
		{ numRuns: 1_000, verbose: 2 },
	);
});

// ---------------------------------------------------------------------------
// fuzz: gcpStorageWriteStream chunks / backpressure / commit failure
// ---------------------------------------------------------------------------

// Every chunk reaches the SDK in order, and the pipeline resolves only when
// the object is committed, otherwise it rejects with the SDK error.
test("fuzz gcpStorageWriteStream w/ random chunks", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.string({ minLength: 1, maxLength: 20 }), { maxLength: 30 }),
			fc.integer({ min: 1, max: 64 }),
			fc.boolean(),
			async (chunks, highWaterMark, fail) => {
				const writable = fakeUpload({ highWaterMark, fail });
				try {
					await pipeline([
						createReadableStream(chunks),
						gcpStorageWriteStream({
							bucket: fakeBucket({ writable }),
							name: "a",
						}),
					]);
					strictEqual(fail, false);
					strictEqual(writable.committed, true);
				} catch (e) {
					strictEqual(fail, true);
					strictEqual(writable.committed, false);
					catchError(chunks, e);
				}
				deepStrictEqual(writable.chunks, chunks);
			},
		),
		{ numRuns: 500, verbose: 2 },
	);
});

// ---------------------------------------------------------------------------
// fuzz: gcpStorageListStream paging
// ---------------------------------------------------------------------------

// getFiles({ autoPaginate: false }) fake: [files, nextQuery, apiResponse].
const pagedBucket = (pages) => {
	const bucket = {
		queries: [],
		getFiles: async (query) => {
			bucket.queries.push(query);
			const index = query.pageToken ?? 0;
			const files = pages[index].map((n) => ({ metadata: { name: n } }));
			const nextQuery =
				index + 1 < pages.length ? { ...query, pageToken: index + 1 } : null;
			return [files, nextQuery, {}];
		},
	};
	return bucket;
};

// Every listed object is yielded once, in order, one request per page; a
// destroy stops paging (at most the page being consumed plus one ahead).
test("fuzz gcpStorageListStream w/ random pages and destroy", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.array(fc.string({ maxLength: 10 }), { maxLength: 5 }), {
				minLength: 1,
				maxLength: 10,
			}),
			fc.option(fc.string({ maxLength: 10 })),
			fc.option(fc.nat({ max: 30 })),
			async (pages, prefix, destroyAfter) => {
				const bucket = pagedBucket(pages);
				const options = prefix === null ? { bucket } : { bucket, prefix };
				const stream = gcpStorageListStream(options);
				const names = pages.flat();
				if (destroyAfter === null) {
					const items = await streamToArray(stream);
					deepStrictEqual(
						items.map((item) => item.name),
						names,
					);
					strictEqual(bucket.queries.length, pages.length);
				} else {
					const seen = [];
					await new Promise((resolve) => {
						stream.on("close", resolve);
						stream.on("data", (item) => {
							seen.push(item.name);
							if (seen.length >= destroyAfter) stream.destroy();
						});
						if (destroyAfter === 0) stream.destroy();
					});
					deepStrictEqual(seen, names.slice(0, seen.length));
					strictEqual(bucket.queries.length <= pages.length, true);
				}
				for (const query of bucket.queries) {
					strictEqual(query.autoPaginate, false);
					strictEqual(query.prefix, prefix ?? undefined);
				}
			},
		),
		{ numRuns: 500, verbose: 2 },
	);
});
