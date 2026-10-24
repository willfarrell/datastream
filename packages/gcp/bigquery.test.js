import { deepStrictEqual, rejects, strictEqual, throws } from "node:assert";
import { EventEmitter, once } from "node:events";
import { Transform, Writable } from "node:stream";
import test, { describe } from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import * as gcpModule from "@datastream/gcp/bigquery";
import {
	gcpBigQueryInsertStream,
	gcpBigQueryLoadStream,
	gcpBigQueryQueryStream,
} from "@datastream/gcp/bigquery";
import { variant } from "../variant.js";

const settle = async () => {
	for (let i = 0; i < 20; i++) {
		await new Promise((resolve) => setImmediate(resolve));
	}
};

const write = (stream, chunk) =>
	new Promise((resolve) => stream.write(chunk, resolve));

const errorOf = (stream) => once(stream, "error").then(([e]) => e);

const writeAll = async (stream, chunks) => {
	for (const chunk of chunks) {
		await write(stream, chunk);
	}
};

// Mirrors @google-cloud/paginator ResourceStream (what createQueryStream
// returns): an object-mode Transform whose _read requests a page, pushes its
// rows and, while push() keeps returning true and end() has not been called,
// immediately requests the next page via setImmediate.
class FakeResourceStream extends Transform {
	constructor(requestFn) {
		super({ objectMode: true });
		this._ended = false;
		this._reading = false;
		this._nextQuery = 0;
		this._requestFn = requestFn;
		this.ends = 0;
	}
	end(...args) {
		this.ends++;
		this._ended = true;
		return super.end(...args);
	}
	_read() {
		if (this._reading) return;
		this._reading = true;
		this._requestFn(this._nextQuery, (err, results, nextQuery) => {
			if (err) {
				this.destroy(err);
				return;
			}
			this._nextQuery = nextQuery;
			let more = true;
			for (const result of results) {
				if (this._ended) break;
				more = this.push(result);
			}
			if (!this._nextQuery) this.end();
			if (more && !this._ended) setImmediate(() => this._read());
			this._reading = false;
		});
	}
}

const fakeBigQuery = (pages) => {
	const bigquery = {
		calls: [],
		requests: 0,
		pending: null,
		createQueryStream(query) {
			bigquery.calls.push(query);
			bigquery.stream = new FakeResourceStream((page, callback) => {
				bigquery.requests++;
				const next = page + 1 < pages.length ? page + 1 : null;
				const reply = () =>
					setImmediate(() => callback(null, pages[page], next));
				if (pages[page] === null) {
					bigquery.pending = (rows) => {
						pages[page] = rows;
						reply();
					};
				} else {
					reply();
				}
			});
			return bigquery.stream;
		},
	};
	return bigquery;
};

describe(`@datastream/gcp/bigquery (${variant})`, () => {
	test(`gcpBigQueryQueryStream requires bigquery`, async () => {
		await rejects(gcpBigQueryQueryStream({ query: "q" }), {
			name: "TypeError",
			message: "gcpBigQueryQueryStream: bigquery required",
		});
	});

	test(`gcpBigQueryQueryStream yields every page's rows`, async () => {
		const bigquery = fakeBigQuery([[{ id: 1 }], [], [{ id: 2 }, { id: 3 }]]);
		const stream = await gcpBigQueryQueryStream({
			bigquery,
			query: "SELECT * FROM t WHERE a = @a",
			params: { a: 1 },
			wrapIntegers: true,
		});
		deepStrictEqual(await streamToArray(stream), [
			{ id: 1 },
			{ id: 2 },
			{ id: 3 },
		]);
		deepStrictEqual(bigquery.calls, [
			{
				query: "SELECT * FROM t WHERE a = @a",
				params: { a: 1 },
				wrapIntegers: true,
			},
		]);
		strictEqual(bigquery.requests, 3);
	});

	test(`gcpBigQueryQueryStream forwards SDK errors`, async () => {
		const error = new Error("Syntax error");
		const bigquery = {
			createQueryStream: () =>
				new FakeResourceStream((_page, callback) => callback(error)),
		};
		await rejects(
			streamToArray(await gcpBigQueryQueryStream({ bigquery, query: "q" })),
			error,
		);
	});

	test(`gcpBigQueryQueryStream destroy stops paging`, async () => {
		const bigquery = fakeBigQuery([
			[{ id: 1 }],
			null,
			[{ id: 2 }],
			[{ id: 3 }],
		]);
		const stream = await gcpBigQueryQueryStream({ bigquery, query: "q" });
		const [row] = await once(stream, "data");
		deepStrictEqual(row, { id: 1 });
		stream.pause();
		await settle();
		strictEqual(bigquery.requests, 2);
		const closed = once(stream, "close");
		stream.destroy();
		await closed;
		strictEqual(bigquery.stream.destroyed, true);
		bigquery.pending([]);
		await settle();
		strictEqual(bigquery.requests, 2);
	});

	test(`gcpBigQueryQueryStream abort destroys the SDK stream`, async () => {
		const bigquery = fakeBigQuery([[{ id: 1 }], null, [{ id: 2 }]]);
		const controller = new AbortController();
		const stream = await gcpBigQueryQueryStream(
			{ bigquery, query: "q" },
			{ signal: controller.signal },
		);
		stream.on("data", () => {});
		const error = errorOf(stream);
		await settle();
		controller.abort();
		strictEqual((await error).name, "AbortError");
		strictEqual(bigquery.stream.destroyed, true);
		strictEqual(bigquery.stream.ends, 1);
	});

	test(`gcpBigQueryQueryStream releases the abort listener at the end`, async () => {
		const bigquery = fakeBigQuery([[{ id: 1 }]]);
		const controller = new AbortController();
		const stream = await gcpBigQueryQueryStream(
			{ bigquery, query: "q" },
			{ signal: controller.signal },
		);
		deepStrictEqual(await streamToArray(stream), [{ id: 1 }]);
		const ends = bigquery.stream.ends;
		controller.abort();
		await settle();
		strictEqual(bigquery.stream.ends, ends);
	});

	test(`gcpBigQueryQueryStream with an aborted signal never queries`, async () => {
		const bigquery = fakeBigQuery([[{ id: 1 }]]);
		const stream = await gcpBigQueryQueryStream(
			{ bigquery, query: "q" },
			{ signal: AbortSignal.abort() },
		);
		await rejects(streamToArray(stream), { name: "AbortError" });
		strictEqual(bigquery.calls.length, 0);
	});

	// Mirrors Table#createWriteStream: bytes go to a multipart upload; only once
	// the body has ended does the upload response create the job ('job'); the
	// stream then stays corked until the job's 'complete' (emitted as
	// 'complete' with the Job, then 'finish'), and a job 'error' destroys it.
	// `respond()` delivers the upload response (the 'job' event), which the SDK
	// emits even if the stream was destroyed meanwhile.
	const fakeTable = ({ highWaterMark = 16384, autoRespond = true } = {}) => {
		const job = new EventEmitter();
		job.id = "job-1";
		job.cancels = 0;
		job.cancel = async () => {
			job.cancels++;
			await job.cancelGate;
			if (job.cancelError) throw job.cancelError;
			return [{}];
		};
		job.complete = () => {
			job.metadata = {
				status: { state: "DONE" },
				statistics: { load: { outputRows: "2", badRecords: "0" } },
			};
			job.emit("complete", job.metadata);
		};
		const table = { calls: [], chunks: [], job, writes: [] };
		table.createWriteStream = (metadata) => {
			table.calls.push(metadata);
			const sdk = new Writable({
				highWaterMark,
				write(chunk, _encoding, callback) {
					table.chunks.push(chunk.toString());
					table.writes.push(callback);
					if (!table.holdWrites) callback();
				},
				final(callback) {
					table.ended = true;
					table.uncork = callback;
					table.respond = () => {
						sdk.emit("job", job);
					};
					if (autoRespond) setImmediate(table.respond);
				},
			});
			// Registered before the caller's listeners, as in the SDK.
			sdk.on("job", () => {
				job
					.on("error", (err) => sdk.destroy(err))
					.on("complete", () => {
						sdk.emit("complete", job);
						table.uncork();
					});
			});
			table.sdk = sdk;
			return sdk;
		};
		return table;
	};

	test(`gcpBigQueryLoadStream requires table`, () => {
		throws(() => gcpBigQueryLoadStream({}), {
			name: "TypeError",
			message: "gcpBigQueryLoadStream: table required",
		});
	});

	test(`gcpBigQueryLoadStream finishes only once the load job completes`, async () => {
		const table = fakeTable();
		const stream = gcpBigQueryLoadStream({
			table,
			writeDisposition: "WRITE_APPEND",
		});
		deepStrictEqual(table.calls, [
			{
				sourceFormat: "NEWLINE_DELIMITED_JSON",
				writeDisposition: "WRITE_APPEND",
			},
		]);
		let finished = false;
		const result = pipeline([
			createReadableStream(['{"a":1}\n', '{"a":2}\n']),
			stream,
		]).then((value) => {
			finished = true;
			return value;
		});
		await settle();
		deepStrictEqual(table.chunks, ['{"a":1}\n', '{"a":2}\n']);
		strictEqual(table.ended, true);
		strictEqual(finished, false);
		table.job.complete();
		deepStrictEqual(await result, {
			bigqueryLoad: {
				jobId: "job-1",
				outputRows: 2,
				statistics: { load: { outputRows: "2", badRecords: "0" } },
			},
		});
		strictEqual(table.job.cancels, 0);
	});

	test(`gcpBigQueryLoadStream sourceFormat and resultKey`, async () => {
		const table = fakeTable();
		const stream = gcpBigQueryLoadStream({
			table,
			sourceFormat: "CSV",
			skipLeadingRows: 1,
			resultKey: "load",
		});
		deepStrictEqual(table.calls, [{ sourceFormat: "CSV", skipLeadingRows: 1 }]);
		const result = pipeline([createReadableStream(["a\n1\n"]), stream]);
		await settle();
		table.job.complete();
		deepStrictEqual(Object.keys(await result), ["load"]);
	});

	// job status.errorResult surfaces as a job 'error' (Job#poll_), which the
	// SDK turns into stream.destroy(err).
	test(`gcpBigQueryLoadStream rejects with the job error`, async () => {
		const table = fakeTable();
		const result = pipeline([
			createReadableStream(['{"a":1}\n']),
			gcpBigQueryLoadStream({ table }),
		]);
		await settle();
		const error = new Error("Provided Schema does not match Table");
		table.job.emit("error", error);
		await rejects(result, error);
		strictEqual(table.job.cancels, 0);
	});

	test(`gcpBigQueryLoadStream errors when the upload fails mid-stream`, async () => {
		const table = fakeTable();
		const stream = gcpBigQueryLoadStream({ table });
		await write(stream, "a\n");
		const error = new Error("401 Unauthorized");
		table.sdk.destroy(error);
		const [e] = await once(stream, "error");
		strictEqual(e, error);
	});

	test(`gcpBigQueryLoadStream waits for the upload to drain`, async () => {
		const table = fakeTable({ highWaterMark: 1 });
		table.holdWrites = true;
		const stream = gcpBigQueryLoadStream({ table });
		let written = false;
		stream.write("abc", () => {
			written = true;
		});
		await settle();
		strictEqual(written, false);
		table.writes[0]();
		await settle();
		strictEqual(written, true);
	});

	test(`gcpBigQueryLoadStream write rejects when the upload fails while draining`, async () => {
		const table = fakeTable({ highWaterMark: 1 });
		table.holdWrites = true;
		const stream = gcpBigQueryLoadStream({ table });
		const errors = [];
		stream.on("error", (e) => errors.push(e));
		const written = new Promise((resolve) => stream.write("abc", resolve));
		await settle();
		const error = new Error("socket hang up");
		table.sdk.destroy(error);
		strictEqual(await written, error);
		await settle();
		deepStrictEqual(errors, [error]);
	});

	test(`gcpBigQueryLoadStream abort before end creates no job`, async () => {
		const table = fakeTable();
		const controller = new AbortController();
		const stream = gcpBigQueryLoadStream(
			{ table },
			{ signal: controller.signal },
		);
		const error = errorOf(stream);
		await write(stream, "a\n");
		const sdkError = once(table.sdk, "error");
		controller.abort();
		strictEqual((await error).name, "AbortError");
		strictEqual((await sdkError)[0].name, "AbortError");
		strictEqual(table.sdk.destroyed, true);
		strictEqual(table.ended, undefined);
	});

	test(`gcpBigQueryLoadStream with an aborted signal never uploads`, async () => {
		const table = fakeTable();
		const stream = gcpBigQueryLoadStream(
			{ table },
			{ signal: AbortSignal.abort() },
		);
		strictEqual((await errorOf(stream)).name, "AbortError");
		await settle();
		strictEqual(table.sdk.destroyed, true);
		deepStrictEqual(table.chunks, []);
	});

	test(`gcpBigQueryLoadStream destroy() without an error tears the upload down`, async () => {
		const table = fakeTable();
		const stream = gcpBigQueryLoadStream({ table });
		await write(stream, "a\n");
		const closed = once(stream, "close");
		stream.destroy();
		await closed;
		strictEqual(table.sdk.destroyed, true);
		strictEqual(table.ended, undefined);
	});

	test(`gcpBigQueryLoadStream abort while the job runs cancels it`, async () => {
		const table = fakeTable();
		const controller = new AbortController();
		const result = pipeline([
			createReadableStream(["a\n"]),
			gcpBigQueryLoadStream({ table }, { signal: controller.signal }),
		]);
		await settle();
		strictEqual(table.job.listenerCount("complete"), 1);
		const gate = Promise.withResolvers();
		table.job.cancelGate = gate.promise;
		table.job.cancelError = new Error("404 Not found");
		let rejected = false;
		result.catch(() => {
			rejected = true;
		});
		controller.abort();
		await settle();
		strictEqual(table.job.cancels, 1);
		strictEqual(rejected, false);
		gate.resolve();
		await rejects(result, { name: "AbortError" });
		strictEqual(table.sdk.destroyed, true);
		strictEqual(table.job.listenerCount("complete"), 0);
	});

	test(`gcpBigQueryLoadStream cancels a job created after the abort`, async () => {
		const table = fakeTable({ autoRespond: false });
		table.job.cancelError = new Error("404 Not found");
		const controller = new AbortController();
		const result = pipeline([
			createReadableStream(["a\n"]),
			gcpBigQueryLoadStream({ table }, { signal: controller.signal }),
		]);
		await settle();
		strictEqual(table.ended, true);
		controller.abort();
		await rejects(result, { name: "AbortError" });
		strictEqual(table.job.cancels, 0);
		table.respond();
		await settle();
		strictEqual(table.job.cancels, 1);
		strictEqual(table.job.listenerCount("complete"), 0);
	});

	class PartialFailureError extends Error {
		constructor(errors) {
			super("no such field: b.");
			this.name = "PartialFailureError";
			this.errors = errors;
		}
	}

	const fakeInsertTable = (fail = () => {}) => {
		const table = {
			calls: [],
			inserted: [],
			insert: async (rows, options) => {
				table.calls.push([rows, options]);
				fail(rows, table.calls.length - 1);
				table.inserted.push(...rows);
				return [{ kind: "bigquery#tableDataInsertAllResponse" }];
			},
		};
		return table;
	};

	const rowsOf = (length) => Array.from({ length }, (_, i) => ({ id: i }));

	test(`gcpBigQueryInsertStream requires table`, () => {
		throws(() => gcpBigQueryInsertStream({}), {
			name: "TypeError",
			message: "gcpBigQueryInsertStream: table required",
		});
	});

	test(`gcpBigQueryInsertStream batches 500 rows per insert`, async () => {
		const table = fakeInsertTable();
		await pipeline([
			createReadableStream(rowsOf(1001)),
			gcpBigQueryInsertStream({
				table,
				skipInvalidRows: true,
				ignoreUnknownValues: true,
			}),
		]);
		deepStrictEqual(
			table.calls.map(([rows]) => rows.length),
			[500, 500, 1],
		);
		deepStrictEqual(table.calls[2], [
			[{ id: 1000 }],
			{ skipInvalidRows: true, ignoreUnknownValues: true },
		]);
		deepStrictEqual(table.inserted, rowsOf(1001));
	});

	test(`gcpBigQueryInsertStream custom batchSize`, async () => {
		const table = fakeInsertTable();
		await pipeline([
			createReadableStream(rowsOf(5)),
			gcpBigQueryInsertStream({ table, batchSize: 2 }),
		]);
		deepStrictEqual(
			table.calls.map(([rows]) => rows.length),
			[2, 2, 1],
		);
		deepStrictEqual(table.calls[0][1], {});
	});

	test(`gcpBigQueryInsertStream sends nothing for empty input`, async () => {
		const table = fakeInsertTable();
		await pipeline([
			createReadableStream([]),
			gcpBigQueryInsertStream({ table }),
		]);
		strictEqual(table.calls.length, 0);
	});

	test(`gcpBigQueryInsertStream batches by maxBatchBytes`, async () => {
		const table = fakeInsertTable();
		await pipeline([
			createReadableStream(Array.from({ length: 5 }, () => ({ a: "x" }))),
			gcpBigQueryInsertStream({ table, maxBatchBytes: 146 }),
		]);
		deepStrictEqual(
			table.calls.map(([rows]) => rows.length),
			[2, 2, 1],
		);
	});

	test(`gcpBigQueryInsertStream default maxBatchBytes is 9 MiB`, async () => {
		const table = fakeInsertTable();
		const row = { a: "x".repeat(99_928) };
		await pipeline([
			createReadableStream(Array.from({ length: 95 }, () => row)),
			gcpBigQueryInsertStream({ table }),
		]);
		deepStrictEqual(
			table.calls.map(([rows]) => rows.length),
			[94, 1],
		);
	});

	test(`gcpBigQueryInsertStream rejects an oversize row up front`, async () => {
		const table = fakeInsertTable();
		const stream = gcpBigQueryInsertStream({ table, maxBatchBytes: 73 });
		await write(stream, { a: "x" });
		const error = errorOf(stream);
		stream.write({ a: "é" });
		const e = await error;
		strictEqual(e.name, "RangeError");
		strictEqual(e.message, "gcpBigQueryInsertStream row exceeds maxBatchBytes");
		deepStrictEqual(e.cause, { bytes: 74, limit: 73 });
		deepStrictEqual(e.failedMessages, [{ a: "x" }]);
		strictEqual(table.calls.length, 0);
	});

	test(`gcpBigQueryInsertStream surfaces partial failures`, async () => {
		const failure = new PartialFailureError([
			{ row: { id: 1 }, errors: [{ reason: "invalid", message: "bad" }] },
		]);
		const table = fakeInsertTable((rows) => {
			if (rows.length === 2) throw failure;
		});
		const stream = gcpBigQueryInsertStream({ table, batchSize: 2 });
		const error = errorOf(stream);
		await writeAll(stream, rowsOf(3));
		const e = await error;
		strictEqual(e, failure);
		deepStrictEqual(e.errors[0].row, { id: 1 });
		deepStrictEqual(e.failedMessages, [{ id: 1 }, { id: 2 }]);
	});

	test(`gcpBigQueryInsertStream request failure returns the whole batch`, async () => {
		const failure = Object.assign(new Error("Not found: Table t"), {
			code: 404,
		});
		const table = fakeInsertTable(() => {
			throw failure;
		});
		const stream = gcpBigQueryInsertStream({ table, batchSize: 2 });
		const error = errorOf(stream);
		await writeAll(stream, rowsOf(3));
		const e = await error;
		strictEqual(e, failure);
		deepStrictEqual(e.failedMessages, rowsOf(3));
	});

	test(`gcpBigQueryInsertStream failure in final rejects pipeline`, async () => {
		const failure = new PartialFailureError([{ row: { id: 0 }, errors: [] }]);
		const table = fakeInsertTable(() => {
			throw failure;
		});
		await rejects(
			pipeline([
				createReadableStream(rowsOf(1)),
				gcpBigQueryInsertStream({ table }),
			]),
			(e) => {
				strictEqual(e, failure);
				deepStrictEqual(e.failedMessages, [{ id: 0 }]);
				return true;
			},
		);
	});

	test(`gcpBigQueryInsertStream destroy attaches buffered rows`, async () => {
		const table = fakeInsertTable();
		const stream = gcpBigQueryInsertStream({ table, batchSize: 2 });
		const error = errorOf(stream);
		await writeAll(stream, rowsOf(3));
		await settle();
		stream.destroy();
		const e = await error;
		strictEqual(
			e.message,
			"gcpBigQueryInsertStream destroyed with buffered rows",
		);
		deepStrictEqual(e.failedMessages, [{ id: 2 }]);
		deepStrictEqual(table.inserted, rowsOf(2));
	});

	test(`gcpBigQueryInsertStream abort attaches buffered rows without sending`, async () => {
		const table = fakeInsertTable();
		const controller = new AbortController();
		const stream = gcpBigQueryInsertStream(
			{ table },
			{ signal: controller.signal },
		);
		const error = errorOf(stream);
		await writeAll(stream, rowsOf(2));
		controller.abort();
		const e = await error;
		strictEqual(e.name, "AbortError");
		deepStrictEqual(e.failedMessages, rowsOf(2));
		strictEqual(table.calls.length, 0);
	});

	test(`gcpBigQueryInsertStream destroy before any write emits no error`, async () => {
		const stream = gcpBigQueryInsertStream({ table: fakeInsertTable() });
		let errored = false;
		stream.on("error", () => {
			errored = true;
		});
		const closed = once(stream, "close");
		stream.destroy();
		await closed;
		strictEqual(errored, false);
	});

	test(`bigquery has no default export`, () => {
		strictEqual(Object.hasOwn(gcpModule, "default"), false);
	});
});
