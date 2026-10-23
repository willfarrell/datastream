// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { once } from "node:events";
import { createWritableStream } from "@datastream/core";
import { gcpDestroyWithFailedMessages, gcpReadableStream } from "./client.js";

// insertAll: <=10 MB per HTTP request, 500 rows per request recommended.
// 9 MiB leaves room for the request envelope.
const INSERT_MAX_BATCH_BYTES = 9 * 1024 * 1024;
// Per-row envelope the SDK adds: {"insertId":"<uuid>","json":...},
const INSERT_ROW_OVERHEAD_BYTES = 64;

export const gcpBigQueryQueryStream = async (options, streamOptions = {}) => {
	const { bigquery, ...query } = options;
	if (!bigquery) {
		throw new TypeError("gcpBigQueryQueryStream: bigquery required");
	}
	async function* command(signal) {
		const rows = bigquery.createQueryStream(query);
		// ResourceStream keeps requesting pages (setImmediate after each one)
		// until its _ended flag, set by end(), is set; destroy() alone does not
		// stop it after an empty page. A page request already in flight cannot
		// be aborted; its rows are dropped (the loop checks _ended before
		// pushing). The iterator's return() then destroys the stream.
		const stop = () => rows.end();
		signal.addEventListener("abort", stop);
		try {
			yield* rows;
		} finally {
			signal.removeEventListener("abort", stop);
		}
	}
	return gcpReadableStream(command, streamOptions);
};

// Bytes in (e.g. after ndjsonFormatStream / csvFormatStream), one load job out.
// Table#createWriteStream uploads the body as a multipart jobs.insert; the
// upload response emits 'job', then the SDK keeps the stream corked until the
// job is DONE ('complete' with the Job) or failed (job 'error' -> destroy).
export const gcpBigQueryLoadStream = (options, streamOptions = {}) => {
	const {
		table,
		sourceFormat = "NEWLINE_DELIMITED_JSON",
		resultKey,
		...metadata
	} = options;
	if (!table) {
		throw new TypeError("gcpBigQueryLoadStream: table required");
	}
	const upload = table.createWriteStream({ sourceFormat, ...metadata });
	let job;
	let aborted = false;
	// Removing the 'complete' listeners stops the SDK's job polling
	// (Operation#listenForEvents_). A cancel is best effort (the job may
	// already have committed) and its failure is dropped: the stream is
	// already being torn down with its own error.
	const cancel = () => {
		job.removeAllListeners("complete");
		return job.cancel().catch(() => {});
	};
	upload.on("job", (created) => {
		job = created;
		if (aborted) cancel();
	});
	let settled = false;
	const done = new Promise((resolve, reject) => {
		upload.once("complete", resolve);
		upload.once("error", reject);
	}).finally(() => {
		settled = true;
	});
	done.catch((error) => stream.destroy(error));

	let value;
	const write = (chunk) => {
		if (!upload.write(chunk)) return once(upload, "drain");
	};
	const final = async () => {
		upload.end();
		const { id, metadata } = await done;
		value = {
			jobId: id,
			outputRows: Number(metadata.statistics.load.outputRows),
			statistics: metadata.statistics,
		};
	};
	// Torn down before the job finished. Before end() the multipart body never
	// completes, so BigQuery creates no job; after it, cancel the job.
	const abort = async (reason) => {
		if (settled) return;
		aborted = true;
		if (job) await cancel();
		upload.destroy(reason);
	};
	const stream = createWritableStream(write, final, {
		...streamOptions,
		abort,
	});
	stream.result = () => ({ key: resultKey ?? "bigqueryLoad", value });
	return stream;
};

// Streaming inserts (tabledata.insertAll): not atomic. Each batch commits on
// its own and a PartialFailureError can leave some rows of a batch inserted.
// Use gcpBigQueryLoadStream for all-or-nothing loads.
export const gcpBigQueryInsertStream = (options, streamOptions = {}) => {
	const {
		table,
		batchSize = 500,
		maxBatchBytes = INSERT_MAX_BATCH_BYTES,
		...insertOptions
	} = options;
	if (!table) {
		throw new TypeError("gcpBigQueryInsertStream: table required");
	}
	let batch = [];
	let batchBytes = 0;
	let failed = [];
	const send = async () => {
		const rows = batch;
		batch = [];
		batchBytes = 0;
		try {
			await table.insert(rows, insertOptions);
		} catch (error) {
			// PartialFailureError.errors = [{ row, errors }]: the rows that failed
			// (after the SDK's partialRetries); the rest were inserted.
			failed =
				error.name === "PartialFailureError"
					? error.errors.map(({ row }) => row)
					: rows;
			throw error;
		}
	};
	const write = (chunk) => {
		const bytes =
			Buffer.byteLength(JSON.stringify(chunk)) + INSERT_ROW_OVERHEAD_BYTES;
		if (bytes > maxBatchBytes) {
			throw new RangeError(
				"gcpBigQueryInsertStream row exceeds maxBatchBytes",
				{ cause: { bytes, limit: maxBatchBytes } },
			);
		}
		const sending =
			batch.length === batchSize || batchBytes + bytes > maxBatchBytes
				? send()
				: undefined;
		batch.push(chunk);
		batchBytes += bytes;
		return sending;
	};
	const final = () => (batch.length ? send() : undefined);
	// An insert in flight at teardown cannot be cancelled (table.insert takes
	// no signal), so its rows are not handed back.
	return gcpDestroyWithFailedMessages(
		createWritableStream(write, final, streamOptions),
		() => [...failed, ...batch],
		"gcpBigQueryInsertStream destroyed with buffered rows",
	);
};
