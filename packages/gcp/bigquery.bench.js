// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
import { EventEmitter } from "node:events";
import { PassThrough, Writable } from "node:stream";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	gcpBigQueryInsertStream,
	gcpBigQueryLoadStream,
	gcpBigQueryQueryStream,
} from "@datastream/gcp/bigquery";

const OPS = 5;
const options = { warmup: 1, samples: 30 };
const COUNT = 10_000;
const rows = Array.from({ length: COUNT }, (_, i) => ({
	id: i,
	name: `row-${i}`,
}));
const lines = rows.map((row) => `${JSON.stringify(row)}\n`);

// Fake SDK objects: no I/O, so the numbers are the stream overhead only.
const bigquery = {
	createQueryStream: () => {
		const stream = new PassThrough({ objectMode: true });
		for (const row of rows) stream.write(row);
		stream.end();
		return stream;
	},
};
const insertTable = { insert: async () => [{}] };
// createWriteStream: 'job' after the body ends, then 'complete' with the Job.
const loadTable = {
	createWriteStream: () => {
		const job = Object.assign(new EventEmitter(), {
			id: "job",
			metadata: { statistics: { load: { outputRows: `${COUNT}` } } },
		});
		const stream = new Writable({
			write: (_chunk, _encoding, callback) => callback(),
			final: (callback) => {
				stream.emit("job", job);
				stream.emit("complete", job);
				callback();
			},
		});
		return stream;
	},
};

suite("gcpBigQuery", () => {
	bench(`query ${COUNT} rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await streamToArray(
				await gcpBigQueryQueryStream({ bigquery, query: "q" }),
			);
		}
		b.end(OPS);
	});

	bench(`insert ${COUNT} rows`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await pipeline([
				createReadableStream(rows),
				gcpBigQueryInsertStream({ table: insertTable }),
			]);
		}
		b.end(OPS);
	});

	bench(`load ${COUNT} ndjson lines`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await pipeline([
				createReadableStream(lines),
				gcpBigQueryLoadStream({ table: loadTable }),
			]);
		}
		b.end(OPS);
	});
});
