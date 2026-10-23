// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { bench, suite } from "node:bench";
import { Readable, Writable } from "node:stream";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	gcpStorageListStream,
	gcpStorageReadStream,
	gcpStorageWriteStream,
} from "@datastream/gcp/storage";

const OPS = 5;
const options = { warmup: 1, samples: 30 };
const COUNT = 10_000;
const PAGE = 1_000;
const chunks = Array.from({ length: COUNT }, (_, i) => `chunk-${i}\n`);
const files = Array.from({ length: COUNT }, (_, i) => ({
	metadata: { name: `exports/${i}.csv`, size: "1024" },
}));

// Fake SDK objects: no I/O, so the numbers are the stream overhead only.
const bucket = {
	getFiles: async (query) => {
		const offset = query.pageToken ?? 0;
		const next = offset + PAGE;
		return [
			files.slice(offset, next),
			next < COUNT ? { ...query, pageToken: next } : null,
			{},
		];
	},
	file: () => ({
		createReadStream: () => Readable.from(chunks),
		createWriteStream: () =>
			new Writable({
				write(_chunk, _encoding, callback) {
					callback();
				},
			}),
	}),
};

suite("gcpStorage", () => {
	bench(`list ${COUNT} objects`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await streamToArray(gcpStorageListStream({ bucket, prefix: "exports/" }));
		}
		b.end(OPS);
	});

	bench(`read ${COUNT} chunks`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await streamToArray(gcpStorageReadStream({ bucket, name: "a.csv" }));
		}
		b.end(OPS);
	});

	bench(`write ${COUNT} chunks`, options, async (b) => {
		b.start();
		for (let op = 0; op < OPS; op++) {
			await pipeline([
				createReadableStream(chunks),
				gcpStorageWriteStream({ bucket, name: "a.csv" }),
			]);
		}
		b.end(OPS);
	});
});
