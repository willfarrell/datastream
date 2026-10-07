// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	GetRecordsCommand,
	KinesisClient,
	PutRecordsCommand,
} from "@aws-sdk/client-kinesis";
import { createWritableStream, timeout } from "@datastream/core";
import { awsBackoff, awsClientDefaults } from "./client.js";

// PutRecords limits: <=500 records, <=5 MiB aggregate, <=1 MiB per record.
const KINESIS_MAX_RECORDS = 500;
const KINESIS_MAX_RECORD_BYTES = 1024 * 1024; // 1 MiB
// 5 MiB aggregate with headroom for request framing.
const KINESIS_MAX_BATCH_BYTES = 5 * 1024 * 1024 - 64 * 1024;

// Buffer.byteLength measures strings (UTF-8) and binary Data (Uint8Array /
// Buffer / ArrayBuffer) alike; an absent field counts as zero bytes.
const recordByteLength = (record) =>
	Buffer.byteLength(record.Data ?? "") +
	Buffer.byteLength(record.PartitionKey ?? "") +
	Buffer.byteLength(record.ExplicitHashKey ?? "");

// Created on first use, so importing the module (or always passing a per-call
// client) never constructs an unused SDK client.
let defaultClient;
const getDefaultClient = () =>
	(defaultClient ??= new KinesisClient(awsClientDefaults));
export const awsKinesisSetClient = (kinesisClient) => {
	defaultClient = kinesisClient;
};

export const awsKinesisGetRecordsStream = async (
	options,
	streamOptions = {},
) => {
	const {
		pollingActive,
		pollingDelay = 1000,
		client,
		...kinesisOptions
	} = options;
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await (client ?? getDefaultClient()).send(
				new GetRecordsCommand(opts),
				{ abortSignal: streamOptions.signal },
			);
			const records = response.Records ?? [];
			for (const item of records) {
				yield item;
			}
			// SDK v3 returns NextShardIterator as undefined (not null) for a closed
			// shard; normalise so either value ends the loop.
			opts.ShardIterator = response.NextShardIterator ?? null;
			// An empty page is not the end while MillisBehindLatest > 0: Kinesis can
			// return no records while the iterator is still behind the tip.
			expectMore =
				opts.ShardIterator !== null &&
				(pollingActive ||
					records.length > 0 ||
					response.MillisBehindLatest > 0);
			// Wait before re-reading after an empty page, whether idle polling or
			// catching up (MillisBehindLatest > 0): back-to-back empty GetRecords
			// calls exceed Kinesis' 5 calls/s/shard limit
			// (ProvisionedThroughputExceeded). No wait once the stream is ending.
			if (expectMore && records.length === 0 && pollingDelay > 0) {
				// Abortable idle wait: rejects immediately and clears the timer
				// when streamOptions.signal aborts mid-delay.
				await timeout(pollingDelay, { signal: streamOptions.signal });
			}
		}
	}
	return command({ ...kinesisOptions });
};

export const awsKinesisPutRecordsStream = (options, streamOptions = {}) => {
	const { retryMaxCount = 10, client, ...putOptions } = options;
	let batch = [];
	let batchBytes = 0;
	const send = async () => {
		if (!batch.length) {
			return;
		}
		let records = batch;
		batch = [];
		batchBytes = 0;
		let retryCount = 0;
		while (true) {
			const response = await (client ?? getDefaultClient()).send(
				new PutRecordsCommand({ ...putOptions, Records: records }),
				{ abortSignal: streamOptions.signal },
			);
			if (!response.FailedRecordCount) {
				return;
			}
			// Retry only the records whose result entry carries an ErrorCode. When
			// the response omits the per-record Records array there is nothing to
			// inspect, so there is nothing to retry.
			const results = response.Records;
			if (!results) {
				return;
			}
			const failed = records.filter(
				(_record, index) => results[index]?.ErrorCode,
			);
			if (!failed.length) {
				return;
			}
			// null = retry without limit
			if (retryCount >= (retryMaxCount ?? Number.POSITIVE_INFINITY)) {
				throw new Error("awsKinesisPutRecords has failed records", {
					cause: results.filter((result) => result.ErrorCode),
				});
			}
			await awsBackoff(retryCount, streamOptions);
			retryCount++;
			records = failed;
		}
	};
	const write = async (chunk) => {
		const chunkBytes = recordByteLength(chunk);
		if (chunkBytes > KINESIS_MAX_RECORD_BYTES) {
			throw new RangeError("awsKinesisPutRecords record exceeds 1MiB limit", {
				cause: { bytes: chunkBytes, limit: KINESIS_MAX_RECORD_BYTES },
			});
		}
		if (
			batch.length === KINESIS_MAX_RECORDS ||
			(batch.length && batchBytes + chunkBytes > KINESIS_MAX_BATCH_BYTES)
		) {
			await send();
		}
		batch.push(chunk);
		batchBytes += chunkBytes;
	};
	const final = () => send();
	return createWritableStream(write, final, streamOptions);
};
