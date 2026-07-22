// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import {
	GetRecordsCommand,
	KinesisClient,
	PutRecordsCommand,
} from "@aws-sdk/client-kinesis";
import { createWritableStream, timeout } from "@datastream/core";
import { awsClientDefaults } from "./client.js";

// PutRecords limits: <=500 records, <=5 MiB aggregate, <=1 MiB per record.
const KINESIS_MAX_RECORDS = 500;
const KINESIS_MAX_RECORD_BYTES = 1024 * 1024; // 1 MiB
// 5 MiB aggregate with headroom for request framing.
const KINESIS_MAX_BATCH_BYTES = 5 * 1024 * 1024 - 64 * 1024;

// Partial failures are overwhelmingly throttling-driven
// (ProvisionedThroughputExceeded); a near-zero early delay (3^0 == 1ms) just
// hammers the throttled stream. Apply a floor so the first retries give
// capacity time to recover, while preserving the ~59sec cap (3^10).
const BACKOFF_FLOOR_MS = 50;
const BACKOFF_CAP_MS = 3 ** 10;
// streamOptions is always supplied by the exported stream function (defaulting
// to {}), so it is never nullish here.
const backoff = (retryCount, streamOptions) =>
	timeout(
		Math.min(BACKOFF_CAP_MS, Math.max(BACKOFF_FLOOR_MS, 3 ** retryCount)),
		{
			signal: streamOptions.signal,
		},
	);

const _textEncoder = new TextEncoder();
const _byteLength = (value) =>
	typeof value === "string"
		? _textEncoder.encode(value).byteLength
		: value.byteLength;

const recordByteLength = (record) => {
	let bytes = 0;
	if (record.Data != null) {
		bytes += _byteLength(record.Data);
	}
	if (record.PartitionKey != null) {
		bytes += _byteLength(record.PartitionKey);
	}
	if (record.ExplicitHashKey != null) {
		bytes += _byteLength(record.ExplicitHashKey);
	}
	return bytes;
};

let client = new KinesisClient(awsClientDefaults);
export const awsKinesisSetClient = (kinesisClient) => {
	client = kinesisClient;
};

export const awsKinesisGetRecordsStream = async (
	options,
	streamOptions = {},
) => {
	const { pollingActive, pollingDelay = 1000, ...kinesisOptions } = options;
	async function* command(opts) {
		let expectMore = true;
		while (expectMore) {
			const response = await client.send(new GetRecordsCommand(opts), {
				abortSignal: streamOptions.signal,
			});
			const records = response.Records ?? [];
			for (const item of records) {
				yield item;
			}
			opts.ShardIterator = response.NextShardIterator;
			expectMore =
				opts.ShardIterator !== null && (pollingActive || records.length > 0);
			if (pollingActive && records.length === 0 && pollingDelay > 0) {
				// Abortable idle wait: rejects immediately and clears the timer
				// when streamOptions.signal aborts mid-delay.
				await timeout(pollingDelay, { signal: streamOptions.signal });
			}
		}
	}
	return command({ ...kinesisOptions });
};

export const awsKinesisPutRecordsStream = (options, streamOptions = {}) => {
	const { retryMaxCount = 10, ...putOptions } = options;
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
			const response = await client.send(
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
			if (retryCount >= retryMaxCount) {
				throw new Error("awsKinesisPutRecords has failed records", {
					cause: results.filter((result) => result.ErrorCode),
				});
			}
			await backoff(retryCount, streamOptions);
			retryCount++;
			records = failed;
		}
	};
	const write = async (chunk) => {
		const chunkBytes = recordByteLength(chunk);
		if (chunkBytes > KINESIS_MAX_RECORD_BYTES) {
			throw new Error("awsKinesisPutRecords record exceeds 1MiB limit", {
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

export default {
	setClient: awsKinesisSetClient,
	getRecordsStream: awsKinesisGetRecordsStream,
	putRecordsStream: awsKinesisPutRecordsStream,
};
