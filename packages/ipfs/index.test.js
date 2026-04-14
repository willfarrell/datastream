// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import {
	createReadableStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import ipfsDefault, { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** ipfsGetStream *** //
test(`${variant}: ipfsGetStream should return readable stream from node.get`, async () => {
	const chunks = ["chunk1", "chunk2", "chunk3"];
	const node = {
		get(_cid) {
			return createReadableStream(chunks);
		},
	};
	const stream = await ipfsGetStream({ node, cid: "QmTest123" });
	const output = await streamToArray(stream);
	deepStrictEqual(output, chunks);
});

test(`${variant}: ipfsGetStream should pass cid to node.get`, async () => {
	let receivedCid;
	const node = {
		get(cid) {
			receivedCid = cid;
			return createReadableStream(["data"]);
		},
	};
	await ipfsGetStream({ node, cid: "bafyTestCidV1" });
	strictEqual(receivedCid, "bafyTestCidV1");
});

test(`${variant}: ipfsGetStream should work in a pipeline`, async () => {
	const chunks = ["hello", "world"];
	const node = {
		get(_cid) {
			return createReadableStream(chunks);
		},
	};
	const streams = [await ipfsGetStream({ node, cid: "QmTest123" })];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);
	deepStrictEqual(output, chunks);
});

// *** ipfsAddStream *** //
test(`${variant}: ipfsAddStream should pipe data through and call node.add on flush`, async () => {
	const input = ["chunk1", "chunk2"];
	let addedData;
	const node = {
		async add(data) {
			addedData = data;
			return { cid: "QmResult123" };
		},
	};
	const streams = [createReadableStream(input), await ipfsAddStream({ node })];
	await pipeline(streams);
	const { key, value } = streams[1].result();
	strictEqual(key, "cid");
	strictEqual(value, "QmResult123");
	deepStrictEqual(addedData, input);
});

test(`${variant}: ipfsAddStream should use custom resultKey`, async () => {
	const node = {
		async add(_data) {
			return { cid: "QmResult123" };
		},
	};
	const streams = [
		createReadableStream(["data"]),
		await ipfsAddStream({ node, resultKey: "ipfsCid" }),
	];
	await pipeline(streams);
	const { key } = streams[1].result();
	strictEqual(key, "ipfsCid");
});

test(`${variant}: ipfsAddStream result should be collected by pipeline`, async () => {
	const node = {
		async add(_data) {
			return { cid: "bafyResult456" };
		},
	};
	const streams = [
		createReadableStream(["data"]),
		await ipfsAddStream({ node }),
	];
	const result = await pipeline(streams);
	strictEqual(result.cid, "bafyResult456");
});

// *** default export *** //
test(`${variant}: default export should include all stream functions`, () => {
	deepStrictEqual(Object.keys(ipfsDefault).sort(), ["addStream", "getStream"]);
});
