// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import ipfsDefault, { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// Mock IPFS node
const mockCid = "QmTest123";
const mockData = Buffer.from("mock ipfs data");
const createMockNode = () => ({
	get(_cid) {
		return mockData;
	},
	add(_stream) {
		return { cid: mockCid };
	},
});

// *** ipfsGetStream *** //
test(`${variant}: ipfsGetStream should return data from node.get`, async () => {
	const node = createMockNode();
	const result = await ipfsGetStream({ node, cid: mockCid });
	deepStrictEqual(result, mockData);
});

// *** ipfsAddStream *** //
test(`${variant}: ipfsAddStream should return stream with result`, async () => {
	const node = createMockNode();
	const stream = await ipfsAddStream({ node }, {});
	const { key, value } = stream.result();
	strictEqual(key, "cid");
	strictEqual(value, mockCid);
});

test(`${variant}: ipfsAddStream should use custom resultKey`, async () => {
	const node = createMockNode();
	const stream = await ipfsAddStream({ node, resultKey: "ipfsCid" }, {});
	const { key } = stream.result();
	strictEqual(key, "ipfsCid");
});

// *** default export *** //
test(`${variant}: default export should include all stream functions`, () => {
	deepStrictEqual(Object.keys(ipfsDefault).sort(), ["addStream", "getStream"]);
});
