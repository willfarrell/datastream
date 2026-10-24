/// <reference lib="dom" />
/// <reference types="node" />
import type { DatastreamPassThrough, StreamResult } from "@datastream/core";
import type { IpfsAbortOptions, IpfsNode } from "@datastream/ipfs";
import { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";
import { describe, expect, test } from "tstyche";

const mockNode: IpfsNode = {
	get(_cid: string) {
		return {};
	},
	async add(_data: AsyncIterable<unknown>) {
		return { cid: "QmTest" };
	},
};

describe("ipfsGetStream", () => {
	test("accepts node and cid", () => {
		expect(
			ipfsGetStream({ node: mockNode, cid: "QmTest" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("ipfsAddStream", () => {
	test("requires node", () => {
		expect(ipfsAddStream).type.not.toBeCallableWith();
		expect(ipfsAddStream).type.not.toBeCallableWith({ resultKey: "cid" });
	});

	test("node.add receives an async iterable", () => {
		expect<Parameters<IpfsNode["add"]>[0]>().type.toBe<
			AsyncIterable<unknown>
		>();
	});

	test("node.add and node.get receive AbortOptions", () => {
		expect<Parameters<IpfsNode["add"]>[1]>().type.toBe<
			IpfsAbortOptions | undefined
		>();
		expect<Parameters<IpfsNode["get"]>[1]>().type.toBe<
			IpfsAbortOptions | undefined
		>();
		expect<IpfsAbortOptions["signal"]>().type.toBe<AbortSignal | undefined>();
	});

	test("returns a pass-through with a cid result", () => {
		type Stream = Awaited<ReturnType<typeof ipfsAddStream>>;
		expect<Stream>().type.toBeAssignableTo<DatastreamPassThrough>();
		expect<ReturnType<Stream["result"]>>().type.toBe<StreamResult<string>>();
	});

	test("accepts options", () => {
		expect(
			ipfsAddStream({
				node: mockNode,
				resultKey: "cid",
			}),
		).type.not.toBeAssignableTo<never>();
	});
});
