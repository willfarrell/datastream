import type { IpfsNode } from "@datastream/ipfs";
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

	test("accepts options", () => {
		expect(
			ipfsAddStream({
				node: mockNode,
				resultKey: "cid",
			}),
		).type.not.toBeAssignableTo<never>();
	});
});
