import type { IpfsNode } from "@datastream/ipfs";
import _default, { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";
import { describe, expect, test } from "tstyche";

const mockNode: IpfsNode = {
	get(_cid: string) {
		return {};
	},
	async add(_data: unknown[]) {
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
	test("accepts options", () => {
		expect(
			ipfsAddStream({
				node: mockNode,
				resultKey: "cid",
			}),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("default export", () => {
	test("has getStream", () => {
		expect(_default.getStream).type.toBe<typeof ipfsGetStream>();
	});

	test("has addStream", () => {
		expect(_default.addStream).type.toBe<typeof ipfsAddStream>();
	});
});
