import _default, { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";
import { describe, expect, test } from "tstyche";

describe("ipfsGetStream", () => {
	test("accepts node and cid", () => {
		expect(
			ipfsGetStream({ node: {}, cid: "QmTest" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("ipfsAddStream", () => {
	test("accepts options", () => {
		expect(
			ipfsAddStream({ node: {}, resultKey: "cid" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("default export", () => {
	test("has getStream", () => {
		expect(_default).type.not.toBeAssignableTo<never>();
	});
});
