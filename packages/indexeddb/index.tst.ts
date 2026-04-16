import {
	indexedDBConnect,
	indexedDBReadStream,
	indexedDBWriteStream,
} from "@datastream/indexeddb";
import { describe, expect, test } from "tstyche";

describe("indexedDBConnect", () => {
	test("is a function", () => {
		expect(indexedDBConnect).type.not.toBeAssignableTo<never>();
	});
});

describe("indexedDBReadStream", () => {
	test("accepts options and returns a promise", () => {
		const db = {} as unknown;
		expect(indexedDBReadStream({ db, store: "test" })).type.toBeAssignableTo<
			Promise<unknown>
		>();
	});

	test("accepts index option", () => {
		const db = {} as unknown;
		expect(
			indexedDBReadStream({ db, store: "test", index: "idx" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("indexedDBWriteStream", () => {
	test("accepts options and returns a promise", () => {
		const db = {} as unknown;
		expect(indexedDBWriteStream({ db, store: "test" })).type.toBeAssignableTo<
			Promise<unknown>
		>();
	});
});
