import type { FetchOptions } from "@datastream/fetch";
import {
	fetchRateLimit,
	fetchReadableStream,
	fetchRequestStream,
	fetchResponseStream,
	fetchSetDefaults,
	fetchWritableStream,
} from "@datastream/fetch";
import { describe, expect, test } from "tstyche";

describe("FetchOptions", () => {
	test("has url property", () => {
		expect<FetchOptions>().type.toBeAssignableTo<{ url?: string }>();
	});

	test("has pagination properties", () => {
		expect<FetchOptions>().type.toBeAssignableTo<{
			dataPath?: string | string[];
			nextPath?: string | string[];
			offsetParam?: string;
			offsetAmount?: number;
		}>();
	});
});

describe("fetchSetDefaults", () => {
	test("returns void", () => {
		expect(fetchSetDefaults({ rateLimit: 0.1 })).type.toBe<void>();
	});
});

describe("fetchReadableStream", () => {
	test("accepts single options", () => {
		expect(
			fetchReadableStream({ url: "https://example.com" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts array of options", () => {
		expect(
			fetchReadableStream([{ url: "https://example.com" }]),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("fetchWritableStream", () => {
	test("returns promise", () => {
		expect(
			fetchWritableStream({ url: "https://example.com" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});
});

describe("aliases", () => {
	test("fetchRequestStream is fetchWritableStream", () => {
		expect(fetchRequestStream).type.toBe<typeof fetchWritableStream>();
	});

	test("fetchResponseStream is fetchReadableStream", () => {
		expect(fetchResponseStream).type.toBe<typeof fetchReadableStream>();
	});
});

describe("fetchRateLimit", () => {
	test("returns Promise<Response>", () => {
		expect(fetchRateLimit({ url: "https://example.com" })).type.toBe<
			Promise<Response>
		>();
	});
});
