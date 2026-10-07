/// <reference lib="dom" />
/// <reference types="node" />
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

describe("FetchOptions retry", () => {
	test("has retryMaxCount property", () => {
		expect<FetchOptions>().type.toHaveProperty("retryMaxCount");
		expect(
			fetchRateLimit({ url: "https://example.com", retryMaxCount: 3 }),
		).type.toBe<Promise<Response>>();
	});
});

describe("FetchOptions limits", () => {
	test("has retryAfterMax, maxPages and maxBodySize properties", () => {
		expect<FetchOptions>().type.toBeAssignableTo<{
			retryMaxCount?: number | null;
			retryAfterMax?: number | null;
			maxPages?: number | null;
			maxBodySize?: number | null;
		}>();
		expect(
			fetchRateLimit({
				url: "https://example.com",
				retryAfterMax: 5000,
				maxPages: 10,
				maxBodySize: null,
			}),
		).type.toBe<Promise<Response>>();
	});

	test("accepts null (unlimited) for every limit", () => {
		expect(fetchRateLimit).type.toBeCallableWith({
			url: "https://example.com",
			retryMaxCount: null,
			retryAfterMax: null,
			maxPages: null,
			maxBodySize: null,
		});
	});

	test("rejects a non-numeric maxPages", () => {
		expect(fetchRateLimit).type.not.toBeCallableWith({
			url: "https://example.com",
			maxPages: "10",
		});
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

	test("takes concurrency in options", () => {
		expect(fetchReadableStream).type.toBeCallableWith([
			{ url: "https://example.com", concurrency: 2 },
		]);
	});
});

describe("fetchWritableStream", () => {
	test("returns promise", () => {
		expect(
			fetchWritableStream({ url: "https://example.com" }),
		).type.toBeAssignableTo<Promise<unknown>>();
	});

	test("accepts resultKey", () => {
		expect(fetchWritableStream).type.toBeCallableWith({
			url: "https://example.com",
			resultKey: "upload",
		});
		expect(fetchWritableStream).type.not.toBeCallableWith({
			url: "https://example.com",
			resultKey: 1,
		});
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
