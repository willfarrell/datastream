/* global Headers, Response */

import {
	deepStrictEqual,
	notStrictEqual,
	ok,
	rejects,
	strictEqual,
} from "node:assert";
import test, { afterEach, describe } from "node:test";
import {
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
	isWritable,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import * as fetchModule from "@datastream/fetch";
import {
	fetchResponseStream,
	fetchSetDefaults,
	fetchWritableStream,
} from "@datastream/fetch";
import { variant } from "../variant.js";

describe(`@datastream/fetch (${variant})`, () => {
	const mockResponses = {
		"https://example.org/csv": () =>
			new Response("a,b,c\n1,2,3", {
				status: 200,
				statusText: "OK",
				headers: new Headers({
					"Content-Type": "text/csv; charset=UTF-8",
				}),
			}),
		"https://example.org/csv?delimiter=_": () =>
			new Response("a_b_c\n1_2_3", {
				status: 200,
				statusText: "OK",
				headers: new Headers({
					"Content-Type": "text/csv; charset=UTF-8",
				}),
			}),
		"https://example.org/json-obj/1": () =>
			new Response(JSON.stringify({ key: "item", value: 1 }), {
				status: 200,
				statusText: "OK",
				headers: new Headers({
					"Content-Type": "application/json; charset=UTF-8",
				}),
			}),
		"https://example.org/json-obj/2": () =>
			new Response(JSON.stringify({ key: "item", value: 2 }), {
				status: 200,
				statusText: "OK",
				headers: new Headers({
					"Content-Type": "application/json; charset=UTF-8",
					Link: '<https://example.org/json-obj/3>; rel="next"',
				}),
			}),
		"https://example.org/json-obj/3": () =>
			new Response(JSON.stringify({ key: "item", value: 3 }), {
				status: 200,
				statusText: "OK",
				headers: new Headers({
					"Content-Type": "application/json; charset=UTF-8",
				}),
			}),
		"https://example.org/json-arr/1": () =>
			new Response(
				JSON.stringify({
					data: [
						{ key: "item", value: 1 },
						{ key: "item", value: 2 },
						{ key: "item", value: 3 },
					],
					next: "https://example.org/json-arr/2",
				}),
				{
					status: 200,
					statusText: "OK",
					headers: new Headers({
						"Content-Type": "application/json; charset=UTF-8",
					}),
				},
			),
		"https://example.org/json-arr/2": () =>
			new Response(
				JSON.stringify({
					data: [
						{ key: "item", value: 4 },
						{ key: "item", value: 5 },
						{ key: "item", value: 6 },
					],
					next: "",
				}),
				{
					status: 200,
					statusText: "OK",
					headers: new Headers({
						"Content-Type": "application/json; charset=UTF-8",
					}),
				},
			),
		[`https://example.org/json-arr?${new URLSearchParams({
			$limit: 3,
			$offset: 0,
		})}`]: () =>
			new Response(
				JSON.stringify({
					data: [
						{ key: "item", value: 1 },
						{ key: "item", value: 2 },
						{ key: "item", value: 3 },
					],
				}),
				{
					status: 200,
					statusText: "OK",
					headers: new Headers({
						"Content-Type": "application/json; charset=UTF-8",
					}),
				},
			),
		[`https://example.org/json-arr?${new URLSearchParams({
			$limit: 3,
			$offset: 3,
		})}`]: () =>
			new Response(
				JSON.stringify({
					data: [
						{ key: "item", value: 4 },
						{ key: "item", value: 5 },
					],
				}),
				{
					status: 200,
					statusText: "OK",
					headers: new Headers({
						"Content-Type": "application/json; charset=UTF-8",
					}),
				},
			),
		[`https://example.org/json-arr?${new URLSearchParams({
			$limit: 3,
			$offset: 6,
		})}`]: () =>
			new Response(JSON.stringify({ data: [] }), {
				status: 200,
				statusText: "OK",
				headers: new Headers({
					"Content-Type": "application/json; charset=UTF-8",
				}),
			}),
		"https://example.org/404": () =>
			new Response("", { status: 404, statusText: "Not Found" }),
		"https://example.org/429": () =>
			new Response("", { status: 429, statusText: "Too Many Requests" }),
	};
	global.fetch = (url, _request) => {
		const mockResponse = mockResponses[url]();
		if (mockResponse) {
			return Promise.resolve(mockResponse);
		}
		throw new Error("mock missing");
	};
	const suiteFetch = global.fetch;
	afterEach(() => {
		global.fetch = suiteFetch;
		fetchSetDefaults({
			rateLimit: 0.01,
			dataPath: undefined,
			nextPath: undefined,
			offsetParam: undefined,
			offsetAmount: undefined,
			maxPages: 10_000,
			maxBodySize: 16_777_216,
			retryMaxCount: 10,
			retryAfterMax: 60_000,
			method: "GET",
			headers: {
				Accept: "application/json",
				"Accept-Encoding": "br, gzip, deflate",
			},
		});
	});

	test(`built-in default method and headers are GET, application/json and br, gzip, deflate`, async (_t) => {
		const originalFetch = global.fetch;
		let capturedHeaders;
		let capturedMethod;
		global.fetch = async (_url, init) => {
			capturedHeaders = init.headers;
			capturedMethod = init.method;
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/builtin-accept", dataPath: "" },
			]);
			await streamToArray(stream);
			strictEqual(capturedMethod, "GET");
			strictEqual(capturedHeaders.Accept, "application/json");
			strictEqual(capturedHeaders["Accept-Encoding"], "br, gzip, deflate");
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should fetch csv`, async (_t) => {
		fetchSetDefaults({ headers: { Accept: "text/csv" } });
		const config = [{ url: "https://example.org/csv" }];
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			Uint8Array.from("a,b,c\n1,2,3".split("").map((x) => x.charCodeAt())),
		]);
	});

	test(`fetchResponseStream should fetch with qs`, async (_t) => {
		fetchSetDefaults({ headers: { Accept: "text/csv" } });
		const config = [{ url: "https://example.org/csv", qs: { delimiter: "_" } }];
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			Uint8Array.from("a_b_c\n1_2_3".split("").map((x) => x.charCodeAt())),
		]);
	});

	test(`fetchResponseStream appends qs to a url that already has a query`, async (_t) => {
		const originalFetch = global.fetch;
		const urls = [];
		global.fetch = async (url) => {
			urls.push(url);
			return new Response("ok", { status: 200 });
		};
		try {
			await streamToArray(
				fetchResponseStream({
					url: "https://example.org/q?a=1",
					qs: { b: "x y" },
				}),
			);
			await streamToArray(
				fetchResponseStream({ url: "https://example.org/q?a=1" }),
			);
			deepStrictEqual(urls, [
				"https://example.org/q?a=1&b=x%20y",
				"https://example.org/q?a=1",
			]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should fetch json objects in parallel`, async (_t) => {
		fetchSetDefaults({ dataPath: "", headers: { Accept: "application/json" } });
		const config = [
			{ url: "https://example.org/json-obj/1" },
			{ url: "https://example.org/json-obj/2" },
		];
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ key: "item", value: 1 },
			{ key: "item", value: 2 },
			{ key: "item", value: 3 },
		]);
	});

	const jsonResponse = (obj) =>
		new Response(JSON.stringify(obj), {
			status: 200,
			statusText: "OK",
			headers: new Headers({ "Content-Type": "application/json" }),
		});

	test(`fetchResponseStream concurrency fetches array items and preserves order`, async (_t) => {
		fetchSetDefaults({ rateLimit: 0 });
		fetchSetDefaults({ dataPath: "", headers: { Accept: "application/json" } });
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			const v = Number(new URL(url).searchParams.get("v"));
			if (v === 1) await new Promise((r) => setTimeout(r, 25));
			return jsonResponse({ value: v });
		};
		try {
			const config = [
				{ url: "https://example.org/c?v=1" },
				{ url: "https://example.org/c?v=2" },
				{ url: "https://example.org/c?v=3" },
			];
			const output = await streamToArray(
				fetchResponseStream(
					config.map((item) => ({ ...item, concurrency: 2 })),
				),
			);
			deepStrictEqual(output, [{ value: 1 }, { value: 2 }, { value: 3 }]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	test(`fetchResponseStream concurrency paces request starts by rateLimit`, async (t) => {
		const now = Date.now();
		t.mock.method(Date, "now", () => now);
		const realSetTimeout = globalThis.setTimeout;
		const waits = [];
		t.mock.method(globalThis, "setTimeout", (fn, ms) => {
			waits.push(ms);
			return realSetTimeout(fn, 0);
		});
		fetchSetDefaults({ dataPath: "" });
		const originalFetch = global.fetch;
		const starts = [];
		global.fetch = async (url) => {
			starts.push(url);
			return jsonResponse({});
		};
		try {
			const config = [
				{ url: "https://example.org/p1", rateLimit: 0.03 },
				{ url: "https://example.org/p2", rateLimit: 0.05 },
				{ url: "https://example.org/p3", rateLimit: 0.01 },
			];
			await streamToArray(
				fetchResponseStream(
					config.map((item) => ({ ...item, concurrency: 2 })),
				),
			);
			deepStrictEqual(starts, [
				"https://example.org/p1",
				"https://example.org/p2",
				"https://example.org/p3",
			]);
			deepStrictEqual(waits, [0, 30, 80]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	test(`fetchResponseStream concurrency surfaces an item error and settles the window`, async (_t) => {
		fetchSetDefaults({ rateLimit: 0 });
		fetchSetDefaults({ dataPath: "" });
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url.includes("bad"))
				return new Response("nope", {
					status: 404,
					headers: new Headers({ "Content-Type": "application/json" }),
				});
			await new Promise((r) => setTimeout(r, 30));
			return jsonResponse({ ok: 1 });
		};
		try {
			const config = [
				{ url: "https://example.org/bad" },
				{ url: "https://example.org/slow" },
			];
			await streamToArray(
				fetchResponseStream(
					config.map((item) => ({ ...item, concurrency: 2 })),
				),
			);
			ok(false, "should have thrown");
		} catch (error) {
			ok(/ 404 /.test(error.message), error.message);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	test(`fetchResponseStream concurrency surfaces a later item error without an unhandled rejection`, async (_t) => {
		fetchSetDefaults({ dataPath: "" });
		const originalFetch = global.fetch;
		const unhandled = [];
		const onUnhandled = (reason) => unhandled.push(reason);
		process.on("unhandledRejection", onUnhandled);
		global.fetch = async (url) => {
			if (url.includes("bad"))
				return new Response("nope", {
					status: 404,
					headers: new Headers({ "Content-Type": "application/json" }),
				});
			await new Promise((r) => setTimeout(r, 30));
			return jsonResponse({ ok: 1 });
		};
		try {
			const config = [
				{ url: "https://example.org/slow", rateLimit: 0 },
				{ url: "https://example.org/bad", rateLimit: 0 },
			];
			let error;
			try {
				await streamToArray(
					fetchResponseStream(
						config.map((item) => ({ ...item, concurrency: 2 })),
					),
				);
			} catch (e) {
				error = e;
			}
			strictEqual(error?.message, "fetch 404 GET https://example.org/bad");
			deepStrictEqual(unhandled, []);
		} finally {
			process.off("unhandledRejection", onUnhandled);
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	test(`fetchResponseStream should fetch paginated json in series`, async (_t) => {
		fetchSetDefaults({ headers: { Accept: "application/json" } });
		const config = {
			url: "https://example.org/json-arr/1",
			dataPath: "data",
			nextPath: "next",
		};

		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ key: "item", value: 1 },
			{ key: "item", value: 2 },
			{ key: "item", value: 3 },
			{ key: "item", value: 4 },
			{ key: "item", value: 5 },
			{ key: "item", value: 6 },
		]);
	});

	test(`fetchResponseStream should work with pipejoin`, async (_t) => {
		fetchSetDefaults({ headers: { Accept: "application/json" } });
		const config = {
			url: "https://example.org/json-arr/1",
			dataPath: "data",
			nextPath: "next",
		};

		const stream = pipejoin([fetchResponseStream(config)]);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ key: "item", value: 1 },
			{ key: "item", value: 2 },
			{ key: "item", value: 3 },
			{ key: "item", value: 4 },
			{ key: "item", value: 5 },
			{ key: "item", value: 6 },
		]);
	});

	test(`fetchResponseStream should work with pipeline`, async (_t) => {
		fetchSetDefaults({ headers: { Accept: "application/json" } });
		const config = {
			url: "https://example.org/json-arr/1",
			dataPath: "data",
			nextPath: "next",
		};

		const result = await pipeline([
			fetchResponseStream(config),
			createPassThroughStream(),
		]);

		deepStrictEqual(result, {});
	});

	test(`fetchResponseStream should paginate using query parameters`, async () => {
		fetchSetDefaults({ headers: { Accept: "application/json" } });
		const config = {
			url: "https://example.org/json-arr",
			qs: {
				$limit: 3,
			},
			offsetParam: "$offset",
			offsetAmount: 3,
			dataPath: "data",
		};

		const stream = pipejoin([fetchResponseStream(config)]);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ key: "item", value: 1 },
			{ key: "item", value: 2 },
			{ key: "item", value: 3 },
			{ key: "item", value: 4 },
			{ key: "item", value: 5 },
		]);
	});

	test(`fetchResponseStream query pagination keeps the offset parameter in place`, async () => {
		const urls = [];
		global.fetch = async (url) => {
			urls.push(url);
			return new Response(
				JSON.stringify({ data: urls.length === 1 ? [1, 2] : [] }),
				{ headers: new Headers({ "Content-Type": "application/json" }) },
			);
		};
		const output = await streamToArray(
			fetchResponseStream({
				url: "https://example.org/offset-first",
				qs: { $offset: 0, $limit: 2 },
				offsetParam: "$offset",
				offsetAmount: 2,
				dataPath: "data",
				rateLimit: 0,
			}),
		);
		deepStrictEqual(output, [1, 2]);
		deepStrictEqual(urls, [
			`https://example.org/offset-first?${new URLSearchParams({ $offset: 0, $limit: 2 })}`,
			`https://example.org/offset-first?${new URLSearchParams({ $offset: 2, $limit: 2 })}`,
		]);
	});

	test(`fetchResponseStream should retry on 429 status`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		fetchSetDefaults({ dataPath: "", headers: { Accept: "application/json" } });
		let callCount = 0;
		const originalFetch = global.fetch;
		global.fetch = (url) => {
			if (url === "https://example.org/429") {
				callCount++;
				if (callCount === 1) {
					return Promise.resolve(
						new Response("", { status: 429, statusText: "Too Many Requests" }),
					);
				}
				return Promise.resolve(
					new Response(JSON.stringify({ success: true }), {
						status: 200,
						statusText: "OK",
						headers: new Headers({ "Content-Type": "application/json" }),
					}),
				);
			}
			return originalFetch(url);
		};

		const config = [{ url: "https://example.org/429" }];
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);

		global.fetch = originalFetch;
		deepStrictEqual(output, [{ success: true }]);
	});

	test(`fetchResponseStream should throw on non-ok response`, async (_t) => {
		fetchSetDefaults({ headers: { Accept: "application/json" } });
		const config = [{ url: "https://example.org/404" }];

		try {
			const stream = fetchResponseStream(config);
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (error) {
			deepStrictEqual(error.message, "fetch 404 GET https://example.org/404");
			deepStrictEqual(error.cause.url, "https://example.org/404");
			deepStrictEqual(error.cause.status, 404);
			deepStrictEqual(error.cause.method, "GET");
		}
	});

	test(`fetchResponseStream error cause must not leak the unredacted URL`, async (_t) => {
		const originalFetch = global.fetch;
		const secretUrl =
			"https://user:pass@example.org/secret?token=abc123&api_key=zzz";
		global.fetch = async (url) => {
			if (url.startsWith("https://user:pass@example.org/secret")) {
				return new Response("", { status: 404, statusText: "Not Found" });
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ headers: { Accept: "application/json" } });
		try {
			const stream = fetchResponseStream([{ url: secretUrl }]);
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (error) {
			ok(!error.message.includes("token=abc123"), "message leaked query token");
			ok(!error.message.includes("user:pass"), "message leaked credentials");
			ok(
				!String(error.cause.url).includes("token=abc123"),
				`cause.url leaked query token: ${error.cause.url}`,
			);
			ok(
				!String(error.cause.url).includes("api_key=zzz"),
				`cause.url leaked api_key: ${error.cause.url}`,
			);
			ok(
				!String(error.cause.url).includes("user:pass"),
				`cause.url leaked credentials: ${error.cause.url}`,
			);
			ok(
				String(error.cause.url).includes("[REDACTED]"),
				`cause.url not redacted: ${error.cause.url}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ headers: { Accept: "application/json" } });
		}
	});

	test(`fetchRateLimit 429 max-retries error cause must not leak the unredacted URL`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const originalFetch = global.fetch;
		const secretUrl = "https://example.org/always-429?token=secret-429";
		global.fetch = () =>
			Promise.resolve(
				new Response("rate limited", {
					status: 429,
					statusText: "Too Many Requests",
				}),
			);
		fetchSetDefaults({ rateLimit: 0 });
		try {
			const stream = fetchResponseStream([
				{ url: secretUrl, rateLimit: 0, retryMaxCount: 2 },
			]);
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (error) {
			ok(error.message.includes("max retries"));
			ok(
				!error.message.includes("token=secret-429"),
				"message leaked query token",
			);
			ok(
				!String(error.cause.url).includes("token=secret-429"),
				`cause.url leaked query token: ${error.cause.url}`,
			);
			ok(
				String(error.cause.url).includes("[REDACTED]"),
				`cause.url not redacted: ${error.cause.url}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchResponseStream should block cross-origin redirects (SSRF)`, async (_t) => {
		const originalFetch = global.fetch;
		let metadataFetched = false;
		global.fetch = async (url, init) => {
			if (url === "https://example.org/redirect-ssrf") {
				deepStrictEqual(init.redirect, "manual");
				return new Response("", {
					status: 302,
					statusText: "Found",
					headers: new Headers({
						Location: "http://169.254.169.254/latest/meta-data/",
					}),
				});
			}
			if (url === "http://169.254.169.254/latest/meta-data/") {
				metadataFetched = true;
				return new Response("creds", { status: 200 });
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ headers: { Accept: "application/json" } });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/redirect-ssrf" },
			]);
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (error) {
			ok(
				error.message.includes("redirect"),
				`expected redirect error, got: ${error.message}`,
			);
			strictEqual(metadataFetched, false, "cross-origin redirect was followed");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ headers: { Accept: "application/json" } });
		}
	});

	test(`fetchResponseStream cross-origin redirect error must not leak unredacted URL`, async (_t) => {
		const originalFetch = global.fetch;
		const secretUrl = "https://example.org/redirect-secret?token=abc123";
		global.fetch = async (url) => {
			if (url === secretUrl) {
				return new Response("", {
					status: 301,
					statusText: "Moved Permanently",
					headers: new Headers({
						Location: "http://127.0.0.1:9000/internal?password=leak",
					}),
				});
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ headers: { Accept: "application/json" } });
		try {
			const stream = fetchResponseStream([{ url: secretUrl }]);
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (error) {
			ok(
				!error.message.includes("token=abc123"),
				"redirect error leaked source token",
			);
			ok(
				!error.message.includes("password=leak"),
				"redirect error leaked destination secret",
			);
			ok(
				!String(error.cause?.url ?? "").includes("token=abc123"),
				`cause.url leaked source token: ${error.cause?.url}`,
			);
			ok(
				!String(error.cause?.location ?? "").includes("password=leak"),
				`cause.location leaked destination secret: ${error.cause?.location}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ headers: { Accept: "application/json" } });
		}
	});

	test(`fetchResponseStream should follow same-origin redirects`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/redirect-same") {
				return new Response("", {
					status: 302,
					statusText: "Found",
					headers: new Headers({
						Location: "https://example.org/redirect-target",
					}),
				});
			}
			if (url === "https://example.org/redirect-target") {
				return new Response(JSON.stringify({ ok: true }), {
					status: 200,
					headers: new Headers({ "Content-Type": "application/json" }),
				});
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ dataPath: "", headers: { Accept: "application/json" } });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/redirect-same" },
			]);
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ ok: true }]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({
				dataPath: undefined,
				headers: { Accept: "application/json" },
			});
		}
	});

	const opaqueRedirect = () => ({
		type: "opaqueredirect",
		status: 0,
		ok: false,
		headers: new Headers(),
		body: null,
	});
	const withUrl = (response, url) =>
		Object.defineProperty(response, "url", { value: url });

	test(`fetchResponseStream follows a same-origin opaque (browser) redirect`, async (_t) => {
		const originalFetch = global.fetch;
		const calls = [];
		global.fetch = async (url, init) => {
			calls.push([url, init.redirect]);
			if (url === "https://example.org/a/start") {
				if (init.redirect === "manual") return opaqueRedirect();
				return withUrl(
					new Response(JSON.stringify({ data: [1] }), {
						status: 200,
						headers: new Headers({
							"Content-Type": "application/json",
							Link: '<page2>; rel="next"',
						}),
					}),
					"https://example.org/b/list",
				);
			}
			return withUrl(
				new Response(JSON.stringify({ data: [2] }), {
					status: 200,
					headers: new Headers({ "Content-Type": "application/json" }),
				}),
				url,
			);
		};
		try {
			const output = await streamToArray(
				fetchResponseStream({
					url: "https://example.org/a/start",
					dataPath: "data",
				}),
			);
			deepStrictEqual(output, [1, 2]);
			deepStrictEqual(calls, [
				["https://example.org/a/start", "manual"],
				["https://example.org/a/start", "follow"],
				["https://example.org/b/page2", "manual"],
			]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream blocks a cross-origin opaque (browser) redirect`, async (_t) => {
		const originalFetch = global.fetch;
		let cancelled = false;
		global.fetch = async (_url, init) => {
			if (init.redirect === "manual") return opaqueRedirect();
			const body = new ReadableStream({
				start(controller) {
					controller.enqueue(new Uint8Array([1]));
					controller.close();
				},
				cancel() {
					cancelled = true;
				},
			});
			return withUrl(
				new Response(body, { status: 200 }),
				"https://evil.example/x?token=secret",
			);
		};
		try {
			let error;
			try {
				await streamToArray(
					fetchResponseStream({ url: "https://example.org/r?key=secret" }),
				);
			} catch (e) {
				error = e;
			}
			strictEqual(
				error?.message,
				"fetch GET https://example.org/r?[REDACTED] blocked cross-origin redirect (https://evil.example does not match https://example.org)",
			);
			deepStrictEqual(error.cause, {
				status: 200,
				url: "https://example.org/r?[REDACTED]",
				location: "https://evil.example/x?[REDACTED]",
				origin: "https://example.org",
				method: "GET",
			});
			strictEqual(cancelled, true);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchRateLimit rejects an opaque (browser) redirect for a non-GET/HEAD request`, async (_t) => {
		const originalFetch = global.fetch;
		const calls = [];
		global.fetch = async (url, init) => {
			calls.push([url, init.method, init.redirect]);
			return opaqueRedirect();
		};
		try {
			const { fetchRateLimit } = await import("@datastream/fetch");
			let error;
			try {
				await fetchRateLimit({
					url: "https://example.org/submit?key=secret",
					method: "POST",
					body: "payload",
					rateLimit: 0,
				});
			} catch (e) {
				error = e;
			}
			strictEqual(
				error?.message,
				'fetch POST https://example.org/submit?[REDACTED] was redirected; browsers hide the redirect target, so only GET/HEAD can be re-requested (set redirect: "follow" to opt out)',
			);
			deepStrictEqual(error.cause, {
				status: 0,
				url: "https://example.org/submit?[REDACTED]",
				method: "POST",
			});
			deepStrictEqual(calls, [
				["https://example.org/submit?key=secret", "POST", "manual"],
			]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchRateLimit re-requests an opaque (browser) redirect for HEAD and lowercase get`, async (_t) => {
		const originalFetch = global.fetch;
		const calls = [];
		global.fetch = async (url, init) => {
			calls.push([init.method, init.redirect]);
			if (init.redirect === "manual") return opaqueRedirect();
			return withUrl(new Response(null, { status: 200 }), url);
		};
		try {
			const { fetchRateLimit } = await import("@datastream/fetch");
			for (const method of ["HEAD", "get"]) {
				const response = await fetchRateLimit({
					url: "https://example.org/r",
					method,
					rateLimit: 0,
				});
				strictEqual(response.status, 200);
			}
			deepStrictEqual(calls, [
				["HEAD", "manual"],
				["HEAD", "follow"],
				["get", "manual"],
				["get", "follow"],
			]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchRateLimit switches to GET without a body on 303, and on 301/302 after POST`, async (_t) => {
		const originalFetch = global.fetch;
		const calls = [];
		global.fetch = async (url, init) => {
			calls.push([url, init.method, init.body]);
			if (url.startsWith("https://example.org/form-")) {
				return new Response(null, {
					status: Number(url.slice(-3)),
					headers: new Headers({ Location: "/result" }),
				});
			}
			return new Response(null, { status: 200 });
		};
		try {
			const { fetchRateLimit } = await import("@datastream/fetch");
			const cases = [
				["POST", 303, "GET", undefined],
				["GET", 303, "GET", undefined],
				["HEAD", 303, "HEAD", undefined],
				["post", 302, "GET", undefined],
				["POST", 301, "GET", undefined],
				["PUT", 302, "PUT", "a=1"],
				["POST", 307, "POST", "a=1"],
			];
			for (const [method, status, nextMethod, nextBody] of cases) {
				calls.length = 0;
				const body = method === "GET" || method === "HEAD" ? undefined : "a=1";
				const response = await fetchRateLimit({
					url: `https://example.org/form-${status}`,
					method,
					body,
					rateLimit: 0,
				});
				strictEqual(response.status, 200);
				deepStrictEqual(calls, [
					[`https://example.org/form-${status}`, method, body],
					["https://example.org/result", nextMethod, nextBody],
				]);
			}
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchRateLimit keeps GET without a body when retrying a 303 target`, async (_t) => {
		const originalFetch = global.fetch;
		const calls = [];
		let retried = false;
		global.fetch = async (url, init) => {
			calls.push([url, init.method, init.body]);
			if (url === "https://example.org/form") {
				return new Response(null, {
					status: 303,
					headers: new Headers({ Location: "/result" }),
				});
			}
			if (!retried) {
				retried = true;
				return new Response(null, {
					status: 429,
					headers: new Headers({ "Retry-After": "0" }),
				});
			}
			return new Response(null, { status: 200 });
		};
		try {
			const { fetchRateLimit } = await import("@datastream/fetch");
			const response = await fetchRateLimit({
				url: "https://example.org/form",
				method: "POST",
				body: "a=1",
				rateLimit: 0,
			});
			strictEqual(response.status, 200);
			deepStrictEqual(calls, [
				["https://example.org/form", "POST", "a=1"],
				["https://example.org/result", "GET", undefined],
				["https://example.org/result", "GET", undefined],
			]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should honor explicit redirect option (opt-in follow)`, async (_t) => {
		const originalFetch = global.fetch;
		let sawFollow = false;
		global.fetch = async (url, init) => {
			if (url === "https://example.org/redirect-optin") {
				sawFollow = init.redirect === "follow";
				return new Response(JSON.stringify({ ok: true }), {
					status: 200,
					headers: new Headers({ "Content-Type": "application/json" }),
				});
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ dataPath: "", headers: { Accept: "application/json" } });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/redirect-optin", redirect: "follow" },
			]);
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ ok: true }]);
			ok(sawFollow, "explicit redirect:follow was not passed through");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({
				dataPath: undefined,
				headers: { Accept: "application/json" },
			});
		}
	});

	test(`fetchWritableStream should create writable stream for upload`, async (_t) => {
		const originalFetch = global.fetch;

		global.fetch = async (_url, _options) => {
			return new Response(JSON.stringify({ uploaded: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};

		const options = {
			url: "https://example.org/upload",
			method: "POST",
		};

		const stream = await fetchWritableStream(options);

		strictEqual(isWritable(stream), true);
		deepStrictEqual(typeof stream.result, "function");

		await pipeline([createReadableStream(["test data"]), stream]);

		const result = stream.result();
		deepStrictEqual(result.key, "output");

		global.fetch = originalFetch;
	});
	test(`fetchWritableStream reports its response under resultKey`, async (_t) => {
		global.fetch = async () =>
			new Response(JSON.stringify({ uploaded: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});

		const stream = await fetchWritableStream({
			url: "https://example.org/upload",
			method: "POST",
			resultKey: "upload",
		});
		const output = await pipeline([createReadableStream(["data"]), stream]);

		deepStrictEqual(Object.keys(output), ["upload"]);
		strictEqual(output.upload.status, 200);
	});

	test(`fetchWritableStream should close the request body on end`, async (_t) => {
		const originalFetch = global.fetch;

		let resolveBody;
		const bodyDone = new Promise((resolve) => {
			resolveBody = resolve;
		});

		global.fetch = async (_url, options) => {
			(async () => {
				const received = [];
				for await (const chunk of options.body) {
					received.push(chunk);
				}
				resolveBody(received);
			})();
			return new Response(JSON.stringify({ uploaded: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};

		const stream = await fetchWritableStream({
			url: "https://example.org/upload",
			method: "POST",
		});

		await pipeline([createReadableStream(["alpha", "beta"]), stream]);
		strictEqual(stream.result().value.status, 200);

		const received = await bodyDone;
		deepStrictEqual(received, ["alpha", "beta"]);

		global.fetch = originalFetch;
	});

	test(`fetchWritableStream does not wait for the response before accepting the body`, {
		timeout: 2000,
	}, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (_url, options) => {
			const received = [];
			for await (const chunk of options.body) {
				received.push(chunk);
			}
			return new Response(JSON.stringify(received), {
				status: 201,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		try {
			const stream = await fetchWritableStream({
				url: "https://example.org/upload-first",
				method: "POST",
			});
			await pipeline([createReadableStream(["alpha", "beta"]), stream]);
			const { key, value } = stream.result();
			strictEqual(key, "output");
			strictEqual(value.status, 201);
			deepStrictEqual(await value.json(), ["alpha", "beta"]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchWritableStream surfaces a failed response when the body ends`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response("", { status: 500, statusText: "Server Error" });
		try {
			const stream = await fetchWritableStream({
				url: "https://example.org/upload-fail",
				method: "POST",
			});
			let error;
			try {
				await pipeline([createReadableStream(["alpha"]), stream]);
			} catch (e) {
				error = e;
			}
			strictEqual(
				error?.message,
				"fetch 500 POST https://example.org/upload-fail",
			);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchWritableStream surfaces an early fetch error on the next write`, async (_t) => {
		const originalFetch = global.fetch;
		let requestBody;
		global.fetch = async (_url, init) => {
			requestBody = init.body;
			return new Response("", { status: 401 });
		};
		try {
			const stream = await fetchWritableStream({
				url: "https://example.org/upload-401",
				method: "PUT",
				rateLimit: 0,
			});
			await new Promise((resolve) => setTimeout(resolve, 0));
			let error;
			try {
				await pipeline([
					createReadableStream(new Array(1100).fill("x")),
					stream,
				]);
			} catch (e) {
				error = e;
			}
			strictEqual(
				error?.message,
				"fetch 401 PUT https://example.org/upload-401",
			);
			const read = requestBody.getReader().read();
			strictEqual(
				await Promise.race([
					read,
					new Promise((resolve) => setTimeout(() => resolve("pending"), 0)),
				]),
				"pending",
			);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchWritableStream leaves the request body readable after a normal end`, async (_t) => {
		const originalFetch = global.fetch;
		let requestBody;
		global.fetch = async (_url, init) => {
			requestBody = init.body;
			return new Response("", { status: 200 });
		};
		try {
			const stream = await fetchWritableStream({
				url: "https://example.org/upload-ok",
				method: "PUT",
				rateLimit: 0,
			});
			await pipeline([createReadableStream(["a", "b", "c"]), stream]);
			await new Promise((resolve) => setTimeout(resolve, 0));
			strictEqual(stream.result().value.status, 200);
			const received = [];
			for await (const chunk of requestBody) received.push(chunk);
			deepStrictEqual(received, ["a", "b", "c"]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	const pendingUpload = () => {
		const upload = {};
		upload.fetch = (_url, init) => {
			upload.init = init;
			upload.signalAbortedAtStart = init.signal.aborted;
			upload.bodyRead = (async () => {
				try {
					for await (const _chunk of init.body);
					return "ended";
				} catch (e) {
					return e;
				}
			})();
			return new Promise((_resolve, reject) => {
				init.signal.addEventListener("abort", () => reject(init.signal.reason));
			});
		};
		return upload;
	};
	const outcome = (promise) =>
		Promise.race([
			promise,
			new Promise((resolve) => setTimeout(() => resolve("pending"), 0)),
		]);

	test(`fetchWritableStream cancels the request and its body when the writable is aborted`, async (_t) => {
		const upload = pendingUpload();
		global.fetch = upload.fetch;
		const stream = await fetchWritableStream({
			url: "https://example.org/upload-abort",
			method: "PUT",
			rateLimit: 0,
		});
		strictEqual(upload.init.signal.aborted, false);
		const reason = new Error("stop");
		if (variant === "browser") {
			await stream.abort(reason);
		} else {
			stream.on("error", () => {});
			stream.destroy(reason);
			await new Promise((resolve) => setTimeout(resolve, 0));
		}
		strictEqual(upload.init.signal.aborted, true);
		strictEqual(upload.init.signal.reason, reason);
		strictEqual(await outcome(upload.bodyRead), reason);
	});

	test(`fetchWritableStream cancels the request when upstream fails`, async (_t) => {
		const upload = pendingUpload();
		global.fetch = upload.fetch;
		const stream = await fetchWritableStream({
			url: "https://example.org/upload-upstream",
			method: "PUT",
			rateLimit: 0,
		});
		const failure = new Error("upstream");
		async function* source() {
			yield "a";
			throw failure;
		}
		let error;
		try {
			await pipeline([createReadableStream(source()), stream]);
		} catch (e) {
			error = e;
		}
		strictEqual(error, failure);
		strictEqual(upload.init.signal.aborted, true);
		ok((await outcome(upload.bodyRead)) instanceof Error);
	});

	test(`fetchWritableStream passes an already aborted signal to the request`, async (_t) => {
		const upload = pendingUpload();
		global.fetch = upload.fetch;
		const controller = new AbortController();
		controller.abort();
		const stream = await fetchWritableStream(
			{ url: "https://example.org/upload-pre", method: "PUT", rateLimit: 0 },
			{ signal: controller.signal },
		);
		if (variant === "node") stream.on("error", () => {});
		strictEqual(upload.signalAbortedAtStart, true);
	});

	test(`fetchWritableStream does not abort the request after a clean finish`, async (_t) => {
		let init;
		global.fetch = async (_url, options) => {
			init = options;
			for await (const _chunk of options.body);
			return new Response("", { status: 200 });
		};
		const stream = await fetchWritableStream({
			url: "https://example.org/upload-clean",
			method: "PUT",
			rateLimit: 0,
		});
		await pipeline([createReadableStream(["a"]), stream]);
		await new Promise((resolve) => setTimeout(resolve, 0));
		strictEqual(init.signal.aborted, false);
	});

	test(`fetchWritableStream applies backpressure to a slow server`, {
		timeout: 10000,
	}, async (_t) => {
		let produced = 0;
		let consumed = 0;
		let maxAhead = 0;
		global.fetch = async (_url, init) => {
			for await (const _chunk of init.body) {
				consumed++;
				await new Promise((resolve) => setTimeout(resolve, 0));
			}
			return new Response("", { status: 200 });
		};
		async function* source() {
			for (let i = 0; i < 1500; i++) {
				produced++;
				maxAhead = Math.max(maxAhead, produced - consumed);
				yield new Uint8Array(1);
			}
		}
		const stream = await fetchWritableStream({
			url: "https://example.org/upload-slow",
			method: "PUT",
			rateLimit: 0,
		});
		await pipeline([createReadableStream(source()), stream]);
		strictEqual(stream.result().value.status, 200);
		strictEqual(consumed, 1500);
		ok(maxAhead < 100, `writer ran ${maxAhead} chunks ahead`);
	});

	test(`fetchWritableStream wakes a write waiting on the body when the request fails`, {
		timeout: 2000,
	}, async (_t) => {
		global.fetch = () =>
			new Promise((resolve) => {
				setTimeout(() => resolve(new Response("", { status: 401 })), 10);
			});
		const stream = await fetchWritableStream({
			url: "https://example.org/upload-wait-401",
			method: "PUT",
			rateLimit: 0,
		});
		let error;
		try {
			await pipeline([createReadableStream(["a", "b", "c"]), stream]);
		} catch (e) {
			error = e;
		}
		strictEqual(
			error?.message,
			"fetch 401 PUT https://example.org/upload-wait-401",
		);
	});

	test(`fetchWritableStream drops writes once fetch cancels the body`, {
		timeout: 2000,
	}, async (_t) => {
		let requestBody;
		global.fetch = async (_url, init) => {
			requestBody = init.body;
			await init.body.cancel();
			await new Promise((resolve) => setTimeout(resolve, 10));
			return new Response("", { status: 200 });
		};
		const stream = await fetchWritableStream({
			url: "https://example.org/upload-cancelled",
			method: "PUT",
			rateLimit: 0,
		});
		await pipeline([createReadableStream(["a", "b", "c"]), stream]);
		strictEqual(stream.result().value.status, 200);
		deepStrictEqual(await requestBody.getReader().read(), {
			done: true,
			value: undefined,
		});
	});

	test(`fetchWritableStream wakes a waiting write when its signal aborts`, {
		timeout: 2000,
	}, async (_t) => {
		const upload = {};
		global.fetch = (_url, init) => {
			upload.init = init;
			return new Promise((_resolve, reject) => {
				init.signal.addEventListener("abort", () => reject(init.signal.reason));
			});
		};
		const controller = new AbortController();
		const stream = await fetchWritableStream(
			{
				url: "https://example.org/upload-wait-abort",
				method: "PUT",
				rateLimit: 0,
			},
			{ signal: controller.signal },
		);
		const reason = new Error("stop");
		const done = pipeline([createReadableStream(["a", "b", "c"]), stream]);
		await new Promise((resolve) => setTimeout(resolve, 10));
		controller.abort(reason);
		let error;
		try {
			await done;
		} catch (e) {
			error = e;
		}
		ok(error);
		strictEqual(upload.init.signal.aborted, true);
	});

	const writerOf = (stream) => {
		if (variant === "browser") {
			const writer = stream.getWriter();
			return {
				write: (chunk) => writer.write(chunk),
				abort: (reason) => writer.abort(reason),
			};
		}
		stream.on("error", () => {});
		return {
			write: (chunk) =>
				new Promise((resolve, reject) => {
					stream.write(chunk, (e) => (e ? reject(e) : resolve()));
				}),
			abort: async (reason) => {
				stream.destroy(reason);
				await new Promise((resolve) => setTimeout(resolve, 0));
			},
		};
	};
	const settled = (promise) =>
		Promise.race([
			promise.then(
				() => "resolved",
				(e) => e,
			),
			new Promise((resolve) => setTimeout(() => resolve("pending"), 10)),
		]);
	const heldUpload = async (streamOptions) => {
		const upload = {};
		upload.response = new Promise((resolve, reject) => {
			upload.respond = resolve;
			upload.fail = reject;
		});
		global.fetch = (_url, init) => {
			upload.init = init;
			return upload.response;
		};
		upload.stream = await fetchWritableStream(
			{ url: "https://example.org/upload-held", method: "PUT", rateLimit: 0 },
			streamOptions,
		);
		Object.assign(upload, writerOf(upload.stream));
		await new Promise((resolve) => setTimeout(resolve, 0));
		return upload;
	};

	test(`fetchWritableStream lets writes run highWaterMark chunks ahead of the server`, async (_t) => {
		const upload = await heldUpload({ highWaterMark: 3 });
		strictEqual(await settled(upload.write("a")), "resolved");
		strictEqual(await settled(upload.write("b")), "resolved");
		strictEqual(await settled(upload.write("c")), "pending");
	});

	test(`fetchWritableStream resumes a waiting write once fetch reads the body`, async (_t) => {
		const upload = await heldUpload();
		const write = upload.write("a");
		strictEqual(await settled(write), "pending");
		const reader = upload.init.body.getReader();
		deepStrictEqual(await reader.read(), { done: false, value: "a" });
		strictEqual(await settled(write), "resolved");
		const reason = new Error("stop");
		await upload.abort(reason);
		strictEqual(upload.init.signal.reason, reason);
	});

	test(`fetchWritableStream resumes a waiting write once fetch cancels the body`, async (_t) => {
		const upload = await heldUpload();
		const write = upload.write("a");
		strictEqual(await settled(write), "pending");
		await upload.init.body.cancel();
		strictEqual(await settled(write), "resolved");
	});

	test(`fetchWritableStream resumes a waiting write once the response arrives`, async (_t) => {
		const upload = await heldUpload();
		const write = upload.write("a");
		strictEqual(await settled(write), "pending");
		upload.respond(new Response("", { status: 200 }));
		strictEqual(await settled(write), "resolved");
	});

	test(`fetchWritableStream rejects a waiting write when the request fails`, async (_t) => {
		const upload = await heldUpload();
		const write = upload.write("a");
		strictEqual(await settled(write), "pending");
		const failure = new Error("down");
		upload.fail(failure);
		strictEqual(await settled(write), failure);
	});

	test(`fetchWritableStream settles a waiting write when its signal aborts`, async (_t) => {
		const controller = new AbortController();
		const upload = await heldUpload({ signal: controller.signal });
		const write = upload.write("a");
		strictEqual(await settled(write), "pending");
		controller.abort(new Error("stop"));
		notStrictEqual(await settled(write), "pending");
	});

	test(`fetchWritableStream does not retry a 429 with a streamed body`, async (_t) => {
		let calls = 0;
		global.fetch = async () => {
			calls++;
			return new Response("", {
				status: 429,
				headers: new Headers({ "Retry-After": "0" }),
			});
		};
		const stream = await fetchWritableStream({
			url: "https://example.org/upload-429",
			method: "PUT",
			rateLimit: 0,
		});
		let error;
		try {
			await pipeline([createReadableStream(["a"]), stream]);
		} catch (e) {
			error = e;
		}
		strictEqual(
			error?.message,
			"fetch 429 PUT https://example.org/upload-429 cannot replay streamed request body",
		);
		deepStrictEqual(error.cause, {
			status: 429,
			url: "https://example.org/upload-429",
			method: "PUT",
		});
		strictEqual(calls, 1);
	});

	for (const status of [307, 308]) {
		test(`fetchWritableStream does not follow a ${status} with a streamed body`, async (_t) => {
			let calls = 0;
			global.fetch = async () => {
				calls++;
				return new Response(null, {
					status,
					headers: new Headers({ Location: "/elsewhere" }),
				});
			};
			const stream = await fetchWritableStream({
				url: "https://example.org/upload-redirect",
				method: "POST",
				rateLimit: 0,
			});
			let error;
			try {
				await pipeline([createReadableStream(["a"]), stream]);
			} catch (e) {
				error = e;
			}
			strictEqual(
				error?.message,
				`fetch ${status} POST https://example.org/upload-redirect cannot replay streamed request body`,
			);
			deepStrictEqual(error.cause, {
				status,
				url: "https://example.org/upload-redirect",
				method: "POST",
			});
			strictEqual(calls, 1);
		});
	}

	// Safari's ReadableStream has getReader() but is not async iterable.
	test(`fetchRateLimit does not replay a ReadableStream body without async iteration`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		let calls = 0;
		global.fetch = async () => {
			calls++;
			return new Response("", { status: 429 });
		};
		let error;
		try {
			await fetchModule.fetchRateLimit({
				url: "https://example.org/reader",
				method: "POST",
				body: { getReader() {} },
				duplex: "half",
				rateLimit: 0,
			});
		} catch (e) {
			error = e;
		}
		strictEqual(
			error?.message,
			"fetch 429 POST https://example.org/reader cannot replay streamed request body",
		);
		strictEqual(calls, 1);
	});

	test(`fetchRateLimit does not replay an async iterable body`, async (_t) => {
		let calls = 0;
		global.fetch = async () => {
			calls++;
			return new Response("", { status: 429 });
		};
		let error;
		try {
			await fetchModule.fetchRateLimit({
				url: "https://example.org/iterable",
				method: "POST",
				body: (async function* () {
					yield "a";
				})(),
				duplex: "half",
				rateLimit: 0,
			});
		} catch (e) {
			error = e;
		}
		strictEqual(
			error?.message,
			"fetch 429 POST https://example.org/iterable cannot replay streamed request body",
		);
		strictEqual(calls, 1);
	});

	test(`fetchRateLimit still replays a non-stream body on 429 and 307`, async (_t) => {
		const bodies = [];
		global.fetch = async (url, init) => {
			bodies.push(init.body);
			if (bodies.length === 1) {
				return new Response("", {
					status: 429,
					headers: new Headers({ "Retry-After": "0" }),
				});
			}
			if (url === "https://example.org/replay") {
				return new Response(null, {
					status: 307,
					headers: new Headers({ Location: "/replayed" }),
				});
			}
			return new Response("", { status: 200 });
		};
		const response = await fetchModule.fetchRateLimit({
			url: "https://example.org/replay",
			method: "POST",
			body: "payload",
			rateLimit: 0,
		});
		strictEqual(response.status, 200);
		deepStrictEqual(bodies, ["payload", "payload", "payload"]);
	});

	const endlessResponse = (init, aborted) =>
		new Response(
			new ReadableStream({
				start(controller) {
					controller.enqueue(new Uint8Array(1));
					init.signal.addEventListener("abort", () => {
						aborted.push(init.signal.reason);
						controller.error(init.signal.reason);
					});
				},
			}),
			{ headers: new Headers({ "Content-Type": "application/octet-stream" }) },
		);

	test(`fetchResponseStream concurrency aborts in-flight siblings on the first error`, {
		timeout: 2000,
	}, async (_t) => {
		const aborted = [];
		global.fetch = async (url, init) => {
			if (url === "https://example.org/fail") {
				await new Promise((resolve) => setTimeout(resolve, 10));
				return new Response("no", { status: 404 });
			}
			return endlessResponse(init, aborted);
		};
		let error;
		try {
			await streamToArray(
				fetchResponseStream([
					{ url: "https://example.org/fail", concurrency: 3, rateLimit: 0 },
					{ url: "https://example.org/slow1", rateLimit: 0 },
					{ url: "https://example.org/slow2", rateLimit: 0 },
				]),
			);
		} catch (e) {
			error = e;
		}
		strictEqual(error?.message, "fetch 404 GET https://example.org/fail");
		strictEqual(aborted.length, 2);
	});

	test(`fetchResponseStream concurrency aborts in-flight siblings when the consumer stops`, {
		timeout: 2000,
	}, async (_t) => {
		const aborted = [];
		let endlessStarted;
		const started = new Promise((resolve) => {
			endlessStarted = resolve;
		});
		global.fetch = async (url, init) => {
			if (url === "https://example.org/quick") {
				await started;
				const chunks = Array.from({ length: 64 }, () => new Uint8Array(1));
				return new Response(
					new ReadableStream({
						pull(controller) {
							const chunk = chunks.shift();
							if (chunk) controller.enqueue(chunk);
							else controller.close();
						},
					}),
					{
						headers: new Headers({
							"Content-Type": "application/octet-stream",
						}),
					},
				);
			}
			endlessStarted();
			return endlessResponse(init, aborted);
		};
		const stream = fetchResponseStream([
			{ url: "https://example.org/quick", concurrency: 2, rateLimit: 0 },
			{ url: "https://example.org/endless", rateLimit: 0 },
		]);
		for await (const _chunk of stream) break;
		await new Promise((resolve) => setTimeout(resolve, 10));
		strictEqual(aborted.length, 1);
	});

	test(`fetchResponseStream concurrency still honours the caller's signal`, {
		timeout: 2000,
	}, async (_t) => {
		const aborted = [];
		global.fetch = async (_url, init) => endlessResponse(init, aborted);
		const controller = new AbortController();
		const stream = fetchResponseStream(
			[
				{ url: "https://example.org/a", concurrency: 2, rateLimit: 0 },
				{ url: "https://example.org/b", rateLimit: 0 },
			],
			{ signal: controller.signal },
		);
		setTimeout(() => controller.abort(new Error("stop")), 10);
		let error;
		try {
			await streamToArray(stream);
		} catch (e) {
			error = e;
		}
		ok(error);
		strictEqual(aborted.length, 2);
	});

	test(`fetchRateLimit should delay between configs within one stream`, async (t) => {
		const now = Date.now();
		t.mock.method(Date, "now", () => now);
		const realSetTimeout = globalThis.setTimeout;
		const waits = [];
		t.mock.method(globalThis, "setTimeout", (fn, ms) => {
			waits.push(ms);
			return realSetTimeout(fn, 0);
		});
		const urls = [];
		global.fetch = async (url) => {
			urls.push(url);
			return new Response(JSON.stringify({ success: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		const config = [
			{ url: "https://example.org/test1", rateLimit: 0.1, dataPath: "" },
			{ url: "https://example.org/test2", rateLimit: 0.1, dataPath: "" },
		];
		await streamToArray(fetchResponseStream(config));
		deepStrictEqual(urls, [
			"https://example.org/test1",
			"https://example.org/test2",
		]);
		deepStrictEqual(waits, [100]);
	});

	test(`fetchRateLimit direct call defaults rateLimit before computing timestamp`, async (_t) => {
		const { fetchRateLimit } = await import("@datastream/fetch");
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		try {
			const options = { url: "https://example.org/direct-rl" };
			await fetchRateLimit(options);
			ok(
				Number.isFinite(options.rateLimitTimestamp),
				`rateLimitTimestamp should be finite, got ${options.rateLimitTimestamp}`,
			);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should handle content-type with suffix`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (_url, _options) => {
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({
					"Content-Type": "application/vnd.api+json",
				}),
			});
		};

		fetchSetDefaults({ dataPath: "" });
		const config = [{ url: "https://example.org/api-json" }];
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);

		global.fetch = originalFetch;
		deepStrictEqual(output, [{ ok: true }]);
	});

	test(`fetchResponseStream should handle offsetParam without offsetAmount`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({ dataPath: "data" });
		const config = {
			url: "https://example.org/partial-offset",
			offsetParam: "$offset",
		};
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);
		global.fetch = originalFetch;
		deepStrictEqual(output, [{ id: 1 }]);
	});

	test(`fetchResponseStream should stop pagination when offset param is empty`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({ dataPath: "data" });
		const config = {
			url: "https://example.org/empty-offset",
			qs: { $offset: "" },
			offsetParam: "$offset",
			offsetAmount: 3,
		};
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);
		global.fetch = originalFetch;
		deepStrictEqual(output, [{ id: 1 }]);
	});

	test(`fetchResponseStream should handle dataPath as array`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			return new Response(JSON.stringify({ a: { b: [{ id: 1 }] } }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({});
		const config = { url: "https://example.org/nested", dataPath: ["a", "b"] };
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);
		global.fetch = originalFetch;
		deepStrictEqual(output, [{ id: 1 }]);
	});

	test(`fetchRateLimit should throw after max retries on persistent 429`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const originalFetch = global.fetch;
		let callCount = 0;
		global.fetch = () => {
			callCount++;
			return Promise.resolve(
				new Response("rate limited", {
					status: 429,
					statusText: "Too Many Requests",
				}),
			);
		};

		fetchSetDefaults({ rateLimit: 0 });
		const config = [
			{
				url: "https://example.org/always-429",
				rateLimit: 0,
				retryMaxCount: 2,
			},
		];
		try {
			const stream = fetchResponseStream(config);
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(
				e.message,
				"fetch 429 GET https://example.org/always-429 max retries (2) exceeded",
			);
			strictEqual(e.name, "RangeError");
			ok(callCount >= 2, `Expected at least 2 calls, got ${callCount}`);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should stop JSON pagination when consumer errors`, async (_t) => {
		const originalFetch = global.fetch;
		let page2Fetched = false;
		global.fetch = async (url) => {
			if (url === "https://example.org/cancel-json/1") {
				return new Response(
					JSON.stringify({
						data: [{ id: 1 }, { id: 2 }],
						next: "https://example.org/cancel-json/2",
					}),
					{
						status: 200,
						headers: new Headers({ "Content-Type": "application/json" }),
					},
				);
			}
			if (url === "https://example.org/cancel-json/2") {
				page2Fetched = true;
				return new Response(JSON.stringify({ data: [{ id: 3 }], next: "" }), {
					status: 200,
					headers: new Headers({ "Content-Type": "application/json" }),
				});
			}
			return originalFetch(url);
		};

		fetchSetDefaults({ dataPath: "data", nextPath: "next" });
		const failing = createTransformStream(() => {
			throw new Error("downstream boom");
		});
		try {
			await pipeline([
				fetchResponseStream({ url: "https://example.org/cancel-json/1" }),
				failing,
			]);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("downstream boom"));
			strictEqual(page2Fetched, false);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined, nextPath: undefined });
		}
	});

	test(`fetchResponseStream binary branch cleanup on read error preserves original error`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/binary-cancel") {
				const r = new Response("ignored", {
					status: 200,
					headers: new Headers({ "Content-Type": "text/csv" }),
				});
				const fakeBody = {
					cancel: async () => {},
					[Symbol.asyncIterator]() {
						return {
							next: async () => {
								throw new Error("binary read boom");
							},
						};
					},
				};
				Object.defineProperty(r, "body", {
					value: fakeBody,
					configurable: true,
				});
				return r;
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ headers: { Accept: "text/csv" } });
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/binary-cancel",
			});
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("binary read boom"),
				`expected original read error, got: ${e.message}`,
			);
			ok(
				!e.message.includes("is not a function"),
				`cleanup called a non-existent .return(): ${e.message}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ headers: { Accept: "application/json" } });
		}
	});

	test(`fetchResponseStream yields nothing for a response without a body`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) =>
			new Response(null, {
				status: url.endsWith("204") ? 204 : 200,
				headers: new Headers({
					"Content-Type": url.endsWith("json")
						? "application/json"
						: "text/csv",
				}),
			});
		try {
			for (const url of [
				"https://example.org/no-body-204",
				"https://example.org/no-body-head-json",
			]) {
				const output = await streamToArray(
					fetchResponseStream({ url, method: "HEAD", rateLimit: 0 }),
				);
				deepStrictEqual(output, []);
			}
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should reject Link header with cross-origin URL`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/ssrf-link") {
				return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
					status: 200,
					headers: new Headers({
						"Content-Type": "application/json",
						Link: '<http://169.254.169.254/metadata>; rel="next"',
					}),
				});
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ dataPath: "data" });
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/ssrf-link",
			});
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("does not match initial URL origin"));
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should reject nextPath with cross-origin URL`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/ssrf-next") {
				return new Response(
					JSON.stringify({
						data: [{ id: 1 }],
						next: "http://127.0.0.1:8080/internal",
					}),
					{
						status: 200,
						headers: new Headers({
							"Content-Type": "application/json",
						}),
					},
				);
			}
			return originalFetch(url);
		};
		fetchSetDefaults({});
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/ssrf-next",
				dataPath: "data",
				nextPath: "next",
			});
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("does not match initial URL origin"));
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should reject nextPath with different protocol`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/ssrf-proto") {
				return new Response(
					JSON.stringify({
						data: [{ id: 1 }],
						next: "file:///etc/passwd",
					}),
					{
						status: 200,
						headers: new Headers({
							"Content-Type": "application/json",
						}),
					},
				);
			}
			return originalFetch(url);
		};
		fetchSetDefaults({});
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/ssrf-proto",
				dataPath: "data",
				nextPath: "next",
			});
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("does not match initial URL origin"));
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should reject Link header with different port`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/ssrf-port") {
				return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
					status: 200,
					headers: new Headers({
						"Content-Type": "application/json",
						Link: '<https://example.org:8443/admin>; rel="next"',
					}),
				});
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ dataPath: "data" });
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/ssrf-port",
			});
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("does not match initial URL origin"));
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream should allow same-origin pagination`, async (_t) => {
		fetchSetDefaults({ dataPath: "", headers: { Accept: "application/json" } });
		const config = [{ url: "https://example.org/json-obj/2" }];
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ key: "item", value: 2 },
			{ key: "item", value: 3 },
		]);
	});

	test(`fetch has no default export`, (_t) => {
		strictEqual(Object.hasOwn(fetchModule, "default"), false);
	});

	test(`fetchResponseStream should throw with message for invalid pagination URL in Link header`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/bad-link") {
				return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
					status: 200,
					headers: new Headers({
						"Content-Type": "application/json",
						Link: '<https://[bad?token=secret>; rel="next"',
					}),
				});
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ dataPath: "data" });
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/bad-link",
			});
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "Invalid pagination URL");
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream error cause.url contains [REDACTED] not empty credentials`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response("", { status: 404, statusText: "Not Found" });
		fetchSetDefaults({});
		try {
			const { fetchRateLimit } = await import("@datastream/fetch");
			await fetchRateLimit({ url: "https://user:pass@example.org/path?q=1" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.cause.url.includes("[REDACTED]"),
				`cause.url should have [REDACTED], got: ${e.cause.url}`,
			);
			ok(
				!e.cause.url.includes("user"),
				`cause.url should not have username, got: ${e.cause.url}`,
			);
			ok(
				!e.cause.url.includes("pass"),
				`cause.url should not have password, got: ${e.cause.url}`,
			);
			ok(
				!e.cause.url.includes(":@"),
				`cause.url should not have empty-string credentials, got: ${e.cause.url}`,
			);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`default headers include non-empty Accept and Accept-Encoding`, async (_t) => {
		const originalFetch = global.fetch;
		let capturedHeaders;
		global.fetch = async (_url, init) => {
			capturedHeaders = init.headers;
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({});
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/headers-test", dataPath: "" },
			]);
			await streamToArray(stream);
			strictEqual(capturedHeaders.Accept, "application/json");
			strictEqual(capturedHeaders["Accept-Encoding"], "br, gzip, deflate");
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchSetDefaults merges headers correctly and preserves defaults`, async (_t) => {
		const originalFetch = global.fetch;
		let capturedHeaders;
		global.fetch = async (_url, init) => {
			capturedHeaders = init.headers;
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({ headers: { "X-Custom": "test" } });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/merge-headers", dataPath: "" },
			]);
			await streamToArray(stream);
			strictEqual(capturedHeaders["X-Custom"], "test");
			ok(
				capturedHeaders.Accept !== undefined,
				"Accept header should be present after merge",
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchWritableStream sends duplex:"half" not empty string`, async (_t) => {
		const originalFetch = global.fetch;
		let capturedInit;
		global.fetch = async (_url, init) => {
			capturedInit = init;
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({});
		const stream = await fetchWritableStream({
			url: "https://example.org/duplex",
			method: "POST",
		});
		await pipeline([createReadableStream([]), stream]);
		strictEqual(capturedInit.duplex, "half");
		global.fetch = originalFetch;
	});

	test(`fetchResponseStream replaces + with %20 in query string`, async (_t) => {
		const originalFetch = global.fetch;
		let capturedUrl;
		global.fetch = async (url) => {
			capturedUrl = url;
			return new Response(JSON.stringify({ data: [] }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({ dataPath: "data" });
		const stream = fetchResponseStream({
			url: "https://example.org/space-test",
			qs: { q: "hello world" },
			dataPath: "data",
		});
		await streamToArray(stream);
		global.fetch = originalFetch;
		ok(
			capturedUrl.includes("%20"),
			`URL should use %20 not +, got: ${capturedUrl}`,
		);
		ok(
			!capturedUrl.includes("+"),
			`URL should not contain +, got: ${capturedUrl}`,
		);
	});

	test(`fetchResponseStream treats text/application-json (non-JSON) as binary`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			return new Response("raw bytes", {
				status: 200,
				headers: new Headers({ "Content-Type": "text/application/json" }),
			});
		};
		fetchSetDefaults({ dataPath: "" });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/text-json" },
			]);
			const output = await streamToArray(stream);
			ok(output.length > 0);
			ok(
				output[0] instanceof Uint8Array,
				`Expected binary output for non-JSON content type, got: ${typeof output[0]}`,
			);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream treats application/jsonl (no separator) as binary`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			return new Response('{"a":1}\n{"a":2}', {
				status: 200,
				headers: new Headers({ "Content-Type": "application/jsonl" }),
			});
		};
		fetchSetDefaults({ dataPath: "" });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/jsonl" },
			]);
			const output = await streamToArray(stream);
			ok(output.length > 0);
			ok(
				output[0] instanceof Uint8Array,
				`Expected binary output for application/jsonl, got: ${typeof output[0]}`,
			);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream treats application/json;charset=utf-8 as JSON`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			return new Response(JSON.stringify({ val: 42 }), {
				status: 200,
				headers: new Headers({
					"Content-Type": "application/json;charset=utf-8",
				}),
			});
		};
		fetchSetDefaults({ dataPath: "" });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/json-charset2" },
			]);
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ val: 42 }]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream treats a mixed-case Application/JSON as JSON`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response(JSON.stringify([1, 2]), {
				status: 200,
				headers: new Headers({ "Content-Type": "Application/JSON" }),
			});
		try {
			const output = await streamToArray(
				fetchResponseStream({
					url: "https://example.org/json-case",
					dataPath: "",
					rateLimit: 0,
				}),
			);
			deepStrictEqual(output, [1, 2]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream handles Link header with no rel=next match`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/link-no-next") {
				return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
					status: 200,
					headers: new Headers({
						"Content-Type": "application/json",
						Link: '<https://example.org/prev>; rel="prev"',
					}),
				});
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ dataPath: "data" });
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/link-no-next",
			});
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ id: 1 }]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream resolves relative Link and nextPath URLs`, async (_t) => {
		const originalFetch = global.fetch;
		const urls = [];
		global.fetch = async (url) => {
			urls.push(url);
			const page = Number(new URL(url).searchParams.get("page") ?? 1);
			return new Response(
				JSON.stringify({
					data: [page],
					next: page === 2 ? "list?page=3" : undefined,
				}),
				{
					status: 200,
					headers: new Headers({
						"Content-Type": "application/json",
						...(page === 1 && { Link: '</rel/list?page=2>; rel="next"' }),
					}),
				},
			);
		};
		try {
			const output = await streamToArray(
				fetchResponseStream({
					url: "https://example.org/rel/list",
					dataPath: "data",
					nextPath: "next",
					maxPages: 3,
				}),
			);
			deepStrictEqual(output, [1, 2, 3]);
			deepStrictEqual(urls, [
				"https://example.org/rel/list",
				"https://example.org/rel/list?page=2",
				"https://example.org/rel/list?page=3",
			]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream picks the rel=next target from a multi-link Link header`, async (_t) => {
		const originalFetch = global.fetch;
		const cases = [
			'<https://example.org/lk?page=1>; rel="prev", <https://example.org/lk?page=3>; rel="next"',
			'<https://example.org/lk?page=3>; rel=next, <https://example.org/lk?page=9>; rel="last"',
			"<https://example.org/lk?page=3>; rel=next;title=n",
			'<https://example.org/lk?page=9>; rel="last", <https://example.org/lk?page=3>; rel="next last"',
			'<https://example.org/lk?page=8>; title="x", <https://example.org/lk?page=3>; rel="next"',
		];
		fetchSetDefaults({ dataPath: "data" });
		try {
			for (const link of cases) {
				global.fetch = async (url) => {
					if (url === "https://example.org/lk") {
						return new Response(JSON.stringify({ data: ["first"] }), {
							status: 200,
							headers: new Headers({
								"Content-Type": "application/json",
								Link: link,
							}),
						});
					}
					return new Response(JSON.stringify({ data: [url] }), {
						status: 200,
						headers: new Headers({ "Content-Type": "application/json" }),
					});
				};
				const output = await streamToArray(
					fetchResponseStream({ url: "https://example.org/lk" }),
				);
				deepStrictEqual(
					output,
					["first", "https://example.org/lk?page=3"],
					link,
				);
			}
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	const jsonBody = (chunks, { headers = {}, onCancel = () => {} } = {}) => {
		const encoder = new TextEncoder();
		const queue = chunks.map((chunk) =>
			typeof chunk === "string" ? encoder.encode(chunk) : chunk,
		);
		return new Response(
			new ReadableStream({
				pull(controller) {
					if (queue.length) controller.enqueue(queue.shift());
					else controller.close();
				},
				cancel: onCancel,
			}),
			{
				status: 200,
				headers: new Headers({
					"Content-Type": "application/json",
					...headers,
				}),
			},
		);
	};
	const readJson = (...maxBodySize) =>
		streamToArray(
			fetchResponseStream({
				url: "https://example.org/big?token=secret",
				dataPath: "",
				rateLimit: 0,
				...(maxBodySize.length && { maxBodySize: maxBodySize[0] }),
			}),
		);
	const tooLargeMessage = (max) =>
		`fetch GET https://example.org/big?[REDACTED] response body exceeds maxBodySize (${max} bytes)`;

	test(`fetchResponseStream rejects a JSON body whose Content-Length exceeds maxBodySize`, async (_t) => {
		let cancelled = 0;
		global.fetch = async () =>
			jsonBody(["[1]"], {
				headers: { "Content-Length": "100" },
				onCancel: () => {
					cancelled++;
				},
			});
		let error;
		try {
			await readJson(10);
		} catch (e) {
			error = e;
		}
		strictEqual(error?.message, tooLargeMessage(10));
		strictEqual(error?.name, "RangeError");
		strictEqual(cancelled, 1);
	});

	test(`fetchResponseStream applies a 16MiB maxBodySize by default and null disables it`, async (_t) => {
		let length;
		global.fetch = async () =>
			jsonBody(["[1]"], { headers: { "Content-Length": `${length}` } });
		length = 16_777_216;
		deepStrictEqual(await readJson(), [1]);
		length = 16_777_217;
		let error;
		try {
			await readJson();
		} catch (e) {
			error = e;
		}
		strictEqual(error?.message, tooLargeMessage(16_777_216));
		deepStrictEqual(await readJson(null), [1]);
		error = undefined;
		try {
			await readJson(undefined);
		} catch (e) {
			error = e;
		}
		strictEqual(error?.message, tooLargeMessage(16_777_216));
	});

	test(`fetchResponseStream counts JSON body bytes while reading against maxBodySize`, async (_t) => {
		let cancelled = 0;
		global.fetch = async () =>
			jsonBody(["[1,", "2", "]"], {
				onCancel: () => {
					cancelled++;
				},
			});
		deepStrictEqual(await readJson(5), [1, 2]);
		strictEqual(cancelled, 0);
		let error;
		try {
			await readJson(3);
		} catch (e) {
			error = e;
		}
		strictEqual(error?.message, tooLargeMessage(3));
		strictEqual(error?.name, "RangeError");
		strictEqual(cancelled, 1);
	});

	test(`fetchResponseStream decodes a JSON body with a character split across chunks`, async (_t) => {
		const bytes = new TextEncoder().encode('["\u00e9"]');
		global.fetch = async () =>
			jsonBody([bytes.subarray(0, 3), bytes.subarray(3)]);
		deepStrictEqual(await readJson(), ["\u00e9"]);
	});

	test(`fetchResponseStream rejects a pagination URL that repeats the current page`, async (_t) => {
		const cases = [
			[
				{ url: "https://example.org/self?token=secret" },
				{ Link: '<https://example.org/self?token=secret>; rel="next"' },
			],
			[{ url: "https://example.org/self?token=secret", nextPath: "next" }, {}],
		];
		for (const [options, headers] of cases) {
			const urls = [];
			global.fetch = async (url) => {
				urls.push(url);
				return new Response(JSON.stringify({ data: [1], next: url }), {
					status: 200,
					headers: new Headers({
						"Content-Type": "application/json",
						...headers,
					}),
				});
			};
			let error;
			try {
				await streamToArray(
					fetchResponseStream({ ...options, dataPath: "data", rateLimit: 0 }),
				);
			} catch (e) {
				error = e;
			}
			strictEqual(urls.length, 1, JSON.stringify(options));
			ok(
				/^fetch GET https:\/\/example\.org\/self\?\[REDACTED\] pagination next URL repeats the current page$/.test(
					error?.message,
				),
				`${JSON.stringify(options)}: ${error?.message}`,
			);
		}
	});

	test(`fetchResponseStream stops with an error after maxPages pages`, async (_t) => {
		const urls = [];
		global.fetch = async (url) => {
			urls.push(url);
			const page = Number(new URL(url).searchParams.get("page"));
			return new Response(JSON.stringify({ data: [page] }), {
				status: 200,
				headers: new Headers({
					"Content-Type": "application/json",
					...(page < 3 && { Link: `<?page=${page + 1}>; rel="next"` }),
				}),
			});
		};
		const read = (maxPages) =>
			streamToArray(
				fetchResponseStream({
					url: "https://example.org/pages?page=1",
					dataPath: "data",
					rateLimit: 0,
					maxPages,
				}),
			);
		deepStrictEqual(await read(3), [1, 2, 3]);
		urls.length = 0;
		const output = [];
		let error;
		try {
			for await (const item of fetchResponseStream({
				url: "https://example.org/pages?page=1",
				dataPath: "data",
				rateLimit: 0,
				maxPages: 2,
			})) {
				output.push(item);
			}
		} catch (e) {
			error = e;
		}
		deepStrictEqual(output, [1, 2]);
		deepStrictEqual(urls, [
			"https://example.org/pages?page=1",
			"https://example.org/pages?page=2",
		]);
		strictEqual(
			error?.message,
			"fetch GET https://example.org/pages?[REDACTED] exceeded maxPages (2)",
		);
		strictEqual(error?.name, "RangeError");
	});

	test(`fetchResponseStream parses the rel parameter of a Link header strictly`, async (_t) => {
		const originalFetch = global.fetch;
		const p2 = "https://example.org/lk?page=2";
		const cases = [
			[`<${p2}>; title="rel=next"`, []],
			[`<${p2}>; rel="nofollow"; foo="rel=next"`, []],
			[`<${p2}>; title="x rel=next"`, []],
			[`<${p2}>; rel=; rel=next`, []],
			[`<${p2}>; rel=next; title=x`, [p2]],
			[`<${p2}>;rel=next`, [p2]],
			[`<${p2}>; REL="next"`, [p2]],
			[`<${p2}>; rel=NEXT`, [p2]],
			[`<${p2}>; rel = "next"`, [p2]],
			[`<${p2}>; rel="next"; title="a>b"`, [p2]],
			[`<${p2}>;rel=next, <https://example.org/lk?page=3>; rel=last`, [p2]],
			[`<<${p2}>; rel="next"`, [p2]],
			[`<${p2}; rel="next"`, []],
			[`<${p2}>`, []],
			[`garbage>; rel="next"`, []],
			["<".repeat(100_000), []],
		];
		fetchSetDefaults({ dataPath: "data" });
		try {
			for (const [link, expected] of cases) {
				global.fetch = async (url) => {
					if (url === "https://example.org/lk") {
						return new Response(JSON.stringify({ data: [] }), {
							status: 200,
							headers: new Headers({
								"Content-Type": "application/json",
								Link: link,
							}),
						});
					}
					return new Response(JSON.stringify({ data: [url] }), {
						status: 200,
						headers: new Headers({ "Content-Type": "application/json" }),
					});
				};
				const output = await streamToArray(
					fetchResponseStream({ url: "https://example.org/lk", rateLimit: 0 }),
				);
				deepStrictEqual(output, expected, link.slice(0, 80));
			}
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	test(`fetchResponseStream resets the 429 retry count after each successful page`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const originalFetch = global.fetch;
		const calls = [];
		global.fetch = async (url) => {
			calls.push(url);
			if (calls.filter((u) => u === url).length === 1) {
				return new Response("", { status: 429 });
			}
			const page = Number(new URL(url).searchParams.get("page"));
			return new Response(JSON.stringify({ data: [page] }), {
				status: 200,
				headers: new Headers({
					"Content-Type": "application/json",
					...(page < 3 && {
						Link: `<https://example.org/rc?page=${page + 1}>; rel="next"`,
					}),
				}),
			});
		};
		try {
			const output = await streamToArray(
				fetchResponseStream({
					url: "https://example.org/rc?page=1",
					dataPath: "data",
					rateLimit: 0,
					retryMaxCount: 2,
				}),
			);
			deepStrictEqual(output, [1, 2, 3]);
			strictEqual(calls.length, 6);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchRateLimit does not wait when rateLimitTimestamp is in the past`, async (_t) => {
		const { fetchRateLimit } = await import("@datastream/fetch");
		const originalFetch = global.fetch;
		const fetchCallTimes = [];
		global.fetch = async () => {
			fetchCallTimes.push(Date.now());
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({ rateLimit: 0 });
		try {
			const past = Date.now() - 1000;
			await fetchRateLimit({
				url: "https://example.org/rl-past1",
				rateLimitTimestamp: past,
				rateLimit: 0,
			});
			await fetchRateLimit({
				url: "https://example.org/rl-past2",
				rateLimitTimestamp: past,
				rateLimit: 0,
			});
			ok(fetchCallTimes.length === 2);
			ok(
				fetchCallTimes[1] - fetchCallTimes[0] < 200,
				`Expected minimal delay, got ${fetchCallTimes[1] - fetchCallTimes[0]}ms`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchRateLimit without __origin derives origin from URL for redirect SSRF check`, async (_t) => {
		const originalFetch = global.fetch;
		let ssrfAttempted = false;
		global.fetch = async (url) => {
			if (url === "https://example.org/no-origin-redirect") {
				return new Response("", {
					status: 302,
					statusText: "Found",
					headers: new Headers({ Location: "http://169.254.169.254/metadata" }),
				});
			}
			if (url === "http://169.254.169.254/metadata") {
				ssrfAttempted = true;
				return new Response("creds", { status: 200 });
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/no-origin-redirect" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("redirect") || e.message.includes("origin"),
				`expected redirect/origin error, got: ${e.message}`,
			);
			strictEqual(
				ssrfAttempted,
				false,
				"SSRF redirect was followed even without __origin",
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit with explicit redirect option passes it through to fetch`, async (_t) => {
		const originalFetch = global.fetch;
		let seenRedirectOption;
		global.fetch = async (_url, init) => {
			seenRedirectOption = init.redirect;
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({
				url: "https://example.org/explicit-follow",
				redirect: "follow",
			});
			strictEqual(seenRedirectOption, "follow");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit does not loop on 302 response without Location header`, async (_t) => {
		const originalFetch = global.fetch;
		let callCount = 0;
		global.fetch = async (url) => {
			callCount++;
			if (url === "https://example.org/redirect-no-location") {
				return new Response("", {
					status: 302,
					statusText: "Found",
					headers: new Headers({}),
				});
			}
			return originalFetch(url);
		};
		fetchSetDefaults({ dataPath: "" });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/redirect-no-location" },
			]);
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("302"), `Expected 302 error, got: ${e.message}`);
			strictEqual(callCount, 1, `Expected 1 fetch call, got ${callCount}`);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchRateLimit blocks after exactly maxRedirects (20) same-origin redirects`, async (_t) => {
		const originalFetch = global.fetch;
		let redirectCount = 0;
		global.fetch = async (url) => {
			redirectCount++;
			if (url.startsWith("https://example.org/redir-")) {
				const n = parseInt(url.split("-").pop(), 10);
				return new Response("", {
					status: 302,
					statusText: "Found",
					headers: new Headers({
						Location: `https://example.org/redir-${n + 1}`,
					}),
				});
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/redir-0" });
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.name, "RangeError");
			ok(
				e.message.includes("exceeded") || e.message.includes("redirect"),
				`Expected max redirect error, got: ${e.message}`,
			);
			ok(
				redirectCount >= 20 && redirectCount <= 22,
				`Expected ~21 redirect calls (>20), got ${redirectCount}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit max-redirect error has correct cause fields`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url.startsWith("https://example.org/cause-redir-")) {
				const n = parseInt(url.split("-").pop(), 10);
				return new Response("", {
					status: 301,
					headers: new Headers({
						Location: `https://example.org/cause-redir-${n + 1}`,
					}),
				});
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/cause-redir-0" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.cause !== undefined, "cause should be defined");
			strictEqual(typeof e.cause.status, "number");
			strictEqual(typeof e.cause.url, "string");
			strictEqual(typeof e.cause.method, "string");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit throws for invalid redirect Location with correct cause`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/bad-location") {
				return new Response("", {
					status: 301,
					headers: new Headers({ Location: "not-a-valid::url" }),
				});
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/bad-location" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.length > 0,
				`Expected non-empty error message, got: ${e.message}`,
			);
			ok(e.cause !== undefined, "cause should be defined");
			strictEqual(typeof e.cause.status, "number");
			strictEqual(typeof e.cause.url, "string");
			strictEqual(typeof e.cause.method, "string");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit cross-origin redirect error cause has location and origin fields`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/ssrf-cause") {
				return new Response("", {
					status: 302,
					headers: new Headers({ Location: "http://169.254.169.254/meta" }),
				});
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/ssrf-cause" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.cause !== undefined, "cause should be defined");
			strictEqual(typeof e.cause.status, "number");
			strictEqual(typeof e.cause.url, "string");
			strictEqual(typeof e.cause.location, "string");
			strictEqual(typeof e.cause.origin, "string");
			strictEqual(typeof e.cause.method, "string");
			strictEqual(e.cause.origin, "https://example.org");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit throws at exactly retryMaxCount retries not retryMaxCount+1`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const originalFetch = global.fetch;
		let callCount = 0;
		global.fetch = () => {
			callCount++;
			return Promise.resolve(
				new Response("", { status: 429, statusText: "Too Many Requests" }),
			);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({ rateLimit: 0 });
		try {
			await fetchRateLimit({
				url: "https://example.org/retry-exact",
				rateLimit: 0,
				retryMaxCount: 3,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("max retries"),
				`Expected max retries error, got: ${e.message}`,
			);
			strictEqual(
				callCount,
				3,
				`Expected exactly 3 fetch calls (retryMaxCount=3), got ${callCount}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchRateLimit max retry error cause has correct fields`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const originalFetch = global.fetch;
		global.fetch = () =>
			Promise.resolve(
				new Response("", { status: 429, statusText: "Too Many Requests" }),
			);
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({ rateLimit: 0 });
		try {
			await fetchRateLimit({
				url: "https://example.org/retry-cause",
				rateLimit: 0,
				retryMaxCount: 1,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.cause !== undefined, "cause should be defined");
			strictEqual(e.cause.status, 429);
			strictEqual(typeof e.cause.url, "string");
			strictEqual(typeof e.cause.method, "string");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchRateLimit non-ok error cause has status, url, method fields`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = () =>
			Promise.resolve(
				new Response("", { status: 501, statusText: "Not Implemented" }),
			);
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/501" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.cause !== undefined, "cause should be defined");
			strictEqual(e.cause.status, 501);
			strictEqual(typeof e.cause.url, "string");
			strictEqual(typeof e.cause.method, "string");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchResponseStream without Retry-After retries successfully after backoff`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const originalFetch = global.fetch;
		let callCount = 0;
		global.fetch = (url) => {
			callCount++;
			if (callCount === 1 && url === "https://example.org/no-retry-after") {
				return Promise.resolve(
					new Response("", { status: 429, statusText: "Too Many Requests" }),
				);
			}
			return Promise.resolve(
				new Response(JSON.stringify({ ok: true }), {
					status: 200,
					headers: new Headers({ "Content-Type": "application/json" }),
				}),
			);
		};
		fetchSetDefaults({ rateLimit: 0 });
		try {
			const stream = fetchResponseStream([
				{
					url: "https://example.org/no-retry-after",
					rateLimit: 0,
					retryMaxCount: 10,
					dataPath: "",
				},
			]);
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ ok: true }]);
			strictEqual(callCount, 2);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchResponseStream with empty string dataPath returns whole body as single item`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			return new Response(JSON.stringify({ root: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({ dataPath: "" });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/root-path", dataPath: "" },
			]);
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ root: true }]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream with null in dataPath chain does not throw (optional chaining)`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			return new Response(JSON.stringify({ data: null }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		fetchSetDefaults({});
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/null-path", dataPath: "data.items" },
			]);
			const output = await streamToArray(stream);
			deepStrictEqual(output, [undefined]);
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream stops pagination cleanly when nextPath returns null`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response(JSON.stringify({ data: [{ id: 1 }], next: null }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		fetchSetDefaults({ dataPath: "data", nextPath: "next" });
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/null-next-vpu",
			});
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ id: 1 }]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined, nextPath: undefined });
		}
	});

	test(`fetchResponseStream stops pagination cleanly when nextPath returns undefined`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response(JSON.stringify({ data: [{ id: 2 }] }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		fetchSetDefaults({ dataPath: "data", nextPath: "next" });
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/undefined-next-vpu",
			});
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ id: 2 }]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined, nextPath: undefined });
		}
	});

	test(`fetchRateLimit does not enter wait branch when timestamp is in the past (if(true) mutant)`, async (_t) => {
		const { fetchRateLimit } = await import("@datastream/fetch");
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		fetchSetDefaults({ rateLimit: 0 });
		const aborted = AbortSignal.abort();
		try {
			const response = await fetchRateLimit(
				{
					url: "https://example.org/rl-gate-past",
					rateLimitTimestamp: Date.now() - 5000,
					rateLimit: 0,
				},
				{ signal: aborted },
			);
			ok(
				response.ok,
				"gate wrongly entered the wait branch (if(true) or aborted signal)",
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchRateLimit uses strict < (not <=) at the now===timestamp boundary`, async (_t) => {
		const { fetchRateLimit } = await import("@datastream/fetch");
		const originalFetch = global.fetch;
		const originalNow = Date.now;
		const fixed = 1_000_000_000_000;
		Date.now = () => fixed;
		global.fetch = async () =>
			new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		fetchSetDefaults({ rateLimit: 0 });
		const aborted = AbortSignal.abort();
		try {
			const response = await fetchRateLimit(
				{
					url: "https://example.org/rl-gate-equal",
					rateLimitTimestamp: fixed,
					rateLimit: 0,
				},
				{ signal: aborted },
			);
			ok(
				response.ok,
				"gate entered wait branch at now===timestamp boundary (<= mutant)",
			);
		} finally {
			Date.now = originalNow;
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`redactUrl replaces username with [REDACTED] not empty string`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response("", { status: 404, statusText: "Not Found" });
		fetchSetDefaults({});
		const { fetchRateLimit } = await import("@datastream/fetch");
		try {
			await fetchRateLimit({ url: "https://adminuser@example.org/secret-u" });
			throw new Error("Should have thrown");
		} catch (e) {
			const causeUrl = e.cause.url;
			strictEqual(
				causeUrl.includes("adminuser"),
				false,
				`cause.url should not expose username, got: ${causeUrl}`,
			);
			const hasAnyRedacted =
				causeUrl.includes("REDACTED") ||
				causeUrl.includes("%5B") ||
				causeUrl.includes("%5BREDACTED");
			strictEqual(
				hasAnyRedacted,
				true,
				`cause.url should have REDACTED marker (not empty username), got: ${causeUrl}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`redactUrl replaces password with [REDACTED] not empty string`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response("", { status: 404, statusText: "Not Found" });
		fetchSetDefaults({});
		const { fetchRateLimit } = await import("@datastream/fetch");
		try {
			await fetchRateLimit({ url: "https://:mysecretpw@example.org/path-p" });
			throw new Error("Should have thrown");
		} catch (e) {
			const causeUrl = e.cause.url;
			strictEqual(
				causeUrl.includes("mysecretpw"),
				false,
				`cause.url should not expose password, got: ${causeUrl}`,
			);
			const hasAnyRedacted =
				causeUrl.includes("REDACTED") ||
				causeUrl.includes("%5B") ||
				causeUrl.includes("%5BREDACTED");
			strictEqual(
				hasAnyRedacted,
				true,
				`cause.url should have REDACTED marker (not empty password), got: ${causeUrl}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit does not delay when rateLimitTimestamp equals current time`, async (_t) => {
		const { fetchRateLimit } = await import("@datastream/fetch");
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		fetchSetDefaults({ rateLimit: 0 });
		try {
			const exactNow = Date.now();
			const start = Date.now();
			await fetchRateLimit({
				url: "https://example.org/rl-equal-now",
				rateLimitTimestamp: exactNow,
				rateLimit: 0,
			});
			const elapsed = Date.now() - start;
			ok(
				elapsed < 200,
				`Expected no delay when rateLimitTimestamp==now, got ${elapsed}ms`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchRateLimit with redirect:error returns 302 not following (manageRedirects false)`, async (_t) => {
		const originalFetch = global.fetch;
		let callCount = 0;
		global.fetch = async (_url) => {
			callCount++;
			if (callCount === 1) {
				return new Response("", {
					status: 302,
					headers: new Headers({
						Location: "https://example.org/redirect-final",
					}),
				});
			}
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({
				url: "https://example.org/redirect-error-opt3",
				redirect: "error",
			});
			throw new Error("MANAGETEST_WRONGPATH");
		} catch (e) {
			ok(
				!e.message.includes("MANAGETEST_WRONGPATH"),
				`Mutant incorrectly followed redirect; error: ${e.message}`,
			);
			strictEqual(
				e.cause?.status,
				302,
				`Expected cause.status=302 from 302 response, got: ${e.cause?.status}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit allows exactly 20 same-origin redirects (> not >=)`, async (_t) => {
		const originalFetch = global.fetch;
		let callCount = 0;
		global.fetch = async (url) => {
			callCount++;
			if (url.startsWith("https://example.org/exact20-redir-")) {
				const n = Number.parseInt(url.split("-").pop(), 10);
				if (n < 20) {
					return new Response("", {
						status: 302,
						headers: new Headers({
							Location: `https://example.org/exact20-redir-${n + 1}`,
						}),
					});
				}
				return new Response(JSON.stringify({ done: true }), {
					status: 200,
					headers: new Headers({ "Content-Type": "application/json" }),
				});
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			const response = await fetchRateLimit({
				url: "https://example.org/exact20-redir-0",
			});
			ok(
				response.ok,
				"Expected success after exactly 20 same-origin redirects",
			);
			strictEqual(
				callCount,
				21,
				`Expected 21 fetch calls (20 redirects + final), got ${callCount}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit invalid Location error message contains "invalid redirect Location"`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/bad-loc-msg2") {
				return new Response("", {
					status: 302,
					headers: new Headers({ Location: "http://[invalid" }),
				});
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/bad-loc-msg2" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("invalid redirect Location"),
				`Expected 'invalid redirect Location' in message, got: ${e.message}`,
			);
			ok(e.cause !== undefined, "Expected cause object");
			strictEqual(
				e.cause.status,
				302,
				`Expected status 302, got: ${e.cause.status}`,
			);
			strictEqual(typeof e.cause.url, "string");
			strictEqual(typeof e.cause.method, "string");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit invalid Location cause has populated status url method (not empty object)`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/bad-loc-cause2") {
				return new Response("", {
					status: 307,
					headers: new Headers({ Location: "https://[incomplete" }),
				});
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/bad-loc-cause2" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.cause !== undefined, "Expected cause object");
			strictEqual(
				e.cause.status,
				307,
				`Expected status 307, got: ${e.cause.status}`,
			);
			strictEqual(
				e.cause.method,
				"GET",
				`Expected method GET, got: ${e.cause.method}`,
			);
			ok(
				typeof e.cause.url === "string" && e.cause.url.length > 0,
				`Expected non-empty cause.url, got: ${e.cause.url}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit handles null response body on 4xx error path`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			const r = new Response("", { status: 404, statusText: "Not Found" });
			Object.defineProperty(r, "body", { value: null, configurable: true });
			return r;
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/null-body-4xx" });
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.cause.status, 404);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit handles null response body on 429 retry path (before retry not max)`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const originalFetch = global.fetch;
		let callCount = 0;
		global.fetch = async () => {
			callCount++;
			if (callCount === 1) {
				const r = new Response("", {
					status: 429,
					statusText: "Too Many Requests",
				});
				Object.defineProperty(r, "body", { value: null, configurable: true });
				return r;
			}
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({ rateLimit: 0 });
		try {
			const response = await fetchRateLimit({
				url: "https://example.org/null-body-429-retry",
				rateLimit: 0,
				retryMaxCount: 3,
			});
			ok(response.ok, "Expected success after retry with null-body 429");
			strictEqual(callCount, 2, `Expected 2 calls (1 retry), got ${callCount}`);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchRateLimit handles null response body on cross-origin redirect`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/null-body-cors-redir") {
				const r = new Response("", {
					status: 302,
					headers: new Headers({ Location: "http://169.254.169.254/meta" }),
				});
				Object.defineProperty(r, "body", { value: null, configurable: true });
				return r;
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/null-body-cors-redir" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("redirect") || e.message.includes("origin"),
				`Expected redirect/origin error, got: ${e.message}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit handles null response body on max-redirect exceeded`, async (_t) => {
		const originalFetch = global.fetch;
		let n = 0;
		global.fetch = async () => {
			n++;
			const r = new Response("", {
				status: 301,
				headers: new Headers({
					Location: `https://example.org/null-max-redir-${n}`,
				}),
			});
			Object.defineProperty(r, "body", { value: null, configurable: true });
			return r;
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/null-max-redir-0" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("exceeded") || e.message.includes("redirect"),
				`Expected max redirect error, got: ${e.message}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit handles null response body on invalid redirect Location`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/null-body-bad-loc") {
				const r = new Response("", {
					status: 301,
					headers: new Headers({ Location: "http://[invalid" }),
				});
				Object.defineProperty(r, "body", { value: null, configurable: true });
				return r;
			}
			return originalFetch(url);
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/null-body-bad-loc" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(
				e.message.includes("invalid redirect Location"),
				`Expected "invalid redirect Location" in message, got: ${e.message}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchResponseStream waits exactly as long as Retry-After says`, async (t) => {
		const now = Date.parse("Wed, 21 Oct 2026 07:28:00 GMT");
		t.mock.method(Date, "now", () => now);
		t.mock.method(Math, "random", () => 0.5);
		const realSetTimeout = globalThis.setTimeout;
		const delays = [];
		t.mock.method(globalThis, "setTimeout", (fn, ms) => {
			delays.push(ms);
			return realSetTimeout(fn, 0);
		});
		const cases = [
			["0", 0],
			["12", 12_000],
			["Wed, 21 Oct 2026 07:28:05 GMT", 5000],
			["Wed, 21 Oct 2026 07:27:55 GMT", 0],
			["3000000", 60_000],
			["Wed, 21 Oct 2026 08:28:00 GMT", 60_000],
			["soon", 500],
			["a1", 500],
			["1a", 500],
		];
		const originalFetch = global.fetch;
		try {
			for (const [retryAfter, expected] of cases) {
				let calls = 0;
				global.fetch = async () => {
					calls++;
					if (calls === 1) {
						return new Response("", {
							status: 429,
							headers: new Headers({ "Retry-After": retryAfter }),
						});
					}
					return new Response(JSON.stringify({ ok: true }), {
						status: 200,
						headers: new Headers({ "Content-Type": "application/json" }),
					});
				};
				delays.length = 0;
				const output = await streamToArray(
					fetchResponseStream({
						url: "https://example.org/retry-after-exact",
						rateLimit: 0,
						dataPath: "",
					}),
				);
				deepStrictEqual(output, [{ ok: true }]);
				deepStrictEqual(delays, [expected], `Retry-After: ${retryAfter}`);
			}
		} finally {
			global.fetch = originalFetch;
		}
	});

	test(`fetchResponseStream clamps Retry-After to the retryAfterMax option`, async (t) => {
		const realSetTimeout = globalThis.setTimeout;
		const delays = [];
		t.mock.method(globalThis, "setTimeout", (fn, ms) => {
			delays.push(ms);
			return realSetTimeout(fn, 0);
		});
		let calls = 0;
		global.fetch = async () =>
			++calls === 1
				? new Response("", {
						status: 429,
						headers: new Headers({ "Retry-After": "12" }),
					})
				: new Response(JSON.stringify({ ok: true }), {
						status: 200,
						headers: new Headers({ "Content-Type": "application/json" }),
					});
		await streamToArray(
			fetchResponseStream({
				url: "https://example.org/retry-after-max",
				rateLimit: 0,
				dataPath: "",
				retryAfterMax: 5000,
			}),
		);
		deepStrictEqual(delays, [5000]);
	});

	test(`fetchResponseStream honours any Retry-After when retryAfterMax is null`, async (t) => {
		const realSetTimeout = globalThis.setTimeout;
		const delays = [];
		t.mock.method(globalThis, "setTimeout", (fn, ms) => {
			delays.push(ms);
			return realSetTimeout(fn, 0);
		});
		for (const [retryAfter, expected] of [
			["120", 120_000],
			["3000000", 2_147_483_647],
		]) {
			let calls = 0;
			global.fetch = async () =>
				++calls === 1
					? new Response("", {
							status: 429,
							headers: new Headers({ "Retry-After": retryAfter }),
						})
					: new Response(JSON.stringify({ ok: true }), {
							status: 200,
							headers: new Headers({ "Content-Type": "application/json" }),
						});
			delays.length = 0;
			await streamToArray(
				fetchResponseStream({
					url: "https://example.org/retry-after-null",
					rateLimit: 0,
					dataPath: "",
					retryAfterMax: null,
				}),
			);
			deepStrictEqual(delays, [expected], `Retry-After: ${retryAfter}`);
		}
	});

	test(`fetchResponseStream retries 429 without limit when retryMaxCount is null`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		let calls = 0;
		global.fetch = async () =>
			++calls <= 11
				? new Response("", { status: 429 })
				: new Response(JSON.stringify({ ok: true }), {
						status: 200,
						headers: new Headers({ "Content-Type": "application/json" }),
					});
		const output = await streamToArray(
			fetchResponseStream({
				url: "https://example.org/retry-null",
				rateLimit: 0,
				dataPath: "",
				retryMaxCount: null,
			}),
		);
		deepStrictEqual(output, [{ ok: true }]);
		strictEqual(calls, 12);
	});

	test(`fetchResponseStream follows pagination without limit when maxPages is null`, async (_t) => {
		global.fetch = async (url) => {
			const page = Number(new URL(url).searchParams.get("page"));
			return new Response(JSON.stringify({ data: [page] }), {
				status: 200,
				headers: new Headers({
					"Content-Type": "application/json",
					...(page < 3 && { Link: `<?page=${page + 1}>; rel="next"` }),
				}),
			});
		};
		const output = await streamToArray(
			fetchResponseStream({
				url: "https://example.org/pages?page=1",
				dataPath: "data",
				rateLimit: 0,
				maxPages: null,
			}),
		);
		deepStrictEqual(output, [1, 2, 3]);
	});

	test(`fetchResponseStream 429 backoff doubles per retry`, async (t) => {
		t.mock.method(Math, "random", () => 0.5);
		const realSetTimeout = globalThis.setTimeout;
		const delays = [];
		t.mock.method(globalThis, "setTimeout", (fn, ms) => {
			delays.push(ms);
			return realSetTimeout(fn, 0);
		});
		let calls = 0;
		global.fetch = async () =>
			++calls <= 3
				? new Response("", { status: 429 })
				: new Response(JSON.stringify({ ok: true }), {
						status: 200,
						headers: new Headers({ "Content-Type": "application/json" }),
					});
		const output = await streamToArray(
			fetchResponseStream({
				url: "https://example.org/backoff",
				rateLimit: 0,
				dataPath: "",
			}),
		);
		deepStrictEqual(output, [{ ok: true }]);
		deepStrictEqual(delays, [500, 1000, 2000]);
	});

	test(`fetchResponseStream 429 backoff is capped at 30000ms`, async (t) => {
		t.mock.method(Math, "random", () => 1);
		const realSetTimeout = globalThis.setTimeout;
		const delays = [];
		t.mock.method(globalThis, "setTimeout", (fn, ms) => {
			delays.push(ms);
			return realSetTimeout(fn, 0);
		});
		let calls = 0;
		global.fetch = async () =>
			++calls <= 7
				? new Response("", { status: 429 })
				: new Response(JSON.stringify({ ok: true }), {
						status: 200,
						headers: new Headers({ "Content-Type": "application/json" }),
					});
		await streamToArray(
			fetchResponseStream({
				url: "https://example.org/backoff-cap",
				rateLimit: 0,
				dataPath: "",
			}),
		);
		deepStrictEqual(delays, [1000, 2000, 4000, 8000, 16_000, 30_000, 30_000]);
	});

	test(`fetchResponseStream with no dataPath yields whole body (pickPath default "" not "Stryker was here!")`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response(JSON.stringify({ myKey: 42 }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		fetchSetDefaults({ dataPath: undefined });
		try {
			const stream = fetchResponseStream([
				{ url: "https://example.org/pickpath-default" },
			]);
			const output = await streamToArray(stream);
			deepStrictEqual(output, [{ myKey: 42 }]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit error message shows [INVALID URL] when URL is completely unparseable`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response("", { status: 404, statusText: "Not Found" });
		fetchSetDefaults({});
		const { fetchRateLimit } = await import("@datastream/fetch");
		try {
			await fetchRateLimit({ url: "not-a-valid-url-no-scheme" });
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(
				e.message,
				"fetch GET [INVALID URL] is not an http: or https: URL",
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	test(`fetchRateLimit handles null response body on 429 max-retries path`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const originalFetch = global.fetch;
		global.fetch = async () => {
			const r = new Response("", {
				status: 429,
				statusText: "Too Many Requests",
			});
			Object.defineProperty(r, "body", { value: null, configurable: true });
			return r;
		};
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({ rateLimit: 0 });
		try {
			await fetchRateLimit({
				url: "https://example.org/null-body-429-max",
				rateLimit: 0,
				retryMaxCount: 1,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(
				e.cause.status,
				429,
				`Expected cause.status=429, got: ${e.cause?.status}`,
			);
			ok(
				e.message.includes("max retries"),
				`Expected "max retries" in message, got: ${e.message}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ rateLimit: 0.01 });
		}
	});

	test(`fetchResponseStream concurrency keeps exactly \`concurrency\` requests in flight`, async (_t) => {
		fetchSetDefaults({ rateLimit: 0 });
		fetchSetDefaults({ dataPath: "" });
		const originalFetch = global.fetch;
		const starts = [];
		let releaseFirst;
		const firstReleased = new Promise((resolve) => {
			releaseFirst = resolve;
		});
		global.fetch = async (url) => {
			starts.push(new URL(url).searchParams.get("v"));
			if (starts.length === 1) await firstReleased;
			return jsonResponse({});
		};
		try {
			const config = [1, 2, 3].map((v) => ({
				url: `https://example.org/w?v=${v}`,
				rateLimit: 0,
			}));
			config[0].concurrency = 2;
			const run = streamToArray(fetchResponseStream(config));
			await new Promise((resolve) => setTimeout(resolve, 20));
			deepStrictEqual(starts, ["1", "2"]);
			releaseFirst();
			await run;
			deepStrictEqual(starts, ["1", "2", "3"]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	test(`fetchResponseStream concurrency copes with fewer items than the window`, async (_t) => {
		fetchSetDefaults({ rateLimit: 0 });
		fetchSetDefaults({ dataPath: "" });
		const originalFetch = global.fetch;
		let calls = 0;
		global.fetch = async () => {
			calls++;
			return jsonResponse({ only: true });
		};
		try {
			const output = await streamToArray(
				fetchResponseStream([
					{ url: "https://example.org/one", concurrency: 2 },
				]),
			);
			deepStrictEqual(output, [{ only: true }]);
			strictEqual(calls, 1);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	const okJson = () =>
		new Response(JSON.stringify({ ok: true }), {
			status: 200,
			headers: new Headers({ "Content-Type": "application/json" }),
		});
	const recordDelays = (t) => {
		const realSetTimeout = globalThis.setTimeout;
		const delays = [];
		t.mock.method(globalThis, "setTimeout", (fn, ms) => {
			delays.push(ms);
			return realSetTimeout(fn, 0);
		});
		return delays;
	};

	test(`fetchRateLimit retries 500, 502, 503 and 504 for idempotent methods`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		for (const status of [500, 502, 503, 504]) {
			for (const method of ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "get"]) {
				let calls = 0;
				global.fetch = async () =>
					++calls === 1 ? new Response("", { status }) : okJson();
				const response = await fetchModule.fetchRateLimit({
					url: "https://example.org/flaky",
					method,
					rateLimit: 0,
				});
				strictEqual(response.status, 200, `${status} ${method}`);
				strictEqual(calls, 2, `${status} ${method}`);
			}
		}
	});

	test(`fetchRateLimit does not retry 5xx for non-idempotent methods or other 5xx statuses`, async (_t) => {
		for (const [status, method] of [
			[503, "POST"],
			[500, "PATCH"],
			[501, "GET"],
			[505, "GET"],
		]) {
			let calls = 0;
			global.fetch = async () => {
				calls++;
				return new Response("", { status });
			};
			await rejects(
				fetchModule.fetchRateLimit({
					url: "https://example.org/once",
					method,
					rateLimit: 0,
				}),
				{
					message: `fetch ${status} ${method} https://example.org/once`,
					cause: { status, url: "https://example.org/once", method },
				},
			);
			strictEqual(calls, 1, `${status} ${method}`);
		}
	});

	test(`fetchRateLimit throws after retryMaxCount attempts on a persistent 503`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		let calls = 0;
		global.fetch = async () => {
			calls++;
			return new Response("down", { status: 503 });
		};
		await rejects(
			fetchModule.fetchRateLimit({
				url: "https://example.org/down?token=secret",
				rateLimit: 0,
				retryMaxCount: 3,
			}),
			(error) => {
				strictEqual(error instanceof RangeError, true);
				strictEqual(
					error.message,
					"fetch 503 GET https://example.org/down?[REDACTED] max retries (3) exceeded",
				);
				deepStrictEqual(error.cause, {
					status: 503,
					url: "https://example.org/down?[REDACTED]",
					method: "GET",
				});
				return true;
			},
		);
		strictEqual(calls, 3);
	});

	test(`fetchRateLimit honours Retry-After on a 503 and backs off exponentially otherwise`, async (t) => {
		t.mock.method(Math, "random", () => 0.5);
		const delays = recordDelays(t);
		let calls = 0;
		global.fetch = async () => {
			calls++;
			if (calls === 1) {
				return new Response("", {
					status: 503,
					headers: new Headers({ "Retry-After": "7" }),
				});
			}
			if (calls < 4) return new Response("", { status: 502 });
			return okJson();
		};
		const response = await fetchModule.fetchRateLimit({
			url: "https://example.org/backoff",
			rateLimit: 0,
		});
		strictEqual(response.status, 200);
		deepStrictEqual(delays, [7000, 1000, 2000]);
	});

	test(`fetchRateLimit retries a network error (TypeError) for idempotent methods`, async (t) => {
		t.mock.method(Math, "random", () => 0.5);
		const delays = recordDelays(t);
		let calls = 0;
		global.fetch = async () => {
			if (++calls < 3) throw new TypeError("fetch failed");
			return okJson();
		};
		const response = await fetchModule.fetchRateLimit({
			url: "https://example.org/net",
			method: "DELETE",
			rateLimit: 0,
		});
		strictEqual(response.status, 200);
		strictEqual(calls, 3);
		deepStrictEqual(delays, [500, 1000]);
	});

	test(`fetchRateLimit rethrows the network error once retryMaxCount attempts are spent`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const errors = [];
		global.fetch = async () => {
			const error = new TypeError("fetch failed");
			errors.push(error);
			throw error;
		};
		await rejects(
			fetchModule.fetchRateLimit({
				url: "https://example.org/net-down",
				rateLimit: 0,
				retryMaxCount: 2,
			}),
			(error) => error === errors[1],
		);
		strictEqual(errors.length, 2);
	});

	test(`fetchRateLimit does not retry a network error for POST, a non-TypeError, or an aborted signal`, async (t) => {
		t.mock.method(Math, "random", () => 0);
		const controller = new AbortController();
		const reason = new TypeError("caller gave up");
		controller.abort(reason);
		const cases = [
			[{ method: "POST" }, {}, new TypeError("fetch failed")],
			[{}, {}, new Error("boom")],
			[{}, {}, new DOMException("aborted", "AbortError")],
			[{}, { signal: controller.signal }, reason],
		];
		for (const [options, streamOptions, thrown] of cases) {
			let calls = 0;
			global.fetch = async () => {
				calls++;
				throw thrown;
			};
			await rejects(
				fetchModule.fetchRateLimit(
					{ url: "https://example.org/no-retry", rateLimit: 0, ...options },
					streamOptions,
				),
				(error) => error === thrown,
			);
			strictEqual(calls, 1, thrown.message);
		}
	});

	test(`fetchWritableStream does not retry a 503 or network error with a streamed body`, async (_t) => {
		for (const failure of [
			() => new Response("", { status: 503 }),
			() => {
				throw new TypeError("fetch failed");
			},
		]) {
			let calls = 0;
			global.fetch = async () => {
				calls++;
				return failure();
			};
			const stream = await fetchWritableStream({
				url: "https://example.org/upload-flaky",
				method: "PUT",
			});
			await rejects(
				pipeline([createReadableStream(["a"]), stream]),
				/^(TypeError: fetch failed|Error: fetch 503 PUT https:\/\/example\.org\/upload-flaky)$/,
			);
			strictEqual(calls, 1);
		}
	});

	test(`fetchRateLimit timeout aborts the request with a TimeoutError and is not retried`, async (_t) => {
		let calls = 0;
		global.fetch = (_url, init) => {
			calls++;
			return new Promise((_resolve, reject) => {
				init.signal.addEventListener("abort", () => reject(init.signal.reason));
			});
		};
		await rejects(
			fetchModule.fetchRateLimit({
				url: "https://example.org/slow",
				rateLimit: 0,
				timeout: 5,
			}),
			{ name: "TimeoutError" },
		);
		strictEqual(calls, 1);
	});

	test(`fetchRateLimit timeout is combined with the caller signal`, async (_t) => {
		const signals = [];
		global.fetch = async (_url, init) => {
			signals.push(init.signal);
			return okJson();
		};
		const controller = new AbortController();
		const { signal } = controller;
		const url = "https://example.org/sig";
		await fetchModule.fetchRateLimit({ url, rateLimit: 0 }, { signal });
		await fetchModule.fetchRateLimit(
			{ url, rateLimit: 0, timeout: null },
			{ signal },
		);
		await fetchModule.fetchRateLimit({ url, rateLimit: 0 });
		await fetchModule.fetchRateLimit(
			{ url, rateLimit: 0, timeout: 60_000 },
			{ signal },
		);
		await fetchModule.fetchRateLimit({ url, rateLimit: 0, timeout: 60_000 });
		strictEqual(signals[0], signal);
		strictEqual(signals[1], signal);
		strictEqual(signals[2], undefined);
		ok(signals[3] instanceof AbortSignal);
		notStrictEqual(signals[3], signal);
		strictEqual(signals[3].aborted, false);
		ok(signals[4] instanceof AbortSignal);
		strictEqual(signals[4].aborted, false);
		const reason = new Error("stop");
		controller.abort(reason);
		strictEqual(signals[3].aborted, true);
		strictEqual(signals[3].reason, reason);
		strictEqual(signals[4].aborted, false);
	});

	test(`fetchResponseStream passes timeout through to each request`, async (_t) => {
		global.fetch = (_url, init) =>
			new Promise((_resolve, reject) => {
				init.signal.addEventListener("abort", () => reject(init.signal.reason));
			});
		await rejects(
			streamToArray(
				fetchResponseStream({
					url: "https://example.org/slow",
					rateLimit: 0,
					timeout: 5,
				}),
			),
			{ name: "TimeoutError" },
		);
	});

	test(`fetchRateLimit rejects non-http(s) URLs before fetching`, async (_t) => {
		let calls = 0;
		global.fetch = async () => {
			calls++;
			return okJson();
		};
		for (const [url, shown] of [
			["file:///etc/passwd", "file:///etc/passwd"],
			["data:text/plain,hi", "data:text/plain,hi"],
			["javascript:alert(1)", "javascript:alert(1)"],
			["ftp://example.org/x?token=secret", "ftp://example.org/x?[REDACTED]"],
			["blob:https://example.org/uuid", "blob:https://example.org/uuid"],
			["not a url", "[INVALID URL]"],
			["/relative/path", "[INVALID URL]"],
		]) {
			await rejects(
				fetchModule.fetchRateLimit({ url, rateLimit: 0 }),
				(error) => {
					strictEqual(error instanceof TypeError, true, url);
					strictEqual(
						error.message,
						`fetch GET ${shown} is not an http: or https: URL`,
					);
					return true;
				},
			);
		}
		strictEqual(calls, 0);
		for (const url of [
			"http://example.org/a",
			"https://example.org/b",
			"HTTPS://example.org/c",
		]) {
			strictEqual(
				(await fetchModule.fetchRateLimit({ url, rateLimit: 0 })).status,
				200,
			);
		}
		strictEqual(calls, 3);
	});

	test(`fetchRateLimit resolves relative URLs against globalThis.location`, async (_t) => {
		const urls = [];
		global.fetch = async (url) => {
			urls.push(url);
			return okJson();
		};
		const descriptor = Object.getOwnPropertyDescriptor(globalThis, "location");
		try {
			Object.defineProperty(globalThis, "location", {
				value: { href: "https://app.example.org/page/" },
				configurable: true,
			});
			strictEqual(
				(await fetchModule.fetchRateLimit({ url: "/api/items", rateLimit: 0 }))
					.status,
				200,
			);
			deepStrictEqual(urls, ["/api/items"]);
			Object.defineProperty(globalThis, "location", {
				value: { href: "file:///index.html" },
				configurable: true,
			});
			await rejects(fetchModule.fetchRateLimit({ url: "x", rateLimit: 0 }), {
				name: "TypeError",
				message: "fetch GET file:///x is not an http: or https: URL",
			});
		} finally {
			if (descriptor) Object.defineProperty(globalThis, "location", descriptor);
			else delete globalThis.location;
		}
	});

	test(`fetchResponseStream and fetchWritableStream reject non-http(s) URLs`, async (_t) => {
		let calls = 0;
		global.fetch = async () => {
			calls++;
			return okJson();
		};
		await rejects(
			streamToArray(fetchResponseStream({ url: "file:///etc/passwd" })),
			{
				name: "TypeError",
				message: "fetch GET file:///etc/passwd is not an http: or https: URL",
			},
		);
		const stream = await fetchWritableStream({
			url: "data:text/plain,x",
			method: "PUT",
		});
		await rejects(pipeline([createReadableStream(["a"]), stream]), {
			name: "TypeError",
			message: "fetch PUT data:text/plain,x is not an http: or https: URL",
		});
		strictEqual(calls, 0);
	});

	test(`fetchResponseStream refuses a non-http(s) pagination next URL`, async (_t) => {
		let calls = 0;
		global.fetch = async () => {
			calls++;
			return new Response(
				JSON.stringify({ items: [1], next: "file:///etc/passwd" }),
				{
					status: 200,
					headers: new Headers({ "Content-Type": "application/json" }),
				},
			);
		};
		await rejects(
			streamToArray(
				fetchResponseStream({
					url: "https://example.org/page",
					nextPath: "next",
					dataPath: "items",
					rateLimit: 0,
				}),
			),
			{
				message:
					"Pagination URL origin (null) does not match initial URL origin (https://example.org)",
			},
		);
		strictEqual(calls, 1);
	});
});
