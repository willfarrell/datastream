/* global Headers, Response */

import { deepStrictEqual, ok, strictEqual } from "node:assert";
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
	const nodeTest = variant === "node" ? test : test.skip;
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
	// global override
	global.fetch = (url, _request) => {
		const mockResponse = mockResponses[url]();
		if (mockResponse) {
			return Promise.resolve(mockResponse);
		}
		throw new Error("mock missing");
	};
	const suiteFetch = global.fetch;
	// Every test starts from the suite mock and the built-in defaults, so no
	// test depends on (or is broken by) state a previous test left behind.
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

	// *** built-in default Accept header (literal, before any pollution) *** //
	// MUST run before any fetchSetDefaults({headers:{Accept:...}}) call below so it
	// observes the un-overridden literal default `Accept: "application/json"`.
	// Kills the StringLiteral mutant that empties the literal to "".
	test(`built-in default Accept header is non-empty application/json`, async (_t) => {
		const originalFetch = global.fetch;
		let capturedHeaders;
		global.fetch = async (_url, init) => {
			capturedHeaders = init.headers;
			return new Response(JSON.stringify({ ok: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		};
		try {
			// Do NOT touch Accept anywhere: rely entirely on the literal default.
			const stream = fetchResponseStream([
				{ url: "https://example.org/builtin-accept", dataPath: "" },
			]);
			await streamToArray(stream);
			strictEqual(capturedHeaders.Accept, "application/json");
		} finally {
			global.fetch = originalFetch;
		}
	});

	// *** fetchResponseStream *** //
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
			// without qs the url is left untouched
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
		// rateLimit 0: pacing is covered by its own test (and a pacing
		// regression then fails fast instead of waiting 1000 / 0.01 ms).
		fetchSetDefaults({ rateLimit: 0 });
		fetchSetDefaults({ dataPath: "", headers: { Accept: "application/json" } });
		const originalFetch = global.fetch;
		// item 1 resolves SLOWER than 2/3 — output must still be [1,2,3], and forces a refill
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
		// Freeze the clock and record each scheduled wait instead of measuring
		// wall-clock fetch() times, which a busy event loop can bunch together.
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
			// Every start is spaced from the previous one by the previous item's
			// rateLimit, including the refill: starts at +0ms, +30ms, +80ms.
			deepStrictEqual(waits, [0, 30, 80]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined });
		}
	});

	test(`fetchResponseStream concurrency surfaces an item error and settles the window`, async (_t) => {
		// rateLimit 0: pacing is covered by its own test (and a pacing
		// regression then fails fast instead of waiting 1000 / 0.01 ms).
		fetchSetDefaults({ rateLimit: 0 });
		fetchSetDefaults({ dataPath: "" });
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url.includes("bad"))
				return new Response("nope", {
					status: 500,
					headers: new Headers({ "Content-Type": "application/json" }),
				});
			await new Promise((r) => setTimeout(r, 30)); // still in flight when the error hits
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
			ok(/ 500 /.test(error.message), error.message);
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
					status: 500,
					headers: new Headers({ "Content-Type": "application/json" }),
				});
			await new Promise((r) => setTimeout(r, 30)); // head still in flight when the error hits
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
			strictEqual(error?.message, "fetch 500 GET https://example.org/bad");
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

	test(`fetchResponseStream should retry on 429 status`, async (t) => {
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
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
			// message is redacted
			ok(!error.message.includes("token=abc123"), "message leaked query token");
			ok(!error.message.includes("user:pass"), "message leaked credentials");
			// cause.url MUST be redacted too
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
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
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

	// *** SSRF: redirect handling *** //
	test(`fetchResponseStream should block cross-origin redirects (SSRF)`, async (_t) => {
		const originalFetch = global.fetch;
		let metadataFetched = false;
		global.fetch = async (url, init) => {
			if (url === "https://example.org/redirect-ssrf") {
				// Simulate a server 302-redirecting to a cloud metadata endpoint.
				// With redirect:"manual" the platform returns the 3xx unfollowed.
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

	// Browsers answer redirect:"manual" with an opaque redirect (status 0, no
	// Location), so the redirect is followed and its final url checked instead.
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
			// the next page resolves against where the redirect ended up
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
			// A finite body, so a regression that lets the redirect through ends
			// the stream (and fails the assertions) instead of hanging on a read.
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

	// Re-issuing a POST with redirect:"follow" would send the body twice and
	// cross-origin hops would already have happened, so only GET/HEAD retry.
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

	test(`fetchResponseStream should honor explicit redirect option (opt-in follow)`, async (_t) => {
		const originalFetch = global.fetch;
		let sawFollow = false;
		global.fetch = async (url, init) => {
			if (url === "https://example.org/redirect-optin") {
				sawFollow = init.redirect === "follow";
				// With redirect:"follow" the platform resolves the redirect itself,
				// so the mock just returns the final response.
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

	// *** fetchWritableStream *** //
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
			// Drain the duplex request body to completion in the background.
			// If end-of-body is never signaled, this never resolves and the
			// test times out.
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
		// end() waited for the response (checked first: if end-of-body were
		// never signalled, awaiting bodyDone below would hang instead of fail)
		strictEqual(stream.result().value.status, 200);

		const received = await bodyDone;
		deepStrictEqual(received, ["alpha", "beta"]);

		global.fetch = originalFetch;
	});

	test(`fetchWritableStream does not wait for the response before accepting the body`, {
		timeout: 2000,
	}, async (_t) => {
		const originalFetch = global.fetch;
		// Like a server that reads the full request body before responding.
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

	// An early rejection (e.g. an immediate 401) must surface on the next write
	// instead of the unread request body filling up to its queue limit.
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
			// nothing was queued on the abandoned request body
			if (variant === "node") strictEqual(requestBody.readableLength, 0);
		} finally {
			global.fetch = originalFetch;
		}
	});

	// After a clean end the body belongs to fetch until it has read it all.
	nodeTest(
		`fetchWritableStream leaves the request body readable after a normal end`,
		async (_t) => {
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
				await pipeline([createReadableStream(["a"]), stream]);
				await new Promise((resolve) => setTimeout(resolve, 0));
				strictEqual(requestBody.destroyed, false);
				strictEqual(stream.result().value.status, 200);
				const received = [];
				for await (const chunk of requestBody) received.push(chunk);
				deepStrictEqual(received, ["a"]);
			} finally {
				global.fetch = originalFetch;
			}
		},
	);

	// A fetch mock that, like a real upload, reads the request body and only
	// settles when its signal aborts. `bodyRead` resolves to "ended" on a clean
	// end of body, or to the error that stopped the read.
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
	// The body read's outcome, or "pending" if it is still open a macrotask
	// later (a body left open would otherwise hang the test).
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
		const bodyError = await outcome(upload.bodyRead);
		if (variant === "browser") {
			strictEqual(bodyError, reason);
		} else {
			// node destroys a signalled Readable with an AbortError carrying the reason
			strictEqual(bodyError.name, "AbortError");
			strictEqual(bodyError.cause, reason);
		}
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

	test(`fetchRateLimit should delay between configs within one stream`, async (t) => {
		// Frozen clock + recorded waits instead of measuring wall-clock gaps.
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
		// A single config array of >=2 entries so the in-generator
		// rateLimitTimestamp carries over between the two requests.
		const config = [
			{ url: "https://example.org/test1", rateLimit: 0.1, dataPath: "" },
			{ url: "https://example.org/test2", rateLimit: 0.1, dataPath: "" },
		];
		await streamToArray(fetchResponseStream(config));
		deepStrictEqual(urls, [
			"https://example.org/test1",
			"https://example.org/test2",
		]);
		// rateLimit 0.1 => the second request waits 100ms, the first not at all.
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
			// Direct call with no rateLimit in options: timestamp must NOT be NaN.
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
			// offsetAmount intentionally omitted → paginateUsingQuery returns undefined
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

	// *** fetchRateLimit 429 max retry regression *** //
	test(`fetchRateLimit should throw after max retries on persistent 429`, async (t) => {
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
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

	// *** JSON pagination cleanup on downstream error *** //
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
			// On error, the JSON generator must be returned so pagination stops;
			// page 2 must never be fetched.
			strictEqual(page2Fetched, false);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ dataPath: undefined, nextPath: undefined });
		}
	});

	// *** Binary branch cleanup on downstream error ***
	// When the response is NON-JSON, fetchGenerator iterates the raw ReadableStream
	// (response.body), which has .cancel() but NO .return(). A downstream consumer
	// error must propagate; the catch block calls response?.cancel?.() then
	// response?.return?.(). The ReadableStream lacks .return, so the inner optional
	// chaining on `.return` MUST be preserved — a mutant turning `response?.return?.()`
	// into `response?.return()` would throw "response.return is not a function" and
	// mask the real "downstream boom" error.
	test(`fetchResponseStream binary branch cleanup on read error preserves original error`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/binary-cancel") {
				// text/csv → non-JSON → binary branch: fetchGenerator iterates
				// response.body directly. We install a body that HAS .cancel()
				// (resolving cleanly) but NO .return(), and whose async iterator
				// throws — so the catch runs, response?.cancel?.() succeeds, and we
				// reach response?.return?.() with response lacking .return.
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
			// Original code: response?.return?.() no-ops (stream lacks .return), so the
			// ORIGINAL read error surfaces. Mutant `response?.return()` calls a
			// non-function and throws "response.return is not a function", masking it.
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

	// *** Null response cleanup: catch fires with response === null/undefined ***
	// A non-JSON response whose body is null makes fetchUnknown return null, so
	// `for await (const chunk of null)` throws and the catch block runs with
	// `response === null`. The outer optional chaining `response?.cancel?.()` and
	// `response?.return?.()` MUST be preserved: a mutant removing the first `?.`
	// (response.cancel?.() / response.return()) dereferences null and throws a
	// "Cannot read properties of null" TypeError, replacing the genuine iteration
	// error. We assert the surfaced error is the iteration error, not the null deref.
	test(`fetchResponseStream null response body surfaces iteration error not null-deref`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () => {
			const r = new Response("ignored", {
				status: 200,
				headers: new Headers({ "Content-Type": "text/csv" }),
			});
			// Force a null body so fetchUnknown returns null and iteration throws.
			Object.defineProperty(r, "body", { value: null, configurable: true });
			return r;
		};
		fetchSetDefaults({ headers: { Accept: "text/csv" } });
		try {
			const stream = fetchResponseStream({
				url: "https://example.org/null-binary-body",
			});
			await streamToArray(stream);
			throw new Error("Should have thrown");
		} catch (e) {
			// Original code: re-throws the iteration error (null is not iterable).
			// Mutant (response.cancel?.()): throws "Cannot read properties of null
			// (reading 'cancel')" BEFORE reaching `throw error`.
			ok(
				!/reading 'cancel'|reading 'return'/.test(e.message),
				`cleanup dereferenced null instead of guarding it: ${e.message}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({ headers: { Accept: "application/json" } });
		}
	});

	// *** SSRF protection: same-origin pagination *** //
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
		// Existing Link header tests (json-obj/2 → json-obj/3) already verify this,
		// but adding an explicit test for clarity.
		fetchSetDefaults({ dataPath: "", headers: { Accept: "application/json" } });
		const config = [{ url: "https://example.org/json-obj/2" }];
		const stream = fetchResponseStream(config);
		const output = await streamToArray(stream);

		deepStrictEqual(output, [
			{ key: "item", value: 2 },
			{ key: "item", value: 3 },
		]);
	});

	// Named exports are canonical; a default export must not come back.
	test(`fetch has no default export`, (_t) => {
		strictEqual(Object.hasOwn(fetchModule, "default"), false);
	});

	// *** validatePaginationUrl: invalid URL catch block and error message *** //
	test(`fetchResponseStream should throw with message for invalid pagination URL in Link header`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async (url) => {
			if (url === "https://example.org/bad-link") {
				return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
					status: 200,
					headers: new Headers({
						"Content-Type": "application/json",
						// unparseable even relative to the current url
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
			// the raw value may carry secrets, so it is redacted
			strictEqual(e.message, "Invalid pagination URL");
		} finally {
			global.fetch = originalFetch;
		}
	});

	// *** redactUrl: username/password redaction uses [REDACTED] not empty string *** //
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

	// *** defaults headers: Accept and Accept-Encoding must not be empty *** //
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

	// *** mergeOptions: headers must be merged object, not {} *** //
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

	// *** duplex: "half" not empty string *** //
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

	// *** URL + → %20 replacement *** //
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

	// *** JSON content-type regex: must anchor at start *** //
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

	// *** JSON content-type regex: json must be followed by end-of-string or `;` ***
	// `application/jsonl` (JSON Lines) shares the `application/json` prefix but is a
	// distinct, line-delimited (binary) format. The trailing `($|;)` group ensures
	// it is NOT mis-detected as a single JSON document. Kills a mutant that drops
	// the group (matching any `application/json...` suffix).
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

	// *** JSON content-type regex: end anchor allows ; but not random suffix *** //
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

	// *** Link header match: optional chaining on null match result *** //
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
					// bounded, so a regression that keeps paginating fails fast
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
			// GitHub style: prev listed before next
			'<https://example.org/lk?page=1>; rel="prev", <https://example.org/lk?page=3>; rel="next"',
			// unquoted rel directly followed by another link
			'<https://example.org/lk?page=3>; rel=next, <https://example.org/lk?page=9>; rel="last"',
			// unquoted rel directly followed by another parameter
			"<https://example.org/lk?page=3>; rel=next;title=n",
			// rel with several space-separated relation types
			'<https://example.org/lk?page=9>; rel="last", <https://example.org/lk?page=3>; rel="next last"',
			// a link without any rel parameter
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

	// JSON pages are buffered whole before parsing, so their size is capped.
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
				// readJson() leaves maxBodySize unset; readJson(x) sets it (even undefined)
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
		// An explicit undefined means "use the default", not unlimited.
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
		// exactly maxBodySize bytes is allowed
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

	// A server that links a page to itself would otherwise be fetched forever.
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
		// exactly maxPages pages is fine
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

	// RFC 8288: rel is a `;`-separated parameter (name and tokens are
	// case-insensitive, whitespace allowed around `=`); a "rel=next" inside
	// another parameter's value, or a later duplicate rel, must not count.
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
			// text before the first `<` is not a link
			[`garbage>; rel="next"`, []],
			// linear time: a header of only `<` used to take O(n^2)
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
		t.mock.method(Math, "random", () => 0); // zero backoff
		const originalFetch = global.fetch;
		const calls = [];
		global.fetch = async (url) => {
			calls.push(url);
			// the first request for every page is rate limited once
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

	// *** rateLimit: now < vs now <= timestamp *** //
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

	// *** initialOrigin: ?? vs && *** //
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

	// *** manageRedirects: explicit redirect option is passed through *** //
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

	// *** redirect while: && vs || condition *** //
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

	// *** maxRedirects: > vs >= and ++ vs -- *** //
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

	// *** maxRedirects error cause must have populated cause object *** //
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

	// *** invalid redirect Location: catch block and error cause *** //
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

	// *** cross-origin redirect error cause fields (location, origin) *** //
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

	// *** 429 retryCount >= vs > retryMaxCount *** //
	test(`fetchRateLimit throws at exactly retryMaxCount retries not retryMaxCount+1`, async (t) => {
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
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

	// *** 429 retry cause fields *** //
	test(`fetchRateLimit max retry error cause has correct fields`, async (t) => {
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
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

	// *** non-ok (non-429) error cause fields *** //
	test(`fetchRateLimit non-ok error cause has status, url, method fields`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = () =>
			Promise.resolve(
				new Response("", { status: 503, statusText: "Service Unavailable" }),
			);
		const { fetchRateLimit } = await import("@datastream/fetch");
		fetchSetDefaults({});
		try {
			await fetchRateLimit({ url: "https://example.org/503" });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.cause !== undefined, "cause should be defined");
			strictEqual(e.cause.status, 503);
			strictEqual(typeof e.cause.url, "string");
			strictEqual(typeof e.cause.method, "string");
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	// *** backoffMs: Math.random() * baseMs succeeds (retry works without crash) *** //
	test(`fetchResponseStream without Retry-After retries successfully after backoff`, async (t) => {
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
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

	// *** pickPath: default "" (not "Stryker was here!") and optional chaining *** //
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

	// *** validatePaginationUrl: null/undefined nextUrl must return without throwing *** //
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

	// *** rateLimit gate: `if (now < ts)` must NOT always-enter and `<` not `<=` ***
	// Timing alone cannot distinguish the mutants (the wait amount is <= 0 when the
	// gate would be wrongly entered, so setTimeout fires immediately). Instead we
	// pass an ALREADY-ABORTED signal: `timeout()` rejects synchronously on an
	// aborted signal, so any *unwanted* entry into the wait branch surfaces as an
	// "Aborted" rejection. Original code (gate false) skips the wait entirely.
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
			// rateLimitTimestamp strictly in the past → `now < ts` is false → no wait.
			// Mutant `if (true)` would call timeout({signal: aborted}) and REJECT.
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
			// now === rateLimitTimestamp exactly. Original `<` → false → no wait.
			// Mutant `<=` → true → timeout({signal: aborted}) → REJECT.
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

	// *** redactUrl: username must become [REDACTED] (or %5BREDACTED%5D) not empty string *** //
	test(`redactUrl replaces username with [REDACTED] not empty string`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response("", { status: 404, statusText: "Not Found" });
		fetchSetDefaults({});
		const { fetchRateLimit } = await import("@datastream/fetch");
		try {
			// NO query string so only username is tested (query-string [REDACTED] would mask the mutation)
			await fetchRateLimit({ url: "https://adminuser@example.org/secret-u" });
			throw new Error("Should have thrown");
		} catch (e) {
			// URL encodes [REDACTED] to %5BREDACTED%5D for username; empty string mutant produces https://@...
			const causeUrl = e.cause.url;
			// Real code: https://%5BREDACTED%5D@example.org/... — mutant: https://@example.org/...
			strictEqual(
				causeUrl.includes("adminuser"),
				false,
				`cause.url should not expose username, got: ${causeUrl}`,
			);
			// Must have some REDACTED marker; empty-string mutant has no REDACTED at all
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

	// *** redactUrl: password must become [REDACTED] (or %5BREDACTED%5D) not empty string *** //
	test(`redactUrl replaces password with [REDACTED] not empty string`, async (_t) => {
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response("", { status: 404, statusText: "Not Found" });
		fetchSetDefaults({});
		const { fetchRateLimit } = await import("@datastream/fetch");
		try {
			// URL with ONLY password (no username) so only the password mutation is tested
			// Real code: url.password="[REDACTED]" → %5BREDACTED%5D in URL; mutant: "" → no REDACTED
			await fetchRateLimit({ url: "https://:mysecretpw@example.org/path-p" });
			throw new Error("Should have thrown");
		} catch (e) {
			const causeUrl = e.cause.url;
			strictEqual(
				causeUrl.includes("mysecretpw"),
				false,
				`cause.url should not expose password, got: ${causeUrl}`,
			);
			// Real: %5BREDACTED%5D for password; empty-string mutant: https://user:@... (no REDACTED)
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

	// *** rateLimit: now < timestamp – timestamp equal to now must NOT trigger a wait *** //
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

	// *** manageRedirects: explicit redirect:error bypasses redirect loop *** //
	// When callerRedirect="error", manageRedirects=false and the redirect-management block is skipped.
	// If mutant changes "if (manageRedirects)" → "if (true)", the block is ALWAYS entered.
	// Difference: with a 302 response, real code returns 302 (not ok, throws error).
	// Mutant: enters redirect loop, follows to target, may return 200 (no error).
	test(`fetchRateLimit with redirect:error returns 302 not following (manageRedirects false)`, async (_t) => {
		const originalFetch = global.fetch;
		let callCount = 0;
		global.fetch = async (_url) => {
			callCount++;
			if (callCount === 1) {
				// Return 302 to same-origin target
				return new Response("", {
					status: 302,
					headers: new Headers({
						Location: "https://example.org/redirect-final",
					}),
				});
			}
			// If a second call is made (mutant follows redirect), return 200
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
			// Mutant (if true): follows redirect, returns 200 → no throw → reaches here (bad!)
			throw new Error("MANAGETEST_WRONGPATH");
		} catch (e) {
			// Real code: throws because 302 is not ok (redirect not followed, status=302 in cause)
			// Mutant: throws "MANAGETEST_WRONGPATH" because it followed the redirect and got 200 (no error)
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

	// *** redirectCount > maxRedirects: exactly 20 redirects must succeed *** //
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

	// *** invalid redirect Location error: message and cause must not be empty *** //
	// http://[invalid actually throws "Invalid URL" in new URL(loc, base) — unlike relative URLs that
	// parse with null origin and hit the cross-origin check first.
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

	// *** response.body?.cancel() – must not throw when body is null *** //
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
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
		// retryMaxCount: 3, first call returns 429 with null body → retryCount=1 < 3 → RETRY path (line 291)
		// NOT the max-retries path (line 279). The cancel at line 291 must handle null body gracefully.
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
			// 2nd call: success
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
		// http://[invalid actually throws "Invalid URL" triggering the catch block at line 241
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

	// RFC 9110: Retry-After is delay-seconds (0 included) or an HTTP-date.
	test(`fetchResponseStream waits exactly as long as Retry-After says`, async (t) => {
		const now = Date.parse("Wed, 21 Oct 2026 07:28:00 GMT");
		t.mock.method(Date, "now", () => now);
		t.mock.method(Math, "random", () => 0.5); // backoff without Retry-After: 0.5 * 1000
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
			// clamped to retryAfterMax (default 60s): larger values would also
			// overflow setTimeout's 2^31-1 ms and fire after 1ms
			["3000000", 60_000],
			["Wed, 21 Oct 2026 08:28:00 GMT", 60_000],
			// not delay-seconds nor a date: fall back to jittered backoff
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

	// retryAfterMax null lifts the cap, but a wait over 2^31-1 ms would overflow
	// setTimeout into a ~1ms wait, so it is still bounded there.
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
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
		let calls = 0;
		// 11 x 429: one more than the default retryMaxCount of 10.
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

	// Full jitter over 1000 * 2 ** (retry - 1): with random 0.5 the waits are
	// exactly half of 1000, 2000, 4000 — no wall clock involved.
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

	// The exponential base is capped at 30s (retry 7 would be 64s uncapped).
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

	// *** pickPath: default parameter "" vs "Stryker was here!" *** //
	test(`fetchResponseStream with no dataPath yields whole body (pickPath default "" not "Stryker was here!")`, async (_t) => {
		// When dataPath is undefined (not set), pickPath(body, undefined) uses default path="".
		// Real: path="" → returns body; Mutant: path="Stryker was here!" → body["Stryker was here!"] = undefined
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response(JSON.stringify({ myKey: 42 }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			});
		fetchSetDefaults({ dataPath: undefined });
		try {
			// No dataPath in options → uses default undefined → pickPath uses default ""
			const stream = fetchResponseStream([
				{ url: "https://example.org/pickpath-default" },
			]);
			const output = await streamToArray(stream);
			// Real: output is [{myKey:42}] (whole body); Mutant: output is [undefined] (body["Stryker was here!"])
			deepStrictEqual(output, [{ myKey: 42 }]);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	// *** redactUrl catch block: must return "[INVALID URL]" not empty string or undefined *** //
	test(`fetchRateLimit error cause.url is [INVALID URL] when URL is completely unparseable`, async (_t) => {
		// Passing an invalid URL string to fetchRateLimit causes redactUrl to throw in new URL(...)
		// and return "[INVALID URL]" from the catch block.
		// Mutant "return """: cause.url = "" (empty string)
		// Mutant "} catch {}": catch block is empty, function returns undefined → cause.url = undefined
		const originalFetch = global.fetch;
		global.fetch = async () =>
			new Response("", { status: 404, statusText: "Not Found" });
		fetchSetDefaults({});
		const { fetchRateLimit } = await import("@datastream/fetch");
		try {
			await fetchRateLimit({ url: "not-a-valid-url-no-scheme" });
			throw new Error("Should have thrown");
		} catch (e) {
			const causeUrl = e.cause?.url;
			strictEqual(
				causeUrl,
				"[INVALID URL]",
				`Expected cause.url to be "[INVALID URL]", got: ${causeUrl}`,
			);
		} finally {
			global.fetch = originalFetch;
			fetchSetDefaults({});
		}
	});

	// *** response.body?.cancel() on 429 max-retries path (line 279) *** //
	test(`fetchRateLimit handles null response body on 429 max-retries path`, async (t) => {
		t.mock.method(Math, "random", () => 0); // zero backoff, no real wait
		// retryMaxCount: 1, first call returns 429 with null body → retryCount=1 >= retryMaxCount=1 → MAX RETRIES path (line 279)
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

	// concurrency > 1 must actually overlap requests: with the first response held
	// back, exactly `concurrency` requests are in flight and the next one only
	// starts once a slot frees up.
	test(`fetchResponseStream concurrency keeps exactly \`concurrency\` requests in flight`, async (_t) => {
		// rateLimit 0: pacing is covered by its own test (and a pacing
		// regression then fails fast instead of waiting 1000 / 0.01 ms).
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
			// rateLimit 0: only the window, not pacing, may hold a request back.
			const config = [1, 2, 3].map((v) => ({
				url: `https://example.org/w?v=${v}`,
				rateLimit: 0,
			}));
			// concurrency is read from the first item and applies to the whole array
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
		// rateLimit 0: pacing is covered by its own test (and a pacing
		// regression then fails fast instead of waiting 1000 / 0.01 ms).
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
});
