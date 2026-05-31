/* global Headers, Response */

import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";
import {
	createPassThroughStream,
	createTransformStream,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";

import fetchDefault, {
	fetchResponseStream,
	fetchSetDefaults,
	fetchWritableStream,
} from "@datastream/fetch";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

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

// *** fetchResponseStream *** //
test(`${variant}: fetchResponseStream should fetch csv`, async (_t) => {
	fetchSetDefaults({ headers: { Accept: "text/csv" } });
	const config = [{ url: "https://example.org/csv" }];
	const stream = fetchResponseStream(config);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		Uint8Array.from("a,b,c\n1,2,3".split("").map((x) => x.charCodeAt())),
	]);
});

test(`${variant}: fetchResponseStream should fetch with qs`, async (_t) => {
	fetchSetDefaults({ headers: { Accept: "text/csv" } });
	const config = [{ url: "https://example.org/csv", qs: { delimiter: "_" } }];
	const stream = fetchResponseStream(config);
	const output = await streamToArray(stream);

	deepStrictEqual(output, [
		Uint8Array.from("a_b_c\n1_2_3".split("").map((x) => x.charCodeAt())),
	]);
});

test(`${variant}: fetchResponseStream should fetch json objects in parallel`, async (_t) => {
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

test(`${variant}: fetchResponseStream should fetch paginated json in series`, async (_t) => {
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

test(`${variant}: fetchResponseStream should work with pipejoin`, async (_t) => {
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

test(`${variant}: fetchResponseStream should work with pipeline`, async (_t) => {
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

test(`${variant}: fetchResponseStream should paginate using query parameters`, async () => {
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

test(`${variant}: fetchResponseStream should retry on 429 status`, async (_t) => {
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

test(`${variant}: fetchResponseStream should throw on non-ok response`, async (_t) => {
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

test(`${variant}: fetchResponseStream error cause must not leak the unredacted URL`, async (_t) => {
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

test(`${variant}: fetchRateLimit 429 max-retries error cause must not leak the unredacted URL`, async (_t) => {
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
test(`${variant}: fetchResponseStream should block cross-origin redirects (SSRF)`, async (_t) => {
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

test(`${variant}: fetchResponseStream cross-origin redirect error must not leak unredacted URL`, async (_t) => {
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

test(`${variant}: fetchResponseStream should follow same-origin redirects`, async (_t) => {
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

test(`${variant}: fetchResponseStream should honor explicit redirect option (opt-in follow)`, async (_t) => {
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
test(`${variant}: fetchWritableStream should create writable stream for upload`, async (_t) => {
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

	deepStrictEqual(typeof stream.write, "function");
	deepStrictEqual(typeof stream.end, "function");
	deepStrictEqual(typeof stream.result, "function");

	stream.write("test data");
	stream.end();

	const result = stream.result();
	deepStrictEqual(result.key, "output");

	global.fetch = originalFetch;
});

test(`${variant}: fetchWritableStream should close the request body on end`, async (_t) => {
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

	stream.write("alpha");
	stream.write("beta");
	stream.end();

	const received = await bodyDone;
	deepStrictEqual(received, ["alpha", "beta"]);

	global.fetch = originalFetch;
});

test(`${variant}: fetchRateLimit should delay between configs within one stream`, async (_t) => {
	const originalFetch = global.fetch;
	const fetchCallTimes = [];

	global.fetch = async (_url, _options) => {
		fetchCallTimes.push(Date.now());
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
	const stream = fetchResponseStream(config);
	await streamToArray(stream);

	global.fetch = originalFetch;

	strictEqual(fetchCallTimes.length, 2);
	// rateLimit 0.1 => ~100ms enforced delay between the two calls.
	ok(
		fetchCallTimes[1] - fetchCallTimes[0] >= 90,
		`expected >=~100ms delay between configs, got ${
			fetchCallTimes[1] - fetchCallTimes[0]
		}ms`,
	);
});

test(`${variant}: fetchRateLimit direct call defaults rateLimit before computing timestamp`, async (_t) => {
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

test(`${variant}: fetchResponseStream should handle content-type with suffix`, async (_t) => {
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

test(`${variant}: fetchResponseStream should handle offsetParam without offsetAmount`, async (_t) => {
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

test(`${variant}: fetchResponseStream should stop pagination when offset param is empty`, async (_t) => {
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

test(`${variant}: fetchResponseStream should handle dataPath as array`, async (_t) => {
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
test(`${variant}: fetchRateLimit should throw after max retries on persistent 429`, async (_t) => {
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
		ok(e.message.includes("max retries"));
		ok(callCount >= 2, `Expected at least 2 calls, got ${callCount}`);
	} finally {
		global.fetch = originalFetch;
	}
});

// *** JSON pagination cleanup on downstream error *** //
test(`${variant}: fetchResponseStream should stop JSON pagination when consumer errors`, async (_t) => {
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

// *** SSRF protection: same-origin pagination *** //
test(`${variant}: fetchResponseStream should reject Link header with cross-origin URL`, async (_t) => {
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

test(`${variant}: fetchResponseStream should reject nextPath with cross-origin URL`, async (_t) => {
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

test(`${variant}: fetchResponseStream should reject nextPath with different protocol`, async (_t) => {
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

test(`${variant}: fetchResponseStream should reject Link header with different port`, async (_t) => {
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

test(`${variant}: fetchResponseStream should allow same-origin pagination`, async (_t) => {
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

// *** default export *** //
test(`${variant}: default export should include all stream functions`, (_t) => {
	deepStrictEqual(Object.keys(fetchDefault).sort(), [
		"readableStream",
		"responseStream",
		"setDefaults",
	]);
});

// *** validatePaginationUrl: invalid URL catch block and error message *** //
test(`${variant}: fetchResponseStream should throw with message for invalid pagination URL in Link header`, async (_t) => {
	const originalFetch = global.fetch;
	global.fetch = async (url) => {
		if (url === "https://example.org/bad-link") {
			return new Response(JSON.stringify({ data: [{ id: 1 }] }), {
				status: 200,
				headers: new Headers({
					"Content-Type": "application/json",
					Link: '<not a valid url!!!>; rel="next"',
				}),
			});
		}
		return originalFetch(url);
	};
	fetchSetDefaults({ dataPath: "data" });
	try {
		const stream = fetchResponseStream({ url: "https://example.org/bad-link" });
		await streamToArray(stream);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(
			e.message.includes("Invalid pagination URL"),
			`expected invalid pagination URL error, got: ${e.message}`,
		);
	} finally {
		global.fetch = originalFetch;
	}
});

// *** redactUrl: username/password redaction uses [REDACTED] not empty string *** //
test(`${variant}: fetchResponseStream error cause.url contains [REDACTED] not empty credentials`, async (_t) => {
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
test(`${variant}: default headers include non-empty Accept and Accept-Encoding`, async (_t) => {
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
test(`${variant}: fetchSetDefaults merges headers correctly and preserves defaults`, async (_t) => {
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
test(`${variant}: fetchWritableStream sends duplex:"half" not empty string`, async (_t) => {
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
	stream.end();
	await new Promise((r) => setTimeout(r, 10));
	strictEqual(capturedInit.duplex, "half");
	global.fetch = originalFetch;
});

// *** URL + → %20 replacement *** //
test(`${variant}: fetchResponseStream replaces + with %20 in query string`, async (_t) => {
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
test(`${variant}: fetchResponseStream treats text/application-json (non-JSON) as binary`, async (_t) => {
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

// *** JSON content-type regex: end anchor allows ; but not random suffix *** //
test(`${variant}: fetchResponseStream treats application/json;charset=utf-8 as JSON`, async (_t) => {
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
test(`${variant}: fetchResponseStream handles Link header with no rel=next match`, async (_t) => {
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

// *** rateLimit: now < vs now <= timestamp *** //
test(`${variant}: fetchRateLimit does not wait when rateLimitTimestamp is in the past`, async (_t) => {
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
test(`${variant}: fetchRateLimit without __origin derives origin from URL for redirect SSRF check`, async (_t) => {
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
test(`${variant}: fetchRateLimit with explicit redirect option passes it through to fetch`, async (_t) => {
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
test(`${variant}: fetchRateLimit does not loop on 302 response without Location header`, async (_t) => {
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
test(`${variant}: fetchRateLimit blocks after exactly maxRedirects (20) same-origin redirects`, async (_t) => {
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
test(`${variant}: fetchRateLimit max-redirect error has correct cause fields`, async (_t) => {
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
test(`${variant}: fetchRateLimit throws for invalid redirect Location with correct cause`, async (_t) => {
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
test(`${variant}: fetchRateLimit cross-origin redirect error cause has location and origin fields`, async (_t) => {
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
test(`${variant}: fetchRateLimit throws at exactly retryMaxCount retries not retryMaxCount+1`, async (_t) => {
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
test(`${variant}: fetchRateLimit max retry error cause has correct fields`, async (_t) => {
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
test(`${variant}: fetchRateLimit non-ok error cause has status, url, method fields`, async (_t) => {
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

// *** Retry-After header: baseMs = retryAfter * 1000 (not / 1000) *** //
test(`${variant}: fetchResponseStream respects Retry-After header as seconds multiplied by 1000`, async (_t) => {
	const originalFetch = global.fetch;
	let callCount = 0;
	const startTime = Date.now();
	let secondCallTime;
	global.fetch = (url) => {
		callCount++;
		if (callCount === 1 && url === "https://example.org/retry-after") {
			return Promise.resolve(
				new Response("", {
					status: 429,
					statusText: "Too Many Requests",
					headers: new Headers({ "Retry-After": "1" }),
				}),
			);
		}
		secondCallTime = Date.now();
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
				url: "https://example.org/retry-after",
				rateLimit: 0,
				retryMaxCount: 2,
				dataPath: "",
			},
		]);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [{ ok: true }]);
		ok(
			secondCallTime - startTime >= 900,
			`Expected >=~1000ms delay from Retry-After:1, got ${secondCallTime - startTime}ms`,
		);
	} finally {
		global.fetch = originalFetch;
		fetchSetDefaults({ rateLimit: 0.01 });
	}
});

// *** backoffMs: Math.random() * baseMs succeeds (retry works without crash) *** //
test(`${variant}: fetchResponseStream without Retry-After retries successfully after backoff`, async (_t) => {
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
test(`${variant}: fetchResponseStream with empty string dataPath returns whole body as single item`, async (_t) => {
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

test(`${variant}: fetchResponseStream with null in dataPath chain does not throw (optional chaining)`, async (_t) => {
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
test(`${variant}: fetchResponseStream stops pagination cleanly when nextPath returns null`, async (_t) => {
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

test(`${variant}: fetchResponseStream stops pagination cleanly when nextPath returns undefined`, async (_t) => {
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

// *** redactUrl: username must become [REDACTED] (or %5BREDACTED%5D) not empty string *** //
test(`${variant}: redactUrl replaces username with [REDACTED] not empty string`, async (_t) => {
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
test(`${variant}: redactUrl replaces password with [REDACTED] not empty string`, async (_t) => {
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
test(`${variant}: fetchRateLimit does not delay when rateLimitTimestamp equals current time`, async (_t) => {
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
test(`${variant}: fetchRateLimit with redirect:error returns 302 not following (manageRedirects false)`, async (_t) => {
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
test(`${variant}: fetchRateLimit allows exactly 20 same-origin redirects (> not >=)`, async (_t) => {
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
		ok(response.ok, "Expected success after exactly 20 same-origin redirects");
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
test(`${variant}: fetchRateLimit invalid Location error message contains "invalid redirect Location"`, async (_t) => {
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

test(`${variant}: fetchRateLimit invalid Location cause has populated status url method (not empty object)`, async (_t) => {
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
test(`${variant}: fetchRateLimit handles null response body on 4xx error path`, async (_t) => {
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

test(`${variant}: fetchRateLimit handles null response body on 429 retry path (before retry not max)`, async (_t) => {
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

test(`${variant}: fetchRateLimit handles null response body on cross-origin redirect`, async (_t) => {
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

test(`${variant}: fetchRateLimit handles null response body on max-redirect exceeded`, async (_t) => {
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

test(`${variant}: fetchRateLimit handles null response body on invalid redirect Location`, async (_t) => {
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

// *** Retry-After || fallback: 0 Retry-After should use 1000ms fallback via || *** //
test(`${variant}: fetchResponseStream uses 1000ms fallback when Retry-After is "0" (0||1000 not 0&&1000)`, async (_t) => {
	// Number.parseInt("0", 10) * 1000 = 0, 0 || 1000 = 1000 (real)
	// Mutant (&&): 0 && 1000 = 0 → near-zero wait
	const originalFetch = global.fetch;
	let callCount = 0;
	const callTimes = [];
	global.fetch = (url) => {
		callCount++;
		callTimes.push(Date.now());
		if (callCount === 1 && url === "https://example.org/retry-after-zero") {
			return Promise.resolve(
				new Response("", {
					status: 429,
					statusText: "Too Many Requests",
					headers: new Headers({ "Retry-After": "0" }),
				}),
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
				url: "https://example.org/retry-after-zero",
				rateLimit: 0,
				retryMaxCount: 2,
				dataPath: "",
			},
		]);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [{ ok: true }]);
		ok(
			callTimes[1] - callTimes[0] >= 900,
			`Expected ~1000ms wait from 0||1000 fallback, got ${callTimes[1] - callTimes[0]}ms`,
		);
	} finally {
		global.fetch = originalFetch;
		fetchSetDefaults({ rateLimit: 0.01 });
	}
});

// *** backoff: 2 ** (retryCount-1) not 2 / (retryCount-1) *** //
test(`${variant}: fetchResponseStream second retry base wait is ~2000ms (2**1=2 not 2/1)`, async (_t) => {
	// retryCount=2: base = 1000 * 2**(2-1) = 2000ms (real)
	// mutant (/): base = 1000 / 2**(2-1) = 500ms
	// With Math.random=1.0: backoffMs = 1.0 * base
	const originalFetch = global.fetch;
	let callCount = 0;
	const callTimes = [];
	global.fetch = (url) => {
		callCount++;
		callTimes.push(Date.now());
		if (callCount <= 2 && url === "https://example.org/exp-backoff-det") {
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
	const origRandom = Math.random;
	Math.random = () => 1.0;
	fetchSetDefaults({ rateLimit: 0 });
	try {
		const stream = fetchResponseStream([
			{
				url: "https://example.org/exp-backoff-det",
				rateLimit: 0,
				retryMaxCount: 10,
				dataPath: "",
			},
		]);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [{ ok: true }]);
		// callTimes[2] - callTimes[1] = wait after retryCount=2
		// Real: 1.0 * 2000 = 2000ms; Mutant (2/): 1.0 * 500 = 500ms
		ok(
			callTimes[2] - callTimes[1] >= 1800,
			`Expected ~2000ms for retryCount=2 (2**1*1000), got ${callTimes[2] - callTimes[1]}ms`,
		);
	} finally {
		Math.random = origRandom;
		global.fetch = originalFetch;
		fetchSetDefaults({ rateLimit: 0.01 });
	}
});

// *** backoffMs: Math.random() * baseMs not Math.random() / baseMs *** //
test(`${variant}: fetchResponseStream jitter is Math.random()*baseMs not divided (random=1.0 test)`, async (_t) => {
	// Math.random=1.0: backoffMs = 1.0 * 1000 = 1000ms (real)
	// Mutant (/): 1.0 / 1000 = 0.001ms → essentially no wait
	const originalFetch = global.fetch;
	let callCount = 0;
	const callTimes = [];
	global.fetch = (url) => {
		callCount++;
		callTimes.push(Date.now());
		if (callCount === 1 && url === "https://example.org/jitter-mult") {
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
	const origRandom = Math.random;
	Math.random = () => 1.0;
	fetchSetDefaults({ rateLimit: 0 });
	try {
		const stream = fetchResponseStream([
			{
				url: "https://example.org/jitter-mult",
				rateLimit: 0,
				retryMaxCount: 10,
				dataPath: "",
			},
		]);
		const output = await streamToArray(stream);
		deepStrictEqual(output, [{ ok: true }]);
		ok(
			callTimes[1] - callTimes[0] >= 900,
			`Expected ~1000ms jitter (random=1.0 * 1000ms), got ${callTimes[1] - callTimes[0]}ms`,
		);
	} finally {
		Math.random = origRandom;
		global.fetch = originalFetch;
		fetchSetDefaults({ rateLimit: 0.01 });
	}
});

// *** pickPath: default parameter "" vs "Stryker was here!" *** //
test(`${variant}: fetchResponseStream with no dataPath yields whole body (pickPath default "" not "Stryker was here!")`, async (_t) => {
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
test(`${variant}: fetchRateLimit error cause.url is [INVALID URL] when URL is completely unparseable`, async (_t) => {
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
test(`${variant}: fetchRateLimit handles null response body on 429 max-retries path`, async (_t) => {
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
