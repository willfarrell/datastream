/* global Headers, Response */

import test from "node:test";
import { streamToArray } from "@datastream/core";
import { fetchResponseStream, fetchSetDefaults } from "@datastream/fetch";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = ["fetch 404 GET", "fetch 500 GET"];
	if (expectedErrors.some((msg) => e.message?.includes(msg))) {
		return;
	}
	console.error(input, e);
	throw e;
};

// Mock fetch for fuzz testing
const originalFetch = global.fetch;
global.fetch = (_url, _request) => {
	// Return JSON array response for any URL
	return Promise.resolve(
		new Response(
			JSON.stringify({
				data: [{ id: 1 }, { id: 2 }],
				items: [{ key: "a" }],
				nested: { deep: { values: [{ n: 1 }] } },
			}),
			{
				status: 200,
				statusText: "OK",
				headers: new Headers({
					"Content-Type": "application/json; charset=UTF-8",
				}),
			},
		),
	);
};

// *** fetchResponseStream w/ various dataPath *** //
test("fuzz fetchResponseStream w/ dataPath variations", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.constantFrom("", "data", "items", "nested.deep.values"),
			async (dataPath) => {
				try {
					fetchSetDefaults({
						rateLimit: 0,
						headers: { Accept: "application/json" },
					});
					const config = {
						url: "https://example.org/fuzz",
						dataPath,
					};
					const stream = fetchResponseStream(config);
					await streamToArray(stream);
				} catch (e) {
					catchError({ dataPath }, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

// *** fetchResponseStream w/ query string params *** //
test("fuzz fetchResponseStream w/ random qs params", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.object({ maxDepth: 0, maxKeys: 5, values: [fc.string()] }),
			async (qs) => {
				try {
					fetchSetDefaults({
						rateLimit: 0,
						headers: { Accept: "application/json" },
					});
					const config = {
						url: "https://example.org/fuzz",
						dataPath: "data",
						qs,
					};
					const stream = fetchResponseStream(config);
					await streamToArray(stream);
				} catch (e) {
					catchError({ qs }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** fetchResponseStream w/ multiple configs *** //
test("fuzz fetchResponseStream w/ array of configs", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					dataPath: fc.constantFrom("", "data", "items"),
				}),
				{ minLength: 1, maxLength: 5 },
			),
			async (configs) => {
				try {
					fetchSetDefaults({
						rateLimit: 0,
						headers: { Accept: "application/json" },
					});
					const fetchConfigs = configs.map((c) => ({
						url: "https://example.org/fuzz",
						...c,
					}));
					const stream = fetchResponseStream(fetchConfigs);
					await streamToArray(stream);
				} catch (e) {
					catchError(configs, e);
				}
			},
		),
		{
			numRuns: 100,
			verbose: 2,
			examples: [],
		},
	);
});

// *** fetchResponseStream w/ dataPath as array *** //
test("fuzz fetchResponseStream w/ dataPath as array", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(fc.constantFrom("nested", "deep", "values", "data", "items"), {
				minLength: 0,
				maxLength: 3,
			}),
			async (dataPath) => {
				try {
					fetchSetDefaults({
						rateLimit: 0,
						headers: { Accept: "application/json" },
					});
					const config = {
						url: "https://example.org/fuzz",
						dataPath,
					};
					const stream = fetchResponseStream(config);
					await streamToArray(stream);
				} catch (e) {
					catchError({ dataPath }, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// Restore original fetch
test.after(() => {
	global.fetch = originalFetch;
});
