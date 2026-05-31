// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global fetch */
import {
	createReadableStream,
	createWritableStream,
	timeout,
} from "@datastream/core";

const validatePaginationUrl = (nextUrl, origin) => {
	if (!nextUrl) return;
	let url;
	try {
		url = new URL(nextUrl);
	} catch {
		throw new Error(`Invalid pagination URL: ${nextUrl}`);
	}
	if (url.origin !== origin) {
		throw new Error(
			`Pagination URL origin (${url.origin}) does not match initial URL origin (${origin})`,
		);
	}
};

// URL.parse returns null (instead of throwing) on an unparseable URL, so the
// origin falls through to undefined without a try/catch — avoiding an
// error-swallowing catch body that is indistinguishable from an empty one.
const originOf = (urlString) => URL.parse(urlString)?.origin;

const redactUrl = (urlString) => {
	try {
		const url = new URL(urlString);
		if (url.search) url.search = "?[REDACTED]";
		if (url.username) url.username = "[REDACTED]";
		if (url.password) url.password = "[REDACTED]";
		return url.toString();
	} catch {
		return "[INVALID URL]";
	}
};

let defaults = {
	// custom
	rateLimit: 0.01, // 100 per sec
	dataPath: undefined, // for json response, where the data is to return form body root
	nextPath: undefined, // for json pagination, body root
	qs: {}, // object to convert to query string
	offsetParam: undefined, // offset query parameter to use for pagination
	offsetAmount: undefined, // offset amount to use for pagination

	// fetch
	method: "GET",
	headers: {
		Accept: "application/json",
		"Accept-Encoding": "br, gzip, deflate",
	},
};

const mergeOptions = (options = {}) => {
	return {
		...defaults,
		...options,
		headers: { ...defaults.headers, ...options.headers },
		qs: { ...defaults.qs, ...options.qs },
	};
};

export const fetchSetDefaults = (options) => {
	defaults = mergeOptions(options);
};

// Note: requires EncodeStream to ensure it's Uint8Array
// Poor browser support - https://github.com/Fyrd/caniuse/issues/6375
export const fetchWritableStream = async (options, streamOptions = {}) => {
	const body = createReadableStream();
	// Duplex: half - For browser compatibility - https://developer.chrome.com/articles/fetch-streaming-requests/#half-duplex
	options = mergeOptions(options);
	const value = await fetchRateLimit({
		...options,
		body,
		duplex: "half",
		signal: streamOptions.signal,
	});
	const write = (chunk) => {
		body.push(chunk);
	};
	// Signal end-of-body so the duplex request body terminates and the
	// in-flight upload can complete. createReadableStream treats a pushed
	// `null` as close on both the Node and Web builds.
	const final = () => {
		body.push(null);
	};
	const stream = createWritableStream(write, final, streamOptions);
	stream.result = () => ({ key: "output", value });
	return stream;
};
export const fetchRequestStream = fetchWritableStream;

export const fetchReadableStream = (fetchOptions, streamOptions = {}) => {
	return createReadableStream(
		fetchGenerator(fetchOptions, streamOptions),
		streamOptions,
	);
};
export const fetchResponseStream = fetchReadableStream;

async function* fetchGenerator(fetchOptions, streamOptions) {
	let rateLimitTimestamp = 0;
	if (!Array.isArray(fetchOptions)) fetchOptions = [fetchOptions];
	for (let options of fetchOptions) {
		options = mergeOptions(options);
		options.rateLimitTimestamp ??= rateLimitTimestamp;

		if (options.offsetParam) {
			options.qs[options.offsetParam] ??= 0;
		}

		if (Object.keys(options.qs).length) {
			options.url += `?${new URLSearchParams(options.qs)}`.replaceAll(
				"+",
				"%20",
			);
		}
		options.__origin = new URL(options.url).origin;
		const response = await fetchUnknown(options, streamOptions);
		try {
			for await (const chunk of response) {
				yield chunk;
			}
		} catch (error) {
			// Binary branch: response is a Web ReadableStream with .cancel().
			// JSON branch: response is the fetchJson async generator with
			// .return(). Call both so resources are released uniformly.
			await response?.cancel?.();
			await response?.return?.();
			throw error;
		}
		// ensure there is rate limiting between req with different options
		rateLimitTimestamp = options.rateLimitTimestamp;
	}
}

// `json` optionally followed by `;parameters` (or a bare trailing `;`).
// Note: the parameter portion is intentionally NOT `;.+` — that form admits a
// Stryker-equivalent mutant (`;.+` and `;.` accept the exact same inputs under
// `.test()`), so we match a bare `;` and let any following parameters be free.
const jsonContentTypeRegExp = /^application\/(.+\+)?json($|;)/;
const fetchUnknown = async (options, streamOptions) => {
	const response = await fetchRateLimit(options, streamOptions);
	if (jsonContentTypeRegExp.test(response.headers.get("Content-Type"))) {
		options.prefetchResponse = response; // hack
		return fetchJson(options, streamOptions);
	}
	return response.body;
};

const nextLinkRegExp = /<(.*?)>; rel="next"/;

async function* fetchJson(options, streamOptions) {
	const { dataPath, nextPath } = options;
	let url;

	while (options.url) {
		const response =
			options.prefetchResponse ??
			(await fetchRateLimit(options, streamOptions));
		delete options.prefetchResponse;
		// NOTE: response.json() buffers the FULL body of this page into memory
		// before any item is yielded — unlike the binary branch which streams
		// chunk-by-chunk with backpressure. A server returning a very large
		// single JSON document (or large pages) is fully materialized here, so
		// keep per-page payloads bounded for untrusted endpoints.
		const body = await response.json();
		url = parseLinkFromHeader(response.headers);
		url ??= parseNextPath(body, nextPath);
		url ??= paginateUsingQuery(options);
		validatePaginationUrl(url, options.__origin);
		options.url = url;
		const data = pickPath(body, dataPath);
		if (Array.isArray(data)) {
			for (const item of data) {
				yield item;
			}

			if (options.offsetParam && !data.length) break;
		} else {
			yield data;
		}
	}
}

const paginateUsingQuery = (options) => {
	if (!options.offsetParam || !options.offsetAmount) return undefined;

	const url = new URL(options.url);
	let offset = url.searchParams.get(options.offsetParam);
	if (!offset) return null;

	offset = Number.parseInt(offset, 10) + options.offsetAmount;
	url.searchParams.delete(options.offsetParam);
	url.searchParams.set(options.offsetParam, offset);
	return url.toString();
};

const parseNextPath = (body, nextPath) => {
	return nextPath ? pickPath(body, nextPath) : undefined;
};

const parseLinkFromHeader = (headers) => {
	const link = headers.get("Link");
	return link?.match(nextLinkRegExp)?.[1];
};

// 3xx statuses that carry a Location and represent a redirect.
const redirectStatuses = new Set([301, 302, 303, 307, 308]);
const maxRedirects = 20;

export const fetchRateLimit = async (options = {}, streamOptions = {}) => {
	// Apply defaults FIRST so rateLimit (and every other option) is populated
	// before it is read; otherwise a direct call without rateLimit computes
	// `Date.now() + 1000 * undefined` = NaN for rateLimitTimestamp.
	// Mutate the passed-in object in place (rather than reassigning to a clone)
	// so callers — notably fetchGenerator, which reads back
	// options.rateLimitTimestamp to carry rate limiting between configs — see
	// the timestamp we compute below.
	Object.assign(options, mergeOptions(options));

	const now = Date.now();
	if (now < (options.rateLimitTimestamp ?? 0)) {
		await timeout(options.rateLimitTimestamp - now, streamOptions);
	}
	options.rateLimitTimestamp = Date.now() + 1000 * options.rateLimit;

	// Same-origin redirect pinning (SSRF defence). The initial origin is pinned
	// to the original request URL; redirects to a different origin are blocked
	// by default so a server cannot 3xx us into internal/metadata endpoints.
	// Callers can opt out by setting `redirect` explicitly ("follow"/"error").
	const callerRedirect = options.redirect;
	const manageRedirects = callerRedirect === undefined;
	// __origin is set by fetchGenerator; derive it for direct callers.
	const initialOrigin = options.__origin ?? originOf(options.url);

	const {
		method,
		headers,
		body,
		mode,
		credentials,
		cache,
		referrer,
		referrerPolicy,
		integrity,
		keepalive,
		duplex,
	} = options;
	const fetchInit = {
		method,
		headers,
		body,
		mode,
		credentials,
		cache,
		// When we manage redirects ourselves we ask the platform NOT to follow
		// them so we can validate each Location before re-issuing.
		redirect: manageRedirects ? "manual" : callerRedirect,
		referrer,
		referrerPolicy,
		integrity,
		keepalive,
		duplex,
		signal: streamOptions.signal,
	};
	let response = await fetch(options.url, fetchInit);

	// Manually follow / validate redirects when we own redirect handling.
	if (manageRedirects) {
		let redirectCount = 0;
		while (
			redirectStatuses.has(response.status) &&
			response.headers.has("Location")
		) {
			const safeUrl = redactUrl(options.url);
			if (++redirectCount > maxRedirects) {
				await response.body?.cancel();
				throw new Error(
					`fetch ${options.method} ${safeUrl} exceeded ${maxRedirects} redirects`,
					{
						cause: {
							status: response.status,
							url: safeUrl,
							method: options.method,
						},
					},
				);
			}
			const location = response.headers.get("Location");
			let target;
			try {
				target = new URL(location, options.url);
			} catch {
				await response.body?.cancel();
				throw new Error(
					`fetch ${options.method} ${safeUrl} returned an invalid redirect Location`,
					{
						cause: {
							status: response.status,
							url: safeUrl,
							method: options.method,
						},
					},
				);
			}
			if (target.origin !== initialOrigin) {
				await response.body?.cancel();
				throw new Error(
					`fetch ${options.method} ${safeUrl} blocked cross-origin redirect (${target.origin} does not match ${initialOrigin})`,
					{
						cause: {
							status: response.status,
							url: safeUrl,
							location: redactUrl(target.toString()),
							origin: initialOrigin,
							method: options.method,
						},
					},
				);
			}
			await response.body?.cancel();
			options.url = target.toString();
			response = await fetch(options.url, fetchInit);
		}
	}

	if (!response.ok) {
		const safeUrl = redactUrl(options.url);
		// 429 Too Many Requests
		if (response.status === 429) {
			options.retryCount = (options.retryCount ?? 0) + 1;
			const retryMaxCount = options.retryMaxCount ?? 10;
			if (options.retryCount >= retryMaxCount) {
				await response.body?.cancel();
				throw new Error(
					`fetch ${response.status} ${options.method} ${safeUrl} max retries (${retryMaxCount}) exceeded`,
					{
						cause: {
							status: response.status,
							url: safeUrl,
							method: options.method,
						},
					},
				);
			}
			await response.body?.cancel();
			const retryAfter = response.headers.get("Retry-After");
			// Full jitter (AWS architecture blog) avoids retry-storm sync-up.
			const baseMs = retryAfter
				? Number.parseInt(retryAfter, 10) * 1000 || 1000
				: Math.min(1000 * 2 ** (options.retryCount - 1), 30_000);
			const backoffMs = retryAfter ? baseMs : Math.random() * baseMs;
			await timeout(backoffMs, streamOptions);
			return fetchRateLimit(options, streamOptions);
		}
		await response.body?.cancel();
		throw new Error(`fetch ${response.status} ${options.method} ${safeUrl}`, {
			cause: {
				status: response.status,
				url: safeUrl,
				method: options.method,
			},
		});
	}
	return response;
};

const pickPath = (obj, path = "") => {
	if (path === "") return obj;
	if (!Array.isArray(path)) path = path.split(".");
	return path.reduce((a, b) => a?.[b], obj);
};

export default {
	setDefaults: fetchSetDefaults,
	readableStream: fetchReadableStream,
	responseStream: fetchReadableStream,
};
