// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global fetch */
import {
	createReadableStream,
	createWritableStream,
	timeout,
} from "@datastream/core";

// Relative targets (valid per RFC 8288) resolve against the current page url.
const resolvePaginationUrl = (nextUrl, currentUrl, origin) => {
	if (!nextUrl) return;
	const url = URL.parse(nextUrl, currentUrl);
	if (!url) {
		throw new Error("Invalid pagination URL");
	}
	if (url.origin !== origin) {
		throw new Error(
			`Pagination URL origin (${url.origin}) does not match initial URL origin (${origin})`,
		);
	}
	return url.toString();
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
	concurrency: 1,
	maxPages: 10_000,
	maxBodySize: 16_777_216,
	retryMaxCount: 10,
	retryAfterMax: 60_000,
	timeout: undefined,

	// fetch
	method: "GET",
	headers: {
		Accept: "application/json",
		"Accept-Encoding": "br, gzip, deflate",
	},
};

const merge = (base, options) => ({
	...base,
	...options,
	headers: { ...base.headers, ...options.headers },
	qs: { ...base.qs, ...options.qs },
});

const mergeOptions = (options = {}) =>
	merge(
		defaults,
		Object.fromEntries(
			Object.entries(options).filter(([, value]) => value !== undefined),
		),
	);

export const fetchSetDefaults = (options) => {
	defaults = merge(defaults, options);
};

// Note: requires EncodeStream to ensure it's Uint8Array
// Poor browser support - https://github.com/Fyrd/caniuse/issues/6375
export const fetchWritableStream = async (options, streamOptions = {}) => {
	const controller = new AbortController();
	let bodyController;
	let resume = () => {};
	const release = () => resume();
	// Set when fetch cancels the body: enqueue()/close() would then throw.
	let cancelled = false;
	const body = new ReadableStream(
		{
			start(c) {
				bodyController = c;
			},
			pull: release,
			cancel() {
				cancelled = true;
				release();
			},
		},
		{ highWaterMark: streamOptions.highWaterMark },
	);
	controller.signal.addEventListener("abort", () => {
		bodyController.error(controller.signal.reason);
		release();
	});
	// Duplex: half - For browser compatibility - https://developer.chrome.com/articles/fetch-streaming-requests/#half-duplex
	options = mergeOptions(options);
	const response = fetchRateLimit(
		{
			...options,
			body,
			duplex: "half",
		},
		{
			...streamOptions,
			// The caller's signal still cancels the request directly (it may
			// already be aborted, which never fires the writable's listener).
			signal: AbortSignal.any([
				controller.signal,
				streamOptions.signal ?? controller.signal,
			]),
		},
	);
	let fetchError;
	let responded = false;
	response.then(
		() => {
			responded = true;
			release();
		},
		(e) => {
			fetchError = e;
			release();
		},
	);
	let value;
	const write = async (chunk) => {
		if (fetchError) throw fetchError;
		if (cancelled) return;
		bodyController.enqueue(chunk);
		if (responded || bodyController.desiredSize > 0) return;
		await new Promise((resolve) => {
			resume = resolve;
		});
		if (fetchError) throw fetchError;
	};
	const final = async () => {
		if (!cancelled) bodyController.close();
		value = await response;
	};
	const abort = (reason) => controller.abort(reason);
	const stream = createWritableStream(write, final, {
		...streamOptions,
		abort,
	});
	stream.result = () => ({ key: options.resultKey ?? "output", value });
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

const fetchItem = async (options, streamOptions) => {
	if (options.offsetParam) {
		options.qs[options.offsetParam] ??= 0;
	}
	const url = new URL(options.url);
	if (Object.keys(options.qs).length) {
		const qs = `${new URLSearchParams(options.qs)}`.replaceAll("+", "%20");
		url.search = url.search ? `${url.search}&${qs}` : qs;
		options.url = url.toString();
	}
	options.__origin = url.origin;
	return fetchUnknown(options, streamOptions);
};

async function* drainResponse(body) {
	try {
		for await (const chunk of body) {
			yield chunk;
		}
	} catch (error) {
		await body.cancel();
		throw error;
	}
}

async function* fetchGenerator(fetchOptions, streamOptions) {
	if (!Array.isArray(fetchOptions)) fetchOptions = [fetchOptions];
	const { concurrency } = mergeOptions(fetchOptions[0]);
	if (concurrency > 1) {
		yield* fetchConcurrent(fetchOptions, concurrency, streamOptions);
		return;
	}

	let rateLimitTimestamp = 0;
	for (let options of fetchOptions) {
		options = mergeOptions(options);
		options.rateLimitTimestamp ??= rateLimitTimestamp;
		const response = await fetchItem(options, streamOptions);
		yield* response;
		rateLimitTimestamp = options.rateLimitTimestamp;
	}
}

async function* fetchConcurrent(fetchOptions, concurrency, streamOptions) {
	const controller = new AbortController();
	streamOptions = {
		...streamOptions,
		signal: AbortSignal.any(
			[controller.signal, streamOptions.signal].filter(Boolean),
		),
	};
	let clock = 0;
	const pace = async (rateLimit) => {
		const now = Date.now();
		const wait = Math.max(0, clock - now);
		clock = now + wait + 1000 * rateLimit;
		await timeout(wait, streamOptions);
	};
	const runItem = async (item) => {
		const options = mergeOptions(item);
		await pace(options.rateLimit);
		options.rateLimit = 0;
		const response = await fetchItem(options, streamOptions);
		const output = [];
		for await (const chunk of response) {
			output.push(chunk); // buffer so results can be yielded in array order
		}
		return output;
	};

	const inFlight = [];
	let next = 0;
	// Queued items only get a handler once they reach the head, so a later
	// item rejecting while the head is still streaming would be an unhandled
	// rejection (fatal in Node). Mark it handled; the original is still awaited.
	const start = () => {
		const promise = runItem(fetchOptions[next++]);
		promise.catch(() => {});
		inFlight.push(promise);
	};
	try {
		fetchOptions.slice(0, concurrency).forEach(start);
		while (inFlight.length) {
			yield* await inFlight.shift();
			if (next < fetchOptions.length) {
				start();
			}
		}
	} finally {
		controller.abort();
		await Promise.allSettled(inFlight);
	}
}

// Case-insensitive: media types are (RFC 9110 §8.3.1).
const jsonContentTypeRegExp = /^application\/(.+\+)?json($|;)/i;
const fetchUnknown = async (options, streamOptions) => {
	const response = await fetchRateLimit(options, streamOptions);
	if (!response.body) return [];
	if (jsonContentTypeRegExp.test(response.headers.get("Content-Type"))) {
		options.prefetchResponse = response; // hack
		return fetchJson(options, streamOptions);
	}
	return drainResponse(response.body);
};

// RFC 8288: each link is `<target>` followed by its `;`-separated params, up to
// the next `<`. rel may be quoted or not, is case-insensitive and may hold
// several space-separated relation types. Only the first rel counts, and it
// must start a parameter (not sit inside e.g. `title="rel=next"`).
// Split on `<`/`>` rather than a global regex: `/<([^>]*)>/g` is O(n^2) on a
// header of many `<`.
const relRegExp = /;\s*rel\s*=\s*"?([^";,]*)/i;

// response.json() would buffer an unbounded body; count bytes as they arrive
// instead (Content-Length alone can be absent, or wrong under compression).
const readJsonBody = async (response, options) => {
	const max = options.maxBodySize ?? Number.POSITIVE_INFINITY;
	const tooLarge = () =>
		new RangeError(
			`fetch ${options.method} ${redactUrl(options.url)} response body exceeds maxBodySize (${max} bytes)`,
		);
	if (Number(response.headers.get("Content-Length")) > max) {
		await response.body.cancel();
		throw tooLarge();
	}
	const reader = response.body.getReader();
	const chunks = [];
	let size = 0;
	for (;;) {
		const { done, value } = await reader.read();
		if (done) break;
		size += value.byteLength;
		if (size > max) {
			await reader.cancel();
			throw tooLarge();
		}
		chunks.push(value);
	}
	// Blob decodes the joined bytes as UTF-8 (a character split across chunks
	// included) and strips a BOM, like response.json().
	return JSON.parse(await new Blob(chunks).text());
};

async function* fetchJson(options, streamOptions) {
	const { dataPath, nextPath } = options;
	let url;
	let pages = 0;
	const maxPages = options.maxPages ?? Number.POSITIVE_INFINITY;

	while (options.url) {
		if (++pages > maxPages) {
			throw new RangeError(
				`fetch ${options.method} ${redactUrl(options.url)} exceeded maxPages (${options.maxPages})`,
			);
		}
		const response =
			options.prefetchResponse ??
			(await fetchRateLimit(options, streamOptions));
		delete options.prefetchResponse;
		const body = await readJsonBody(response, options);
		url = parseLinkFromHeader(response.headers);
		url ??= parseNextPath(body, nextPath);
		url ??= paginateUsingQuery(options);
		const nextUrl = resolvePaginationUrl(url, options.url, options.__origin);
		if (nextUrl === new URL(options.url).href) {
			throw new Error(
				`fetch ${options.method} ${redactUrl(nextUrl)} pagination next URL repeats the current page`,
			);
		}
		options.url = nextUrl;
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
	url.searchParams.set(options.offsetParam, offset);
	return url.toString();
};

const parseNextPath = (body, nextPath) => {
	return nextPath ? pickPath(body, nextPath) : undefined;
};

const parseLinkFromHeader = (headers) => {
	const link = headers.get("Link");
	if (!link) return undefined;
	for (const part of link.split("<").slice(1)) {
		const [target, params] = part.split(">");
		const rels = params?.match(relRegExp)?.[1].toLowerCase().split(" ");
		if (rels?.includes("next")) {
			return target;
		}
	}
};

const assertSameOriginRedirect = async (
	response,
	options,
	location,
	initialOrigin,
) => {
	const origin = originOf(location);
	if (origin === initialOrigin) return;
	await response.body?.cancel();
	const safeUrl = redactUrl(options.url);
	throw new Error(
		`fetch ${options.method} ${safeUrl} blocked cross-origin redirect (${origin} does not match ${initialOrigin})`,
		{
			cause: {
				status: response.status,
				url: safeUrl,
				location: redactUrl(location),
				origin: initialOrigin,
				method: options.method,
			},
		},
	);
};

// RFC 9110 Retry-After: delay-seconds or an HTTP-date; undefined when neither.
// Clamped to `max`: a hostile/huge value would otherwise stall the stream, and
// anything over 2^31-1 ms overflows setTimeout into a 1ms (no backoff) wait, so
// that stays the bound even when `max` is null (no cap).
const maxTimeoutMs = 2_147_483_647;
const retryAfterMs = (value, max) => {
	let ms;
	if (/^\d+$/.test(value)) {
		ms = Number(value) * 1000;
	} else {
		const date = Date.parse(value);
		if (Number.isNaN(date)) return undefined;
		ms = Math.max(0, date - Date.now());
	}
	return Math.min(ms, max ?? maxTimeoutMs);
};

// 3xx statuses that carry a Location and represent a redirect.
const redirectStatuses = new Set([301, 302, 303, 307, 308]);
const maxRedirects = 20;
const replayableMethods = new Set(["GET", "HEAD"]);

// A streamed request body (fetchWritableStream's ReadableStream, a Node
// Readable, an async generator) is consumed by the first request, so it cannot
// be re-sent on a retry or a body-preserving redirect.
const isStreamBody = (body) =>
	typeof body?.getReader === "function" ||
	typeof body?.[Symbol.asyncIterator] === "function";
const cannotReplay = (response, options, safeUrl) =>
	new Error(
		`fetch ${response.status} ${options.method} ${safeUrl} cannot replay streamed request body`,
		{
			cause: {
				status: response.status,
				url: safeUrl,
				method: options.method,
			},
		},
	);

// RFC 9110 §9.2.2: repeating these leaves the server as one request would.
const idempotentMethods = new Set(["GET", "HEAD", "OPTIONS", "PUT", "DELETE"]);
// Transient server failures. 429 is retried for every method instead: the
// server refused the request without processing it.
const retryStatuses = new Set([500, 502, 503, 504]);
const canRetry = (options) =>
	idempotentMethods.has(options.method.toUpperCase()) &&
	!isStreamBody(options.body);

const retriesExhausted = (options) => {
	options.retryCount = (options.retryCount ?? 0) + 1;
	return (
		options.retryCount >= (options.retryMaxCount ?? Number.POSITIVE_INFINITY)
	);
};

const backoff = (options, streamOptions, retryAfter) =>
	timeout(
		retryAfterMs(retryAfter, options.retryAfterMax) ??
			Math.random() * Math.min(1000 * 2 ** (options.retryCount - 1), 30_000),
		streamOptions,
	);

// fetch also accepts file:, data:, blob: (and more, per platform); only
// http(s) is a network request, and only it gets the origin pinning below.
// A relative URL resolves against the page in a browser (as fetch() itself
// does); with no location (Node) it stays unparseable and is rejected.
const assertHttpUrl = (options) => {
	const url = URL.parse(options.url, globalThis.location?.href);
	if (url?.protocol === "http:" || url?.protocol === "https:") return;
	throw new TypeError(
		`fetch ${options.method} ${redactUrl(url?.href ?? options.url)} is not an http: or https: URL`,
	);
};

const request = async (options, fetchInit, manageRedirects, initialOrigin) => {
	let response = await fetch(options.url, fetchInit);
	if (!manageRedirects) return response;
	// Browsers answer redirect:"manual" with an opaque redirect (status 0,
	// Location hidden), so let the browser follow it and check where it
	// ended up instead. Node exposes the 3xx and is handled below.
	if (response.type === "opaqueredirect") {
		if (!replayableMethods.has(options.method.toUpperCase())) {
			const safeUrl = redactUrl(options.url);
			throw new Error(
				`fetch ${options.method} ${safeUrl} was redirected; browsers hide the redirect target, so only GET/HEAD can be re-requested (set redirect: "follow" to opt out)`,
				{
					cause: {
						status: response.status,
						url: safeUrl,
						method: options.method,
					},
				},
			);
		}
		response = await fetch(options.url, { ...fetchInit, redirect: "follow" });
		await assertSameOriginRedirect(
			response,
			options,
			response.url,
			initialOrigin,
		);
		options.url = response.url;
	}
	let redirectCount = 0;
	while (
		redirectStatuses.has(response.status) &&
		response.headers.has("Location")
	) {
		const safeUrl = redactUrl(options.url);
		if (++redirectCount > maxRedirects) {
			await response.body?.cancel();
			throw new RangeError(
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
		await assertSameOriginRedirect(
			response,
			options,
			target.toString(),
			initialOrigin,
		);
		await response.body?.cancel();
		options.url = target.toString();
		// Fetch spec: 303 (except HEAD), and 301/302 after a POST, re-request
		// with GET and no body; resending would submit the body twice. Set on
		// options too so a retry of the new url stays GET.
		const upperMethod = options.method.toUpperCase();
		if (
			(response.status === 303 && upperMethod !== "HEAD") ||
			((response.status === 301 || response.status === 302) &&
				upperMethod === "POST")
		) {
			options.method = "GET";
			options.body = undefined;
			fetchInit.method = "GET";
			fetchInit.body = undefined;
		}
		// 307/308 (and 301/302 for non-POST) re-send the body.
		if (isStreamBody(fetchInit.body)) {
			throw cannotReplay(response, options, safeUrl);
		}
		response = await fetch(options.url, fetchInit);
	}
	return response;
};

export const fetchRateLimit = async (options = {}, streamOptions = {}) => {
	// Apply defaults FIRST so rateLimit (and every other option) is populated
	// before it is read; otherwise a direct call without rateLimit computes
	// `Date.now() + 1000 * undefined` = NaN for rateLimitTimestamp.
	// Mutate the passed-in object in place (rather than reassigning to a clone)
	// so callers — notably fetchGenerator, which reads back
	// options.rateLimitTimestamp to carry rate limiting between configs — see
	// the timestamp we compute below.
	Object.assign(options, mergeOptions(options));
	assertHttpUrl(options);

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
		signal:
			(options.timeout ?? null) === null
				? streamOptions.signal
				: AbortSignal.any(
						[AbortSignal.timeout(options.timeout), streamOptions.signal].filter(
							Boolean,
						),
					),
	};
	let response;
	try {
		response = await request(
			options,
			fetchInit,
			manageRedirects,
			initialOrigin,
		);
	} catch (error) {
		// fetch rejects with a TypeError on a network failure (DNS, connection
		// reset, offline). An abort or timeout is never retried, even when the
		// caller's abort reason is itself a TypeError.
		if (
			!(error instanceof TypeError) ||
			fetchInit.signal?.aborted ||
			!canRetry(options) ||
			retriesExhausted(options)
		) {
			throw error;
		}
		await backoff(options, streamOptions);
		return fetchRateLimit(options, streamOptions);
	}

	if (!response.ok) {
		const safeUrl = redactUrl(options.url);
		const cause = {
			status: response.status,
			url: safeUrl,
			method: options.method,
		};
		await response.body?.cancel();
		if (
			response.status === 429 ||
			(retryStatuses.has(response.status) && canRetry(options))
		) {
			if (retriesExhausted(options)) {
				throw new RangeError(
					`fetch ${response.status} ${options.method} ${safeUrl} max retries (${options.retryMaxCount}) exceeded`,
					{ cause },
				);
			}
			if (isStreamBody(options.body)) {
				throw cannotReplay(response, options, safeUrl);
			}
			await backoff(
				options,
				streamOptions,
				response.headers.get("Retry-After"),
			);
			return fetchRateLimit(options, streamOptions);
		}
		throw new Error(`fetch ${response.status} ${options.method} ${safeUrl}`, {
			cause,
		});
	}
	options.retryCount = 0;
	return response;
};

const pickPath = (obj, path = "") => {
	if (path === "") return obj;
	if (!Array.isArray(path)) path = path.split(".");
	return path.reduce((a, b) => a?.[b], obj);
};
