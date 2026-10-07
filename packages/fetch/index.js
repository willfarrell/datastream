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
		// Unparseable even as a relative reference, so there is nothing to
		// redact-and-show (redactUrl could only return "[INVALID URL]").
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
	concurrency: 1, // array items fetched at once; read from the first item
	// Limits: null = unlimited
	maxPages: 10_000, // JSON pagination: max pages fetched per request config
	maxBodySize: 16_777_216, // bytes; max JSON page body
	retryMaxCount: 10, // max attempts on 429 before throwing
	retryAfterMax: 60_000, // ms; upper bound for a 429 Retry-After wait (<= 2^31-1)

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

// Per request, an option left undefined falls back to its default (so
// `maxPages: undefined` keeps the 10_000 cap; only null lifts a limit).
const mergeOptions = (options = {}) =>
	merge(
		defaults,
		Object.fromEntries(
			Object.entries(options).filter(([, value]) => value !== undefined),
		),
	);

// Unfiltered: fetchSetDefaults({ key: undefined }) clears a default.
export const fetchSetDefaults = (options) => {
	defaults = merge(defaults, options);
};

// Note: requires EncodeStream to ensure it's Uint8Array
// Poor browser support - https://github.com/Fyrd/caniuse/issues/6375
export const fetchWritableStream = async (options, streamOptions = {}) => {
	// Aborting/destroying the writable before it finishes cancels the request
	// (and errors its body) through this controller; see abort() below.
	const controller = new AbortController();
	// The body errors on this signal in both builds.
	const body = createReadableStream(undefined, { signal: controller.signal });
	// Duplex: half - For browser compatibility - https://developer.chrome.com/articles/fetch-streaming-requests/#half-duplex
	options = mergeOptions(options);
	// Not awaited: a server may read the whole body before responding, so the
	// writable must be returned (and written to) while the request is pending.
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
	// Record an early failure (e.g. an immediate 401) so the next write throws
	// it; otherwise nobody reads the body and writes pile up until the queue
	// limit error hides the real cause. final() still awaits the rejection.
	let fetchError;
	response.catch((e) => {
		fetchError = e;
	});
	let value;
	const write = (chunk) => {
		if (fetchError) throw fetchError;
		body.push(chunk);
	};
	// Signal end-of-body so the duplex request body terminates and the
	// in-flight upload can complete. createReadableStream treats a pushed
	// `null` as close on both the Node and Web builds.
	const final = async () => {
		body.push(null);
		value = await response;
	};
	// Torn down before finishing (destroy/abort, upstream error, signal): end
	// the upload instead of leaving the request and its body open.
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
		// Append to (rather than replace) any query already on the url.
		url.search = url.search ? `${url.search}&${qs}` : qs;
		options.url = url.toString();
	}
	options.__origin = url.origin;
	return fetchUnknown(options, streamOptions);
};

async function* drainResponse(response) {
	try {
		for await (const chunk of response) {
			yield chunk;
		}
	} catch (error) {
		await response?.cancel?.();
		await response?.return?.();
		throw error;
	}
}

async function* fetchGenerator(fetchOptions, streamOptions) {
	if (!Array.isArray(fetchOptions)) fetchOptions = [fetchOptions];
	// One knob for the whole array, so it is taken from the first item (or the
	// fetchSetDefaults default).
	// Anything not above 1 (0, negative, null) means sequential.
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
		yield* drainResponse(response); // stream chunk-by-chunk (backpressure)
		rateLimitTimestamp = options.rateLimitTimestamp;
	}
}

async function* fetchConcurrent(fetchOptions, concurrency, streamOptions) {
	let clock = 0;
	// Starts are spaced by the previous item's rateLimit; a zero wait still
	// yields a macrotask, which is negligible next to a network round trip.
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
		for await (const chunk of drainResponse(response)) {
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
		// forEach rather than a `while (…) start()` loop, whose exit condition
		// depends on start() and so spins forever if start() ever does nothing.
		fetchOptions.slice(0, concurrency).forEach(start);
		while (inFlight.length) {
			yield* await inFlight.shift();
			if (next < fetchOptions.length) {
				start();
			}
		}
	} finally {
		await Promise.allSettled(inFlight);
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
		// Bounds a server that paginates forever (e.g. a cycle of next links).
		if (++pages > maxPages) {
			throw new RangeError(
				`fetch ${options.method} ${redactUrl(options.url)} exceeded maxPages (${options.maxPages})`,
			);
		}
		const response =
			options.prefetchResponse ??
			(await fetchRateLimit(options, streamOptions));
		delete options.prefetchResponse;
		// NOTE: each JSON page is buffered whole before any item is yielded —
		// unlike the binary branch which streams chunk-by-chunk with
		// backpressure — so readJsonBody caps it at maxBodySize.
		const body = await readJsonBody(response, options);
		url = parseLinkFromHeader(response.headers);
		url ??= parseNextPath(body, nextPath);
		url ??= paginateUsingQuery(options);
		const nextUrl = resolvePaginationUrl(url, options.url, options.__origin);
		// A page linking to itself would be re-fetched forever.
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
	url.searchParams.delete(options.offsetParam);
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
		// Browsers answer redirect:"manual" with an opaque redirect (status 0,
		// Location hidden), so let the browser follow it and check where it
		// ended up instead. Node exposes the 3xx and is handled below.
		if (response.type === "opaqueredirect") {
			// Re-issuing is only safe for GET/HEAD: a POST would send its body
			// twice (and a streamed body cannot be replayed at all).
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
			response = await fetch(options.url, fetchInit);
		}
	}

	if (!response.ok) {
		const safeUrl = redactUrl(options.url);
		// 429 Too Many Requests
		if (response.status === 429) {
			options.retryCount = (options.retryCount ?? 0) + 1;
			const retryMaxCount = options.retryMaxCount ?? Number.POSITIVE_INFINITY;
			if (options.retryCount >= retryMaxCount) {
				await response.body?.cancel();
				throw new RangeError(
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
			// Full jitter (AWS architecture blog) avoids retry-storm sync-up.
			const backoffMs =
				retryAfterMs(
					response.headers.get("Retry-After"),
					options.retryAfterMax,
				) ??
				Math.random() * Math.min(1000 * 2 ** (options.retryCount - 1), 30_000);
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
	// Retries are per request: a later page gets the full retry budget again.
	options.retryCount = 0;
	return response;
};

const pickPath = (obj, path = "") => {
	if (path === "") return obj;
	if (!Array.isArray(path)) path = path.split(".");
	return path.reduce((a, b) => a?.[b], obj);
};
