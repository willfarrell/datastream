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
	const stream = createWritableStream(write, streamOptions);
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
		for await (const chunk of response) {
			yield chunk;
		}
		// ensure there is rate limiting between req with different options
		rateLimitTimestamp = options.rateLimitTimestamp;
	}
}

const jsonContentTypeRegExp = /^application\/(.+\+)?json($|;.+)/;
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

export const fetchRateLimit = async (options, streamOptions = {}) => {
	const now = Date.now();
	if (now < (options.rateLimitTimestamp ?? 0)) {
		await timeout(options.rateLimitTimestamp - now, streamOptions);
	}
	options.rateLimitTimestamp = Date.now() + 1000 * options.rateLimit;
	options = mergeOptions(options); // for when called directly

	const {
		method,
		headers,
		body,
		mode,
		credentials,
		cache,
		redirect,
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
		redirect,
		referrer,
		referrerPolicy,
		integrity,
		keepalive,
		duplex,
		signal: streamOptions.signal,
	};
	const response = await fetch(options.url, fetchInit);
	if (!response.ok) {
		// 429 Too Many Requests
		if (response.status === 429) {
			options.retryCount = (options.retryCount ?? 0) + 1;
			const retryMaxCount = options.retryMaxCount ?? 10;
			if (options.retryCount >= retryMaxCount) {
				await response.body?.cancel();
				throw new Error(
					`fetch ${response.status} ${options.method} ${options.url} max retries (${retryMaxCount}) exceeded`,
					{
						cause: {
							status: response.status,
							url: options.url,
							method: options.method,
						},
					},
				);
			}
			await response.body?.cancel();
			const retryAfter = response.headers.get("Retry-After");
			const backoffMs = retryAfter
				? Number.parseInt(retryAfter, 10) * 1000 || 1000
				: Math.min(1000 * 2 ** (options.retryCount - 1), 30_000);
			await timeout(backoffMs, streamOptions);
			return fetchRateLimit(options, streamOptions);
		}
		await response.body?.cancel();
		throw new Error(
			`fetch ${response.status} ${options.method} ${options.url}`,
			{
				cause: {
					status: response.status,
					url: options.url,
					method: options.method,
				},
			},
		);
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
