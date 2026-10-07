// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { GetSchemaVersionCommand, GlueClient } from "@aws-sdk/client-glue";
import { awsClientDefaults } from "./client.js";

let defaultClient;

export const awsGlueSchemaRegistrySetClient = (glueClient) => {
	defaultClient = glueClient;
};

export const awsGlueSchemaRegistryResolver = ({
	client,
	clientOptions,
	cacheExpiry = -1,
	maxCacheSize = 1000,
} = {}) => {
	// null = unbounded cache
	const cacheCap = maxCacheSize ?? Number.POSITIVE_INFINITY;
	// Cache the lazily-created client per resolver invocation (closure-local) so
	// each resolver honors its own clientOptions. The module-level defaultClient
	// is reserved strictly for the explicit setClient() path.
	let lazyClient;
	const resolveClient = () => {
		if (client) return client;
		if (defaultClient) return defaultClient;
		lazyClient ??= new GlueClient({
			...awsClientDefaults,
			...clientOptions,
		});
		return lazyClient;
	};

	// Map preserves insertion order, so we get O(1) FIFO eviction by deleting
	// the oldest key when we hit the cap.
	const cache = new Map(); // schemaVersionId -> { value, expires }
	// Dedup in-flight lookups so parallel resolves for the same id don't fan
	// out to N Glue calls (Glue rate-limits aggressively).
	const inflight = new Map(); // schemaVersionId -> Promise<value>

	const evictIfNeeded = () => {
		while (cache.size >= cacheCap) {
			const oldest = cache.keys().next().value;
			cache.delete(oldest);
		}
	};

	return async (schemaVersionId) => {
		if (typeof schemaVersionId !== "string" || schemaVersionId.length === 0) {
			throw new TypeError(
				"awsGlueSchemaRegistryResolver: schemaVersionId required",
			);
		}
		const hit = cache.get(schemaVersionId);
		// expires === -1 is the never-expires sentinel; otherwise it is an absolute
		// timestamp and the entry is fresh while it is still in the future.
		if (hit && (hit.expires === -1 || hit.expires > Date.now())) {
			return hit.value;
		}
		// Drop the stale entry so it doesn't sit ahead of fresh keys in the
		// Map's FIFO order and get re-inserted at the tail below. delete() is a
		// no-op when the key is absent, so the (stale-only) hit is implied.
		cache.delete(schemaVersionId);
		const pending = inflight.get(schemaVersionId);
		if (pending) return pending;

		const promise = (async () => {
			try {
				const resp = await resolveClient().send(
					new GetSchemaVersionCommand({ SchemaVersionId: schemaVersionId }),
				);
				const value = {
					schemaVersionId: resp.SchemaVersionId,
					schemaDefinition: resp.SchemaDefinition,
					dataFormat: resp.DataFormat,
				};
				// delete-then-set so a refreshed key lands at the tail of the
				// Map's insertion order, keeping eviction true-FIFO.
				cache.delete(schemaVersionId);
				// maxCacheSize <= 0 disables caching; evicting down to a non-positive
				// cap would otherwise spin forever on an empty Map.
				if (cacheCap > 0) {
					evictIfNeeded();
					cache.set(schemaVersionId, {
						value,
						expires: cacheExpiry < 0 ? -1 : Date.now() + cacheExpiry,
					});
				}
				return value;
			} finally {
				inflight.delete(schemaVersionId);
			}
		})();
		inflight.set(schemaVersionId, promise);
		return promise;
	};
};
