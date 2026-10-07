import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import { GetSchemaVersionCommand, GlueClient } from "@aws-sdk/client-glue";
import * as awsModule from "@datastream/aws/glue-schema-registry";
import {
	awsGlueSchemaRegistryResolver,
	awsGlueSchemaRegistrySetClient,
} from "@datastream/aws/glue-schema-registry";
import { mockClient } from "aws-sdk-client-mock";
import { variant } from "../variant.js";

describe(`@datastream/aws/glue-schema-registry (${variant})`, () => {
	test(`awsGlueSchemaRegistryResolver fetches and returns schema metadata`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: 'syntax = "proto3";',
			DataFormat: "PROTOBUF",
		});

		const resolve = awsGlueSchemaRegistryResolver();
		const result = await resolve("v1");
		strictEqual(result.schemaVersionId, "v1");
		strictEqual(result.dataFormat, "PROTOBUF");
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
	});

	test(`awsGlueSchemaRegistryResolver caches by schemaVersionId`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		const resolve = awsGlueSchemaRegistryResolver({ cacheExpiry: -1 });
		await resolve("v1");
		await resolve("v1");
		await resolve("v1");
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
	});

	test(`awsGlueSchemaRegistryResolver respects cacheExpiry`, async (t) => {
		t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		const resolve = awsGlueSchemaRegistryResolver({ cacheExpiry: 1 });
		await resolve("v1");
		t.mock.timers.tick(5);
		await resolve("v1");
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 2);
	});

	test(`awsGlueSchemaRegistryResolver dedupes concurrent in-flight lookups`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		let pendingResolve;
		client.on(GetSchemaVersionCommand).callsFake(
			() =>
				new Promise((r) => {
					pendingResolve = () =>
						r({
							SchemaVersionId: "v1",
							SchemaDefinition: "x",
							DataFormat: "AVRO",
						});
				}),
		);

		const resolve = awsGlueSchemaRegistryResolver();
		// Fire three parallel lookups before any settle.
		const p1 = resolve("v1");
		const p2 = resolve("v1");
		const p3 = resolve("v1");
		// Only one Glue command should be in flight.
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
		pendingResolve();
		const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
		strictEqual(r1, r2);
		strictEqual(r2, r3);
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
	});

	test(`awsGlueSchemaRegistryResolver honors per-resolver clientOptions on the lazy-init path`, async () => {
		// Clear any module-level client set by earlier tests so the LAZY-init path
		// (no explicit client passed) is exercised for both resolvers.
		awsGlueSchemaRegistrySetClient(undefined);

		// mockClient stubs .send on every GlueClient instance while leaving the real
		// constructor intact, so each lazily-built client still carries the region
		// it was constructed with. callsFake receives the live client instance so we
		// can read back which client actually serviced each lookup.
		const mock = mockClient(GlueClient);
		const seenRegions = [];
		mock.on(GetSchemaVersionCommand).callsFake(async (input, getClient) => {
			const region = await getClient().config.region();
			seenRegions.push(region);
			return {
				SchemaVersionId: input.SchemaVersionId,
				SchemaDefinition: "x",
				DataFormat: "AVRO",
			};
		});

		// First resolver triggers lazy construction with us-east-1.
		const resolveA = awsGlueSchemaRegistryResolver({
			clientOptions: { region: "us-east-1" },
		});
		// Second resolver is created with a DIFFERENT region. It must build/use its
		// own client, not silently reuse the first resolver's lazily-built client.
		const resolveB = awsGlueSchemaRegistryResolver({
			clientOptions: { region: "eu-west-1" },
		});

		await resolveA("v1");
		await resolveB("v2");

		deepStrictEqual(seenRegions, ["us-east-1", "eu-west-1"]);
	});

	test(`awsGlueSchemaRegistryResolver evicts oldest entry past maxCacheSize`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).callsFake((args) =>
			Promise.resolve({
				SchemaVersionId: args.SchemaVersionId,
				SchemaDefinition: "x",
				DataFormat: "AVRO",
			}),
		);

		const resolve = awsGlueSchemaRegistryResolver({ maxCacheSize: 2 });
		await resolve("v1");
		await resolve("v2");
		await resolve("v3"); // evicts v1
		// v1 should miss now and re-fetch
		await resolve("v1");
		const calls = client.commandCalls(GetSchemaVersionCommand);
		strictEqual(calls.length, 4); // v1, v2, v3, v1-again
		strictEqual(calls[3].args[0].input.SchemaVersionId, "v1");
	});

	// maxCacheSize 0 disables caching: every lookup re-fetches. (Previously the
	// eviction loop `while (cache.size >= 0)` spun forever on an empty cache.)
	test(`awsGlueSchemaRegistryResolver with maxCacheSize 0 does not cache`, async () => {
		let calls = 0;
		const client = {
			send: async (command) => {
				calls++;
				return {
					SchemaVersionId: command.input.SchemaVersionId,
					SchemaDefinition: "x",
					DataFormat: "AVRO",
				};
			},
		};

		const resolve = awsGlueSchemaRegistryResolver({ client, maxCacheSize: 0 });
		const expected = {
			schemaVersionId: "v1",
			schemaDefinition: "x",
			dataFormat: "AVRO",
		};
		deepStrictEqual(await resolve("v1"), expected);
		deepStrictEqual(await resolve("v1"), expected);
		strictEqual(calls, 2);
	});

	// maxCacheSize null = unbounded: past the default cap of 1000 nothing is
	// evicted, so the first id is still served from cache.
	test(`awsGlueSchemaRegistryResolver with maxCacheSize null never evicts`, async () => {
		let calls = 0;
		const client = {
			send: async (command) => {
				calls++;
				return {
					SchemaVersionId: command.input.SchemaVersionId,
					SchemaDefinition: "x",
					DataFormat: "AVRO",
				};
			},
		};

		const resolve = awsGlueSchemaRegistryResolver({
			client,
			maxCacheSize: null,
		});
		for (let i = 0; i < 1001; i++) {
			await resolve(`v${i}`);
		}
		deepStrictEqual(await resolve("v0"), {
			schemaVersionId: "v0",
			schemaDefinition: "x",
			dataFormat: "AVRO",
		});
		strictEqual(calls, 1001);
	});

	// *** setClient routes lookups to the configured default client *** //
	test(`awsGlueSchemaRegistrySetClient default client services resolvers without an explicit client`, async () => {
		awsGlueSchemaRegistrySetClient(undefined);

		// A resolver with neither `client` nor a default would lazily build a real
		// GlueClient. Setting a default mock makes it the one that handles the call.
		const def = mockClient(GlueClient);
		def.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});
		awsGlueSchemaRegistrySetClient(def);

		const resolve = awsGlueSchemaRegistryResolver();
		const result = await resolve("v1");
		strictEqual(result.schemaVersionId, "v1");
		strictEqual(def.commandCalls(GetSchemaVersionCommand).length, 1);
	});

	// *** explicit client takes precedence over the module default *** //
	test(`awsGlueSchemaRegistryResolver prefers an explicit client over the default`, async () => {
		const def = mockClient(GlueClient);
		def.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "from-default",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});
		awsGlueSchemaRegistrySetClient(def);

		// Build a separate, explicit client instance with its own stub.
		const explicit = new GlueClient({ region: "us-east-1" });
		explicit.send = async () => ({
			SchemaVersionId: "from-explicit",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		const resolve = awsGlueSchemaRegistryResolver({ client: explicit });
		const result = await resolve("v1");
		// The explicit client serviced the lookup, not the module default.
		strictEqual(result.schemaVersionId, "from-explicit");
		strictEqual(def.commandCalls(GetSchemaVersionCommand).length, 0);
	});

	// *** schemaVersionId validation *** //
	test(`awsGlueSchemaRegistryResolver rejects a non-string id`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({ SchemaVersionId: "v1" });

		const resolve = awsGlueSchemaRegistryResolver();
		await rejects(() => resolve(123), {
			name: "TypeError",
			message: "awsGlueSchemaRegistryResolver: schemaVersionId required",
		});
		await rejects(() => resolve(undefined), {
			name: "TypeError",
			message: "awsGlueSchemaRegistryResolver: schemaVersionId required",
		});
		// No Glue call for invalid input.
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 0);
	});

	test(`awsGlueSchemaRegistryResolver rejects an empty-string id`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({ SchemaVersionId: "" });

		const resolve = awsGlueSchemaRegistryResolver();
		await rejects(() => resolve(""), {
			name: "TypeError",
			message: "awsGlueSchemaRegistryResolver: schemaVersionId required",
		});
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 0);
	});

	// *** default cacheExpiry (-1) means cache never expires *** //
	test(`awsGlueSchemaRegistryResolver default cacheExpiry caches indefinitely`, async (t) => {
		t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		// No cacheExpiry option => default -1 (sentinel for "never expires"). Even
		// after the (mocked) clock advances the entry must still be served from
		// cache. A `+1` mutant on the default would give a 1ms TTL and force a
		// re-fetch.
		const resolve = awsGlueSchemaRegistryResolver();
		await resolve("v1");
		t.mock.timers.tick(10);
		await resolve("v1");
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
	});

	// *** positive cacheExpiry serves from cache within the TTL window *** //
	test(`awsGlueSchemaRegistryResolver serves from cache within a positive TTL`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		// A generous 60s TTL: a second immediate lookup is within the window so it
		// must hit the cache (expires = Date.now() + cacheExpiry, in the future). A
		// `Date.now() - cacheExpiry` mutant would put expiry in the past -> re-fetch.
		const resolve = awsGlueSchemaRegistryResolver({ cacheExpiry: 60_000 });
		await resolve("v1");
		await resolve("v1");
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
	});

	// *** setClient stores the default client used by client-less resolvers *** //
	test(`awsGlueSchemaRegistrySetClient stores the default client reference`, async () => {
		// A plain stub (no GlueClient prototype) proves the stored default is used by
		// resolvers that pass neither `client` nor `clientOptions`. A `setClient(){}`
		// mutant (or a `if (defaultClient) return defaultClient` -> false mutant) would
		// fall through to lazily building a real GlueClient and the stub's send would
		// never run.
		let calls = 0;
		const stub = {
			send: async () => {
				calls++;
				return {
					SchemaVersionId: "v1",
					SchemaDefinition: "x",
					DataFormat: "AVRO",
				};
			},
		};
		awsGlueSchemaRegistrySetClient(stub);

		const resolve = awsGlueSchemaRegistryResolver();
		const result = await resolve("v1");
		strictEqual(result.schemaVersionId, "v1");
		strictEqual(calls, 1);
		awsGlueSchemaRegistrySetClient(undefined);
	});

	// *** cacheExpiry === 0 is a finite (immediately-stale) TTL, NOT the never-expire
	// sentinel: `cacheExpiry < 0` must be strict (`<=` would treat 0 as never-expire) *** //
	test(`awsGlueSchemaRegistryResolver treats cacheExpiry 0 as a finite TTL`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		const realNow = Date.now;
		let now = 1_000_000;
		Date.now = () => now;
		try {
			const resolve = awsGlueSchemaRegistryResolver({ cacheExpiry: 0 });
			await resolve("v1"); // expires = now + 0 = now
			now += 1; // advance time so the entry is strictly in the past
			await resolve("v1"); // expired -> re-fetch
			// `cacheExpiry <= 0` mutant would store -1 (never-expire) -> only 1 call.
			strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 2);
		} finally {
			Date.now = realNow;
		}
	});

	// *** freshness boundary is strict `>` Date.now() (an entry expiring exactly now
	// is stale): a `>=` mutant would serve it from cache *** //
	test(`awsGlueSchemaRegistryResolver re-fetches an entry whose expiry equals now`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		const realNow = Date.now;
		const now = 2_000_000;
		Date.now = () => now; // frozen: expires (now + 0) === Date.now()
		try {
			const resolve = awsGlueSchemaRegistryResolver({ cacheExpiry: 0 });
			await resolve("v1"); // expires = now
			await resolve("v1"); // hit.expires (now) > Date.now() (now) === false -> stale
			// `>=` mutant: now >= now is true -> served from cache -> only 1 call.
			strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 2);
		} finally {
			Date.now = realNow;
		}
	});

	// *** default export shape *** //
	// Named exports are canonical; a default export must not come back.
	test(`glue-schema-registry has no default export`, (_t) => {
		strictEqual(Object.hasOwn(awsModule, "default"), false);
	});
});
