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
		const p1 = resolve("v1");
		const p2 = resolve("v1");
		const p3 = resolve("v1");
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
		pendingResolve();
		const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
		strictEqual(r1, r2);
		strictEqual(r2, r3);
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
	});

	test(`awsGlueSchemaRegistryResolver honors per-resolver clientOptions on the lazy-init path`, async () => {
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

		const resolveA = awsGlueSchemaRegistryResolver({
			clientOptions: { region: "us-east-1" },
		});
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
		await resolve("v3");
		await resolve("v1");
		const calls = client.commandCalls(GetSchemaVersionCommand);
		strictEqual(calls.length, 4);
		strictEqual(calls[3].args[0].input.SchemaVersionId, "v1");
	});

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

	test(`awsGlueSchemaRegistrySetClient default client services resolvers without an explicit client`, async () => {
		awsGlueSchemaRegistrySetClient(undefined);

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

	test(`awsGlueSchemaRegistryResolver prefers an explicit client over the default`, async () => {
		const def = mockClient(GlueClient);
		def.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "from-default",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});
		awsGlueSchemaRegistrySetClient(def);

		const explicit = new GlueClient({ region: "us-east-1" });
		explicit.send = async () => ({
			SchemaVersionId: "from-explicit",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		const resolve = awsGlueSchemaRegistryResolver({ client: explicit });
		const result = await resolve("v1");
		strictEqual(result.schemaVersionId, "from-explicit");
		strictEqual(def.commandCalls(GetSchemaVersionCommand).length, 0);
	});

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

	test(`awsGlueSchemaRegistryResolver default cacheExpiry caches indefinitely`, async (t) => {
		t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		const resolve = awsGlueSchemaRegistryResolver();
		await resolve("v1");
		t.mock.timers.tick(10);
		await resolve("v1");
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
	});

	test(`awsGlueSchemaRegistryResolver serves from cache within a positive TTL`, async () => {
		const client = mockClient(GlueClient);
		awsGlueSchemaRegistrySetClient(client);
		client.on(GetSchemaVersionCommand).resolves({
			SchemaVersionId: "v1",
			SchemaDefinition: "x",
			DataFormat: "AVRO",
		});

		const resolve = awsGlueSchemaRegistryResolver({ cacheExpiry: 60_000 });
		await resolve("v1");
		await resolve("v1");
		strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
	});

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
			await resolve("v1");
			now += 1;
			await resolve("v1");
			strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 2);
		} finally {
			Date.now = realNow;
		}
	});

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
		Date.now = () => now;
		try {
			const resolve = awsGlueSchemaRegistryResolver({ cacheExpiry: 0 });
			await resolve("v1");
			await resolve("v1");
			strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 2);
		} finally {
			Date.now = realNow;
		}
	});

	test(`awsGlueSchemaRegistryResolver re-queues a refreshed key at the FIFO tail`, async (t) => {
		t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
		const fetched = [];
		const client = {
			send: async (command) => {
				fetched.push(command.input.SchemaVersionId);
				return {
					SchemaVersionId: command.input.SchemaVersionId,
					SchemaDefinition: "x",
					DataFormat: "AVRO",
				};
			},
		};
		const resolve = awsGlueSchemaRegistryResolver({
			client,
			cacheExpiry: 10,
			maxCacheSize: 3,
		});
		await resolve("v1");
		t.mock.timers.tick(5);
		await resolve("v2");
		t.mock.timers.tick(7);
		await resolve("v1");
		await resolve("v3");
		await resolve("v4");
		await resolve("v1");
		deepStrictEqual(fetched, ["v1", "v2", "v1", "v3", "v4"]);
	});

	test(`glue-schema-registry has no default export`, (_t) => {
		strictEqual(Object.hasOwn(awsModule, "default"), false);
	});
});
