import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test from "node:test";
import { GetSchemaVersionCommand, GlueClient } from "@aws-sdk/client-glue";
import glueDefault, {
	awsGlueSchemaRegistryResolver,
	awsGlueSchemaRegistrySetClient,
} from "@datastream/aws/glue-schema-registry";
import { mockClient } from "aws-sdk-client-mock";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: awsGlueSchemaRegistryResolver fetches and returns schema metadata`, async () => {
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

test(`${variant}: awsGlueSchemaRegistryResolver caches by schemaVersionId`, async () => {
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

test(`${variant}: awsGlueSchemaRegistryResolver respects cacheExpiry`, async () => {
	const client = mockClient(GlueClient);
	awsGlueSchemaRegistrySetClient(client);
	client.on(GetSchemaVersionCommand).resolves({
		SchemaVersionId: "v1",
		SchemaDefinition: "x",
		DataFormat: "AVRO",
	});

	const resolve = awsGlueSchemaRegistryResolver({ cacheExpiry: 1 });
	await resolve("v1");
	await new Promise((r) => setTimeout(r, 5));
	await resolve("v1");
	strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 2);
});

test(`${variant}: awsGlueSchemaRegistryResolver dedupes concurrent in-flight lookups`, async () => {
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

test(`${variant}: awsGlueSchemaRegistryResolver honors per-resolver clientOptions on the lazy-init path`, async () => {
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

test(`${variant}: awsGlueSchemaRegistryResolver evicts oldest entry past maxCacheSize`, async () => {
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

// *** setClient routes lookups to the configured default client *** //
test(`${variant}: awsGlueSchemaRegistrySetClient default client services resolvers without an explicit client`, async () => {
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
test(`${variant}: awsGlueSchemaRegistryResolver prefers an explicit client over the default`, async () => {
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
test(`${variant}: awsGlueSchemaRegistryResolver rejects a non-string id`, async () => {
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

test(`${variant}: awsGlueSchemaRegistryResolver rejects an empty-string id`, async () => {
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
test(`${variant}: awsGlueSchemaRegistryResolver default cacheExpiry caches indefinitely`, async () => {
	const client = mockClient(GlueClient);
	awsGlueSchemaRegistrySetClient(client);
	client.on(GetSchemaVersionCommand).resolves({
		SchemaVersionId: "v1",
		SchemaDefinition: "x",
		DataFormat: "AVRO",
	});

	// No cacheExpiry option => default -1 (sentinel for "never expires"). Even
	// after a real delay the entry must still be served from cache. A `+1`
	// mutant on the default would give a 1ms TTL and force a re-fetch.
	const resolve = awsGlueSchemaRegistryResolver();
	await resolve("v1");
	await new Promise((r) => setTimeout(r, 10));
	await resolve("v1");
	strictEqual(client.commandCalls(GetSchemaVersionCommand).length, 1);
});

// *** positive cacheExpiry serves from cache within the TTL window *** //
test(`${variant}: awsGlueSchemaRegistryResolver serves from cache within a positive TTL`, async () => {
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
test(`${variant}: awsGlueSchemaRegistrySetClient stores the default client reference`, async () => {
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
test(`${variant}: awsGlueSchemaRegistryResolver treats cacheExpiry 0 as a finite TTL`, async () => {
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
test(`${variant}: awsGlueSchemaRegistryResolver re-fetches an entry whose expiry equals now`, async () => {
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
test(`${variant}: glue default export exposes setClient and resolver`, () => {
	deepStrictEqual(Object.keys(glueDefault).sort(), ["resolver", "setClient"]);
	strictEqual(glueDefault.setClient, awsGlueSchemaRegistrySetClient);
	strictEqual(glueDefault.resolver, awsGlueSchemaRegistryResolver);
});
