import { deepStrictEqual } from "node:assert";
import test, { describe } from "node:test";
import { variant } from "../variant.js";

describe(`@datastream/aws/client (${variant})`, async () => {
	// client.js has no build step (shipped as source); import it directly.
	const { awsClientDefaults, awsRegionSupportsFips } = await import(
		`file://${new URL("./client.js", import.meta.url).pathname}`
	);

	test(`awsClientDefaults.useFipsEndpoint resolves AWS_REGION lazily`, (_t) => {
		const original = process.env.AWS_REGION;
		try {
			process.env.AWS_REGION = "eu-west-1";
			deepStrictEqual(awsClientDefaults.useFipsEndpoint, false);

			// Changing AWS_REGION after import must be reflected (not frozen at import)
			process.env.AWS_REGION = "us-east-1";
			deepStrictEqual(awsClientDefaults.useFipsEndpoint, true);
		} finally {
			if (original === undefined) {
				delete process.env.AWS_REGION;
			} else {
				process.env.AWS_REGION = original;
			}
		}
	});

	test(`awsRegionSupportsFips includes GovCloud regions`, (_t) => {
		deepStrictEqual(awsRegionSupportsFips("us-gov-east-1"), true);
		deepStrictEqual(awsRegionSupportsFips("us-gov-west-1"), true);
		deepStrictEqual(awsRegionSupportsFips("ca-central-1"), true);
		deepStrictEqual(awsRegionSupportsFips("eu-west-1"), false);
		deepStrictEqual(awsRegionSupportsFips(undefined), false);
	});

	// client.js is shipped as raw source (no build step; not an export), so there is
	// no client.node.mjs to import. Exercise the same source module to assert the
	// full FIPS region set, the lookup function and the lazy getter.
	const built = await import(
		`file://${new URL("./client.js", import.meta.url).pathname}`
	);

	test(`built awsRegionSupportsFips matches the exact FIPS region set`, (_t) => {
		// Every region that must be FIPS-capable (kills StringLiteral mutants that
		// blank an individual region, and the Set/array-declaration mutants).
		for (const region of [
			"us-east-1",
			"us-east-2",
			"us-west-1",
			"us-west-2",
			"ca-central-1",
			"ca-west-1",
			"us-gov-east-1",
			"us-gov-west-1",
		]) {
			deepStrictEqual(built.awsRegionSupportsFips(region), true);
		}
		// Regions NOT in the set must be false (kills the `() => undefined` mutant,
		// which would make every lookup falsy/undefined rather than a real boolean).
		deepStrictEqual(built.awsRegionSupportsFips("eu-west-1"), false);
		deepStrictEqual(built.awsRegionSupportsFips("ap-southeast-2"), false);
		deepStrictEqual(built.awsRegionSupportsFips(undefined), false);
	});

	test(`built awsClientDefaults.useFipsEndpoint reflects AWS_REGION lazily`, (_t) => {
		const original = process.env.AWS_REGION;
		try {
			process.env.AWS_REGION = "us-west-1";
			deepStrictEqual(built.awsClientDefaults.useFipsEndpoint, true);
			process.env.AWS_REGION = "eu-central-1";
			deepStrictEqual(built.awsClientDefaults.useFipsEndpoint, false);
		} finally {
			if (original === undefined) {
				delete process.env.AWS_REGION;
			} else {
				process.env.AWS_REGION = original;
			}
		}
	});
});
