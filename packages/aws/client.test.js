import { deepStrictEqual } from "node:assert";
import test, { describe } from "node:test";
import { variant } from "../variant.js";

describe(`@datastream/aws/client (${variant})`, async () => {
	const { awsClientDefaults, awsRegionSupportsFips } = await import(
		`file://${new URL("./client.js", import.meta.url).pathname}`
	);

	test(`awsClientDefaults.useFipsEndpoint resolves AWS_REGION lazily`, (_t) => {
		const original = process.env.AWS_REGION;
		try {
			process.env.AWS_REGION = "eu-west-1";
			deepStrictEqual(awsClientDefaults.useFipsEndpoint, false);

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

	const built = await import(
		`file://${new URL("./client.js", import.meta.url).pathname}`
	);

	test(`built awsRegionSupportsFips matches the exact FIPS region set`, (_t) => {
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
