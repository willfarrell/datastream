// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, ok, strictEqual } from "node:assert";
import test from "node:test";
import mskDefault, { awsMskIamMechanism } from "@datastream/aws/msk-iam";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

test(`${variant}: awsMskIamMechanism returns kafkajs OAUTHBEARER config`, () => {
	const mech = awsMskIamMechanism({ region: "us-east-1" });
	strictEqual(mech.mechanism, "oauthbearer");
	ok(typeof mech.oauthBearerProvider === "function");
});

test(`${variant}: awsMskIamMechanism rejects missing region`, () => {
	try {
		awsMskIamMechanism({});
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("region"));
	}
});

test(`${variant}: msk-iam default export exposes the mechanism factory`, () => {
	deepStrictEqual(Object.keys(mskDefault), ["mechanism"]);
	strictEqual(mskDefault.mechanism, awsMskIamMechanism);
});

// generateAuthToken presigns a Kafka URL locally from static credentials (no
// network), so we can exercise oauthBearerProvider end-to-end by supplying
// throwaway credentials via the environment for the duration of the call.
test(`${variant}: oauthBearerProvider returns a token and expiry`, async () => {
	const prev = {
		id: process.env.AWS_ACCESS_KEY_ID,
		secret: process.env.AWS_SECRET_ACCESS_KEY,
		region: process.env.AWS_REGION,
	};
	process.env.AWS_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE";
	process.env.AWS_SECRET_ACCESS_KEY =
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
	process.env.AWS_REGION = "us-east-1";
	try {
		const mech = awsMskIamMechanism({ region: "us-east-1" });
		const result = await mech.oauthBearerProvider();
		// The provider must surface generateAuthToken's { token, expiryTime } as
		// { value, expiryTime } — a `{}` body (or a `{}` return) drops these.
		ok(typeof result.value === "string");
		ok(result.value.length > 0);
		ok(typeof result.expiryTime === "number");
		ok(result.expiryTime > 0);
	} finally {
		if (prev.id === undefined) delete process.env.AWS_ACCESS_KEY_ID;
		else process.env.AWS_ACCESS_KEY_ID = prev.id;
		if (prev.secret === undefined) delete process.env.AWS_SECRET_ACCESS_KEY;
		else process.env.AWS_SECRET_ACCESS_KEY = prev.secret;
		if (prev.region === undefined) delete process.env.AWS_REGION;
		else process.env.AWS_REGION = prev.region;
	}
});

// The `generateAuthToken({ region, awsDebugCreds, ttl })` argument object must
// forward the resolver's region; a `{}` mutant drops region and the signer
// rejects with a region-required error. We assert the provider rejects when no
// region can be resolved from args or environment.
test(`${variant}: oauthBearerProvider forwards region to the signer`, async () => {
	const prevRegion = process.env.AWS_REGION;
	const prevDefault = process.env.AWS_DEFAULT_REGION;
	delete process.env.AWS_REGION;
	delete process.env.AWS_DEFAULT_REGION;
	try {
		// Build the mechanism with a region so awsMskIamMechanism itself passes,
		// then strip the env so the ONLY region the signer can see is the one
		// forwarded inside the generateAuthToken({ region }) object literal.
		const mech = awsMskIamMechanism({ region: "us-east-1" });
		process.env.AWS_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE";
		process.env.AWS_SECRET_ACCESS_KEY =
			"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
		const result = await mech.oauthBearerProvider();
		ok(typeof result.value === "string");
		ok(result.value.length > 0);
	} finally {
		delete process.env.AWS_ACCESS_KEY_ID;
		delete process.env.AWS_SECRET_ACCESS_KEY;
		if (prevRegion !== undefined) process.env.AWS_REGION = prevRegion;
		if (prevDefault !== undefined) process.env.AWS_DEFAULT_REGION = prevDefault;
	}
});
