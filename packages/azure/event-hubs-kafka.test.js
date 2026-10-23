import { deepStrictEqual, rejects, strictEqual, throws } from "node:assert";
import test, { describe } from "node:test";
import * as azureModule from "@datastream/azure/event-hubs-kafka";
import { azureEventHubsKafkaMechanism } from "@datastream/azure/event-hubs-kafka";
import { variant } from "../variant.js";

describe(`@datastream/azure/event-hubs-kafka (${variant})`, () => {
	test(`azureEventHubsKafkaMechanism returns a namespace-scoped token`, async () => {
		let scope;
		const credential = {
			getToken: async (s) => {
				scope = s;
				return { token: "t", expiresOnTimestamp: 123 };
			},
		};
		const mech = azureEventHubsKafkaMechanism({
			credential,
			namespace: "my-namespace",
		});
		strictEqual(mech.mechanism, "oauthbearer");
		deepStrictEqual(await mech.oauthBearerProvider(), {
			value: "t",
			expiryTime: 123,
		});
		strictEqual(scope, "https://my-namespace.servicebus.windows.net/.default");
	});

	test(`azureEventHubsKafkaMechanism rejects when the credential returns no token`, async () => {
		const credential = { getToken: async () => null };
		const mech = azureEventHubsKafkaMechanism({
			credential,
			namespace: "my-namespace",
		});
		await rejects(mech.oauthBearerProvider(), {
			message: "azureEventHubsKafkaMechanism: credential returned no token",
		});
	});

	test(`azureEventHubsKafkaMechanism requires credential and namespace`, () => {
		throws(() => azureEventHubsKafkaMechanism(), /credential/);
		throws(() => azureEventHubsKafkaMechanism({ credential: {} }), /namespace/);
	});

	// Azure namespace naming rules: 6-50 letters, digits or hyphens, starting
	// with a letter and ending with a letter or digit.
	const namespaceError = {
		name: "TypeError",
		message:
			"azureEventHubsKafkaMechanism: namespace must be 6-50 letters, digits or hyphens, start with a letter and end with a letter or digit",
	};
	for (const namespace of [
		"abcdef",
		"Abcde1",
		"a1B-cZ",
		`Z${"a".repeat(48)}9`,
	]) {
		test(`azureEventHubsKafkaMechanism accepts namespace ${namespace}`, async () => {
			let scope;
			const credential = {
				getToken: async (s) => {
					scope = s;
					return { token: "t", expiresOnTimestamp: 1 };
				},
			};
			await azureEventHubsKafkaMechanism({
				credential,
				namespace,
			}).oauthBearerProvider();
			strictEqual(
				scope,
				`https://${namespace}.servicebus.windows.net/.default`,
			);
		});
	}
	for (const namespace of [
		"abcde",
		`a${"b".repeat(50)}`,
		"1abcdef",
		"-abcdef",
		"abcdef-",
		"abc_def",
		"abc def",
		"my-ns.evil.example",
		"evil.example/x?",
		"abcdef\n",
	]) {
		test(`azureEventHubsKafkaMechanism rejects namespace ${JSON.stringify(namespace)}`, () => {
			throws(
				() => azureEventHubsKafkaMechanism({ credential: {}, namespace }),
				namespaceError,
			);
		});
	}

	test(`event-hubs-kafka has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
