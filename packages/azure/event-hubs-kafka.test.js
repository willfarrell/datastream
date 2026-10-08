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
		const mech = azureEventHubsKafkaMechanism({ credential, namespace: "ns" });
		strictEqual(mech.mechanism, "oauthbearer");
		deepStrictEqual(await mech.oauthBearerProvider(), {
			value: "t",
			expiryTime: 123,
		});
		strictEqual(scope, "https://ns.servicebus.windows.net/.default");
	});

	test(`azureEventHubsKafkaMechanism rejects when the credential returns no token`, async () => {
		const credential = { getToken: async () => null };
		const mech = azureEventHubsKafkaMechanism({ credential, namespace: "ns" });
		await rejects(mech.oauthBearerProvider(), {
			message: "azureEventHubsKafkaMechanism: credential returned no token",
		});
	});

	test(`azureEventHubsKafkaMechanism requires credential and namespace`, () => {
		throws(() => azureEventHubsKafkaMechanism(), /credential/);
		throws(() => azureEventHubsKafkaMechanism({ credential: {} }), /namespace/);
	});

	test(`event-hubs-kafka has no default export`, () => {
		strictEqual(Object.hasOwn(azureModule, "default"), false);
	});
});
