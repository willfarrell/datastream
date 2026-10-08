// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

// Event Hubs Kafka endpoint (<namespace>.servicebus.windows.net:9093) with
// Microsoft Entra ID: SASL OAUTHBEARER using a token for the namespace.
// credential = any @azure/identity TokenCredential (DefaultAzureCredential, ...).
export const azureEventHubsKafkaMechanism = ({
	credential,
	namespace,
} = {}) => {
	if (!credential) {
		throw new TypeError("azureEventHubsKafkaMechanism: credential required");
	}
	if (!namespace) {
		throw new TypeError("azureEventHubsKafkaMechanism: namespace required");
	}
	const scope = `https://${namespace}.servicebus.windows.net/.default`;
	return {
		mechanism: "oauthbearer",
		oauthBearerProvider: async () => {
			// TokenCredential.getToken may resolve null when no token is available.
			const accessToken = await credential.getToken(scope);
			if (!accessToken) {
				throw new Error(
					"azureEventHubsKafkaMechanism: credential returned no token",
				);
			}
			const { token, expiresOnTimestamp } = accessToken;
			return { value: token, expiryTime: expiresOnTimestamp };
		},
	};
};
