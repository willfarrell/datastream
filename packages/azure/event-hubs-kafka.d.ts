// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

export interface AzureEventHubsKafkaMechanism {
	mechanism: "oauthbearer";
	oauthBearerProvider: () => Promise<{ value: string; expiryTime?: number }>;
}

export function azureEventHubsKafkaMechanism(options: {
	credential: {
		getToken: (
			scope: string,
		) => Promise<{ token: string; expiresOnTimestamp: number } | null>;
	};
	namespace: string;
}): AzureEventHubsKafkaMechanism;
