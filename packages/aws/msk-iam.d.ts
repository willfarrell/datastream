// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

export interface AwsMskIamMechanism {
	mechanism: "oauthbearer";
	oauthBearerProvider: () => Promise<{ value: string; expiryTime?: number }>;
}

export function awsMskIamMechanism(options: {
	region: string;
	awsDebugCreds?: boolean;
	ttl?: number;
}): AwsMskIamMechanism;
