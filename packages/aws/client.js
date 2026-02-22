// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
export const awsClientDefaults = {
	useFipsEndpoint: [
		"us-east-1",
		"us-east-2",
		"us-west-1",
		"us-west-2",
		"ca-central-1",
	].includes(process.env.AWS_REGION),
};
