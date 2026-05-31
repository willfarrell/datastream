// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

// AWS regions that expose FIPS 140-2/140-3 validated endpoints. This includes
// the US/Canada commercial regions and BOTH GovCloud regions (which most need
// FIPS and were previously, incorrectly, excluded).
const fipsRegions = new Set([
	"us-east-1",
	"us-east-2",
	"us-west-1",
	"us-west-2",
	"ca-central-1",
	"ca-west-1",
	"us-gov-east-1",
	"us-gov-west-1",
]);

export const awsRegionSupportsFips = (region) => fipsRegions.has(region);

export const awsClientDefaults = {
	// Lazy getter so AWS_REGION is resolved when a client is constructed rather
	// than frozen at module-import time (test harnesses / lazy config set it
	// after import).
	get useFipsEndpoint() {
		// Guard the global `process` access so this shared module can also be
		// imported in non-node runtimes (browsers/edge) where `process` is
		// undefined; there it simply resolves to the non-FIPS default.
		return awsRegionSupportsFips(globalThis.process?.env?.AWS_REGION);
	},
};
