// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
export const awsGlueSchemaRegistrySetClient = (_client) => {
	throw new Error("awsGlueSchemaRegistrySetClient: Not supported");
};

export const awsGlueSchemaRegistryResolver = (_options = {}) => {
	throw new Error("awsGlueSchemaRegistryResolver: Not supported");
};

export default {
	setClient: awsGlueSchemaRegistrySetClient,
	resolver: awsGlueSchemaRegistryResolver,
};
