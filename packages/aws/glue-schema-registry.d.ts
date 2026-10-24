// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

export interface GlueSchemaVersion {
	schemaVersionId: string;
	schemaDefinition: string;
	dataFormat: string;
}

export function awsGlueSchemaRegistrySetClient(client: unknown): void;

export function awsGlueSchemaRegistryResolver(options?: {
	client?: unknown;
	clientOptions?: Record<string, unknown>;
	cacheExpiry?: number;
	// Max cached schema versions (default 1000); 0 disables caching, null = unlimited.
	maxCacheSize?: number | null;
}): (schemaVersionId: string) => Promise<GlueSchemaVersion>;
