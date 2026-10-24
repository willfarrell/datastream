/// <reference types="node" />

import { Readable, Writable } from "node:stream";
import type { DatastreamReadable, DatastreamWritable } from "@datastream/core";
import {
	type GcpStorageObjectMetadata,
	gcpStorageListStream,
	gcpStorageReadStream,
	gcpStorageWriteStream,
} from "@datastream/gcp/storage";
import { describe, expect, test } from "tstyche";

const file = {
	createReadStream: () => Readable.from([]),
	createWriteStream: () => new Writable(),
};
const bucket = {
	file: (_name: string) => file,
	getFiles: async (_query: object) => [[], null, {}] as [unknown[], null, {}],
};

describe("gcpStorageReadStream", () => {
	test("returns a byte readable", () => {
		expect(
			gcpStorageReadStream({ bucket, name: "a", start: 0, end: 9 }),
		).type.toBe<DatastreamReadable<Uint8Array>>();
	});
	test("bucket and name are required", () => {
		expect(gcpStorageReadStream).type.not.toBeCallableWith({ name: "a" });
		expect(gcpStorageReadStream).type.not.toBeCallableWith({ bucket });
	});
	test("validation is crc32c, md5 or a boolean", () => {
		expect(gcpStorageReadStream).type.toBeCallableWith({
			bucket,
			name: "a",
			validation: "md5",
		});
		expect(gcpStorageReadStream).type.not.toBeCallableWith({
			bucket,
			name: "a",
			validation: "sha1",
		});
	});
});

describe("gcpStorageWriteStream", () => {
	test("returns a writable", () => {
		expect(
			gcpStorageWriteStream({
				bucket,
				name: "a",
				resumable: false,
				contentType: "text/csv",
			}),
		).type.toBe<DatastreamWritable<Uint8Array | string>>();
	});
	test("bucket and name are required", () => {
		expect(gcpStorageWriteStream).type.not.toBeCallableWith({ name: "a" });
		expect(gcpStorageWriteStream).type.not.toBeCallableWith({ bucket });
	});
	test("name is a string", () => {
		expect(gcpStorageWriteStream).type.not.toBeCallableWith({
			bucket,
			name: 1,
		});
	});
});

describe("gcpStorageListStream", () => {
	test("returns a readable of object metadata", () => {
		expect(gcpStorageListStream({ bucket, prefix: "a/" })).type.toBe<
			DatastreamReadable<GcpStorageObjectMetadata>
		>();
	});
	test("bucket needs only getFiles", () => {
		expect(gcpStorageListStream).type.toBeCallableWith({
			bucket: { getFiles: bucket.getFiles },
		});
	});
	test("bucket is required and prefix is a string", () => {
		expect(gcpStorageListStream).type.not.toBeCallableWith({});
		expect(gcpStorageListStream).type.not.toBeCallableWith({
			bucket,
			prefix: 1,
		});
	});
});
