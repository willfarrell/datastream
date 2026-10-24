import { deepStrictEqual, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import * as gcp from "@datastream/gcp";
import * as bigquery from "@datastream/gcp/bigquery";
import * as pubsub from "@datastream/gcp/pubsub";
import * as storage from "@datastream/gcp/storage";

describe("@datastream/gcp", () => {
	test(`the root re-exports every subpath, and nothing else`, () => {
		deepStrictEqual(Object.keys(gcp).sort(), [
			"gcpBigQueryInsertStream",
			"gcpBigQueryLoadStream",
			"gcpBigQueryQueryStream",
			"gcpPubSubAckStream",
			"gcpPubSubNackStream",
			"gcpPubSubPublishStream",
			"gcpPubSubSubscribeStream",
			"gcpStorageListStream",
			"gcpStorageReadStream",
			"gcpStorageWriteStream",
		]);
		const root = new Map(Object.entries(gcp));
		for (const sub of [bigquery, pubsub, storage]) {
			for (const [name, value] of Object.entries(sub)) {
				strictEqual(root.get(name), value, name);
			}
		}
	});
});
