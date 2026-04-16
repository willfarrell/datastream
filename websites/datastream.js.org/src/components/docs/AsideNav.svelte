<script>
import LayoutAsideNav from "@design-system/components/LayoutAsideNav.svelte";
import { page } from "$app/state";

const { params, data, form } = page;

const nav = {
	"Getting Started": {
		Introduction: "/docs",
		"Quick Start": "/docs/quick-start",
		"Core Concepts": "/docs/core-concepts",
		core: "/docs/packages/core",
	},
	"I/O": {
		file: "/docs/packages/file",
		fetch: "/docs/packages/fetch",
		aws: "/docs/packages/aws",
		indexeddb: "/docs/packages/indexeddb",
		ipfs: "/docs/packages/ipfs",
	},
	"Data Formats": {
		csv: "/docs/packages/csv",
		string: "/docs/packages/string",
		object: "/docs/packages/object",
		base64: "/docs/packages/base64",
		charset: "/docs/packages/charset",
	},
	Processing: {
		validate: "/docs/packages/validate",
		compress: "/docs/packages/compress",
		digest: "/docs/packages/digest",
	},
	Examples: {
		"CSV ETL": "/docs/examples/csv-etl",
		"API to DynamoDB": "/docs/examples/api-pagination-dynamodb",
		"S3 to S3": "/docs/examples/s3-transform",
		"Browser files": "/docs/examples/browser-file-processing",
		"Checksum & compress": "/docs/examples/checksum-compress",
		Parquet: "/docs/examples/parquet",
	},
};
</script>

<LayoutAsideNav {nav} />
