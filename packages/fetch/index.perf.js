/* global Headers, Response */
import test from "node:test";
import {
	createReadableStream,
	pipeline,
	streamToArray,
} from "@datastream/core";
import {
	fetchResponseStream,
	fetchSetDefaults,
	fetchWritableStream,
} from "@datastream/fetch";
import { Bench } from "tinybench";

// -- Mock setup --

const time = 5_000;
const ITEMS = 1_000;
const PAGE_SIZE = 100;

const generatePage = (offset, size) => ({
	data: Array.from({ length: size }, (_, i) => ({
		id: offset + i,
		name: `item_${offset + i}`,
		value: Math.random(),
	})),
});

global.fetch = (url, _options) => {
	const parsed = new URL(url);

	if (parsed.pathname === "/json-single") {
		return Promise.resolve(
			new Response(JSON.stringify(generatePage(0, ITEMS)), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			}),
		);
	}

	if (parsed.pathname === "/json-paginated") {
		const offset = Number.parseInt(
			parsed.searchParams.get("$offset") ?? "0",
			10,
		);
		const page = generatePage(offset, Math.min(PAGE_SIZE, ITEMS - offset));
		return Promise.resolve(
			new Response(JSON.stringify(page), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			}),
		);
	}

	if (parsed.pathname === "/csv") {
		const rows = Array.from(
			{ length: ITEMS },
			(_, i) => `${i},item_${i},${Math.random()}`,
		);
		const csv = `id,name,value\n${rows.join("\n")}\n`;
		return Promise.resolve(
			new Response(csv, {
				status: 200,
				headers: new Headers({ "Content-Type": "text/csv" }),
			}),
		);
	}

	if (parsed.pathname === "/upload") {
		return Promise.resolve(
			new Response(JSON.stringify({ uploaded: true }), {
				status: 200,
				headers: new Headers({ "Content-Type": "application/json" }),
			}),
		);
	}

	return Promise.resolve(new Response("", { status: 404 }));
};

// -- Tests --

test("perf: fetchResponseStream (single JSON response)", async () => {
	const bench = new Bench({ name: "fetchResponseStream (single JSON)", time });
	fetchSetDefaults({
		dataPath: "data",
		rateLimit: 0,
		headers: { Accept: "application/json" },
	});

	bench.add(`${ITEMS} items, single page`, async () => {
		const config = [{ url: "https://example.org/json-single" }];
		const stream = fetchResponseStream(config);
		await streamToArray(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: fetchResponseStream (paginated JSON)", async () => {
	const bench = new Bench({
		name: "fetchResponseStream (paginated JSON)",
		time,
	});
	fetchSetDefaults({
		dataPath: "data",
		rateLimit: 0,
		headers: { Accept: "application/json" },
	});

	bench.add(
		`${ITEMS} items, ${PAGE_SIZE}/page, offset pagination`,
		async () => {
			const config = {
				url: "https://example.org/json-paginated",
				dataPath: "data",
				offsetParam: "$offset",
				offsetAmount: PAGE_SIZE,
				qs: { $offset: 0 },
			};
			const stream = fetchResponseStream(config);
			await streamToArray(stream);
		},
	);

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: fetchResponseStream (CSV body)", async () => {
	const bench = new Bench({ name: "fetchResponseStream (CSV)", time });
	fetchSetDefaults({ rateLimit: 0, headers: { Accept: "text/csv" } });

	bench.add(`${ITEMS} rows CSV`, async () => {
		const config = [{ url: "https://example.org/csv" }];
		const stream = fetchResponseStream(config);
		await streamToArray(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});

test("perf: fetchWritableStream", async () => {
	const bench = new Bench({ name: "fetchWritableStream", time });
	fetchSetDefaults({ rateLimit: 0 });

	bench.add("upload 1MB string", async () => {
		const data = "x".repeat(1_024 * 1_024);
		const stream = [
			createReadableStream(data),
			await fetchWritableStream({
				url: "https://example.org/upload",
				method: "POST",
			}),
		];
		await pipeline(stream);
	});

	await bench.run();
	console.log(`\n${bench.name}`);
	console.table(bench.table());
});
