import test from "node:test";
import { fileReadStream, fileWriteStream } from "@datastream/file";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = ["Invalid extension"];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	if (e.code === "ENOENT") {
		return;
	}
	console.error(input, e);
	throw e;
};

const types = [
	{ accept: { "text/csv": [".csv"] } },
	{ accept: { "application/json": [".json"] } },
];

// *** fileReadStream w/ path *** //
test("fuzz fileReadStream w/ path", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (path) => {
			try {
				fileReadStream({ path, types });
			} catch (e) {
				catchError(path, e);
			}
		}),
		{
			numRuns: 10_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** fileWriteStream w/ path *** //
test("fuzz fileWriteStream w/ path", async () => {
	await fc.assert(
		fc.asyncProperty(fc.string(), async (path) => {
			try {
				fileWriteStream({ path, types });
			} catch (e) {
				catchError(path, e);
			}
		}),
		{
			numRuns: 10_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** fileReadStream w/ types *** //
test("fuzz fileReadStream w/ types", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.array(
				fc.record({
					accept: fc.dictionary(
						fc.string(),
						fc.array(fc.string({ minLength: 1, maxLength: 5 })),
					),
				}),
			),
			async (fuzzTypes) => {
				try {
					// Only test the type enforcement logic, not the actual file open
					fileReadStream({ path: "/dev/null", types: fuzzTypes });
				} catch (e) {
					catchError(fuzzTypes, e);
				}
			},
		),
		{
			numRuns: 10_000,
			verbose: 2,
			examples: [],
		},
	);
});
