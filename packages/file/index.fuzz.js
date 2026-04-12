import { strictEqual } from "node:assert";
import test from "node:test";
import { fileReadStream, fileWriteStream } from "@datastream/file";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [
		"Invalid extension",
		"Path traversal detected",
		"Symbolic links are not allowed",
	];
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

// *** path traversal protection regression *** //
test("fileReadStream should reject path traversal when basePath is set", () => {
	try {
		fileReadStream({
			path: "/etc/passwd",
			basePath: "/tmp/safe",
			types: [],
		});
		throw new Error("Should have thrown");
	} catch (e) {
		strictEqual(e.message, "Path traversal detected");
	}
});

test("fileReadStream should reject relative path traversal when basePath is set", () => {
	try {
		fileReadStream({
			path: "/tmp/safe/../../etc/passwd",
			basePath: "/tmp/safe",
			types: [],
		});
		throw new Error("Should have thrown");
	} catch (e) {
		strictEqual(e.message, "Path traversal detected");
	}
});

test("fileWriteStream should reject path traversal when basePath is set", () => {
	try {
		fileWriteStream({
			path: "/etc/shadow",
			basePath: "/tmp/safe",
			types: [],
		});
		throw new Error("Should have thrown");
	} catch (e) {
		strictEqual(e.message, "Path traversal detected");
	}
});
