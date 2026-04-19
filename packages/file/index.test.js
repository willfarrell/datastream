// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, strictEqual, throws } from "node:assert";
import { mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { streamToString } from "@datastream/core";
import fileDefault, { fileReadStream, fileWriteStream } from "@datastream/file";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

const testDir = mkdtempSync(join(tmpdir(), "datastream-file-test-"));
const testFile = join(testDir, "test.csv");
const testContent = "a,b,c\n1,2,3\n";

test.before(() => {
	writeFileSync(testFile, testContent);
});

test.after(() => {
	rmSync(testDir, { recursive: true, force: true });
});

const csvTypes = [{ accept: { "text/csv": [".csv"] } }];
const jsonTypes = [{ accept: { "application/json": [".json"] } }];

// *** fileReadStream *** //
test(`${variant}: fileReadStream should read a file`, async () => {
	const stream = fileReadStream({ path: testFile });
	const output = await streamToString(stream);
	strictEqual(output, testContent);
});

// *** Path traversal *** //
test(`${variant}: fileReadStream should reject path traversal`, () => {
	throws(() => fileReadStream({ path: "/etc/passwd", basePath: testDir }), {
		message: "Path traversal detected",
	});
});

test(`${variant}: fileReadStream should reject relative path traversal`, () => {
	throws(
		() =>
			fileReadStream({
				path: join(testDir, "../../etc/passwd"),
				basePath: testDir,
			}),
		{ message: "Path traversal detected" },
	);
});

test(`${variant}: fileWriteStream should reject path traversal`, () => {
	throws(() => fileWriteStream({ path: "/etc/shadow", basePath: testDir }), {
		message: "Path traversal detected",
	});
});

test(`${variant}: fileReadStream should reject sibling-prefix bypass`, () => {
	// basePath '/tmp/foo' must not contain '/tmp/foobar/secret'
	throws(
		() =>
			fileReadStream({ path: `${testDir}-sibling/x.csv`, basePath: testDir }),
		{ message: "Path traversal detected" },
	);
});

// *** Symlink rejection *** //
test(`${variant}: fileReadStream should reject symlinks`, () => {
	const linkPath = join(testDir, "link.csv");
	symlinkSync(testFile, linkPath);
	throws(() => fileReadStream({ path: linkPath, basePath: testDir }), {
		message: "Symbolic links are not allowed",
	});
});

// *** Extension enforcement *** //
test(`${variant}: fileReadStream should accept matching extension`, async () => {
	const stream = fileReadStream({ path: testFile, types: csvTypes });
	await streamToString(stream);
});

test(`${variant}: fileReadStream should reject non-matching extension`, () => {
	throws(() => fileReadStream({ path: testFile, types: jsonTypes }), {
		message: "Invalid extension",
	});
});

test(`${variant}: fileReadStream should allow any extension when types is empty`, async () => {
	const stream = fileReadStream({ path: testFile, types: [] });
	await streamToString(stream);
});

test(`${variant}: fileWriteStream should reject non-matching extension`, () => {
	throws(
		() =>
			fileWriteStream({
				path: join(testDir, "out.csv"),
				types: jsonTypes,
			}),
		{ message: "Invalid extension" },
	);
});

// *** No basePath = no path checks *** //
test(`${variant}: fileReadStream allows any path without basePath`, async () => {
	const stream = fileReadStream({ path: testFile });
	await streamToString(stream);
});

// *** default export *** //
test(`${variant}: default export should include all stream functions`, () => {
	deepStrictEqual(Object.keys(fileDefault).sort(), [
		"readStream",
		"writeStream",
	]);
});
