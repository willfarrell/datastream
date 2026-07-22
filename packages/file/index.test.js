// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, strictEqual, throws } from "node:assert";
import {
	mkdtempSync,
	readFileSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
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

// *** fileReadStream with basePath - success case *** //
test(`${variant}: fileReadStream with basePath should read a file`, async () => {
	const stream = fileReadStream({ path: testFile, basePath: testDir });
	const output = await streamToString(stream);
	strictEqual(output, testContent);
});

// *** fileReadStream without basePath should follow symlinks *** //
test(`${variant}: fileReadStream without basePath can read through symlink`, async () => {
	const linkPath = join(testDir, "sym-no-base.csv");
	try {
		symlinkSync(testFile, linkPath);
	} catch (_) {
		// already exists
	}
	// Without basePath, no O_NOFOLLOW - symlinks are followed (read succeeds)
	const stream = fileReadStream({ path: linkPath });
	const output = await streamToString(stream);
	strictEqual(output, testContent);
});

// *** fileWriteStream without basePath - follows symlinks *** //
test(`${variant}: fileWriteStream without basePath can write through symlink`, async () => {
	const outFile = join(testDir, "real-target.csv");
	writeFileSync(outFile, "");
	const writeLinkPath = join(testDir, "write-sym-no-base.csv");
	try {
		symlinkSync(outFile, writeLinkPath);
	} catch (_) {
		// already exists
	}
	// Without basePath: no O_NOFOLLOW - symlinks are followed (write succeeds)
	const stream = fileWriteStream({ path: writeLinkPath });
	await new Promise((resolve, reject) => {
		stream.on("finish", resolve);
		stream.on("error", reject);
		stream.write(Buffer.from("via-symlink"));
		stream.end();
	});
	strictEqual(readFileSync(outFile, "utf8"), "via-symlink");
});

// *** fileWriteStream with basePath - success case *** //
test(`${variant}: fileWriteStream with basePath should write a new file`, async () => {
	const outFile = join(testDir, "written.csv");
	const stream = fileWriteStream({ path: outFile, basePath: testDir });
	await new Promise((resolve, reject) => {
		stream.on("finish", resolve);
		stream.on("error", reject);
		stream.write(Buffer.from("written content"));
		stream.end();
	});
	strictEqual(readFileSync(outFile, "utf8"), "written content");
});

// *** fileReadStream with basePath - streamOptions passed through *** //
test(`${variant}: fileReadStream with basePath passes streamOptions`, async () => {
	const stream = fileReadStream(
		{ path: testFile, basePath: testDir },
		{ highWaterMark: 1 },
	);
	const output = await streamToString(stream);
	strictEqual(output, testContent);
});

// *** fileWriteStream with basePath - streamOptions passed through *** //
test(`${variant}: fileWriteStream with basePath passes streamOptions`, async () => {
	const outFile = join(testDir, "opts.csv");
	const stream = fileWriteStream(
		{ path: outFile, basePath: testDir },
		{ highWaterMark: 1 },
	);
	await new Promise((resolve, reject) => {
		stream.on("finish", resolve);
		stream.on("error", reject);
		stream.write(Buffer.from("opts"));
		stream.end();
	});
	strictEqual(readFileSync(outFile, "utf8"), "opts");
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

// *** Path traversal - basePath itself (rel === '') *** //
test(`${variant}: fileReadStream rejects when path equals basePath`, () => {
	throws(() => fileReadStream({ path: testDir, basePath: testDir }), {
		message: "Path traversal detected",
	});
});

test(`${variant}: fileWriteStream rejects when path equals basePath`, () => {
	throws(() => fileWriteStream({ path: testDir, basePath: testDir }), {
		message: "Path traversal detected",
	});
});

// *** Symlink rejection *** //
test(`${variant}: fileReadStream should reject symlinks`, () => {
	const linkPath = join(testDir, "link.csv");
	try {
		symlinkSync(testFile, linkPath);
	} catch (_) {
		// already exists
	}
	throws(() => fileReadStream({ path: linkPath, basePath: testDir }), {
		message: "Symbolic links are not allowed",
	});
});

test(`${variant}: fileWriteStream should reject symlinks`, () => {
	const linkPath = join(testDir, "write-link.csv");
	try {
		symlinkSync(testFile, linkPath);
	} catch (_) {
		// already exists
	}
	throws(() => fileWriteStream({ path: linkPath, basePath: testDir }), {
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

// *** ENOENT is allowed (new file for write) *** //
test(`${variant}: fileWriteStream with basePath allows new file (ENOENT ok)`, async () => {
	const outFile = join(testDir, "brand-new.csv");
	const stream = fileWriteStream({ path: outFile, basePath: testDir });
	await new Promise((resolve, reject) => {
		stream.on("finish", resolve);
		stream.on("error", reject);
		stream.write(Buffer.from("new"));
		stream.end();
	});
	strictEqual(readFileSync(outFile, "utf8"), "new");
});

// *** Path not found for non-ENOENT stat errors *** //
test(`${variant}: fileReadStream throws Path not found for non-ENOENT stat error`, () => {
	// testFile is a regular file, not a directory - accessing subpath causes ENOTDIR
	const notDir = join(testFile, "inside.csv");
	throws(() => fileReadStream({ path: notDir, basePath: testDir }), {
		message: "Path not found",
	});
});

// *** Path not found error has a cause *** //
test(`${variant}: fileReadStream Path not found error carries cause`, () => {
	const notDir = join(testFile, "inside.csv");
	throws(
		() => fileReadStream({ path: notDir, basePath: testDir }),
		(e) => {
			strictEqual(e.message, "Path not found");
			strictEqual(typeof e.cause, "object");
			return true;
		},
	);
});

// *** No basePath = no path checks *** //
test(`${variant}: fileReadStream allows any path without basePath`, async () => {
	const stream = fileReadStream({ path: testFile });
	await streamToString(stream);
});

// *** fileWriteStream with basePath: failed constructor leaves existing file intact *** //
test(`${variant}: fileWriteStream with basePath preserves existing file when stream constructor fails`, () => {
	const guardFile = join(testDir, "guard.csv");
	const originalContent = "important,data\n1,2,3\n";
	writeFileSync(guardFile, originalContent);
	// {start:-5} is an invalid option that makes createWriteStream throw synchronously
	throws(
		() =>
			fileWriteStream({ path: guardFile, basePath: testDir }, { start: -5 }),
		(e) => typeof e === "object" && e !== null,
	);
	// The file must still contain its original content — not be wiped to zero bytes
	strictEqual(readFileSync(guardFile, "utf8"), originalContent);
});

// *** fd-based streams when basePath is provided *** //
// When basePath is set, fileReadStream must use openSync+O_NOFOLLOW and pass the
// resulting fd to createReadStream (not the file path).  The returned stream's .fd
// property is already a number in that case, whereas a path-based stream has .fd===null
// until the OS opens it.  Mutants that skip the if(basePath!=null) block would produce
// a path-based stream whose .fd is null at construction time.
test(`${variant}: fileReadStream with basePath returns an fd-based stream`, () => {
	const stream = fileReadStream({ path: testFile, basePath: testDir });
	strictEqual(typeof stream.fd, "number");
	stream.destroy();
});

test(`${variant}: fileWriteStream with basePath returns an fd-based stream`, () => {
	const outFile = join(testDir, "fd-write-check.csv");
	writeFileSync(outFile, "");
	const stream = fileWriteStream({ path: outFile, basePath: testDir });
	strictEqual(typeof stream.fd, "number");
	stream.destroy();
});

// *** default export *** //
test(`${variant}: default export should include all stream functions`, () => {
	deepStrictEqual(Object.keys(fileDefault).sort(), [
		"readStream",
		"writeStream",
	]);
});
