// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import { once } from "node:events";
import {
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	statSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test, { describe } from "node:test";
import {
	createReadableStream,
	isWritable,
	pipeline,
	streamToString,
} from "@datastream/core";
import { fileReadStream, fileWriteStream } from "@datastream/file";
import { variant } from "../variant.js";

describe(`@datastream/file (${variant})`, () => {
	const nodeTest = variant === "node" ? test : test.skip;

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

	nodeTest(`fileReadStream should read a file`, async () => {
		const stream = await fileReadStream({ path: testFile });
		const output = await streamToString(stream);
		strictEqual(output, testContent);
	});

	nodeTest(`fileReadStream with basePath should read a file`, async () => {
		const stream = await fileReadStream({ path: testFile, basePath: testDir });
		const output = await streamToString(stream);
		strictEqual(output, testContent);
	});

	nodeTest(
		`fileReadStream without basePath can read through symlink`,
		async () => {
			const linkPath = join(testDir, "sym-no-base.csv");
			try {
				symlinkSync(testFile, linkPath);
			} catch (_) {}
			const stream = await fileReadStream({ path: linkPath });
			const output = await streamToString(stream);
			strictEqual(output, testContent);
		},
	);

	nodeTest(
		`fileWriteStream without basePath can write through symlink`,
		async () => {
			const outFile = join(testDir, "real-target.csv");
			writeFileSync(outFile, "");
			const writeLinkPath = join(testDir, "write-sym-no-base.csv");
			try {
				symlinkSync(outFile, writeLinkPath);
			} catch (_) {}
			const stream = await fileWriteStream({ path: writeLinkPath });
			await new Promise((resolve, reject) => {
				stream.on("finish", resolve);
				stream.on("error", reject);
				stream.write(Buffer.from("via-symlink"));
				stream.end();
			});
			strictEqual(readFileSync(outFile, "utf8"), "via-symlink");
		},
	);

	nodeTest(
		`fileWriteStream with basePath should write a new file`,
		async () => {
			const outFile = join(testDir, "written.csv");
			const stream = await fileWriteStream({
				path: outFile,
				basePath: testDir,
			});
			await new Promise((resolve, reject) => {
				stream.on("finish", resolve);
				stream.on("error", reject);
				stream.write(Buffer.from("written content"));
				stream.end();
			});
			strictEqual(readFileSync(outFile, "utf8"), "written content");
		},
	);

	nodeTest(`fileReadStream with basePath passes streamOptions`, async () => {
		const stream = await fileReadStream(
			{ path: testFile, basePath: testDir },
			{ highWaterMark: 1 },
		);
		const output = await streamToString(stream);
		strictEqual(output, testContent);
	});

	nodeTest(`fileWriteStream with basePath passes streamOptions`, async () => {
		const outFile = join(testDir, "opts.csv");
		const stream = await fileWriteStream(
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

	nodeTest(`fileReadStream should reject path traversal`, async () => {
		await rejects(
			() => fileReadStream({ path: "/etc/passwd", basePath: testDir }),
			{
				message: "Path traversal detected",
			},
		);
	});

	nodeTest(`fileReadStream should reject relative path traversal`, async () => {
		await rejects(
			() =>
				fileReadStream({
					path: join(testDir, "../../etc/passwd"),
					basePath: testDir,
				}),
			{ message: "Path traversal detected" },
		);
	});

	nodeTest(`fileWriteStream should reject path traversal`, async () => {
		await rejects(
			() => fileWriteStream({ path: "/etc/shadow", basePath: testDir }),
			{
				message: "Path traversal detected",
			},
		);
	});

	nodeTest(`fileReadStream should reject sibling-prefix bypass`, async () => {
		await rejects(
			() =>
				fileReadStream({ path: `${testDir}-sibling/x.csv`, basePath: testDir }),
			{ message: "Path traversal detected" },
		);
	});

	nodeTest(`fileReadStream rejects when path equals basePath`, async () => {
		await rejects(() => fileReadStream({ path: testDir, basePath: testDir }), {
			message: "Path traversal detected",
		});
	});

	nodeTest(`fileWriteStream rejects when path equals basePath`, async () => {
		await rejects(() => fileWriteStream({ path: testDir, basePath: testDir }), {
			message: "Path traversal detected",
		});
	});

	nodeTest(
		`fileReadStream and fileWriteStream reject path equal to a symlinked basePath`,
		async () => {
			const realBase = join(testDir, "realbase");
			const linkBase = join(testDir, "linkbase");
			mkdirSync(realBase);
			symlinkSync(realBase, linkBase);
			try {
				await rejects(
					() => fileReadStream({ path: linkBase, basePath: linkBase }),
					{ message: "Path traversal detected" },
				);
				await rejects(
					() => fileWriteStream({ path: linkBase, basePath: linkBase }),
					{ message: "Path traversal detected" },
				);
			} finally {
				rmSync(linkBase, { force: true });
				rmSync(realBase, { recursive: true, force: true });
			}
		},
	);

	nodeTest(`fileReadStream should reject symlinks`, async () => {
		const linkPath = join(testDir, "link.csv");
		try {
			symlinkSync(testFile, linkPath);
		} catch (_) {}
		await rejects(() => fileReadStream({ path: linkPath, basePath: testDir }), {
			message: "Symbolic links are not allowed",
		});
	});

	nodeTest(`fileWriteStream should reject symlinks`, async () => {
		const linkPath = join(testDir, "write-link.csv");
		try {
			symlinkSync(testFile, linkPath);
		} catch (_) {}
		await rejects(
			() => fileWriteStream({ path: linkPath, basePath: testDir }),
			{
				message: "Symbolic links are not allowed",
			},
		);
	});

	nodeTest(`fileReadStream should accept matching extension`, async () => {
		const stream = await fileReadStream({ path: testFile, types: csvTypes });
		await streamToString(stream);
	});

	nodeTest(`fileReadStream should reject non-matching extension`, async () => {
		await rejects(() => fileReadStream({ path: testFile, types: jsonTypes }), {
			message: "Invalid extension",
		});
	});

	nodeTest(
		`fileReadStream should allow any extension when types is empty`,
		async () => {
			const stream = await fileReadStream({ path: testFile, types: [] });
			await streamToString(stream);
		},
	);

	nodeTest(`extension check is case-insensitive`, async () => {
		const upperFile = join(testDir, "UPPER.CSV");
		writeFileSync(upperFile, testContent);
		strictEqual(
			await streamToString(
				await fileReadStream({ path: upperFile, types: csvTypes }),
			),
			testContent,
		);
		const writable = await fileWriteStream({
			path: join(testDir, "out.json"),
			types: [{ accept: { "application/json": [".JSON"] } }],
		});
		strictEqual(isWritable(writable), true);
		writable.destroy();
		await rejects(() => fileReadStream({ path: upperFile, types: jsonTypes }), {
			message: "Invalid extension",
		});
	});

	nodeTest(`fileWriteStream should reject non-matching extension`, async () => {
		await rejects(
			() =>
				fileWriteStream({
					path: join(testDir, "out.csv"),
					types: jsonTypes,
				}),
			{ message: "Invalid extension" },
		);
	});

	nodeTest(
		`fileWriteStream with basePath allows new file (ENOENT ok)`,
		async () => {
			const outFile = join(testDir, "brand-new.csv");
			const stream = await fileWriteStream({
				path: outFile,
				basePath: testDir,
			});
			await new Promise((resolve, reject) => {
				stream.on("finish", resolve);
				stream.on("error", reject);
				stream.write(Buffer.from("new"));
				stream.end();
			});
			strictEqual(readFileSync(outFile, "utf8"), "new");
		},
	);

	nodeTest(
		`fileReadStream throws Path not found for non-ENOENT stat error`,
		async () => {
			const notDir = join(testFile, "inside.csv");
			await rejects(() => fileReadStream({ path: notDir, basePath: testDir }), {
				message: "Path not found",
			});
		},
	);

	nodeTest(`fileReadStream Path not found error carries cause`, async () => {
		const notDir = join(testFile, "inside.csv");
		await rejects(
			() => fileReadStream({ path: notDir, basePath: testDir }),
			(e) => {
				strictEqual(e.message, "Path not found");
				strictEqual(typeof e.cause, "object");
				return true;
			},
		);
	});

	nodeTest(`fileReadStream allows any path without basePath`, async () => {
		const stream = await fileReadStream({ path: testFile });
		await streamToString(stream);
	});

	nodeTest(
		`fileWriteStream with basePath preserves existing file when stream constructor fails`,
		async () => {
			const guardFile = join(testDir, "guard.csv");
			const originalContent = "important,data\n1,2,3\n";
			writeFileSync(guardFile, originalContent);
			await rejects(
				() =>
					fileWriteStream(
						{ path: guardFile, basePath: testDir },
						{ highWaterMark: -1 },
					),
				(e) => typeof e === "object" && e !== null,
			);
			strictEqual(readFileSync(guardFile, "utf8"), originalContent);
		},
	);

	nodeTest(
		`fileReadStream with basePath returns an fd-based stream`,
		async () => {
			const stream = await fileReadStream({
				path: testFile,
				basePath: testDir,
			});
			strictEqual(typeof stream.fd, "number");
			stream.destroy();
		},
	);

	nodeTest(
		`fileWriteStream with basePath returns an fd-based stream`,
		async () => {
			const outFile = join(testDir, "fd-write-check.csv");
			writeFileSync(outFile, "");
			const stream = await fileWriteStream({
				path: outFile,
				basePath: testDir,
			});
			strictEqual(typeof stream.fd, "number");
			stream.destroy();
		},
	);

	nodeTest(
		`fileReadStream and fileWriteStream reject a path through a symlinked directory`,
		async () => {
			const outsideDir = mkdtempSync(
				join(tmpdir(), "datastream-file-outside-"),
			);
			try {
				writeFileSync(join(outsideDir, "secret.csv"), "secret");
				symlinkSync(outsideDir, join(testDir, "dirlink"));
				await rejects(
					() =>
						fileReadStream({
							path: join(testDir, "dirlink", "secret.csv"),
							basePath: testDir,
						}),
					{ message: "Path traversal detected" },
				);
				await rejects(
					() =>
						fileWriteStream({
							path: join(testDir, "dirlink", "new.csv"),
							basePath: testDir,
						}),
					{ message: "Path traversal detected" },
				);
			} finally {
				rmSync(join(testDir, "dirlink"), { force: true });
				rmSync(outsideDir, { recursive: true, force: true });
			}
		},
	);

	nodeTest(
		`fileReadStream and fileWriteStream accept names starting with ".."`,
		async () => {
			const dotFile = join(testDir, "..data.csv");
			mkdirSync(join(testDir, "..cache"), { recursive: true });
			const nestedFile = join(testDir, "..cache", "x.csv");
			for (const outFile of [dotFile, nestedFile]) {
				await pipeline([
					createReadableStream(["dots"]),
					await fileWriteStream({ path: outFile, basePath: testDir }),
				]);
				strictEqual(
					await streamToString(
						await fileReadStream({ path: outFile, basePath: testDir }),
					),
					"dots",
				);
			}
		},
	);

	nodeTest(
		`fileReadStream rejects a symlinked directory pointing at the parent of basePath`,
		async () => {
			const link = join(testDir, "uplink");
			symlinkSync(dirname(testDir), link);
			try {
				await rejects(
					() =>
						fileReadStream({ path: join(link, "x.csv"), basePath: testDir }),
					{ message: "Path traversal detected" },
				);
			} finally {
				rmSync(link, { force: true });
			}
		},
	);

	nodeTest(
		`fileWriteStream with a missing parent directory throws Path not found`,
		async () => {
			await rejects(
				() =>
					fileWriteStream({
						path: join(testDir, "missing", "x.csv"),
						basePath: testDir,
					}),
				(e) => {
					strictEqual(e.message, "Path not found");
					strictEqual(e.cause.code, "ENOENT");
					return true;
				},
			);
		},
	);

	nodeTest(
		`fileWriteStream with basePath replaces a longer existing file`,
		async () => {
			const outFile = join(testDir, "overwrite.csv");
			writeFileSync(outFile, "A".repeat(20));
			await pipeline([
				createReadableStream([Buffer.from("new")]),
				await fileWriteStream({ path: outFile, basePath: testDir }),
			]);
			strictEqual(readFileSync(outFile, "utf8"), "new");
		},
	);

	nodeTest(
		`fileWriteStream accepts string chunks from a pipeline`,
		async () => {
			const outFile = join(testDir, "strings.csv");
			await pipeline([
				createReadableStream(["a,b\n", "1,2\n"]),
				await fileWriteStream({ path: outFile }),
			]);
			strictEqual(readFileSync(outFile, "utf8"), "a,b\n1,2\n");
		},
	);

	nodeTest(
		`fileWriteStream with basePath accepts string chunks from a pipeline`,
		async () => {
			const outFile = join(testDir, "strings-base.csv");
			await pipeline([
				createReadableStream(["x", "y"]),
				await fileWriteStream({ path: outFile, basePath: testDir }),
			]);
			strictEqual(readFileSync(outFile, "utf8"), "xy");
		},
	);

	nodeTest(`fileReadStream reads in byte chunks, not objectMode`, async () => {
		const bigFile = join(testDir, "big.csv");
		writeFileSync(bigFile, "z".repeat(100));
		for (const basePath of [undefined, testDir]) {
			const stream = await fileReadStream({ path: bigFile, basePath });
			strictEqual(stream.readableObjectMode, false);
			const chunks = [];
			for await (const chunk of stream) chunks.push(chunk);
			strictEqual(chunks.length, 1);
			strictEqual(chunks[0].length, 100);
		}
	});

	nodeTest(`fileReadStream honours highWaterMark in bytes`, async () => {
		const bigFile = join(testDir, "hwm.csv");
		writeFileSync(bigFile, "z".repeat(10));
		for (const basePath of [undefined, testDir]) {
			const stream = await fileReadStream(
				{ path: bigFile, basePath },
				{ highWaterMark: 4 },
			);
			const chunks = [];
			for await (const chunk of stream) chunks.push(chunk.length);
			deepStrictEqual(chunks, [4, 4, 2]);
		}
	});

	nodeTest(`fileReadStream and fileWriteStream honour signal`, async () => {
		const outFile = join(testDir, "signal.csv");
		for (const basePath of [undefined, testDir]) {
			for (const make of [fileReadStream, fileWriteStream]) {
				const controller = new AbortController();
				const stream = await make(
					{ path: make === fileReadStream ? testFile : outFile, basePath },
					{ signal: controller.signal },
				);
				const error = once(stream, "error", {
					signal: AbortSignal.timeout(1000),
				});
				const reason = new Error("stop");
				controller.abort(reason);
				const [e] = await error;
				strictEqual(e.name, "AbortError");
				strictEqual(e.cause, reason);
			}
		}
	});

	nodeTest(`fileWriteStream with flags "a" appends`, async () => {
		for (const basePath of [undefined, testDir]) {
			const outFile = join(testDir, `append-${basePath ? "base" : "path"}.csv`);
			writeFileSync(outFile, "old,");
			await pipeline([
				createReadableStream(["new"]),
				await fileWriteStream({ path: outFile, basePath }, { flags: "a" }),
			]);
			strictEqual(readFileSync(outFile, "utf8"), "old,new");
		}
	});

	nodeTest(
		`fileWriteStream with flags "wx" refuses an existing file`,
		async () => {
			const outFile = join(testDir, "exclusive.csv");
			writeFileSync(outFile, "keep");
			await rejects(
				() =>
					fileWriteStream(
						{ path: outFile, basePath: testDir },
						{ flags: "wx" },
					),
				{ code: "EEXIST" },
			);
			strictEqual(readFileSync(outFile, "utf8"), "keep");
		},
	);

	nodeTest(`fileWriteStream with basePath honours mode`, async () => {
		const outFile = join(testDir, "mode.csv");
		await pipeline([
			createReadableStream(["m"]),
			await fileWriteStream(
				{ path: outFile, basePath: testDir },
				{ mode: 0o600 },
			),
		]);
		strictEqual(statSync(outFile).mode & 0o777, 0o600);
	});

	nodeTest(`fileWriteStream strips datastream-only streamOptions`, async () => {
		for (const basePath of [undefined, testDir]) {
			const outFile = join(testDir, `strip-${basePath ? "base" : "path"}.csv`);
			await pipeline([
				createReadableStream(["s", "t"]),
				await fileWriteStream(
					{ path: outFile, basePath },
					{
						objectMode: true,
						readableObjectMode: true,
						writableObjectMode: true,
						chunkSize: 1,
					},
				),
			]);
			strictEqual(readFileSync(outFile, "utf8"), "st");
		}
	});

	nodeTest(`fileWriteStream with basePath honours start`, async () => {
		const outFile = join(testDir, "start.csv");
		writeFileSync(outFile, "abcdef");
		await pipeline([
			createReadableStream(["XY"]),
			await fileWriteStream({ path: outFile, basePath: testDir }, { start: 2 }),
		]);
		strictEqual(readFileSync(outFile, "utf8"), "\0\0XY");
	});

	nodeTest(
		`fileWriteStream with basePath closes the fd when the stream constructor throws`,
		async () => {
			const guardFile = join(testDir, "fd-leak.csv");
			const before = readdirSync("/dev/fd").length;
			await rejects(
				() =>
					fileWriteStream(
						{ path: guardFile, basePath: testDir },
						{ highWaterMark: -1 },
					),
				{ code: "ERR_INVALID_ARG_VALUE" },
			);
			strictEqual(readdirSync("/dev/fd").length, before);
		},
	);

	nodeTest(
		`fileReadStream with basePath closes the fd when the stream constructor throws`,
		async () => {
			const before = readdirSync("/dev/fd").length;
			await rejects(
				() =>
					fileReadStream(
						{ path: testFile, basePath: testDir },
						{ start: "bad" },
					),
				{ code: "ERR_INVALID_ARG_TYPE" },
			);
			strictEqual(readdirSync("/dev/fd").length, before);
		},
	);

	nodeTest(`fileWriteStream with flags "r+" updates in place`, async () => {
		for (const basePath of [undefined, testDir]) {
			const outFile = join(testDir, `update-${basePath ? "base" : "path"}.csv`);
			writeFileSync(outFile, "hello world");
			await pipeline([
				createReadableStream(["J"]),
				await fileWriteStream({ path: outFile, basePath }, { flags: "r+" }),
			]);
			strictEqual(readFileSync(outFile, "utf8"), "Jello world");
		}
	});

	nodeTest(
		`fileWriteStream with basePath and flags "r+" does not create`,
		async () => {
			const outFile = join(testDir, "update-missing.csv");
			await rejects(
				() =>
					fileWriteStream(
						{ path: outFile, basePath: testDir },
						{ flags: "r+" },
					),
				{ code: "ENOENT" },
			);
			strictEqual(readdirSync(testDir).includes("update-missing.csv"), false);
		},
	);

	nodeTest(`fileReadStream honours encoding and start`, async () => {
		for (const basePath of [undefined, testDir]) {
			const stream = await fileReadStream(
				{ path: testFile, basePath },
				{ encoding: "utf8", start: 2 },
			);
			const chunks = [];
			for await (const chunk of stream) chunks.push(chunk);
			deepStrictEqual(chunks, [testContent.slice(2)]);
		}
	});

	nodeTest(`fileReadStream and fileWriteStream return a Promise`, async () => {
		const read = fileReadStream({ path: testFile });
		strictEqual(read instanceof Promise, true);
		strictEqual(await streamToString(await read), testContent);
		const write = fileWriteStream({ path: join(testDir, "promise.csv") });
		strictEqual(write instanceof Promise, true);
		await pipeline([createReadableStream(["x"]), await write]);
		strictEqual(readFileSync(join(testDir, "promise.csv"), "utf8"), "x");
	});

	nodeTest(
		`fileReadStream rejects (not throws) on an invalid path`,
		async () => {
			await rejects(
				fileReadStream({ path: "/etc/passwd", basePath: testDir }),
				{
					message: "Path traversal detected",
				},
			);
		},
	);

	test(`module has only named exports`, async () => {
		const mod = await import("@datastream/file");
		deepStrictEqual(Object.keys(mod).sort(), [
			"fileReadStream",
			"fileWriteStream",
		]);
	});

	if (variant === "browser") {
		const written = [];
		let pickedName = "in.csv";
		let pickerArgs;
		globalThis.window = {
			showOpenFilePicker: async (args) => {
				pickerArgs = args;
				return [{ getFile: async () => new File([testContent], pickedName) }];
			},
			showSaveFilePicker: async ({ suggestedName }) => ({
				name: suggestedName,
				createWritable: async () =>
					new WritableStream({
						write(chunk) {
							written.push(chunk);
						},
					}),
			}),
		};

		test(`fileReadStream streams the picked file`, async () => {
			const stream = await fileReadStream({ types: csvTypes });
			strictEqual(await streamToString(stream), testContent);
			deepStrictEqual(pickerArgs, { types: csvTypes });
		});

		test(`fileReadStream accepts any extension without types`, async () => {
			strictEqual(await streamToString(await fileReadStream({})), testContent);
		});

		test(`fileReadStream rejects a non-matching extension`, async () => {
			await rejects(fileReadStream({ types: jsonTypes }), /Invalid extension/);
		});

		test(`fileWriteStream writes to the picked handle`, async () => {
			const stream = await fileWriteStream({
				path: "out.csv",
				types: csvTypes,
			});
			await pipeline([createReadableStream(["a", "b"]), stream]);
			deepStrictEqual(written, ["a", "b"]);
		});

		test(`fileWriteStream rejects a non-matching or missing extension`, async () => {
			await rejects(
				fileWriteStream({ path: "out.txt", types: csvTypes }),
				/Invalid extension/,
			);
			await rejects(
				fileWriteStream({ path: "noext", types: csvTypes }),
				/Invalid extension/,
			);
		});

		test(`fileReadStream accepts a dotfile whose whole name is the extension`, async () => {
			pickedName = ".csv";
			try {
				strictEqual(
					await streamToString(await fileReadStream({ types: csvTypes })),
					testContent,
				);
			} finally {
				pickedName = "in.csv";
			}
		});

		test(`extension check is case-insensitive`, async () => {
			pickedName = "IN.CSV";
			try {
				strictEqual(
					await streamToString(await fileReadStream({ types: csvTypes })),
					testContent,
				);
			} finally {
				pickedName = "in.csv";
			}
			strictEqual(
				isWritable(
					await fileWriteStream({
						path: "out.json",
						types: [{ accept: { "application/json": [".JSON"] } }],
					}),
				),
				true,
			);
			await rejects(
				fileWriteStream({ path: "OUT.TXT", types: csvTypes }),
				/Invalid extension/,
			);
		});

		test(`fileWriteStream accepts an extensionless path when types allow ""`, async () => {
			const types = [{ accept: { "text/plain": [""] } }];
			strictEqual(
				isWritable(await fileWriteStream({ path: "noext", types })),
				true,
			);
		});
	}
});
