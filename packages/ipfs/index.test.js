// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { deepStrictEqual, rejects, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import {
	createReadableStream,
	isReadable,
	pipejoin,
	pipeline,
	streamToArray,
} from "@datastream/core";
import { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";
import { variant } from "../variant.js";
import { makeIpfsAddStream } from "./shared.js";

describe(`@datastream/ipfs (${variant})`, () => {
	const nodeTest = variant === "node" ? test : test.skip;

	test(`ipfsGetStream should return readable stream from node.get`, async () => {
		const chunks = ["chunk1", "chunk2", "chunk3"];
		const node = {
			get(_cid) {
				return createReadableStream(chunks);
			},
		};
		const stream = await ipfsGetStream({ node, cid: "QmTest123" });
		const output = await streamToArray(stream);
		deepStrictEqual(output, chunks);
	});

	test(`ipfsGetStream should pass cid to node.get`, async () => {
		let receivedCid;
		const node = {
			get(cid) {
				receivedCid = cid;
				return createReadableStream(["data"]);
			},
		};
		await ipfsGetStream({ node, cid: "bafyTestCidV1" });
		strictEqual(receivedCid, "bafyTestCidV1");
	});

	test(`ipfsGetStream should work in a pipeline`, async () => {
		const chunks = ["hello", "world"];
		const node = {
			get(_cid) {
				return createReadableStream(chunks);
			},
		};
		const streams = [await ipfsGetStream({ node, cid: "QmTest123" })];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, chunks);
	});

	test(`ipfsGetStream should normalize a raw async iterable into a platform stream`, async () => {
		const node = {
			get(_cid) {
				return (async function* () {
					yield "x";
					yield "y";
				})();
			},
		};
		const stream = await ipfsGetStream({ node, cid: "QmIter" });
		strictEqual(isReadable(stream), true);
		const output = await streamToArray(stream);
		deepStrictEqual(output, ["x", "y"]);
	});

	test(`ipfsGetStream should await an async node.get (Promise<asyncIterable>)`, async () => {
		const node = {
			get(_cid) {
				return Promise.resolve(
					(async function* () {
						yield "x";
						yield "y";
					})(),
				);
			},
		};
		const stream = await ipfsGetStream({ node, cid: "QmAsyncGet" });
		strictEqual(isReadable(stream), true);
		const output = await streamToArray(stream);
		deepStrictEqual(output, ["x", "y"]);
	});

	test(`ipfsAddStream should pipe data through and call node.add`, async () => {
		const input = ["chunk1", "chunk2"];
		let receivedChunks;
		const node = {
			async add(source) {
				receivedChunks = [];
				for await (const chunk of source) {
					receivedChunks.push(chunk);
				}
				return { cid: "QmResult123" };
			},
		};
		const streams = [
			createReadableStream(input),
			await ipfsAddStream({ node }),
		];
		await pipeline(streams);
		const { key, value } = streams[1].result();
		strictEqual(key, "cid");
		strictEqual(value, "QmResult123");
		deepStrictEqual(receivedChunks, input);
	});

	test(`ipfsAddStream should pass every chunk through to downstream`, async () => {
		const input = ["chunk1", "chunk2", "chunk3"];
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				return { cid: "QmPassthrough" };
			},
		};
		const streams = [
			createReadableStream(input),
			await ipfsAddStream({ node }),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, input);
		deepStrictEqual(streams[1].result().value, "QmPassthrough");
	});

	test(`ipfsAddStream should feed node.add an async iterable, not a buffered array`, async () => {
		let source;
		const node = {
			async add(s) {
				source = s;
				const chunks = [];
				for await (const chunk of s) {
					chunks.push(chunk);
				}
				return { cid: "QmStreamed" };
			},
		};
		const streams = [
			createReadableStream(["a", "b", "c"]),
			await ipfsAddStream({ node }),
		];
		await pipeline(streams);
		strictEqual(Array.isArray(source), false);
		strictEqual(typeof source[Symbol.asyncIterator], "function");
		strictEqual(streams[1].result().value, "QmStreamed");
	});

	test(`ipfsAddStream should propagate node.add errors through the pipeline`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
					throw new Error("add failed");
				}
			},
		};
		const streams = [
			createReadableStream(["a", "b"]),
			await ipfsAddStream({ node }),
		];
		await rejects(() => pipeline(streams), /add failed/);
		strictEqual(streams[1].result().value, undefined);
	});

	test(`ipfsAddStream should use custom resultKey`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				return { cid: "QmResult123" };
			},
		};
		const streams = [
			createReadableStream(["data"]),
			await ipfsAddStream({ node, resultKey: "ipfsCid" }),
		];
		await pipeline(streams);
		const { key } = streams[1].result();
		strictEqual(key, "ipfsCid");
	});

	test(`ipfsAddStream result should be collected by pipeline`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				return { cid: "bafyResult456" };
			},
		};
		const streams = [
			createReadableStream(["data"]),
			await ipfsAddStream({ node }),
		];
		const result = await pipeline(streams);
		strictEqual(result.cid, "bafyResult456");
	});

	nodeTest(
		`ipfsAddStream should terminate node.add source when the stream is destroyed`,
		async () => {
			let iterationSettled = false;
			const caught = {};
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
						}
					} catch (e) {
						caught.error = e;
					} finally {
						iterationSettled = true;
					}
					return { cid: "QmDestroyed" };
				},
			};
			const stream = await ipfsAddStream({ node });
			stream.write("a");
			await new Promise((resolve) => setTimeout(resolve, 10));
			stream.destroy(new Error("aborted"));

			const settled = await Promise.race([
				(async () => {
					while (!iterationSettled) {
						await new Promise((resolve) => setTimeout(resolve, 10));
					}
					return true;
				})(),
				new Promise((resolve) => setTimeout(() => resolve(false), 1000)),
			]);
			strictEqual(settled, true);
			strictEqual(caught.error instanceof Error, true);
		},
	);

	nodeTest(
		`ipfsAddStream should terminate node.add source when the pipeline errors upstream`,
		async () => {
			let iterationSettled = false;
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
						}
					} finally {
						iterationSettled = true;
					}
					return { cid: "QmUpstreamError" };
				},
			};
			const upstream = createReadableStream(undefined);
			upstream.push("a");
			const addStream = await ipfsAddStream({ node });
			const run = pipeline([upstream, addStream]);
			await new Promise((resolve) => setTimeout(resolve, 10));
			upstream.destroy(new Error("upstream boom"));
			await rejects(() => run, /boom/);

			const settled = await Promise.race([
				(async () => {
					while (!iterationSettled) {
						await new Promise((resolve) => setTimeout(resolve, 10));
					}
					return true;
				})(),
				new Promise((resolve) => setTimeout(() => resolve(false), 1000)),
			]);
			strictEqual(settled, true);
		},
	);

	nodeTest(
		`ipfsAddStream should bound queue depth under a slow consumer (backpressure)`,
		async () => {
			const total = 50;
			let accepted = 0;
			let consumed = 0;
			let maxInFlight = 0;

			const node = {
				async add(source) {
					for await (const _chunk of source) {
						consumed += 1;
						await new Promise((resolve) => setTimeout(resolve, 5));
					}
					return { cid: "QmBounded" };
				},
			};

			const input = createReadableStream(undefined);
			const observer = await ipfsAddStream({ node }, { highWaterMark: 2 });
			const originalWrite = observer.write.bind(observer);
			observer.write = (chunk, ...rest) => {
				accepted += 1;
				maxInFlight = Math.max(maxInFlight, accepted - consumed);
				return originalWrite(chunk, ...rest);
			};

			const run = pipeline([input, observer]);
			for (let i = 0; i < total; i++) {
				input.push(`chunk-${i}`);
			}
			input.push(null);
			const result = await run;
			strictEqual(result.cid, "QmBounded");
			strictEqual(consumed, total);
			strictEqual(
				maxInFlight < total,
				true,
				`queue depth ${maxInFlight} should be bounded well below ${total}`,
			);
			strictEqual(
				maxInFlight <= 10,
				true,
				`queue depth ${maxInFlight} should stay small under backpressure`,
			);
		},
	);

	test(`ipfsAddStream should throw a clear error when node.add omits cid`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				return undefined;
			},
		};
		const streams = [
			createReadableStream(["a"]),
			await ipfsAddStream({ node }),
		];
		await rejects(() => pipeline(streams), /node\.add did not return/);
	});

	test(`ipfsAddStream should terminate node.add source on abort signal`, async () => {
		let iterationSettled = false;
		const node = {
			async add(source) {
				try {
					for await (const _chunk of source) {
					}
				} finally {
					iterationSettled = true;
				}
				return { cid: "QmAborted" };
			},
		};
		const controller = new AbortController();
		const input = createReadableStream(undefined, {
			signal: controller.signal,
		});
		const addStream = await ipfsAddStream(
			{ node },
			{ signal: controller.signal },
		);
		const run = pipeline([input, addStream], { signal: controller.signal });
		input.push("a");
		await new Promise((resolve) => setTimeout(resolve, 10));
		controller.abort();
		await rejects(() => run);

		const settled = await Promise.race([
			(async () => {
				while (!iterationSettled) {
					await new Promise((resolve) => setTimeout(resolve, 10));
				}
				return true;
			})(),
			new Promise((resolve) => setTimeout(() => resolve(false), 1000)),
		]);
		strictEqual(settled, true);
	});

	test(`ipfsAddStream should throw a clear error when node.add returns a non-object (string)`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				return "QmNotAnObject";
			},
		};
		const streams = [
			createReadableStream(["a"]),
			await ipfsAddStream({ node }),
		];
		await rejects(() => pipeline(streams), /node\.add did not return/);
	});

	test(`ipfsAddStream should throw a clear error when node.add returns an object without cid`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				return { hash: "QmSomethingElse" };
			},
		};
		const streams = [
			createReadableStream(["a"]),
			await ipfsAddStream({ node }),
		];
		await rejects(() => pipeline(streams), /node\.add did not return/);
	});

	test(`ipfsAddStream should throw a clear error when node.add returns a boolean`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				return true;
			},
		};
		const streams = [
			createReadableStream(["a"]),
			await ipfsAddStream({ node }),
		];
		await rejects(() => pipeline(streams), /node\.add did not return/);
	});

	test(`ipfsAddStream should throw a clear error when node.add returns null`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				return null;
			},
		};
		const streams = [
			createReadableStream(["a"]),
			await ipfsAddStream({ node }),
		];
		await rejects(() => pipeline(streams), /node\.add did not return/);
	});

	test(`ipfsAddStream should preserve the first error when add rejects after a prior error`, async () => {
		const node = {
			async add(_source) {
				throw new Error("add error");
			},
		};
		const addStream = await ipfsAddStream({ node });
		const streams = [createReadableStream(["a"]), addStream];
		await rejects(() => pipeline(streams), /add error/);
		strictEqual(addStream.result().value, undefined);
	});

	test(`ipfsAddStream should not block writes after node.add resolves early (consumerDone)`, async () => {
		let addSettled = false;
		const node = {
			async add(source) {
				for await (const _chunk of source) {
					break;
				}
				addSettled = true;
				return { cid: "QmEarlyResolve" };
			},
		};
		const source = createReadableStream();
		const run = pipeline([source, await ipfsAddStream({ node })]);

		source.push("first");
		const settled = await Promise.race([
			(async () => {
				while (!addSettled) {
					await new Promise((resolve) => setTimeout(resolve, 10));
				}
				return true;
			})(),
			new Promise((resolve) => setTimeout(() => resolve(false), 1000)),
		]);
		strictEqual(settled, true, "add() should settle quickly after first chunk");

		source.push("second");
		source.push(null);
		const result = await Promise.race([
			run,
			new Promise((_resolve, reject) =>
				setTimeout(
					() => reject(new Error("chunk after early resolve hung")),
					1000,
				),
			),
		]);
		strictEqual(result.cid, "QmEarlyResolve");
	});

	nodeTest(
		`ipfsAddStream should reject immediately when error is set before writing`,
		async () => {
			const node = {
				async add(_source) {
					throw new Error("add error before write");
				},
			};
			const addStream = await ipfsAddStream({ node });
			await new Promise((resolve) => setTimeout(resolve, 30));

			const writeResult = await Promise.race([
				new Promise((resolve, reject) => {
					addStream.write("chunk", (err) =>
						err ? reject(err) : resolve("ok"),
					);
				}).catch((e) => ({ error: e })),
				new Promise((resolve) => setTimeout(() => resolve("timeout"), 500)),
			]);
			strictEqual(
				writeResult !== "timeout",
				true,
				"write must not hang when error is already set",
			);
			strictEqual(
				typeof writeResult === "object" && writeResult.error instanceof Error,
				true,
				"write should reject with an error when error is pre-set",
			);
			addStream.destroy();
		},
	);

	nodeTest(
		`ipfsAddStream should reject the pending write when error is set while a chunk is parked`,
		async () => {
			let resolveReady;
			const ready = new Promise((r) => {
				resolveReady = r;
			});
			const node = {
				async add(source) {
					resolveReady();
					await new Promise((resolve) => setTimeout(resolve, 50));
					for await (const _chunk of source) {
					}
					return { cid: "QmRace" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			await ready;

			const writePromise = new Promise((resolve, reject) => {
				addStream.write("parked-chunk", (err) =>
					err ? reject(err) : resolve("ok"),
				);
			});
			await new Promise((resolve) => setTimeout(resolve, 10));
			addStream.destroy(new Error("injected error"));

			const result = await Promise.race([
				writePromise.catch((e) => ({ error: e })),
				new Promise((resolve) => setTimeout(() => resolve("timeout"), 500)),
			]);
			strictEqual(result !== "timeout", true, "write should not hang");
			strictEqual(
				typeof result === "object" && result.error instanceof Error,
				true,
				"write should reject when error is set while chunk is parked",
			);
		},
	);

	nodeTest(
		`ipfsAddStream teardown after done should not set error`,
		async () => {
			const node = {
				async add(source) {
					for await (const _chunk of source) {
					}
					return { cid: "QmDoneFirst" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			const streams = [createReadableStream(["a"]), addStream];
			const res = await pipeline(streams);
			strictEqual(res.cid, "QmDoneFirst");
			addStream.emit("close");
			strictEqual(addStream.result().value, "QmDoneFirst");
		},
	);

	nodeTest(
		`ipfsAddStream destroy after flush lets node.add finish the source cleanly`,
		async () => {
			let release;
			const gate = new Promise((resolve) => {
				release = resolve;
			});
			let finish;
			const outcome = new Promise((resolve) => {
				finish = resolve;
			});
			const node = {
				async add(source) {
					const iterator = source[Symbol.asyncIterator]();
					await iterator.next();
					await gate;
					try {
						finish(await iterator.next());
					} catch (e) {
						finish(e);
					}
					return { cid: "QmLate" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			await new Promise((resolve) => addStream.write("a", resolve));
			addStream.end();
			await new Promise((resolve) => setImmediate(resolve));
			addStream.destroy();
			await new Promise((resolve) => setImmediate(resolve));
			release();
			deepStrictEqual(await outcome, { value: undefined, done: true });
		},
	);

	nodeTest(
		`ipfsAddStream teardown without argument should set a fallback error`,
		async () => {
			const caught = {};
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
						}
					} catch (e) {
						caught.error = e;
					}
					return { cid: "QmFallback" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			addStream.write("a");
			await new Promise((resolve) => setTimeout(resolve, 10));
			addStream.destroy();
			await new Promise((resolve) => setTimeout(resolve, 50));
			strictEqual(
				caught.error instanceof Error,
				true,
				"source should throw a real Error even when teardown called without argument",
			);
		},
	);

	nodeTest(
		`ipfsAddStream teardown fallback error message should describe the cause`,
		async () => {
			const caught = {};
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
						}
					} catch (e) {
						caught.error = e;
					}
					return { cid: "QmMsg" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			addStream.write("a");
			await new Promise((resolve) => setTimeout(resolve, 10));
			addStream.destroy();
			await new Promise((resolve) => setTimeout(resolve, 50));
			strictEqual(caught.error instanceof Error, true);
			strictEqual(
				caught.error.message.includes("ipfsAddStream destroyed"),
				true,
				`expected "ipfsAddStream destroyed" in message, got: "${caught.error?.message}"`,
			);
		},
	);

	nodeTest(
		`ipfsAddStream should settle node.add source when stream is closed without error`,
		async () => {
			let iterationSettled = false;
			const caught = {};
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
						}
					} catch (e) {
						caught.error = e;
					} finally {
						iterationSettled = true;
					}
					return { cid: "QmClose" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			addStream.write("a");
			await new Promise((resolve) => setTimeout(resolve, 10));
			addStream.destroy();

			const settled = await Promise.race([
				(async () => {
					while (!iterationSettled) {
						await new Promise((resolve) => setTimeout(resolve, 10));
					}
					return true;
				})(),
				new Promise((resolve) => setTimeout(() => resolve(false), 1000)),
			]);
			strictEqual(
				settled,
				true,
				"node.add source must settle when stream is closed without error (close event)",
			);
			strictEqual(caught.error instanceof Error, true);
			strictEqual(caught.error.message, "ipfsAddStream destroyed");
		},
	);

	test(`ipfsGetStream passes streamOptions.signal to node.get`, async () => {
		const controller = new AbortController();
		const calls = [];
		const node = {
			get(...args) {
				calls.push(args);
				return createReadableStream(["data"]);
			},
		};
		await ipfsGetStream({ node, cid: "QmSig" }, { signal: controller.signal });
		await ipfsGetStream({ node, cid: "QmNoSig" });
		deepStrictEqual(calls, [
			["QmSig", { signal: controller.signal }],
			["QmNoSig", { signal: undefined }],
		]);
	});

	test(`ipfsAddStream passes streamOptions.signal to node.add`, async () => {
		const controller = new AbortController();
		const options = [];
		const node = {
			async add(source, addOptions) {
				options.push(addOptions);
				for await (const _chunk of source) {
				}
				return { cid: "QmSig" };
			},
		};
		await pipeline([
			createReadableStream(["a"]),
			await ipfsAddStream({ node }, { signal: controller.signal }),
		]);
		await pipeline([
			createReadableStream(["a"]),
			await ipfsAddStream({ node }),
		]);
		deepStrictEqual(options, [
			{ signal: controller.signal },
			{ signal: undefined },
		]);
	});

	if (variant === "browser") {
		test(`ipfsAddStream removes its abort listener once node.add settles`, async (t) => {
			for (const fail of [false, true]) {
				const controller = new AbortController();
				const { signal } = controller;
				const added = [];
				const removed = [];
				t.mock.method(signal, "addEventListener", (...args) =>
					added.push(args),
				);
				t.mock.method(signal, "removeEventListener", (...args) =>
					removed.push(args),
				);
				const node = {
					async add(source) {
						for await (const _chunk of source) {
						}
						if (fail) throw new Error("add failed");
						return { cid: "QmListener" };
					},
				};
				const addStream = await ipfsAddStream({ node }, { signal });
				const ours = added.filter((args) => args.length === 3);
				strictEqual(ours.length, 1);
				deepStrictEqual(ours[0][0], "abort");
				deepStrictEqual(ours[0][2], { once: true });
				const listener = ours[0][1];
				const removedOurs = () =>
					removed.filter(([, handler]) => handler === listener);
				deepStrictEqual(removedOurs(), []);
				const run = pipeline([createReadableStream(["a"]), addStream]);
				if (fail) await rejects(run, { message: "add failed" });
				else await run;
				deepStrictEqual(removedOurs(), [["abort", listener]]);
			}
		});

		test(`ipfsAddStream aborted with a null reason fails node.add with the fallback error`, async () => {
			const controller = new AbortController();
			let pulled;
			const started = new Promise((resolve) => {
				pulled = resolve;
			});
			let outcome;
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
							pulled();
						}
					} catch (e) {
						outcome = e;
					}
					return { cid: "QmNull" };
				},
			};
			const addStream = await ipfsAddStream(
				{ node },
				{ signal: controller.signal },
			);
			addStream.readable
				.getReader()
				.read()
				.catch(() => {});
			addStream.writable
				.getWriter()
				.write("a")
				.catch(() => {});
			await started;
			controller.abort(null);
			await new Promise((resolve) => setTimeout(resolve, 10));
			strictEqual(outcome.message, "ipfsAddStream destroyed");
		});

		test(`ipfsAddStream abort after flush lets node.add finish the source cleanly`, async () => {
			const controller = new AbortController();
			let release;
			const gate = new Promise((resolve) => {
				release = resolve;
			});
			let flushed;
			const holding = new Promise((resolve) => {
				flushed = resolve;
			});
			let outcome;
			const node = {
				async add(source) {
					const iterator = source[Symbol.asyncIterator]();
					await iterator.next();
					flushed();
					await gate;
					outcome = await iterator.next();
					return { cid: "QmLate" };
				},
			};
			const run = pipeline([
				createReadableStream(["a"]),
				await ipfsAddStream({ node }, { signal: controller.signal }),
			]);
			await holding;
			await new Promise((resolve) => setTimeout(resolve, 10));
			controller.abort();
			release();
			deepStrictEqual(await run, { cid: "QmLate" });
			deepStrictEqual(outcome, { done: true, value: undefined });
		});
	}

	const within = async (promise, label) => {
		let timer;
		try {
			return await Promise.race([
				promise,
				new Promise((_resolve, reject) => {
					timer = setTimeout(() => reject(new Error(`${label} hung`)), 500);
				}),
			]);
		} finally {
			clearTimeout(timer);
		}
	};

	test(`ipfsAddStream releases a parked chunk when node.add resolves without pulling it`, async () => {
		const node = {
			async add(_source) {
				await new Promise((resolve) => setTimeout(resolve, 20));
				return { cid: "QmUnpulled" };
			},
		};
		const result = await within(
			pipeline([
				createReadableStream(["a", "b"]),
				await ipfsAddStream({ node }),
			]),
			"pipeline",
		);
		strictEqual(result.cid, "QmUnpulled");
	});

	test(`ipfsAddStream ends a source that is waiting when the input ends`, async () => {
		const node = {
			async add(source) {
				const chunks = [];
				for await (const chunk of source) {
					chunks.push(chunk);
				}
				return { cid: chunks.join(",") };
			},
		};
		const input = createReadableStream();
		const run = pipeline([input, await ipfsAddStream({ node })]);
		input.push("a");
		await new Promise((resolve) => setTimeout(resolve, 20));
		input.push(null);
		strictEqual((await within(run, "pipeline")).cid, "a");
	});

	nodeTest(
		`ipfsAddStream destroy rejects a parked write while node.add is stalled`,
		async () => {
			let release;
			const gate = new Promise((resolve) => {
				release = resolve;
			});
			const node = {
				async add(_source) {
					await gate;
					return { cid: "QmStalled" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			addStream.on("error", () => {});
			const written = new Promise((resolve) => addStream.write("a", resolve));
			await new Promise((resolve) => setTimeout(resolve, 10));
			addStream.destroy(new Error("stalled"));
			const error = await within(written, "parked write");
			strictEqual(error.message, "stalled");
			release();
		},
	);

	nodeTest(`makeIpfsAddStream unwires once node.add settles`, async () => {
		for (const fail of [false, true]) {
			const calls = [];
			let release;
			const gate = new Promise((resolve) => {
				release = resolve;
			});
			const node = {
				async add(source) {
					for await (const _chunk of source) {
					}
					await gate;
					if (fail) throw new Error("add failed");
					return { cid: "QmUnwire" };
				},
			};
			const streamOptions = {};
			const addStream = await makeIpfsAddStream((stream, teardown, options) => {
				calls.push(["wire", stream, typeof teardown, options]);
				return (...args) => calls.push(["unwire", args.length]);
			})({ node }, streamOptions);
			deepStrictEqual(calls, [["wire", addStream, "function", streamOptions]]);
			const run = pipeline([createReadableStream(["a"]), addStream]);
			await new Promise((resolve) => setTimeout(resolve, 10));
			strictEqual(calls.length, 1);
			release();
			if (fail) await rejects(run, { message: "add failed" });
			else await run;
			deepStrictEqual(calls.slice(1), [["unwire", 1]]);
		}
	});

	nodeTest(
		`makeIpfsAddStream does not leak node.add's rejection when wiring throws`,
		async () => {
			const unhandled = [];
			const onUnhandled = (reason) => unhandled.push(reason);
			process.on("unhandledRejection", onUnhandled);
			try {
				const node = {
					async add(_source) {
						throw new Error("add failed");
					},
				};
				await rejects(
					makeIpfsAddStream(() => {
						throw new Error("wire failed");
					})({ node }),
					{ message: "wire failed" },
				);
				await new Promise((resolve) => setTimeout(resolve, 10));
				deepStrictEqual(unhandled, []);
			} finally {
				process.off("unhandledRejection", onUnhandled);
			}
		},
	);

	test(`module has only named exports`, async () => {
		const mod = await import("@datastream/ipfs");
		deepStrictEqual(Object.keys(mod).sort(), [
			"ipfsAddStream",
			"ipfsGetStream",
		]);
	});
});
