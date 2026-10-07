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

describe(`@datastream/ipfs (${variant})`, () => {
	// Node stream internals (write/destroy/emit) drive these teardown tests.
	const nodeTest = variant === "node" ? test : test.skip;

	// *** ipfsGetStream *** //
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
		// node.get can hand back whatever the client uses (here a bare async
		// generator). ipfsGetStream must normalize it to a platform stream so
		// downstream pipeline behavior is consistent across environments.
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
		// The dominant real-world client shape: node.get is async and resolves to
		// the content stream. ipfsGetStream must await it before normalizing.
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

	// *** ipfsAddStream *** //
	test(`ipfsAddStream should pipe data through and call node.add`, async () => {
		const input = ["chunk1", "chunk2"];
		let receivedChunks;
		const node = {
			async add(source) {
				// node.add receives an async-iterable source (streamed), not a
				// buffered array.
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

	// Kill: ConditionalExpression survivor `if (error) reject(error)` → `if (true)` in
	// the resolvePulled callback. When a parked chunk is pulled with no error, the
	// producer's transform callback must RESOLVE so the Transform pushes the chunk
	// downstream. With the mutant it rejects with `undefined`, which the core
	// Transform treats as a (falsy) non-error and therefore skips `this.push(chunk)`
	// — so the passed-through output is dropped. Asserting the full passthrough
	// output catches that.
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
		// The add transform is a pass-through: every input chunk must reach
		// downstream in order, not be dropped.
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
		// The source must be a streamed async iterable, never a materialized Array.
		strictEqual(Array.isArray(source), false);
		strictEqual(typeof source[Symbol.asyncIterator], "function");
		strictEqual(streams[1].result().value, "QmStreamed");
	});

	test(`ipfsAddStream should propagate node.add errors through the pipeline`, async () => {
		const node = {
			async add(source) {
				// Consume one chunk then fail, simulating an IPFS client error.
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
		// The cid is never set when the add rejects.
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
			// node.add iterates the source forever until it ends/throws. When the
			// transform is destroyed (e.g. aborted), the source generator must throw or
			// return so node.add settles instead of hanging on a parked promise.
			let iterationSettled = false;
			let iterationError;
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
							// keep consuming
						}
					} catch (e) {
						iterationError = e;
					} finally {
						iterationSettled = true;
					}
					return { cid: "QmDestroyed" };
				},
			};
			const stream = await ipfsAddStream({ node });
			// Write one chunk so node.add is actively iterating, then destroy.
			stream.write("a");
			await new Promise((resolve) => setTimeout(resolve, 10));
			stream.destroy(new Error("aborted"));

			// Wait up to 1s for node.add's source iteration to settle.
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
			// The generator should have thrown the destroy error into node.add.
			strictEqual(iterationError instanceof Error, true);
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
							// keep consuming
						}
					} finally {
						iterationSettled = true;
					}
					return { cid: "QmUpstreamError" };
				},
			};
			// Upstream readable that emits one chunk then errors.
			const upstream = createReadableStream(undefined);
			upstream.push("a");
			const addStream = await ipfsAddStream({ node });
			const run = pipeline([upstream, addStream]);
			await new Promise((resolve) => setTimeout(resolve, 10));
			upstream.destroy(new Error("upstream boom"));
			await rejects(() => run, /boom/);

			// node.add's source iteration must settle, not hang forever.
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
			// node.add consumes slowly. The transform must apply real backpressure so
			// the producer can't race ahead and buffer the whole input in memory.
			const total = 50;
			let accepted = 0; // chunks the transform has taken from upstream
			let consumed = 0; // chunks node.add has pulled from the source
			let maxInFlight = 0; // max (accepted - consumed): unconsumed queue depth

			const node = {
				async add(source) {
					for await (const _chunk of source) {
						consumed += 1;
						// Slow consumer.
						await new Promise((resolve) => setTimeout(resolve, 5));
					}
					return { cid: "QmBounded" };
				},
			};

			const input = createReadableStream(undefined);
			// Observe how many chunks the transform accepts before they are consumed.
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
			// With real backpressure the unconsumed in-flight depth stays small and
			// bounded; without it, the producer races ahead and buffers ~all `total`.
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
		// A legacy / differently-shaped client may resolve without a { cid }. The
		// pipeline should reject with an actionable message, not an opaque
		// "Cannot read properties of undefined (reading 'cid')".
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

	// *** ipfsAddStream result validation guards *** //

	// Kill: ConditionalExpression survivors on line 49 (`result == null` → false, and
	// `typeof result !== "object"` → false). A non-null primitive result must still throw
	// because the `typeof` guard must remain active.
	test(`ipfsAddStream should throw a clear error when node.add returns a non-object (string)`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				// A string is non-null AND non-object; strips the null-check branch but
				// the typeof branch should fire.
				return "QmNotAnObject";
			},
		};
		const streams = [
			createReadableStream(["a"]),
			await ipfsAddStream({ node }),
		];
		await rejects(() => pipeline(streams), /node\.add did not return/);
	});

	// Kill: ConditionalExpression survivor where `typeof result !== "object"` → false.
	// An object that lacks a "cid" property must still throw, exercising the
	// `!("cid" in result)` branch independently.
	test(`ipfsAddStream should throw a clear error when node.add returns an object without cid`, async () => {
		const node = {
			async add(source) {
				for await (const _chunk of source) {
				}
				// Non-null object, passes typeof check, but has no "cid" key.
				return { hash: "QmSomethingElse" };
			},
		};
		const streams = [
			createReadableStream(["a"]),
			await ipfsAddStream({ node }),
		];
		await rejects(() => pipeline(streams), /node\.add did not return/);
	});

	// Kill: LogicalOperator survivor where first `||` → `&&`.
	// A boolean (non-null, non-object) must trigger the error via the typeof branch.
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

	// Kill: ConditionalExpression survivor `result == null` → `false` (line 47).
	// null is the one value where `result == null` is true but `typeof null` is
	// "object", so with `result == null` replaced by `false`, the code would fall
	// through to `!("cid" in null)` which throws a TypeError (not our message).
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

	// *** teardown / error path tests *** //

	// Kill: AssignmentOperator survivor `error ??= e` → `error &&= e` (line 55).
	// When error is already recorded and add() rejects, the original error must be preserved.
	test(`ipfsAddStream should preserve the first error when add rejects after a prior error`, async () => {
		const node = {
			async add(_source) {
				// Reject immediately without consuming any chunks.
				throw new Error("add error");
			},
		};
		const addStream = await ipfsAddStream({ node });
		const streams = [createReadableStream(["a"]), addStream];
		await rejects(() => pipeline(streams), /add error/);
		strictEqual(addStream.result().value, undefined);
	});

	// Kill: BooleanLiteral survivor `consumerDone = true` → `consumerDone = false` (line 56).
	// When add() resolves successfully before all chunks have been pushed (early resolve),
	// consumerDone must be set to true so subsequent parked writes resolve immediately
	// instead of hanging forever waiting for a consumer that no longer exists.
	test(`ipfsAddStream should not block writes after node.add resolves early (consumerDone)`, async () => {
		let addSettled = false;
		const node = {
			async add(source) {
				// Consume just one chunk then return — simulating a client that resolves
				// without draining the full source.
				for await (const _chunk of source) {
					break;
				}
				addSettled = true;
				return { cid: "QmEarlyResolve" };
			},
		};
		const source = createReadableStream();
		const run = pipeline([source, await ipfsAddStream({ node })]);

		// Push a chunk to kick off add()'s consumption of the source, then wait
		// for add() to settle (it consumed one chunk and returned { cid }).
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

		// Another chunk must pass straight through (consumerDone) instead of
		// parking forever, so the pipeline completes.
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

	// Kill: ConditionalExpression survivor `if (error)` → `if (false)` in transform callback
	// (line 68), and the BlockStatement survivor where the reject+return block is removed.
	// When error is already set before a chunk is written, transform() must reject
	// immediately (not park the chunk or silently resolve).
	nodeTest(
		`ipfsAddStream should reject immediately when error is set before writing`,
		async () => {
			const node = {
				async add(_source) {
					// Reject immediately without consuming any chunks.
					throw new Error("add error before write");
				},
			};
			const addStream = await ipfsAddStream({ node });
			// Let the add() rejection propagate and set `error`.
			await new Promise((resolve) => setTimeout(resolve, 30));

			// Now write a chunk; since `error` is set, transform() must reject, not hang.
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
			// The write should have produced an error callback (not silently resolved).
			strictEqual(
				typeof writeResult === "object" && writeResult.error instanceof Error,
				true,
				"write should reject with an error when error is pre-set",
			);
			addStream.destroy();
		},
	);

	// Kill: ConditionalExpression survivors on line 79 `if (error) reject(error)` →
	// `if (true)` or `if (false)`. The resolvePulled callback runs when source() pulls
	// a chunk. If an error was injected between parking and pulling, the producer
	// (transform callback) must reject, not resolve.
	nodeTest(
		`ipfsAddStream should reject the pending write when error is set while a chunk is parked`,
		async () => {
			let resolveReady;
			const ready = new Promise((r) => {
				resolveReady = r;
			});
			const node = {
				async add(source) {
					// Signal that we are ready to start consuming, then pause before pulling.
					resolveReady();
					await new Promise((resolve) => setTimeout(resolve, 50)); // park
					// Now drain so source() gets woken and the error branch is exercised.
					for await (const _chunk of source) {
					}
					return { cid: "QmRace" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			await ready; // add() has started

			// Park a chunk (filled=true, resolvePulled set), then inject an error before
			// source() wakes up to pull it.
			const writePromise = new Promise((resolve, reject) => {
				addStream.write("parked-chunk", (err) =>
					err ? reject(err) : resolve("ok"),
				);
			});
			// Short wait so the chunk is parked (filled=true, resolvePulled is set).
			await new Promise((resolve) => setTimeout(resolve, 10));
			// Inject error via destroy, which calls teardown → sets error → calls onPulled.
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

	// Kill: ConditionalExpression survivor `if (done) return` → `if (false) return` (line 93).
	// teardown() after done=true must be a no-op: error must stay undefined when called
	// with no argument after a successful flush.
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
			// Complete the pipeline successfully; this sets done=true then awaits addPromise.
			const res = await pipeline(streams);
			strictEqual(res.cid, "QmDoneFirst");
			// Emitting "close" again after a clean run must not throw or change the result.
			addStream.emit("close");
			// result must still be valid
			strictEqual(addStream.result().value, "QmDoneFirst");
		},
	);

	// Kill: `if (done) return` → `if (false) return` in teardown. Once flush has
	// ended the source (done), a destroy while node.add is still finishing must
	// not inject an error: node.add's next pull sees a clean end of source.
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
					await iterator.next(); // pull "a" so its write completes
					await gate; // hold node.add open while the stream flushes
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
			addStream.end(); // flush: done = true, then awaits node.add
			await new Promise((resolve) => setImmediate(resolve));
			addStream.destroy();
			// Let the destroy's "close" (which runs teardown) fire before node.add pulls.
			await new Promise((resolve) => setImmediate(resolve));
			release();
			deepStrictEqual(await outcome, { value: undefined, done: true });
		},
	);

	// Kill: LogicalOperator survivor `error ??= e ?? new Error(...)` → `e && new Error(...)`
	// (line 94). When teardown is called without an argument (e is undefined), the
	// fallback `new Error("ipfsAddStream destroyed")` must be used, not `undefined`.
	nodeTest(
		`ipfsAddStream teardown without argument should set a fallback error`,
		async () => {
			let sourceError;
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
						}
					} catch (e) {
						sourceError = e;
					}
					return { cid: "QmFallback" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			addStream.write("a");
			await new Promise((resolve) => setTimeout(resolve, 10));
			// Destroy with no error argument → teardown(undefined) → fallback Error needed.
			addStream.destroy();
			await new Promise((resolve) => setTimeout(resolve, 50));
			// source() must have thrown a real Error, not undefined.
			strictEqual(
				sourceError instanceof Error,
				true,
				"source should throw a real Error even when teardown called without argument",
			);
		},
	);

	// Kill: StringLiteral survivor `new Error("ipfsAddStream destroyed")` → `new Error("")`
	// (line 94). The fallback error must carry the descriptive message.
	nodeTest(
		`ipfsAddStream teardown fallback error message should describe the cause`,
		async () => {
			let sourceError;
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
						}
					} catch (e) {
						sourceError = e;
					}
					return { cid: "QmMsg" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			addStream.write("a");
			await new Promise((resolve) => setTimeout(resolve, 10));
			addStream.destroy(); // no error arg → fallback message used
			await new Promise((resolve) => setTimeout(resolve, 50));
			strictEqual(sourceError instanceof Error, true);
			strictEqual(
				sourceError.message.includes("ipfsAddStream destroyed"),
				true,
				`expected "ipfsAddStream destroyed" in message, got: "${sourceError?.message}"`,
			);
		},
	);

	// Kill: StringLiteral survivor `stream.on("close", ...)` → `stream.on("", ...)`
	// (line 99), and the ArrowFunction survivor `() => teardown()` → `() => undefined`.
	// Destroying a stream without an error fires "close" but not "error"; if the "close"
	// handler is missing or is a no-op, teardown never runs and source() hangs forever.
	nodeTest(
		`ipfsAddStream should settle node.add source when stream is closed without error`,
		async () => {
			let iterationSettled = false;
			let iterationError;
			const node = {
				async add(source) {
					try {
						for await (const _chunk of source) {
						}
					} catch (e) {
						iterationError = e;
					} finally {
						iterationSettled = true;
					}
					return { cid: "QmClose" };
				},
			};
			const addStream = await ipfsAddStream({ node });
			addStream.write("a");
			await new Promise((resolve) => setTimeout(resolve, 10));
			// Destroy without passing an error so only "close" fires (no "error" emitted).
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
			// teardown ran (stream was destroyed before completion), so source()
			// threw the fallback teardown error into node.add.
			strictEqual(iterationError instanceof Error, true);
			strictEqual(iterationError.message, "ipfsAddStream destroyed");
		},
	);

	// *** named exports only *** //
	test(`module has only named exports`, async () => {
		const mod = await import("@datastream/ipfs");
		deepStrictEqual(Object.keys(mod).sort(), [
			"ipfsAddStream",
			"ipfsGetStream",
		]);
	});
});
