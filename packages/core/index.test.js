import { deepStrictEqual, ok, rejects, strictEqual, throws } from "node:assert";
import { EventEmitter, getEventListeners } from "node:events";
import { Readable } from "node:stream";
import test, { describe, mock } from "node:test";
import {
	concatBytes,
	createChunkDecoder,
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
	createWritableStream,
	isReadable,
	isWritable,
	makeOptions,
	pipejoin,
	pipeline,
	resolveLazy,
	result,
	streamToArray,
	streamToBuffer,
	streamToObject,
	streamToString,
	timeout,
} from "@datastream/core";
import { objectCountStream } from "@datastream/object";
import { variant } from "../variant.js";

const spy = (impl) => {
	const fn = mock.fn(impl);
	Object.defineProperty(fn, "callCount", {
		get() {
			return fn.mock.callCount();
		},
	});
	return fn;
};

describe(`@datastream/core (${variant})`, async () => {
	// Node-only behaviour (Readable internals, Buffer collectors, NULL_SENTINEL).
	const nodeTest = variant === "node" ? test : test.skip;
	// Node-only export (the browser build has no backpressureGauge at all), so it
	// can't be a static named import shared by both builds.
	const { backpressureGauge } = await import("@datastream/core");
	// streamToBuffer yields a Buffer (node) or a Uint8Array (browser).
	const text = (bytes) => new TextDecoder().decode(bytes);

	// *** streamTo{Array,String,Object} *** //
	const types = {
		boolean: [true, false],
		integer: [-1, 0, 1],
		decimal: [-1.1, 0.0, 1.1],
		strings: ["a", "b", "c"],
		buffer: ["a", "b", "c"].map((i) => Buffer.from(i)),
		date: [new Date(), new Date()],
		array: [
			["a", "b"],
			["1", "2"],
		],
		object: [{ a: "1" }, { a: "2" }, { a: "3" }],
	};
	for (const type of Object.keys(types)) {
		test(`streamToArray should work with readable ${type} stream`, async (_t) => {
			const input = types[type];
			const streams = [createReadableStream(input)];
			const stream = pipejoin(streams);
			const output = await streamToArray(stream);

			deepStrictEqual(output, input);
		});

		test(`streamToArray should work with transform ${type} stream`, async (_t) => {
			const input = types[type];
			const streams = [createReadableStream(input), createTransformStream()];
			const stream = pipejoin(streams);
			const output = await streamToArray(stream);

			deepStrictEqual(output, input);
		});

		test(`streamToObject should work with transform ${type} stream`, async (_t) => {
			const input = types[type];
			const streams = [
				createReadableStream(input),
				createTransformStream((chunk, enqueue) => {
					enqueue({ [type]: chunk });
				}),
			];
			const stream = pipejoin(streams);
			const output = await streamToObject(stream);

			deepStrictEqual(output, { [type]: input[input.length - 1] });
		});

		test(`streamToString should work with readable ${type} stream`, async (_t) => {
			const input = types[type];
			const streams = [createReadableStream(input)];
			const stream = pipejoin(streams);
			const output = await streamToString(stream);

			deepStrictEqual(output, input.join(""));
		});

		test(`streamToString should work with transform ${type} stream`, async (_t) => {
			const input = types[type];
			const streams = [createReadableStream(input), createTransformStream()];
			const stream = pipejoin(streams);
			const output = await streamToString(stream);

			deepStrictEqual(output, input.join(""));
		});
	}

	// *** streamTo{Array,String,Object} with async iterable (non-Node stream) *** //
	test(`streamToArray should work with async iterable`, async (_t) => {
		async function* gen() {
			yield "a";
			yield "b";
		}
		const output = await streamToArray(gen());
		deepStrictEqual(output, ["a", "b"]);
	});

	test(`streamToObject should work with async iterable`, async (_t) => {
		async function* gen() {
			yield { x: 1 };
			yield { y: 2 };
		}
		const output = await streamToObject(gen());
		deepStrictEqual(output, { x: 1, y: 2 });
	});

	test(`streamToString should work with async iterable`, async (_t) => {
		async function* gen() {
			yield "hello";
			yield " world";
		}
		const output = await streamToString(gen());
		strictEqual(output, "hello world");
	});

	test(`streamToBuffer should work with async iterable`, async (_t) => {
		async function* gen() {
			yield "hello";
			yield " world";
		}
		const output = await streamToBuffer(gen());
		strictEqual(text(output), "hello world");
	});

	// *** streamToBuffer *** //
	test(`streamToBuffer should collect buffers into single buffer`, async (_t) => {
		const input = ["hello", " ", "world"];
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToBuffer(stream);

		deepStrictEqual(text(output), "hello world");
	});

	test(`streamToBuffer should work with Uint8Array`, async (_t) => {
		const input = [Uint8Array.from([104, 101, 108, 108, 111])]; // "hello"
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToBuffer(stream);

		deepStrictEqual(text(output), "hello");
	});

	// Raw byte window of any view/ArrayBuffer: Buffer.from(view) used to drop a
	// DataView and truncate Uint16 elements; the browser build used to
	// stringify them ("[object DataView]").
	test(`streamToBuffer keeps the raw bytes of any binary chunk`, async (_t) => {
		const hi = Uint8Array.from([0, 104, 105, 0]).buffer;
		const input = [
			new DataView(hi, 1, 2),
			new Uint16Array([0x6968]),
			Uint8Array.from([104, 105]).buffer,
		];
		const output = await streamToBuffer(createReadableStream(input));

		deepStrictEqual(text(output), "hihihi");
		deepStrictEqual(
			text(
				await streamToBuffer(
					(async function* () {
						yield* input;
					})(),
				),
			),
			"hihihi",
		);
	});

	// A throwing 'data' listener used to escape as an uncaught exception and
	// resolve with partial data.
	nodeTest(
		`streamToBuffer rejects on a chunk Buffer.from cannot convert`,
		async (_t) => {
			await rejects(streamToBuffer(createReadableStream([1, 2])), {
				code: "ERR_INVALID_ARG_TYPE",
			});
		},
	);

	// Direct source import so the node run still pins browser-build parity
	// (streamToBuffer was once missing from the browser build entirely).
	test(`web build exports a working streamToBuffer`, async (_t) => {
		const web = await import(
			`file://${new URL("./index.browser.js", import.meta.url).pathname}`
		);
		strictEqual(typeof web.streamToBuffer, "function");
		const out = await web.streamToBuffer(
			web.createReadableStream(["hello", " ", "world"]),
		);
		ok(out instanceof Uint8Array);
		strictEqual(new TextDecoder().decode(out), "hello world");
	});

	// *** backpressureGauge *** //
	nodeTest(`backpressureGauge should measure stream metrics`, async (_t) => {
		const input = ["a", "b", "c"];
		const streams = {
			readable: createReadableStream(input),
			transform: createTransformStream(),
		};

		const metrics = backpressureGauge(streams);

		deepStrictEqual(typeof metrics, "object");
		deepStrictEqual(typeof metrics.readable, "object");
		deepStrictEqual(typeof metrics.transform, "object");
		deepStrictEqual(metrics.readable.timeline, []);
		deepStrictEqual(metrics.readable.total, {});
	});

	nodeTest(
		`backpressureGauge should track pause and resume events`,
		async (_t) => {
			const transform = createTransformStream();
			const streams = { transform };

			const metrics = backpressureGauge(streams);

			// Simulate pause event
			transform.emit("pause");
			// Simulate resume event (with timestamp set)
			transform.emit("resume");

			// Check that timeline was updated
			deepStrictEqual(metrics.transform.timeline.length, 1);
			strictEqual(typeof metrics.transform.timeline[0].timestamp, "number");
			strictEqual(typeof metrics.transform.timeline[0].duration, "number");
		},
	);

	nodeTest(
		`backpressureGauge should track resume without prior pause`,
		async (_t) => {
			const transform = createTransformStream();
			const streams = { transform };

			const metrics = backpressureGauge(streams);

			// Simulate resume event without prior pause
			transform.emit("resume");

			// startTimestamp should be set
			strictEqual(typeof metrics.transform.total.timestamp, "undefined");

			// Simulate end event
			transform.emit("end");

			// total should now have values
			strictEqual(typeof metrics.transform.total.timestamp, "number");
			strictEqual(typeof metrics.transform.total.duration, "number");
		},
	);

	nodeTest(
		`backpressureGauge records one interval per pause/resume pair`,
		async (_t) => {
			const transform = createTransformStream();
			const metrics = backpressureGauge({ transform });

			// One real pause/resume pair, then a stray resume with no intervening pause.
			transform.emit("pause");
			transform.emit("resume");
			transform.emit("resume");

			// The stray resume must not record a phantom interval from the stale pause.
			strictEqual(metrics.transform.timeline.length, 1);
		},
	);

	// *** createReadableStream *** //
	test(`createReadableStream should create a readable stream from string`, async (_t) => {
		const input = "abc";
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		strictEqual(isReadable(streams[0]), true);
		strictEqual(isWritable(streams[0]), false);
		deepStrictEqual(output, input);
	});

	test(`createReadableStream should chunk long strings`, async (_t) => {
		const input = "x".repeat(17 * 1024); // where 16*1024 is the default chunkSize/highWaterMark
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output.length, 2);
	});

	test(`createReadableStream should create a readable stream from array`, async (_t) => {
		const input = ["a", "b", "c"];
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(isReadable(streams[0]), true);
		strictEqual(isWritable(streams[0]), false);
		deepStrictEqual(output, input);
	});

	test(`createReadableStream should create a readable stream from iterable`, async (_t) => {
		function* input() {
			yield "a";
			yield "b";
			yield "c";
		}
		const streams = [createReadableStream(input())];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(isReadable(streams[0]), true);
		strictEqual(isWritable(streams[0]), false);
		deepStrictEqual(output, ["a", "b", "c"]);
	});

	test(`createReadableStream should create a readable stream from ArrayBuffer`, async (_t) => {
		const input = new Uint8Array([1, 2, 3, 4, 5]).buffer;
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(output.length, 1);
		deepStrictEqual(Array.from(output[0]), [1, 2, 3, 4, 5]);
	});

	test(`createReadableStream should allow pushing values onto it`, async (_t) => {
		const streams = [createReadableStream()];
		const stream = pipejoin(streams);
		streams[0].push("a");
		streams[0].push(null);
		const output = await streamToArray(stream);

		deepStrictEqual(output, ["a"]);
	});

	test(`createReadableStream push mode errors when streamOptions.signal aborts`, async (_t) => {
		const controller = new AbortController();
		const source = createReadableStream(undefined, {
			signal: controller.signal,
		});
		const outputPromise = streamToArray(source);
		const reason = new Error("stop");
		controller.abort(reason);
		await rejects(outputPromise, (e) => {
			if (variant === "node") {
				strictEqual(e.name, "AbortError");
				strictEqual(e.cause, reason);
			} else {
				strictEqual(e, reason);
			}
			return true;
		});
	});

	test(`createReadableStream push mode errors when streamOptions.signal is already aborted`, async (_t) => {
		const source = createReadableStream(undefined, {
			signal: AbortSignal.abort(),
		});
		await rejects(streamToArray(source), { name: "AbortError" });
	});

	test(`createReadableStream read() is invoked when consumer requests data before push`, async (_t) => {
		// Start consuming BEFORE pushing so the Readable's _read() hook is triggered
		// when the stream is in flowing mode but the buffer is empty.
		const source = createReadableStream();
		const outputPromise = streamToArray(source);
		// Push data asynchronously so _read() fires first.
		process.nextTick(() => {
			source.push("x");
			source.push(null);
		});
		const output = await outputPromise;
		deepStrictEqual(output, ["x"]);
	});

	if (variant === "node") {
		const { backpressureGauge } = await import("@datastream/core");
		test(`backpressureGauge should chunk really long strings`, async (_t) => {
			const input = "x".repeat(1024 * 1024); // where 16*1024 is the default chunkSize/highWaterMark
			const streams = [
				createReadableStream(input),
				createPassThroughStream(async () => {
					await timeout(5);
				}),
				createWritableStream(),
			];
			const metrics = backpressureGauge(streams);

			await pipeline(streams);
			// console.log(JSON.stringify(metrics))

			deepStrictEqual(metrics["0"].timeline.length, 3);
			deepStrictEqual(metrics["1"].timeline.length, 0);
		});
	}

	// *** createPassThroughStream *** //
	test(`createPassThroughStream should create a pass through stream`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const streams = [
			createReadableStream(input),
			createPassThroughStream(transform, {}),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(isReadable(streams[1]), true);
		strictEqual(isWritable(streams[1]), true);
		strictEqual(transform.callCount, 3);
		deepStrictEqual(output, input);
	});

	test(`createPassThroughStream should create a pass through stream with flush`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const flush = spy();
		const streams = [
			createReadableStream(input),
			createPassThroughStream(transform, flush, {}),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(isReadable(streams[1]), true);
		strictEqual(isWritable(streams[1]), true);
		strictEqual(transform.callCount, 3);
		strictEqual(flush.callCount, 1);
		deepStrictEqual(output, input);
	});

	test(`createPassThroughStream should catch transform error`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = () => {
			throw new Error("error");
		};
		const streams = [
			createReadableStream(input),
			createPassThroughStream(transform),
		];
		await rejects(pipeline(streams), { message: "error" });
	});

	test(`createPassThroughStream should handle async passThrough`, async (_t) => {
		const input = ["a", "b", "c"];
		const streams = [
			createReadableStream(input),
			createPassThroughStream(async () => {}),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, input);
	});

	test(`createPassThroughStream should handle async flush`, async (_t) => {
		const input = ["a", "b", "c"];
		const streams = [
			createReadableStream(input),
			createPassThroughStream(
				() => {},
				async () => {},
			),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, input);
	});

	test(`createPassThroughStream should catch flush error`, async (_t) => {
		const input = ["a", "b", "c"];
		const flush = () => {
			throw new Error("error");
		};
		const streams = [
			createReadableStream(input),
			createPassThroughStream(() => {}, flush),
		];
		await rejects(pipeline(streams), { message: "error" });
	});

	// *** createTransformStream *** //
	test(`createTransformStream should create a transform stream`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const streams = [
			createReadableStream(input),
			createTransformStream(transform, {}),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(isReadable(streams[1]), true);
		strictEqual(isWritable(streams[1]), true);
		strictEqual(transform.callCount, 3);
		deepStrictEqual(output, []);
	});

	test(`createTransformStream should create a transform stream with flush`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const flush = spy();
		const streams = [
			createReadableStream(input),
			createTransformStream(transform, flush, {}),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		strictEqual(isReadable(streams[1]), true);
		strictEqual(isWritable(streams[1]), true);
		strictEqual(transform.callCount, 3);
		strictEqual(flush.callCount, 1);
		deepStrictEqual(output, []);
	});

	test(`createTransformStream should catch transform error`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = () => {
			throw new Error("error");
		};
		const streams = [
			createReadableStream(input),
			createTransformStream(transform),
		];
		await rejects(pipeline(streams), { message: "error" });
	});

	test(`createTransformStream should handle async transform`, async (_t) => {
		const input = ["a", "b", "c"];
		const streams = [
			createReadableStream(input),
			createTransformStream(async (chunk, enqueue) => {
				enqueue(chunk);
			}),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, input);
	});

	test(`createTransformStream should handle async flush`, async (_t) => {
		const input = ["a", "b", "c"];
		const streams = [
			createReadableStream(input),
			createTransformStream(
				(chunk, enqueue) => enqueue(chunk),
				async () => {},
			),
		];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, input);
	});

	test(`createTransformStream should catch flush error`, async (_t) => {
		const input = ["a", "b", "c"];
		const flush = () => {
			throw new Error("error");
		};
		const streams = [
			createReadableStream(input),
			createTransformStream(() => {}, flush),
		];
		await rejects(pipeline(streams), { message: "error" });
	});

	// *** createWritableStream *** //
	test(`createWritableStream should create a writable stream`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const streams = [
			createReadableStream(input),
			createWritableStream(transform, {}),
		];

		strictEqual(isReadable(streams[1]), false);
		strictEqual(isWritable(streams[1]), true);

		const result = await pipeline(streams);

		strictEqual(transform.callCount, 3);
		deepStrictEqual(result, {});
	});

	test(`createWritableStream should create a writable stream with final`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const final = spy();
		const streams = [
			createReadableStream(input),
			createWritableStream(transform, final, {}),
		];

		strictEqual(isReadable(streams[1]), false);
		strictEqual(isWritable(streams[1]), true);

		const result = await pipeline(streams);

		strictEqual(transform.callCount, 3);
		strictEqual(final.callCount, 1);
		deepStrictEqual(result, {});
	});

	test(`createWritableStream should catch transform error`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = () => {
			throw new Error("error");
		};
		const streams = [
			createReadableStream(input),
			createWritableStream(transform),
		];
		await rejects(pipeline(streams), { message: "error" });
	});

	test(`createWritableStream should handle async write`, async (_t) => {
		const input = ["a", "b", "c"];
		const collected = [];
		const streams = [
			createReadableStream(input),
			createWritableStream(async (chunk) => {
				collected.push(chunk);
			}),
		];
		await pipeline(streams);

		deepStrictEqual(collected, input);
	});

	test(`createWritableStream should handle async final`, async (_t) => {
		const input = ["a", "b", "c"];
		let finalized = false;
		const streams = [
			createReadableStream(input),
			createWritableStream(
				() => {},
				async () => {
					finalized = true;
				},
			),
		];
		await pipeline(streams);

		strictEqual(finalized, true);
	});

	test(`createWritableStream should catch final error`, async (_t) => {
		const input = ["a", "b", "c"];
		const final = () => {
			throw new Error("error");
		};
		const streams = [
			createReadableStream(input),
			createWritableStream(() => {}, final),
		];
		await rejects(pipeline(streams), { message: "error" });
	});

	// *** createBranchStream *** //
	/*if (variant === "node") {
		test(`createBranchStream should create a branch stream`, async (_t) => {
			const input = ["a", "b", "c"];
			const transform = spy();

			const stream = createWritableStream(transform);
			stream.result = () => ({ key: "a", value: 1 });

			const streams = [
				createReadableStream(input),
				createBranchStream({ streams: [stream] }),
				createWritableStream(transform),
			];

			strictEqual(isReadable(streams[1]), true);
			strictEqual(isWritable(streams[1]), true);

			const result = await pipeline(streams);

			deepStrictEqual(result, { branch: { a: 1 } });
			strictEqual(transform.callCount, 6);
		});
	}*/

	// *** pipeline *** //
	test(`pipeline should add writable to end of streams array`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const streams = [
			createReadableStream(input),
			objectCountStream(),
			createTransformStream(transform),
		];
		const result = await pipeline(streams);

		strictEqual(isReadable(streams[1]), true);
		strictEqual(isWritable(streams[1]), true);
		strictEqual(transform.callCount, 3);
		deepStrictEqual(result, { objectCount: 3 });
	});

	test(`pipeline should throw error when promise passed in`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const streams = [
			createReadableStream(input),
			Promise.resolve(objectCountStream()),
			createTransformStream(transform),
		];
		await rejects(pipeline(streams), {
			message: "Promise instead of stream passed in at index 1",
		});
	});

	test(`pipeline should throw error when a stream thrown an error`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = (chunk, enqueue) => {
			if (chunk === "b") throw new Error("Error");
			enqueue(chunk);
		};
		const streams = [
			createReadableStream(input),
			createTransformStream(transform),
		];
		await rejects(pipeline(streams), { message: "Error" });
	});

	// The index-0 check is easy to miss with reduce() (no initial value skips
	// the callback for the first element), e.g. an un-awaited async source.
	test(`pipeline should throw error when promise passed in at index 0`, async (_t) => {
		const streams = [
			Promise.resolve(createReadableStream(["a"])),
			createTransformStream(spy()),
		];
		await rejects(pipeline(streams), {
			message: "Promise instead of stream passed in at index 0",
		});
		throws(() => pipejoin(streams), {
			message: "Promise instead of stream passed in at index 0",
		});
	});

	// An already-aborted signal never fires "abort" again, so it must be checked
	// up front (the node build errors immediately too).
	test(`createTransformStream and createPassThroughStream error on an already-aborted signal`, async (_t) => {
		const signal = AbortSignal.abort();
		await rejects(
			streamToArray(
				pipejoin([
					createReadableStream(["a"]),
					createTransformStream(undefined, { signal }),
				]),
			),
			{ name: "AbortError" },
		);
		await rejects(
			streamToArray(
				pipejoin([
					createReadableStream(["a"]),
					createPassThroughStream(() => {}, { signal }),
				]),
			),
			{ name: "AbortError" },
		);
	});

	// A source that throws errors the stream without close()/cancel(); its abort
	// listener must still be removed from a shared signal.
	test(`createReadableStream removes its abort listener when the source throws`, async (_t) => {
		const controller = new AbortController();
		const source = (async function* () {
			yield 1;
			throw new Error("boom");
		})();
		await rejects(
			streamToArray(
				createReadableStream(source, { signal: controller.signal }),
			),
			{ message: "boom" },
		);
		await new Promise((resolve) => setImmediate(resolve));
		strictEqual(getEventListeners(controller.signal, "abort").length, 0);
	});

	// *** pipejoin *** //
	test(`pipejoin should throw error when promise passed in`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const streams = [
			createReadableStream(input),
			Promise.resolve(objectCountStream()),
			createTransformStream(transform),
		];
		// Validation is synchronous: the Promise is rejected before any piping.
		throws(() => pipejoin(streams), {
			message: "Promise instead of stream passed in at index 1",
		});
	});

	// *** timeout *** //
	test(`timeout should resolve after delay`, async (t) => {
		// Mock timers: a wall-clock `elapsed >= 9` check was flaky under load.
		t.mock.timers.enable({ apis: ["setTimeout"] });
		let resolved = false;
		const pending = timeout(10).then(() => {
			resolved = true;
		});
		t.mock.timers.tick(9);
		await Promise.resolve();
		strictEqual(resolved, false);
		t.mock.timers.tick(1);
		// Flush microtasks instead of awaiting `pending`, so a too-late timer
		// fails the assertion rather than hanging the test.
		await Promise.resolve();
		strictEqual(resolved, true);
		await pending;
	});

	// *** result *** //
	test(`result should collect stream results`, async (_t) => {
		const stream1 = createPassThroughStream(() => {});
		stream1.result = () => ({ key: "a", value: 1 });
		const stream2 = createPassThroughStream(() => {});
		const output = await result([stream1, stream2]);
		deepStrictEqual(output, { a: 1 });
	});

	test(`result should skip streams with falsy key`, async (_t) => {
		const stream1 = createPassThroughStream(() => {});
		stream1.result = () => ({ key: undefined, value: 1 });
		const output = await result([stream1]);
		deepStrictEqual(output, {});
	});

	// *** makeOptions *** //
	if (variant === "node") {
		test(`makeOptions should return interoperable structure`, async (_t) => {
			const options = makeOptions({
				highWaterMark: 1,
				chunkSize: 2,
			});
			deepStrictEqual(options, {
				chunkSize: 2,
				highWaterMark: 1,
				writableHighWaterMark: 1,
				writableObjectMode: true,
				objectMode: true,
				readableObjectMode: true,
				readableHighWaterMark: 1,
				signal: undefined,
			});
		});
	}

	// *** timeout abort cleanup regression *** //
	test(`timeout should clear timer when aborted`, async (_t) => {
		const controller = new AbortController();
		const promise = timeout(60_000, { signal: controller.signal });
		controller.abort();
		try {
			await promise;
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "Aborted");
			deepStrictEqual(e.cause, { code: "AbortError" });
		}
	});

	test(`timeout should reject immediately if signal already aborted`, async (_t) => {
		const controller = new AbortController();
		controller.abort();
		try {
			await timeout(60_000, { signal: controller.signal });
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "Aborted");
		}
	});

	// *** maxBufferSize *** //
	test(`streamToArray should throw when exceeding maxBufferSize`, async (_t) => {
		const stream = createReadableStream(["aaa", "bbb", "ccc"]);
		try {
			await streamToArray(stream, { maxBufferSize: 6 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToArray should not throw when within maxBufferSize`, async (_t) => {
		const stream = createReadableStream(["aaa", "bbb"]);
		const result = await streamToArray(stream, { maxBufferSize: 6 });
		deepStrictEqual(result, ["aaa", "bbb"]);
	});

	test(`streamToString should throw when exceeding maxBufferSize`, async (_t) => {
		const stream = createReadableStream(["aaa", "bbb", "ccc"]);
		try {
			await streamToString(stream, { maxBufferSize: 6 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToObject should throw when exceeding maxBufferSize`, async (_t) => {
		const stream = createReadableStream([
			{ a: 1 },
			{ b: 2 },
			{ c: 3 },
			{ d: 4 },
		]);
		try {
			await streamToObject(stream, { maxBufferSize: 2 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToBuffer should throw when exceeding maxBufferSize`, async (_t) => {
		const stream = createReadableStream([
			Buffer.from("aaa"),
			Buffer.from("bbb"),
			Buffer.from("ccc"),
		]);
		try {
			await streamToBuffer(stream, { maxBufferSize: 6 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	// *** createReadableStream queue limit regression *** //
	test(`createReadableStream should throw when queue exceeds limit`, async (_t) => {
		const stream = createReadableStream(undefined, { highWaterMark: 3 });
		stream.push("a");
		stream.push("b");
		stream.push("c");
		try {
			stream.push("d");
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("exceeds limit"));
		}
	});

	// *** shared helpers *** //
	test(`createChunkDecoder reassembles a multi-byte char split across chunks`, () => {
		const { decode, flush } = createChunkDecoder();
		const bytes = new TextEncoder().encode("aé€b");
		strictEqual(decode(bytes.subarray(0, 2)), "a");
		strictEqual(decode(bytes.subarray(2, 4)), "é");
		strictEqual(decode(bytes.subarray(4, 5)), "");
		strictEqual(decode(bytes.subarray(5)), "€b");
		strictEqual(flush(), "");
	});

	test(`createChunkDecoder flush emits an incomplete sequence as U+FFFD`, () => {
		const { decode, flush } = createChunkDecoder();
		strictEqual(decode(new Uint8Array([0x61, 0xe2, 0x82])), "a");
		strictEqual(flush(), "\ufffd");
		// flush resets the decoder for reuse.
		strictEqual(flush(), "");
	});

	test(`createChunkDecoder passes strings through and handles empty chunks`, () => {
		const { decode, flush } = createChunkDecoder();
		strictEqual(decode(new Uint8Array([0xc3])), "");
		strictEqual(decode(new Uint8Array(0)), "");
		strictEqual(decode(""), "");
		// A string chunk is returned untouched, even with a split byte pending.
		strictEqual(decode("\ufeffx"), "\ufeffx");
		strictEqual(decode(new Uint8Array([0xa9])), "é");
		strictEqual(flush(), "");
	});

	test(`createChunkDecoder strips a leading BOM by default and keeps it with ignoreBOM`, () => {
		const bom = new Uint8Array([0xef, 0xbb, 0xbf, 0x61]);
		strictEqual(createChunkDecoder().decode(bom), "a");
		strictEqual(createChunkDecoder({ ignoreBOM: true }).decode(bom), "\ufeffa");
	});

	test(`concatBytes of no arrays is empty`, () => {
		deepStrictEqual(concatBytes([]), new Uint8Array(0));
	});

	test(`concatBytes of one array copies it`, () => {
		const a = new Uint8Array([1, 2, 3]);
		const out = concatBytes([a]);
		deepStrictEqual(out, new Uint8Array([1, 2, 3]));
		ok(out !== a);
		ok(out.buffer !== a.buffer);
	});

	test(`concatBytes of many arrays honours subarray views`, () => {
		const backing = new Uint8Array([9, 1, 2, 9, 9, 3, 9]);
		const out = concatBytes([
			backing.subarray(1, 3),
			new Uint8Array(0),
			backing.subarray(5, 6),
			new Uint8Array([4, 5]),
		]);
		deepStrictEqual(out, new Uint8Array([1, 2, 3, 4, 5]));
		strictEqual(out.byteOffset, 0);
		strictEqual(out.buffer.byteLength, 5);
	});

	test(`resolveLazy passes through values and calls thunks`, () => {
		strictEqual(resolveLazy(5), 5);
		strictEqual(resolveLazy("x"), "x");
		strictEqual(
			resolveLazy(() => 42),
			42,
		);
	});

	// *** Node NULL_SENTINEL collector regression *** //
	// The documented way to flow a null through a node object-mode stream is
	// enqueue(null), which createTransformStream wraps as NULL_SENTINEL. Every
	// collector must unwrap it via fromSafe, not just streamToArray.
	test(`streamToArray unwraps NULL_SENTINEL from object stream`, async (_t) => {
		const streams = [
			createReadableStream(["a"]),
			createTransformStream((_chunk, enqueue) => enqueue(null)),
		];
		const output = await streamToArray(pipejoin(streams));
		deepStrictEqual(output, [null]);
	});

	test(`streamToString unwraps NULL_SENTINEL from object stream`, async (_t) => {
		const streams = [
			createReadableStream(["a"]),
			createTransformStream((_chunk, enqueue) => enqueue(null)),
		];
		const output = await streamToString(pipejoin(streams));
		// fromSafe(null) -> null; "".join semantics turn null into an empty string.
		strictEqual(output, "");
	});

	test(`streamToObject unwraps NULL_SENTINEL from object stream`, async (_t) => {
		const streams = [
			createReadableStream(["a"]),
			createTransformStream((_chunk, enqueue) => {
				enqueue({ x: 1 });
				enqueue(null);
			}),
		];
		// Object.assign(value, null) is a no-op, so the null chunk must not throw.
		const output = await streamToObject(pipejoin(streams));
		deepStrictEqual(output, { x: 1 });
	});

	test(`streamToBuffer unwraps NULL_SENTINEL from object stream`, async (_t) => {
		const streams = [
			createReadableStream(["a"]),
			createTransformStream((_chunk, enqueue) => {
				enqueue("hi");
				enqueue(null);
			}),
		];
		// Buffer.from(null) throws; fromSafe(null) must yield an empty buffer.
		const output = await streamToBuffer(pipejoin(streams));
		strictEqual(text(output), "hi");
	});

	// *** Node backpressureGauge writable total regression *** //
	if (variant === "node") {
		test(`backpressureGauge records total for writable sinks`, async (_t) => {
			const writable = createWritableStream();
			const streams = {
				readable: createReadableStream(["a", "b", "c"]),
				writable,
			};
			const metrics = backpressureGauge(streams);
			await pipeline(Object.values(streams));
			// Writable streams emit 'finish'/'close', not 'end', so total must be
			// recorded from those lifecycle events too.
			strictEqual(typeof metrics.writable.total.timestamp, "number");
			strictEqual(typeof metrics.writable.total.duration, "number");
		});
	}

	// *** Node streamToObject __proto__ contract regression *** //
	test(`streamToObject does not expose own __proto__ data key`, async (_t) => {
		// JSON.parse produces an own __proto__ key; the accumulator must not surface
		// it as an own enumerable property on the returned object.
		const evil = JSON.parse('{"__proto__":{"polluted":true},"safe":1}');
		const streams = [
			createReadableStream(["a"]),
			createTransformStream((_chunk, enqueue) => enqueue(evil)),
		];
		const output = await streamToObject(pipejoin(streams));
		strictEqual(Object.hasOwn(output, "__proto__"), false);
		strictEqual(output.safe, 1);
		// And no global prototype pollution occurred.
		strictEqual({}.polluted, undefined);
	});

	// *** Web build direct-import regressions *** //
	// The bare `@datastream/core` import always resolves to the node build, so the
	// Under the browser run `@datastream/core` is the built browser bundle; the
	// node run imports the source directly so these paths are pinned there too.
	const loadWeb = () =>
		variant === "browser"
			? import("@datastream/core")
			: import(
					`file://${new URL("./index.browser.js", import.meta.url).pathname}`
				);

	test(`web streamToString decodes byte chunks instead of comma-joining`, async (_t) => {
		const web = await loadWeb();
		async function* gen() {
			yield new TextEncoder().encode("hi");
			yield new TextEncoder().encode("!");
		}
		const output = await web.streamToString(gen());
		strictEqual(output, "hi!");
	});

	test(`web streamToString decodes multibyte split across chunks`, async (_t) => {
		const web = await loadWeb();
		const bytes = new TextEncoder().encode("é"); // 2 bytes: 0xC3 0xA9
		async function* gen() {
			yield bytes.subarray(0, 1);
			yield bytes.subarray(1);
		}
		const output = await web.streamToString(gen());
		strictEqual(output, "é");
	});

	test(`web streamToString still joins string chunks`, async (_t) => {
		const web = await loadWeb();
		async function* gen() {
			yield "hello";
			yield " world";
		}
		const output = await web.streamToString(gen());
		strictEqual(output, "hello world");
	});

	// A bare ArrayBuffer is not an ArrayBuffer view; without the
	// `instanceof ArrayBuffer` arm it would be String()-ified to
	// "[object ArrayBuffer]" instead of decoded.
	test(`streamToString decodes bare ArrayBuffer chunks`, async (_t) => {
		const ab = new TextEncoder().encode("hi").buffer;
		strictEqual(await streamToString(createReadableStream([ab])), "hi");
	});

	test(`web createReadableStream honors typed-array byteOffset/byteLength`, async (_t) => {
		const web = await loadWeb();
		// A subarray view over a larger buffer must stream only its own window,
		// not the whole backing ArrayBuffer (adjacent-heap leak otherwise).
		const full = new Uint8Array([0, 0, 1, 2, 3, 0, 0]);
		const view = full.subarray(2, 5); // [1,2,3]
		const out = await web.streamToBuffer(web.createReadableStream(view));
		deepStrictEqual(Array.from(out), [1, 2, 3]);
	});

	test(`web createReadableStream honors Buffer byteOffset/byteLength`, async (_t) => {
		const web = await loadWeb();
		// Node Buffers share a pooled allocation; reading the whole backing buffer
		// would leak pooled bytes and yield far more than 5 bytes.
		const buf = Buffer.from([1, 2, 3, 4, 5]);
		const out = await web.streamToBuffer(web.createReadableStream(buf));
		deepStrictEqual(Array.from(out), [1, 2, 3, 4, 5]);
	});

	test(`web create*Stream remove the abort listener on error`, async (_t) => {
		const web = await loadWeb();
		const controller = new AbortController();
		const { signal } = controller;
		const before = signal.removeEventListener;
		let removed = 0;
		// Count removeEventListener calls for 'abort' to detect listener cleanup.
		signal.removeEventListener = function (type, ...rest) {
			if (type === "abort") removed += 1;
			return before.call(this, type, ...rest);
		};
		const streams = [
			web.createReadableStream(["a", "b", "c"]),
			web.createTransformStream(
				() => {
					throw new Error("boom");
				},
				{ signal },
			),
			web.createWritableStream(() => {}, { signal }),
		];
		try {
			await web.pipeline(streams);
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "boom");
		}
		// Both the transform and the writable registered an abort listener; both
		// must be removed on the error path.
		ok(removed >= 2, `expected >=2 abort listener removals, got ${removed}`);
	});

	// Race against a short timeout: an unsupported input must error promptly, not
	// hang the stream forever (which would otherwise stall the whole test run).
	const withTimeout = (promise, ms = 1000) =>
		Promise.race([
			promise,
			new Promise((_resolve, reject) =>
				setTimeout(() => reject(new Error("hung: stream never settled")), ms),
			),
		]);

	test(`web createReadableStream errors on unsupported scalar input`, async (_t) => {
		const web = await loadWeb();
		try {
			await withTimeout(web.streamToArray(web.createReadableStream(42)));
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e instanceof TypeError, `expected TypeError, got ${e}`);
			ok(e.message.includes("unsupported input"));
		}
	});

	test(`web createReadableStream errors clearly on null input`, async (_t) => {
		const web = await loadWeb();
		try {
			await withTimeout(web.streamToArray(web.createReadableStream(null)));
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e instanceof TypeError, `expected TypeError, got ${e}`);
			ok(e.message.includes("unsupported input"));
		}
	});

	test(`web isReadable/isWritable return false for null/undefined`, async (_t) => {
		const web = await loadWeb();
		strictEqual(web.isReadable(null), false);
		strictEqual(web.isReadable(undefined), false);
		strictEqual(web.isWritable(null), false);
		strictEqual(web.isWritable(undefined), false);
		// And primitives must not throw either.
		strictEqual(web.isReadable(42), false);
		strictEqual(web.isWritable("x"), false);
	});

	// ===========================================================================
	// *** Mutation-killing tests (node build) ***
	// Each test below pins a specific behavior so a one-line mutation of the
	// source would flip an assertion. Grouped by the source construct targeted.
	// ===========================================================================

	// --- EventEmitter-only collector path (the `typeof stream.on === "function"`
	// branch). A Node Readable is also async-iterable, so deleting the .on branch
	// still works via the async path. A bare EventEmitter is NOT async-iterable, so
	// only the .on path can drain it; this distinguishes the two code paths. ---
	const emitterStream = (chunks) => {
		const ee = new EventEmitter();
		// Not async-iterable on purpose: no Symbol.asyncIterator, no .pipe.
		process.nextTick(() => {
			for (const c of chunks) ee.emit("data", c);
			ee.emit("end");
		});
		return ee;
	};

	if (variant === "node") {
		test(`streamToArray uses the .on path for plain EventEmitters`, async (_t) => {
			const out = await streamToArray(emitterStream(["a", "b", "c"]));
			deepStrictEqual(out, ["a", "b", "c"]);
		});

		test(`streamToString uses the .on path for plain EventEmitters`, async (_t) => {
			const out = await streamToString(emitterStream(["a", "b", "c"]));
			strictEqual(out, "abc");
		});

		test(`streamToObject uses the .on path for plain EventEmitters`, async (_t) => {
			const out = await streamToObject(emitterStream([{ a: 1 }, { b: 2 }]));
			deepStrictEqual(out, { a: 1, b: 2 });
		});

		test(`streamToBuffer uses the .on path for plain EventEmitters`, async (_t) => {
			const out = await streamToBuffer(
				emitterStream([Buffer.from("ab"), Buffer.from("c")]),
			);
			strictEqual(out.toString(), "abc");
		});

		// The .on path must reject on 'error' (kills deletion of stream.on("error")).
		test(`streamToArray .on path rejects on error event`, async (_t) => {
			const ee = new EventEmitter();
			ee.destroy = () => {};
			process.nextTick(() => ee.emit("error", new Error("boom")));
			try {
				await withTimeout(streamToArray(ee));
				throw new Error("Should have thrown");
			} catch (e) {
				strictEqual(e.message, "boom");
			}
		});
	}

	// --- maxBufferSize boundary precision for every collector. The threshold is a
	// strict `>` and the running total is an ADDITION. These pin `>` (not >=, <=),
	// the `+=` (not -=), and that the boundary value itself does NOT throw. ---

	// streamToArray: object-mode length fallback is `?? 1` per chunk.
	test(`streamToArray counts each non-sized chunk as 1 (?? 1 fallback)`, async (_t) => {
		// 3 plain objects -> size 3. maxBufferSize 3 must pass (boundary, not > ).
		const ok3 = await streamToArray(createReadableStream([{}, {}, {}]), {
			maxBufferSize: 3,
		});
		strictEqual(ok3.length, 3);
		// A 4th identical chunk -> size 4 > 3 must throw (kills `?? 0`, `-=`, `>=`).
		try {
			await streamToArray(createReadableStream([{}, {}, {}, {}]), {
				maxBufferSize: 3,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToArray at exact maxBufferSize boundary does not throw`, async (_t) => {
		// "aaa"+"bbb" = 6 chars; maxBufferSize 6 is the boundary and must pass.
		const out = await streamToArray(createReadableStream(["aaa", "bbb"]), {
			maxBufferSize: 6,
		});
		deepStrictEqual(out, ["aaa", "bbb"]);
	});

	test(`streamToArray uses byteLength when length is absent`, async (_t) => {
		// Uint8Array has byteLength but no string length; size must accrue 5.
		const chunk = Uint8Array.from([1, 2, 3, 4, 5]);
		try {
			await streamToArray(createReadableStream([chunk]), { maxBufferSize: 4 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
		// And 5 (exact) passes.
		const out = await streamToArray(createReadableStream([chunk]), {
			maxBufferSize: 5,
		});
		strictEqual(out.length, 1);
	});

	test(`streamToString at exact boundary passes, one over throws`, async (_t) => {
		const ok6 = await streamToString(createReadableStream(["aaa", "bbb"]), {
			maxBufferSize: 6,
		});
		strictEqual(ok6, "aaabbb");
		try {
			await streamToString(createReadableStream(["aaa", "bbbc"]), {
				maxBufferSize: 6,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToString non-sized chunk fallback is 0, not 1`, async (_t) => {
		// streamToString uses `?? 0`: an object with no length contributes 0, so a
		// tiny maxBufferSize must NOT trip on object-mode chunks. (Kills `?? 1`.)
		const out = await streamToString(createReadableStream([{}, {}, {}]), {
			maxBufferSize: 0,
		});
		// String(...) of objects joined; just assert it did not throw.
		strictEqual(typeof out, "string");
	});

	test(`streamToObject at exact boundary passes, one over throws`, async (_t) => {
		// Each object counts as 1 (?? 1). 3 objects, boundary 3 passes.
		const ok3 = await streamToObject(
			createReadableStream([{ a: 1 }, { b: 2 }, { c: 3 }]),
			{ maxBufferSize: 3 },
		);
		deepStrictEqual(ok3, { a: 1, b: 2, c: 3 });
		try {
			await streamToObject(
				createReadableStream([{ a: 1 }, { b: 2 }, { c: 3 }, { d: 4 }]),
				{ maxBufferSize: 3 },
			);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToBuffer at exact boundary passes, one over throws`, async (_t) => {
		const ok6 = await streamToBuffer(
			createReadableStream([Buffer.from("aaa"), Buffer.from("bbb")]),
			{ maxBufferSize: 6 },
		);
		strictEqual(text(ok6), "aaabbb");
		try {
			await streamToBuffer(
				createReadableStream([Buffer.from("aaa"), Buffer.from("bbbc")]),
				{ maxBufferSize: 6 },
			);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	// The maxBufferSize error messages must name the collector and the limit value
	// (kills StringLiteral -> "" on each `buffer exceeds maxBufferSize (...)`).
	test(`maxBufferSize errors carry collector name and limit`, async (_t) => {
		const cases = [
			[streamToArray, ["aaa", "bbb", "ccc"], "streamToArray"],
			[streamToString, ["aaa", "bbb", "ccc"], "streamToString"],
			[streamToBuffer, [Buffer.from("aaaaaaa")], "streamToBuffer"],
		];
		for (const [fn, input, name] of cases) {
			try {
				await fn(createReadableStream(input), { maxBufferSize: 6 });
				throw new Error("Should have thrown");
			} catch (e) {
				ok(e.message.includes(name), `expected ${name} in: ${e.message}`);
				ok(e.message.includes("6"), `expected limit 6 in: ${e.message}`);
			}
		}
		try {
			await streamToObject(
				createReadableStream([{ a: 1 }, { b: 2 }, { c: 3 }]),
				{
					maxBufferSize: 2,
				},
			);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("streamToObject"));
			ok(e.message.includes("2"));
		}
	});

	// Limits: undefined -> default (unlimited for collectors), null -> unlimited,
	// and exceeding a limit is a RangeError.
	test(`streamTo* treat maxBufferSize null as unlimited`, async (_t) => {
		deepStrictEqual(
			await streamToArray(createReadableStream(["aaa", "bbb"]), {
				maxBufferSize: null,
			}),
			["aaa", "bbb"],
		);
		strictEqual(
			await streamToString(createReadableStream(["aaa", "bbb"]), {
				maxBufferSize: null,
			}),
			"aaabbb",
		);
		deepStrictEqual(
			await streamToObject(createReadableStream([{ a: 1 }, { b: 2 }]), {
				maxBufferSize: null,
			}),
			{ a: 1, b: 2 },
		);
		strictEqual(
			text(
				await streamToBuffer(createReadableStream(["aaa", "bbb"]), {
					maxBufferSize: null,
				}),
			),
			"aaabbb",
		);
	});

	test(`streamTo* limit errors are RangeErrors`, async (_t) => {
		const cases = [
			[streamToArray, ["aaa", "bbb"], "streamToArray"],
			[streamToString, ["aaa", "bbb"], "streamToString"],
			[streamToObject, [{ a: 1 }, { b: 2 }], "streamToObject"],
			[streamToBuffer, ["aaa", "bbb"], "streamToBuffer"],
		];
		for (const [fn, input, name] of cases) {
			await rejects(fn(createReadableStream(input), { maxBufferSize: 1 }), {
				name: "RangeError",
				message: `${name} buffer exceeds maxBufferSize (1)`,
			});
		}
	});

	// --- async-iterable path also enforces maxBufferSize boundary (covers the
	// second copy of each guard in the for-await branch). ---
	const asyncGen = (chunks) =>
		(async function* () {
			for (const c of chunks) yield c;
		})();

	test(`streamToArray async path enforces boundary precisely`, async (_t) => {
		const out = await streamToArray(asyncGen(["aaa", "bbb"]), {
			maxBufferSize: 6,
		});
		deepStrictEqual(out, ["aaa", "bbb"]);
		try {
			await streamToArray(asyncGen(["aaa", "bbbc"]), { maxBufferSize: 6 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToString async path enforces boundary precisely`, async (_t) => {
		const out = await streamToString(asyncGen(["aaa", "bbb"]), {
			maxBufferSize: 6,
		});
		strictEqual(out, "aaabbb");
		try {
			await streamToString(asyncGen(["aaa", "bbbc"]), { maxBufferSize: 6 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToObject async path enforces boundary precisely`, async (_t) => {
		const out = await streamToObject(asyncGen([{ a: 1 }, { b: 2 }, { c: 3 }]), {
			maxBufferSize: 3,
		});
		deepStrictEqual(out, { a: 1, b: 2, c: 3 });
		try {
			await streamToObject(asyncGen([{ a: 1 }, { b: 2 }, { c: 3 }, { d: 4 }]), {
				maxBufferSize: 3,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToBuffer async path enforces boundary precisely`, async (_t) => {
		const out = await streamToBuffer(
			asyncGen([Buffer.from("aaa"), Buffer.from("bbb")]),
			{ maxBufferSize: 6 },
		);
		strictEqual(text(out), "aaabbb");
		try {
			await streamToBuffer(
				asyncGen([Buffer.from("aaa"), Buffer.from("bbbc")]),
				{
					maxBufferSize: 6,
				},
			);
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	// streamToObject async byteLength fallback (Uint8Array chunks count by bytes).
	test(`streamToObject byteLength fallback counts bytes`, async (_t) => {
		// A typed array of 5 bytes -> size 5 > 4 must throw even in object mode.
		const chunk = Uint8Array.from([1, 2, 3, 4, 5]);
		try {
			await streamToObject(createReadableStream([chunk]), { maxBufferSize: 4 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	// The size fallback is `length ?? byteLength ?? N`, NOT `length && byteLength`.
	// A chunk with NO `length` but a truthy `byteLength` must count by byteLength.
	// Under the `&&` (LogicalOperator) mutant, `undefined && byteLength` is
	// undefined, so the fallback collapses to N (1 or 0) and the limit is not hit.
	// Use a plain object exposing only `byteLength` so `?.length` is undefined.
	const byteLenChunk = (n, extra = {}) => ({ byteLength: n, ...extra });

	test(`streamToArray counts a byteLength-only chunk by its byteLength`, async (_t) => {
		// .on path: size 5 > 4 must throw (kills `?? -> &&` at the .on guard).
		try {
			await streamToArray(createReadableStream([byteLenChunk(5)]), {
				maxBufferSize: 4,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
		// async path: same expectation.
		try {
			await streamToArray(asyncGen([byteLenChunk(5)]), { maxBufferSize: 4 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToObject counts a byteLength-only chunk by its byteLength`, async (_t) => {
		try {
			await streamToObject(createReadableStream([byteLenChunk(5, { a: 1 })]), {
				maxBufferSize: 4,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
		try {
			await streamToObject(asyncGen([byteLenChunk(5, { a: 1 })]), {
				maxBufferSize: 4,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	test(`streamToString counts a byteLength-only chunk by its byteLength`, async (_t) => {
		// streamToString fallback is `?? 0`; a byteLength-only chunk of 5 still
		// exceeds maxBufferSize 4 via byteLength (kills `?? -> &&`).
		try {
			await streamToString(createReadableStream([byteLenChunk(5)]), {
				maxBufferSize: 4,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
		try {
			await streamToString(asyncGen([byteLenChunk(5)]), { maxBufferSize: 4 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	// --- async path `?? N` size fallbacks when chunks have neither length nor
	// byteLength (kills `?? 1` / `?? 0` in the for-await branch of each collector). ---

	test(`streamToArray async path ?? 1 fallback counts no-size chunks`, async (_t) => {
		// A plain object has no length/byteLength; ?? 1 makes it count as 1.
		// 4 such chunks -> size 4 > 3 must throw (kills the ?? 1 deletion mutant).
		try {
			await streamToArray(asyncGen([{}, {}, {}, {}]), { maxBufferSize: 3 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
		// 3 chunks -> size 3, boundary passes.
		const out = await streamToArray(asyncGen([{}, {}, {}]), {
			maxBufferSize: 3,
		});
		strictEqual(out.length, 3);
	});

	test(`streamToString async path ?? 0 fallback does not count no-size chunks`, async (_t) => {
		// streamToString uses ?? 0 for chunks without length/byteLength.
		// Even with maxBufferSize: 0, plain-object chunks must NOT trip the limit.
		const out = await streamToString(asyncGen([{}, {}]), { maxBufferSize: 0 });
		strictEqual(typeof out, "string");
	});

	nodeTest(
		`streamToBuffer async path handles null-sentinel chunk`,
		async (_t) => {
			// The async path of streamToBuffer wraps each chunk with fromSafe(...) ?? [].
			// Yielding the NULL_SENTINEL symbol triggers fromSafe -> null -> [] -> empty Buffer.
			const NULL_SENTINEL = Symbol.for("@datastream/null");
			async function* gen() {
				yield "hi";
				yield NULL_SENTINEL;
			}
			const out = await streamToBuffer(gen());
			// Only "hi" contributes bytes; the null-sentinel chunk becomes an empty buffer.
			strictEqual(out.toString(), "hi");
		},
	);

	// --- NULL_SENTINEL round-trip: a transform enqueue(null) flows through as null,
	// AND a literal pushed null is treated as EOF (does not appear). ---
	test(`createReadableStream array null becomes a flowing null value`, async (_t) => {
		// toSafe maps array `null` -> NULL_SENTINEL so it is not interpreted as EOF;
		// the collector's fromSafe maps it back to null.
		const out = await streamToArray(createReadableStream(["a", null, "b"]));
		deepStrictEqual(out, ["a", null, "b"]);
	});

	// --- sanitizeObject: own-enumerable __proto__ key must be stripped. Use a
	// literal own-enumerable key via Object.defineProperty so the
	// `key === "__proto__"` guard is exercised through Object.keys. ---
	test(`streamToObject strips an own-enumerable __proto__ key`, async (_t) => {
		const evil = {};
		Object.defineProperty(evil, "__proto__", {
			value: { polluted: true },
			enumerable: true,
			configurable: true,
			writable: true,
		});
		Object.defineProperty(evil, "safe", {
			value: 1,
			enumerable: true,
			configurable: true,
			writable: true,
		});
		strictEqual(Object.keys(evil).includes("__proto__"), true);
		const out = await streamToObject(asyncGen([evil]));
		strictEqual(Object.hasOwn(out, "__proto__"), false);
		strictEqual(out.safe, 1);
		// Without the `key === "__proto__"` guard, `out["__proto__"] = ...` would
		// replace out's prototype (out is a plain `{}`), leaking `polluted` and
		// changing the prototype. The guard must keep a clean Object.prototype.
		strictEqual(Object.getPrototypeOf(out), Object.prototype);
		strictEqual(out.polluted, undefined);
	});

	// --- pipeline: lastStream index is length - 1 (kills `+ 1`). A single pure
	// Readable needs an appended writable sink to terminate; with `+ 1` the index
	// is undefined, isReadable(undefined) is false, no sink is appended, and
	// pipelinePromise([readable]) rejects with "streams argument must be specified".
	// This also kills the isReadable(lastStream) branch deletion. ---
	test(`pipeline appends a sink for a single pure readable`, async (_t) => {
		const out = await withTimeout(
			pipeline([createReadableStream(["a", "b", "c"])]),
		);
		deepStrictEqual(out, {});
	});

	// --- pipeline promise guard loop must actually iterate (kills loop `false`,
	// `idx >= l`, block deletion, the `typeof ... then` check, and the index in the
	// message). A promise at index 2 must be reported with that index. ---
	test(`pipeline reports the index of a promise-instead-of-stream`, async (_t) => {
		const streams = [
			createReadableStream(["a"]),
			createTransformStream(),
			Promise.resolve(createTransformStream()),
		];
		try {
			await pipeline(streams);
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "Promise instead of stream passed in at index 2");
		}
	});

	// pipejoin has no onError callback any more: a stream error tears the chain
	// down and surfaces on the returned stream (browser parity) instead of being
	// rethrown on process.nextTick as an uncaught exception.
	nodeTest(
		`pipejoin surfaces errors on the returned stream without an uncaught rethrow`,
		async (_t) => {
			const uncaught = spy();
			const prior = process.listeners("uncaughtException");
			for (const l of prior) process.removeListener("uncaughtException", l);
			process.on("uncaughtException", uncaught);
			try {
				const legacyOnError = spy();
				const streams = [
					createReadableStream(["a"]),
					createTransformStream(() => {
						throw new Error("boom2");
					}),
				];
				const joined = pipejoin(streams, legacyOnError);
				await rejects(streamToArray(joined), { message: "boom2" });
				await new Promise((resolve) => setImmediate(resolve));
				strictEqual(legacyOnError.callCount, 0);
				strictEqual(uncaught.callCount, 0);
			} finally {
				process.removeListener("uncaughtException", uncaught);
				for (const l of prior) process.on("uncaughtException", l);
			}
		},
	);

	// --- createReadableStream branch coverage ---

	// Array.isArray branch: array maps null -> sentinel so an embedded null is
	// preserved as a value (raw null would be EOF). (kills branch deletion /
	// `false` / `true`).
	test(`createReadableStream array branch preserves embedded null`, async (_t) => {
		const out = await streamToArray(createReadableStream([null, "x"]));
		deepStrictEqual(out, [null, "x"]);
	});

	// object-with-byteLength branch (ArrayBuffer) yields a single Uint8Array chunk.
	test(`createReadableStream routes ArrayBuffer to byte chunker`, async (_t) => {
		const buf = new Uint8Array([9, 8, 7]).buffer;
		const out = await streamToArray(createReadableStream(buf));
		strictEqual(out.length, 1);
		deepStrictEqual(Array.from(out[0]), [9, 8, 7]);
	});

	// String routing splits by chunkSize via substring (kills MethodExpression
	// yield input2 and the position arithmetic).
	test(`createReadableStream chunks strings by chunkSize via substring`, async (_t) => {
		const input = "abcdefgh";
		const out = await streamToArray(
			createReadableStream(input, { chunkSize: 3 }),
		);
		deepStrictEqual(out, ["abc", "def", "gh"]);
	});

	test(`createReadableStream string chunk boundary is exact`, async (_t) => {
		// length 6, chunkSize 3 -> exactly 2 full chunks, no empty trailing chunk
		// (kills while `<=` which would emit an extra empty "" at position===length).
		const out = await streamToArray(
			createReadableStream("abcdef", { chunkSize: 3 }),
		);
		deepStrictEqual(out, ["abc", "def"]);
	});

	// chunkSize <= 0 must throw (kills `<= 0` -> `< 0`, `false`, and message "").
	test(`createReadableStream rejects zero chunkSize for strings`, (_t) => {
		try {
			createReadableStream("abc", { chunkSize: 0 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("positive number"));
		}
	});

	test(`createReadableStream rejects negative chunkSize for strings`, (_t) => {
		try {
			createReadableStream("abc", { chunkSize: -1 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("positive number"));
		}
	});

	test(`createReadableStream rejects zero chunkSize for ArrayBuffer`, (_t) => {
		try {
			createReadableStream(new Uint8Array([1, 2, 3]).buffer, { chunkSize: 0 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("positive number"));
		}
	});

	test(`createReadableStream rejects negative chunkSize for ArrayBuffer`, (_t) => {
		try {
			createReadableStream(new Uint8Array([1, 2, 3]).buffer, { chunkSize: -2 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("positive number"));
		}
	});

	// ArrayBuffer chunking by size (kills while/position mutants and the subarray
	// window arithmetic in the arraybuffer iterator).
	if (variant === "node") {
		test(`createReadableStream chunks ArrayBuffer by chunkSize`, async (_t) => {
			const buf = new Uint8Array([1, 2, 3, 4, 5]).buffer;
			const out = await streamToArray(
				createReadableStream(buf, { chunkSize: 2 }),
			);
			deepStrictEqual(
				out.map((c) => Array.from(c)),
				[[1, 2], [3, 4], [5]],
			);
		});
	}

	// createReadableStream() push queue guard: the `chunk !== null` half means
	// pushing null (EOF) is always allowed even past the limit.
	if (variant === "node") {
		test(`createReadableStream allows null push even at queue limit`, async (_t) => {
			const stream = createReadableStream(undefined, { highWaterMark: 2 });
			stream.push("a");
			stream.push("b");
			// At the limit; pushing null (EOF) must NOT throw (kills `chunk !== null`
			// -> `true`, which would throw on the null push).
			stream.push(null);
			const out = await streamToArray(stream);
			deepStrictEqual(out, ["a", "b"]);
		});

		test(`createReadableStream allows pushing up to the limit`, async (_t) => {
			const stream = createReadableStream(undefined, { highWaterMark: 3 });
			stream.push("a");
			stream.push("b");
			stream.push("c");
			stream.push(null);
			const out = await streamToArray(stream);
			deepStrictEqual(out, ["a", "b", "c"]);
		});
	}

	// --- createPassThroughStream default passThrough is identity (kills the
	// ArrowFunction mutant `() => undefined`). With no transform fn, chunks must
	// flow through unchanged. ---
	test(`createPassThroughStream default identity passes chunks through`, async (_t) => {
		const out = await streamToArray(
			pipejoin([createReadableStream(["a", "b"]), createPassThroughStream()]),
		);
		deepStrictEqual(out, ["a", "b"]);
	});

	// --- The thenable check must be a strict `typeof result.then === "function"`.
	// A handler that returns a non-null, non-thenable value (e.g. a string) must NOT
	// be treated as a promise: with `&& true` (or `!== "function"`, or `=== ""`)
	// the code would call `result.then(...)` on a string and throw. Returning a
	// plain value must pass cleanly. ---
	test(`createPassThroughStream tolerates a non-thenable return value`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream(["a", "b"]),
				createPassThroughStream((c) => `sync:${c}`),
			]),
		);
		deepStrictEqual(out, ["a", "b"]);
	});

	test(`createTransformStream tolerates a non-thenable return value`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream(["a", "b"]),
				createTransformStream((c, enqueue) => {
					enqueue(c);
					return "sync-return";
				}),
			]),
		);
		deepStrictEqual(out, ["a", "b"]);
	});

	test(`createWritableStream tolerates a non-thenable write return`, async (_t) => {
		const seen = [];
		await pipeline([
			createReadableStream(["a", "b"]),
			createWritableStream((c) => {
				seen.push(c);
				return `sync:${c}`;
			}),
		]);
		deepStrictEqual(seen, ["a", "b"]);
	});

	test(`createPassThroughStream flush tolerates a non-thenable return`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream(["a"]),
				createPassThroughStream(
					(c) => c,
					() => "flush-sync",
				),
			]),
		);
		deepStrictEqual(out, ["a"]);
	});

	test(`createTransformStream flush tolerates a non-thenable return`, async (_t) => {
		const out = await streamToArray(
			pipejoin([
				createReadableStream(["a"]),
				createTransformStream(
					(c, enqueue) => enqueue(c),
					() => "flush-sync",
				),
			]),
		);
		deepStrictEqual(out, ["a"]);
	});

	test(`createWritableStream final tolerates a non-thenable return`, async (_t) => {
		const seen = [];
		await pipeline([
			createReadableStream(["a"]),
			createWritableStream(
				(c) => seen.push(c),
				() => "final-sync",
			),
		]);
		deepStrictEqual(seen, ["a"]);
	});

	// --- async vs sync result detection in create*Stream. A function returning a
	// thenable must be awaited BEFORE the chunk is pushed / callback runs. Ordering
	// proves the promise branch (kills `=== "function"` -> `!== "function"` /
	// `true` / `false` / "" in the thenable checks). ---
	test(`createPassThroughStream awaits a returned promise before continuing`, async (_t) => {
		const order = [];
		const streams = [
			createReadableStream(["a"]),
			createPassThroughStream(async () => {
				order.push("transform-start");
				await timeout(20);
				order.push("transform-end");
			}),
			createWritableStream((c) => {
				order.push(`write-${c}`);
			}),
		];
		await pipeline(streams);
		deepStrictEqual(order, ["transform-start", "transform-end", "write-a"]);
	});

	test(`createTransformStream awaits a returned promise before continuing`, async (_t) => {
		const order = [];
		const streams = [
			createReadableStream(["a"]),
			createTransformStream(async (chunk, enqueue) => {
				order.push("t-start");
				await timeout(20);
				order.push("t-end");
				enqueue(chunk);
			}),
			createWritableStream((c) => order.push(`w-${c}`)),
		];
		await pipeline(streams);
		deepStrictEqual(order, ["t-start", "t-end", "w-a"]);
	});

	test(`createWritableStream awaits a returned promise before final`, async (_t) => {
		const order = [];
		const streams = [
			createReadableStream(["a"]),
			createWritableStream(
				async () => {
					order.push("write-start");
					await timeout(20);
					order.push("write-end");
				},
				() => {
					order.push("final");
				},
			),
		];
		await pipeline(streams);
		deepStrictEqual(order, ["write-start", "write-end", "final"]);
	});

	test(`createPassThroughStream awaits an async flush before finishing`, async (_t) => {
		const order = [];
		const streams = [
			createReadableStream(["a"]),
			createPassThroughStream(
				() => order.push("pass"),
				async () => {
					order.push("flush-start");
					await timeout(20);
					order.push("flush-end");
				},
			),
			createWritableStream(),
		];
		await pipeline(streams);
		deepStrictEqual(order, ["pass", "flush-start", "flush-end"]);
	});

	test(`createTransformStream awaits an async flush before finishing`, async (_t) => {
		const order = [];
		const streams = [
			createReadableStream(["a"]),
			createTransformStream(
				(c, enqueue) => {
					order.push("t");
					enqueue(c);
				},
				async () => {
					order.push("flush-start");
					await timeout(20);
					order.push("flush-end");
				},
			),
			createWritableStream(),
		];
		await pipeline(streams);
		deepStrictEqual(order, ["t", "flush-start", "flush-end"]);
	});

	// --- timeout abort: exact error shape (kills cause/code ObjectLiteral and
	// StringLiteral mutants) for BOTH the already-aborted and abort-during paths. ---
	test(`timeout already-aborted rejects with AbortError cause code`, async (_t) => {
		const controller = new AbortController();
		controller.abort();
		try {
			await timeout(1000, { signal: controller.signal });
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "Aborted");
			deepStrictEqual(e.cause, { code: "AbortError" });
			strictEqual(e.cause.code, "AbortError");
		}
	});

	test(`timeout abort-during rejects with AbortError cause code`, async (_t) => {
		const controller = new AbortController();
		const promise = timeout(60_000, { signal: controller.signal });
		controller.abort();
		try {
			await promise;
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "Aborted");
			deepStrictEqual(e.cause, { code: "AbortError" });
			strictEqual(e.cause.code, "AbortError");
		}
	});

	// timeout abort handler `settled` guard: after an abort, the later timer firing
	// must NOT resolve the already-rejected promise (double-settle is a no-op).
	test(`timeout abort wins the race; later timer does not resolve`, async (_t) => {
		const controller = new AbortController();
		const promise = timeout(30, { signal: controller.signal });
		controller.abort();
		let resolved = false;
		let rejected = false;
		promise.then(
			() => {
				resolved = true;
			},
			() => {
				rejected = true;
			},
		);
		// Wait past the original 30ms timer to ensure its callback, if it ran,
		// cannot flip an already-settled promise to resolved.
		await timeout(60);
		strictEqual(rejected, true);
		strictEqual(resolved, false);
	});

	test(`timeout removes its abort listener after resolving normally`, async (_t) => {
		const controller = new AbortController();
		const { signal } = controller;
		let removedAbort = 0;
		const realRemove = signal.removeEventListener.bind(signal);
		signal.removeEventListener = (type, ...rest) => {
			if (type === "abort") removedAbort += 1;
			return realRemove(type, ...rest);
		};
		await timeout(10, { signal });
		// On normal resolution the listener registered for "abort" must be removed
		// (kills the `if (signal) signal.removeEventListener("abort", ...)` and the
		// "" string mutant in the resolve path).
		strictEqual(removedAbort, 1);
	});

	test(`timeout removes its abort listener on the abort path`, async (_t) => {
		const controller = new AbortController();
		const { signal } = controller;
		let removedAbort = 0;
		const realRemove = signal.removeEventListener.bind(signal);
		signal.removeEventListener = (type, ...rest) => {
			if (type === "abort") removedAbort += 1;
			return realRemove(type, ...rest);
		};
		const promise = timeout(60_000, { signal });
		controller.abort();
		await promise.catch(() => {});
		// The abort handler must remove its own "abort" listener (kills the
		// removeEventListener("abort", ...) -> "" string mutant on the abort path).
		strictEqual(removedAbort, 1);
	});

	// --- backpressureGauge: duration arithmetic is a SUBTRACTION (Date.now() -
	// start), so durations are small/non-negative, not gigantic sums (kills `+`). ---
	nodeTest(
		`backpressureGauge pause/resume duration is a small subtraction`,
		async (_t) => {
			const transform = createTransformStream();
			const metrics = backpressureGauge({ transform });
			transform.emit("pause");
			await timeout(15);
			transform.emit("resume");
			const { duration } = metrics.transform.timeline[0];
			ok(duration >= 0, `duration should be >=0, got ${duration}`);
			// A summed (Date.now()+timestamp) value would be ~2*Date.now() (>1e12).
			ok(duration < 1e6, `duration should be small, got ${duration}`);
		},
	);

	// total recorded via 'finish'/'close' for writable sinks, AND the
	// double-record guard means total.timestamp is set exactly once.
	nodeTest(
		`backpressureGauge records writable total exactly once`,
		async (_t) => {
			const writable = createWritableStream();
			const metrics = backpressureGauge({ writable });
			// Fire all three terminal events; guard must record total exactly once.
			writable.emit("finish");
			const first = metrics.writable.total.timestamp;
			const firstDuration = metrics.writable.total.duration;
			strictEqual(typeof first, "number");
			await timeout(30);
			writable.emit("close");
			writable.emit("end");
			// Both timestamp AND duration must be unchanged by the later events. The
			// recorded timestamp is always startTimestamp (so it can't move), but
			// duration is Date.now()-start, so a re-record after a 30ms wait would
			// bump duration. The `!= null` guard must prevent that (kills it -> false).
			strictEqual(metrics.writable.total.timestamp, first);
			strictEqual(metrics.writable.total.duration, firstDuration);
			ok(metrics.writable.total.duration >= 0);
			ok(metrics.writable.total.duration < 1e6);
		},
	);

	// 'finish' and 'close' events are both wired (kills on("") string mutants):
	// a writable that emits only 'finish' (no 'end') must still get a total.
	nodeTest(
		`backpressureGauge records total on the finish event`,
		async (_t) => {
			const writable = createWritableStream();
			const metrics = backpressureGauge({ writable });
			writable.emit("finish");
			strictEqual(typeof metrics.writable.total.timestamp, "number");
		},
	);

	nodeTest(`backpressureGauge records total on the close event`, async (_t) => {
		const writable = createWritableStream();
		const metrics = backpressureGauge({ writable });
		writable.emit("close");
		strictEqual(typeof metrics.writable.total.timestamp, "number");
	});

	// --- pipeline derives objectMode from a trailing readable's state and pushes a
	// sink built from a (non-empty) streamOptions object. With the readable-last
	// case, object chunks must drain without a non-buffer-chunk error. (covers the
	// isReadable branch and the streamOptions object literal.) ---
	test(`pipeline drains an object-mode readable-last stream`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1 }, { b: 2 }]),
			createTransformStream((c, enqueue) => enqueue(c)),
		];
		const out = await withTimeout(pipeline(streams));
		deepStrictEqual(out, {});
	});

	// --- pipeline passes the spread streamOptions object (not `{}`) to
	// pipelinePromise for a readable-last pipeline. A signal supplied in
	// streamOptions and aborted mid-flight must abort the pipeline. The
	// ObjectLiteral -> `{}` mutant drops the signal so the pipeline runs to
	// completion and resolves instead of rejecting. ---
	if (variant === "node") {
		test(`pipeline forwards a streamOptions signal to the readable-last sink`, async (_t) => {
			// A slow trailing readable so the abort lands while the pipeline runs.
			let emitted = 0;
			const slowReadable = new Readable({
				objectMode: true,
				read() {
					setTimeout(() => {
						this.push(emitted < 10 ? String(emitted++) : null);
					}, 20);
				},
			});
			const controller = new AbortController();
			const pending = pipeline([slowReadable], { signal: controller.signal });
			setTimeout(() => controller.abort(), 30);
			try {
				await withTimeout(pending);
				throw new Error("Should have thrown");
			} catch (e) {
				strictEqual(e.code, "ABORT_ERR");
			}
		});
	}

	// --- audit: a caller-supplied terminal writable must still honor
	// streamOptions.signal (the browser build only gave the signal to a sink it
	// appended itself, so it read every chunk after abort). ---
	test(`pipeline rejects on streamOptions.signal abort with a caller-supplied writable`, async (_t) => {
		const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
		async function* gen() {
			for (let i = 0; i < 5; i++) {
				await sleep(10);
				yield String(i);
			}
		}
		const written = [];
		const controller = new AbortController();
		const run = pipeline(
			[
				createReadableStream(gen()),
				createWritableStream((chunk) => {
					written.push(chunk);
				}),
			],
			{ signal: controller.signal },
		);
		await sleep(25);
		controller.abort();
		await rejects(run, { name: "AbortError" });
		await sleep(60);
		ok(written.length < 5, `wrote ${written.length} chunks after abort`);
	});

	// --- regression: aborting streamOptions.signal mid-write must tear the
	// source down (iterator return() for generators, cancel() for a raw
	// ReadableStream). The browser build once handed the signal to every
	// pipeThrough, whose own abort handling raced the terminal pipe and never
	// cancelled the source, leaking paginated fetches / DB cursors. ---
	test(`pipeline abort mid-write runs the source iterator's return()`, async (_t) => {
		const tick = () => new Promise((resolve) => setImmediate(resolve));
		let returned = false;
		function* gen() {
			try {
				let i = 0;
				while (true) yield String(i++);
			} finally {
				returned = true;
			}
		}
		const controller = new AbortController();
		// Abort while a write is still pending (it settles on the next tick).
		const sink = createWritableStream(
			() =>
				new Promise((resolve) =>
					setImmediate(() => {
						controller.abort();
						resolve();
					}),
				),
		);
		await rejects(
			pipeline([createReadableStream(gen()), createTransformStream(), sink], {
				signal: controller.signal,
			}),
			{ name: "AbortError" },
		);
		await tick();
		strictEqual(returned, true);
	});

	if (variant === "browser") {
		test(`pipeline abort mid-write cancels a raw ReadableStream source`, async (_t) => {
			const tick = () => new Promise((resolve) => setImmediate(resolve));
			let pulls = 0;
			let cancelReason;
			const source = new ReadableStream({
				pull(c) {
					pulls += 1;
					c.enqueue(String(pulls));
				},
				cancel(reason) {
					cancelReason = reason;
				},
			});
			const controller = new AbortController();
			const sink = createWritableStream(
				() =>
					new Promise((resolve) =>
						setImmediate(() => {
							controller.abort();
							resolve();
						}),
					),
			);
			await rejects(
				pipeline([source, createTransformStream(), sink], {
					signal: controller.signal,
				}),
				{ name: "AbortError" },
			);
			await tick();
			strictEqual(cancelReason?.name, "AbortError");
		});
	}

	// --- size accumulation `chunk?.length ?? chunk?.byteLength ?? N` is computed
	// for every chunk. An `undefined` chunk leaves both optional chains nullish so
	// the `?? N` fallback applies. Removing either `?.` (OptionalChaining mutant)
	// makes `undefined.length` / `undefined.byteLength` throw, so a stream carrying
	// an `undefined` chunk must still drain cleanly. Covers .on and async paths of
	// every collector. ---
	if (variant === "node") {
		test(`streamToArray tolerates an undefined chunk (.on + async paths)`, async (_t) => {
			const onOut = await streamToArray(emitterStream([undefined, "a"]));
			deepStrictEqual(onOut, [undefined, "a"]);
			const asyncOut = await streamToArray(asyncGen([undefined, "a"]));
			deepStrictEqual(asyncOut, [undefined, "a"]);
		});

		test(`streamToString tolerates an undefined chunk (.on + async paths)`, async (_t) => {
			const onOut = await streamToString(emitterStream([undefined, "a"]));
			strictEqual(onOut, "a");
			const asyncOut = await streamToString(asyncGen([undefined, "a"]));
			strictEqual(asyncOut, "a");
		});

		test(`streamToObject tolerates an undefined chunk (.on + async paths)`, async (_t) => {
			const onOut = await streamToObject(emitterStream([undefined, { a: 1 }]));
			deepStrictEqual(onOut, { a: 1 });
			const asyncOut = await streamToObject(asyncGen([undefined, { a: 1 }]));
			deepStrictEqual(asyncOut, { a: 1 });
		});
	}

	// --- createReadableStream input dispatch. An iterable WITHOUT a `.map` method
	// (a Set) must reach the `Readable.from(input)` branch. The Array.isArray ->
	// `true` mutant would call `set.map(...)`, which throws because Set has no map. ---
	test(`createReadableStream streams a non-array iterable (Set) input`, async (_t) => {
		const input = new Set(["a", "b", "c"]);
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, ["a", "b", "c"]);
	});

	// --- The node string/byte chunkers read `streamOptions?.chunkSize`, so an
	// explicit `null` options object falls back to the default chunkSize (the
	// OptionalChaining mutant would throw on `null.chunkSize`). ---
	nodeTest(
		`createReadableStream tolerates null streamOptions for strings and bytes`,
		async (_t) => {
			strictEqual(
				await streamToString(createReadableStream("abcdef", null)),
				"abcdef",
			);
			const input = new Uint8Array([1, 2, 3, 4, 5]).buffer;
			const output = await streamToArray(createReadableStream(input, null));
			deepStrictEqual(Array.from(output[0]), [1, 2, 3, 4, 5]);
		},
	);

	// --- string chunker loops `while (position < length)`. With a chunkSize that
	// divides the input length exactly, the `<=` (EqualityOperator) mutant runs one
	// extra iteration and yields a trailing empty-string chunk. Pin the exact chunk
	// list so a phantom "" chunk fails the assertion. ---
	test(`createReadableStream string stops exactly at the end (no trailing empty chunk)`, async (_t) => {
		// length 8, chunkSize 4 -> exactly 2 chunks, no remainder.
		const stream = createReadableStream("abcdefgh", { chunkSize: 4 });
		const output = await streamToArray(stream);
		deepStrictEqual(output, ["abcd", "efgh"]);
	});

	// --- byte chunker loops `while (position < length)`. With a chunkSize
	// dividing byteLength exactly, the `<=` mutant runs one extra iteration and
	// yields a trailing empty (zero-length) chunk. Pin exactly 2 chunks. ---
	test(`createReadableStream ArrayBuffer stops exactly at the end (no trailing empty chunk)`, async (_t) => {
		// 8 bytes, chunkSize 4 -> exactly 2 chunks of 4 bytes, no remainder.
		const input = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]).buffer;
		const stream = createReadableStream(input, { chunkSize: 4 });
		const output = await streamToArray(stream);
		strictEqual(output.length, 2);
		deepStrictEqual(Array.from(output[0]), [1, 2, 3, 4]);
		deepStrictEqual(Array.from(output[1]), [5, 6, 7, 8]);
	});

	// --- aborting a stream over a generator must call its return() so upstream
	// cleanup (finally blocks: DB cursors, fetch bodies) runs. ---
	test(`createReadableStream abort calls the source iterator's return()`, async (_t) => {
		const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
		let cleaned = false;
		async function* gen() {
			try {
				// Unbounded but paced source; a pending next() would queue return().
				for (;;) {
					await sleep(2);
					yield "a";
				}
			} finally {
				cleaned = true;
			}
		}
		const controller = new AbortController();
		const stream = createReadableStream(gen(), { signal: controller.signal });
		const out = streamToArray(stream);
		await sleep(10);
		controller.abort();
		await rejects(out, { name: "AbortError" });
		await sleep(10);
		strictEqual(cleaned, true);
	});

	// --- createReadableStream covers the removed createReadableStreamFrom{String,
	// ArrayBuffer}: empty string / zero-length ArrayBuffer (or SharedArrayBuffer)
	// stream no chunks, and a view streams its raw bytes. ---
	test(`createReadableStream streams empty string and zero-length buffers as no chunks`, async (_t) => {
		deepStrictEqual(await streamToArray(createReadableStream("")), []);
		deepStrictEqual(
			await streamToArray(createReadableStream(new ArrayBuffer(0))),
			[],
		);
		deepStrictEqual(
			await streamToArray(createReadableStream(new SharedArrayBuffer(0))),
			[],
		);
		const [shared] = await streamToArray(
			createReadableStream(new SharedArrayBuffer(2)),
		);
		deepStrictEqual(Array.from(shared), [0, 0]);
		// A view streams its raw bytes, not element values (Uint16 0x0102).
		const u16 = new Uint16Array([0x0102]);
		const [chunk] = await streamToArray(createReadableStream(u16));
		deepStrictEqual(Array.from(chunk), Array.from(new Uint8Array(u16.buffer)));
	});

	// --- createPassThroughStream default passThrough is `(chunk) => chunk`. The
	// default's return value feeds the thenable detection: when a chunk is itself a
	// rejecting thenable, the default returns it, the stream awaits it, and the
	// rejection surfaces as a stream error. The ArrowFunction mutant
	// (`() => undefined`) returns undefined, skips the await, and the pipeline
	// would resolve instead of rejecting. ---
	test(`createPassThroughStream default forwards the chunk's own thenable (rejection surfaces)`, async (_t) => {
		// A thenable object whose `then` rejects. Built via a computed key so the
		// linter's no-thenable-literal rule is not tripped; it is intentional here.
		const thenKey = "then";
		const rejecting = {
			[thenKey](_resolve, reject) {
				reject(new Error("thenable-rejected"));
			},
		};
		// Push the thenable directly (Readable.from would await/reject it before the
		// transform); piping hands the raw thenable to the default passThrough, whose
		// returned value (the chunk) is then awaited.
		const source = createReadableStream();
		process.nextTick(() => {
			source.push(rejecting);
			source.push(null);
		});
		const streams = [source, createPassThroughStream()];
		try {
			await withTimeout(pipeline(streams));
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "thenable-rejected");
		}
	});

	// --- createWritableStream final handler awaits a returned thenable. An async
	// `final` that REJECTS must propagate as a pipeline error. The thenable-check
	// mutants (`if (false)`, `typeof ... === ""`) would take the else branch and
	// resolve, swallowing the rejection. ---
	test(`createWritableStream propagates a rejecting async final`, async (_t) => {
		const streams = [
			createReadableStream(["a", "b", "c"]),
			createWritableStream(
				() => {},
				async () => {
					throw new Error("final-rejected");
				},
			),
		];
		try {
			await withTimeout(pipeline(streams));
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "final-rejected");
		}
	});

	// ===========================================================================
	// *** Stream-leak resilience ***
	// Legacy `.pipe()` only tears a chain down on `finish`; when a stream errors
	// or a consumer closes early it leaves the upstream source running, producing
	// into a dead pipeline (the classic Node.js stream leak). pipejoin must mirror
	// what pipeline()/pipelinePromise does internally: destroy EVERY stream on the
	// first error so nothing is left running. pipeline() (pipelinePromise) already
	// does this; these tests pin both behaviours so neither can regress.
	// ===========================================================================

	// pipejoin's teardown is node-build behaviour (the browser build has no
	// teardown); this pins the already-destroyed skip so it can't regress.
	nodeTest(`pipejoin teardown skips already-destroyed streams`, async (_t) => {
		const source = createReadableStream(["a", "b", "c"]);
		const failing = createTransformStream(() => {
			throw new Error("teardown-failure");
		});
		const sink = createWritableStream();
		// Count how often the already-destroyed erroring stream gets destroy()ed: Node
		// tears `failing` down on throw, so teardown's loop must SKIP it (the
		// `!stream.destroyed` guard) — it must end at exactly one destroy, not two.
		let failingDestroyCalls = 0;
		const failingDestroy = failing.destroy.bind(failing);
		failing.destroy = (error) => {
			failingDestroyCalls++;
			return failingDestroy(error);
		};
		// Destroying the streams re-emits 'error', re-entering teardown; the
		// `!stream.destroyed` guard makes the re-entry a no-op.
		pipejoin([source, failing, sink]);
		await timeout(50);
		strictEqual(
			failingDestroyCalls,
			1,
			"already-destroyed stream must not be re-destroyed",
		);
		strictEqual(source.destroyed, true);
		strictEqual(failing.destroyed, true);
		strictEqual(sink.destroyed, true);
	});

	if (variant === "node") {
		// A mid-chain error must destroy the upstream source AND the downstream sink,
		// not just the stream that threw. With bare `.pipe()` the source survives and
		// keeps producing — the leak this guards against.
		test(`pipejoin destroys every stream when one errors (no upstream leak)`, async (_t) => {
			const source = createReadableStream(["a", "b", "c"]);
			const failing = createTransformStream(() => {
				throw new Error("mid-failure");
			});
			const sink = createWritableStream();
			// The error surfaces on the (unconsumed) sink; we assert on teardown here.
			pipejoin([source, failing, sink]);
			await timeout(50);
			strictEqual(source.destroyed, true, "source must be destroyed");
			strictEqual(
				failing.destroyed,
				true,
				"failing transform must be destroyed",
			);
			strictEqual(sink.destroyed, true, "downstream sink must be destroyed");
		});

		// The error must still reach a collector consuming the joined stream, and the
		// upstream source must be torn down rather than left producing.
		test(`pipejoin surfaces the error to a collector and tears the source down`, async (_t) => {
			const source = createReadableStream(["a", "b", "c"]);
			const failing = createTransformStream((chunk, enqueue) => {
				if (chunk === "b") throw new Error("collector-failure");
				enqueue(chunk);
			});
			const joined = pipejoin([source, failing]);
			try {
				await withTimeout(streamToArray(joined));
				throw new Error("Should have thrown");
			} catch (e) {
				strictEqual(e.message, "collector-failure");
			}
			strictEqual(source.destroyed, true, "source must be destroyed on error");
		});

		// A consumer that stops reading early (break out of the async iterator) must
		// not leave the source running: the iterator protocol destroys the readable,
		// and pipejoin's teardown must propagate that to every upstream stream.
		test(`pipejoin tears the source down when the consumer stops early`, async (_t) => {
			const source = createReadableStream(["a", "b", "c", "d", "e"]);
			const passThrough = createPassThroughStream((c) => c);
			const joined = pipejoin([source, passThrough]);
			for await (const chunk of joined) {
				if (chunk === "b") break; // early exit -> iterator destroys `joined`
			}
			await timeout(50);
			strictEqual(joined.destroyed, true, "joined stream must be destroyed");
			strictEqual(
				source.destroyed,
				true,
				"source must be destroyed on early exit",
			);
		});

		// Characterization: pipeline() (pipelinePromise) already destroys the whole
		// chain on a mid-stream error. Pin it so the resilient teardown can't regress.
		test(`pipeline destroys the source when a transform errors`, async (_t) => {
			const source = createReadableStream(["a", "b", "c"]);
			const streams = [
				source,
				createTransformStream(() => {
					throw new Error("pipeline-mid-failure");
				}),
			];
			try {
				await withTimeout(pipeline(streams));
				throw new Error("Should have thrown");
			} catch (e) {
				strictEqual(e.message, "pipeline-mid-failure");
			}
			strictEqual(source.destroyed, true, "pipeline must destroy the source");
		});
	}

	// *** createWritableStream streamOptions.abort *** //
	// Same contract in both builds: abort(reason) runs once when the sink is torn
	// down before it finishes by something other than its own write/final failing
	// (an upstream/pipe error, a writer abort / destroy(), or its signal firing).
	const upstreamFailure = () => [
		createReadableStream(["a"]),
		createTransformStream(() => {
			throw new Error("upstream");
		}),
	];

	test(`createWritableStream abort runs once with the upstream error`, async () => {
		const abort = spy();
		const sink = createWritableStream(() => {}, { abort });
		await rejects(pipeline([...upstreamFailure(), sink]), {
			message: "upstream",
		});
		strictEqual(abort.callCount, 1);
		strictEqual(abort.mock.calls[0].arguments[0].message, "upstream");
	});

	test(`createWritableStream abort also works alongside a final callback`, async () => {
		const abort = spy();
		const final = spy();
		const sink = createWritableStream(() => {}, final, { abort });
		await rejects(pipeline([...upstreamFailure(), sink]), {
			message: "upstream",
		});
		strictEqual(abort.callCount, 1);
		strictEqual(final.callCount, 0);
	});

	test(`createWritableStream abort is not called on a clean finish`, async () => {
		const abort = spy();
		const final = spy();
		await pipeline([
			createReadableStream(["a", "b"]),
			createWritableStream(() => {}, final, { abort }),
		]);
		strictEqual(final.callCount, 1);
		strictEqual(abort.callCount, 0);
	});

	test(`createWritableStream abort is not called when write or final itself fails`, async () => {
		const abort = spy();
		await rejects(
			pipeline([
				createReadableStream(["a"]),
				createWritableStream(
					() => {
						throw new Error("write-failed");
					},
					{ abort },
				),
			]),
			{ message: "write-failed" },
		);
		await rejects(
			pipeline([
				createReadableStream(["a"]),
				createWritableStream(
					async () => {
						throw new Error("async-write-failed");
					},
					{ abort },
				),
			]),
			{ message: "async-write-failed" },
		);
		await rejects(
			pipeline([
				createReadableStream(["a"]),
				createWritableStream(
					() => {},
					() => {
						throw new Error("final-failed");
					},
					{ abort },
				),
			]),
			{ message: "final-failed" },
		);
		await rejects(
			pipeline([
				createReadableStream(["a"]),
				createWritableStream(
					() => {},
					async () => {
						throw new Error("async-final-failed");
					},
					{ abort },
				),
			]),
			{ message: "async-final-failed" },
		);
		strictEqual(abort.callCount, 0);
	});

	test(`createWritableStream abort runs when the sink is aborted/destroyed directly`, async () => {
		const abort = spy();
		const sink = createWritableStream(() => {}, { abort });
		// node: destroy() without an error; browser: WritableStream#abort() with no
		// reason. Both hand abort() an undefined reason.
		if (variant === "node") {
			sink.destroy();
			await new Promise((resolve) => setImmediate(resolve));
			// destroy() must still complete (its callback runs after abort()).
			strictEqual(sink.closed, true);
		} else {
			await sink.abort();
		}
		strictEqual(abort.callCount, 1);
		deepStrictEqual(abort.mock.calls[0].arguments, [undefined]);
	});

	test(`createWritableStream abort runs once when its signal fires`, async () => {
		const abort = spy();
		const controller = new AbortController();
		const sink = createWritableStream(() => {}, {
			signal: controller.signal,
			abort,
		});
		if (variant === "node") sink.on("error", () => {});
		controller.abort();
		await new Promise((resolve) => setImmediate(resolve));
		// A later teardown must not re-run it.
		if (variant === "node") sink.destroy(new Error("again"));
		else await sink.abort(new Error("again")).catch(() => {});
		strictEqual(abort.callCount, 1);
		strictEqual(abort.mock.calls[0].arguments[0].name, "AbortError");
	});

	// Node destroys a Writable built with an already-aborted signal; the browser
	// build must error it too (and run the hook) instead of accepting writes.
	test(`createWritableStream with an already-aborted signal errors and runs abort once`, async () => {
		const abort = spy();
		const write = spy();
		const controller = new AbortController();
		controller.abort();
		const sink = createWritableStream(write, {
			signal: controller.signal,
			abort,
		});
		if (variant === "node") {
			const errored = new Promise((resolve) => sink.on("error", resolve));
			strictEqual((await errored).name, "AbortError");
		} else {
			await rejects(sink.getWriter().write("a"), { name: "AbortError" });
		}
		strictEqual(write.callCount, 0);
		strictEqual(abort.callCount, 1);
		strictEqual(abort.mock.calls[0].arguments[0].name, "AbortError");
	});

	test(`createWritableStream ignores a throwing or rejecting abort callback`, async () => {
		for (const abort of [
			() => {
				throw new Error("abort-threw");
			},
			async () => {
				throw new Error("abort-rejected");
			},
		]) {
			// The sink is already failing with `reason`; abort's own failure must
			// neither replace that error nor surface as an unhandled rejection.
			await rejects(
				pipeline([
					...upstreamFailure(),
					createWritableStream(() => {}, { abort }),
				]),
				{ message: "upstream" },
			);
			const controller = new AbortController();
			const sink = createWritableStream(() => {}, {
				signal: controller.signal,
				abort,
			});
			if (variant === "node") sink.on("error", () => {});
			controller.abort();
			await new Promise((resolve) => setImmediate(resolve));
		}
	});

	test(`createWritableStream abort waits for an async abort before settling`, async () => {
		const order = [];
		const abort = async () => {
			await Promise.resolve();
			order.push("abort-done");
		};
		const sink = createWritableStream(() => {}, { abort });
		await rejects(pipeline([...upstreamFailure(), sink]), {
			message: "upstream",
		});
		order.push("pipeline-settled");
		deepStrictEqual(order, ["abort-done", "pipeline-settled"]);
	});

	// *** Major-version API: removed exports stay removed *** //
	test(`removed core exports are absent`, async () => {
		const mod = await import("@datastream/core");
		for (const name of [
			"default",
			"createReadableStreamFromString",
			"createReadableStreamFromArrayBuffer",
			"shallowClone",
			"deepClone",
			"shallowEqual",
			"deepEqual",
		]) {
			strictEqual(mod[name], undefined, name);
		}
		// Platform-unsupported: the browser build has no backpressureGauge export.
		strictEqual(
			typeof mod.backpressureGauge,
			variant === "node" ? "function" : "undefined",
		);
	});

	// Every browser stream registers an abort listener when given a signal; it
	// must be dropped on every terminal path (clean close, cancel, abort) or a
	// long-lived shared signal accumulates one listener per stream ever built.
	if (variant === "browser") {
		test(`create*Stream drop the abort listener on close, cancel and abort`, async () => {
			const controller = new AbortController();
			const { signal } = controller;
			await pipeline([
				createReadableStream(["a"], { signal }),
				createPassThroughStream(undefined, { signal }),
				createTransformStream(undefined, { signal }),
				createWritableStream(() => {}, { signal }),
			]);
			strictEqual(getEventListeners(signal, "abort").length, 0);

			for (const make of [createPassThroughStream, createTransformStream]) {
				await make(undefined, { signal }).readable.cancel();
			}
			await createWritableStream(() => {}, { signal }).abort();
			await createReadableStream(
				(async function* () {
					yield "a";
				})(),
				{ signal },
			).cancel();
			strictEqual(getEventListeners(signal, "abort").length, 0);

			const pending = createReadableStream(undefined, { signal });
			controller.abort();
			await rejects(streamToArray(pending), { name: "AbortError" });
			strictEqual(getEventListeners(signal, "abort").length, 0);
		});

		test(`createTransformStream errors when its signal aborts mid-stream`, async () => {
			const controller = new AbortController();
			const source = createReadableStream();
			const run = pipeline([
				source,
				createTransformStream(undefined, { signal: controller.signal }),
			]);
			source.push("a");
			controller.abort();
			await rejects(run, { name: "AbortError" });
		});

		test(`createReadableStream errors immediately on an already-aborted signal`, async () => {
			const signal = AbortSignal.abort();
			await rejects(streamToArray(createReadableStream(["a"], { signal })), {
				name: "AbortError",
			});
			strictEqual(getEventListeners(signal, "abort").length, 0);
		});
	}

	test(`streamTo* count a null chunk as one item against maxBufferSize`, async () => {
		const input = () => createReadableStream(["a", null, "b"]);
		deepStrictEqual(await streamToArray(input(), { maxBufferSize: 3 }), [
			"a",
			null,
			"b",
		]);
		await rejects(
			streamToArray(input(), { maxBufferSize: 2 }),
			/maxBufferSize/,
		);
		strictEqual(await streamToString(input(), { maxBufferSize: 2 }), "ab");
		deepStrictEqual(
			await streamToObject(createReadableStream([{ a: 1 }, null]), {
				maxBufferSize: 2,
			}),
			{ a: 1 },
		);
	});

	if (variant === "browser") {
		test(`streamToString flushes an incomplete trailing multibyte sequence`, async () => {
			const bytes = new TextEncoder().encode("é");
			strictEqual(
				await streamToString(createReadableStream([bytes.subarray(0, 1)])),
				"�",
			);
		});

		// chunkSize only means the readable slice size; it no longer turns into a
		// queuing-strategy size() (which made highWaterMark a byte-ish budget).
		test(`makeOptions forwards highWaterMark and ignores chunkSize in the strategies`, () => {
			const { readableStrategy, writableStrategy, signal } = makeOptions({
				highWaterMark: 2,
				chunkSize: 5,
			});
			deepStrictEqual(readableStrategy, { highWaterMark: 2 });
			deepStrictEqual(writableStrategy, { highWaterMark: 2 });
			strictEqual(signal, undefined);
			deepStrictEqual(makeOptions().readableStrategy, {
				highWaterMark: undefined,
			});
		});

		test(`createReadableStream chunks strings and bytes exactly and pulls iterables lazily`, async () => {
			deepStrictEqual(
				await streamToArray(createReadableStream("abcd", { chunkSize: 2 })),
				["ab", "cd"],
			);
			const byteChunks = await streamToArray(
				createReadableStream(Uint8Array.of(1, 2, 3, 4), { chunkSize: 2 }),
			);
			deepStrictEqual(
				byteChunks.map((chunk) => [...chunk]),
				[
					[1, 2],
					[3, 4],
				],
			);
			deepStrictEqual(
				await streamToArray(createReadableStream(new Set([1, 2]))),
				[1, 2],
			);
			// A bare iterator without return(): pulled one item at a time, cancel and
			// abort must still settle.
			const source = () => {
				let pulled = 0;
				return {
					pulled: () => pulled,
					[Symbol.asyncIterator]: () => ({
						next: async () =>
							pulled < 3
								? { value: pulled++, done: false }
								: { value: undefined, done: true },
					}),
				};
			};
			const lazy = source();
			const reader = createReadableStream(lazy, {
				highWaterMark: 1,
			}).getReader();
			deepStrictEqual(await reader.read(), { value: 0, done: false });
			ok(lazy.pulled() <= 2, `pulled ${lazy.pulled()} ahead`);
			await reader.cancel();
			const controller = new AbortController();
			const aborted = streamToArray(
				createReadableStream(source(), { signal: controller.signal }),
			);
			controller.abort();
			await rejects(aborted, { name: "AbortError" });
		});

		test(`createPassThroughStream and createWritableStream error when their signal aborts mid-stream`, async () => {
			for (const make of [
				(signal) => createPassThroughStream(undefined, { signal }),
				(signal) => createWritableStream(() => {}, { signal }),
			]) {
				const controller = new AbortController();
				const source = createReadableStream();
				const run = pipeline([source, make(controller.signal)]);
				source.push("a");
				controller.abort();
				await rejects(run, { name: "AbortError" });
			}
		});

		test(`create*Stream propagate a throwing callback and drop the abort listener`, async () => {
			const { signal } = new AbortController();
			const boom = () => {
				throw new Error("boom");
			};
			await rejects(
				pipeline([
					createReadableStream(["a"]),
					createPassThroughStream(boom, { signal }),
				]),
				/boom/,
			);
			await rejects(
				pipeline([
					createReadableStream(["a"]),
					createTransformStream(boom, { signal }),
				]),
				/boom/,
			);
			await rejects(
				pipeline([
					createReadableStream(["a"]),
					createTransformStream(undefined, boom, { signal }),
				]),
				/boom/,
			);
			await rejects(
				pipeline([
					createReadableStream(["a"]),
					createWritableStream(boom, { signal }),
				]),
				/boom/,
			);
			strictEqual(getEventListeners(signal, "abort").length, 0);
		});

		test(`createTransformStream flush can enqueue`, async () => {
			const streams = [
				createReadableStream(["a"]),
				createTransformStream(undefined, (enqueue) => enqueue("z")),
			];
			deepStrictEqual(await streamToArray(pipejoin(streams)), ["a", "z"]);
		});
	}

	// --- audit: streamToString must decode byte chunks with a streaming decoder
	// so a multibyte char split across chunks survives (was "\ufffd\ufffd" in node).
	// createReadableStream(array) exercises the .on path in node; asyncGen the
	// async-iterator path. ---
	test(`streamToString decodes a multibyte char split across byte chunks`, async (_t) => {
		const bytes = new TextEncoder().encode("é"); // 0xC3 0xA9
		const split = () => [bytes.subarray(0, 1), bytes.subarray(1)];
		strictEqual(await streamToString(createReadableStream(split())), "é");
		strictEqual(await streamToString(asyncGen(split())), "é");
	});

	// --- audit: typed-array views must stream their raw bytes (byteOffset/
	// byteLength window), not element values; node copied element-wise
	// (Uint16 -> [2,4]), a DataView yielded nothing and an empty Buffer yielded
	// one chunk. Both builds must agree. ---
	test(`createReadableStream streams the raw bytes of any typed-array view`, async (_t) => {
		const bytesOf = async (input) =>
			(await streamToArray(createReadableStream(input))).map((c) =>
				Array.from(c),
			);
		const u16 = new Uint16Array([0x0102, 0x0304]);
		const expected = Array.from(new Uint8Array(u16.buffer));
		deepStrictEqual(await bytesOf(u16), [expected]);
		const backing = new Uint8Array([0, 1, 2, 3, 0]).buffer;
		deepStrictEqual(await bytesOf(new DataView(backing, 1, 3)), [[1, 2, 3]]);
		deepStrictEqual(await bytesOf(Buffer.alloc(0)), []);
		deepStrictEqual(await bytesOf(new DataView(backing, 0, 0)), []);
	});
});
