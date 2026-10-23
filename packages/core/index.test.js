import { deepStrictEqual, ok, rejects, strictEqual, throws } from "node:assert";
import { EventEmitter, getEventListeners } from "node:events";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
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
	const nodeTest = variant === "node" ? test : test.skip;
	// Node-only export (the browser build has no backpressureGauge at all), so it
	// can't be a static named import shared by both builds.
	const { backpressureGauge } = await import("@datastream/core");
	const text = (bytes) => new TextDecoder().decode(bytes);

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

	test(`streamToBuffer should collect buffers into single buffer`, async (_t) => {
		const input = ["hello", " ", "world"];
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToBuffer(stream);

		deepStrictEqual(text(output), "hello world");
	});

	test(`streamToBuffer should work with Uint8Array`, async (_t) => {
		const input = [Uint8Array.from([104, 101, 108, 108, 111])];
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToBuffer(stream);

		deepStrictEqual(text(output), "hello");
	});

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

	nodeTest(
		`streamToBuffer rejects on a chunk Buffer.from cannot convert`,
		async (_t) => {
			await rejects(streamToBuffer(createReadableStream([1, 2])), {
				code: "ERR_INVALID_ARG_TYPE",
			});
		},
	);

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

			transform.emit("pause");
			transform.emit("resume");

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

			transform.emit("resume");

			strictEqual(typeof metrics.transform.total.timestamp, "undefined");

			transform.emit("end");

			strictEqual(typeof metrics.transform.total.timestamp, "number");
			strictEqual(typeof metrics.transform.total.duration, "number");
		},
	);

	nodeTest(
		`backpressureGauge records one interval per pause/resume pair`,
		async (_t) => {
			const transform = createTransformStream();
			const metrics = backpressureGauge({ transform });

			transform.emit("pause");
			transform.emit("resume");
			transform.emit("resume");

			strictEqual(metrics.transform.timeline.length, 1);
		},
	);

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
		const input = "x".repeat(17 * 1024);
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
		const source = createReadableStream();
		const outputPromise = streamToArray(source);
		process.nextTick(() => {
			source.push("x");
			source.push(null);
		});
		const output = await outputPromise;
		deepStrictEqual(output, ["x"]);
	});

	for (const [name, collect] of Object.entries({
		streamToArray,
		streamToObject,
		streamToString,
		streamToBuffer,
	})) {
		nodeTest(
			`${name} rejects with ERR_STREAM_PREMATURE_CLOSE on destroy() without error`,
			async (_t) => {
				const source = new Readable({ objectMode: true, read() {} });
				source.push("a");
				const outputPromise = collect(source);
				setImmediate(() => source.destroy());
				await rejects(outputPromise, {
					message: "Premature close",
					code: "ERR_STREAM_PREMATURE_CLOSE",
				});
			},
		);
	}

	test(`createReadableStream passes null through from a sync iterable`, async (_t) => {
		const input = function* () {
			yield 1;
			yield null;
			yield 2;
		};
		const output = await streamToArray(createReadableStream(input()));
		deepStrictEqual(output, [1, null, 2]);
	});

	test(`createReadableStream passes null through from an async iterable`, async (_t) => {
		const input = async function* () {
			yield 1;
			yield null;
			yield 2;
		};
		const output = await streamToArray(createReadableStream(input()));
		deepStrictEqual(output, [1, null, 2]);
	});

	nodeTest(
		`createReadableStream throws synchronously for a non-iterable`,
		async (_t) => {
			throws(() => createReadableStream(42), { code: "ERR_INVALID_ARG_TYPE" });
		},
	);

	test(`createReadableStream push is a no-op after push(null)`, async (_t) => {
		const source = createReadableStream();
		source.push("a");
		source.push(null);
		source.push(null);
		const output = await streamToArray(source);
		source.push("b");
		deepStrictEqual(output, ["a"]);
	});

	test(`createReadableStream push is a no-op after abort`, async (_t) => {
		const controller = new AbortController();
		const source = createReadableStream(undefined, {
			signal: controller.signal,
		});
		const outputPromise = streamToArray(source);
		controller.abort(new Error("stop"));
		source.push("a");
		source.push(null);
		await rejects(outputPromise);
	});

	test(`createReadableStream push is a no-op after cancel`, async (_t) => {
		const source = createReadableStream();
		if (variant === "node") source.destroy();
		else await source.cancel();
		source.push("a");
		source.push(null);
	});

	if (variant === "node") {
		const { backpressureGauge } = await import("@datastream/core");
		test(`backpressureGauge should chunk really long strings`, async (_t) => {
			const input = "x".repeat(1024 * 1024);
			const streams = [
				createReadableStream(input),
				createPassThroughStream(async () => {
					await timeout(5);
				}),
				createWritableStream(),
			];
			const metrics = backpressureGauge(streams);

			await pipeline(streams);

			deepStrictEqual(metrics["0"].timeline.length, 3);
			deepStrictEqual(metrics["1"].timeline.length, 0);
		});
	}

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

	test(`pipejoin should throw error when promise passed in`, async (_t) => {
		const input = ["a", "b", "c"];
		const transform = spy();
		const streams = [
			createReadableStream(input),
			Promise.resolve(objectCountStream()),
			createTransformStream(transform),
		];
		throws(() => pipejoin(streams), {
			message: "Promise instead of stream passed in at index 1",
		});
	});

	test(`timeout should resolve after delay`, async (t) => {
		t.mock.timers.enable({ apis: ["setTimeout"] });
		let resolved = false;
		const pending = timeout(10).then(() => {
			resolved = true;
		});
		t.mock.timers.tick(9);
		await Promise.resolve();
		strictEqual(resolved, false);
		t.mock.timers.tick(1);
		await Promise.resolve();
		strictEqual(resolved, true);
		await pending;
	});

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

	test(`timeout should clear timer when aborted`, async (_t) => {
		const timers = () =>
			process.getActiveResourcesInfo().filter((r) => r === "Timeout").length;
		const before = timers();
		const controller = new AbortController();
		const promise = timeout(60_000, { signal: controller.signal });
		strictEqual(timers(), before + 1);
		controller.abort();
		try {
			await promise;
			throw new Error("Should have thrown");
		} catch (e) {
			strictEqual(e.message, "Aborted");
			deepStrictEqual(e.cause, { code: "AbortError" });
		}
		strictEqual(timers(), before);
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
		strictEqual(flush(), "");
	});

	test(`createChunkDecoder passes strings through and handles empty chunks`, () => {
		const { decode, flush } = createChunkDecoder();
		strictEqual(decode(new Uint8Array([0xc3])), "");
		strictEqual(decode(new Uint8Array(0)), "");
		strictEqual(decode(""), "");
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
		const output = await streamToBuffer(pipejoin(streams));
		strictEqual(text(output), "hi");
	});

	if (variant === "node") {
		test(`backpressureGauge records total for writable sinks`, async (_t) => {
			const writable = createWritableStream();
			const streams = {
				readable: createReadableStream(["a", "b", "c"]),
				writable,
			};
			const metrics = backpressureGauge(streams);
			await pipeline(Object.values(streams));
			strictEqual(typeof metrics.writable.total.timestamp, "number");
			strictEqual(typeof metrics.writable.total.duration, "number");
		});
	}

	test(`streamToObject does not expose own __proto__ data key`, async (_t) => {
		const evil = JSON.parse('{"__proto__":{"polluted":true},"safe":1}');
		const streams = [
			createReadableStream(["a"]),
			createTransformStream((_chunk, enqueue) => enqueue(evil)),
		];
		const output = await streamToObject(pipejoin(streams));
		strictEqual(Object.hasOwn(output, "__proto__"), false);
		strictEqual(output.safe, 1);
		strictEqual({}.polluted, undefined);
	});

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
		const bytes = new TextEncoder().encode("é");
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

	test(`streamToString decodes bare ArrayBuffer chunks`, async (_t) => {
		const ab = new TextEncoder().encode("hi").buffer;
		strictEqual(await streamToString(createReadableStream([ab])), "hi");
	});

	test(`web createReadableStream honors typed-array byteOffset/byteLength`, async (_t) => {
		const web = await loadWeb();
		const full = new Uint8Array([0, 0, 1, 2, 3, 0, 0]);
		const view = full.subarray(2, 5);
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
		ok(removed >= 2, `expected >=2 abort listener removals, got ${removed}`);
	});

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
		strictEqual(web.isReadable(42), false);
		strictEqual(web.isWritable("x"), false);
	});

	const emitterStream = (chunks) => {
		const ee = new EventEmitter();
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

	test(`streamToArray counts each non-sized chunk as 1 (?? 1 fallback)`, async (_t) => {
		const ok3 = await streamToArray(createReadableStream([{}, {}, {}]), {
			maxBufferSize: 3,
		});
		strictEqual(ok3.length, 3);
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
		const out = await streamToArray(createReadableStream(["aaa", "bbb"]), {
			maxBufferSize: 6,
		});
		deepStrictEqual(out, ["aaa", "bbb"]);
	});

	test(`streamToArray uses byteLength when length is absent`, async (_t) => {
		const chunk = Uint8Array.from([1, 2, 3, 4, 5]);
		try {
			await streamToArray(createReadableStream([chunk]), { maxBufferSize: 4 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
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
		const out = await streamToString(createReadableStream([{}, {}, {}]), {
			maxBufferSize: 0,
		});
		strictEqual(typeof out, "string");
	});

	test(`streamToObject at exact boundary passes, one over throws`, async (_t) => {
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

	test(`streamToObject byteLength fallback counts bytes`, async (_t) => {
		const chunk = Uint8Array.from([1, 2, 3, 4, 5]);
		try {
			await streamToObject(createReadableStream([chunk]), { maxBufferSize: 4 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
	});

	const byteLenChunk = (n, extra = {}) => ({ byteLength: n, ...extra });

	test(`streamToArray counts a byteLength-only chunk by its byteLength`, async (_t) => {
		try {
			await streamToArray(createReadableStream([byteLenChunk(5)]), {
				maxBufferSize: 4,
			});
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
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

	test(`streamToArray async path ?? 1 fallback counts no-size chunks`, async (_t) => {
		try {
			await streamToArray(asyncGen([{}, {}, {}, {}]), { maxBufferSize: 3 });
			throw new Error("Should have thrown");
		} catch (e) {
			ok(e.message.includes("maxBufferSize"));
		}
		const out = await streamToArray(asyncGen([{}, {}, {}]), {
			maxBufferSize: 3,
		});
		strictEqual(out.length, 3);
	});

	test(`streamToString async path ?? 0 fallback does not count no-size chunks`, async (_t) => {
		const out = await streamToString(asyncGen([{}, {}]), { maxBufferSize: 0 });
		strictEqual(typeof out, "string");
	});

	nodeTest(
		`streamToBuffer async path handles null-sentinel chunk`,
		async (_t) => {
			const NULL_SENTINEL = Symbol.for("@datastream/null");
			async function* gen() {
				yield "hi";
				yield NULL_SENTINEL;
			}
			const out = await streamToBuffer(gen());
			strictEqual(out.toString(), "hi");
		},
	);

	test(`createReadableStream array null becomes a flowing null value`, async (_t) => {
		const out = await streamToArray(createReadableStream(["a", null, "b"]));
		deepStrictEqual(out, ["a", null, "b"]);
	});

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
		strictEqual(Object.getPrototypeOf(out), Object.prototype);
		strictEqual(out.polluted, undefined);
	});

	test(`pipeline appends a sink for a single pure readable`, async (_t) => {
		const out = await withTimeout(
			pipeline([createReadableStream(["a", "b", "c"])]),
		);
		deepStrictEqual(out, {});
	});

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

	test(`createReadableStream array branch preserves embedded null`, async (_t) => {
		const out = await streamToArray(createReadableStream([null, "x"]));
		deepStrictEqual(out, [null, "x"]);
	});

	test(`createReadableStream routes ArrayBuffer to byte chunker`, async (_t) => {
		const buf = new Uint8Array([9, 8, 7]).buffer;
		const out = await streamToArray(createReadableStream(buf));
		strictEqual(out.length, 1);
		deepStrictEqual(Array.from(out[0]), [9, 8, 7]);
	});

	test(`createReadableStream chunks strings by chunkSize via substring`, async (_t) => {
		const input = "abcdefgh";
		const out = await streamToArray(
			createReadableStream(input, { chunkSize: 3 }),
		);
		deepStrictEqual(out, ["abc", "def", "gh"]);
	});

	test(`createReadableStream string chunk boundary is exact`, async (_t) => {
		const out = await streamToArray(
			createReadableStream("abcdef", { chunkSize: 3 }),
		);
		deepStrictEqual(out, ["abc", "def"]);
	});

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

	if (variant === "node") {
		test(`createReadableStream allows null push even at queue limit`, async (_t) => {
			const stream = createReadableStream(undefined, { highWaterMark: 2 });
			stream.push("a");
			stream.push("b");
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

	test(`createPassThroughStream default identity passes chunks through`, async (_t) => {
		const out = await streamToArray(
			pipejoin([createReadableStream(["a", "b"]), createPassThroughStream()]),
		);
		deepStrictEqual(out, ["a", "b"]);
	});

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
		strictEqual(removedAbort, 1);
	});

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
			ok(duration < 1e6, `duration should be small, got ${duration}`);
		},
	);

	nodeTest(
		`backpressureGauge records writable total exactly once`,
		async (_t) => {
			const writable = createWritableStream();
			const metrics = backpressureGauge({ writable });
			writable.emit("finish");
			const first = metrics.writable.total.timestamp;
			const firstDuration = metrics.writable.total.duration;
			strictEqual(typeof first, "number");
			await timeout(30);
			writable.emit("close");
			writable.emit("end");
			strictEqual(metrics.writable.total.timestamp, first);
			strictEqual(metrics.writable.total.duration, firstDuration);
			ok(metrics.writable.total.duration >= 0);
			ok(metrics.writable.total.duration < 1e6);
		},
	);

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

	test(`pipeline drains an object-mode readable-last stream`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1 }, { b: 2 }]),
			createTransformStream((c, enqueue) => enqueue(c)),
		];
		const out = await withTimeout(pipeline(streams));
		deepStrictEqual(out, {});
	});

	if (variant === "node") {
		test(`pipeline forwards a streamOptions signal to the readable-last sink`, async (_t) => {
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

	test(`createReadableStream streams a non-array iterable (Set) input`, async (_t) => {
		const input = new Set(["a", "b", "c"]);
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);
		deepStrictEqual(output, ["a", "b", "c"]);
	});

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

	test(`createReadableStream string stops exactly at the end (no trailing empty chunk)`, async (_t) => {
		const stream = createReadableStream("abcdefgh", { chunkSize: 4 });
		const output = await streamToArray(stream);
		deepStrictEqual(output, ["abcd", "efgh"]);
	});

	test(`createReadableStream ArrayBuffer stops exactly at the end (no trailing empty chunk)`, async (_t) => {
		const input = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]).buffer;
		const stream = createReadableStream(input, { chunkSize: 4 });
		const output = await streamToArray(stream);
		strictEqual(output.length, 2);
		deepStrictEqual(Array.from(output[0]), [1, 2, 3, 4]);
		deepStrictEqual(Array.from(output[1]), [5, 6, 7, 8]);
	});

	test(`createReadableStream abort calls the source iterator's return()`, async (_t) => {
		const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
		let cleaned = false;
		async function* gen() {
			try {
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
		const u16 = new Uint16Array([0x0102]);
		const [chunk] = await streamToArray(createReadableStream(u16));
		deepStrictEqual(Array.from(chunk), Array.from(new Uint8Array(u16.buffer)));
	});

	test(`createPassThroughStream default forwards the chunk's own thenable (rejection surfaces)`, async (_t) => {
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

	nodeTest(`pipejoin teardown skips already-destroyed streams`, async (_t) => {
		const source = createReadableStream(["a", "b", "c"]);
		const failing = createTransformStream(() => {
			throw new Error("teardown-failure");
		});
		const sink = createWritableStream();
		let failingDestroyCalls = 0;
		const failingDestroy = failing.destroy.bind(failing);
		failing.destroy = (error) => {
			failingDestroyCalls++;
			return failingDestroy(error);
		};
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
		test(`pipejoin destroys every stream when one errors (no upstream leak)`, async (_t) => {
			const source = createReadableStream(["a", "b", "c"]);
			const failing = createTransformStream(() => {
				throw new Error("mid-failure");
			});
			const sink = createWritableStream();
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

		test(`pipejoin tears the source down when the consumer stops early`, async (_t) => {
			const source = createReadableStream(["a", "b", "c", "d", "e"]);
			const passThrough = createPassThroughStream((c) => c);
			const joined = pipejoin([source, passThrough]);
			for await (const chunk of joined) {
				if (chunk === "b") break;
			}
			await timeout(50);
			strictEqual(joined.destroyed, true, "joined stream must be destroyed");
			strictEqual(
				source.destroyed,
				true,
				"source must be destroyed on early exit",
			);
		});

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
		if (variant === "node") {
			sink.destroy();
			await new Promise((resolve) => setImmediate(resolve));
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
		if (variant === "node") sink.destroy(new Error("again"));
		else await sink.abort(new Error("again")).catch(() => {});
		strictEqual(abort.callCount, 1);
		strictEqual(abort.mock.calls[0].arguments[0].name, "AbortError");
	});

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

	const withoutUncaught = async (fn) => {
		const caught = spy();
		const events = ["uncaughtException", "unhandledRejection"];
		const prior = events.map((event) => process.listeners(event));
		events.forEach((event, i) => {
			for (const l of prior[i]) process.removeListener(event, l);
			process.on(event, caught);
		});
		try {
			await fn();
			await new Promise((resolve) => setTimeout(resolve, 30));
			strictEqual(caught.callCount, 0);
		} finally {
			events.forEach((event, i) => {
				process.removeListener(event, caught);
				for (const l of prior[i]) process.on(event, l);
			});
		}
	};
	const abortedBy = (reason) => (e) => {
		if (variant === "node") {
			strictEqual(e.name, "AbortError");
			strictEqual(e.cause, reason);
		} else {
			strictEqual(e, reason);
		}
		return true;
	};

	nodeTest(
		`pipeline rejects promptly when a stream was already destroyed without an error`,
		async () => {
			const source = createReadableStream();
			source.push(null);
			source.destroy();
			let wrote = false;
			const sink = createWritableStream(() => {
				wrote = true;
			});
			const settled = await Promise.race([
				pipeline([source, sink]).then(
					() => "resolved",
					(error) => `${error.code}: ${error.message}`,
				),
				new Promise((resolve) => setTimeout(resolve, 1000, "hung")),
			]);
			strictEqual(settled, "ERR_STREAM_PREMATURE_CLOSE: Premature close");
			strictEqual(sink.destroyed, true);
			strictEqual(wrote, false);
		},
	);

	test(`pipeline rejects with the abort for streams built with an already-aborted signal`, async () => {
		const hooks = [
			undefined,
			() => {},
			async () => {
				await new Promise((resolve) => setTimeout(resolve, 5));
			},
		];
		for (const hook of hooks) {
			await withoutUncaught(async () => {
				const reason = new Error("stop");
				const controller = new AbortController();
				controller.abort(reason);
				const { signal } = controller;
				const order = [];
				const abort =
					hook &&
					spy(async () => {
						await hook();
						order.push("abort-done");
					});
				await rejects(
					pipeline([
						createReadableStream(["a"]),
						createWritableStream(() => {}, { signal, abort }),
					]),
					abortedBy(reason),
				);
				order.push("pipeline-settled");
				if (abort) {
					strictEqual(abort.callCount, 1);
					if (variant === "node") {
						deepStrictEqual(order, ["abort-done", "pipeline-settled"]);
					}
				}
			});
		}
	});

	test(`pipeline rejects with the abort for an already-aborted transform or streamOptions.signal`, async () => {
		await withoutUncaught(async () => {
			const reason = new Error("stop");
			const controller = new AbortController();
			controller.abort(reason);
			const { signal } = controller;
			const write = spy();
			const source = createReadableStream(["a"]);
			await rejects(
				pipeline([
					source,
					createTransformStream(undefined, { signal }),
					createPassThroughStream(() => {}, { signal }),
					createWritableStream(write, { signal }),
				]),
				abortedBy(reason),
			);
			strictEqual(write.callCount, 0);
			if (variant === "node") strictEqual(source.destroyed, true);
			await rejects(
				pipeline([
					new ReadableStream({
						pull(c) {
							c.enqueue("a");
						},
					}),
					createWritableStream(write, { signal }),
				]),
				abortedBy(reason),
			);
			strictEqual(write.callCount, 0);
			await rejects(
				pipeline([createReadableStream(["a"])], { signal }),
				abortedBy(reason),
			);
		});
	});

	test(`pipeline rejects with the abort when the signal fires mid-stream with an async hook`, async () => {
		await withoutUncaught(async () => {
			const reason = new Error("stop");
			const controller = new AbortController();
			const abort = spy(async () => {
				await new Promise((resolve) => setTimeout(resolve, 5));
			});
			const run = pipeline([
				createReadableStream(["a"]),
				createWritableStream(() => controller.abort(reason), {
					signal: controller.signal,
					abort,
				}),
			]);
			await rejects(run, abortedBy(reason));
			strictEqual(abort.callCount, 1);
		});
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
		strictEqual(
			typeof mod.backpressureGauge,
			variant === "node" ? "function" : "undefined",
		);
	});

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

		const abortable = {
			createPassThroughStream: (signal) =>
				createPassThroughStream(undefined, { signal }),
			createTransformStream: (signal) =>
				createTransformStream(undefined, { signal }),
			createWritableStream: (signal) =>
				createWritableStream(() => {}, { signal }),
		};
		for (const [name, make] of Object.entries(abortable)) {
			test(`${name} errors when its signal aborts while idle`, async () => {
				const controller = new AbortController();
				const source = createReadableStream();
				const run = pipeline([source, make(controller.signal)]);
				source.push("a");
				await timeout(10);
				controller.abort();
				await rejects(run, { name: "AbortError" });
			});

			test(`${name} errors on an already-aborted signal while idle`, async () => {
				const signal = AbortSignal.abort();
				await rejects(pipeline([createReadableStream(), make(signal)]), {
					name: "AbortError",
				});
			});
		}

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

	test(`streamToString decodes a multibyte char split across byte chunks`, async (_t) => {
		const bytes = new TextEncoder().encode("é");
		const split = () => [bytes.subarray(0, 1), bytes.subarray(1)];
		strictEqual(await streamToString(createReadableStream(split())), "é");
		strictEqual(await streamToString(asyncGen(split())), "é");
	});

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

describe(`@datastream/core createReadableStream pull loop (${variant})`, () => {
	const nodeTest = variant === "node" ? test : test.skip;
	const browserTest = variant === "browser" ? test : test.skip;

	nodeTest(
		`createReadableStream never calls an async iterator's next() concurrently`,
		async (_t) => {
			let pending = false;
			let pulled = 0;
			const source = {
				[Symbol.asyncIterator]: () => ({
					next: () => {
						if (pending) throw new Error("concurrent next()");
						pending = true;
						return new Promise((resolve) =>
							setImmediate(() => {
								pending = false;
								resolve(
									pulled < 20
										? { value: pulled++, done: false }
										: { value: undefined, done: true },
								);
							}),
						);
					},
				}),
			};
			const out = await streamToArray(
				createReadableStream(source, { highWaterMark: 4 }),
			);
			deepStrictEqual(
				out,
				Array.from({ length: 20 }, (_, i) => i),
			);
		},
	);

	nodeTest(
		`createReadableStream awaits promise values from a sync iterable in order`,
		async (_t) => {
			const out = await streamToArray(
				createReadableStream([Promise.resolve("a"), "b", Promise.resolve("c")]),
			);
			deepStrictEqual(out, ["a", "b", "c"]);
		},
	);

	nodeTest(
		`createReadableStream errors when a promise value from a sync iterable rejects`,
		async (_t) => {
			await rejects(
				streamToArray(
					createReadableStream(["a", Promise.reject(new Error("boom"))]),
				),
				{ message: "boom" },
			);
		},
	);

	nodeTest(
		`createReadableStream pulls a sync iterable up to highWaterMark per read() and no further`,
		async (_t) => {
			let ticked = false;
			const ticks = [];
			let pulled = 0;
			const source = {
				[Symbol.iterator]: () => ({
					next: () => {
						ticks.push(ticked);
						ticked = false;
						process.nextTick(() => {
							ticked = true;
						});
						return pulled < 8
							? { value: pulled++, done: false }
							: { value: undefined, done: true };
					},
				}),
			};
			const stream = createReadableStream(source, { highWaterMark: 4 });
			strictEqual(stream.read(), 0);
			strictEqual(pulled, 4);
			deepStrictEqual(ticks, [false, false, false, false]);
			stream.destroy();

			pulled = 0;
			const lazy = createReadableStream(source);
			strictEqual(lazy.read(), 0);
			strictEqual(pulled, 1);
			lazy.destroy();
		},
	);

	browserTest(
		`createReadableStream pulls a sync iterable up to highWaterMark per pull()`,
		async (_t) => {
			let ticked = false;
			const ticks = [];
			let pulled = 0;
			const source = {
				[Symbol.iterator]: () => ({
					next: () => {
						ticks.push(ticked);
						ticked = false;
						queueMicrotask(() => {
							ticked = true;
						});
						return pulled < 8
							? { value: pulled++, done: false }
							: { value: undefined, done: true };
					},
				}),
			};
			const stream = createReadableStream(source, { highWaterMark: 2 });
			await new Promise((resolve) => setImmediate(resolve));
			strictEqual(pulled, 2);
			deepStrictEqual(ticks, [false, false]);
			const reader = stream.getReader();
			deepStrictEqual(await reader.read(), { value: 0, done: false });
			await new Promise((resolve) => setImmediate(resolve));
			strictEqual(pulled, 3);
			deepStrictEqual(ticks, [false, false, true]);
			await reader.cancel();
		},
	);

	browserTest(
		`createReadableStream ends a sync iterable cleanly with chunks still queued`,
		async (_t) => {
			deepStrictEqual(
				await streamToArray(
					createReadableStream(["a", "b", "c"], { highWaterMark: 2 }),
				),
				["a", "b", "c"],
			);
		},
	);

	test(`createReadableStream errors and removes its abort listener when a sync source throws`, async (_t) => {
		const controller = new AbortController();
		const source = (function* () {
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

	nodeTest(
		`createReadableStream destroy() runs a sync iterator's return() before close`,
		async (_t) => {
			let cleaned = false;
			function* gen() {
				try {
					for (;;) yield "a";
				} finally {
					cleaned = true;
				}
			}
			const stream = createReadableStream(gen());
			strictEqual(stream.read(), "a");
			stream.destroy();
			await new Promise((resolve) => stream.on("close", resolve));
			strictEqual(cleaned, true);

			const bare = createReadableStream({
				[Symbol.iterator]: () => ({
					next: () => ({ value: "b", done: false }),
				}),
			});
			strictEqual(bare.read(), "b");
			bare.destroy();
			await new Promise((resolve) => bare.on("close", resolve));
			strictEqual(bare.errored, null);
		},
	);

	nodeTest(
		`createReadableStream destroy() keeps its own error over a failing return()`,
		async (_t) => {
			const gen = () => ({
				[Symbol.iterator]() {
					return this;
				},
				next: () => ({ value: "a", done: false }),
				return: () => {
					throw new Error("cleanup-failed");
				},
			});
			const errored = createReadableStream(gen());
			strictEqual(errored.read(), "a");
			errored.destroy(new Error("destroy-error"));
			await rejects(finished(errored), { message: "destroy-error" });

			const clean = createReadableStream(gen());
			strictEqual(clean.read(), "a");
			clean.destroy();
			await rejects(finished(clean), { message: "cleanup-failed" });
		},
	);

	test(`createReadableStream resumes a sync iterable after backpressure`, async (_t) => {
		const input = Array.from({ length: 1000 }, (_, i) => i);
		const seen = [];
		await pipeline([
			createReadableStream(input, { highWaterMark: 2 }),
			createTransformStream(
				async (chunk, enqueue) => {
					await new Promise((resolve) => setImmediate(resolve));
					enqueue(chunk);
				},
				{ highWaterMark: 1 },
			),
			createTransformStream((chunk, enqueue) => {
				seen.push(chunk);
				enqueue(chunk);
			}),
		]);
		deepStrictEqual(seen, input);
	});
});
