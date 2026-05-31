import { deepStrictEqual, ok, strictEqual } from "node:assert";
import { EventEmitter } from "node:events";
import test, { mock } from "node:test";
import {
	backpressureGauge,
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
	createWritableStream,
	deepClone,
	deepEqual,
	isReadable,
	isWritable,
	makeOptions,
	pipejoin,
	pipeline,
	resolveLazy,
	result,
	shallowClone,
	shallowEqual,
	streamToArray,
	streamToBuffer,
	streamToObject,
	streamToString,
	timeout,
} from "@datastream/core";
import { objectCountStream } from "@datastream/object";

const spy = (impl) => {
	const fn = mock.fn(impl);
	Object.defineProperty(fn, "callCount", {
		get() {
			return fn.mock.callCount();
		},
	});
	return fn;
};

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

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
	test(`${variant}: streamToArray should work with readable ${type} stream`, async (_t) => {
		const input = types[type];
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, input);
	});

	test(`${variant}: streamToArray should work with transform ${type} stream`, async (_t) => {
		const input = types[type];
		const streams = [createReadableStream(input), createTransformStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepStrictEqual(output, input);
	});

	test(`${variant}: streamToObject should work with transform ${type} stream`, async (_t) => {
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

	test(`${variant}: streamToString should work with readable ${type} stream`, async (_t) => {
		const input = types[type];
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, input.join(""));
	});

	test(`${variant}: streamToString should work with transform ${type} stream`, async (_t) => {
		const input = types[type];
		const streams = [createReadableStream(input), createTransformStream()];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepStrictEqual(output, input.join(""));
	});
}

// *** streamTo{Array,String,Object} with async iterable (non-Node stream) *** //
test(`${variant}: streamToArray should work with async iterable`, async (_t) => {
	async function* gen() {
		yield "a";
		yield "b";
	}
	const output = await streamToArray(gen());
	deepStrictEqual(output, ["a", "b"]);
});

test(`${variant}: streamToObject should work with async iterable`, async (_t) => {
	async function* gen() {
		yield { x: 1 };
		yield { y: 2 };
	}
	const output = await streamToObject(gen());
	deepStrictEqual(output, { x: 1, y: 2 });
});

test(`${variant}: streamToString should work with async iterable`, async (_t) => {
	async function* gen() {
		yield "hello";
		yield " world";
	}
	const output = await streamToString(gen());
	strictEqual(output, "hello world");
});

test(`${variant}: streamToBuffer should work with async iterable`, async (_t) => {
	async function* gen() {
		yield "hello";
		yield " world";
	}
	const output = await streamToBuffer(gen());
	strictEqual(output.toString(), "hello world");
});

// *** streamToBuffer *** //
test(`${variant}: streamToBuffer should collect buffers into single buffer`, async (_t) => {
	const input = ["hello", " ", "world"];
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToBuffer(stream);

	deepStrictEqual(output.toString(), "hello world");
});

test(`${variant}: streamToBuffer should work with Uint8Array`, async (_t) => {
	const input = [Uint8Array.from([104, 101, 108, 108, 111])]; // "hello"
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToBuffer(stream);

	deepStrictEqual(output.toString(), "hello");
});

// The bare `@datastream/core` import resolves to the node build under both
// --conditions (Node always injects the `node` condition first), so the web
// build is only actually exercised by importing it directly. streamToBuffer
// was missing from the web build entirely; assert parity here.
test(`${variant}: web build exports a working streamToBuffer`, async (_t) => {
	const web = await import(
		`file://${new URL("./index.web.js", import.meta.url).pathname}`
	);
	strictEqual(typeof web.streamToBuffer, "function");
	const out = await web.streamToBuffer(
		web.createReadableStream(["hello", " ", "world"]),
	);
	ok(out instanceof Uint8Array);
	strictEqual(new TextDecoder().decode(out), "hello world");
});

// *** backpressureGauge *** //
test(`${variant}: backpressureGauge should measure stream metrics`, async (_t) => {
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

test(`${variant}: backpressureGauge should track pause and resume events`, async (_t) => {
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
});

test(`${variant}: backpressureGauge should track resume without prior pause`, async (_t) => {
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
});

test(`${variant}: backpressureGauge records one interval per pause/resume pair`, async (_t) => {
	const transform = createTransformStream();
	const metrics = backpressureGauge({ transform });

	// One real pause/resume pair, then a stray resume with no intervening pause.
	transform.emit("pause");
	transform.emit("resume");
	transform.emit("resume");

	// The stray resume must not record a phantom interval from the stale pause.
	strictEqual(metrics.transform.timeline.length, 1);
});

// *** createReadableStream *** //
test(`${variant}: createReadableStream should create a readable stream from string`, async (_t) => {
	const input = "abc";
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	strictEqual(isReadable(streams[0]), true);
	strictEqual(isWritable(streams[0]), false);
	deepStrictEqual(output, input);
});

test(`${variant}: createReadableStream should chunk long strings`, async (_t) => {
	const input = "x".repeat(17 * 1024); // where 16*1024 is the default chunkSize/highWaterMark
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output.length, 2);
});

test(`${variant}: createReadableStream should create a readable stream from array`, async (_t) => {
	const input = ["a", "b", "c"];
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(isReadable(streams[0]), true);
	strictEqual(isWritable(streams[0]), false);
	deepStrictEqual(output, input);
});

test(`${variant}: createReadableStream should create a readable stream from iterable`, async (_t) => {
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

test(`${variant}: createReadableStream should create a readable stream from ArrayBuffer`, async (_t) => {
	const input = new Uint8Array([1, 2, 3, 4, 5]).buffer;
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	strictEqual(output.length, 1);
	deepStrictEqual(Array.from(output[0]), [1, 2, 3, 4, 5]);
});

test(`${variant}: createReadableStream should allow pushing values onto it`, async (_t) => {
	const streams = [createReadableStream()];
	const stream = pipejoin(streams);
	streams[0].push("a");
	streams[0].push(null);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["a"]);
});

if (variant === "node") {
	const { backpressureGauge } = await import("@datastream/core");
	test(`${variant}: backpressureGauge should chunk really long strings`, async (_t) => {
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
test(`${variant}: createPassThroughStream should create a pass through stream`, async (_t) => {
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

test(`${variant}: createPassThroughStream should create a pass through stream with flush`, async (_t) => {
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

test(`${variant}: createPassThroughStream should catch transform error`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = () => {
		throw new Error("error");
	};
	const streams = [
		createReadableStream(input),
		createPassThroughStream(transform),
	];
	try {
		await pipeline(streams);
	} catch (e) {
		strictEqual(e.message, "error");
	}
});

test(`${variant}: createPassThroughStream should handle async passThrough`, async (_t) => {
	const input = ["a", "b", "c"];
	const streams = [
		createReadableStream(input),
		createPassThroughStream(async () => {}),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	deepStrictEqual(output, input);
});

test(`${variant}: createPassThroughStream should handle async flush`, async (_t) => {
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

test(`${variant}: createPassThroughStream should catch flush error`, async (_t) => {
	const input = ["a", "b", "c"];
	const flush = () => {
		throw new Error("error");
	};
	const streams = [
		createReadableStream(input),
		createPassThroughStream(() => {}, flush),
	];
	try {
		await pipeline(streams);
	} catch (e) {
		strictEqual(e.message, "error");
	}
});

// *** createTransformStream *** //
test(`${variant}: createTransformStream should create a transform stream`, async (_t) => {
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

test(`${variant}: createTransformStream should create a transform stream with flush`, async (_t) => {
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

test(`${variant}: createTransformStream should catch transform error`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = () => {
		throw new Error("error");
	};
	const streams = [
		createReadableStream(input),
		createTransformStream(transform),
	];
	try {
		await pipeline(streams);
	} catch (e) {
		strictEqual(e.message, "error");
	}
});

test(`${variant}: createTransformStream should handle async transform`, async (_t) => {
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

test(`${variant}: createTransformStream should handle async flush`, async (_t) => {
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

test(`${variant}: createTransformStream should catch flush error`, async (_t) => {
	const input = ["a", "b", "c"];
	const flush = () => {
		throw new Error("error");
	};
	const streams = [
		createReadableStream(input),
		createTransformStream(() => {}, flush),
	];
	try {
		await pipeline(streams);
	} catch (e) {
		strictEqual(e.message, "error");
	}
});

// *** createWritableStream *** //
test(`${variant}: createWritableStream should create a writable stream`, async (_t) => {
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

test(`${variant}: createWritableStream should create a writable stream with final`, async (_t) => {
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

test(`${variant}: createWritableStream should catch transform error`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = () => {
		throw new Error("error");
	};
	const streams = [
		createReadableStream(input),
		createWritableStream(transform),
	];
	try {
		await pipeline(streams);
	} catch (e) {
		strictEqual(e.message, "error");
	}
});

test(`${variant}: createWritableStream should handle async write`, async (_t) => {
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

test(`${variant}: createWritableStream should handle async final`, async (_t) => {
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

test(`${variant}: createWritableStream should catch final error`, async (_t) => {
	const input = ["a", "b", "c"];
	const final = () => {
		throw new Error("error");
	};
	const streams = [
		createReadableStream(input),
		createWritableStream(() => {}, final),
	];
	try {
		await pipeline(streams);
	} catch (e) {
		strictEqual(e.message, "error");
	}
});

// *** createBranchStream *** //
/*if (variant === "node") {
	test(`${variant}: createBranchStream should create a branch stream`, async (_t) => {
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
test(`${variant}: pipeline should add writable to end of streams array`, async (_t) => {
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
	deepStrictEqual(result, { count: 3 });
});

test(`${variant}: pipeline should throw error when promise passed in`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = spy();
	const streams = [
		createReadableStream(input),
		Promise.resolve(objectCountStream()),
		createTransformStream(transform),
	];
	try {
		await pipeline(streams);
	} catch (e) {
		strictEqual(e.message, "Promise instead of stream passed in at index 1");
	}
});

test(`${variant}: pipeline should throw error when a stream thrown an error`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = (chunk, enqueue) => {
		if (chunk === "b") throw new Error("Error");
		enqueue(chunk);
	};
	const streams = [
		createReadableStream(input),
		createTransformStream(transform),
	];
	try {
		await pipeline(streams);
		strictEqual(true, false);
	} catch (e) {
		strictEqual(e.message, "Error");
	}
});

// *** pipejoin *** //
test(`${variant}: pipejoin should throw error when promise passed in`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = spy();
	const streams = [
		createReadableStream(input),
		Promise.resolve(objectCountStream()),
		createTransformStream(transform),
	];
	try {
		for await (const item of pipejoin(streams)) {
			console.log(item);
		}
	} catch (e) {
		strictEqual(e.message, "Promise instead of stream passed in at index 1");
	}
});

// *** timeout *** //
test(`${variant}: timeout should resolve after delay`, async (_t) => {
	const start = Date.now();
	await timeout(10);
	const elapsed = Date.now() - start;
	ok(elapsed >= 9);
});

// *** result *** //
test(`${variant}: result should collect stream results`, async (_t) => {
	const stream1 = createPassThroughStream(() => {});
	stream1.result = () => ({ key: "a", value: 1 });
	const stream2 = createPassThroughStream(() => {});
	const output = await result([stream1, stream2]);
	deepStrictEqual(output, { a: 1 });
});

test(`${variant}: result should skip streams with falsy key`, async (_t) => {
	const stream1 = createPassThroughStream(() => {});
	stream1.result = () => ({ key: undefined, value: 1 });
	const output = await result([stream1]);
	deepStrictEqual(output, {});
});

// *** makeOptions *** //
if (variant === "node") {
	test(`${variant}: makeOptions should return interoperable structure`, async (_t) => {
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
test(`${variant}: timeout should clear timer when aborted`, async (_t) => {
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

test(`${variant}: timeout should reject immediately if signal already aborted`, async (_t) => {
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
test(`${variant}: streamToArray should throw when exceeding maxBufferSize`, async (_t) => {
	const stream = createReadableStream(["aaa", "bbb", "ccc"]);
	try {
		await streamToArray(stream, { maxBufferSize: 6 });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxBufferSize"));
	}
});

test(`${variant}: streamToArray should not throw when within maxBufferSize`, async (_t) => {
	const stream = createReadableStream(["aaa", "bbb"]);
	const result = await streamToArray(stream, { maxBufferSize: 6 });
	deepStrictEqual(result, ["aaa", "bbb"]);
});

test(`${variant}: streamToString should throw when exceeding maxBufferSize`, async (_t) => {
	const stream = createReadableStream(["aaa", "bbb", "ccc"]);
	try {
		await streamToString(stream, { maxBufferSize: 6 });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxBufferSize"));
	}
});

test(`${variant}: streamToObject should throw when exceeding maxBufferSize`, async (_t) => {
	const stream = createReadableStream([{ a: 1 }, { b: 2 }, { c: 3 }, { d: 4 }]);
	try {
		await streamToObject(stream, { maxBufferSize: 2 });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxBufferSize"));
	}
});

test(`${variant}: streamToBuffer should throw when exceeding maxBufferSize`, async (_t) => {
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
if (variant === "node") {
	test(`${variant}: createReadableStream should throw when queue exceeds limit`, async (_t) => {
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
}

// *** deepClone / deepEqual error paths *** //
test(`${variant}: deepClone throws for non-cloneable values`, () => {
	try {
		deepClone({ fn: () => {} });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("clone chunk"));
	}
});

test(`${variant}: deepEqual throws for circular references`, () => {
	const a = {};
	a.self = a;
	try {
		deepEqual(a, a);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("stringify chunk"));
	}
});

// *** shared helpers *** //
test(`${variant}: resolveLazy passes through values and calls thunks`, () => {
	strictEqual(resolveLazy(5), 5);
	strictEqual(resolveLazy("x"), "x");
	strictEqual(
		resolveLazy(() => 42),
		42,
	);
});

test(`${variant}: shallowClone copies own enumerable properties`, () => {
	const src = { a: 1, b: { c: 2 } };
	const copy = shallowClone(src);
	deepStrictEqual(copy, src);
	strictEqual(copy === src, false);
	strictEqual(copy.b === src.b, true);
});

test(`${variant}: deepClone deep-copies via structuredClone`, () => {
	const src = { a: 1, b: { c: 2 } };
	const copy = deepClone(src);
	deepStrictEqual(copy, src);
	strictEqual(copy.b === src.b, false);
});

test(`${variant}: shallowEqual compares own keys`, () => {
	strictEqual(shallowEqual({ a: 1 }, { a: 1 }), true);
	strictEqual(shallowEqual({ a: 1 }, { a: 2 }), false);
	strictEqual(shallowEqual({ a: 1 }, { a: 1, b: 2 }), false);
	strictEqual(shallowEqual(null, null), true);
	strictEqual(shallowEqual(null, { a: 1 }), false);
});

test(`${variant}: deepEqual via JSON serialization`, () => {
	strictEqual(deepEqual({ a: { b: 1 } }, { a: { b: 1 } }), true);
	strictEqual(deepEqual({ a: { b: 1 } }, { a: { b: 2 } }), false);
});

// *** Node NULL_SENTINEL collector regression *** //
// The documented way to flow a null through a node object-mode stream is
// enqueue(null), which createTransformStream wraps as NULL_SENTINEL. Every
// collector must unwrap it via fromSafe, not just streamToArray.
test(`${variant}: streamToArray unwraps NULL_SENTINEL from object stream`, async (_t) => {
	const streams = [
		createReadableStream(["a"]),
		createTransformStream((_chunk, enqueue) => enqueue(null)),
	];
	const output = await streamToArray(pipejoin(streams));
	deepStrictEqual(output, [null]);
});

test(`${variant}: streamToString unwraps NULL_SENTINEL from object stream`, async (_t) => {
	const streams = [
		createReadableStream(["a"]),
		createTransformStream((_chunk, enqueue) => enqueue(null)),
	];
	const output = await streamToString(pipejoin(streams));
	// fromSafe(null) -> null; "".join semantics turn null into an empty string.
	strictEqual(output, "");
});

test(`${variant}: streamToObject unwraps NULL_SENTINEL from object stream`, async (_t) => {
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

test(`${variant}: streamToBuffer unwraps NULL_SENTINEL from object stream`, async (_t) => {
	const streams = [
		createReadableStream(["a"]),
		createTransformStream((_chunk, enqueue) => {
			enqueue("hi");
			enqueue(null);
		}),
	];
	// Buffer.from(null) throws; fromSafe(null) must yield an empty buffer.
	const output = await streamToBuffer(pipejoin(streams));
	strictEqual(output.toString(), "hi");
});

// *** Node backpressureGauge writable total regression *** //
if (variant === "node") {
	test(`${variant}: backpressureGauge records total for writable sinks`, async (_t) => {
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
test(`${variant}: streamToObject does not expose own __proto__ data key`, async (_t) => {
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
// web source is exercised only by importing it directly here.
const loadWeb = () =>
	import(`file://${new URL("./index.web.js", import.meta.url).pathname}`);

test(`${variant}: web streamToString decodes byte chunks instead of comma-joining`, async (_t) => {
	const web = await loadWeb();
	async function* gen() {
		yield new TextEncoder().encode("hi");
		yield new TextEncoder().encode("!");
	}
	const output = await web.streamToString(gen());
	strictEqual(output, "hi!");
});

test(`${variant}: web streamToString decodes multibyte split across chunks`, async (_t) => {
	const web = await loadWeb();
	const bytes = new TextEncoder().encode("é"); // 2 bytes: 0xC3 0xA9
	async function* gen() {
		yield bytes.subarray(0, 1);
		yield bytes.subarray(1);
	}
	const output = await web.streamToString(gen());
	strictEqual(output, "é");
});

test(`${variant}: web streamToString still joins string chunks`, async (_t) => {
	const web = await loadWeb();
	async function* gen() {
		yield "hello";
		yield " world";
	}
	const output = await web.streamToString(gen());
	strictEqual(output, "hello world");
});

test(`${variant}: web createReadableStream honors typed-array byteOffset/byteLength`, async (_t) => {
	const web = await loadWeb();
	// A subarray view over a larger buffer must stream only its own window,
	// not the whole backing ArrayBuffer (adjacent-heap leak otherwise).
	const full = new Uint8Array([0, 0, 1, 2, 3, 0, 0]);
	const view = full.subarray(2, 5); // [1,2,3]
	const out = await web.streamToBuffer(web.createReadableStream(view));
	deepStrictEqual(Array.from(out), [1, 2, 3]);
});

test(`${variant}: web createReadableStream honors Buffer byteOffset/byteLength`, async (_t) => {
	const web = await loadWeb();
	// Node Buffers share a pooled allocation; reading the whole backing buffer
	// would leak pooled bytes and yield far more than 5 bytes.
	const buf = Buffer.from([1, 2, 3, 4, 5]);
	const out = await web.streamToBuffer(web.createReadableStream(buf));
	deepStrictEqual(Array.from(out), [1, 2, 3, 4, 5]);
});

test(`${variant}: web create*Stream remove the abort listener on error`, async (_t) => {
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

test(`${variant}: web createReadableStream errors on unsupported scalar input`, async (_t) => {
	const web = await loadWeb();
	try {
		await withTimeout(web.streamToArray(web.createReadableStream(42)));
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e instanceof TypeError, `expected TypeError, got ${e}`);
		ok(e.message.includes("unsupported input"));
	}
});

test(`${variant}: web createReadableStream errors clearly on null input`, async (_t) => {
	const web = await loadWeb();
	try {
		await withTimeout(web.streamToArray(web.createReadableStream(null)));
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e instanceof TypeError, `expected TypeError, got ${e}`);
		ok(e.message.includes("unsupported input"));
	}
});

test(`${variant}: web isReadable/isWritable return false for null/undefined`, async (_t) => {
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
	test(`${variant}: streamToArray uses the .on path for plain EventEmitters`, async (_t) => {
		const out = await streamToArray(emitterStream(["a", "b", "c"]));
		deepStrictEqual(out, ["a", "b", "c"]);
	});

	test(`${variant}: streamToString uses the .on path for plain EventEmitters`, async (_t) => {
		const out = await streamToString(emitterStream(["a", "b", "c"]));
		strictEqual(out, "abc");
	});

	test(`${variant}: streamToObject uses the .on path for plain EventEmitters`, async (_t) => {
		const out = await streamToObject(emitterStream([{ a: 1 }, { b: 2 }]));
		deepStrictEqual(out, { a: 1, b: 2 });
	});

	test(`${variant}: streamToBuffer uses the .on path for plain EventEmitters`, async (_t) => {
		const out = await streamToBuffer(
			emitterStream([Buffer.from("ab"), Buffer.from("c")]),
		);
		strictEqual(out.toString(), "abc");
	});

	// The .on path must reject on 'error' (kills deletion of stream.on("error")).
	test(`${variant}: streamToArray .on path rejects on error event`, async (_t) => {
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
test(`${variant}: streamToArray counts each non-sized chunk as 1 (?? 1 fallback)`, async (_t) => {
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

test(`${variant}: streamToArray at exact maxBufferSize boundary does not throw`, async (_t) => {
	// "aaa"+"bbb" = 6 chars; maxBufferSize 6 is the boundary and must pass.
	const out = await streamToArray(createReadableStream(["aaa", "bbb"]), {
		maxBufferSize: 6,
	});
	deepStrictEqual(out, ["aaa", "bbb"]);
});

test(`${variant}: streamToArray uses byteLength when length is absent`, async (_t) => {
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

test(`${variant}: streamToString at exact boundary passes, one over throws`, async (_t) => {
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

test(`${variant}: streamToString non-sized chunk fallback is 0, not 1`, async (_t) => {
	// streamToString uses `?? 0`: an object with no length contributes 0, so a
	// tiny maxBufferSize must NOT trip on object-mode chunks. (Kills `?? 1`.)
	const out = await streamToString(createReadableStream([{}, {}, {}]), {
		maxBufferSize: 0,
	});
	// String(...) of objects joined; just assert it did not throw.
	strictEqual(typeof out, "string");
});

test(`${variant}: streamToObject at exact boundary passes, one over throws`, async (_t) => {
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

test(`${variant}: streamToBuffer at exact boundary passes, one over throws`, async (_t) => {
	const ok6 = await streamToBuffer(
		createReadableStream([Buffer.from("aaa"), Buffer.from("bbb")]),
		{ maxBufferSize: 6 },
	);
	strictEqual(ok6.toString(), "aaabbb");
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
test(`${variant}: maxBufferSize errors carry collector name and limit`, async (_t) => {
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
		await streamToObject(createReadableStream([{ a: 1 }, { b: 2 }, { c: 3 }]), {
			maxBufferSize: 2,
		});
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("streamToObject"));
		ok(e.message.includes("2"));
	}
});

// --- async-iterable path also enforces maxBufferSize boundary (covers the
// second copy of each guard in the for-await branch). ---
const asyncGen = (chunks) =>
	(async function* () {
		for (const c of chunks) yield c;
	})();

test(`${variant}: streamToArray async path enforces boundary precisely`, async (_t) => {
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

test(`${variant}: streamToString async path enforces boundary precisely`, async (_t) => {
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

test(`${variant}: streamToObject async path enforces boundary precisely`, async (_t) => {
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

test(`${variant}: streamToBuffer async path enforces boundary precisely`, async (_t) => {
	const out = await streamToBuffer(
		asyncGen([Buffer.from("aaa"), Buffer.from("bbb")]),
		{ maxBufferSize: 6 },
	);
	strictEqual(out.toString(), "aaabbb");
	try {
		await streamToBuffer(asyncGen([Buffer.from("aaa"), Buffer.from("bbbc")]), {
			maxBufferSize: 6,
		});
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("maxBufferSize"));
	}
});

// streamToObject async byteLength fallback (Uint8Array chunks count by bytes).
test(`${variant}: streamToObject byteLength fallback counts bytes`, async (_t) => {
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

test(`${variant}: streamToArray counts a byteLength-only chunk by its byteLength`, async (_t) => {
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

test(`${variant}: streamToObject counts a byteLength-only chunk by its byteLength`, async (_t) => {
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

test(`${variant}: streamToString counts a byteLength-only chunk by its byteLength`, async (_t) => {
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

// --- NULL_SENTINEL round-trip: a transform enqueue(null) flows through as null,
// AND a literal pushed null is treated as EOF (does not appear). ---
test(`${variant}: createReadableStream array null becomes a flowing null value`, async (_t) => {
	// toSafe maps array `null` -> NULL_SENTINEL so it is not interpreted as EOF;
	// the collector's fromSafe maps it back to null.
	const out = await streamToArray(createReadableStream(["a", null, "b"]));
	deepStrictEqual(out, ["a", null, "b"]);
});

// --- sanitizeObject: own-enumerable __proto__ key must be stripped. Use a
// literal own-enumerable key via Object.defineProperty so the
// `key === "__proto__"` guard is exercised through Object.keys. ---
test(`${variant}: streamToObject strips an own-enumerable __proto__ key`, async (_t) => {
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
test(`${variant}: pipeline appends a sink for a single pure readable`, async (_t) => {
	const out = await withTimeout(
		pipeline([createReadableStream(["a", "b", "c"])]),
	);
	deepStrictEqual(out, {});
});

// --- pipeline promise guard loop must actually iterate (kills loop `false`,
// `idx >= l`, block deletion, the `typeof ... then` check, and the index in the
// message). A promise at index 2 must be reported with that index. ---
test(`${variant}: pipeline reports the index of a promise-instead-of-stream`, async (_t) => {
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

// --- pipejoin forwards stream errors to the onError callback (kills onError
// block deletion / process.nextTick block deletion and the on("error")
// string mutant). ---
test(`${variant}: pipejoin error handler receives error on the right event`, async (_t) => {
	let got;
	const streams = [
		createReadableStream(["a"]),
		createTransformStream(() => {
			throw new Error("boom2");
		}),
	];
	pipejoin(streams, (e) => {
		got = e.message;
	});
	await timeout(50);
	strictEqual(got, "boom2");
});

// pipejoin's DEFAULT onError rethrows asynchronously via process.nextTick.
// Deleting that body would silently swallow stream errors. Capture the rethrow
// through an uncaughtException listener (kills the onError default-block and the
// process.nextTick block deletions). Node only restored when done.
if (variant === "node") {
	test(`${variant}: pipejoin default onError rethrows the error`, async (_t) => {
		let caught;
		const handler = (e) => {
			caught = e.message;
		};
		// Temporarily own uncaughtException so the async rethrow is observable
		// without crashing the test process.
		const prior = process.listeners("uncaughtException");
		for (const l of prior) process.removeListener("uncaughtException", l);
		process.on("uncaughtException", handler);
		try {
			const streams = [
				createReadableStream(["a"]),
				createTransformStream(() => {
					throw new Error("default-rethrow");
				}),
			];
			const joined = pipejoin(streams); // default onError
			joined.on("error", () => {});
			joined.resume();
			await timeout(100);
			strictEqual(caught, "default-rethrow");
		} finally {
			process.removeListener("uncaughtException", handler);
			for (const l of prior) process.on("uncaughtException", l);
		}
	});
}

// --- createReadableStream branch coverage ---

// Array.isArray branch: array maps null -> sentinel so an embedded null is
// preserved as a value (raw null would be EOF). (kills branch deletion /
// `false` / `true`).
test(`${variant}: createReadableStream array branch preserves embedded null`, async (_t) => {
	const out = await streamToArray(createReadableStream([null, "x"]));
	deepStrictEqual(out, [null, "x"]);
});

// object-with-byteLength branch (ArrayBuffer) yields a single Uint8Array chunk.
test(`${variant}: createReadableStream routes ArrayBuffer to byte chunker`, async (_t) => {
	const buf = new Uint8Array([9, 8, 7]).buffer;
	const out = await streamToArray(createReadableStream(buf));
	strictEqual(out.length, 1);
	deepStrictEqual(Array.from(out[0]), [9, 8, 7]);
});

// String routing splits by chunkSize via substring (kills MethodExpression
// yield input2 and the position arithmetic).
test(`${variant}: createReadableStream chunks strings by chunkSize via substring`, async (_t) => {
	const input = "abcdefgh";
	const out = await streamToArray(
		createReadableStream(input, { chunkSize: 3 }),
	);
	deepStrictEqual(out, ["abc", "def", "gh"]);
});

test(`${variant}: createReadableStream string chunk boundary is exact`, async (_t) => {
	// length 6, chunkSize 3 -> exactly 2 full chunks, no empty trailing chunk
	// (kills while `<=` which would emit an extra empty "" at position===length).
	const out = await streamToArray(
		createReadableStream("abcdef", { chunkSize: 3 }),
	);
	deepStrictEqual(out, ["abc", "def"]);
});

// chunkSize <= 0 must throw (kills `<= 0` -> `< 0`, `false`, and message "").
test(`${variant}: createReadableStream rejects zero chunkSize for strings`, (_t) => {
	try {
		createReadableStream("abc", { chunkSize: 0 });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("positive number"));
	}
});

test(`${variant}: createReadableStream rejects negative chunkSize for strings`, (_t) => {
	try {
		createReadableStream("abc", { chunkSize: -1 });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("positive number"));
	}
});

test(`${variant}: createReadableStream rejects zero chunkSize for ArrayBuffer`, (_t) => {
	try {
		createReadableStream(new Uint8Array([1, 2, 3]).buffer, { chunkSize: 0 });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("positive number"));
	}
});

test(`${variant}: createReadableStream rejects negative chunkSize for ArrayBuffer`, (_t) => {
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
	test(`${variant}: createReadableStream chunks ArrayBuffer by chunkSize`, async (_t) => {
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
	test(`${variant}: createReadableStream allows null push even at queue limit`, async (_t) => {
		const stream = createReadableStream(undefined, { highWaterMark: 2 });
		stream.push("a");
		stream.push("b");
		// At the limit; pushing null (EOF) must NOT throw (kills `chunk !== null`
		// -> `true`, which would throw on the null push).
		stream.push(null);
		const out = await streamToArray(stream);
		deepStrictEqual(out, ["a", "b"]);
	});

	test(`${variant}: createReadableStream allows pushing up to the limit`, async (_t) => {
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
test(`${variant}: createPassThroughStream default identity passes chunks through`, async (_t) => {
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
test(`${variant}: createPassThroughStream tolerates a non-thenable return value`, async (_t) => {
	const out = await streamToArray(
		pipejoin([
			createReadableStream(["a", "b"]),
			createPassThroughStream((c) => `sync:${c}`),
		]),
	);
	deepStrictEqual(out, ["a", "b"]);
});

test(`${variant}: createTransformStream tolerates a non-thenable return value`, async (_t) => {
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

test(`${variant}: createWritableStream tolerates a non-thenable write return`, async (_t) => {
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

test(`${variant}: createPassThroughStream flush tolerates a non-thenable return`, async (_t) => {
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

test(`${variant}: createTransformStream flush tolerates a non-thenable return`, async (_t) => {
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

test(`${variant}: createWritableStream final tolerates a non-thenable return`, async (_t) => {
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
test(`${variant}: createPassThroughStream awaits a returned promise before continuing`, async (_t) => {
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

test(`${variant}: createTransformStream awaits a returned promise before continuing`, async (_t) => {
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

test(`${variant}: createWritableStream awaits a returned promise before final`, async (_t) => {
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

test(`${variant}: createPassThroughStream awaits an async flush before finishing`, async (_t) => {
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

test(`${variant}: createTransformStream awaits an async flush before finishing`, async (_t) => {
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

// --- deepClone / deepEqual preserve the original error as `cause` (kills
// ObjectLiteral -> {} which drops the cause). ---
test(`${variant}: deepClone attaches the original error as cause`, (_t) => {
	try {
		deepClone({ fn: () => {} });
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("clone chunk"));
		ok(e.cause instanceof Error, "expected a cause error");
	}
});

test(`${variant}: deepEqual attaches the original error as cause`, (_t) => {
	const a = {};
	a.self = a;
	try {
		deepEqual(a, a);
		throw new Error("Should have thrown");
	} catch (e) {
		ok(e.message.includes("stringify chunk"));
		ok(e.cause instanceof Error, "expected a cause error");
	}
});

// --- shallowEqual: the `b == null` half of the guard. shallowEqual(obj, null)
// must be false; if `|| b == null` were dropped it would call Object.keys(null)
// and throw instead of returning false. ---
test(`${variant}: shallowEqual returns false when only b is null`, (_t) => {
	strictEqual(shallowEqual({ a: 1 }, null), false);
	strictEqual(shallowEqual({ a: 1 }, undefined), false);
});

// --- timeout abort: exact error shape (kills cause/code ObjectLiteral and
// StringLiteral mutants) for BOTH the already-aborted and abort-during paths. ---
test(`${variant}: timeout already-aborted rejects with AbortError cause code`, async (_t) => {
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

test(`${variant}: timeout abort-during rejects with AbortError cause code`, async (_t) => {
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
test(`${variant}: timeout abort wins the race; later timer does not resolve`, async (_t) => {
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

test(`${variant}: timeout removes its abort listener after resolving normally`, async (_t) => {
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

test(`${variant}: timeout removes its abort listener on the abort path`, async (_t) => {
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
test(`${variant}: backpressureGauge pause/resume duration is a small subtraction`, async (_t) => {
	const transform = createTransformStream();
	const metrics = backpressureGauge({ transform });
	transform.emit("pause");
	await timeout(15);
	transform.emit("resume");
	const { duration } = metrics.transform.timeline[0];
	ok(duration >= 0, `duration should be >=0, got ${duration}`);
	// A summed (Date.now()+timestamp) value would be ~2*Date.now() (>1e12).
	ok(duration < 1e6, `duration should be small, got ${duration}`);
});

if (variant === "node") {
	// total recorded via 'finish'/'close' for writable sinks, AND the
	// double-record guard means total.timestamp is set exactly once.
	test(`${variant}: backpressureGauge records writable total exactly once`, async (_t) => {
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
	});

	// 'finish' and 'close' events are both wired (kills on("") string mutants):
	// a writable that emits only 'finish' (no 'end') must still get a total.
	test(`${variant}: backpressureGauge records total on the finish event`, async (_t) => {
		const writable = createWritableStream();
		const metrics = backpressureGauge({ writable });
		writable.emit("finish");
		strictEqual(typeof metrics.writable.total.timestamp, "number");
	});

	test(`${variant}: backpressureGauge records total on the close event`, async (_t) => {
		const writable = createWritableStream();
		const metrics = backpressureGauge({ writable });
		writable.emit("close");
		strictEqual(typeof metrics.writable.total.timestamp, "number");
	});
}

// --- pipeline derives objectMode from a trailing readable's state and pushes a
// sink built from a (non-empty) streamOptions object. With the readable-last
// case, object chunks must drain without a non-buffer-chunk error. (covers the
// isReadable branch and the streamOptions object literal.) ---
if (variant === "node") {
	test(`${variant}: pipeline drains an object-mode readable-last stream`, async (_t) => {
		const streams = [
			createReadableStream([{ a: 1 }, { b: 2 }]),
			createTransformStream((c, enqueue) => enqueue(c)),
		];
		const out = await withTimeout(pipeline(streams));
		deepStrictEqual(out, {});
	});
}
