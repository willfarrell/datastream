import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";
import {
	backpressureGuage,
	createPassThroughStream,
	createReadableStream,
	createTransformStream,
	createWritableStream,
	isReadable,
	isWritable,
	makeOptions,
	pipejoin,
	pipeline,
	streamToArray,
	streamToBuffer,
	streamToObject,
	streamToString,
	timeout,
} from "@datastream/core";
import { objectCountStream } from "@datastream/object";
import sinon from "sinon";

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

// *** backpressureGuage *** //
test(`${variant}: backpressureGuage should measure stream metrics`, async (_t) => {
	const input = ["a", "b", "c"];
	const streams = {
		readable: createReadableStream(input),
		transform: createTransformStream(),
	};

	const metrics = backpressureGuage(streams);

	deepStrictEqual(typeof metrics, "object");
	deepStrictEqual(typeof metrics.readable, "object");
	deepStrictEqual(typeof metrics.transform, "object");
	deepStrictEqual(metrics.readable.timeline, []);
	deepStrictEqual(metrics.readable.total, {});
});

test(`${variant}: backpressureGuage should track pause and resume events`, async (_t) => {
	const transform = createTransformStream();
	const streams = { transform };

	const metrics = backpressureGuage(streams);

	// Simulate pause event
	transform.emit("pause");
	// Simulate resume event (with timestamp set)
	transform.emit("resume");

	// Check that timeline was updated
	deepStrictEqual(metrics.transform.timeline.length, 1);
	strictEqual(typeof metrics.transform.timeline[0].timestamp, "number");
	strictEqual(typeof metrics.transform.timeline[0].duration, "number");
});

test(`${variant}: backpressureGuage should track resume without prior pause`, async (_t) => {
	const transform = createTransformStream();
	const streams = { transform };

	const metrics = backpressureGuage(streams);

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

test(`${variant}: createReadableStream should allow pushing values onto it`, async (_t) => {
	const streams = [createReadableStream()];
	const stream = pipejoin(streams);
	streams[0].push("a");
	streams[0].push(null);
	const output = await streamToArray(stream);

	deepStrictEqual(output, ["a"]);
});

if (variant === "node") {
	const { backpressureGuage } = await import("@datastream/core");
	test(`${variant}: backpressureGuage should chunk really long strings`, async (_t) => {
		const input = "x".repeat(1024 * 1024); // where 16*1024 is the default chunkSize/highWaterMark
		const streams = [
			createReadableStream(input),
			createPassThroughStream(async () => {
				await timeout(5);
			}),
			createWritableStream(),
		];
		const metrics = backpressureGuage(streams);

		await pipeline(streams);
		// console.log(JSON.stringify(metrics))

		deepStrictEqual(metrics["0"].timeline.length, 3);
		deepStrictEqual(metrics["1"].timeline.length, 0);
	});
}

// *** createPassThroughStream *** //
test(`${variant}: createPassThroughStream should create a pass through stream`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = sinon.spy();
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
	const transform = sinon.spy();
	const flush = sinon.spy();
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
	const transform = sinon.spy();
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
	const transform = sinon.spy();
	const flush = sinon.spy();
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
	const transform = sinon.spy();
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
	const transform = sinon.spy();
	const final = sinon.spy();
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
		const transform = sinon.spy();

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
	const transform = sinon.spy();
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
	const transform = sinon.spy();
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
	const transform = sinon.spy();
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

// Default error handler in pipejoin is tested through pipeline error tests

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
} else if (variant === "webstream") {
	// test(`${variant}: makeOptions should return interoperable structure`, async (_t) => {
	//   // Web Stream always is in object mode
	//   const options = makeOptions({ highWaterMark: 1, chunkSize: 2 })
	//   deepStrictEqual(options, {
	//     writableStrategy: {
	//       highWaterMark: 1,
	//       size: { chunk: 2 }
	//     },
	//     readableStrategy: {
	//       highWaterMark: 1,
	//       size: { chunk: 2 }
	//     },
	//     signal: undefined
	//   })
	// })
}
