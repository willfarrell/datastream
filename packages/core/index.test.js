import { deepEqual, equal } from "node:assert";
import test from "node:test";
import {
	createBranchStream,
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

		deepEqual(output, input);
	});

	test(`${variant}: streamToArray should work with transform ${type} stream`, async (_t) => {
		const input = types[type];
		const streams = [createReadableStream(input), createTransformStream()];
		const stream = pipejoin(streams);
		const output = await streamToArray(stream);

		deepEqual(output, input);
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

		deepEqual(output, { [type]: input[input.length - 1] });
	});

	test(`${variant}: streamToString should work with readable ${type} stream`, async (_t) => {
		const input = types[type];
		const streams = [createReadableStream(input)];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepEqual(output, input.join(""));
	});

	test(`${variant}: streamToString should work with transform ${type} stream`, async (_t) => {
		const input = types[type];
		const streams = [createReadableStream(input), createTransformStream()];
		const stream = pipejoin(streams);
		const output = await streamToString(stream);

		deepEqual(output, input.join(""));
	});
}

// *** createReadableStream *** //
test(`${variant}: createReadableStream should create a readable stream from string`, async (_t) => {
	const input = "abc";
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToString(stream);

	equal(isReadable(streams[0]), true);
	equal(isWritable(streams[0]), false);
	deepEqual(output, input);
});

test(`${variant}: createReadableStream should chunk long strings`, async (_t) => {
	const input = "x".repeat(17 * 1024); // where 16*1024 is the default chunkSize/highWaterMark
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	equal(output.length, 2);
});

test(`${variant}: createReadableStream should create a readable stream from array`, async (_t) => {
	const input = ["a", "b", "c"];
	const streams = [createReadableStream(input)];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	equal(isReadable(streams[0]), true);
	equal(isWritable(streams[0]), false);
	deepEqual(output, input);
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

	equal(isReadable(streams[0]), true);
	equal(isWritable(streams[0]), false);
	deepEqual(output, ["a", "b", "c"]);
});

test(`${variant}: createReadableStream should allow pushing values onto it`, async (_t) => {
	const streams = [createReadableStream()];
	const stream = pipejoin(streams);
	streams[0].push("a");
	streams[0].push(null);
	const output = await streamToArray(stream);

	deepEqual(output, ["a"]);
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

		deepEqual(metrics["0"].timeline.length, 3);
		deepEqual(metrics["1"].timeline.length, 0);
	});
}

// *** createPassThroughStream *** //
test(`${variant}: createPassThroughStream should create a passs through stream`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = sinon.spy();
	const streams = [
		createReadableStream(input),
		createPassThroughStream(transform, {}),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	equal(isReadable(streams[1]), true);
	equal(isWritable(streams[1]), true);
	equal(transform.callCount, 3);
	deepEqual(output, input);
});

test(`${variant}: createPassThroughStream should create a passs through stream with flush`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = sinon.spy();
	const flush = sinon.spy();
	const streams = [
		createReadableStream(input),
		createPassThroughStream(transform, flush, {}),
	];
	const stream = pipejoin(streams);
	const output = await streamToArray(stream);

	equal(isReadable(streams[1]), true);
	equal(isWritable(streams[1]), true);
	equal(transform.callCount, 3);
	equal(flush.callCount, 1);
	deepEqual(output, input);
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
		equal(e.message, "error");
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
		equal(e.message, "error");
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

	equal(isReadable(streams[1]), true);
	equal(isWritable(streams[1]), true);
	equal(transform.callCount, 3);
	deepEqual(output, []);
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

	equal(isReadable(streams[1]), true);
	equal(isWritable(streams[1]), true);
	equal(transform.callCount, 3);
	equal(flush.callCount, 1);
	deepEqual(output, []);
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
		equal(e.message, "error");
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
		equal(e.message, "error");
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

	equal(isReadable(streams[1]), false);
	equal(isWritable(streams[1]), true);

	const result = await pipeline(streams);

	equal(transform.callCount, 3);
	deepEqual(result, {});
});

test(`${variant}: createWritableStream should create a writable stream with final`, async (_t) => {
	const input = ["a", "b", "c"];
	const transform = sinon.spy();
	const final = sinon.spy();
	const streams = [
		createReadableStream(input),
		createWritableStream(transform, final, {}),
	];

	equal(isReadable(streams[1]), false);
	equal(isWritable(streams[1]), true);

	const result = await pipeline(streams);

	equal(transform.callCount, 3);
	equal(final.callCount, 1);
	deepEqual(result, {});
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
		equal(e.message, "error");
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
		equal(e.message, "error");
	}
});

// *** createBranchStream *** //
if (variant === "node") {
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

		equal(isReadable(streams[1]), true);
		equal(isWritable(streams[1]), true);

		const result = await pipeline(streams);

		deepEqual(result, { branch: { a: 1 } });
		equal(transform.callCount, 6);
	});
}

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

	equal(isReadable(streams[1]), true);
	equal(isWritable(streams[1]), true);
	equal(transform.callCount, 3);
	deepEqual(result, { count: 3 });
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
		equal(e.message, "Promise instead of stream passed in at index 1");
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
		equal(true, false);
	} catch (e) {
		equal(e.message, "Error");
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
		equal(e.message, "Promise instead of stream passed in at index 1");
	}
});

// TODO update to catch error
// test(`${variant}: pipejoin should throw error when a stream thrown an error`, async (_t) => {
// 	const input = ["a", "b", "c"];

// 	const transform = (chunk, enqueue) => {
// 		if (chunk === "b") throw new Error("Error");
// 		enqueue(chunk);
// 	};

// 	const streams = [
// 		createReadableStream(input),
// 		createTransformStream(transform),
// 	];

// 	try {
// 	  for await (const item of pipejoin(streams)) {
// 	    console.log(item)
// 	  }
// 	} catch (e) {
// 	  equal(e.message, 'Error')
// 	}
// });

// *** makeOptions *** //
if (variant === "node") {
	test(`${variant}: makeOptions should return interoperable structure`, async (_t) => {
		const options = makeOptions({
			highWaterMark: 1,
			chunkSize: 2,
		});
		deepEqual(options, {
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
	//   deepEqual(options, {
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
