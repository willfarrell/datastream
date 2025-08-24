import { deepEqual } from "node:assert";
import test from "node:test";
import { base64DecodeStream, base64EncodeStream } from "@datastream/base64";
// import sinon from 'sinon'
import {
	createReadableStream,
	pipejoin,
	streamToString,
} from "@datastream/core";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes("--conditions=")) {
		variant = execArgv.replace(flag, "");
	}
}

// *** base64EncodeStream *** //
test(`${variant}: base64EncodeStream should encode`, async (_t) => {
	const input = "encode";
	const streams = [createReadableStream(input), base64EncodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepEqual(output, btoa(input));
});

// *** base64DecodeStream *** //
test(`${variant}: base64DecodeStream should decode`, async (_t) => {
	const input = "decode";
	const streams = [createReadableStream(btoa(input)), base64DecodeStream()];
	const output = await streamToString(pipejoin(streams));

	deepEqual(output, input);
});

// *** Misc *** //
test(`${variant}: base64Stream should encode/decode`, async (_t) => {
	for (let i = 1; i <= 16; i++) {
		const input = "x".repeat(i);
		const streams = [
			createReadableStream(input),
			base64EncodeStream(),
			base64DecodeStream(),
		];
		const output = await streamToString(pipejoin(streams));

		deepEqual(output, input);
	}
});
