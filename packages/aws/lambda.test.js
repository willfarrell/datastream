import { deepStrictEqual, strictEqual } from "node:assert";
import test from "node:test";

let variant = "unknown";
for (const execArgv of process.execArgv) {
	const flag = "--conditions=";
	if (execArgv.includes(flag)) {
		variant = execArgv.replace(flag, "");
	}
}

if (variant === "node") {
	const { InvokeWithResponseStreamCommand, LambdaClient } = await import(
		"@aws-sdk/client-lambda"
	);
	const lambdaModule = await import("@datastream/aws/lambda");
	const {
		default: lambdaDefault,
		awsLambdaReadableStream,
		awsLambdaSetClient,
	} = lambdaModule;
	const { createReadableStream, pipeline } = await import("@datastream/core");
	const { mockClient } = await import("aws-sdk-client-mock");

	test(`${variant}: awsLambdaReadableStream should return chunk`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		const encoder = new TextEncoder();
		const decoder = new TextDecoder();

		client.on(InvokeWithResponseStreamCommand, {}).resolves({
			EventStream: createReadableStream([
				{
					PayloadChunk: {
						Payload: encoder.encode("1"),
					},
				},
				{
					PayloadChunk: {
						Payload: encoder.encode("2"),
					},
				},
			]),
		});

		let result = "";
		for await (const chunk of await awsLambdaReadableStream({})) {
			result += decoder.decode(chunk);
		}

		deepStrictEqual(result, "12");
	});

	test(`${variant}: awsLambdaReadableStream should throw error`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		client.on(InvokeWithResponseStreamCommand, {}).resolves({
			EventStream: createReadableStream([
				{
					InvokeComplete: {
						ErrorCode: "ErrorCode",
						ErrorDetails: "ErrorDetails",
					},
				},
			]),
		});

		try {
			await pipeline([awsLambdaReadableStream({})]);

			strictEqual(true, false);
		} catch (e) {
			strictEqual(e.message, "ErrorCode");
			strictEqual(e.cause.ErrorDetails, "ErrorDetails");
			strictEqual(e.cause.index, 0);
		}
	});

	test(`${variant}: awsLambdaReadableStream should handle array of options (multiple invocations)`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		const encoder = new TextEncoder();
		const decoder = new TextDecoder();

		client
			.on(InvokeWithResponseStreamCommand, { FunctionName: "fn1" })
			.resolves({
				EventStream: createReadableStream([
					{
						PayloadChunk: {
							Payload: encoder.encode("a"),
						},
					},
					{
						PayloadChunk: {
							Payload: encoder.encode("b"),
						},
					},
				]),
			});

		client
			.on(InvokeWithResponseStreamCommand, { FunctionName: "fn2" })
			.resolves({
				EventStream: createReadableStream([
					{
						PayloadChunk: {
							Payload: encoder.encode("c"),
						},
					},
				]),
			});

		let result = "";
		for await (const chunk of await awsLambdaReadableStream([
			{ FunctionName: "fn1" },
			{ FunctionName: "fn2" },
		])) {
			result += decoder.decode(chunk);
		}

		deepStrictEqual(result, "abc");
	});

	test(`${variant}: awsLambdaReadableStream should pass abort signal to client.send`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		const encoder = new TextEncoder();
		client.on(InvokeWithResponseStreamCommand).resolves({
			EventStream: createReadableStream([
				{ PayloadChunk: { Payload: encoder.encode("ok") } },
			]),
		});

		const controller = new AbortController();
		for await (const _chunk of await awsLambdaReadableStream(
			{ FunctionName: "f" },
			{ signal: controller.signal },
		)) {
			// consume
		}

		const calls = client.commandCalls(InvokeWithResponseStreamCommand);
		deepStrictEqual(calls[0].args[1]?.abortSignal, controller.signal);
	});

	test(`${variant}: awsLambdaReadableStream should use per-call client option`, async (_t) => {
		// default client must NOT be used when options.client is supplied
		const defaultClientMock = mockClient(LambdaClient);
		awsLambdaSetClient(defaultClientMock);
		defaultClientMock
			.on(InvokeWithResponseStreamCommand)
			.rejects(new Error("default client should not be called"));

		const perCallClient = mockClient(LambdaClient);
		const encoder = new TextEncoder();
		const decoder = new TextDecoder();
		perCallClient.on(InvokeWithResponseStreamCommand).resolves({
			EventStream: createReadableStream([
				{ PayloadChunk: { Payload: encoder.encode("z") } },
			]),
		});

		let result = "";
		for await (const chunk of await awsLambdaReadableStream({
			FunctionName: "f",
			client: perCallClient,
		})) {
			result += decoder.decode(chunk);
		}

		deepStrictEqual(result, "z");
		deepStrictEqual(
			perCallClient.commandCalls(InvokeWithResponseStreamCommand).length,
			1,
		);
		deepStrictEqual(
			defaultClientMock.commandCalls(InvokeWithResponseStreamCommand).length,
			0,
		);
	});

	test(`${variant}: awsLambdaReadableStream should not send client field as invoke input`, async (_t) => {
		const perCallClient = mockClient(LambdaClient);
		const encoder = new TextEncoder();
		perCallClient.on(InvokeWithResponseStreamCommand).resolves({
			EventStream: createReadableStream([
				{ PayloadChunk: { Payload: encoder.encode("ok") } },
			]),
		});

		for await (const _chunk of await awsLambdaReadableStream({
			FunctionName: "f",
			client: perCallClient,
		})) {
			// consume
		}

		const calls = perCallClient.commandCalls(InvokeWithResponseStreamCommand);
		deepStrictEqual(calls[0].args[0].input.client, undefined);
		deepStrictEqual(calls[0].args[0].input.FunctionName, "f");
	});

	test(`${variant}: awsLambdaReadableStream should include FunctionName in error cause`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		client
			.on(InvokeWithResponseStreamCommand, { FunctionName: "fn1" })
			.resolves({
				EventStream: createReadableStream([
					{
						InvokeComplete: {
							ErrorCode: "Unhandled",
							ErrorDetails: "boom",
						},
					},
				]),
			});

		try {
			for await (const _chunk of await awsLambdaReadableStream([
				{ FunctionName: "fn1" },
			])) {
				// consume
			}
			strictEqual(true, false);
		} catch (e) {
			strictEqual(e.message, "Unhandled");
			strictEqual(e.cause.FunctionName, "fn1");
			strictEqual(e.cause.index, 0);
			strictEqual(e.cause.ErrorDetails, "boom");
		}
	});

	test(`${variant}: default export should include all stream functions`, (_t) => {
		deepStrictEqual(Object.keys(lambdaDefault).sort(), [
			"readableStream",
			"responseStream",
			"setClient",
		]);
	});

	// A non-payload, non-complete event must be silently skipped: neither yielded
	// nor treated as an error. Pins the `chunk?.PayloadChunk?.Payload` /
	// `chunk?.InvokeComplete?.ErrorCode` guards and the `else if (...)` condition
	// (an `else if (true)` mutant would throw on the empty frame).
	test(`${variant}: awsLambdaReadableStream skips frames with neither PayloadChunk nor InvokeComplete`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		const encoder = new TextEncoder();
		const decoder = new TextDecoder();
		// A null/undefined frame must be tolerated by the leading `chunk?.` guards:
		// dropping the optional chain (`chunk.PayloadChunk` / `chunk.InvokeComplete`)
		// would throw a TypeError on these frames.
		client.on(InvokeWithResponseStreamCommand).resolves({
			EventStream: createReadableStream([
				null,
				undefined,
				{},
				{ SomethingElse: { foo: "bar" } },
				{ PayloadChunk: { Payload: encoder.encode("ok") } },
				{ InvokeComplete: {} },
			]),
		});

		let result = "";
		for await (const chunk of await awsLambdaReadableStream({
			FunctionName: "f",
		})) {
			result += decoder.decode(chunk);
		}
		deepStrictEqual(result, "ok");
	});

	// An InvokeComplete WITHOUT an ErrorCode must NOT throw (success completion).
	// Kills the `else if (true)` mutant which would throw on every complete.
	test(`${variant}: awsLambdaReadableStream treats InvokeComplete without ErrorCode as success`, async (_t) => {
		const client = mockClient(LambdaClient);
		awsLambdaSetClient(client);

		const encoder = new TextEncoder();
		const decoder = new TextDecoder();
		client.on(InvokeWithResponseStreamCommand).resolves({
			EventStream: createReadableStream([
				{ PayloadChunk: { Payload: encoder.encode("done") } },
				{ InvokeComplete: { LogResult: "..." } },
			]),
		});

		let result = "";
		for await (const chunk of await awsLambdaReadableStream({
			FunctionName: "f",
		})) {
			result += decoder.decode(chunk);
		}
		deepStrictEqual(result, "done");
	});

	// setClient must STORE the passed client as the module default. mockClient
	// patches the SDK prototype (so instance identity is invisible to it); use a
	// plain stub whose own send() proves the stored reference is actually used. A
	// `setClient(){}` mutant leaves the original default in place and the stub's
	// send is never called.
	test(`${variant}: awsLambdaSetClient stores the passed client as the default`, async (_t) => {
		const encoder = new TextEncoder();
		const decoder = new TextDecoder();
		let calls = 0;
		const stub = {
			send: async () => {
				calls++;
				return {
					EventStream: createReadableStream([
						{ PayloadChunk: { Payload: encoder.encode("stub") } },
					]),
				};
			},
		};
		awsLambdaSetClient(stub);

		let result = "";
		for await (const chunk of await awsLambdaReadableStream({
			FunctionName: "f",
		})) {
			result += decoder.decode(chunk);
		}
		deepStrictEqual(result, "stub");
		deepStrictEqual(calls, 1);
	});

	// setClient swaps the module default used when no per-call client is given.
	test(`${variant}: awsLambdaSetClient routes to the newly set default client`, async (_t) => {
		const first = mockClient(LambdaClient);
		awsLambdaSetClient(first);
		first
			.on(InvokeWithResponseStreamCommand)
			.rejects(new Error("stale default client should not be used"));

		const encoder = new TextEncoder();
		const decoder = new TextDecoder();
		const second = mockClient(LambdaClient);
		second.on(InvokeWithResponseStreamCommand).resolves({
			EventStream: createReadableStream([
				{ PayloadChunk: { Payload: encoder.encode("new") } },
			]),
		});
		awsLambdaSetClient(second);

		let result = "";
		for await (const chunk of await awsLambdaReadableStream({
			FunctionName: "f",
		})) {
			result += decoder.decode(chunk);
		}
		deepStrictEqual(result, "new");
		deepStrictEqual(
			second.commandCalls(InvokeWithResponseStreamCommand).length,
			1,
		);
		deepStrictEqual(
			first.commandCalls(InvokeWithResponseStreamCommand).length,
			0,
		);
	});
}
