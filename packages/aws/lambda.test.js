import { deepStrictEqual, strictEqual } from "node:assert";
import test, { describe } from "node:test";
import { variant } from "../variant.js";

describe(`@datastream/aws/lambda (${variant})`, async () => {
	if (variant === "node") {
		const { InvokeWithResponseStreamCommand, LambdaClient } = await import(
			"@aws-sdk/client-lambda"
		);
		const lambdaModule = await import("@datastream/aws/lambda");
		const { awsLambdaReadableStream, awsLambdaSetClient } = lambdaModule;
		const { createReadableStream, pipeline } = await import("@datastream/core");
		const { mockClient } = await import("aws-sdk-client-mock");

		test(`awsLambdaReadableStream should return chunk`, async (_t) => {
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

		test(`awsLambdaReadableStream should throw error`, async (_t) => {
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

		test(`awsLambdaReadableStream should handle array of options (multiple invocations)`, async (_t) => {
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

		test(`awsLambdaReadableStream should pass abort signal to client.send`, async (_t) => {
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
			const sent = calls[0].args[1]?.abortSignal;
			deepStrictEqual(sent.aborted, false);
			controller.abort();
			deepStrictEqual(sent.aborted, true);
		});

		test(`awsLambdaReadableStream destroy aborts the in-flight request`, {
			timeout: 1000,
		}, async (_t) => {
			let sent;
			awsLambdaSetClient({
				send: (_command, { abortSignal }) => {
					sent = abortSignal;
					return new Promise((_resolve, reject) => {
						abortSignal?.addEventListener("abort", () =>
							reject(abortSignal.reason),
						);
					});
				},
			});
			const stream = awsLambdaReadableStream({ FunctionName: "f" });
			stream.resume();
			await new Promise((resolve) => setImmediate(resolve));
			const closed = new Promise((resolve) => stream.once("close", resolve));
			stream.destroy();
			await closed;
			strictEqual(sent?.aborted, true);
		});

		test(`awsLambdaReadableStream should use per-call client option`, async (_t) => {
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

		test(`awsLambdaReadableStream should not send client field as invoke input`, async (_t) => {
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
			}

			const calls = perCallClient.commandCalls(InvokeWithResponseStreamCommand);
			deepStrictEqual(calls[0].args[0].input.client, undefined);
			deepStrictEqual(calls[0].args[0].input.FunctionName, "f");
		});

		test(`awsLambdaReadableStream should include FunctionName in error cause`, async (_t) => {
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
				}
				strictEqual(true, false);
			} catch (e) {
				strictEqual(e.message, "Unhandled");
				strictEqual(e.cause.FunctionName, "fn1");
				strictEqual(e.cause.index, 0);
				strictEqual(e.cause.ErrorDetails, "boom");
			}
		});

		test(`lambda has no default export`, (_t) => {
			strictEqual(Object.hasOwn(lambdaModule, "default"), false);
		});

		test(`awsLambdaReadableStream skips frames with neither PayloadChunk nor InvokeComplete`, async (_t) => {
			const client = mockClient(LambdaClient);
			awsLambdaSetClient(client);

			const encoder = new TextEncoder();
			const decoder = new TextDecoder();
			// Frames come from an async generator, not createReadableStream: Node 26
			// object-mode Readable async-iteration breaks on null/undefined values.
			async function* eventStream() {
				yield null;
				yield undefined;
				yield {};
				yield { SomethingElse: { foo: "bar" } };
				yield { PayloadChunk: { Payload: encoder.encode("ok") } };
				yield { InvokeComplete: {} };
			}
			client.on(InvokeWithResponseStreamCommand).resolves({
				EventStream: eventStream(),
			});

			let result = "";
			for await (const chunk of await awsLambdaReadableStream({
				FunctionName: "f",
			})) {
				result += decoder.decode(chunk);
			}
			deepStrictEqual(result, "ok");
		});

		test(`awsLambdaReadableStream treats InvokeComplete without ErrorCode as success`, async (_t) => {
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
		test(`awsLambdaSetClient stores the passed client as the default`, async (_t) => {
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

		test(`awsLambdaSetClient routes to the newly set default client`, async (_t) => {
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
});
