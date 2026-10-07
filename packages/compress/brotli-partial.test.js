import { deepStrictEqual, rejects } from "node:assert";
import { register } from "node:module";
import test, { describe } from "node:test";
import {
	createReadableStream,
	createWritableStream,
	pipeline,
} from "@datastream/core";
import { variant } from "../variant.js";

describe(`@datastream/compress/brotli-partial (${variant})`, async () => {
	// brotli-wasm's streaming contract lets compress()/decompress() consume only
	// part of their input when the output buffer fills (input_offset < length),
	// but the shipped wasm never actually does: it always swallows the whole
	// input first. Swap it, for this process only, for an echo codec that takes
	// five bytes per call, so the re-feed loop in brotli.browser.js is exercised.
	register(
		`data:text/javascript,${encodeURIComponent(`
			export async function resolve(specifier, context, nextResolve) {
				if (specifier === "brotli-wasm") {
					return { url: "data:text/javascript," + encodeURIComponent(${JSON.stringify(`
						export const BrotliStreamResultCode = { ResultSuccess: 1, NeedsMoreInput: 2, NeedsMoreOutput: 3 };
						const step = (input) => {
							if (input === undefined) return { buf: new Uint8Array(0), code: BrotliStreamResultCode.ResultSuccess, input_offset: 0 };
							const taken = Math.min(5, input.length);
							return {
								buf: input.slice(0, taken),
								code: taken < input.length ? BrotliStreamResultCode.NeedsMoreOutput : BrotliStreamResultCode.ResultSuccess,
								input_offset: taken,
							};
						};
						export class CompressStream { compress(input) { return step(input); } }
						export class DecompressStream { decompress(input) { return step(input); } }
						export default Promise.resolve({ CompressStream, DecompressStream, BrotliStreamResultCode });
					`)}), shortCircuit: true };
				}
				return nextResolve(specifier, context);
			}
		`)}`,
		import.meta.url,
	);

	const { brotliCompressStream, brotliDecompressStream } = await import(
		variant === "browser"
			? "@datastream/compress/brotli"
			: `file://${new URL("./brotli.browser.js", import.meta.url).pathname}`
	);

	test(`browser brotli streams re-feed the unconsumed input tail until the engine drains it`, async () => {
		const input = ["abcdefghijkl", "mnopq"]; // 12 = 5 + 5 + 2, then 5
		const echoed = async (stream) => {
			const chunks = [];
			await pipeline([
				createReadableStream(input),
				stream,
				createWritableStream((chunk) =>
					chunks.push(new TextDecoder().decode(chunk)),
				),
			]);
			return chunks;
		};
		// The compressor's flush drains the engine one last time (an empty buffer here).
		deepStrictEqual(await echoed(brotliCompressStream()), [
			"abcde",
			"fghij",
			"kl",
			"mnopq",
			"",
		]);
		deepStrictEqual(await echoed(brotliDecompressStream()), [
			"abcde",
			"fghij",
			"kl",
			"mnopq",
		]);
		await rejects(
			echoed(brotliDecompressStream({ maxOutputSize: 16 })),
			/Decompression output exceeds maxOutputSize \(16 bytes\)/,
		);
	});
});
