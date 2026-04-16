import test from "node:test";
import { ipfsAddStream, ipfsGetStream } from "@datastream/ipfs";
import fc from "fast-check";

const catchError = (input, e) => {
	const expectedErrors = [];
	if (expectedErrors.includes(e.message)) {
		return;
	}
	console.error(input, e);
	throw e;
};

// *** ipfsGetStream *** //
test("fuzz ipfsGetStream w/ random cid", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.string({ minLength: 1, maxLength: 100 }),
			async (cid) => {
				try {
					const node = {
						get(_cid) {
							return Buffer.from(`data-${_cid}`);
						},
						add(_stream) {
							return { cid: "QmFuzz" };
						},
					};
					await ipfsGetStream({ node, cid });
				} catch (e) {
					catchError(cid, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});

// *** ipfsAddStream *** //
test("fuzz ipfsAddStream w/ random resultKey", async () => {
	await fc.assert(
		fc.asyncProperty(
			fc.option(fc.string({ minLength: 1, maxLength: 50 })),
			async (resultKey) => {
				try {
					const node = {
						get(_cid) {
							return Buffer.from("data");
						},
						add(_stream) {
							return { cid: "QmFuzzAdd" };
						},
					};
					const options = { node };
					if (resultKey !== null) {
						options.resultKey = resultKey;
					}
					const stream = await ipfsAddStream(options);
					stream.result();
				} catch (e) {
					catchError(resultKey, e);
				}
			},
		),
		{
			numRuns: 1_000,
			verbose: 2,
			examples: [],
		},
	);
});
