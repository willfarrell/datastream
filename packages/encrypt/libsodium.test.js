import { rejects, strictEqual } from "node:assert";
import { register } from "node:module";
import test, { describe } from "node:test";
import { isWritable } from "@datastream/core";
import { variant } from "../variant.js";

describe(`@datastream/encrypt/libsodium (${variant})`, async () => {
	// libsodium-wrappers is an optional peer dependency of the browser build. Swap
	// it, for this process only (loader hooks are per test file), for a fake whose
	// `ready` thenable fails on demand and which has no default export - pinning
	// both the "install it" error and the namespace fallback without uninstalling
	// anything or touching the real-libsodium tests in index.test.js.
	register(
		`data:text/javascript,${encodeURIComponent(`
			export async function resolve(specifier, context, nextResolve) {
				if (specifier === "libsodium-wrappers") {
					return { url: "data:text/javascript," + encodeURIComponent(${JSON.stringify(`
						export const ready = {
							then(resolve, reject) {
								globalThis.__libsodiumMissing ? reject(new Error("not installed")) : resolve();
							},
						};
					`)}), shortCircuit: true };
				}
				return nextResolve(specifier, context);
			}
		`)}`,
		import.meta.url,
	);

	const { encryptStream } = await import(
		variant === "browser"
			? "@datastream/encrypt"
			: `file://${new URL("./index.browser.js", import.meta.url).pathname}`
	);
	const key = new Uint8Array(32);

	test(`browser CHACHA20-POLY1305 explains a missing libsodium-wrappers`, async () => {
		globalThis.__libsodiumMissing = true;
		try {
			await rejects(
				encryptStream({ key, algorithm: "CHACHA20-POLY1305" }),
				/requires libsodium-wrappers\. Install it: npm install libsodium-wrappers/,
			);
		} finally {
			delete globalThis.__libsodiumMissing;
		}
	});

	test(`browser CHACHA20-POLY1305 accepts a libsodium build without a default export`, async () => {
		strictEqual(
			isWritable(
				await encryptStream({
					key,
					iv: new Uint8Array(12),
					algorithm: "CHACHA20-POLY1305",
				}),
			),
			true,
		);
	});
});
