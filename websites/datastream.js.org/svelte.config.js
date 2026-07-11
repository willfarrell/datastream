import { resolve } from "node:path";
import adapter from "@sveltejs/adapter-cloudflare";
import { mdsvex } from "mdsvex";
import tardisec from "./.tardisec.sveltekit.json" with { type: "json" };
import { rehypeAddHeadingIds } from "./src/lib/rehype-add-heading-ids.js";
import { remarkExtractHeadings } from "./src/lib/remark-extract-headings.js";

const domain = process.env.ORIGIN ?? "datastream.js.org";
const origin = domain;
const config = {
	kit: {
		adapter: adapter({}),
		alias: {
			"@design-system": resolve("../../node_modules/@willfarrell-ds/svelte"),
			"@components": resolve("./src/components"),
			"@hooks": resolve("./src/hooks"),
			"@scripts": resolve("./src/scripts"),
			"@styles": resolve("./src/styles"),
		},
		appDir: "_",
		csp: {
			...tardisec.kit.csp,
			mode: "hash",
			directives: {
				"default-src": ["none"], // 'report-sha256'
				"base-uri": ["none"],
				"connect-src": ["self"],
				"form-action": ["self"],
				"frame-ancestors": ["none"],
				"img-src": ["self"],
				"manifest-src": ["self"],
				"script-src": ["self"],
				"script-src-attr": ["report-sample"],
				//"script-src-elem": ['self'],
				"style-src": ["self"],
				"style-src-attr": ["report-sample"],
				//"style-src-elem": ['self'],
				//'trusted-types':[],
				//'require-trusted-types-for': ['script'],
				"upgrade-insecure-requests": true,
				"worker-src": ["self"],
				"report-to": ["default"],
				//'report-uri': [`https://${domain}.report-to.org`]
			},
			reportOnly: {
				"base-uri": ["none"],
				"default-src": ["none", "report-sample", "'report-sha256'"],
				"form-action": ["none"],
				"frame-ancestors": ["none"],
				"report-to": ["default"],
				"report-uri": [
					"https://9a3890fa-a783-51c2-b20f-0dbe457d526f.report-to.org",
				],
				"require-trusted-types-for": ["script"],
			},
		},
		csrf: {
			trustedOrigins: [origin],
		},
	},
	preprocess: [
		mdsvex({
			extensions: [".md"],
			layout: {
				_: resolve("./src/components/docs/mdsvex-layout.svelte"),
			},
			remarkPlugins: [remarkExtractHeadings],
			rehypePlugins: [rehypeAddHeadingIds],
		}),
	],
	extensions: [".svelte", ".md"],
	prerender: {
		concurrency: 5,
		crawl: false,
		entries: ["/", "/sitemap.xml", "/llms.txt"],
		handleHttpError: "warn",
		handleMissingId: "warn",
		handleEntryGeneratorMismatch: "warn",
		origin: `https://${origin}`,
	},
	onwarn(warning, defaultHandler) {
		if (warning.code === "attribute_avoid_is") return;
		if (warning.code === "non_reactive_update") return;

		warning.message = `[${warning.code}] ${warning.message}`;
		defaultHandler(warning);
	},
};

export default config;
