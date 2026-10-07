import { mkdirSync } from "node:fs";
import { sveltekit } from "@sveltejs/kit/vite";
import { defineConfig } from "vite";
import mkcert from "vite-plugin-mkcert";
import sitemap from "vite-plugin-sitemap";
import sriPrerendered from "vite-plugin-sri";

const sitemapOutDir = ".svelte-kit/cloudflare/";
mkdirSync(sitemapOutDir, { recursive: true });

export default defineConfig({
	plugins: [
		sveltekit(),
		mkcert({ mkcertPath: "/opt/homebrew/bin/mkcert" }),
		sriPrerendered(),
		sitemap({ hostname: "https://datastream.js.org", outDir: sitemapOutDir }),
	],
	build: {
		assetsInlineLimit: 0,
	},
	optimizeDeps: {
		exclude: ["@willfarrell-ds/svelte", "@willfarrell-ds/vanilla"],
		// ds-codeblock imports prismjs (CJS), excluded packages skip that interop
		include: ["prismjs"],
	},
});
