import { mkdirSync } from "node:fs";
import { sveltekit } from "@sveltejs/kit/vite";
import { createLogger, defineConfig } from "vite";
import mkcert from "vite-plugin-mkcert";
import sitemap from "vite-plugin-sitemap";
import sriPrerendered from "vite-plugin-sri";

const sitemapOutDir = "build/assets/";
mkdirSync(sitemapOutDir, { recursive: true });

// TODO remove after vite 8 — https://github.com/vitejs/vite/issues/19498
const logger = createLogger();
const originalWarn = logger.warn.bind(logger);
logger.warn = (msg, options) => {
	if (msg.includes("node:async_hooks")) return;
	originalWarn(msg, options);
};

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
	customLogger: logger,
	ssr: {
		noExternal: ["prismjs"],
	},
	optimizeDeps: {
		exclude: ["@willfarrell-ds/svelte", "@willfarrell-ds/vanilla"],
	},
});
