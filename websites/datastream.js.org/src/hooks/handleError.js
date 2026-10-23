//import { stderr } from "node:process"; // CloudFlare doesn't support

export async function handleError({ kind, error, event }) {
	// Kit 3 also passes expected `error()` throws here; v2 never did
	if (kind === "app") return;
	console.error(
		`${JSON.stringify({
			log_level: "ERROR",
			message: error?.message ?? "Internal Error",
			stack: error?.stack,
			cause: error?.cause,
			status_code: error?.status ?? 500,
			request_id: "00000000-0000-0000-0000-000000000000",
			// Never the raw event: it carries request headers (ip, user agent,
			// cookies) and, on Cloudflare, platform.env bindings.
			path: event.url.pathname,
			route: event.route.id,
		})}\n`,
	);
}
