//import { stderr } from "node:process"; // CloudFlare doesn't support

export async function handleError({ error, event }) {
	console.error(
		`${JSON.stringify({
			log_level: "ERROR",
			message: error.message,
			stack: error.stack,
			cause: error.cause,
			status_code: error.statusCode ?? null,
			request_id: "00000000-0000-0000-0000-000000000000",
			// Never the raw event: it carries request headers (ip, user agent,
			// cookies) and, on Cloudflare, platform.env bindings.
			path: event.url.pathname,
			route: event.route.id,
		})}\n`,
	);
}
