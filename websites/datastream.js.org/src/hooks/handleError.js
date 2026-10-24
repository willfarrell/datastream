//import { stderr } from "node:process"; // CloudFlare doesn't support

export async function handleError({ kind, error, event }) {
	if (kind === "app") return;
	console.error(
		`${JSON.stringify({
			log_level: "ERROR",
			message: error?.message ?? "Internal Error",
			stack: error?.stack,
			cause: error?.cause,
			status_code: error?.status ?? 500,
			request_id: "00000000-0000-0000-0000-000000000000",
			path: event.url.pathname,
			route: event.route.id,
		})}\n`,
	);
}
