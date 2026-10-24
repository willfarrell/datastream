// TODO convert into adapter
// import * as env from '$env/static/private';
// import { redirect } from "@utils/sveltekit.js";
//import { recommendHttpHeader } from "@utils/recommend.js";
import { isRedirect } from "@sveltejs/kit";
import tardisec from "../../.tardisec.json" with { type: "json" };

const kitOwnsRedirect = (event) =>
	event.url?.pathname?.endsWith("/__data.json") ||
	event.request?.headers?.get("x-sveltekit-action") === "true";

const tardisecMiddleware = async ({ event, resolve }) => {
	// const { url, params, cookies } = event;

	let response;
	try {
		response = await resolve(event);
	} catch (e) {
		if (!isRedirect(e) || kitOwnsRedirect(event)) throw e;
		response = new Response(null, {
			status: e.status,
			headers: { location: e.location },
		});
	}

	const keys = Object.keys(tardisec.http.headers);
	for (let i = keys.length; i--; ) {
		const headerKey = keys[i];
		const headerValue = tardisec.http.headers[headerKey];
		if (headerValue && !response.headers.has(headerKey)) {
			response.headers.set(headerKey, headerValue);
		}
	}

	response.headers.delete("X-Sveltekit-Page");

	return response;
};

export default tardisecMiddleware;
