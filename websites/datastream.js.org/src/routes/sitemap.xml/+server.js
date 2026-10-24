const pages = Object.keys(import.meta.glob("/src/routes/docs/**/+page.md"));

export const prerender = true;

// url.origin is config paths.origin while prerendering
export const GET = async ({ url }) => {
	const paths = [
		"/",
		...pages
			.map((path) =>
				path.replace("/src/routes", "").replace(/\/\+page\.md$/, ""),
			)
			.sort(),
	];

	const urls = paths
		.map((path) => `<url><loc>${url.origin}${path}</loc></url>`)
		.join("");

	return new Response(
		`<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${urls}</urlset>`,
		{
			headers: {
				"Content-Type": "application/xml; charset=utf-8",
				"Cache-Control": "public, max-age=3600",
			},
		},
	);
};
