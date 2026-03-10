const modules = import.meta.glob("/src/routes/docs/**/+page.md", {
	query: "?raw",
	import: "default",
	eager: true,
});

export const prerender = true;

export const GET = async () => {
	const contents = [];

	const paths = Object.keys(modules).sort();

	for (const path of paths) {
		const relativePath = path
			.replace("/src/routes/docs/", "")
			.replace(/\+page\.md$/, "")
			.replace(/^\//, "");

		contents.push(`\n\n// File: ${relativePath}\n\n`);
		contents.push(modules[path]);
	}

	return new Response(contents.join(""), {
		headers: {
			"Content-Type": "text/plain; charset=utf-8",
			"Cache-Control": "public, max-age=3600",
		},
	});
};
