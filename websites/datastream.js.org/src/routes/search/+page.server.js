const modules = import.meta.glob("/src/routes/docs/**/+page.md", {
	query: "?raw",
	import: "default",
	eager: true,
});

function getFiles() {
	const files = [];
	for (const [path, content] of Object.entries(modules)) {
		const filePath = path
			.replace("/src/routes/docs/", "")
			.replace(/\+page\.md$/, "")
			.replace(/^\//, "");
		files.push({ filePath, content });
	}
	return files;
}

function extractTitle(content) {
	// Try to extract title from frontmatter
	const frontmatterMatch = content.match(/^---\s*\n([\s\S]*?)\n---/);
	if (frontmatterMatch) {
		const titleMatch = frontmatterMatch[1].match(/title:\s*(.+)/);
		if (titleMatch) {
			return titleMatch[1].trim();
		}
	}

	// Fallback to first # heading
	const headingMatch = content.match(/^#\s+(.+)$/m);
	if (headingMatch) {
		return headingMatch[1].trim();
	}

	return "Untitled";
}

function cleanContentForSearch(content) {
	// Remove frontmatter
	const withoutFrontmatter = content.replace(/^---\s*\n[\s\S]*?\n---\s*\n/, "");

	// Remove markdown syntax for cleaner search
	return withoutFrontmatter
		.replace(/```[\s\S]*?```/g, "") // Remove code blocks
		.replace(/`[^`]+`/g, "") // Remove inline code
		.replace(/^\s*#{1,6}\s+/gm, "") // Remove headings
		.replace(/\[([^\]]+)\]\([^)]+\)/g, "$1") // Convert links to text
		.replace(/[*_]{1,2}([^*_]+)[*_]{1,2}/g, "$1") // Remove bold/italic
		.replace(/^\s*[-*+]\s+/gm, "") // Remove list markers
		.replace(/\n\s*\n/g, " ") // Collapse multiple newlines
		.replace(/\s+/g, " ") // Normalize whitespace
		.trim();
}

function searchContent(content, query) {
	const cleanContent = cleanContentForSearch(content);
	const lowerContent = cleanContent.toLowerCase();
	const lowerQuery = query.toLowerCase();

	return lowerContent.includes(lowerQuery);
}

function extractDescription(content, query, maxLength = 150) {
	// Validate query to prevent ReDoS attacks
	if (!query || query.length > 100 || query.length < 1) {
		return null;
	}

	// Reject queries with excessive repetition (potential ReDoS pattern)
	if (/(.)\1{20,}/.test(query)) {
		return null;
	}

	const cleanContent = cleanContentForSearch(content);

	const lowerCleanContent = cleanContent.toLowerCase();
	const lowerQuery = query.toLowerCase();
	const queryIndex = lowerCleanContent.indexOf(lowerQuery);

	if (queryIndex === -1) {
		return null;
	}

	const snippetStart = Math.max(0, queryIndex - Math.floor(maxLength / 2));
	const snippetEnd = Math.min(cleanContent.length, snippetStart + maxLength);

	let snippet = cleanContent.substring(snippetStart, snippetEnd);

	if (snippetStart > 0) snippet = `...${snippet}`;
	if (snippetEnd < cleanContent.length) snippet = `${snippet}...`;

	// Bold the query match (case-insensitive) with safe regex handling
	try {
		const escapedQuery = query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
		// SAST: Query is validated (max length, no excessive repetition) and properly escaped
		// nosemgrep: javascript.lang.security.audit.detect-non-literal-regexp.detect-non-literal-regexp
		const regex = new RegExp(`(${escapedQuery})`, "gi");
		snippet = snippet.replace(regex, "<strong>$1</strong>");
	} catch (e) {
		console.warn("Search highlighting failed:", e.message);
	}

	return snippet;
}

export function load({ url }) {
	const query = url.searchParams.get("q");

	if (!query || query.trim() === "") {
		return {
			results: [],
			query: "",
		};
	}

	const files = getFiles();
	const results = [];
	const maxResults = 10;

	for (const { filePath, content } of files) {
		if (results.length >= maxResults) break;

		if (searchContent(content, query)) {
			const href = `/docs/${filePath}`;
			const title = extractTitle(content);
			const description = extractDescription(content, query);
			const id = filePath.replace(/\//g, "-") || "home";

			results.push({
				id,
				href,
				title,
				description,
			});
		}
	}

	return {
		results,
		query: query.trim(),
	};
}
