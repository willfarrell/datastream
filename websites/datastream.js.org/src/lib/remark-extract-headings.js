import { toString as nodetoString } from "mdast-util-to-string";
import visit from "unist-util-visit";

/**
 * Remark plugin to extract H2 headings and add them to frontmatter
 */
export function remarkExtractHeadings() {
	return (tree, file) => {
		const headings = [];

		visit(tree, "heading", (node) => {
			// Only extract H2 headings
			if (node.depth === 2) {
				// Strip any angle brackets from the plain-text heading (cosmetic: the
				// value is only used as an auto-escaped TOC label and an [a-z0-9-] slug).
				// Single global char-class pass so the removal is complete/idempotent.
				const text = nodetoString(node).replace(/[<>]/g, "");
				// Create slug from heading text
				const id = text
					.toLowerCase()
					.replace(/[^a-z0-9]+/g, "-")
					.replace(/^-|-$/g, "");

				headings.push({ id, text });
			}
		});

		// Add headings to frontmatter
		if (!file.data.fm) {
			file.data.fm = {};
		}
		file.data.fm.headings = headings;
	};
}
