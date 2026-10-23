import visit from "unist-util-visit";

/**
 * mdsvex highlights fences before rehype runs, so they arrive as raw html.
 */
export function rehypeCopyPre() {
	return (tree) => {
		visit(tree, "raw", (node) => {
			node.value = node.value.replace(
				/^<pre class="language-[^"]*"/,
				'<pre is="ds-copy-pre"',
			);
		});
	};
}
