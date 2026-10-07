<script>
import H1 from "@design-system/components/Heading1.svelte";
import LayoutAside from "@design-system/components/LayoutAside.svelte";
import LayoutTableOfContents from "@design-system/components/LayoutTableOfContents.svelte";
import NavScrollspy from "@design-system/components/NavScrollspy.svelte";
import A from "@design-system/elements/a.svelte";
import Hgroup from "@design-system/elements/hgroup.svelte";
import Li from "@design-system/elements/li.svelte";
import Ol from "@design-system/elements/ol.svelte";
import copyPreUrl from "@willfarrell-ds/vanilla/components/ds-copy-pre.js?worker&url";
import AsideNav from "./AsideNav.svelte";

const { title = "Documentation", description = "", headings = [], children } = $props();
</script>
<svelte:head>
	<link rel="modulepreload" href={copyPreUrl} />
	<title>{title} | datastream</title>
	{#if description}<meta name="description" content={description} />{/if}
</svelte:head>
<LayoutAside>
    {#snippet aside()}
      <AsideNav />
    {/snippet}
<LayoutTableOfContents>
    {#snippet header()}
		<Hgroup>
			<H1>{title}</H1>
		</Hgroup>
    {/snippet}
    {#snippet aside()}
		<NavScrollspy>
			<Ol>
				{#if headings.length > 0}
					{#each headings as heading}
						<Li><A href="#{heading.id}">{heading.text}</A></Li>
					{/each}
				{/if}
			</Ol>
		</NavScrollspy>
    {/snippet}
	{@render children?.()}
</LayoutTableOfContents>
</LayoutAside>
