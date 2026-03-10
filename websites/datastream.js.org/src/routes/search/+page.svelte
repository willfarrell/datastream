<script>
import Card from "@design-system/components/Card.svelte";
import H1 from "@design-system/components/Heading1.svelte";
import H3 from "@design-system/components/Heading3.svelte";
import LayoutCenter from "@design-system/components/LayoutCenter.svelte";
import A from "@design-system/elements/a.svelte";
import Li from "@design-system/elements/li.svelte";
import P from "@design-system/elements/p.svelte";
import Section from "@design-system/elements/section.svelte";
import Span from "@design-system/elements/span.svelte";
import Ul from "@design-system/elements/ul.svelte";

let { data } = $props();
const results = $derived(data.results);
</script>

<svelte:head>
    <title>Search | datastream</title>
</svelte:head>
<LayoutCenter>
    <Section>
        <H1>Search results</H1>
        {#if results.length}
            <Ul class="grid">
                {#each results as card}
                    <Card id={card.id}>
                        <H3
                            ><A href={card.href} aria-describedby={card.id}
                                >{card.title}</A
                            ></H3
                        >
                        {#if card.description}
                            <P>{@html card.description}</P>
                        {/if}
                        {#if card.button}
                            <Span aria-hidden="true" id={card.id}
                                >{card.button}</Span
                            >
                        {/if}
                    </Card>
                {/each}
            </Ul>
        {:else}
            <P>No pages found.</P>
        {/if}
    </Section>
</LayoutCenter>
