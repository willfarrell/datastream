<script>
import Card from "@willfarrell-ds/svelte/components/Card.svelte";
import H1 from "@willfarrell-ds/svelte/components/Heading1.svelte";
import H2 from "@willfarrell-ds/svelte/components/Heading2.svelte";
import LayoutCenter from "@willfarrell-ds/svelte/components/LayoutCenter.svelte";
import A from "@willfarrell-ds/svelte/elements/a.svelte";
import P from "@willfarrell-ds/svelte/elements/p.svelte";
import Section from "@willfarrell-ds/svelte/elements/section.svelte";
import Span from "@willfarrell-ds/svelte/elements/span.svelte";
import Ul from "@willfarrell-ds/svelte/elements/ul.svelte";

let { data } = $props();
const results = $derived(data.results);
</script>

<svelte:head>
    <title>Search | datastream</title>
    <meta name="description" content="Search the datastream documentation." />
</svelte:head>
<LayoutCenter>
    <Section>
        <H1>Search results</H1>
        {#if results.length}
            <Ul class="grid">
                {#each results as card}
                    <Card id={card.id}>
                        <H2
                            ><A href={card.href} aria-describedby={card.id}
                                >{card.title}</A
                            ></H2
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
