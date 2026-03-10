<script>
    import H1 from "@design-system/components/Heading1.svelte";
    import Div from "@design-system/elements/div.svelte";
    import Main from "@design-system/elements/main.svelte";
    import P from "@design-system/elements/p.svelte";
    import { page } from "$app/state";
</script>

<svelte:head>
    <title>{page.status} | datastream</title>
</svelte:head>

<Main id="main" class="container-error">
    <Div>
        <H1>{page.status}</H1>
        {#if page.status === 500}
            <P>
                {import.meta.env.AWS_REQUEST_ID ??
                    "00000000-0000-0000-0000-000000000000"}
            </P>
        {/if}
    </Div>
</Main>

<style>
    :global(html) {
        height: 100%;
    }
    :global(body) {
        height: 100%;
    }
    :global(main.container-error) {
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        height: 100%;

        & div {
            width: 40ch;
            text-align: center;
        }
        & span {
            padding: 0 1rem 1rem 1rem;
            white-space: nowrap;
        }
    }
</style>
