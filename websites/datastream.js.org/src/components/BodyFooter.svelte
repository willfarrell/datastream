<script>
import BodyFooter from "@design-system/components/BodyFooter.svelte";
import Image from "@design-system/components/Image.svelte";
import A from "@design-system/elements/a.svelte";
import { page } from "$app/state";

const { children } = $props();
const { url, params, data, form } = page;

const navLinks = {
	Docs: {
		Documentation: "/docs",
		Packages: "/docs/packages/core",
		LLMs: "/llms.txt",
	},
	"Community & support": {
		GitHub: "https://github.com/willfarrell/datastream",
	},
	More: {
		npm: "https://www.npmjs.com/package/@datastream/core",
		"OpenSSF Scorecard":
			"https://scorecard.dev/viewer/?uri=github.com/willfarrell/datastream",
		Sponsor: "https://github.com/sponsors/willfarrell",
	},
};
</script>

<BodyFooter navLinks={navLinks}>
    {#snippet logo()}
        <A href="/">
          <Image
            src="/img/datastream-logo.svg"
            alt="datastream logo, to the homepage"
            width="48"
			height="48"
          />
        </A>
    {/snippet}
</BodyFooter>
