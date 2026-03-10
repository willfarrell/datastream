<script>
import Card from "@design-system/components/Card.svelte";
import Codeblock from "@design-system/components/Codeblock.svelte";
import H1 from "@design-system/components/Heading1.svelte";
import H2 from "@design-system/components/Heading2.svelte";
import H3 from "@design-system/components/Heading3.svelte";
import Icon from "@design-system/components/Icon.svelte";
import Image from "@design-system/components/Image.svelte";
import LayoutSlices from "@design-system/components/LayoutSlices.svelte";
import A from "@design-system/elements/a.svelte";
import Div from "@design-system/elements/div.svelte";
import Li from "@design-system/elements/li.svelte";
import P from "@design-system/elements/p.svelte";
import Section from "@design-system/elements/section.svelte";
import Span from "@design-system/elements/span.svelte";
import Ul from "@design-system/elements/ul.svelte";

const codeExample = `import { pipeline, createReadableStream } from '@datastream/core'
import { csvParseStream } from '@datastream/csv'
import { validateStream } from '@datastream/validate'
import { gzipCompressStream } from '@datastream/compress'

const streams = [
  createReadableStream(csvData),
  csvParseStream({ header: true }),
  validateStream(schema),
  gzipCompressStream()
]

await pipeline(streams)`;
</script>

<svelte:head>
    <title>datastream</title>
    <meta
        name="description"
        content="Streams made easy. Commonly used stream patterns for Web Streams API and NodeJS Stream."
    />
</svelte:head>
<LayoutSlices>
    <Section class="slice hero vertical">
        <Div class="inline-center">
            <Image
                alt="datastream logo"
                src="/img/datastream-logo.svg"
                width="96"
                height="96"
                decoding="auto"
                fetchpriority="high"
                loading="eager"
            />
            <H1>&lt;datastream&gt;</H1>

            <P
                >Streams made easy. Commonly used stream patterns for Web Streams API and NodeJS Stream.
                Modular, composable, and lightweight.</P
            >
            <Div role="group">
                <P>
                    <A href="/docs" class="button"
                        ><Span>Get started</Span>
                        <Icon src="/img/icons.svg#arrow-inline-end" /></A
                    ></P
                ></Div
            >
            <H2 class="visually-hidden">Strong community</H2>
            <Ul class="grid">
                <Li class="stat">
                    <H3
                        ><Icon src="/img/icons.svg#download" />
                        <Span>100k+</Span></H3
                    >
                    <P>Weekly downloads</P>
                </Li>
                <Li class="stat">
                    <H3
                        ><Icon src="/img/icons.svg#feather" />
                        <Span>Lightweight</Span></H3
                    >
                    <P>Fewer dependencies</P>
                </Li>
                <Li class="stat">
                    <H3
                        ><Icon src="/img/icons.svg#list-check" />
                        <Span>96%+</Span></H3
                    >
                    <P>Test coverage</P>
                </Li>
            </Ul>
        </Div>
    </Section>
    <Section class="slice grid">
        <Div class="inline-center">
            <H2>Everything you need for stream processing</H2>
        </Div>
        <Ul class="grid">
            <Li class="card feature">
                <H3>Dual platform</H3>
                <P
                    >Works with both Web Streams API and Node.js Streams.
                    Write once, run anywhere — server, browser, or edge.</P
                >
            </Li>
            <Li class="card feature">
                <H3>Modular</H3>
                <P
                    >Install only what you need. Each stream type is a separate
                    package, keeping your bundle size minimal.</P
                >
            </Li>
            <Li class="card feature">
                <H3>Composable pipeline</H3>
                <P
                    >Chain readable, transform, and writable streams together
                    with a simple <code>pipeline()</code> call. Build complex
                    data flows from simple parts.</P
                >
            </Li>
            <Li class="card feature">
                <H3>Common patterns included</H3>
                <P
                    >CSV parsing, compression, character encoding, validation,
                    base64, cryptographic digests — all the patterns you
                    commonly need, ready to use.</P
                >
            </Li>
            <Li class="card feature">
                <H3>AWS integrations</H3>
                <P
                    >Stream data to and from DynamoDB, S3, SQS, SNS, and Lambda
                    with purpose-built stream adapters.</P
                >
            </Li>
            <Li class="card feature">
                <H3>Lightweight</H3>
                <P
                    >Zero-bloat design. Each package has minimal dependencies
                    and small install size, perfect for serverless and
                    edge environments.</P
                >
            </Li>
        </Ul>
    </Section>
    <Section class="slice vertical">
        <Div class="inline-center">
            <H2>Show me the code!</H2>
            <P
                >Build a complete data pipeline in just a few lines:</P
            >
        </Div>
        <Div class="inline-center">
            <Codeblock code={codeExample}>{codeExample}</Codeblock>
        </Div>
    </Section>
    <Section class="slice vertical">
        <Div class="inline-center">
            <H2>Packages</H2>
            <P>Explore the available stream packages.</P>
        </Div>
        <Ul class="grid">
            <Card href="/docs/packages/core" class="package" id="core">
                <H3><A href="/docs/packages/core">core</A></H3>
                <P>Pipeline, stream utilities, and factory functions.</P>
            </Card>
            <Card href="/docs/packages/aws" class="package" id="aws">
                <H3><A href="/docs/packages/aws">aws</A></H3>
                <P>DynamoDB, S3, SQS, SNS, and Lambda streams.</P>
            </Card>
            <Card href="/docs/packages/csv" class="package" id="csv">
                <H3><A href="/docs/packages/csv">csv</A></H3>
                <P>Parse and format CSV data.</P>
            </Card>
            <Card href="/docs/packages/compress" class="package" id="compress">
                <H3><A href="/docs/packages/compress">compress</A></H3>
                <P>Brotli, gzip, deflate, and zstd compression.</P>
            </Card>
            <Card href="/docs/packages/charset" class="package" id="charset">
                <H3><A href="/docs/packages/charset">charset</A></H3>
                <P>Character encoding detection, decoding, and encoding.</P>
            </Card>
            <Card href="/docs/packages/validate" class="package" id="validate">
                <H3><A href="/docs/packages/validate">validate</A></H3>
                <P>Schema validation for stream data.</P>
            </Card>
            <Card href="/docs/packages/object" class="package" id="object">
                <H3><A href="/docs/packages/object">object</A></H3>
                <P>Object manipulation, batching, pivoting, and filtering.</P>
            </Card>
            <Card href="/docs/packages/string" class="package" id="string">
                <H3><A href="/docs/packages/string">string</A></H3>
                <P>String streams, splitting, replacing, and counting.</P>
            </Card>
        </Ul>
    </Section>
    <Section class="slice vertical">
        <Div class="inline-center">
            <H2>Ready to get started?</H2><P>
                Install datastream in seconds and start building stream
                pipelines today.
            </P>
            <A href="/docs" class="button"
                ><Span>Read the docs</Span>
                <Icon src="/img/icons.svg#arrow-inline-end" />
            </A>
        </Div>
    </Section>
</LayoutSlices>
