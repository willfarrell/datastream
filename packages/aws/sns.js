import { PublishBatchCommand, SNSClient } from "@aws-sdk/client-sns";
import { createWritableStream } from "@datastream/core";

const awsClientDefaults = {
	// https://aws.amazon.com/compliance/fips/
	useFipsEndpoint: [
		"us-east-1",
		"us-east-2",
		"us-west-1",
		"us-west-2",
	].includes(process.env.AWS_REGION),
};

let client = new SNSClient(awsClientDefaults);
export const awsSNSSetClient = (snsClient) => {
	client = snsClient;
};

export const awsSNSPublishMessageStream = (options, streamOptions) => {
	let batch = [];
	const send = () => {
		options.PublishBatchRequestEntries = batch;
		batch = [];
		return client.send(new PublishBatchCommand(options));
	};
	const write = async (chunk) => {
		if (batch.length === 10) {
			await send();
		}
		batch.push(chunk);
	};
	const final = send;
	return createWritableStream(write, final, streamOptions);
};

export default {
	setClient: awsSNSSetClient,
	publishMessageStream: awsSNSPublishMessageStream,
};
