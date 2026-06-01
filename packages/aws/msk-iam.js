// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import { generateAuthToken } from "aws-msk-iam-sasl-signer-js";

// SECURITY: `awsDebugCreds: true` is forwarded verbatim to
// aws-msk-iam-sasl-signer-js's generateAuthToken, which then LOGS the resolved
// AWS access key id / credential details. This leaks credential material into
// application logs. Never enable awsDebugCreds in production; treat it as a
// local-debugging-only flag.
export const awsMskIamMechanism = ({ region, awsDebugCreds, ttl } = {}) => {
	if (!region) {
		throw new TypeError("awsMskIamMechanism: region required");
	}
	return {
		mechanism: "oauthbearer",
		oauthBearerProvider: async () => {
			const { token, expiryTime } = await generateAuthToken({
				region,
				awsDebugCreds,
				ttl,
			});
			return { value: token, expiryTime };
		},
	};
};

export default { mechanism: awsMskIamMechanism };
