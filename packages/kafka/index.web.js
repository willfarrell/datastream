// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT

// @datastream/kafka wraps kafkajs, which speaks the Kafka wire protocol over raw
// TCP sockets — unavailable in the browser. There is no Web Streams equivalent,
// so the browser/`webstream` export resolves to these stubs: importing the
// package stays side-effect free (safe for bundlers and SSR), but calling any
// export fails loudly instead of silently returning from an empty module. Use
// this package from Node.js.
const unsupported = (name) => () => {
	throw new Error(
		`${name} is not supported in the browser: @datastream/kafka requires Node.js (kafkajs over TCP).`,
	);
};

export const kafkaConnect = unsupported("kafkaConnect");
export const kafkaProduceStream = unsupported("kafkaProduceStream");
export const kafkaConsumeStream = unsupported("kafkaConsumeStream");

export default {
	connect: kafkaConnect,
	produceStream: kafkaProduceStream,
	consumeStream: kafkaConsumeStream,
};
