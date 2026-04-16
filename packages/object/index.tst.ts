/// <reference lib="dom" />
/// <reference types="node" />
import {
	objectBatchStream,
	objectCountStream,
	objectFromEntriesStream,
	objectKeyJoinStream,
	objectKeyMapStream,
	objectKeyValueStream,
	objectKeyValuesStream,
	objectOmitStream,
	objectPickStream,
	objectPivotLongToWideStream,
	objectPivotWideToLongStream,
	objectReadableStream,
	objectSkipConsecutiveDuplicatesStream,
	objectToEntriesStream,
	objectValueMapStream,
} from "@datastream/object";
import { describe, expect, test } from "tstyche";

describe("objectReadableStream", () => {
	test("accepts array input", () => {
		expect(objectReadableStream([{ a: 1 }])).type.not.toBeAssignableTo<never>();
	});

	test("accepts no input", () => {
		expect(objectReadableStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("objectCountStream", () => {
	test("returns stream with result", () => {
		const stream = objectCountStream();
		expect(stream.result()).type.not.toBeAssignableTo<never>();
	});

	test("accepts resultKey", () => {
		expect(
			objectCountStream({ resultKey: "total" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectBatchStream", () => {
	test("requires keys", () => {
		expect(
			objectBatchStream({ keys: ["id"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectPivotLongToWideStream", () => {
	test("requires keys and valueParam", () => {
		expect(
			objectPivotLongToWideStream({ keys: ["a"], valueParam: "v" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts optional delimiter", () => {
		expect(
			objectPivotLongToWideStream({
				keys: ["a"],
				valueParam: "v",
				delimiter: "-",
			}),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectPivotWideToLongStream", () => {
	test("requires keys", () => {
		expect(
			objectPivotWideToLongStream({ keys: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts optional keyParam and valueParam", () => {
		expect(
			objectPivotWideToLongStream({
				keys: ["a"],
				keyParam: "k",
				valueParam: "v",
			}),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectKeyValueStream", () => {
	test("requires key and value", () => {
		expect(
			objectKeyValueStream({ key: "name", value: "score" }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectKeyValuesStream", () => {
	test("requires key", () => {
		expect(
			objectKeyValuesStream({ key: "name" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts optional values", () => {
		expect(
			objectKeyValuesStream({ key: "name", values: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectKeyJoinStream", () => {
	test("requires keys and separator", () => {
		expect(
			objectKeyJoinStream({
				keys: { full: ["first", "last"] },
				separator: " ",
			}),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectKeyMapStream", () => {
	test("requires keys mapping", () => {
		expect(
			objectKeyMapStream({ keys: { oldName: "newName" } }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectValueMapStream", () => {
	test("requires key and values", () => {
		expect(
			objectValueMapStream({
				key: "status",
				values: { "0": "inactive", "1": "active" },
			}),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectPickStream", () => {
	test("requires keys array", () => {
		expect(
			objectPickStream({ keys: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectOmitStream", () => {
	test("requires keys array", () => {
		expect(
			objectOmitStream({ keys: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectFromEntriesStream", () => {
	test("accepts keys array", () => {
		expect(
			objectFromEntriesStream({ keys: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts keys function", () => {
		expect(
			objectFromEntriesStream({ keys: () => ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectToEntriesStream", () => {
	test("accepts keys array", () => {
		expect(
			objectToEntriesStream({ keys: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("objectSkipConsecutiveDuplicatesStream", () => {
	test("accepts no options", () => {
		expect(
			objectSkipConsecutiveDuplicatesStream(),
		).type.not.toBeAssignableTo<never>();
	});
});
