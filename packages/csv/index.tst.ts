/// <reference lib="dom" />
/// <reference types="node" />
import type {
	CsvCoerceType,
	CsvDelimiters,
	CsvError,
	CsvParserResult,
} from "@datastream/csv";
import {
	csvArrayToObjectStream,
	csvCoerceValuesStream,
	csvDetectDelimitersStream,
	csvDetectHeaderStream,
	csvFormatStream,
	csvInjectHeaderStream,
	csvObjectToArrayStream,
	csvParseStream,
	csvQuotedParser,
	csvRemoveEmptyRowsStream,
	csvRemoveMalformedRowsStream,
	csvUnquotedParser,
} from "@datastream/csv";
import { describe, expect, test } from "tstyche";

describe("CsvDelimiters", () => {
	test("has optional delimiter properties", () => {
		expect<CsvDelimiters>().type.toBeAssignableTo<{
			delimiterChar?: string;
			newlineChar?: string;
			quoteChar?: string;
			escapeChar?: string;
		}>();
	});
});

describe("CsvParserResult", () => {
	test("has required fields", () => {
		expect<CsvParserResult>().type.toBeAssignableTo<{
			rows: string[][];
			tail: string;
			numCols: number;
			idx: number;
		}>();
	});
});

describe("csvDetectDelimitersStream", () => {
	test("returns stream with result", () => {
		const stream = csvDetectDelimitersStream();
		expect(stream.result).type.not.toBeAssignableTo<never>();
	});

	test("rejects removed chunkSize", () => {
		expect(csvDetectDelimitersStream).type.not.toBeCallableWith({
			chunkSize: 2048,
		});
	});

	test("accepts maxBufferSize", () => {
		expect(
			csvDetectDelimitersStream({ maxBufferSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts null maxBufferSize (unlimited)", () => {
		expect(
			csvDetectDelimitersStream({ maxBufferSize: null }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects non-number maxBufferSize", () => {
		expect(csvDetectDelimitersStream).type.not.toBeCallableWith({
			maxBufferSize: "1024",
		});
	});
});

describe("csvDetectHeaderStream", () => {
	test("returns stream with result", () => {
		const stream = csvDetectHeaderStream();
		expect(stream.result).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxBufferSize", () => {
		expect(
			csvDetectHeaderStream({ maxBufferSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts null maxBufferSize (unlimited)", () => {
		expect(
			csvDetectHeaderStream({ maxBufferSize: null }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects removed chunkSize", () => {
		expect(csvDetectHeaderStream).type.not.toBeCallableWith({
			chunkSize: 2048,
		});
	});

	test("rejects non-number maxBufferSize", () => {
		expect(csvDetectHeaderStream).type.not.toBeCallableWith({
			maxBufferSize: "1024",
		});
	});
});

describe("csvQuotedParser", () => {
	test("returns CsvParserResult", () => {
		expect(csvQuotedParser("a,b\n1,2")).type.toBe<CsvParserResult>();
	});

	test("accepts options and isFlushing", () => {
		expect(
			csvQuotedParser("a,b", { delimiterChar: "," }, true),
		).type.toBe<CsvParserResult>();
	});
});

describe("csvUnquotedParser", () => {
	test("returns CsvParserResult", () => {
		expect(csvUnquotedParser("a,b\n1,2")).type.toBe<CsvParserResult>();
	});
});

describe("csvParseStream", () => {
	test("accepts no options", () => {
		expect(csvParseStream()).type.not.toBeAssignableTo<never>();
	});

	test("accepts parser options", () => {
		expect(
			csvParseStream({ delimiterChar: "," }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects removed chunkSize", () => {
		expect(csvParseStream).type.not.toBeCallableWith({ chunkSize: 1024 });
	});

	test("accepts maxFieldSize", () => {
		expect(
			csvParseStream({ maxFieldSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
		expect(
			csvParseStream({ maxFieldSize: null }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects renamed fieldMaxSize", () => {
		expect(csvParseStream).type.not.toBeCallableWith({ fieldMaxSize: 1024 });
	});

	test("accepts null maxErrorRows (unlimited)", () => {
		expect(
			csvParseStream({ maxErrorRows: null }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts lazy delimiter options", () => {
		expect(
			csvParseStream({ delimiterChar: () => "," }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxErrorRows", () => {
		expect(
			csvParseStream({ maxErrorRows: 10 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects non-number maxErrorRows", () => {
		expect(csvParseStream).type.not.toBeCallableWith({ maxErrorRows: "10" });
	});

	test("result errors carry a count", () => {
		expect<CsvError["count"]>().type.toBe<number | undefined>();
	});
});

describe("csvRemoveMalformedRowsStream", () => {
	test("accepts no options", () => {
		expect(csvRemoveMalformedRowsStream()).type.not.toBeAssignableTo<never>();
	});

	test("accepts headers option", () => {
		expect(
			csvRemoveMalformedRowsStream({ headers: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxErrorRows", () => {
		expect(
			csvRemoveMalformedRowsStream({ maxErrorRows: 10 }),
		).type.not.toBeAssignableTo<never>();
		expect(
			csvRemoveMalformedRowsStream({ maxErrorRows: null }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("csvRemoveEmptyRowsStream", () => {
	test("accepts no options", () => {
		expect(csvRemoveEmptyRowsStream()).type.not.toBeAssignableTo<never>();
	});

	test("accepts maxErrorRows", () => {
		expect(
			csvRemoveEmptyRowsStream({ maxErrorRows: 10 }),
		).type.not.toBeAssignableTo<never>();
		expect(
			csvRemoveEmptyRowsStream({ maxErrorRows: null }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("csvCoerceValuesStream", () => {
	test("accepts column types", () => {
		expect(
			csvCoerceValuesStream({ columns: { age: "number", active: "boolean" } }),
		).type.not.toBeAssignableTo<never>();
	});

	test("has no result or resultKey", () => {
		expect(csvCoerceValuesStream).type.not.toBeCallableWith({
			resultKey: "coerce",
		});
		expect(csvCoerceValuesStream()).type.not.toHaveProperty("result");
	});

	test("CsvCoerceType values", () => {
		expect<"number">().type.toBeAssignableTo<CsvCoerceType>();
		expect<"boolean">().type.toBeAssignableTo<CsvCoerceType>();
		expect<"null">().type.toBeAssignableTo<CsvCoerceType>();
		expect<"date">().type.toBeAssignableTo<CsvCoerceType>();
		expect<"json">().type.toBeAssignableTo<CsvCoerceType>();
		expect<"string">().type.not.toBeAssignableTo<CsvCoerceType>();
	});
});

describe("csvInjectHeaderStream", () => {
	test("requires header", () => {
		expect(
			csvInjectHeaderStream({ header: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("csvFormatStream", () => {
	test("accepts no options", () => {
		expect(csvFormatStream()).type.not.toBeAssignableTo<never>();
	});

	test("accepts delimiter options", () => {
		expect(
			csvFormatStream({ delimiterChar: "\t" }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts escapeFormulae", () => {
		expect(
			csvFormatStream({ escapeFormulae: false }),
		).type.not.toBeAssignableTo<never>();
	});

	test("rejects non-boolean escapeFormulae", () => {
		expect(csvFormatStream).type.not.toBeCallableWith({ escapeFormulae: "no" });
	});
});

describe("csvArrayToObjectStream", () => {
	test("requires headers", () => {
		expect(
			csvArrayToObjectStream({ headers: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts lazy headers", () => {
		expect(
			csvArrayToObjectStream({ headers: () => ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("csvObjectToArrayStream", () => {
	test("requires headers", () => {
		expect(
			csvObjectToArrayStream({ headers: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});
