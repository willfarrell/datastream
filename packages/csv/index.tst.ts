import type {
	CsvCoerceType,
	CsvDelimiters,
	CsvParserResult,
} from "@datastream/csv";
import {
	csvArrayToObject,
	csvCoerceValuesStream,
	csvDetectDelimitersStream,
	csvDetectHeaderStream,
	csvFormatStream,
	csvInjectHeaderStream,
	csvObjectToArray,
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

	test("accepts chunkSize", () => {
		expect(
			csvDetectDelimitersStream({ chunkSize: 2048 }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("csvDetectHeaderStream", () => {
	test("returns stream with result", () => {
		const stream = csvDetectHeaderStream();
		expect(stream.result).type.not.toBeAssignableTo<never>();
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
			csvParseStream({ delimiterChar: ",", chunkSize: 1024 }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts lazy delimiter options", () => {
		expect(
			csvParseStream({ delimiterChar: () => "," }),
		).type.not.toBeAssignableTo<never>();
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
});

describe("csvRemoveEmptyRowsStream", () => {
	test("accepts no options", () => {
		expect(csvRemoveEmptyRowsStream()).type.not.toBeAssignableTo<never>();
	});
});

describe("csvCoerceValuesStream", () => {
	test("accepts column types", () => {
		expect(
			csvCoerceValuesStream({ columns: { age: "number", active: "boolean" } }),
		).type.not.toBeAssignableTo<never>();
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
});

describe("csvArrayToObject", () => {
	test("requires headers", () => {
		expect(
			csvArrayToObject({ headers: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});

	test("accepts lazy headers", () => {
		expect(
			csvArrayToObject({ headers: () => ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});

describe("csvObjectToArray", () => {
	test("requires headers", () => {
		expect(
			csvObjectToArray({ headers: ["a", "b"] }),
		).type.not.toBeAssignableTo<never>();
	});
});
