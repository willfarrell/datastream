// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type { StreamOptions, StreamResult } from "@datastream/core";

export interface CsvDelimiters {
	delimiterChar?: string;
	newlineChar?: string;
	quoteChar?: string;
	escapeChar?: string;
}

export interface CsvParserOptions extends CsvDelimiters {
	numCols?: number;
	idx?: number;
	delimiterCharCode?: number;
	delimiterCharLength?: number;
	delimiterCharSingle?: boolean;
	newlineCharCode?: number;
	newlineCharSingle?: number;
	newlineCharLength?: number;
	quoteCharCode?: number;
	escapeCharCode?: number;
	escapeIsQuote?: boolean;
	escapedQuote?: string;
}

export interface CsvParserResult {
	rows: string[][];
	tail: string;
	numCols: number;
	idx: number;
	errors?: Record<string, CsvError>;
}

export interface CsvError {
	id: string;
	message: string;
	idx: number[];
}

export function csvDetectDelimitersStream(
	options?: {
		chunkSize?: number;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<CsvDelimiters>;
};

export function csvDetectHeaderStream(
	options?: {
		chunkSize?: number;
		parser?: (
			text: string,
			options: CsvParserOptions,
			isFlushing: boolean,
		) => CsvParserResult;
		delimiterChar?: string | (() => string);
		newlineChar?: string | (() => string);
		quoteChar?: string | (() => string);
		escapeChar?: string | (() => string);
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<{ header: string[] }>;
};

export function csvQuotedParser(
	text: string,
	options?: CsvParserOptions,
	isFlushing?: boolean,
): CsvParserResult;
export function csvUnquotedParser(
	text: string,
	options?: CsvParserOptions,
	isFlushing?: boolean,
): CsvParserResult;

export function csvParseStream(
	options?: {
		chunkSize?: number;
		resultKey?: string;
		parser?: (
			text: string,
			options: CsvParserOptions,
			isFlushing: boolean,
		) => CsvParserResult;
		delimiterChar?: string | (() => string);
		newlineChar?: string | (() => string);
		quoteChar?: string | (() => string);
		escapeChar?: string | (() => string);
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<Record<string, CsvError>>;
};

export function csvRemoveMalformedRowsStream(
	options?: {
		headers?: string[] | (() => string[]);
		onErrorEnqueue?: boolean;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<Record<string, CsvError>>;
};

export function csvRemoveEmptyRowsStream(
	options?: {
		onErrorEnqueue?: boolean;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<Record<string, CsvError>>;
};

export type CsvCoerceType = "number" | "boolean" | "null" | "date" | "json";

export function csvCoerceValuesStream(
	options?: {
		columns?: Record<string, CsvCoerceType>;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): unknown & {
	result: () => StreamResult<Record<string, unknown>>;
};

export function csvInjectHeaderStream(
	options: {
		header: string[];
	},
	streamOptions?: StreamOptions,
): unknown;

export function csvFormatStream(
	options?: CsvDelimiters,
	streamOptions?: StreamOptions,
): unknown;

export function csvArrayToObject(
	options: {
		headers: string[] | (() => string[]);
	},
	streamOptions?: StreamOptions,
): unknown;

export function csvObjectToArray(
	options: {
		headers: string[] | (() => string[]);
	},
	streamOptions?: StreamOptions,
): unknown;
