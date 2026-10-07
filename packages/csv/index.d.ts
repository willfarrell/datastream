// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
import type {
	DatastreamPassThrough,
	DatastreamTransform,
	StreamOptions,
	StreamResult,
} from "@datastream/core";

export interface CsvDelimiters {
	delimiterChar?: string;
	newlineChar?: string;
	quoteChar?: string;
	escapeChar?: string;
}

export interface CsvParserOptions extends CsvDelimiters {
	numCols?: number;
	idx?: number;
	maxFieldSize?: number | null;
	delimiterCharCode?: number;
	delimiterCharLength?: number;
	delimiterCharSingle?: boolean;
	newlineCharCode?: number;
	newlineCharSingle?: boolean;
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
	/** Row indexes, capped at the stream's `maxErrorRows`. */
	idx: number[];
	/** True number of failing rows (stream results always include it). */
	count?: number;
}

export function csvDetectDelimitersStream(
	options?: {
		maxBufferSize?: number | null;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamPassThrough & {
	result: () => StreamResult<CsvDelimiters>;
};

export function csvDetectHeaderStream(
	options?: {
		maxBufferSize?: number | null;
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
): DatastreamPassThrough & {
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
		maxFieldSize?: number | null;
		maxErrorRows?: number | null;
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
): DatastreamTransform & {
	result: () => StreamResult<Record<string, CsvError>>;
};

export function csvRemoveMalformedRowsStream(
	options?: {
		headers?: string[] | (() => string[]);
		onErrorEnqueue?: boolean;
		maxErrorRows?: number | null;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform & {
	result: () => StreamResult<Record<string, CsvError>>;
};

export function csvRemoveEmptyRowsStream(
	options?: {
		onErrorEnqueue?: boolean;
		maxErrorRows?: number | null;
		resultKey?: string;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform & {
	result: () => StreamResult<Record<string, CsvError>>;
};

export type CsvCoerceType = "number" | "boolean" | "null" | "date" | "json";

export function csvCoerceValuesStream(
	options?: {
		columns?: Record<string, CsvCoerceType>;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function csvInjectHeaderStream(
	options: {
		header: string[];
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function csvFormatStream(
	options?: CsvDelimiters & {
		escapeFormulae?: boolean;
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function csvArrayToObjectStream(
	options: {
		headers: string[] | (() => string[]);
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;

export function csvObjectToArrayStream(
	options: {
		headers: string[] | (() => string[]);
	},
	streamOptions?: StreamOptions,
): DatastreamTransform;
