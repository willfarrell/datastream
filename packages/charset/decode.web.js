// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
/* global TextDecoderStream */

const supportedEncodings = new Set([
	"utf-8",
	"utf-16le",
	"utf-16be",
	"ibm866",
	"iso-8859-2",
	"iso-8859-3",
	"iso-8859-4",
	"iso-8859-5",
	"iso-8859-6",
	"iso-8859-7",
	"iso-8859-8",
	"iso-8859-8-i",
	"iso-8859-10",
	"iso-8859-13",
	"iso-8859-14",
	"iso-8859-15",
	"iso-8859-16",
	"koi8-r",
	"koi8-u",
	"macintosh",
	"windows-874",
	"windows-1250",
	"windows-1251",
	"windows-1252",
	"windows-1253",
	"windows-1254",
	"windows-1255",
	"windows-1256",
	"windows-1257",
	"windows-1258",
	"x-mac-cyrillic",
	"gbk",
	"gb18030",
	"big5",
	"euc-jp",
	"iso-2022-jp",
	"shift_jis",
	"euc-kr",
	"replacement",
	"x-user-defined",
]);

export const charsetDecodeStream = ({ charset } = {}, _streamOptions = {}) => {
	if (charset !== null && !supportedEncodings.has(charset.toLowerCase())) {
		throw new Error(
			`charsetDecodeStream: Unsupported web encoding "${charset}"`,
		);
	}
	return new TextDecoderStream(charset);
};

export default charsetDecodeStream;
