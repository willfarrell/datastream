/* global TextDecoderStream */

export const charsetDecodeStream = ({ charset } = {}, _streamOptions = {}) => {
	// doesn't support signal?
	return new TextDecoderStream(charset);
};

export default charsetDecodeStream;
