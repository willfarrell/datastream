/* global TextDecoderStream */

export const charsetDecodeStream = ({ charset } = {}, streamOptions) => {
  // doesn't support signal?
  return new TextDecoderStream(charset)
}

export default charsetDecodeStream
