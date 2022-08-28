/* global TextDecoderStream */

export const charsetDecodeStream = (charset = 'UTF-8', streamOptions) => {
  // doesn't support signal?
  return new TextDecoderStream(charset)
}

export default charsetDecodeStream
