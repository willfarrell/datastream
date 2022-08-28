/* global TextEncoderStream */

export const charsetDecodeStream = (charset = 'UTF-8', streamOptions) => {
  // doesn't support signal?
  return new TextEncoderStream(charset)
}

export default charsetDecodeStream
