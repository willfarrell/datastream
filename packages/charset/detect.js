import { createPassThroughStream } from '@datastream/core'
import detect from 'charset-detector'

const charsets = {
  'UTF-8': 0,
  'UTF-16BE': 0,
  'UTF-16LE': 0,
  'UTF-32BE': 0,
  'UTF-32LE': 0,
  Shift_JIS: 0,
  'ISO-2022-JP': 0,
  'ISO-2022-CN': 0,
  'ISO-2022-KR': 0,
  GB18030: 0,
  'EUC-JP': 0,
  'EUC-KR': 0,
  Big5: 0,
  'ISO-8859-1': 0,
  'ISO-8859-2': 0,
  'ISO-8859-5': 0,
  'ISO-8859-6': 0,
  'ISO-8859-7': 0,
  'ISO-8859-8-I': 0,
  'ISO-8859-8': 0,
  'windows-1251': 0,
  'windows-1256': 0,
  'windows-1252': 0,
  'windows-1254': 0,
  'windows-1250': 0,
  'KOIR8-R': 0,
  'ISO-8859-9': 0
}

export const charsetDetectStream = ({ resultKey } = {}, streamOptions) => {
  const passThrough = (chunk) => {
    const matches = detect(chunk)
    if (matches.length) {
      for (const match of matches) {
        charsets[match.charsetName] += match.confidence
      }
    }
  }
  const stream = createPassThroughStream(passThrough, streamOptions)
  stream.result = () => {
    const values = Object.entries(charsets)
      .map(([charset, confidence]) => ({ charset, confidence }))
      .sort((a, b) => b.confidence - a.confidence)
    return { key: resultKey ?? 'charset', value: values[0] }
  }
  return stream
}

export default charsetDetectStream
