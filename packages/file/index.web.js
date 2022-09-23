import { createReadableStream } from '@datastream/core'

export const fileReadStream = async ({types}, streamOptions) => {
  const [fileHandle] = await window.showOpenFilePicker({ types })
  const fileData = await fileHandle.getFile();
  return createReadableStream(fileData)
}

export const fileWriteStream = async ({ path, types }, streamOptions) => {
  const fileHandle = await window.showSaveFilePicker({suggestedName:path, types})
  return fileHandle.createWritable()
}

export default {
  readStream: fileReadStream,
  writeStream: fileWriteStream
}
  