import { create } from 'ipfs-core'
import { createPassThroughStream } from '@datastream/core'

export const ipfsGetStream = async ({ node, repo, cid }, streamoptions) => {
  node ??= await create({ repo })
  return node.get(cid)
}

export const ipfsAddStream = async (
  { node, repo, resultKey } = {},
  streamOptions
) => {
  node ??= await create({ repo })

  const stream = createPassThroughStream(() => {}, streamOptions)
  const { cid } = node.add(stream)

  stream.result = () => ({
    key: resultKey ?? 'cid',
    value: cid
  })
  return stream
}

export default {
  getStream: ipfsGetStream,
  addStream: ipfsAddStream
}
