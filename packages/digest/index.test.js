import test from 'node:test'
import { equal } from 'node:assert'

import { pipeline, createReadableStream } from '@datastream/core'

import { digestStream } from '@datastream/digest'

let variant = 'unknown'
for (const execArgv of process.execArgv) {
  const flag = '--conditions='
  if (execArgv.includes('--conditions=')) {
    variant = execArgv.replace(flag, '')
  }
}

test(`${variant}: digestStream should calculate digest`, async (t) => {
  const streams = [
    createReadableStream('1,2,3,4'),
    await digestStream('SHA2-256')
  ]
  const result = await pipeline(streams)

  const { key, value } = streams[1].result()

  equal(key, 'digest')
  equal(
    result.digest,
    'SHA2-256:37db36876b9ccaaa88394679f019c3435af9320dea117e867003840317870e25'
  )
  equal(
    value,
    'SHA2-256:37db36876b9ccaaa88394679f019c3435af9320dea117e867003840317870e25'
  )
})
