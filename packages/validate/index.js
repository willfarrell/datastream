import { createTransformStream } from '@datastream/core'
import { compile } from 'ajv-cmd'

const ajvDefaults = {
  strict: true,
  coerceTypes: true,
  allErrors: true,
  useDefaults: 'empty',
  messages: true // needs to be true to allow multi-locale errorMessage to work
}

// This is pulled out due to it's performance cost (50-100ms on cold start)
// Precompile your schema during a build step is recommended.
export const transpileSchema = (schema, ajvOptions) => {
  const options = { ...ajvDefaults, ...ajvOptions }
  return compile(schema, options)
}

export const validateStream = (
  { schema, idxStart, resultKey, ...ajvOptions },
  streamOptions
) => {
  idxStart ??= 0

  if (typeof schema !== 'function') {
    schema = transpileSchema(schema)
  }

  const value = {} // aka errors
  let idx = idxStart - 1
  const transform = (chunk, enqueue) => {
    idx += 1

    const chunkValid = schema(chunk)
    // console.log({ chunkValid })
    if (!chunkValid) {
      for (const error of schema.errors) {
        const { id, keys, message } = processError(error)

        if (!value[id]) {
          value[id] = { id, keys, message, idx: [] }
        }
        value[id].idx.push(idx)
      }
    }
    enqueue(chunk) // TODO option to not pass chunk on?
  }
  const stream = createTransformStream(transform, streamOptions)
  stream.result = () => ({ key: resultKey ?? 'validate', value })
  return stream
}

const processError = (error) => {
  const message = error.message || ''

  let id = error.schemaPath

  let keys = []
  if (error.keyword === 'errorMessage') {
    error.params.errors.forEach((error) => {
      const value = makeKeys(error)
      if (value) keys.push(value)
    })
    keys = [...new Set(keys.sort())]
  } else {
    keys.push(makeKeys(error))
  }
  if (!error.instancePath && keys.length) {
    id += `/${keys.join('|')}`
  }
  return { id, keys, message }
}

const makeKeys = (error) => {
  // deps groups columns that are related in anyOf/oneOf.
  /* error.params.deps ?? */
  return (
    error.params.missingProperty ||
    error.params.additionalProperty ||
    error.instancePath.replace('/', '')
  )
}

export default validateStream
