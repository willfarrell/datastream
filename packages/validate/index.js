import { createTransformStream } from '@datastream/core'
import _ajv from 'ajv/dist/2020.js'
import formats from 'ajv-formats'
import formatsDraft2019 from 'ajv-formats-draft2019'
// import ajvErrors from 'ajv-errors'
import uriResolver from 'fast-uri'

const Ajv = _ajv.default // esm workaround for linting

const ajvDefaults = {
  strict: true,
  coerceTypes: true,
  allErrors: true,
  useDefaults: 'empty',
  uriResolver
}

export const validateStream = (
  { schema, idxStart, resultKey, ...ajvOptions },
  streamOptions
) => {
  idxStart ??= 0
  // language ??= 'en'

  if (typeof schema !== 'function') {
    const ajv = new Ajv({ ...ajvDefaults, ...ajvOptions })
    formats(ajv)
    formatsDraft2019(ajv)
    // ajvErrors(ajv)
    schema = ajv.compile(schema)
  }

  const value = {} // aka errors
  let idx = idxStart - 1
  const transform = (chunk, enqueue) => {
    idx += 1

    const chunkValid = schema(chunk)
    // console.log({ chunkValid })
    if (!chunkValid) {
      // if (availableLanguages) {
      //   availableLanguages[language](chunkSchema.errors)
      // }

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
