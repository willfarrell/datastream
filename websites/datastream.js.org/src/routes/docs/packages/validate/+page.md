---
title: validate
description: JSON Schema validation for object streams using Ajv.
---

JSON Schema validation for object streams using Ajv.

## Install

```bash
npm install @datastream/validate
```

## `validateStream` <span class="badge">Transform</span>

Validates each object chunk against a JSON Schema. Invalid rows are dropped by default; errors are collected in `.result()`.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schema` | `object \| function` | — | JSON Schema object, or pre-compiled validation function |
| `idxStart` | `number` | `0` | Starting row index for error tracking |
| `onErrorEnqueue` | `boolean` | `false` | If `true`, invalid rows are kept in the stream |
| `allowCoerceTypes` | `boolean` | `true` | If `false`, emits original data without Ajv coercion |
| `resultKey` | `string` | `"validate"` | Key in pipeline result |

### Result

Errors grouped by schema path:

```javascript
{
  '#/required': {
    id: '#/required',
    keys: ['email'],
    message: "must have required property 'email'",
    idx: [3, 7, 15]
  },
  '#/properties/age/type': {
    id: '#/properties/age/type',
    keys: ['age'],
    message: 'must be number',
    idx: [5]
  }
}
```

### Example

```javascript
import { pipeline, createReadableStream } from '@datastream/core'
import { validateStream } from '@datastream/validate'

const schema = {
  type: 'object',
  required: ['name', 'age'],
  properties: {
    name: { type: 'string', minLength: 1 },
    age: { type: 'number', minimum: 0 },
    email: { type: 'string', format: 'email' },
  },
  additionalProperties: false,
}

const result = await pipeline([
  createReadableStream([
    { name: 'Alice', age: 30, email: 'alice@example.com' },
    { name: '', age: -1 },
    { name: 'Bob', age: 25 },
  ]),
  validateStream({ schema }),
])

console.log(result.validate)
// Errors for row 1 (name minLength, age minimum)
```

### Keep invalid rows

```javascript
validateStream({ schema, onErrorEnqueue: true })
```

### Pre-compiled schema

For better cold-start performance, pre-compile your schema during the build step:

```javascript
import { transpileSchema, validateStream } from '@datastream/validate'

const validate = transpileSchema(schema)
validateStream({ schema: validate })
```

## `transpileSchema`

Pre-compiles a JSON Schema into a validation function using Ajv.

### Ajv defaults

| Option | Default | Description |
|--------|---------|-------------|
| `strict` | `true` | Strict mode |
| `coerceTypes` | `true` | Coerce types to match schema |
| `allErrors` | `true` | Report all errors, not just the first |
| `useDefaults` | `"empty"` | Apply defaults for missing/empty values |
| `messages` | `true` | Include error messages |

```javascript
import { transpileSchema } from '@datastream/validate'

const validate = transpileSchema(schema, { coerceTypes: false })
```
