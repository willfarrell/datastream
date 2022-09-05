# core

## Functions
- `pipeline(stream[], steamOptions)`: Connects streams and awaits until completion. Returns results from stream taps. Will add in a terminating Writable if missing.
- `pipejoin(stream[])`: Connects streams and returns resulting stream for use with async iterators
- `result(stream)`: Run and combine streams result responses

- `streamToArray(stream)`: Returns array from stream chunks. stream must not end with Writable.
- `streamToString(stream)`: Returns string from stream chunks. stream must not end with Writable.
- `streamToObject(stream)`: Returns object from stream chunks. stream must not end with Writable.
- `isReadable(stream)`: Return bool is stream is Readable
- `isWritable(stream)`: Return bool is stream is Writable
- `makeOptions(options)`: Make options interoperable between Readable/Writable and Transform
- `creatReadableStream(input = '', steamOptions)`: Create a Readable stream from input (string, array, iterable) with options.
- `creatPassThroughStream((chunk)=>{}, steamOptions)`: Create a Pass Through stream that allows observation of chunk while being passed through.
- `creatTransformStream((chunk, enqueue)=>{}, steamOptions)`: Create a Transform stream that allows mutation of chunk before being passed.
- `creatWritableStream((chunk)=>{}, steamOptions)`: Create a Writable stream that allows mutation of chunk before being passed.
- `tee(stream)`: 
- `timeout(ms, {signal})`: setTimeout promise that can be aborted.

- `steamOptions`:
  - `highWaterMark`
  - `chunkSize`
  - `signal`

## Examples

```javascript
import {
  pipejoin,
  streamToArray,
  createReadableStream,
  createTransformStream
} from '@datastream/core'
import { csvParseStream } from '@datastream/csv'

let count
const streams = [
  createReadableStream('a,b,c\r\n1,2,3'),
  createTransformStream((chunk, enqueue) => {
	chunk.b += 1
	enqueue(chunk)
  }),
  createTransformStream(console.log)
]

const river = pipejoin(streams)
const output = await streamToArray(river)
```
