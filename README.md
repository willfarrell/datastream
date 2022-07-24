# datastream
Commonly used stream patterns for Web Streams API and NodeJS Stream.
- `@datastream/core`
  - pipeline
  - pipejoin
  - streamToArray
  - streamToString
  - isReadable
  - isWritable
  - makeOptions
  - createReadableStream
  - createTransformStream
  - createWritableStream
 
## Streams

- Readable: The start of a pipeline of streams that injects data into a stream.
- PassThrough: Does not modify the data, but listens to the data and prepares a result that can be retrieved.
- Transform: Modifies data as it passes through.
- Writable: The end of a pipeline of streams that stores data from the stream.

### Basics
- `@datastream/string`
  - stringReadableStream [Readable]
  - stringLengthStream [PassThrough]
  - stringOutputStream [PassThrough]
- `@datastream/object`
  - objectReadableStream [Readable]
  - objectCountStream [PassThrough]
  - objectBatchStream [Transform]
  - objectOutputStream [PassThrough]

### Common
- `@datastream/digest`
  - digestStream [PassThrough]

