export const indexedDBReadStream = async (_option, _streamOptions = {}) => {
	throw new Error("indexedDBReadStream: Not supported");
};

export const indexedDBWriteStream = async (_options, _streamOptions = {}) => {
	throw new Error("indexedDBWriteStream: Not supported");
};

export default {
	readStream: indexedDBReadStream,
	writeStream: indexedDBWriteStream,
};
