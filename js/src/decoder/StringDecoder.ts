import { IntWrapper } from './IntWrapper';
import { StreamMetadataDecoder } from '../metadata/stream/StreamMetadataDecoder';
import { IntegerDecoder } from './IntegerDecoder';
import { BitSet } from 'bitset';

const textDecoder = new TextDecoder("utf-8");

export class StringDecoder {

    /*
     * String column layouts:
     * -> plain -> present, length, data
     * -> dictionary -> present, length, dictionary, data
     * -> fsst dictionary -> symbolTable, symbolLength, dictionary, length, present, data
     * */

    public static decode(
        data: Uint8Array, offset: IntWrapper, numStreams: number,
        presentStream: BitSet, numValues: number) {
        let dictionaryLengthStream: Int32Array = null;
        let offsetStream: Int32Array = null;
        const dataStream: Uint8Array = null;
        let dictionaryStream: Uint8Array = null;
        /* eslint-disable @typescript-eslint/no-unused-vars */
        let symbolLengthStream: Int32Array = null;
        let symbolTableStream: Uint8Array = null;

        for (let i = 0; i < numStreams; i++) {
            const streamMetadata = StreamMetadataDecoder.decode(data, offset);
            switch (streamMetadata.physicalStreamType()) {
                case 'OFFSET': {
                    offsetStream = IntegerDecoder.decodeIntStream(data, offset, streamMetadata, false);
                    break;
                }
                case 'LENGTH': {
                    const ls = IntegerDecoder.decodeIntStream(data, offset, streamMetadata, false);
                    if (streamMetadata.logicalStreamType().lengthType() === 'DICTIONARY') {
                        dictionaryLengthStream = ls;
                    } else {
                        symbolLengthStream = ls;
                    }
                    break;
                }
                case 'DATA': {
                    const ds = data.slice(offset.get(), offset.get() + streamMetadata.byteLength());
                    offset.add(streamMetadata.byteLength());
                    if (streamMetadata.logicalStreamType().dictionaryType() === 'SINGLE') {
                        dictionaryStream = ds;
                    } else {
                        symbolTableStream = ds;
                    }
                    break;
                }
                default:
                    console.log("StringDecoder encountered unknown stream type: " + streamMetadata.physicalStreamType());
                    return;
            }
        }

        if (symbolTableStream) {
            throw new Error("TODO: FSST decoding for strings is not yet implemented");
        } else if (dictionaryStream) {
            return this.decodeDictionary(presentStream, dictionaryLengthStream, dictionaryStream, offsetStream, numValues);
        } else {
            return this.decodePlain(presentStream, dictionaryLengthStream, dataStream, numValues);
        }
    }

    private static decodePlain(presentStream: BitSet, lengthStream: Int32Array, utf8Values: Uint8Array, numValues: number): string[] {
        // TODO: preallocate?
        const decodedValues: string[] = [];
        let lengthOffset = 0;
        let strOffset = 0;
        for (let i = 0; i < numValues; i++) {
            const present = presentStream.get(i);
            if (present) {
                const length = lengthStream[lengthOffset++];
                // TODO: defer decoding / store in Uint8Array?
                const value = textDecoder.decode(utf8Values.slice(strOffset, strOffset + length));
                decodedValues.push(value);
                strOffset += length;
            }
        }
        return decodedValues;
    }

    private static decodeDictionary(
        presentStream: BitSet, lengthStream: Int32Array, utf8Values: Uint8Array,
        dictionaryOffsets: Int32Array, numValues: number
    ): string[] {
        // TODO: preallocate?
        const dictionary: string[] = [];
        let dictionaryOffset = 0;
        for (const length of lengthStream) {
            const value = textDecoder.decode(utf8Values.slice(dictionaryOffset, dictionaryOffset + length));
            dictionary.push(value);
            dictionaryOffset += length;
        }

        // TODO: preallocate?
        const values: string[] = [];
        let offset = 0;

        for (let i = 0; i < numValues; i++) {
            const present = presentStream.get(i);
            if (present) {
                const value = dictionary[dictionaryOffsets[offset++]];
                values[i] = value;
            }
        }

        return values;
    }
}
