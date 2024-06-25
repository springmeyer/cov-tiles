import { StreamMetadata } from '../metadata/stream/StreamMetadata';
import { StreamMetadataDecoder } from '../metadata/stream/StreamMetadataDecoder';
import { IntWrapper } from './IntWrapper';
import { DecodingUtils } from './DecodingUtils';
import { IntegerDecoder } from './IntegerDecoder';
import { FloatDecoder } from './FloatDecoder';
import { DoubleDecoder } from './DoubleDecoder';
import { StringDecoder } from './StringDecoder';
import { ScalarType } from '../metadata/ScalarType';

class PropertyDecoder {

    public static decodePropertyColumn(data: Uint8Array, offset: IntWrapper, physicalType: number, numStreams: number) {
        let presentStreamMetadata: StreamMetadata | null = null;
        if (physicalType !== undefined) {
            let presentStream = null;
            let numValues = 0;
            if (numStreams > 1) {
                presentStreamMetadata = StreamMetadataDecoder.decode(data, offset);
                numValues = presentStreamMetadata.numValues();
                presentStream = DecodingUtils.decodeBooleanRle(data, presentStreamMetadata.numValues(), offset);
            }
            switch (physicalType) {
                case ScalarType.BOOLEAN: {
                    const dataStreamMetadata = StreamMetadataDecoder.decode(data, offset);
                    const dataStream = DecodingUtils.decodeBooleanRle(data, dataStreamMetadata.numValues(), offset);
                    // TODO: two pass over presentStream in order to avoid overallocation?
                    const values = new Array(presentStreamMetadata.numValues());
                    let counter = 0;
                    for (let i = 0; i < presentStreamMetadata.numValues(); i++) {
                        const value = presentStream.get(i) ? Boolean(dataStream.get(counter++)) : null;
                        if (value !== null) {
                            values[i] = value;
                        }
                    }
                    return values;
                }
                case ScalarType.UINT_32: {
                    const dataStreamMetadata = StreamMetadataDecoder.decode(data, offset);
                    const dataStream = IntegerDecoder.decodeIntStream(data, offset, dataStreamMetadata, false);
                    const values = new Uint32Array(presentStreamMetadata.numValues());
                    let counter = 0;
                    for (let i = 0; i < presentStreamMetadata.numValues(); i++) {
                        const value = presentStream.get(i) ? dataStream[counter++] : null;
                        if (value !== null) {
                            values[i] = value;
                        }
                    }
                    return values;
                }
                case ScalarType.INT_32: {
                    const dataStreamMetadata = StreamMetadataDecoder.decode(data, offset);
                    const dataStream = IntegerDecoder.decodeIntStream(data, offset, dataStreamMetadata, true);
                    const values = new Int32Array(presentStreamMetadata.numValues());
                    let counter = 0;
                    for (let i = 0; i < presentStreamMetadata.numValues(); i++) {
                        const value = presentStream.get(i) ? dataStream[counter++] : null;
                        if (value !== null) {
                            values[i] = value;
                        }
                    }
                    return values;
                }
                case ScalarType.DOUBLE: {
                    const dataStreamMetadata = StreamMetadataDecoder.decode(data, offset);
                    const dataStream = DoubleDecoder.decodeDoubleStream(data, offset, dataStreamMetadata);
                    const values = new Float64Array(presentStreamMetadata.numValues());
                    let counter = 0;
                    for (let i = 0; i < presentStreamMetadata.numValues(); i++) {
                        const value = presentStream.get(i) ? dataStream[counter++] : null;
                        if (value !== null) {
                            values[i] = value;
                        }
                    }
                    return values;
                }
                case ScalarType.FLOAT: {
                    const dataStreamMetadata = StreamMetadataDecoder.decode(data, offset);
                    const dataStream = FloatDecoder.decodeFloatStream(data, offset, dataStreamMetadata);
                    const values = new Float32Array(presentStreamMetadata.numValues());
                    let counter = 0;
                    for (let i = 0; i < presentStreamMetadata.numValues(); i++) {
                        const value = presentStream.get(i) ? dataStream[counter++] : null;
                        if (value !== null) {
                            values[i] = value;
                        }
                    }
                    return values;
                }
                case ScalarType.UINT_64: {
                    const dataStreamMetadata = StreamMetadataDecoder.decode(data, offset);
                    const dataStream = IntegerDecoder.decodeLongStream(data, offset, dataStreamMetadata, false);
                    const values = new BigUint64Array(presentStreamMetadata.numValues());
                    let counter = 0;
                    for (let i = 0; i < presentStreamMetadata.numValues(); i++) {
                        const value = presentStream.get(i) ? dataStream[counter++] : null;
                        if (value !== null) {
                            values[i] = value;
                        }
                    }
                    return values;
                }
                case ScalarType.INT_64: {
                    const dataStreamMetadata = StreamMetadataDecoder.decode(data, offset);
                    const dataStream = IntegerDecoder.decodeLongStream(data, offset, dataStreamMetadata, true);
                    const values = new BigInt64Array(presentStreamMetadata.numValues());
                    let counter = 0;
                    for (let i = 0; i < presentStreamMetadata.numValues(); i++) {
                        const value = presentStream.get(i) ? dataStream[counter++] : null;
                        if (value !== null) {
                            values[i] = value;
                        }
                    }
                    return values;
                }
                case ScalarType.STRING: {
                    return StringDecoder.decode(data, offset, numStreams - 1, presentStream, numValues);
                }
                default:
                    throw new Error("The specified data type for the field is currently not supported " + physicalType);
            }
        }

        if (numStreams === 1) {
            throw new Error("Present stream currently not supported for Structs.");
        } else {
            // TODO
            throw new Error("Strings are not supported yet for Structs.");
            //const result = StringDecoder.decodeSharedDictionary(data, offset, column);
            //return result.getRight();
        }
    }
}

export { PropertyDecoder };
