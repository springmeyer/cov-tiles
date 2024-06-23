import { DecodingUtils } from './DecodingUtils';
import { IntWrapper } from './IntWrapper';
import { StreamMetadata } from '../metadata/stream/StreamMetadata';
import { LogicalLevelTechnique } from '../metadata/stream/LogicalLevelTechnique';
import { PhysicalLevelTechnique } from '../metadata/stream/PhysicalLevelTechnique';
import { MortonEncodedStreamMetadata } from '../metadata/stream/MortonEncodedStreamMetadata';
import { RleEncodedStreamMetadata } from '../metadata/stream/RleEncodedStreamMetadata';

class IntegerDecoder {

    public static decodeMortonStream(data: Uint8Array, offset: IntWrapper, streamMetadata: MortonEncodedStreamMetadata): Int32Array {
        let values: Int32Array;
        if (streamMetadata.physicalLevelTechnique() === PhysicalLevelTechnique.FAST_PFOR) {
            throw new Error("Specified physical level technique not yet supported: " + streamMetadata.physicalLevelTechnique());
            // TODO
            //values = DecodingUtils.decodeFastPfor128(data, streamMetadata.numValues(), streamMetadata.byteLength(), offset);
        } else if (streamMetadata.physicalLevelTechnique() === PhysicalLevelTechnique.VARINT) {
            values = DecodingUtils.decodeVarint(data, offset, streamMetadata.numValues());
        } else {
            throw new Error("Specified physical level technique not yet supported: " + streamMetadata.physicalLevelTechnique());
        }

        return this.decodeMortonDelta(values, streamMetadata.numBits(), streamMetadata.coordinateShift());
    }

    private static decodeMortonDelta(data: Int32Array, numBits: number, coordinateShift: number): Int32Array {
        const vertices = new Int32Array(data.length * 2);
        let previousMortonCode = 0;
        let counter = 0;
        for (const deltaCode of data) {
            const code = previousMortonCode + deltaCode;
            let x = 0;
            let y = 0;
            const codeY = code >> 1;
            for (let i = 0; i < numBits; i++) {
                x |= (code & (1 << (2 * i))) >> i;
                y |= (codeY & (1 << (2 * i))) >> i;
            }
            vertices[counter++] = x - coordinateShift;
            vertices[counter++] = y - coordinateShift;
            previousMortonCode = code;
        }
        return vertices;
    }

    private static decodeMortonCodes(data: Int32Array, numBits: number, coordinateShift: number): Int32Array {
        const vertices = new Int32Array(data.length * 2);
        let counter = 0;
        for (const code of data) {
            let x = 0;
            let y = 0;
            const codeY = code >> 1;
            for (let i = 0; i < numBits; i++) {
                x |= (code & (1 << (2 * i))) >> i;
                y |= (codeY & (1 << (2 * i))) >> i;
            }
            vertices[counter++] = x - coordinateShift;
            vertices[counter++] = y - coordinateShift;
        }
        return vertices;
    }

    public static decodeIntStream(data: Uint8Array, offset: IntWrapper, streamMetadata: StreamMetadata, isSigned: boolean): Int32Array {
        let values: Int32Array;
        if (streamMetadata.physicalLevelTechnique() === PhysicalLevelTechnique.FAST_PFOR) {
            throw new Error("Specified physical level technique not yet supported: " + streamMetadata.physicalLevelTechnique());
            // TODO
            //values = DecodingUtils.decodeFastPfor128(data, streamMetadata.numValues(), streamMetadata.byteLength(), offset);
        } else if (streamMetadata.physicalLevelTechnique() === PhysicalLevelTechnique.VARINT) {
            values = DecodingUtils.decodeVarint(data, offset, streamMetadata.numValues());
        } else {
            throw new Error("Specified physical level technique not yet supported: " + streamMetadata.physicalLevelTechnique());
        }
        return this.decodeIntArray(values, streamMetadata, isSigned);
    }

    private static decodeIntArray(values: Int32Array, streamMetadata: StreamMetadata, isSigned: boolean): Int32Array {
        switch (streamMetadata.logicalLevelTechnique1()) {
            case LogicalLevelTechnique.DELTA: {
                if (streamMetadata.logicalLevelTechnique2() === LogicalLevelTechnique.RLE) {
                    const rleMetadata = streamMetadata as RleEncodedStreamMetadata;
                    // TODO: stop mutating these values?
                    values =
                        DecodingUtils.decodeUnsignedRLE(
                            values, rleMetadata.runs(), rleMetadata.numRleValues());
                    return this.decodeZigZagDelta(values);
                }
                return this.decodeZigZagDelta(values);
            }
            case LogicalLevelTechnique.RLE: {
                const rleMetadata = streamMetadata as RleEncodedStreamMetadata;
                const decodedValues = this.decodeRLE(values, rleMetadata.runs());
                return isSigned ? this.decodeZigZag(decodedValues) : decodedValues;
            }
            case LogicalLevelTechnique.NONE: {
                return isSigned ? this.decodeZigZag(values) : values;
            }
            case LogicalLevelTechnique.MORTON: {
                const mortonMetadata = streamMetadata as MortonEncodedStreamMetadata;
                return this.decodeMortonCodes(values, mortonMetadata.numBits(), mortonMetadata.coordinateShift());
            }
            case LogicalLevelTechnique.COMPONENTWISE_DELTA: {
                DecodingUtils.decodeComponentwiseDeltaVec2(values);
                return values;
            }
            default:
                throw new Error("The specified logical level technique is not supported for integers: " + streamMetadata.logicalLevelTechnique1());
        }
    }

    public static decodeLongStream(data: Uint8Array, offset: IntWrapper, streamMetadata: StreamMetadata, isSigned: boolean): BigInt64Array {
        if (streamMetadata.physicalLevelTechnique() !== PhysicalLevelTechnique.VARINT) {
            throw new Error("Specified physical level technique not yet supported: " + streamMetadata.physicalLevelTechnique());
        }

        const values = DecodingUtils.decodeLongVarint(data, offset, streamMetadata.numValues());
        return this.decodeLongArray(values, streamMetadata, isSigned);
    }

    private static decodeLongArray(values: BigInt64Array, streamMetadata: StreamMetadata, isSigned: boolean): BigInt64Array {
        switch (streamMetadata.logicalLevelTechnique1()) {
            case LogicalLevelTechnique.DELTA: {
                if (streamMetadata.logicalLevelTechnique2() === LogicalLevelTechnique.RLE) {
                    const rleMetadata = streamMetadata as RleEncodedStreamMetadata;
                    values =
                        DecodingUtils.decodeUnsignedRLELong(
                            values, rleMetadata.runs(), rleMetadata.numRleValues());
                    return this.decodeLongZigZagDelta(values);
                }
                return this.decodeLongZigZagDelta(values);
            }
            case LogicalLevelTechnique.RLE: {
                const rleMetadata = streamMetadata as RleEncodedStreamMetadata;
                const decodedValues = this.decodeLongRLE(values, rleMetadata.runs());
                return isSigned ? this.decodeZigZagLong(decodedValues) : decodedValues;
            }
            case LogicalLevelTechnique.NONE: {
                return isSigned ? this.decodeZigZagLong(values) : values;
            }
            default:
                throw new Error("The specified logical level technique is not supported for integers: " + streamMetadata.logicalLevelTechnique1());
        }
    }

    private static decodeRLE(data: Int32Array, numRuns: number): Int32Array {
        // Note: if this array is initialied like new Array<number>(numRleValues)
        // like the java implementation does, the array will potentially contain
        // extra uninitialized values
        // TODO: figure out how to pre-initialize the array with the correct size
        const values = new Array<number>();
        for (let i = 0; i < numRuns; i++) {
            const run = data[i];
            const value = data[i + numRuns];
            for (let j = 0; j < run; j++) {
                values.push(value);
            }
        }
        return new Int32Array(values);
    }

    private static decodeLongRLE(data: BigInt64Array, numRuns: number): BigInt64Array {
        // Note: if this array is initialied like new Array<number>(numRleValues)
        // like the java implementation does, the array will potentially contain
        // extra uninitialized values
        const values = new Array<bigint>();
        for (let i = 0; i < numRuns; i++) {
            const run = data[i];
            const value = data[i + numRuns];
            for (let j = 0; j < run; j++) {
                values.push(value);
            }
        }
        return new BigInt64Array(values);
    }

    private static decodeZigZagDelta(data: Int32Array): Int32Array {
        const values = new Int32Array(data.length);
        let previousValue = 0;
        let counter = 0;
        for (const zigZagDelta of data) {
            const value = previousValue + DecodingUtils.decodeZigZag(zigZagDelta);
            values[counter++] = value;
            previousValue = value;
        }
        return values;
    }

    private static decodeLongZigZagDelta(data: BigInt64Array): BigInt64Array {
        const values = new BigInt64Array(data.length);
        let previousValue = 0n;
        let counter = 0;
        for (const zigZagDelta of data) {
            const value = previousValue + DecodingUtils.decodeZigZagLong(zigZagDelta);
            values[counter++] = value;
            previousValue = value;
        }
        return values;
    }

    // TODO: check if mutating in place would be faster than returning a new array
    private static decodeZigZag(data: Int32Array): Int32Array {
        return data.map(zigZagDelta => DecodingUtils.decodeZigZag(zigZagDelta));
    }

    private static decodeZigZagLong(data: BigInt64Array): BigInt64Array {
        return data.map(zigZagDelta => DecodingUtils.decodeZigZagLong(zigZagDelta));
    }
}

export { IntegerDecoder };
