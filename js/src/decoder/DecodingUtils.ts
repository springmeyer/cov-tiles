import { IntWrapper } from './IntWrapper';
import { BitSet } from 'bitset';

export class DecodingUtils {

    public static decodeComponentwiseDeltaVec2(data: Int32Array): void {
        data[0] = (data[0] >>> 1) ^ ((data[0] << 31) >> 31);
        data[1] = (data[1] >>> 1) ^ ((data[1] << 31) >> 31);
        for (let i = 2; i < data.length; i += 2) {
            data[i] = ((data[i] >>> 1) ^ ((data[i] << 31) >> 31)) + data[i - 2];
            data[i + 1] = ((data[i + 1] >>> 1) ^ ((data[i + 1] << 31) >> 31)) + data[i - 1];
        }
    }
    public static decodeVarint(src: Uint8Array, pos: IntWrapper, numValues: number): Int32Array {
        const values = new Int32Array(numValues);
        let dstOffset = 0;
        for (let i = 0; i < numValues; i++) {
            const offset = this.decodeVarintInternal(src, pos.get(), values, dstOffset);
            dstOffset++;
            pos.set(offset);
        }
        return values;
    }

    // Source: https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/util/VarInt.java
    private static decodeVarintInternal(src: Uint8Array, offset: number, dst: Int32Array, dstOffset: number): number {
        let b = src[offset++];
        let value = b & 0x7f;
        if ((b & 0x80) === 0) {
            dst[dstOffset] = value;
            return offset;
        }

        b = src[offset++];
        value |= (b & 0x7f) << 7;
        if ((b & 0x80) === 0) {
            dst[dstOffset] = value;
            return offset;
        }

        b = src[offset++];
        value |= (b & 0x7f) << 14;
        if ((b & 0x80) === 0) {
            dst[dstOffset] = value;
            return offset;
        }

        b = src[offset++];
        value |= (b & 0x7f) << 21;
        dst[dstOffset] = value;
        return offset;
    }

    public static decodeLongVarint(src: Uint8Array, pos: IntWrapper, numValues: number): BigInt64Array {
        const values = new BigInt64Array(numValues);
        for (let i = 0; i < numValues; i++) {
            const value = this.decodeLongVarintInternal(src, pos);
            values[i] = value;
        }
        return values;
    }

    private static decodeLongVarintInternal(bytes: Uint8Array, pos: IntWrapper): bigint {
        let value = 0n;
        let shift = 0;
        let index = pos.get();
        while (index < bytes.length) {
            const b = bytes[index++];
            value |= BigInt(b & 0x7F) << BigInt(shift);
            if ((b & 0x80) === 0) {
                break;
            }
            shift += 7;
            if (shift >= 64) {
                throw new Error("Varint too long");
            }
        }
        pos.set(index);
        return value;
    }

    public static decodeZigZag(encoded: number): number {
        return (encoded >>> 1) ^ (-(encoded & 1));
    }

    public static decodeZigZagArray(encoded: Int32Array): void {
        for (let i = 0; i < encoded.length; i++) {
            encoded[i] = this.decodeZigZag(encoded[i]);
        }
    }

    public static decodeZigZagLong(encoded: bigint): bigint {
        return (encoded >> 1n) ^ (-(encoded & 1n));
    }

    public static decodeZigZagLongArray(encoded: BigInt64Array): void {
        for (let i = 0; i < encoded.length; i++) {
            encoded[i] = this.decodeZigZagLong(encoded[i]);
        }
    }

    public static decodeByteRle(buffer: Uint8Array, numBytes: number, pos: IntWrapper): Uint8Array {
        const values = new Uint8Array(numBytes);

        let valueOffset = 0;
        while (valueOffset < numBytes) {
            const header = buffer[pos.increment()];

            /* Runs */
            if (header <= 0x7f) {
                const numRuns = header + 3;
                const value = buffer[pos.increment()];
                const endValueOffset = valueOffset + numRuns;
                values.fill(value, valueOffset, endValueOffset);
                valueOffset = endValueOffset;
            } else {
                /* Literals */
                const numLiterals = 256 - header;
                for (let i = 0; i < numLiterals; i++) {
                    values[valueOffset++] = buffer[pos.increment()];
                }
            }
        }
        return values;
    }

    public static decodeUnsignedRLE(data: Int32Array, numRuns: number, numTotalValues: number): Int32Array {
        const values = new Int32Array(numTotalValues);
        let offset = 0;
        for (let i = 0; i < numRuns; i++) {
            const runLength = data[i];
            const value = data[i + numRuns];
            values.fill(value, offset, offset + runLength);
            offset += runLength;
        }
        return values;
    }

    public static decodeUnsignedRLELong(data: BigInt64Array, numRuns: number, numTotalValues: number): BigInt64Array {
        const values = new BigInt64Array(numTotalValues);
        let offset = 0;
        for (let i = 0; i < numRuns; i++) {
            const runLength = data[i];
            const value = data[i + numRuns];
            values.fill(value, offset, offset + Number(runLength));
            offset += Number(runLength);
        }
        return values;
    }

    public static decodeBooleanRle(buffer: Uint8Array, numBooleans: number, pos: IntWrapper): BitSet {
        const numBytes = Math.ceil(numBooleans / 8.0);
        return new BitSet(this.decodeByteRle(buffer, numBytes, pos));
    }

    public static decodeFloatsLE(encodedValues: Uint8Array, pos: IntWrapper, numValues: number): Float32Array {
        const fb = new Float32Array(new Uint8Array(encodedValues.slice(pos.get(), pos.get() + numValues * Float32Array.BYTES_PER_ELEMENT)).buffer);
        pos.set(pos.get() + numValues * Float32Array.BYTES_PER_ELEMENT);
        return fb;
    }

    public static decodeDoublesLE(encodedValues: Uint8Array, pos: IntWrapper, numValues: number): Float64Array {
        const bytesPerElement = Float64Array.BYTES_PER_ELEMENT;
        const fb = new Float64Array(new Uint16Array(encodedValues.slice(pos.get(), pos.get() + numValues * bytesPerElement)).buffer);
        pos.set(pos.get() + numValues * bytesPerElement);
        return fb;
    }
}
