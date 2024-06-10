import { IntWrapper } from "./intWrapper";

export enum PhysicalLevelTechnique {
    NONE,
    /* Preferred option, tends to produce the best compression ratio and decoding performance.
    * But currently only limited to 32 bit integer. */
    FAST_PFOR,
    /* Can produce better results in combination with a heavyweight compression scheme like Gzip.
    *  Simple compression scheme where the decoder are easier to implement compared to FastPfor.*/
    VARINT,
    /* Adaptive Lossless floating-Point Compression */
    ALP
}

export enum LogicalLevelTechnique {
    NONE,
    DELTA,
    COMPONENTWISE_DELTA,
    RLE,
    MORTON,
    /* Pseudodecimal Encoding of floats -> only for the exponent integer part an additional logical level technique is used.
    *  Both exponent and significant parts are encoded with the same physical level technique */
    PDE
}

export enum OffsetType {
    VERTEX,
    INDEX,
    STRING,
    KEY
}

export enum DictionaryType {
    NONE,
    SINGLE,
    SHARED,
    VERTEX,
    MORTON,
    FSST
}

export enum LengthType {
    VAR_BINARY,
    GEOMETRIES,
    PARTS,
    RINGS,
    TRIANGLES,
    SYMBOL,
    DICTIONARY
}

export enum PhysicalStreamType {
    PRESENT,
    DATA,
    OFFSET,
    LENGTH
}

export interface LogicalStreamType {
    dictionaryType: DictionaryType;
    offsetType: OffsetType;
    lengthType: LengthType;
}

export interface StreamMetadata {
    physicalStreamType: PhysicalStreamType;
    logicalStreamType: LogicalStreamType;
    logicalLevelTechnique1: LogicalLevelTechnique;
    logicalLevelTechnique2: LogicalLevelTechnique;
    physicalLevelTechnique: PhysicalLevelTechnique;
    numValues: number;
    byteLength: number;
}

export function decodeStreamMetadata(tile: Uint8Array, offset: IntWrapper): StreamMetadata {
    const streamType = tile[offset.get()];
    const physicalStreamType : PhysicalStreamType = Object.values(PhysicalStreamType)[streamType >> 4] as PhysicalStreamType;
    let logicalStreamType: LogicalStreamType = null;

    switch (physicalStreamType) {
        case PhysicalStreamType.DATA:
            logicalStreamType = new LogicalStreamType(DictionaryType.values[streamType & 0x0f]);
            break;
        case PhysicalStreamType.OFFSET:
            logicalStreamType = new LogicalStreamType(OffsetType.values[streamType & 0x0f]);
            break;
        case PhysicalStreamType.LENGTH:
            logicalStreamType = new LogicalStreamType(LengthType.values[streamType & 0x0f]);
            break;
    }
    offset.increment();

    const encodingsHeader: number = tile[offset.get()] & 0xFF;
    const logicalLevelTechnique1: LogicalLevelTechnique = LogicalLevelTechnique.values()[encodingsHeader >> 5];
    const logicalLevelTechnique2: LogicalLevelTechnique = LogicalLevelTechnique.values()[encodingsHeader >> 2 & 0x7];
    const physicalLevelTechnique: PhysicalLevelTechnique = PhysicalLevelTechnique.values()[encodingsHeader & 0x3];
    offset.increment();
    const sizeInfo: [number, number] = DecodingUtils.decodeVarint(tile, offset, 2);
    const numValues: number = sizeInfo[0];
    const byteLength: number = sizeInfo[1];
    return new StreamMetadata(physicalStreamType, logicalStreamType, logicalLevelTechnique1, logicalLevelTechnique2,
            physicalLevelTechnique, numValues, byteLength);
}
