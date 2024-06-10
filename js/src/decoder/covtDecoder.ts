import {
    decodeByteRle,
    decodeDeltaNumberVarints,
    decodeDeltaVarints,
    decodeInt64Rle,
    decodeInt64Varints,
    decodeNumberRle,
    decodeBooleanRle,
    decodeString,
    decodeStringField,
    decodeUint32Rle,
    decodeUInt64Rle,
    decodeUint64Varints,
    decodeVarint,
    decodeVarintVals,
    decodeZigZagVarint,
} from "./decodingUtils";
import { GeometryColumn, LayerTable, PropertyColumn } from "./layerTable";
import { ColumnDataType, ColumnEncoding, ColumnMetadata, LayerMetadata } from "./covtMetadata";
import { TileSetMetadata } from "./mlt_tileset_metadata_pb";
import { GeometryType } from "./geometry";
import { decodeStreamMetadata, StreamMetadata, PhysicalLevelTechnique } from "./streamMetadata";
import ieee754 from "ieee754";
import { IntWrapper } from "./intWrapper";

export class CovtDecoder {
    private static readonly ID_COLUMN_NAME = "id";
    private static readonly GEOMETRY_COLUMN_NAME = "geometry";
    private static readonly GEOMETRY_OFFSETS_STREAM_NAME = "geometry_offsets";
    private static readonly PART_OFFSETS_STREAM_NAME = "part_offsets";
    private static readonly RING_OFFSETS_STREAM_NAME = "ring_offsets";
    private static readonly VERTEX_OFFSETS_STREAM_NAME = "vertex_offsets";
    private static readonly VERTEX_BUFFER_STREAM_NAME = "vertex_buffer";

    private readonly layerTables = new Map<string, LayerTable>();
    // Temporary structure to place data for incremental testing
    public readonly mltLayers = [];

    constructor(private readonly tile: Uint8Array, private readonly tilesetMetadata: TileSetMetadata) {
        let offset = new IntWrapper(0);

        while (offset.get() < tile.length) {
            let ids: number[] | null = null;
            // let geometries: Geometry[] | null = null;
            // const properties: { [key: string]: any[] } = {};
            const version = tile[offset.get()];
            offset.increment();
            const infos = decodeVarintVals(tile, offset, 4);
            const featureTableId = infos[0];
            const tileExtent = infos[1];
            const maxTileExtent = infos[2];
            const numFeatures = infos[3];
            const metadata = tilesetMetadata.featureTables[featureTableId];
            console.log(JSON.stringify(metadata));
            if (!metadata) {
                // For now, return to avoid program hanging (which is expected until we handle all parsing)
                console.log("TODO: No metadata found for feature table id: " + featureTableId);
                break;
            }
            for (const column of metadata.columns) {
                const columnName = column.name;
                const numStreams = decodeVarintVals(tile, offset, 1)[0];

                if (columnName === CovtDecoder.ID_COLUMN_NAME) {
                    if(numStreams === 2){
                        const presentStreamMetadata = decodeStreamMetadata(tile, offset);
                        const presentStream = decodeBooleanRle(tile, presentStreamMetadata.numValues, presentStreamMetadata.byteLength, offset);
                    }

                    const idDataStreamMetadata = decodeStreamMetadata(tile, offset);
                    // TODO: need FASTPFOR to handle this or to fix https://github.com/maplibre/maplibre-tile-spec/pull/63
                    ids = idDataStreamMetadata.physicalLevelTechnique == PhysicalLevelTechnique.FAST_PFOR?
                            IntegerDecoder.decodeIntStream(tile, offset, idDataStreamMetadata, false).
                                    stream().mapToLong(i -> i).boxed().collect(Collectors.toList()):
                            IntegerDecoder.decodeLongStream(tile, offset, idDataStreamMetadata, false);
                    ids = idDataStreamMetadata.physicalLevelTechnique === PhysicalLevelTechnique.FAST_PFOR
                            ? IntegerDecoder.decodeIntStream(tile, offset, idDataStreamMetadata, false)
                                .stream()
                                .mapToLong((i) => i)
                                .toArray()
                            : IntegerDecoder.decodeLongStream(tile, offset, idDataStreamMetadata, false);
                }
                else if (columnName === CovtDecoder.GEOMETRY_COLUMN_NAME) {
                    // var geometryColumn = GeometryDecoder.decodeGeometryColumn(tile, numStreams, offset);
                    // geometries = GeometryDecoder.decodeGeometry(geometryColumn);
                }
                else {
                    // TODO
                }
            }

            this.mltLayers.push({ version, tileExtent, maxTileExtent, numFeatures, featureTableId, metadata });
        }
    }

    get layerNames(): string[] {
        return Array.from(this.layerTables.keys());
    }

    getLayerTable(layerName: string): LayerTable {
        return this.layerTables.get(layerName);
    }

    /*
     * - Depending on the geometryType the following topology streams are encoded: geometryOffsets, partOffsets, ringOffsets and vertexOffsets
     * Logical Representation
     * - Point: no stream
     * - LineString: Part offsets
     * - Polygon: Part offsets (Polygon), Ring offsets (LinearRing)
     * - MultiPoint: Geometry offsets -> array of offsets indicate where the vertices of each MultiPoint start
     * - MultiLineString: Geometry offsets, Part offsets (LineString)
     * - MultiPolygon -> Geometry offsets, Part offsets (Polygon), Ring offsets (LinearRing)
     * -> In addition when Indexed Coordinate Encoding (ICE) is used Vertex offsets stream is added
     * Physical Representation
     **/
    //TODO: use absolute offsets regarding the vertex buffer not the numer of geometries, parts, ....
    // private decodeGeometryColumn(
    //     tile: Uint8Array,
    //     numStreams: number,
    //     offset: IntWrapper
    // ): { geometryColumn: GeometryColumn; offset: number }: GeometryColumn {
    //     const geometryTypeMetadata = decodeStreamMetadata.decode(tile, offset);
    //     const geometryTypes = IntegerDecoder.decodeIntStream(tile, offset, geometryTypeMetadata, false);

    //     let numGeometries: number[] | null = null;
    //     let numParts: number[] | null = null;
    //     let numRings: number[] | null = null;
    //     let vertexOffsets: number[] | null = null;
    //     let mortonVertexBuffer: number[] | null = null;
    //     let vertexBuffer: number[] | null = null;

    //     for (let i = 0; i < numStreams - 1; i++) {
    //         const geometryStreamMetadata = StreamMetadataDecoder.decode(tile, offset);
    //         switch (geometryStreamMetadata.physicalStreamType()) {
    //             case 'LENGTH':
    //                 switch (geometryStreamMetadata.logicalStreamType().lengthType()) {
    //                     case 'GEOMETRIES':
    //                         numGeometries = IntegerDecoder.decodeIntStream(tile, offset, geometryStreamMetadata, false);
    //                         break;
    //                     case 'PARTS':
    //                         numParts = IntegerDecoder.decodeIntStream(tile, offset, geometryStreamMetadata, false);
    //                         break;
    //                     case 'RINGS':
    //                         numRings = IntegerDecoder.decodeIntStream(tile, offset, geometryStreamMetadata, false);
    //                         break;
    //                     case 'TRIANGLES':
    //                         throw new NotImplementedException("Not implemented yet.");
    //                 }
    //                 break;
    //             case 'OFFSET':
    //                 vertexOffsets = IntegerDecoder.decodeIntStream(tile, offset, geometryStreamMetadata, false);
    //                 break;
    //             case 'DATA':
    //                 if (DictionaryType.VERTEX === geometryStreamMetadata.logicalStreamType().dictionaryType()) {
    //                     // TODO: add Varint decoding
    //                     if (geometryStreamMetadata.physicalLevelTechnique() !== PhysicalLevelTechnique.FAST_PFOR) {
    //                         throw new Error("Currently only FastPfor encoding supported for the VertexBuffer.");
    //                     }
    //                     vertexBuffer = DecodingUtils.decodeFastPfor128DeltaCoordinates(tile, geometryStreamMetadata.numValues(), geometryStreamMetadata.byteLength(), offset);
    //                 } else {
    //                     mortonVertexBuffer = IntegerDecoder.decodeMortonStream(tile, offset, geometryStreamMetadata as MortonEncodedStreamMetadata);
    //                 }
    //                 break;
    //         }
    //     }

    //     return new GeometryColumn(geometryTypes, numGeometries, numParts, numRings, vertexOffsets, mortonVertexBuffer, vertexBuffer);
    // }




        // const geometryStreams = columnMetadata.streams;
        // const [geometryTypes, topologyStreamsOffset] = decodeByteRle(tile, numFeatures, offset);
        // offset = topologyStreamsOffset;

        // //TODO: Currently the topology streams (offsets arrays) are not implemented as absolute offsets -> change
        // let geometryOffsets: Uint32Array;
        // const geometryOffsetsMetadata = geometryStreams.get(CovtDecoder.GEOMETRY_OFFSETS_STREAM_NAME);
        // if (geometryOffsetsMetadata) {
        //     const [values, nextOffset] = decodeUint32Rle(tile, geometryOffsetsMetadata.numValues, offset);
        //     geometryOffsets = values;
        //     offset = nextOffset;
        // }

        // let partOffsets: Uint32Array;
        // const partOffsetsMetadata = geometryStreams.get(CovtDecoder.PART_OFFSETS_STREAM_NAME);
        // if (partOffsetsMetadata) {
        //     const [values, nextOffset] = decodeUint32Rle(tile, partOffsetsMetadata.numValues, offset);
        //     partOffsets = values;
        //     offset = nextOffset;
        // }

        // const vertexBufferMetadata = geometryStreams.get(CovtDecoder.VERTEX_BUFFER_STREAM_NAME);
        // if (columnMetadata.columnEncoding === ColumnEncoding.INDEXED_COORDINATE_ENCODING) {
        //     /* ICE encoding currently only supported for LineStrings and MultiLineStrings*/
        //     const vertexOffsetsMetadata = geometryStreams.get(CovtDecoder.VERTEX_OFFSETS_STREAM_NAME);
        //     const [vertexOffsets, vertexBufferOffset] = decodeDeltaVarints(
        //         tile,
        //         vertexOffsetsMetadata.numValues,
        //         offset,
        //     );
        //     const [vertexBuffer, nextOffset] = this.decodeDeltaVarintCoordinates(
        //         tile,
        //         vertexBufferMetadata.numValues,
        //         vertexBufferOffset,
        //     );

        //     offset = nextOffset;
        //     const geometries = { geometryTypes, geometryOffsets, partOffsets, vertexOffsets, vertexBuffer };
        //     return { geometryColumn: geometries, offset };
        // }

        // //TODO: refactor -> decode like in ICE encoding now without VertexOffset stream
        // let ringOffsets: Uint32Array;
        // const ringOffsetsMetadata = geometryStreams.get(CovtDecoder.RING_OFFSETS_STREAM_NAME);
        // if (ringOffsetsMetadata) {
        //     const [values, nextOffset] = decodeUint32Rle(tile, ringOffsetsMetadata.numValues, offset);
        //     ringOffsets = values;
        //     offset = nextOffset;
        // }

        // const vertexBuffer = new Int32Array(vertexBufferMetadata.numValues * 2);
        // let partOffsetCounter = 0;
        // let ringOffsetCounter = 0;
        // let geometryOffsetCounter = 0;
        // let vertexBufferOffset = 0;
        // for (const geometryType of geometryTypes) {
        //     switch (geometryType) {
        //         case GeometryType.POINT: {
        //             const [x, nextYOffset] = decodeZigZagVarint(tile, offset);
        //             const [y, nextXOffset] = decodeZigZagVarint(tile, nextYOffset);
        //             vertexBuffer[vertexBufferOffset++] = x;
        //             vertexBuffer[vertexBufferOffset++] = y;
        //             offset = nextXOffset;
        //             break;
        //         }
        //         case GeometryType.LINESTRING: {
        //             const numVertices = partOffsets[partOffsetCounter++];
        //             const [nextOffset, newVertexBufferOffset] = this.decodeLineString(
        //                 tile,
        //                 offset,
        //                 numVertices,
        //                 vertexBuffer,
        //                 vertexBufferOffset,
        //             );
        //             offset = nextOffset;
        //             vertexBufferOffset = newVertexBufferOffset;
        //             break;
        //         }
        //         case GeometryType.POLYGON: {
        //             const numRings = partOffsets[partOffsetCounter++];
        //             for (let i = 0; i < numRings; i++) {
        //                 const numVertices = ringOffsets[ringOffsetCounter++];
        //                 const [nextOffset, newVertexBufferOffset] = this.decodeLineString(
        //                     tile,
        //                     offset,
        //                     numVertices,
        //                     vertexBuffer,
        //                     vertexBufferOffset,
        //                 );
        //                 offset = nextOffset;
        //                 vertexBufferOffset = newVertexBufferOffset;
        //             }
        //             break;
        //         }
        //         case GeometryType.MULTI_LINESTRING: {
        //             const numLineStrings = geometryOffsets[geometryOffsetCounter++];
        //             for (let i = 0; i < numLineStrings; i++) {
        //                 const numVertices = partOffsets[partOffsetCounter++];
        //                 const [nextOffset, newVertexBufferOffset] = this.decodeLineString(
        //                     tile,
        //                     offset,
        //                     numVertices,
        //                     vertexBuffer,
        //                     vertexBufferOffset,
        //                 );
        //                 offset = nextOffset;
        //                 vertexBufferOffset = newVertexBufferOffset;
        //             }
        //             break;
        //         }
        //         case GeometryType.MULTI_POLYGON: {
        //             const numPolygons = geometryOffsets[geometryOffsetCounter++];
        //             for (let i = 0; i < numPolygons; i++) {
        //                 const numRings = partOffsets[partOffsetCounter++];
        //                 for (let j = 0; j < numRings; j++) {
        //                     const numVertices = ringOffsets[ringOffsetCounter++];
        //                     const [nextOffset, newVertexBufferOffset] = this.decodeLineString(
        //                         tile,
        //                         offset,
        //                         numVertices,
        //                         vertexBuffer,
        //                         vertexBufferOffset,
        //                     );
        //                     offset = nextOffset;
        //                     vertexBufferOffset = newVertexBufferOffset;
        //                 }
        //             }
        //             break;
        //         }
        //     }
        // }

    //     const geometries: GeometryColumn = { geometryTypes, geometryOffsets, partOffsets, ringOffsets, vertexBuffer };
    //     return { geometryColumn: geometries, offset };
    // }

    // private decodeLineString(
    //     tile: Uint8Array,
    //     bufferOffset: number,
    //     numVertices: number,
    //     vertexBuffer: Int32Array,
    //     vertexBufferOffset,
    // ): [offset: number, vertexBufferOffset: number] {
    //     let x = 0;
    //     let y = 0;
    //     for (let i = 0; i < numVertices; i++) {
    //         const [deltaX, nextYOffset] = decodeZigZagVarint(tile, bufferOffset);
    //         const [deltaY, nextXOffset] = decodeZigZagVarint(tile, nextYOffset);
    //         x += deltaX;
    //         y += deltaY;
    //         vertexBuffer[vertexBufferOffset++] = x;
    //         vertexBuffer[vertexBufferOffset++] = y;
    //         bufferOffset = nextXOffset;
    //     }

    //     return [bufferOffset, vertexBufferOffset];
    // }

    // private decodeDeltaVarintCoordinates(
    //     tile: Uint8Array,
    //     numCoordinates: number,
    //     offset = 0,
    // ): [vertices: Int32Array, offset: number] {
    //     const vertices = new Int32Array(numCoordinates * 2);

    //     let x = 0;
    //     let y = 0;
    //     let coordIndex = 0;
    //     for (let i = 0; i < numCoordinates; i++) {
    //         const [deltaX, nextYOffset] = decodeZigZagVarint(tile, offset);
    //         const [deltaY, nextXOffset] = decodeZigZagVarint(tile, nextYOffset);

    //         x += deltaX;
    //         y += deltaY;
    //         vertices[coordIndex++] = x;
    //         vertices[coordIndex++] = y;

    //         offset = nextXOffset;
    //     }

    //     return [vertices, offset];
    // }

    // private decodePropertyColumn(
    //     tile: Uint8Array,
    //     offset: number,
    //     columnMetadata: ColumnMetadata,
    //     numFeatures: number,
    // ): {
    //     data: PropertyColumn;
    //     offset: number;
    // } {
    //     if (columnMetadata.columnEncoding === ColumnEncoding.LOCALIZED_DICTIONARY) {
    //         const streams = columnMetadata.streams;
    //         //TODO: optimize
    //         const lengthDictionaryOffset =
    //             offset +
    //             Array.from(streams)
    //                 .filter(([name, data]) => name !== "length" && name !== "dictionary")
    //                 .reduce((p, [name, data]) => p + data.byteLength, 0);

    //         const numLengthValues = streams.get("length").numValues;
    //         const [lengthStream, dictionaryStreamOffset] = decodeUint32Rle(
    //             tile,
    //             numLengthValues,
    //             lengthDictionaryOffset,
    //         );
    //         const [dictionaryStream, nextColumnOffset] = this.decodeStringDictionary(
    //             tile,
    //             dictionaryStreamOffset,
    //             lengthStream,
    //         );

    //         const localizedStreams = new Map<string, [Uint8Array, Uint32Array]>();
    //         let presentStream: Uint8Array = null;
    //         let i = 0;
    //         for (const [streamName, streamData] of streams) {
    //             if (i >= streams.size - 2) {
    //                 break;
    //             }

    //             if (i % 2 === 0) {
    //                 const numBytes = Math.ceil(numFeatures / 8);
    //                 const [nextPresentStream, dataOffset] = decodeByteRle(tile, numBytes, offset);
    //                 presentStream = nextPresentStream;
    //                 offset = dataOffset;
    //             } else {
    //                 const [dataStream, nextStreamOffset] = decodeUint32Rle(tile, streamData.numValues, offset);
    //                 offset = nextStreamOffset;
    //                 const columnName = columnMetadata.columnName;
    //                 const propertyName = columnName === streamName ? columnName : `${columnName}:${streamName}`;
    //                 localizedStreams.set(propertyName, [presentStream, dataStream]);
    //             }

    //             i++;
    //         }

    //         return { data: { dictionaryStream, localizedStreams }, offset: nextColumnOffset };
    //     }

    //     const numBytesPresentStream = Math.ceil(numFeatures / 8);
    //     const [presentStream, dataOffset] = decodeByteRle(tile, numBytesPresentStream, offset);
    //     switch (columnMetadata.columnType) {
    //         case ColumnDataType.BOOLEAN: {
    //             const [dataStream, nextColumnOffset] = decodeByteRle(tile, numBytesPresentStream, dataOffset);
    //             return { data: { presentStream, dataStream }, offset: nextColumnOffset };
    //         }
    //         case ColumnDataType.INT_64:
    //         case ColumnDataType.UINT_64: {
    //             const numPropertyValues = columnMetadata.streams.get("data").numValues;
    //             if (columnMetadata.columnEncoding === ColumnEncoding.VARINT) {
    //                 const [dataStream, nextColumnOffset] =
    //                     columnMetadata.columnType === ColumnDataType.UINT_64
    //                         ? decodeUint64Varints(tile, numPropertyValues, dataOffset)
    //                         : decodeInt64Varints(tile, numPropertyValues, dataOffset);
    //                 return {
    //                     data: { presentStream, dataStream },
    //                     offset: nextColumnOffset,
    //                 };
    //             } else if (columnMetadata.columnEncoding === ColumnEncoding.RLE) {
    //                 const [dataStream, nextColumnOffset] =
    //                     columnMetadata.columnType === ColumnDataType.UINT_64
    //                         ? decodeUInt64Rle(tile, numPropertyValues, dataOffset)
    //                         : decodeInt64Rle(tile, numPropertyValues, dataOffset);
    //                 return {
    //                     data: { presentStream, dataStream },
    //                     offset: nextColumnOffset,
    //                 };
    //             } else {
    //                 throw new Error("Specified encoding not supported for a int property type.");
    //             }
    //         }
    //         case ColumnDataType.FLOAT: {
    //             const numPropertyValues = columnMetadata.streams.get("data").numValues;
    //             const dataStream = new Float32Array(numPropertyValues);
    //             let offset = dataOffset;
    //             for (let i = 0; i < numPropertyValues; i++) {
    //                 dataStream[i] = ieee754.read(tile, offset, true, 23, Float32Array.BYTES_PER_ELEMENT);
    //                 offset += Float32Array.BYTES_PER_ELEMENT;
    //             }

    //             return {
    //                 data: { presentStream, dataStream },
    //                 offset,
    //             };
    //         }
    //         case ColumnDataType.STRING: {
    //             const numDataValues = columnMetadata.streams.get("data").numValues;
    //             const numLengthValues = columnMetadata.streams.get("length").numValues;
    //             const [dataStream, lengthStreamOffset] = decodeUint32Rle(tile, numDataValues, dataOffset);
    //             const [lengthStream, dictionaryStreamOffset] = decodeUint32Rle(
    //                 tile,
    //                 numLengthValues,
    //                 lengthStreamOffset,
    //             );
    //             const [dictionaryStream, nextColumnOffset] = this.decodeStringDictionary(
    //                 tile,
    //                 dictionaryStreamOffset,
    //                 lengthStream,
    //             );

    //             return {
    //                 data: { presentStream, dataStream, dictionaryStream },
    //                 offset: nextColumnOffset,
    //             };
    //         }
    //     }
    // }

    // private decodeStringDictionary(
    //     tile: Uint8Array,
    //     offset: number,
    //     lengths: Uint32Array,
    // ): [values: string[], offset: number] {
    //     const values = [];
    //     for (let i = 0; i < lengths.length; i++) {
    //         const length = lengths[i];
    //         const endOffset = offset + length;
    //         const value = decodeString(tile, offset, endOffset);
    //         values.push(value);
    //         offset = endOffset;
    //     }

    //     return [values, offset];
    // }
}
