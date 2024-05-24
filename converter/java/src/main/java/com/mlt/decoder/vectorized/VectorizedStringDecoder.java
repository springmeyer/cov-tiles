package com.mlt.decoder.vectorized;

import com.mlt.metadata.stream.DictionaryType;
import com.mlt.metadata.stream.LengthType;
import com.mlt.metadata.stream.StreamMetadataDecoder;
import com.mlt.metadata.tileset.MltTilesetMetadata;
import com.mlt.vector.BitVector;
import com.mlt.vector.Vector;
import com.mlt.vector.dictionary.DictionaryDataVector;
import com.mlt.vector.dictionary.StringDictionaryVector;
import com.mlt.vector.dictionary.StringSharedDictionaryVector;
import com.mlt.vector.flat.StringFlatVector;
// import com.mlt.vector.fsstdictionary.StringFsstDictionaryVector;
// import com.mlt.vector.fsstdictionary.StringSharedFsstDictionaryVector;
import me.lemire.integercompression.IntWrapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class VectorizedStringDecoder {

    private VectorizedStringDecoder(){}

    //TODO: create baseclass
    /** Not optimized for random access only for sequential iteration */
    public static Vector decode(String name, byte[] data, IntWrapper offset, int numStreams,
                                BitVector bitVector) throws IOException {
        /*
         * String column layouts:
         * -> plain -> present, length, data
         * -> dictionary -> present, length, dictionary, data
         * -> fsst dictionary -> symbolTable, symbolLength, dictionary, length, present, data
         * */

        IntBuffer dictionaryLengthStream = null;
        IntBuffer offsetStream = null;
        ByteBuffer dictionaryStream = null;
        IntBuffer symbolLengthStream = null;
        ByteBuffer symbolTableStream = null;
        for(var i = 0; i < numStreams; i++){
            var streamMetadata = StreamMetadataDecoder.decode(data, offset);
            switch (streamMetadata.physicalStreamType()){
                case OFFSET:{
                    offsetStream = VectorizedIntegerDecoder.decodeIntStream(data, offset, streamMetadata, false);
                    break;
                }
                case LENGTH:{
                    var ls = VectorizedIntegerDecoder.decodeIntStream(data, offset, streamMetadata, false);
                    if(LengthType.DICTIONARY.equals(streamMetadata.logicalStreamType().lengthType())){
                        dictionaryLengthStream = ls;
                    }
                    else{
                        symbolLengthStream = ls;
                    }

                    break;
                }
                case DATA:{
                    var ds = ByteBuffer.wrap(data, offset.get(), streamMetadata.byteLength());
                    offset.add(streamMetadata.byteLength());
                    if(DictionaryType.SINGLE.equals(streamMetadata.logicalStreamType().dictionaryType())){
                        dictionaryStream = ds;
                    }
                    else{
                        symbolTableStream = ds;
                    }
                    break;
                }
            }
        }

        if(symbolTableStream != null){
            System.out.println("symbolTableStream cannot be used as FSST is currently disabled");
            // return decodeFsstDictionary(name, bitVector, offsetStream, dictionaryLengthStream, dictionaryStream,
            //         symbolLengthStream, symbolTableStream);
        }
        // else if(dictionaryStream != null){
        if(dictionaryStream != null){
            return decodeDictionary(name, bitVector, offsetStream, dictionaryLengthStream, dictionaryStream);
        }

        return decodePlain(name, bitVector, offsetStream, dictionaryStream);
    }

    public static Vector decodeToRandomAccessFormat(String name, byte[] data, IntWrapper offset, int numStreams,
                                                    BitVector bitVector, int numFeatures)  {
        //TODO: handle ConstVector
        IntBuffer dictionaryLengthStream = null;
        IntBuffer offsetStream = null;
        ByteBuffer dictionaryStream = null;
        IntBuffer symbolLengthStream = null;
        ByteBuffer symbolTableStream = null;
        for(var i = 0; i < numStreams; i++){
            var streamMetadata = StreamMetadataDecoder.decode(data, offset);
            switch (streamMetadata.physicalStreamType()){
                case OFFSET:{
                    boolean isNullable = streamMetadata.numValues() != numFeatures;
                    offsetStream = isNullable?
                            VectorizedIntegerDecoder.decodeNullableIntStream(data, offset, streamMetadata, false, bitVector) :
                            VectorizedIntegerDecoder.decodeIntStream(data, offset, streamMetadata, false);
                    break;
                }
                case LENGTH:{
                    var ls = VectorizedIntegerDecoder.decodeLengthStreamToOffsetBuffer(data, offset, streamMetadata);
                    if(LengthType.DICTIONARY.equals(streamMetadata.logicalStreamType().lengthType())){
                        dictionaryLengthStream = ls;
                    }
                    else{
                        symbolLengthStream = ls;
                    }

                    break;
                }
                case DATA:{
                    var ds = ByteBuffer.wrap(data, offset.get(), streamMetadata.byteLength());
                    offset.add(streamMetadata.byteLength());
                    if(DictionaryType.SINGLE.equals(streamMetadata.logicalStreamType().dictionaryType())){
                        dictionaryStream = ds;
                    }
                    else{
                        symbolTableStream = ds;
                    }
                    break;
                }
            }
        }

        if(symbolTableStream != null){
            System.out.println("symbolTableStream cannot be used as FSST is currently disabled");
            // return decodeFsstDictionary(name, bitVector, offsetStream, dictionaryLengthStream, dictionaryStream,
            //         symbolLengthStream, symbolTableStream);
        }
        // else if(dictionaryStream != null){
        if(dictionaryStream != null){
            return decodeDictionary(name, bitVector, offsetStream, dictionaryLengthStream, dictionaryStream);
        }

        return decodePlain(name, bitVector, offsetStream, dictionaryStream);
    }

    //TODO: create baseclass for shared dictionary
    /** Not optimized for random access only for sequential iteration */
    public static Vector decodeSharedDictionary(
            byte[] data, IntWrapper offset,  MltTilesetMetadata.Column column){
        IntBuffer dictionaryLengthBuffer = null;
        ByteBuffer dictionaryBuffer = null;
        IntBuffer symbolLengthBuffer = null;
        ByteBuffer symbolTableBuffer = null;

        //TODO: refactor to be spec compliant -> start by decoding the FieldMetadata, StreamMetadata and PresentStream
        boolean dictionaryStreamDecoded = false;
        while(!dictionaryStreamDecoded){
            var streamMetadata = StreamMetadataDecoder.decode(data, offset);
            switch (streamMetadata.physicalStreamType()){
                case LENGTH:{
                    if(LengthType.DICTIONARY.equals(streamMetadata.logicalStreamType().lengthType())){
                        dictionaryLengthBuffer = VectorizedIntegerDecoder.decodeIntStream(data, offset, streamMetadata, false);
                    }
                    else{
                        symbolLengthBuffer = VectorizedIntegerDecoder.decodeIntStream(data, offset, streamMetadata, false);
                    }
                    break;
                }
                case DATA:{
                    //TODO: fix -> only shared is allowed in that case
                    if(DictionaryType.SINGLE.equals(streamMetadata.logicalStreamType().dictionaryType()) ||
                            DictionaryType.SHARED.equals(streamMetadata.logicalStreamType().dictionaryType())){
                        dictionaryBuffer = ByteBuffer.wrap(data, offset.get(), streamMetadata.byteLength());
                        dictionaryStreamDecoded = true;
                    }
                    else{
                        symbolTableBuffer = ByteBuffer.wrap(data, offset.get(), streamMetadata.byteLength());
                    }

                    offset.add(streamMetadata.byteLength());
                    break;
                }
            }
        }


        var chieldFields = column.getComplexType().getChildrenList();
        var fieldVectors = new DictionaryDataVector[chieldFields.size()];
        var i = 0;
        for(var childField : chieldFields){
            var numStreams = VectorizedDecodingUtils.decodeVarint(data, offset, 1).get(0);
            if(numStreams != 2 || childField.hasComplexField() || childField.getScalarField().getPhysicalType() != MltTilesetMetadata.ScalarType.STRING){
                throw new IllegalArgumentException("Currently only optional string fields are implemented for a struct.");
            }

            var presentStreamMetadata = StreamMetadataDecoder.decode(data, offset);
            var presentStream = VectorizedDecodingUtils.decodeBooleanRle(data, presentStreamMetadata.numValues(), offset);
            var offsetStreamMetadata = StreamMetadataDecoder.decode(data, offset);
            var offsetStream = VectorizedIntegerDecoder.decodeIntStream(data, offset, offsetStreamMetadata, false);

            //TODO: get delimiter sign from column mappings
            var columnName = column.getName() + (childField.getName() == "default"? "" : (":"  + childField.getName()));
            //TODO: refactor to work also when present stream is null
            var dataVector = new DictionaryDataVector(columnName, new BitVector(presentStream, presentStreamMetadata.numValues()),
                    offsetStream);
            fieldVectors[i++] = dataVector;
        }

        if (symbolTableBuffer != null) {
            System.out.println("symbolTableBuffer cannot be used as FSST is currently disabled");
            // return new StringSharedFsstDictionaryVector(column.getName(), dictionaryLengthBuffer,
            // dictionaryBuffer, symbolLengthBuffer, symbolTableBuffer, fieldVectors);
        }
        // } else {
        return new StringSharedDictionaryVector(column.getName(), dictionaryLengthBuffer, dictionaryBuffer, fieldVectors);
        // }
    }

    public static Vector decodeSharedDictionaryToRandomAccessFormat(
            byte[] data, IntWrapper offset,  MltTilesetMetadata.Column column, int numFeatures){
        IntBuffer dictionaryLengthBuffer = null;
        ByteBuffer dictionaryBuffer = null;
        IntBuffer symbolLengthBuffer = null;
        ByteBuffer symbolTableBuffer = null;

        //TODO: refactor to be spec compliant -> start by decoding the FieldMetadata, StreamMetadata and PresentStream
        boolean dictionaryStreamDecoded = false;
        while(!dictionaryStreamDecoded){
            var streamMetadata = StreamMetadataDecoder.decode(data, offset);
            switch (streamMetadata.physicalStreamType()){
                case LENGTH:{
                    if(LengthType.DICTIONARY.equals(streamMetadata.logicalStreamType().lengthType())){
                        dictionaryLengthBuffer = VectorizedIntegerDecoder.decodeLengthStreamToOffsetBuffer(data, offset,
                                streamMetadata);
                    }
                    else{
                        symbolLengthBuffer = VectorizedIntegerDecoder.decodeLengthStreamToOffsetBuffer(data, offset,
                                streamMetadata);
                    }
                    break;
                }
                case DATA:{
                    //TODO: fix -> only shared is allowed in that case
                    if(DictionaryType.SINGLE.equals(streamMetadata.logicalStreamType().dictionaryType()) ||
                            DictionaryType.SHARED.equals(streamMetadata.logicalStreamType().dictionaryType())){
                        dictionaryBuffer = ByteBuffer.wrap(data, offset.get(), streamMetadata.byteLength());
                        dictionaryStreamDecoded = true;
                    }
                    else{
                        symbolTableBuffer = ByteBuffer.wrap(data, offset.get(), streamMetadata.byteLength());
                    }

                    offset.add(streamMetadata.byteLength());
                    break;
                }
            }
        }

        var chieldFields = column.getComplexType().getChildrenList();
        var fieldVectors = new DictionaryDataVector[chieldFields.size()];
        var i = 0;
        for(var childField : chieldFields){
            var numStreams = VectorizedDecodingUtils.decodeVarint(data, offset, 1).get(0);
            if(numStreams != 2 || childField.hasComplexField() || childField.getScalarField().getPhysicalType() != MltTilesetMetadata.ScalarType.STRING){
                throw new IllegalArgumentException("Currently only optional string fields are implemented for a struct.");
            }

            var presentStreamMetadata = StreamMetadataDecoder.decode(data, offset);
            //TODO: check if ConstVector
            var presentStream = VectorizedDecodingUtils.decodeBooleanRle(data, presentStreamMetadata.numValues(), offset);
            var offsetStreamMetadata = StreamMetadataDecoder.decode(data, offset);
            boolean isNullable = offsetStreamMetadata.numValues() != numFeatures;
            var offsetStream = isNullable?
                    VectorizedIntegerDecoder.decodeNullableIntStream(data, offset, offsetStreamMetadata, false,
                            new BitVector(presentStream, presentStreamMetadata.numValues())) :
                    VectorizedIntegerDecoder.decodeIntStream(data, offset, offsetStreamMetadata, false);

            //TODO: get delimiter sign from column mappings
            var columnName = column.getName() + (childField.getName() == "default"? "" : (":"  + childField.getName()));
            //TODO: refactor to work also when present stream is null
            var dataVector = new DictionaryDataVector(columnName, new BitVector(presentStream, presentStreamMetadata.numValues()),
                    offsetStream);
            fieldVectors[i++] = dataVector;
        }

        if (symbolTableBuffer != null) {
            System.out.println("symbolTableBuffer cannot be used as FSST is currently disabled");
            // return new StringSharedFsstDictionaryVector(column.getName(), dictionaryLengthBuffer,
            // dictionaryBuffer, symbolLengthBuffer, symbolTableBuffer, fieldVectors);
        }
        // } else {
        return new StringSharedDictionaryVector(column.getName(), dictionaryLengthBuffer, dictionaryBuffer, fieldVectors);
        // }
    }

    private static StringFlatVector decodePlain(String name, BitVector nullabilityVector, IntBuffer lengthStream,
                                                ByteBuffer utf8Values){
        return new StringFlatVector(name, nullabilityVector, lengthStream, utf8Values);
    }

    private static StringDictionaryVector decodeDictionary(String name, BitVector nullabilityVector, IntBuffer dictionaryOffsets,
                                                 IntBuffer lengthStream, ByteBuffer utf8Values){
        return new StringDictionaryVector(name, nullabilityVector, dictionaryOffsets, lengthStream, utf8Values);
    }

    // private static StringFsstDictionaryVector decodeFsstDictionary(String name, BitVector nullabilityVector,
    //                                                            IntBuffer dictionaryOffsets, IntBuffer lengthStream,
    //                                                            ByteBuffer utf8Values, IntBuffer symbolLengthStream,
    //                                                            ByteBuffer symbolTable){
    //     return new StringFsstDictionaryVector(name, nullabilityVector, dictionaryOffsets, lengthStream,
    //             utf8Values, symbolLengthStream, symbolTable);
    // }

}
