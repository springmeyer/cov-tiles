package com.mlt.converter.encodings;

import org.apache.commons.lang3.ArrayUtils;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.*;

public class BooleanEncoder {

    private BooleanEncoder(){}

    public static byte[] encodeBooleanStream(List<Boolean> values, StreamType streamType) throws IOException {
        var valueStream = new BitSet(values.size());
        for(var i = 0; i < values.size(); i++){
            var value = values.get(i);
            valueStream.set(i, value);
        }

        var encodedValueStream = EncodingUtils.encodeBooleanRle(valueStream, values.size());
        var valuesMetadata = new StreamMetadata(streamType, LogicalLevelTechnique.BOOLEAN_RLE,
                PhysicalLevelTechnique.VARINT,  values.size(), encodedValueStream.length, Optional.empty()).encode();

        return ArrayUtils.addAll(valuesMetadata, encodedValueStream);
    }

    public static int encodeBooleanStreamWithRoaringBitmap(List<Boolean> values, StreamType streamType) throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        var i = 0;
        for(var p : values){
            if(p){
                bitmap.add(i++);
            }
        }

        //TODO: add RoaringBitmap encodingd
        var valuesMetadata = new StreamMetadata(streamType, LogicalLevelTechnique.NONE,
                PhysicalLevelTechnique.VARINT,  values.size(), bitmap.toMutableRoaringBitmap().serializedSizeInBytes(),
                Optional.empty()).encode();
        return valuesMetadata.length + bitmap.toMutableRoaringBitmap().serializedSizeInBytes();
    }

    public static byte[] encodeBooleanAsInt(List<Boolean> values, StreamType streamType) throws IOException {
        var intValues = new ArrayList<Integer>(values.size());
        for(var p : values){
            intValues.add(p? 1 : 0);
        }

        return IntegerEncoder.encodeIntStream(intValues, PhysicalLevelTechnique.FAST_PFOR, false, streamType);
    }

}
