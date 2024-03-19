package com.mlt.converter.encodings;

import org.apache.commons.lang3.ArrayUtils;

import java.util.List;
import java.util.Optional;

public class FloatEncoder {

    private FloatEncoder(){}

    public static byte[] encodeFloatStream(List<Float> values){
        //TODO: add encodings
        float[] floatArray = new float[values.size()];
        for (int i = 0 ; i < values.size(); i++) {
            floatArray[i] = values.get(i);
        }
        var encodedValueStream = EncodingUtils.encodeFloatsLE(floatArray);

        var valuesMetadata = new StreamMetadata(StreamType.DATA, LogicalLevelTechnique.NONE, PhysicalLevelTechnique.NONE,
                values.size(), encodedValueStream.length, Optional.empty()).encode();

        return ArrayUtils.addAll(valuesMetadata, encodedValueStream);
    }

}
