package com.mlt.decoder;

import com.mlt.converter.encodings.PhysicalLevelTechnique;
import com.mlt.converter.encodings.StreamMetadata;
import com.mlt.converter.geometry.ZOrderCurve;
import me.lemire.integercompression.IntWrapper;
import org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class IntegerDecoder {

    private IntegerDecoder(){}

    public static List<Integer> decodeMortonStream(byte[] data, IntWrapper offset, StreamMetadata streamMetadata){
        int[] values;
        if(streamMetadata.physicalLevelTechnique() == PhysicalLevelTechnique.FAST_PFOR){
            //TODO: numValues is not right if rle or delta rle is used -> add separate flag in StreamMetadata
            values = DecodingUtils.decodeFastPfor128(data, streamMetadata.numValues(), streamMetadata.byteLength(), offset);
        }
        else if(streamMetadata.physicalLevelTechnique() == PhysicalLevelTechnique.VARINT){
            values = DecodingUtils.decodeVarint(data, offset, streamMetadata.numValues());
        }
        else{
            throw new IllegalArgumentException("Specified physical level technique not yet supported.");
        }

        return decodeMortonDelta(values, streamMetadata.numBits(), streamMetadata.coordinateShift());
    }

    private static List<Integer> decodeMortonDelta(int[] data, int numBits, int coordinateShift){
        var vertices = new ArrayList<Integer>(data.length * 2);
        var previousMortonCode = 0;
        for(var deltaCode : data){
            var mortonCode = previousMortonCode + deltaCode;
            var vertex = decodeMortonCode(mortonCode, numBits, coordinateShift);
            vertices.add(vertex[0]);
            vertices.add(vertex[1]);
            previousMortonCode = mortonCode;
        }

        return vertices;
    }

    private static int[] decodeMortonCode(int mortonCode, int numBits, int coordinateShift) {
        int x = decodeMorton(mortonCode, numBits) - coordinateShift;
        int y = decodeMorton(mortonCode >> 1, numBits) - coordinateShift;
        return new int[]{x, y};
    }

    private static int decodeMorton(int code, int numBits) {
        int coordinate = 0;
        for (int i = 0; i < numBits; i++) {
            coordinate |= (code & (1L << (2 * i))) >> i;
        }
        return coordinate;
    }

    public static List<Integer> decodeIntStream(byte[] data, IntWrapper offset, StreamMetadata streamMetadata, boolean isSigned){
        int[] values = null;
        if(streamMetadata.physicalLevelTechnique() == PhysicalLevelTechnique.FAST_PFOR){
            //TODO: numValues is not right if rle or delta rle is used -> add separate flag in StreamMetadata
            values = DecodingUtils.decodeFastPfor128(data, streamMetadata.numValues(), streamMetadata.byteLength(), offset);
        }
        else if(streamMetadata.physicalLevelTechnique() == PhysicalLevelTechnique.VARINT){
            values = DecodingUtils.decodeVarint(data, offset, streamMetadata.numValues());
        }
        else{
            throw new IllegalArgumentException("Specified physical level technique not yet supported.");
        }

        switch (streamMetadata.logicalLevelTechnique()){
            case RLE:{
                var decodedValues = decodeRLE(values, streamMetadata.numRunValues().get());
                return isSigned? decodeZigZag(decodedValues.stream().mapToInt(i -> i).toArray()) : decodedValues;
            }
            case DELTA:
                return decodeZigZagDelta(values);
            case DELTA_RLE:
                return decodeDeltaRLE(values, streamMetadata.numRunValues().get());
            case NONE:{
                var decodedValues = Arrays.stream(values).boxed().collect(Collectors.toList());
                return isSigned? decodeZigZag(decodedValues.stream().mapToInt(i -> i).toArray()) : decodedValues;
            }
            default:
                throw new IllegalArgumentException("The specified logical level technique is not supported for integers.");
        }
    }

    public static List<Long> decodeLongStream(byte[] data, IntWrapper offset, StreamMetadata streamMetadata, boolean isSigned){
        if(streamMetadata.physicalLevelTechnique() != PhysicalLevelTechnique.VARINT){
            throw new IllegalArgumentException("Specified physical level technique not yet supported.");
        }

        var values = DecodingUtils.decodeLongVarint(data, offset, streamMetadata.numValues());

        switch (streamMetadata.logicalLevelTechnique()){
            case RLE:{
                var decodedValues =  decodeRLE(values, streamMetadata.numRunValues().get());
                return isSigned? decodeZigZag(decodedValues.stream().mapToLong(i -> i).toArray()) : decodedValues;
            }
            case DELTA:
                return decodeZigZagDelta(values);
            case DELTA_RLE:{
                return decodeDeltaRLE(values, streamMetadata.numRunValues().get());
            }
            case NONE:{
                var decodedValues = Arrays.stream(values).boxed().collect(Collectors.toList());
                return isSigned? decodeZigZag(decodedValues.stream().mapToLong(i -> i).toArray()) : decodedValues;
            }
            default:
                throw new IllegalArgumentException("The specified logical level technique is not supported for integers.");
        }
    }

    //TODO: quick and dirty -> write fast vectorized solution
    private static List<Integer> decodeRLE(int[] data, int numRuns){
        //TODO: add numRleValues property to StreamMetadata to allocate values array
        var values = new ArrayList<Integer>();
        for(var i = 0; i < numRuns; i++){
            var run = data[i];
            var value = data[i + numRuns];
            for(var j = 0; j < run; j++){
                values.add(value);
            }
        }

        return values;
    }

    //TODO: quick and dirty -> write fast vectorized solution
    private static List<Long> decodeRLE(long[] data, int numRuns){
        var values = new ArrayList<Long>();
        for(var i = 0; i < numRuns; i++){
            var run = data[i];
            var value = data[i + numRuns];
            for(var j = 0; j < run; j++){
                values.add(value);
            }
        }

        return values;
    }

    private static List<Integer> decodeDeltaRLE(int[] data, int numRuns){
        var deltaValues = new ArrayList<Integer>();
        for(var i = 0; i < numRuns; i++){
            var run = data[i];
            /* Only values are zig-zag encoded */
            var delta = DecodingUtils.decodeZigZag(data[i + numRuns]);
            //values.add(delta + previousValue);
            for(var j = 0; j < run; j++){
                deltaValues.add(delta);
            }
        }

        //TODO: merge rle and delta encoding
        var values = new ArrayList<Integer>(deltaValues.size());
        var previousValue = 0;
        for(var delta : deltaValues){
            var value = delta + previousValue;
            values.add(value);
            previousValue = value;
        }

        return values;
    }

    private static List<Long> decodeDeltaRLE(long[] data, int numRuns){
        var deltaValues = new ArrayList<Long>();
        for(var i = 0; i < numRuns; i++){
            var run = data[i];
            /* Only values are zig-zag encoded */
            var delta = DecodingUtils.decodeZigZag(data[i + numRuns]);
            //values.add(delta + previousValue);
            for(var j = 0; j < run; j++){
                deltaValues.add(delta);
            }
        }

        //TODO: merge rle and delta encoding
        var values = new ArrayList<Long>(deltaValues.size());
        var previousValue = 0l;
        for(var delta : deltaValues){
            var value = delta + previousValue;
            values.add(value);
            previousValue = value;
        }

        return values;
    }

    private static List<Integer> decodeZigZagDelta(int[] data){
        var values = new ArrayList<Integer>(data.length);
        var previousValue = 0;
        for(var zigZagDelta : data){
            var delta = DecodingUtils.decodeZigZag(zigZagDelta);
            var value = previousValue + delta;
            values.add(value);
            previousValue = value;
        }

        return values;
    }

    private static List<Long> decodeZigZagDelta(long[] data){
        var values = new ArrayList<Long>(data.length);
        var previousValue = 0l;
        for(var zigZagDelta : data){
            var delta = DecodingUtils.decodeZigZag(zigZagDelta);
            var value = previousValue + delta;
            values.add(value);
            previousValue = value;
        }

        return values;
    }

    private static List<Long> decodeZigZag(long[] data){
        var values = new ArrayList<Long>(data.length);
        for(var zigZagDelta : data){
            var value = DecodingUtils.decodeZigZag(zigZagDelta);
            values.add(value);
        }
        return values;
    }

    private static List<Integer> decodeZigZag(int[] data){
        var values = new ArrayList<Integer>(data.length);
        for(var zigZagDelta : data){
            var value = DecodingUtils.decodeZigZag(zigZagDelta);
            values.add(value);
        }
        return values;
    }
}
