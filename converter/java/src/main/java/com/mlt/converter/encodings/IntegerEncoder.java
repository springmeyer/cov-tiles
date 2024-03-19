package com.mlt.converter.encodings;

import com.google.common.collect.Lists;
import com.mlt.converter.geometry.ZOrderCurve;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IntegerEncoder {

    static class IntegerEncodingResult{
        public LogicalLevelIntegerTechnique technique;
        public byte[] encodedValues;
        /* If rle or delta-rle encoding is used, otherwise can be ignored */
        public int numRuns;
        public int physicalLevelEncodedValuesLength;
    }

    public enum LogicalLevelIntegerTechnique{
        PLAIN,
        DELTA,
        RLE,
        DELTA_RLE
    }

    private IntegerEncoder(){}

    public static byte[] encodeMortonStream(List<Integer> values, int numBits, int coordinateShift){
        var encodedValueStream = encodeMortonCodes(values);
        var v = encodedValueStream.getRight();
        var valuesMetadata = new StreamMetadata(StreamType.MORTON_VERTEX_BUFFER, LogicalLevelTechnique.DELTA, encodedValueStream.getLeft(),
                v.physicalLevelEncodedValuesLength, v.encodedValues.length,
                numBits, coordinateShift).encode();

        return ArrayUtils.addAll(valuesMetadata, v.encodedValues);
    }

    public static byte[] encodeIntStream(List<Integer> values, PhysicalLevelTechnique physicalLevelTechnique, boolean isSigned,
                                         StreamType streamType){
        var encodedValueStream = IntegerEncoder.encodeInt(values, physicalLevelTechnique, isSigned);
        var valueStreamLogicalLevelTechnique = LogicalLevelTechnique.fromIntegerLogicalLevelTechnique(encodedValueStream.technique);
        var valuesMetadata = new StreamMetadata(streamType, valueStreamLogicalLevelTechnique, physicalLevelTechnique,
                encodedValueStream.physicalLevelEncodedValuesLength, encodedValueStream.encodedValues.length, valueStreamLogicalLevelTechnique == LogicalLevelTechnique.RLE ||
                valueStreamLogicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE?
                Optional.of(encodedValueStream.numRuns): Optional.empty()).encode();

        return ArrayUtils.addAll(valuesMetadata, encodedValueStream.encodedValues);
    }

    public static byte[] encodeLongStream(List<Long> values, boolean isSigned,
                                         StreamType streamType){
        /* Currently FastPfor is only supported with 32 bit so for long we always have to fallback to Varint encoding */
        var encodedValueStream = IntegerEncoder.encodeLong(values, isSigned);
        var valueStreamLogicalLevelTechnique = LogicalLevelTechnique.fromIntegerLogicalLevelTechnique(encodedValueStream.technique);
        var valuesMetadata = new StreamMetadata(streamType, valueStreamLogicalLevelTechnique, PhysicalLevelTechnique.VARINT,
                encodedValueStream.physicalLevelEncodedValuesLength, encodedValueStream.encodedValues.length, valueStreamLogicalLevelTechnique == LogicalLevelTechnique.RLE ||
                valueStreamLogicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE?
                Optional.of(encodedValueStream.numRuns): Optional.empty()).encode();

        return ArrayUtils.addAll(valuesMetadata, encodedValueStream.encodedValues);
    }

    //TODO: make dependent on specified LogicalLevelTechnique
    public static Pair<PhysicalLevelTechnique, IntegerEncodingResult> encodeMortonCodes(List<Integer> values) {
        var previousValue = 0;
        var deltaValues = new ArrayList<Integer>();
        for(var i = 0; i < values.size(); i++){
            var value = values.get(i);
            var delta = value - previousValue;
            deltaValues.add(delta);
            previousValue = value;
        }

        var fastPforDelta = encodeFastPfor(deltaValues, false);
        var varintDelta = encodeVarint(deltaValues.stream().mapToLong(i -> i).boxed().collect(Collectors.toList()), false);

        var result = new IntegerEncodingResult();
        result.technique = LogicalLevelIntegerTechnique.DELTA;
        result.physicalLevelEncodedValuesLength = values.size();
        result.numRuns = 0;
        if(fastPforDelta.length < varintDelta.length){
            result.encodedValues = fastPforDelta;
            return Pair.of(PhysicalLevelTechnique.FAST_PFOR, result);
        }

        result.encodedValues = varintDelta;
        return Pair.of(PhysicalLevelTechnique.VARINT, result);
    }

    /*
    * Integers are encoded based on the two lightweight compression techniques delta and rle as well
    * as a combination of both schemes called delta-rle.
    *
    * */
    public static IntegerEncodingResult encodeInt(List<Integer> values, PhysicalLevelTechnique physicalLevelTechnique, boolean isSigned) {
        var previousValue = 0;
        var previousDelta = 0;
        var runs = 1;
        var deltaRuns = 1;
        var deltaValues = new ArrayList<Integer>();
        for(var i = 0; i < values.size(); i++){
            var value = values.get(i);
            var delta = value - previousValue;
            deltaValues.add(delta);

            if(value != previousValue && i != 0){
                runs++;
            }

            if(delta != previousDelta && i != 0){
                deltaRuns++;
            }

            previousValue = value;
            previousDelta = delta;
        }

        BiFunction<List<Integer>, Boolean, byte[]> encoder = physicalLevelTechnique == PhysicalLevelTechnique.FAST_PFOR?
                (v, s) -> encodeFastPfor(v, s) :
                (v, s) -> encodeVarint(v.stream().mapToLong(i -> i).boxed().collect(Collectors.toList()), s);

        var plainEncodedValues = encoder.apply(values, isSigned);
        var deltaEncodedValues = encoder.apply(deltaValues, true);
        var encodedValues = Lists.newArrayList(plainEncodedValues, deltaEncodedValues);
        byte[] rleEncodedValues = null;
        byte[] deltaRleEncodedValues = null;
        var rlePhysicalLevelEncodedValuesLength = 0;
        var deltaRlePhysicalLevelEncodedValuesLength = 0;

        /* Use selection logic from BTR Blocks -> https://github.com/maxi-k/btrblocks/blob/c954ffd31f0873003dbc26bf1676ac460d7a3b05/btrblocks/scheme/double/RLE.cpp#L17 */
        if(values.size() / runs >= 2){
            //TODO: get rid of conversion
            var rleValues = EncodingUtils.encodeRle(values.stream().mapToInt(i -> i).toArray());
            rlePhysicalLevelEncodedValuesLength = rleValues.getLeft().size() + rleValues.getRight().size();
            rleEncodedValues = encoder.apply(Stream.concat(rleValues.getLeft().stream(), isSigned?
                            Arrays.stream(EncodingUtils.encodeZigZag(rleValues.getRight().stream().mapToInt(i -> i).toArray())).boxed() :
                            rleValues.getRight().stream()).toList(), false);
        }

        if(deltaValues.size() / deltaRuns >= 2){
            //TODO: get rid of conversion
            var deltaRleValues = EncodingUtils.encodeRle(deltaValues.stream().mapToInt(i -> i).toArray());
            deltaRlePhysicalLevelEncodedValuesLength = deltaRleValues.getLeft().size() + deltaRleValues.getRight().size();
            var zigZagDelta = EncodingUtils.encodeZigZag(deltaRleValues.getRight().stream().mapToInt(i -> i).toArray());
            //TODO: encode runs and length separate?
            deltaRleEncodedValues = encoder.apply(Stream.concat(deltaRleValues.getLeft().stream(), Arrays.stream(zigZagDelta).boxed()).toList(), false);
        }

        encodedValues.add(rleEncodedValues);
        encodedValues.add(deltaRleEncodedValues);

        //TODO: refactor -> find proper solution
        var encodedValuesSizes = encodedValues.stream().map(v -> v == null ? Integer.MAX_VALUE: v.length).collect(Collectors.toList());
        var index = encodedValuesSizes.indexOf(Collections.min(encodedValuesSizes));
        var encoding = LogicalLevelIntegerTechnique.values()[index];

        var result = new IntegerEncodingResult();
        result.encodedValues = encodedValues.get(index);
        result.technique = encoding;
        result.physicalLevelEncodedValuesLength = values.size();
        if(encoding == LogicalLevelIntegerTechnique.RLE){
            result.numRuns = runs;
            result.physicalLevelEncodedValuesLength = rlePhysicalLevelEncodedValuesLength;
        }
        if(encoding == LogicalLevelIntegerTechnique.DELTA_RLE){
            result.numRuns = deltaRuns;
            result.physicalLevelEncodedValuesLength = deltaRlePhysicalLevelEncodedValuesLength;
        }

        return result;
    }

    //TODO: make generic to merge with encodeInt
    public static IntegerEncodingResult encodeLong(List<Long> values, boolean isSigned){
        var previousValue = 0l;
        var previousDelta = 0l;
        var runs = 1;
        var deltaRuns = 1;
        var deltaValues = new ArrayList<Long>();
        for(var i = 0; i < values.size(); i++){
            var value = values.get(i);
            var delta = value - previousValue;
            deltaValues.add(delta);

            if(value != previousValue && i != 0){
                runs++;
            }

            if(delta != previousDelta && i != 0){
                deltaRuns++;
            }

            previousValue = value;
            previousDelta = delta;
        }

        BiFunction<List<Long>, Boolean, byte[]> encoder =
                (v, s) -> encodeVarint(v.stream().mapToLong(i -> i).boxed().collect(Collectors.toList()), s);

        var plainEncodedValues = encoder.apply(values, isSigned);
        var deltaEncodedValues = encoder.apply(deltaValues, true);
        var encodedValues = Lists.newArrayList(plainEncodedValues, deltaEncodedValues);

        byte[] rleEncodedValues = null;
        byte[] deltaRleEncodedValues = null;
        var rlePhysicalLevelEncodedValuesLength = 0;
        var deltaRlePhysicalLevelEncodedValuesLength = 0;

        /* Use selection logic from BTR Blocks -> https://github.com/maxi-k/btrblocks/blob/c954ffd31f0873003dbc26bf1676ac460d7a3b05/btrblocks/scheme/double/RLE.cpp#L17 */
        if(values.size() / runs >= 2){
            //TODO: get rid of conversion
            var rleValues = EncodingUtils.encodeRle(values.stream().mapToLong(i -> i).toArray());
            rlePhysicalLevelEncodedValuesLength = rleValues.getLeft().size() + rleValues.getRight().size();
            rleEncodedValues = encoder.apply(Stream.concat(rleValues.getLeft().stream().mapToLong(i -> i).boxed(), isSigned?
                    Arrays.stream(EncodingUtils.encodeZigZag(rleValues.getRight().stream().mapToLong(i -> i).toArray())).boxed() :
                    rleValues.getRight().stream()).toList(), false);
        }

        if(deltaValues.size() / deltaRuns >= 2){
            //TODO: get rid of conversion
            var deltaRleValues = EncodingUtils.encodeRle(deltaValues.stream().mapToLong(i -> i).toArray());
            deltaRlePhysicalLevelEncodedValuesLength = deltaRleValues.getLeft().size() + deltaRleValues.getRight().size();
            var zigZagDelta = EncodingUtils.encodeZigZag(deltaRleValues.getRight().stream().mapToLong(i -> i).toArray());
            //TODO: encode runs and length separate?
            deltaRleEncodedValues = encoder.apply(Stream.concat(deltaRleValues.getLeft().stream().mapToLong(i -> i).boxed(),
                    Arrays.stream(zigZagDelta).boxed()).toList(), false);
        }

        encodedValues.add(rleEncodedValues);
        encodedValues.add(deltaRleEncodedValues);

        //TODO: refactor -> find proper solution
        var encodedValuesSizes = encodedValues.stream().map(v -> v == null ? Integer.MAX_VALUE: v.length).collect(Collectors.toList());
        var index = encodedValuesSizes.indexOf(Collections.min(encodedValuesSizes));
        var encoding = LogicalLevelIntegerTechnique.values()[index];

        var result = new IntegerEncodingResult();
        result.encodedValues = encodedValues.get(index);
        result.technique = encoding;
        result.physicalLevelEncodedValuesLength = values.size();
        if(encoding == LogicalLevelIntegerTechnique.RLE){
            result.numRuns = runs;
            result.physicalLevelEncodedValuesLength = rlePhysicalLevelEncodedValuesLength;
        }
        if(encoding == LogicalLevelIntegerTechnique.DELTA_RLE){
            result.numRuns = deltaRuns;
            result.physicalLevelEncodedValuesLength = deltaRlePhysicalLevelEncodedValuesLength;
        }

        return result;
    }

    private static byte[] encodeFastPfor(List<Integer> values, boolean signed){
        return EncodingUtils.encodeFastPfor128(values.stream().mapToInt(i -> i).toArray(), signed, false);
    }

    private static byte[] encodeVarint(List<Long> values, boolean signed){
        return  EncodingUtils.encodeVarints(values.stream().mapToLong(i -> i).toArray(), signed, false);
    }

    /*
     * TODOs:
     * - find selection algorithm -> find sampling method and use statistics e.g. for rle and dictionary encoding
     * - which rle method to use
     *   - separate runs and values array or store as pairs?
     *   - when separate arrays use FastPFOR on each separate to have fewer exceptions?
     *   - more efficient encodings of literals like ORC RLE V1? -> analyze topology columns and int property columns
     *   - when separate arrays how encode in metadata?
     * */
    /*
     * -> iterate of colum and calculate average run length
     * -> if average run length > 2 -> add RLE to encoding scheme pool
     * -> else use delta and plain encoding -> compare delta with plain
     * -> if delta is used test again for rle encoding
     * -> use null suppression
     *   -> if long varint
     *   -> if int and not optimized for gzip -> SIMD-FastPfor
     *   -> if optimized for gzip -> varint
     * */
    /* Use selection logic from BTR Blocks -> https://github.com/maxi-k/btrblocks/blob/c954ffd31f0873003dbc26bf1676ac460d7a3b05/btrblocks/scheme/double/RLE.cpp#L17 */
    /*public static byte[] convertUnsignedInt(List<Integer> values, NullSuppressionEncoder encoder) throws IOException {

        var previousValue = 0;
        var runs = 1;
        var deltaValues = new ArrayList<Integer>();
        for(var i = 0; i < values.size(); i++){
            var value = values.get(i);
            var delta = value - previousValue;
            deltaValues.add(delta);

            if(value != previousValue && i != 0){
                runs++;
            }

            previousValue = value;
        }

        //TODO: use sum bit width of deltas to detect if delta can be used

        var samples = getSamples(values);
        var deltaSamples = getSamples(deltaValues);

        var plainEncodedSize = encoder.encode(samples, false).length;
        var deltaEncodedSize = encoder.encode(deltaSamples, true).length;

       if(values.size() / runs >= 2){
            // add rle to scheme encoding pool
            //TODO: get rid of that conversion
            //var rleEncodedSize = EncodingUtils.encodeDeltaRle(samples.stream().mapToLong(i -> i).toArray(), false);
            //var deltaRleEncodedSize = EncodingUtils.encodeDeltaRle(deltaSamples.stream().mapToLong(i -> i).toArray(), false);
            //TODO: get rid of conversion
            var rleEncodedSamples = EncodingUtils.encodeRle(samples.stream().mapToInt(i -> i).toArray());
            var rleEncodedSamplesSize = encoder.encode(Stream.concat(rleEncodedSamples.getLeft().stream(),
                    rleEncodedSamples.getRight().stream()).toList(), false).length;
            //System.out.println(rleEncodedSamples);
        }

        //TODO: check again runs if rle should be usded again
        //TODO: encode runs and length separate?
        //TODO: add size and length for runs and values in the metadata
        var deltaRleEncodedSamples = EncodingUtils.encodeRle(deltaSamples.stream().mapToInt(i -> i).toArray());
        //TODO: only values has to be zig-zag encoded not runs
        var deltaRleEncodedSamplesSize = encoder.encode(Stream.concat(deltaRleEncodedSamples.getLeft().stream(),
                deltaRleEncodedSamples.getRight().stream()).toList(), true).length;

        var zigZagDelta = EncodingUtils.encodeZigZag(deltaRleEncodedSamples.getRight().stream().mapToInt(i -> i).toArray());
        var optimizedDeltaRleEncodedSamplesSize = encoder.encode(
                Stream.concat(deltaRleEncodedSamples.getLeft().stream(), Arrays.stream(zigZagDelta).boxed()).toList(), false).length;


        var orcDeltaEncodedSampleSize = EncodingUtils.encodeDeltaRle(samples.stream().mapToLong(l -> l).toArray(), false);

        var separateDeltaRleEncodedSamplesSize = encoder.encode(deltaRleEncodedSamples.getLeft(), false).length +
                encoder.encode(deltaRleEncodedSamples.getRight(), true).length;

        return null;
    }*/

    private static List<Integer> getSamples(List<Integer> values){
        //TODO: implement sampling strategy
        /* Inspired by BTRBlock sampling strategy:
         *  - take about 1% of all data as samples
         *  - divide into ? blocks?
         * */
        //return List<List<Integer>>

        return values;
    }

}
