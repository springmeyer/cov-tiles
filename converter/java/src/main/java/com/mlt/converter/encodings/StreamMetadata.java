package com.mlt.converter.encodings;

import com.google.common.primitives.Bytes;
import com.mlt.converter.CollectionUtils;
import com.mlt.decoder.DecodingUtils;
import me.lemire.integercompression.IntWrapper;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Optional;

public class StreamMetadata{
    private StreamType streamType;
    private LogicalLevelTechnique logicalLevelTechnique;
    private PhysicalLevelTechnique physicalLevelTechnique;
    private int numValues;
    private int byteLength;
    private Optional<Integer> numRunValues;
    private int numBits;
    private int coordinateShift;

    //TODO: refactor to use build pattern
    public StreamMetadata(StreamType streamType, LogicalLevelTechnique logicalLevelTechnique,
                              PhysicalLevelTechnique physicalLevelTechnique, int numValues, int byteLength, Optional<Integer> numRunValues) {
        this.streamType = streamType;
        this.logicalLevelTechnique = logicalLevelTechnique;
        this.physicalLevelTechnique = physicalLevelTechnique;
        this.numValues = numValues;
        this.byteLength = byteLength;
        this.numRunValues = numRunValues;
        this.numBits = -1;
    }

    public StreamMetadata(StreamType streamType, LogicalLevelTechnique logicalLevelTechnique,
                          PhysicalLevelTechnique physicalLevelTechnique, int numValues, int byteLength,
                          int numBits, int coordinateShift) {
        this(streamType, logicalLevelTechnique, physicalLevelTechnique, numValues, byteLength, Optional.empty());
        this.numBits = numBits;
        this.coordinateShift = coordinateShift;
    }

    public byte[] encode(){
        //TODO: really narrow down PhysicalLevelTechnique to one bit so no room for extension?
        var encodedStreamType = (byte)(streamType.ordinal());
        var encodedEncodingScheme  =(byte)(logicalLevelTechnique.ordinal() << 4 | physicalLevelTechnique.ordinal());
        var varints = numRunValues.isPresent()? new long[]{numValues, byteLength, numRunValues.get()} : new long[]{numValues, byteLength};
        var encodedVarints = EncodingUtils.encodeVarints(varints, false, false);
        //TODO: refactor
        if(numBits > -1){
            var mortonInfos = EncodingUtils.encodeVarints(new long[]{numBits, coordinateShift}, false, false);
            encodedVarints = ArrayUtils.addAll(encodedVarints, mortonInfos);
        }
        return Bytes.concat(new byte[]{encodedStreamType, encodedEncodingScheme}, encodedVarints);
    }

    public static StreamMetadata decode(byte[] tile, IntWrapper offset){
        var streamType = StreamType.values()[tile[offset.get()]];
        offset.increment();
        var encodingsHeader = tile[offset.get()];
        var logicalLevelTechnique = LogicalLevelTechnique.values()[encodingsHeader >> 4];
        var physicalLevelTechnique = PhysicalLevelTechnique.values()[encodingsHeader & 0xF];
        offset.increment();
        var sizeInfos = DecodingUtils.decodeVarint(tile, offset, 2);
        var numValues = sizeInfos[0];
        var byteLength = sizeInfos[1];
        Optional<Integer> numRuns = logicalLevelTechnique == LogicalLevelTechnique.RLE || logicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE?
                Optional.of(DecodingUtils.decodeVarint(tile, offset, 1)[0]): Optional.empty();
        if(streamType != StreamType.MORTON_VERTEX_BUFFER){
            return new StreamMetadata(streamType, logicalLevelTechnique, physicalLevelTechnique, numValues, byteLength, numRuns);
        }

        var mortonInfos = DecodingUtils.decodeVarint(tile, offset, 2);
        var numBits = mortonInfos[0];
        var coordinateShift = mortonInfos[1];
        return new StreamMetadata(streamType, logicalLevelTechnique, physicalLevelTechnique, numValues, byteLength,
                numBits, coordinateShift);
    }

    public StreamType streamType(){
        return this.streamType;
    }

    public LogicalLevelTechnique logicalLevelTechnique(){
        return this.logicalLevelTechnique;
    }

    public PhysicalLevelTechnique physicalLevelTechnique(){
        return this.physicalLevelTechnique;
    }

    public int numValues(){
        return this.numValues;
    }

    public int byteLength(){
        return this.byteLength;
    }

    public Optional<Integer> numRunValues(){
        return this.numRunValues;
    }

    public int numBits(){
        return this.numBits;
    }

    public int coordinateShift(){
        return this.coordinateShift;
    }
}

/*public record StreamMetadata(StreamType streamType, LogicalLevelTechnique logicalLevelTechnique,
                             PhysicalLevelTechnique physicalLevelTechnique, int numValues, int byteLength, Optional<Integer> numRunValues){

    public byte[] encode(){
        //TODO: really narrow down PhysicalLevelTechnique to one bit so no room for extension?
        var encodedStreamType = (byte)(streamType.ordinal());
        var encodedEncodingScheme  =(byte)(logicalLevelTechnique.ordinal() << 4 | physicalLevelTechnique.ordinal());
        var varints = numRunValues.isPresent()? new long[]{numValues, byteLength, numRunValues.get()} : new long[]{numValues, byteLength};
        var encodedVarints = EncodingUtils.encodeVarints(varints, false, false);
        return Bytes.concat(new byte[]{encodedStreamType, encodedEncodingScheme}, encodedVarints);
    }

    public static StreamMetadata decode(byte[] tile, IntWrapper offset){
        var streamType = StreamType.values()[tile[offset.get()]];
        offset.increment();
        var encodingsHeader = tile[offset.get()];
        var logicalLevelTechnique = LogicalLevelTechnique.values()[encodingsHeader >> 4];
        var physicalLevelTechnique = PhysicalLevelTechnique.values()[encodingsHeader & 0xF];
        offset.increment();
        var sizeInfos = DecodingUtils.decodeVarint(tile, offset, 2);
        var numValues = sizeInfos[0];
        var byteLength = sizeInfos[1];
        Optional<Integer> numRuns = logicalLevelTechnique == LogicalLevelTechnique.RLE || logicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE?
                Optional.of(DecodingUtils.decodeVarint(tile, offset, 1)[0]): Optional.empty();
        return new StreamMetadata(streamType, logicalLevelTechnique, physicalLevelTechnique, numValues, byteLength, numRuns);
    }
}*/

