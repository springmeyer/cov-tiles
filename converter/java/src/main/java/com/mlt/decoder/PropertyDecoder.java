package com.mlt.decoder;

import com.mlt.converter.encodings.StreamMetadata;
import com.mlt.metadata.MaplibreTileMetadata;
import me.lemire.integercompression.IntWrapper;

import java.io.IOException;
import java.util.*;

public class PropertyDecoder {

    private PropertyDecoder(){}

    public static Object decodePropertyColumn(byte[] data, IntWrapper offset, MaplibreTileMetadata.FieldMetadata fieldMetadata, int numStreams) throws IOException {
        var columnDataType = fieldMetadata.getDataType();
        StreamMetadata presentStreamMetadata = null;
        BitSet presentStream = null;
        if(numStreams > 1 && (columnDataType == MaplibreTileMetadata.DataType.BOOLEAN || columnDataType == MaplibreTileMetadata.DataType.UINT_32 ||
                columnDataType == MaplibreTileMetadata.DataType.INT_32 || columnDataType == MaplibreTileMetadata.DataType.UINT_64 ||
                columnDataType == MaplibreTileMetadata.DataType.INT_64 || columnDataType == MaplibreTileMetadata.DataType.FLOAT ||
                columnDataType == MaplibreTileMetadata.DataType.DOUBLE)){
            presentStreamMetadata = StreamMetadata.decode(data, offset);
            presentStream = DecodingUtils.decodeBooleanRle(data, presentStreamMetadata.numValues(), presentStreamMetadata.byteLength(), offset);
        }

        switch (columnDataType){
            case BOOLEAN: {
                var dataStreamMetadata = StreamMetadata.decode(data, offset);
                var dataStream = DecodingUtils.decodeBooleanRle(data, dataStreamMetadata.numValues(), dataStreamMetadata.byteLength(), offset);
                var booleanValues = new ArrayList<Boolean>(presentStreamMetadata.numValues());
                var counter = 0;
                for(var i = 0; i < presentStreamMetadata.numValues(); i++){
                    var value = presentStream.get(i)? dataStream.get(counter++) : null;
                    booleanValues.add(value);
                }
                return booleanValues;
            }
            case UINT_32:{
                var dataStreamMetadata = StreamMetadata.decode(data, offset);
                var dataStream = IntegerDecoder.decodeIntStream(data, offset, dataStreamMetadata, false);
                var counter = 0;
                var values = new ArrayList<Integer>();
                for(var i = 0; i < presentStreamMetadata.numValues(); i++){
                    var value = presentStream.get(i) ? dataStream.get(counter++) : null;
                    values.add(value);
                }
                return values;
            }
            case INT_32:{
                var dataStreamMetadata = StreamMetadata.decode(data, offset);
                var dataStream = IntegerDecoder.decodeIntStream(data, offset, dataStreamMetadata, true);
                var values = new ArrayList<Integer>();
                var counter = 0;
                for(var i = 0; i < presentStreamMetadata.numValues(); i++){
                    var value = presentStream.get(i) ? dataStream.get(counter++) : null;
                    values.add(value);
                }
                return values;
            }
            /*case UINT_64:{
                break;
            }
            case INT_64:{
                break;
            }*/
            case FLOAT:{
                var dataStreamMetadata = StreamMetadata.decode(data, offset);
                var dataStream = FloatDecoder.decodeFloatStream(data, offset, dataStreamMetadata);
                var values = new ArrayList<Float>();
                var counter = 0;
                for(var i = 0; i < presentStreamMetadata.numValues(); i++){
                    var value = presentStream.get(i) ? dataStream.get(counter++) : null;
                    values.add(value);
                }
                return values;
            }
            /*case DOUBLE:{
                break;
            }*/
            case UINT_64:{
                var dataStreamMetadata = StreamMetadata.decode(data, offset);
                var dataStream = IntegerDecoder.decodeLongStream(data, offset, dataStreamMetadata, false);
                var counter = 0;
                var values = new ArrayList<Long>();
                for(var i = 0; i < presentStreamMetadata.numValues(); i++){
                    var value = presentStream.get(i) ? dataStream.get(counter++) : null;
                    values.add(value);
                }
                return values;
            }
            case INT_64:{
                var dataStreamMetadata = StreamMetadata.decode(data, offset);
                var dataStream = IntegerDecoder.decodeLongStream(data, offset, dataStreamMetadata, true);
                var values = new ArrayList<Long>();
                var counter = 0;
                for(var i = 0; i < presentStreamMetadata.numValues(); i++){
                    var value = presentStream.get(i) ? dataStream.get(counter++) : null;
                    values.add(value);
                }
                return values;
            }
            case STRING: {
                var strValues = StringDecoder.decode(data, offset, fieldMetadata, numStreams);
                return strValues.getRight();
            }
            case STRUCT:{
                if (numStreams == 1) {
                    //var presentStreamMetadata = StreamMetadata.decode(data, offset);
                    //var presentStream = DecodingUtils.decodeBooleanRle(data, presentStreamMetadata.numValues(), presentStreamMetadata.byteLength(), offset);
                    //TODO: process present stream
                    //var values = StringDecoder.decodeSharedDictionary(data, offset, fieldMetadata);
                    throw new IllegalArgumentException("Present stream currently not supported for Structs.");
                }
                else{
                    var result = StringDecoder.decodeSharedDictionary(data, offset, fieldMetadata);
                    /*var numValues = result.getLeft();
                    var presentStreams = result.getMiddle();
                    var propertyColumns = result.getRight();
                    var propertyMap = new HashMap<String, List<String>>();
                    for(var propertyColumn : propertyColumns.entrySet()){
                        var columnName = propertyColumn.getKey();
                        var columnPresentStream = presentStreams.get(columnName);
                        var columnPropertyValues = propertyColumn.getValue();
                        var values = new ArrayList<String>();
                        var counter = 0;
                        for(var j = 0; j < numValues.get(columnName); j++){
                            var value = columnPresentStream.get(j) ? columnPropertyValues.get(counter++) : null;
                            values.add(value);
                        }
                        propertyMap.put(columnName, values);
                    }*/

                    return result.getRight();
                }
            }
            default:
                throw new IllegalArgumentException("The specified data type for the field is currently not supported.");
        }
    }

}
