package com.mlt.converter.encodings;

import com.google.common.primitives.Bytes;
import com.mlt.converter.CollectionUtils;
import com.mlt.converter.mvt.ColumnMapping;
import com.mlt.converter.mvt.Feature;
import com.mlt.metadata.MaplibreTileMetadata;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PropertyEncoder {

    public static byte[] encodeProperties(List<Feature> features, MaplibreTileMetadata.LayerMetadata layerMetadata,
                                          PhysicalLevelTechnique physicalLevelTechnique, Optional<List<ColumnMapping>> columnMappings,
                                          boolean useSharedDictionary) throws IOException {

        var featureScopedPropertyColumns = new byte[0];

        var i = 0;
        /*
        * FieldMetadata -> Streams
        * Stream
        * -> metadata
        * -> present
        * -> length
        * -> data
        * */
        for(var fieldMetadata : layerMetadata.getFieldsList().stream().filter(f ->
                f.getDataType() != MaplibreTileMetadata.DataType.GEOMETRY && f.getDataType() != MaplibreTileMetadata.DataType.GEOMETRY_Z
                && f.getName() != "id").collect(Collectors.toList())){

            //TODO: also detect if column is nullable to get rid of the present stream in some cases
            switch (fieldMetadata.getDataType()){
                case BOOLEAN: {
                    var booleanColumn = convertBooleanColumn(features, fieldMetadata.getName());
                    var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{2}, false, false);
                    featureScopedPropertyColumns = CollectionUtils.concatByteArrays(featureScopedPropertyColumns, encodedFieldMetadata, booleanColumn);
                    break;
                }
                case UINT_32:{
                    var intColumn = encodeInt32Column(features, fieldMetadata.getName(), physicalLevelTechnique, false);
                    var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{2}, false, false);
                    featureScopedPropertyColumns = CollectionUtils.concatByteArrays(featureScopedPropertyColumns, encodedFieldMetadata, intColumn);
                    break;
                }
                case INT_32:{
                    var intColumn = encodeInt32Column(features, fieldMetadata.getName(), physicalLevelTechnique, true);
                    var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{2}, false, false);
                    featureScopedPropertyColumns = CollectionUtils.concatByteArrays(featureScopedPropertyColumns, encodedFieldMetadata, intColumn);
                    break;
                }
                case UINT_64:{
                    var intColumn = encodeInt64Column(features, fieldMetadata.getName(), false);
                    var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{2}, false, false);
                    featureScopedPropertyColumns = CollectionUtils.concatByteArrays(featureScopedPropertyColumns, encodedFieldMetadata, intColumn);
                    break;
                }
                case INT_64:{
                    var intColumn = encodeInt64Column(features, fieldMetadata.getName(), true);
                    var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{2}, false, false);
                    featureScopedPropertyColumns = CollectionUtils.concatByteArrays(featureScopedPropertyColumns, encodedFieldMetadata, intColumn);
                    break;
                }
                case FLOAT:{
                    var floatColumn = encodeFloatColumn(features, fieldMetadata.getName());
                    var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{2}, false, false);
                    featureScopedPropertyColumns = CollectionUtils.concatByteArrays(featureScopedPropertyColumns, encodedFieldMetadata, floatColumn);
                    break;
                }
                case STRING: {
                    /*
                    * -> Single Column
                    *   -> Plain -> present, length, data
                    *   -> Dictionary -> present, length, data, dictonary
                    * -> N Columns Dictionary
                    *   -> SharedDictionaryLength, SharedDictionary, present1, data1, present2, data2
                    * -> N Columns FsstDictonary
                    *
                    * */
                    var present = features.stream().map(f -> f.properties().get(fieldMetadata.getName()) != null)
                            .collect(Collectors.toList());
                    var presentStream = BooleanEncoder.encodeBooleanStream(present, StreamType.PRESENT);
                    var values = features.stream().map(f -> (String)f.properties().get(fieldMetadata.getName())).
                            filter(v -> v != null).collect(Collectors.toList());
                    var stringColumn = StringEncoder.encode(values, PhysicalLevelTechnique.FAST_PFOR);
                    /* Add 1 for present stream */
                    var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{stringColumn.getLeft() + 1}, false, false);
                    featureScopedPropertyColumns = CollectionUtils.concatByteArrays(featureScopedPropertyColumns, encodedFieldMetadata, presentStream, stringColumn.getRight());
                    break;
                }
                case STRUCT:{
                    /* We limit the nesting level to one in this implementation */
                    //var sharedDictionary = new ArrayList<Pair<List<Boolean>, List<String>>>();
                    var sharedDictionary = new ArrayList<List<String>>();
                    var columnMapping = columnMappings.get().get(i++);

                    /* Plan -> when there is a struct filed and the useShareDictionaryFlag is enabled
                     *  share the dictionary for all string columns which are located one after
                     * the other in the sequence */
                    /*if(useSharedDictionary){
                        var sharedDictionaryColumns = new ArrayList<List<String>>();
                        var fields = fieldMetadata.getChildrenList();
                        var isPreviousFieldString = fields.get(0).getDataType() == MaplibreTileMetadata.DataType.STRING;
                        for(var j = 1; i < fields.size(); j++){
                            if(fields.get(0).getDataType() == MaplibreTileMetadata.DataType.STRING && isPreviousFieldString){
                                //TODO: add null values if not presetn
                                sharedDictionaryColumns.add()
                            }

                        }
                    }*/

                    for(var nestedFieldMetadata : fieldMetadata.getChildrenList()){
                        if(nestedFieldMetadata.getDataType() == MaplibreTileMetadata.DataType.STRING){
                            if(useSharedDictionary){
                                //request all string columns in row and merge
                                if(nestedFieldMetadata.getName() == "default"){
                                    /*var propertyColumn = features.stream().map(f -> (String)f.properties().
                                            get(columnMapping.mvtPropertyPrefix())).filter(p -> p != null).collect(Collectors.toList());
                                    var presentStream =  features.stream().map(f -> f.properties().
                                            get(columnMapping.mvtPropertyPrefix()) != null).collect(Collectors.toList());
                                    sharedDictionary.add(Pair.of(presentStream, propertyColumn));*/
                                    var propertyColumn = features.stream().map(f -> (String)f.properties().
                                            get(columnMapping.mvtPropertyPrefix())).collect(Collectors.toList());
                                    sharedDictionary.add(propertyColumn);
                                }
                                else{
                                    /*var propertyColumn = features.stream().map(f -> (String)f.properties().get(columnMapping.mvtPropertyPrefix() +
                                            columnMapping.mvtDelimiterSign() + nestedFieldMetadata.getName())).filter(p -> p != null).collect(Collectors.toList());
                                    var presentStream =  features.stream().map(f -> f.properties().get(columnMapping.mvtPropertyPrefix() +
                                            columnMapping.mvtDelimiterSign() + nestedFieldMetadata.getName()) != null).collect(Collectors.toList());
                                    sharedDictionary.add(propertyColumn);*/
                                    var propertyColumn = features.stream().map(f -> (String)f.properties().get(columnMapping.mvtPropertyPrefix() +
                                            columnMapping.mvtDelimiterSign() + nestedFieldMetadata.getName())).collect(Collectors.toList());
                                    sharedDictionary.add(propertyColumn);
                                }
                            }
                            else{
                                throw new IllegalArgumentException("Only shared dictionary encoding is currently supported for nested properties.");
                            }
                        }
                        else{
                            throw new IllegalArgumentException("Only fields of type String are currently supported as nested properties.");
                            //convertProperties(features, layerMetadata, physicalLevelTechnique, columnMappings, useSharedDictionary);
                        }
                    }

                    var nestedColumns = StringEncoder.encodeSharedDictionary(sharedDictionary, PhysicalLevelTechnique.FAST_PFOR);
                    var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{0}, false, false);
                    //TODO: add present stream and present stream metadata for struct column
                    featureScopedPropertyColumns = CollectionUtils.concatByteArrays(featureScopedPropertyColumns, encodedFieldMetadata, nestedColumns.getRight());
                    break;
                }
                default:
                    throw new IllegalArgumentException("The specified data type for the field is currently not supported.");
            }
        }

        return featureScopedPropertyColumns;
    }

    private static byte[] convertBooleanColumn(List<Feature> features, String fieldName) throws IOException {
        var presentStream = new BitSet(features.size());
        var dataStream = new BitSet();
        var dataStreamIndex = 0;
        var presentStreamIndex = 0;
        for(var feature : features){
            var propertyValue = feature.properties().get(fieldName);
            if(propertyValue != null){
                dataStream.set(dataStreamIndex++, (boolean) propertyValue);
                presentStream.set(presentStreamIndex++, true);
            }
            else{
                presentStream.set(presentStreamIndex++, false);
            }
        }

        //TODO: test boolean rle against roaring bitmaps and integer encoding
        var encodedPresentStream = EncodingUtils.encodeBooleanRle(presentStream, presentStreamIndex);
        var encodedDataStream = EncodingUtils.encodeBooleanRle(dataStream, dataStreamIndex);
        var encodedPresentStreamMetadata = new StreamMetadata(StreamType.PRESENT, LogicalLevelTechnique.BOOLEAN_RLE,
                PhysicalLevelTechnique.VARINT, presentStreamIndex, encodedPresentStream.length, Optional.empty()).encode();
        var encodedDataStreamMetadata = new StreamMetadata(StreamType.DATA, LogicalLevelTechnique.BOOLEAN_RLE,
                PhysicalLevelTechnique.VARINT, dataStreamIndex, encodedDataStream.length, Optional.empty()).encode();
        return Bytes.concat(encodedPresentStreamMetadata, encodedPresentStream, encodedDataStreamMetadata, encodedDataStream);
    }

    private static byte[] encodeFloatColumn(List<Feature> features, String fieldName) throws IOException {
        var values = new ArrayList<Float>();
        var present = new ArrayList<Boolean>(features.size());
        for (var feature : features) {
            var propertyValue = feature.properties().get(fieldName);
            if (propertyValue != null) {
                values.add((float) propertyValue);
                present.add(true);
            } else {
                present.add(false);
            }
        }

        //TODO: test boolean rle against roaring bitmaps and integer encoding
        var encodedPresentStream = BooleanEncoder.encodeBooleanStream(present, StreamType.PRESENT);
        var encodedDataStream = FloatEncoder.encodeFloatStream(values);

        return Bytes.concat(encodedPresentStream, encodedDataStream);
    }

    private static byte[] encodeInt32Column(List<Feature> features, String fieldName, PhysicalLevelTechnique physicalLevelTechnique,
                                            boolean isSigned) throws IOException {
        var values = new ArrayList<Integer>();
        var present = new ArrayList<Boolean>(features.size());
        for(var feature : features){
            var propertyValue = feature.properties().get(fieldName);
            if(propertyValue != null){
                values.add((int)propertyValue);
                present.add(true);
            }
            else{
                present.add(false);
            }
        }

        //TODO: test boolean rle against roaring bitmaps and integer encoding
        var encodedPresentStream = BooleanEncoder.encodeBooleanStream(present, StreamType.PRESENT);
        var encodedDataStream = IntegerEncoder.encodeIntStream(values, physicalLevelTechnique, isSigned, StreamType.DATA);

        return Bytes.concat(encodedPresentStream, encodedDataStream);
    }

    private static byte[] encodeInt64Column(List<Feature> features, String fieldName, boolean isSigned) throws IOException {
        var values = new ArrayList<Long>();
        var present = new ArrayList<Boolean>(features.size());
        for(var feature : features){
            var propertyValue = feature.properties().get(fieldName);
            if(propertyValue != null){
                values.add((long)propertyValue);
                present.add(true);
            }
            else{
                present.add(false);
            }
        }

        //TODO: test boolean rle against roaring bitmaps and integer encoding
        var encodedPresentStream = BooleanEncoder.encodeBooleanStream(present, StreamType.PRESENT);
        var encodedDataStream = IntegerEncoder.encodeLongStream(values, isSigned, StreamType.DATA);

        return Bytes.concat(encodedPresentStream, encodedDataStream);
    }


    /*private static byte[] encodeStreamMetadata(StreamType streamType, LogicalLevelTechnique logicalLevelTechnique,
                                        PhysicalLevelTechnique physicalLevelTechnique, int numValues, int byteLength,
                                        Optional<Integer> numRuns){
        //TODO: really narrow down PhysicalLevelTechnique to one bit so no room for extension?
        var header = (byte)(streamType.ordinal() << 4 | logicalLevelTechnique.ordinal() << 1 | physicalLevelTechnique.ordinal());
        var varints = numRuns.isPresent()? new long[]{numValues, byteLength, numRuns.get()} : new long[]{numValues, byteLength};
        var encodedVarints = EncodingUtils.encodeVarints(varints, false, false);
        return Bytes.concat(new byte[]{header}, encodedVarints);
    }*/

    /*private static <T, U> U convertPropertyColumn(String columnName, ColumnMetadata metadata, List<Feature> features,
                                                  TriFunction<ColumnMetadata, List<Boolean>, List<T>, U> columnDataFunc){
        var presentStream = new ArrayList<Boolean>();
        var dataStream = new ArrayList<T>();

        for(var feature : features){
            var properties = feature.properties();
            var propertyValue = properties.get(columnName);

            if(!properties.containsKey(columnName)){
                presentStream.add(false);
                continue;
            }

            presentStream.add(true);
            dataStream.add((T)propertyValue);
        }

        return columnDataFunc.apply(metadata, presentStream, dataStream);
    }*/

}
