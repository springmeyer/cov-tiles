package com.mlt.converter;

import com.mlt.converter.encodings.*;
import com.mlt.converter.mvt.ColumnMapping;
import com.mlt.converter.mvt.Feature;
import com.mlt.converter.mvt.MapboxVectorTile;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.mlt.metadata.MaplibreTileMetadata.*;

public class MltConverter {
    private static byte VERSION = 1;
    private static String ID_COLUMN_NAME = "id";
    private static String GEOMETRY_COLUMN_NAME = "geometry";

    /*
    * The MLT metadata are serialized to a separate protobuf file.
    * This metadata file holds the scheme for the complete tileset, so it only has to be requested once from a map client for
    * the full session because it contains the scheme information for all tiles.
    * This is a POC approach for generating the MLT metadata from a MVT tile.
    * Possible approaches in production:
    * - Generate the metadata scheme from the MVT TileJson file -> but not all types of MLT (like float, double, ...)
    *   are available in the TileJSON file
    * - Use the TileJSON as the base to get all property names and request as many tiles as needed to get the concrete data types
    * - Define a separate scheme file where all informations are contained
    * To bring the flattened MVT properties into a nested structure it has to have the following structure:
    * propertyPrefix|Delimiter|propertySuffix -> Example: name, name:us, name:en
    * */
    public static TileMetadata createTileMetadata(MapboxVectorTile mvt, Optional<List<ColumnMapping>> columnMappings, boolean isIdPresent){
        var tileMetadataBuilder = TileMetadata.newBuilder();
        for(var layer : mvt.layers()){
            //TODO: add id and geometry column
            var layerMetadata = new LinkedHashMap<String, FieldMetadata>();

            if(isIdPresent){
                /* Narrow down for unsigned long to unsigned int if possible as it can be currently more efficiently encoded
                *  based on FastPFOR (64 bit variant not yet implemented) instead of Varint
                * */
                var idDataType = layer.features().stream().allMatch(f -> f.id() < Math.pow(2, 32))
                        ? DataType.UINT_32 : DataType.UINT_64;
                var idMetadata = createFieldMetadata(ID_COLUMN_NAME, true, idDataType);
                layerMetadata.put(ID_COLUMN_NAME, idMetadata);
            }

            var geometryMetadata = createFieldMetadata(GEOMETRY_COLUMN_NAME, false, DataType.GEOMETRY);
            layerMetadata.put(GEOMETRY_COLUMN_NAME, geometryMetadata);

            var nestedPropertiesMetadata = new LinkedHashMap<String, FieldMetadata.Builder>();
            for(var feature : layer.features()){
                /* sort so that the name of the nested column comes first as it has to be the shortest */
                //TODO: refactor
                var properties = feature.properties().entrySet().stream().
                        sorted((a,b) -> a.getKey().compareTo(b.getKey())).
                        collect(Collectors.toList());
                for(var property : properties){
                    var fieldName = property.getKey();

                    if(layerMetadata.containsKey(fieldName)){
                        continue;
                    }

                    var propertyValue = property.getValue();
                    DataType dataType = null;
                    if(propertyValue instanceof Boolean){
                        dataType = DataType.BOOLEAN;
                    }
                    //TODO: also handle unsigned int to avoid zigZag coding
                    else if(propertyValue instanceof Integer){
                        dataType = DataType.INT_32;
                    }
                    //TODO: also handle unsigned long to avoid zigZag coding
                    else if(propertyValue instanceof  Long){
                        dataType = DataType.INT_64;
                    }
                    else if(propertyValue instanceof Float){
                        dataType = DataType.FLOAT;
                    }
                    else if(propertyValue instanceof Double){
                        dataType = DataType.DOUBLE;
                    }
                    else if(propertyValue instanceof String){
                        dataType = DataType.STRING;
                    }
                    else{
                        throw new IllegalArgumentException("Specified data type currently not supported.");
                    }

                    if(columnMappings.isPresent()){
                        if(columnMappings.get().stream().anyMatch(m -> fieldName.equals(m.mvtPropertyPrefix())) &&
                                !nestedPropertiesMetadata.containsKey(fieldName)){
                            /* case where the top-level field is present like name (name:de, name:us, ...) and has a a value.
                            *  In this case the field is mapped to the name default. */
                            var children = createFieldMetadata("default", true, dataType);
                            var fieldMetadataBuilder = createFieldMetadataBuilder(fieldName, children);
                            nestedPropertiesMetadata.put(fieldName, fieldMetadataBuilder);
                            continue;
                        }
                        else if (columnMappings.get().stream().anyMatch(m -> fieldName.contains(m.mvtPropertyPrefix() + m.mvtDelimiterSign()))){
                            var columnMappping = columnMappings.get().stream().
                                    filter(m ->  fieldName.contains(m.mvtPropertyPrefix() + m.mvtDelimiterSign())).findFirst().get();
                            var nestedFieldName = fieldName.split(columnMappping.mvtDelimiterSign())[1];
                            var children = createFieldMetadata(nestedFieldName, true, dataType);
                            if(nestedPropertiesMetadata.containsKey(columnMappping.mvtPropertyPrefix())){
                                /* add the nested properties to the parent like the name:* properties to the name parent */
                                if(!nestedPropertiesMetadata.get(columnMappping.mvtPropertyPrefix()).getChildrenList().stream().
                                        anyMatch(c -> c.getName().equals(nestedFieldName))){
                                    nestedPropertiesMetadata.get(columnMappping.mvtPropertyPrefix()).addChildren(children);
                                }
                            }
                            else{
                                /* Case where there is no explicit property available which serves as the name
                                * for the top-level field. For example there is no name property only name:* */
                                var fieldMetadataBuilder = createFieldMetadataBuilder(columnMappping.mvtPropertyPrefix(), children);
                                nestedPropertiesMetadata.put(fieldName, fieldMetadataBuilder);
                            }
                            continue;
                        }
                    }

                    var fieldMetadata = createFieldMetadata(fieldName, true, dataType);
                    layerMetadata.put(fieldName, fieldMetadata);
                }
            }

            nestedPropertiesMetadata.forEach((k,v) -> layerMetadata.put(k, v.build()));
            var layerMetadataBuilder = LayerMetadata.newBuilder();
            layerMetadataBuilder.setName(layer.name());
            layerMetadata.forEach((k,v) -> layerMetadataBuilder.addFields(v));
            tileMetadataBuilder.addLayers(layerMetadataBuilder.build());
        }

        //TODO: sort fields based on data type
        //var dataTypeSortedColumns  = tileMetadataBuilder.getLayers(5).getFieldsList().stream().
        //        sorted((a, b) -> a.getDataType().ordinal() - b.getDataType().ordinal()).collect(Collectors.toList());

        return tileMetadataBuilder.build();
    }

    private static FieldMetadata createFieldMetadata(String fieldName, boolean nullable, DataType dataType){
        var a = FieldMetadata.newBuilder().setName(fieldName);
        return FieldMetadata.newBuilder().setName(fieldName).setNullable(true).setDataType(dataType)
                .setColumneScope(ColumnScope.FEATURE).build();
    }

    private static FieldMetadata.Builder createFieldMetadataBuilder(String fieldName, FieldMetadata children){
        return FieldMetadata.newBuilder().setName(fieldName).setNullable(true).setDataType(DataType.STRUCT)
                .addChildren(children)
                .setColumneScope(ColumnScope.FEATURE);
    }

    /*
    *
    * Converts a MVT file to a MLT file.
    *
    * */
    public static byte[] convertMvt(MapboxVectorTile mvt, ConversionConfig config, TileMetadata tileMetadata,
                                    Optional<List<ColumnMapping>> columnMappings) throws IOException {
        var mapLibreTile = new byte[0];
        /*
        * - parse MVT
        * - create proto schema file -> can be created afterwards or during conversion when the full tile is scanned
        *   - create from tilejson
        *   - collect it from the tile data since tilejson has no distinction between int and float
        * - specify fields which should be nested with prefix and delimiter -> in config file
        * - determine which encoding to use
        *   - Id
        *   - Geometry
        *       -> geometryType -> rle
        *       -> topology streams
        *           -> rle and delta varint/fastpfor
        *           -> used with heavyweight compression -> varint
        *           -> default -> FastPFOR
        *           -> also use rle?
        *       -> vertexBuffer -> FastPFOR or
        *       -> Tests
        *           -> use VertexDictionary -> use Morton or Coordinates
        *           -> use rle for topology streams
        *           -> use varint or fastpfor
        *   - Properties
        *       - String
        *           - Plain
        *           - Fsst
        *           - Dictionary
        *           - Fsst Dictionary
        * */
        var layerId = 0;
        for(var layer : mvt.layers()){
            /* Add layer header
            *  -> version -> u8
            *  -> externalSchema -> u8 -> start without and only allow a external schema and not self contained tiles?
            *  -> layerId -> u32 -> varint
            *  -> layerExtent -> u32 -> varint
            *  -> numFeatures -> u32 -> varint
            *  */
            var layerMetadata = tileMetadata.getLayers(layerId);
            var features = layer.features();

            var geometries = features.stream().map(f -> f.geometry()).collect(Collectors.toList());

            Triple<Integer, byte[], Integer> geometryColumn = null;
            byte[] encodedGeometryFieldMetadata;
            List<Long> ids;
            var sortedFeatures = new ArrayList<Feature>();
            if(layer.name().equals("building") || layer.name().equals("poi")){
                ids = features.stream().map(f -> f.id()).sorted().collect(Collectors.toList());
                geometryColumn = GeometryEncoder.encodeGeometryColumn(geometries, PhysicalLevelTechnique.FAST_PFOR,
                        layer.tileExtent(), ids);
                encodedGeometryFieldMetadata = EncodingUtils.encodeVarints(new long[]{geometryColumn.getLeft()}, false, false);
                for(var newId : ids){
                    var f = features.stream().filter(fe -> fe.id() == newId).findFirst().get();
                    sortedFeatures.add(f);
                }
            }
            else{
                //TODO: quick and dirty -> check if id is present
                ids = new ArrayList<>(features.stream().map(f -> f.id()).collect(Collectors.toList()));
                geometryColumn = GeometryEncoder.encodeGeometryColumn(geometries, PhysicalLevelTechnique.FAST_PFOR,
                        layer.tileExtent(), ids);
                encodedGeometryFieldMetadata = EncodingUtils.encodeVarints(new long[]{geometryColumn.getLeft()}, false, false);

                for(var newId : ids){
                    var f = features.stream().filter(fe -> fe.id() == newId).findFirst().get();
                    sortedFeatures.add(f);
                }
            }

            /*var featureScopedPropertyColumns = PropertyEncoder.encodeProperties(features, layerMetadata,
                    config.physicalLevelTechnique(), columnMappings, config.useSharedDictionaryEncoding());*/
            var featureScopedPropertyColumns = PropertyEncoder.encodeProperties(sortedFeatures, layerMetadata,
                    config.physicalLevelTechnique(), columnMappings, config.useSharedDictionaryEncoding());

            var encodedLayerInfo = EncodingUtils.encodeVarints(new long[]{layerId++, layer.tileExtent(), geometryColumn.getRight(),
                    features.size()}, false, false);
            mapLibreTile = CollectionUtils.concatByteArrays(mapLibreTile, new byte[]{VERSION}, encodedLayerInfo);
            if(config.includeIds() && !layer.name().equals("transportation")){
            //if(config.includeIds()){
                /* if ids are encoded sort the ids */
                //TODO: sort ids
                var idMetadata = layerMetadata.getFieldsList().stream().
                        filter(f -> f.getName().equals(ID_COLUMN_NAME)).findFirst().get();
                //var idColumn = enocdeIdColumn(features, config.physicalLevelTechnique(), idMetadata);
                var idColumn = enocdeIdColumnTest(ids, config.physicalLevelTechnique(), idMetadata);
                //TODO: check if nullable so that we can get rid of the present stream in som cases
                var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{2}, false, false);
                mapLibreTile = CollectionUtils.concatByteArrays(mapLibreTile, encodedFieldMetadata, idColumn);

                System.out.println(tileMetadata.getLayers(layerId-1).getName() + " Id: " + idColumn.length / 1000d + ", geometry: " + geometryColumn.getMiddle().length / 1000d
                        + ", properties: " + featureScopedPropertyColumns.length / 1000d);
            }
            mapLibreTile = CollectionUtils.concatByteArrays(mapLibreTile, encodedGeometryFieldMetadata, geometryColumn.getMiddle());
            mapLibreTile = ArrayUtils.addAll(mapLibreTile, featureScopedPropertyColumns);

        }

        return mapLibreTile;
    }

    private static byte[] encodeIdColumn(List<Feature> features, PhysicalLevelTechnique physicalLevelTechnique, FieldMetadata fieldMetadata) throws IOException {
        /* Id column has to be of type unsigned int or long to allow efficient encoding
         * and has to be the first column in a layer if present.
         *  */
        var ids = features.stream().map(f -> f.id()).collect(Collectors.toList());
        var idPresent = features.stream().map(f -> true).collect(Collectors.toList());
        var idPresentStream = BooleanEncoder.encodeBooleanStream(idPresent, StreamType.PRESENT);
        /*
         * Convert the long values to integer values if possible for the usage in the FastPFOR compression
         * which is currently limited to 32 bit encodings.
         * */
        if(physicalLevelTechnique == PhysicalLevelTechnique.FAST_PFOR && fieldMetadata.getDataType() == DataType.UINT_32){
            var intIds = CollectionUtils.toIntList(ids);
            byte[] idDataStream;
            if(intIds.isPresent()){
                idDataStream = IntegerEncoder.encodeIntStream(intIds.get(), PhysicalLevelTechnique.FAST_PFOR,
                        false, StreamType.DATA);
            }
            else{
                idDataStream = IntegerEncoder.encodeLongStream(ids, false, StreamType.DATA);
            }
            return ArrayUtils.addAll(idPresentStream, idDataStream);
        }
        else if(physicalLevelTechnique == PhysicalLevelTechnique.VARINT && fieldMetadata.getDataType() == DataType.UINT_32){
            var intIds = CollectionUtils.toIntList(ids);
            var idDataStream = IntegerEncoder.encodeIntStream(intIds.get(), PhysicalLevelTechnique.VARINT,
                    false, StreamType.DATA);
            return ArrayUtils.addAll(idPresentStream, idDataStream);
        }
        else{
            var idDataStream = IntegerEncoder.encodeLongStream(ids, false, StreamType.DATA);
            return ArrayUtils.addAll(idPresentStream, idDataStream);
        }
    }

    private static byte[] enocdeIdColumnTest(List<Long> ids, PhysicalLevelTechnique physicalLevelTechnique, FieldMetadata fieldMetadata) throws IOException {
        /* Id column has to be of type unsigned int or long to allow efficient encoding
         * and has to be the first column in a layer if present.
         *  */
        var idPresent = ids.stream().map(f -> true).collect(Collectors.toList());
        var idPresentStream = BooleanEncoder.encodeBooleanStream(idPresent, StreamType.PRESENT);
        /*
         * Convert the long values to integer values if possible for the usage in the FastPFOR compression
         * which is currently limited to 32 bit encodings.
         * */
        if(physicalLevelTechnique == PhysicalLevelTechnique.FAST_PFOR && fieldMetadata.getDataType() == DataType.UINT_32){
            var intIds = CollectionUtils.toIntList(ids);
            byte[] idDataStream;
            if(intIds.isPresent()){
                idDataStream = IntegerEncoder.encodeIntStream(intIds.get(), PhysicalLevelTechnique.FAST_PFOR,
                        false, StreamType.DATA);
            }
            else{
                idDataStream = IntegerEncoder.encodeLongStream(ids, false, StreamType.DATA);
            }
            return ArrayUtils.addAll(idPresentStream, idDataStream);
        }
        else if(physicalLevelTechnique == PhysicalLevelTechnique.VARINT && fieldMetadata.getDataType() == DataType.UINT_32){
            var intIds = CollectionUtils.toIntList(ids);
            var idDataStream = IntegerEncoder.encodeIntStream(intIds.get(), PhysicalLevelTechnique.VARINT,
                    false, StreamType.DATA);
            return ArrayUtils.addAll(idPresentStream, idDataStream);
        }
        else{
            var idDataStream = IntegerEncoder.encodeLongStream(ids, false, StreamType.DATA);
            return ArrayUtils.addAll(idPresentStream, idDataStream);
        }
    }

}
