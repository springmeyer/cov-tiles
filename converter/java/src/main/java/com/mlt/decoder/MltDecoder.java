package com.mlt.decoder;

import com.mlt.converter.encodings.PhysicalLevelTechnique;
import com.mlt.converter.encodings.StreamMetadata;
import com.mlt.data.Feature;
import com.mlt.data.Layer;
import com.mlt.data.MapLibreTile;
import com.mlt.metadata.MaplibreTileMetadata;
import me.lemire.integercompression.IntWrapper;
import org.locationtech.jts.geom.Geometry;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MltDecoder {
    private MltDecoder(){}

    public static MapLibreTile decodeMlTile(byte[] tile, MaplibreTileMetadata.TileMetadata tileMetadata) throws IOException {
        var offset = new IntWrapper(0);
        var mltLayers = new ArrayList<Layer>();
        while(offset.get() < tile.length){
            List<Long> ids = null;
            Geometry[] geometries = null;
            var properties = new HashMap<String, List<Object>>();

            var layerVersion = tile[offset.get()];
            offset.increment();
            var layerInfos = DecodingUtils.decodeVarint(tile, offset, 4);
            var layerId = layerInfos[0];
            var tileExtent = layerInfos[1];
            var maxTileExtent = layerInfos[2];
            var numFeatures = layerInfos[3];

            var metadata = tileMetadata.getLayers(layerId);
            for(var columnMetadata : metadata.getFieldsList()){
                var columnName = columnMetadata.getName();
                var numStreams = DecodingUtils.decodeVarint(tile, offset, 1)[0];
                //TODO: compare based on ids
                if(columnName.equals("id")){
                    if(numStreams == 2){
                        var presentStreamMetadata = StreamMetadata.decode(tile, offset);
                        var presentStream = DecodingUtils.decodeBooleanRle(tile, presentStreamMetadata.numValues(), presentStreamMetadata.byteLength(), offset);
                    }

                    var idDataStreamMetadata = StreamMetadata.decode(tile, offset);
                    ids = idDataStreamMetadata.physicalLevelTechnique() == PhysicalLevelTechnique.FAST_PFOR?
                            IntegerDecoder.decodeIntStream(tile, offset, idDataStreamMetadata, false).
                                    stream().mapToLong(i -> i).boxed().collect(Collectors.toList()):
                            IntegerDecoder.decodeLongStream(tile, offset, idDataStreamMetadata, false);
                }
                else if(columnName.equals("geometry")){
                    System.out.println(layerId);
                    var geometryColumn = GeometryDecoder.decodeGeometryColumn(tile, numStreams, offset);
                    geometries = GeometryDecoder.decodeGeometry(geometryColumn);
                }
                else{
                        var propertyColumn = PropertyDecoder.decodePropertyColumn(tile, offset, columnMetadata, numStreams);
                        if(propertyColumn instanceof HashMap<?,?>){
                            var p = ((Map<String, Object>)propertyColumn);
                            for(var a : p.entrySet()){
                                properties.put(a.getKey(), (List<Object>)a.getValue());
                            }
                        }
                        else{
                            properties.put(columnName, (ArrayList)propertyColumn);
                        }
                }
            }

            var features = new ArrayList<Feature>(numFeatures);
            for(var j = 0; j < numFeatures; j++){
                var p = new HashMap<String, Object>();
                for(var propertyColumn : properties.entrySet()){
                    if(propertyColumn.getValue() == null){
                        p.put(propertyColumn.getKey(), null);
                    }
                    else{
                        var v = propertyColumn.getValue().get(j);
                        p.put(propertyColumn.getKey(), v);
                    }
                }
                var feature = new Feature(ids.get(j), geometries[j], p);
                features.add(feature);
            }
            var layer = new Layer(metadata.getName(), features);
            mltLayers.add(layer);
        }

        return new MapLibreTile(mltLayers);
    }

}
