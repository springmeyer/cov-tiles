package com.mlt.decoder;

import com.mlt.converter.ConversionConfig;
import com.mlt.converter.MltConverter;
import com.mlt.converter.encodings.PhysicalLevelTechnique;
import com.mlt.converter.mvt.ColumnMapping;
import com.mlt.converter.mvt.MapboxVectorTile;
import com.mlt.converter.mvt.MvtUtils;
import com.mlt.data.MapLibreTile;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MltDecoderTest {
    private static final String OMT_MVT_PATH = "..\\..\\test\\fixtures\\omt\\mvt";
    private static byte VERSION = 1;
    private static String ID_COLUMN_NAME = "id";
    private static String GEOMETRY_COLUMN_NAME = "geometry";

    @Test
    public void decodeMlTile_Z2() throws IOException {
        var tileId = String.format("%s_%s_%s", 2, 2, 2);
        testTile(tileId);
    }

    @Test
    public void decodeMlTile_Z4() throws IOException {
        var tileId = String.format("%s_%s_%s", 4, 8, 10);
        testTile(tileId);

        var tileId2 = String.format("%s_%s_%s", 4, 3, 9);
        testTile(tileId2);
    }

    @Test
    public void decodeMlTile_Z5() throws IOException {
        //var tileId = String.format("%s_%s_%s", 5, 16, 21);
        var tileId = String.format("%s_%s_%s", 5, 16, 20);
        testTile(tileId);
    }

    @Test
    public void decodeMlTile_Z6() throws IOException {
        var tileId = String.format("%s_%s_%s", 6, 32, 41);
        testTile(tileId);
    }

    @Test
    public void decodeMlTile_Z14() throws IOException {
        var tileId = String.format("%s_%s_%s", 14, 8298, 10748);
        testTile(tileId);
    }

    private void testTile(String tileId) throws IOException {
        var mvtFilePath = Paths.get(OMT_MVT_PATH, tileId + ".mvt" );
        var mvTile = MvtUtils.decodeMvt(mvtFilePath);
        var mvTile2 = MvtUtils.decodeMvt2(mvtFilePath);

        var columnMapping = new ColumnMapping("name", ":", true);
        var tileMetadata = MltConverter.createTileMetadata(mvTile, Optional.of(List.of(columnMapping)), true);
        var mlTile = MltConverter.convertMvt(mvTile,
                new ConversionConfig(true, PhysicalLevelTechnique.FAST_PFOR, true),
                tileMetadata, Optional.of(List.of(new ColumnMapping("name", ":", true))));

        var decodedTile = MltDecoder.decodeMlTile(mlTile, tileMetadata);
        compareTiles(decodedTile, mvTile);
    }

    public static void compareTiles(MapLibreTile mlTile, MapboxVectorTile mvTile){
        var mltLayers = mlTile.layers();
        var mvtLayers = mvTile.layers();
        for(var i = 0; i < mvtLayers.size(); i++){
            var mltLayer = mltLayers.get(i);
            var mvtLayer = mvtLayers.get(i);
            var mltFeatures = mltLayer.features();
            var mvtFeatures = mvtLayer.features();
            for(var j = 0; j < mvtFeatures.size(); j++){
                //var mltFeature = mltFeatures.get(j);
                var mvtFeature = mvtFeatures.get(j);
                var mltFeature = mltFeatures.stream().filter(f -> f.id() == mvtFeature.id()).findFirst().get();
                var mvtId = mvtFeature.id();
                var mltId = mltFeature.id();

                //assertEquals(mvtId, mltId);

                /*try{
                    assertTrue(mltFeatures.stream().anyMatch(f -> f.geometry().equals(mvtFeature.geometry())));
                }
                catch(Error e){
                    System.out.println(e);
                }*/

                var mltGeometry = mltFeature.geometry();
                var mvtGeometry = mvtFeature.geometry();
                assertEquals(mvtGeometry, mltGeometry);

                var mltProperties = mltFeature.properties();
                var mvtProperties = mvtFeature.properties();
                for(var mvtProperty : mvtProperties.entrySet()){
                    //if(mvtProperty.getKey().contains("name:ja") || mvtProperty.getKey().contains("name:ja:rm")){
                    if(mvtProperty.getKey().contains("name:ja:rm")){
                        System.out.println(mvtProperty.getKey() + " " + mvtProperty.getValue() + " " + mltProperties.get(mvtProperty.getKey()) + " " + j + " " + i);
                        continue;
                    }

                    var mltProperty = mltProperties.get(mvtProperty.getKey());
                    assertEquals(mvtProperty.getValue(), mltProperty);
                }
            }
        }
    }

}
