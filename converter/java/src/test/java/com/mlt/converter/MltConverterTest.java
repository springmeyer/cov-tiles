package com.mlt.converter;

import com.mlt.converter.encodings.EncodingUtils;
import com.mlt.converter.encodings.PhysicalLevelTechnique;
import com.mlt.converter.mvt.ColumnMapping;
import com.mlt.converter.mvt.MapboxVectorTile;
import com.mlt.converter.mvt.MvtUtils;
import com.mlt.decoder.MltDecoder;
import com.mlt.decoder.MltDecoderTest;
import com.mlt.metadata.MaplibreTileMetadata;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.CoordinateXY;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MltConverterTest { ;
    private static final String OMT_MVT_PATH = "..\\..\\test\\fixtures\\omt\\mvt";
    private static final String BING_MVT_PATH = "..\\..\\test\\fixtures\\bing\\mvt";

    @Test
    public void createTileMetadata_Omt_ValidMetadata() throws IOException {
        var expectedPropertiesScheme = Map.of(
                "water", Map.of("class", MaplibreTileMetadata.DataType.STRING),
                "waterway", Map.of("class", MaplibreTileMetadata.DataType.STRING),
                "landuse", Map.of("class", MaplibreTileMetadata.DataType.STRING),
                "transportation", Map.of("class", MaplibreTileMetadata.DataType.STRING, "brunnel", MaplibreTileMetadata.DataType.STRING),
                "water_name", Map.of("class", MaplibreTileMetadata.DataType.STRING, "intermittent", MaplibreTileMetadata.DataType.INT_64,
                    "name", MaplibreTileMetadata.DataType.STRUCT),
                "place", Map.of("class", MaplibreTileMetadata.DataType.STRING, "iso:a2", MaplibreTileMetadata.DataType.STRING,
                        "name", MaplibreTileMetadata.DataType.STRUCT)
        );
        var waternameNameProperties = List.of("default", "ar", "az", "be", "bg", "br", "bs", "ca", "co", "cs", "cy", "da", "de",
                "el", "en", "eo", "es", "et", "eu", "fi", "fr", "fy", "ga", "he", "hi", "hr", "hu", "hy", "id", "int", "is", "it",
                "ja", "ka", "kk", "ko", "ku", "la", "latin", "lt", "lv", "mk", "ml", "mt", "nl", "no", "pl", "pt", "ro", "ru",
                "sk", "sl", "sr", "sr-Latn", "sv", "ta", "th", "tr" , "uk", "zh");
        var placeNameProperties = Stream.concat(waternameNameProperties.stream(),
                List.of("am","gd","kn","lb","oc","rm","sq","te","nonlatin").stream()).collect(Collectors.toList());

        var tileId = String.format("%s_%s_%s", 5, 16, 21);
        var mvtFilePath = Paths.get(OMT_MVT_PATH, tileId + ".mvt" );
        var mvTile = MvtUtils.decodeMvt2(mvtFilePath);

        var mapping = new ColumnMapping("name", ":", true);
        var tileMetadata = MltConverter.createTileMetadata(mvTile, Optional.of(List.of(mapping)), true);

        assertEquals(mvTile.layers().size(), tileMetadata.getLayersCount());
        for(var i = 0; i < mvTile.layers().size(); i++){
            var mvtLayer = mvTile.layers().get(i);
            var mltLayerMetadata = tileMetadata.getLayers(i);

            assertEquals(mvtLayer.name(), mltLayerMetadata.getName());

            var idColumnMetadata = mltLayerMetadata.getFieldsList().stream().filter(f -> f.getName().equals("id")).findFirst().get();
            System.out.println(mvtLayer.name());
            var expectedIdDataType = mvtLayer.name().equals("place") ?
                    MaplibreTileMetadata.DataType.UINT_64 : MaplibreTileMetadata.DataType.UINT_32;
            assertEquals(expectedIdDataType, idColumnMetadata.getDataType());

            var geometryColumnMetadata = mltLayerMetadata.getFieldsList().stream().filter(f -> f.getName().equals("geometry")).findFirst().get();
            assertEquals(MaplibreTileMetadata.DataType.GEOMETRY, geometryColumnMetadata.getDataType());

            var expectedPropertySchema = expectedPropertiesScheme.get(mltLayerMetadata.getName());
            var mltProperties = mltLayerMetadata.getFieldsList();
            for(var expectedProperty : expectedPropertySchema.entrySet()){
                var mltProperty = mltProperties.stream().filter(p -> expectedProperty.getKey().equals(p.getName())).findFirst();
                assertTrue(mltProperty.isPresent());
                var actualDataType = mltProperty.get().getDataType();
                assertEquals(expectedProperty.getValue(), actualDataType);

                if(actualDataType.equals(MaplibreTileMetadata.DataType.STRUCT)){
                    var nestedFields = mltProperty.get().getChildrenList();
                    var expectedPropertyNames = mltLayerMetadata.getName().equals("place")?
                            placeNameProperties : waternameNameProperties;
                    assertEquals(expectedPropertyNames.size(), nestedFields.size());
                    for(var child : nestedFields){
                        /* In this test all nested name:* fields are of type string */
                        assertEquals(MaplibreTileMetadata.DataType.STRING, child.getDataType());
                        assertTrue(expectedPropertyNames.contains(child.getName()));
                    }
                }
            }
        }
    }

    /* Amazon Here schema based vector tiles tests  --------------------------------------------------------- */

    @Test
    public void convert_AmazonRandomZLevels_ValidMLtTile() throws IOException {
        var tiles = Stream.of(new File("..\\..\\test\\fixtures\\amazon_here\\mvt").listFiles())
                .filter(file -> !file.isDirectory())
                .map(File::getAbsoluteFile)
                .collect(Collectors.toSet());
        for(var tile : tiles){
            System.out.println("-------------------------------------------------------------------------------");
            runOmtTest2(tile.getAbsolutePath());
        }
    }

    /* OpenMapTiles schema based vector tiles tests 2  --------------------------------------------------------- */

    @Test
    public void convert_OmtRandomZLevels_ValidMLtTile() throws IOException {
        var tiles = Stream.of(new File("..\\..\\test\\fixtures\\omt2").listFiles())
                .filter(file -> !file.isDirectory())
                .map(File::getAbsoluteFile)
                .collect(Collectors.toSet());
        for(var tile : tiles){
            System.out.println("-------------------------------------------------------------------------------");
            runOmtTest2(tile.getAbsolutePath());
        }
    }

    private static void runOmtTest2(String tile) throws IOException {
        var mvtFilePath = Paths.get(tile);
        var mvTile = MvtUtils.decodeMvt(mvtFilePath);
        //var mvTile = MvtUtils.decodeMvt2(mvtFilePath);

        var columnMapping = new ColumnMapping("name", ":", true);
        var tileMetadata = MltConverter.createTileMetadata(mvTile, Optional.of(List.of(columnMapping)), true);
        var mlTile = MltConverter.convertMvt(mvTile,
                new ConversionConfig(true, PhysicalLevelTechnique.FAST_PFOR, true),
                tileMetadata, Optional.of(List.of(new ColumnMapping("name", ":", true))));

        var decodedMlTile = MltDecoder.decodeMlTile(mlTile, tileMetadata);
        MltDecoderTest.compareTiles(decodedMlTile, mvTile);

        var mvtSize = Files.readAllBytes(mvtFilePath).length;
        System.out.printf("MVT size: %s, MLT size: %s, reduction %s%% \n", mvtSize / 1024d, mlTile.length / 1024d,
                (1 - mlTile.length / (double) mvtSize) * 100);
    }


    /* OpenMapTiles schema based vector tiles tests --------------------------------------------------------- */

    @Test
    public void shared_delta_dictionary() throws IOException {
        var tileId = String.format("%s_%s_%s", 4, 8, 10);
        //var mvtFilePath = Paths.get(OMT_MVT_PATH, tileId + ".mvt" );
        var mvtFilePath = Paths.get(OMT_MVT_PATH, "5_a" + ".pbf" );
        //var mvTile = Files.readAllBytes(mvtFilePath);
        var decodedMvTile = MvtUtils.decodeMvt(mvtFilePath);
        //var tileId2 = String.format("%s_%s_%s", 5, 16, 20);
        //var mvtFilePath2 = Paths.get(OMT_MVT_PATH, tileId2 + ".mvt" );
        var mvtFilePath2 = Paths.get(OMT_MVT_PATH, "11_b" + ".pbf" );
        //var mvTile2 = Files.readAllBytes(mvtFilePath2);
        var decodedMvTile2 = MvtUtils.decodeMvt(mvtFilePath2);
        System.out.println("test");
    }

    @Test
    public void convert_OmtTileZ2_ValidMLtTile() throws IOException {
        //TODO: change vector tiles decoder -> polygons are not valid
        var tileId = String.format("%s_%s_%s", 2, 2, 2);
        runOmtTest(tileId);
    }

    @Test
    public void convert_OmtTilesZ3_ValidMltTile() throws IOException {
        runOmtTests(3, 4, 4 ,5, 5);
    }

    @Test
    public void convert_OmtTileZ4_ValidMltTile() throws IOException {
        var tileId = String.format("%s_%s_%s", 4, 8, 10);
        runOmtTest(tileId);

        var tileId2 = String.format("%s_%s_%s", 4, 3, 9);
        runOmtTest(tileId2);
    }

    @Test
    public void convert_OmtTileZ5_ValidMltTile() throws IOException {
        runOmtTests(5, 16, 17, 20, 21);
    }

    @Test
    public void convert_OmtTileZ6_ValidMltTile() throws IOException {
        runOmtTests(6, 32, 34, 41, 42);
    }

    @Test
    public void convert_OmtTilesZ7_ValidMltTile() throws IOException {
        runOmtTests(7, 66, 68, 83, 85);
    }

    @Test
    public void convert_OmtTilesZ8_ValidMltTile() throws IOException {
        runOmtTests(8, 132, 135, 170, 171);
    }

    @Test
    public void convert_OmtTilesZ9_ValidMltTile() throws IOException {
        runOmtTests(9, 264, 266, 340, 342);
    }

    @Test
    public void convert_OmtTilesZ10_ValidCovTile() throws IOException {
        runOmtTests(10, 530, 533, 682, 684);
    }

    @Test
    public void convert_OmtTilesZ11_ValidCovTile() throws IOException {
        runOmtTests(11, 1062, 1065, 1366, 1368);
    }

    @Test
    public void convert_OmtTilesZ12_ValidCovTile() throws IOException {
        runOmtTests(12, 2130, 2134, 2733, 2734);
    }

    @Test
    public void convert_OmtTilesZ13_ValidMltTile() throws IOException {
        runOmtTests(13, 4264, 4267, 5467, 5468);
    }

    @Test
    public void convert_OmtTileZ14_ValidMltTile() throws IOException {
        runOmtTests(14, 8296, 8300, 10748, 10749);
    }

    private static void runOmtTest(String tileId) throws IOException {
        var mvtFilePath = Paths.get(OMT_MVT_PATH, tileId + ".mvt" );
        var mvTile = MvtUtils.decodeMvt(mvtFilePath);
        //var mvTile = MvtUtils.decodeMvt2(mvtFilePath);

        var columnMapping = new ColumnMapping("name", ":", true);
        var tileMetadata = MltConverter.createTileMetadata(mvTile, Optional.of(List.of(columnMapping)), true);
        var mlTile = MltConverter.convertMvt(mvTile,
                new ConversionConfig(true, PhysicalLevelTechnique.FAST_PFOR, true),
                tileMetadata, Optional.of(List.of(new ColumnMapping("name", ":", true))));

        //var decodedMlTile = MltDecoder.decodeMlTile(mlTile, tileMetadata);
        //MltDecoderTest.compareTiles(decodedMlTile, mvTile);

        var mvtSize = Files.readAllBytes(mvtFilePath).length;
        System.out.printf("MVT size: %s, MLT size: %s, reduction %s%% \n", mvtSize / 1024d, mlTile.length / 1024d,
                (1 - mlTile.length / (double)mvtSize) * 100);
    }

    private static void runOmtTests(int zoom, int minX, int maxX, int minY, int maxY) throws IOException {
        var ratios = 0d;
        var counter = 0;
        for (var x = minX; x <= maxX; x++) {
            for (var y = minY; y <= maxY; y++) {
                var tileId = String.format("%s_%s_%s", zoom, x, y);
                var mvtFilePath = Paths.get(OMT_MVT_PATH, tileId + ".mvt" );
                var mvTile = Files.readAllBytes(mvtFilePath);
                var decodedMvTile = MvtUtils.decodeMvt(mvtFilePath);
                var decodedMvTile2 = MvtUtils.decodeMvt2(mvtFilePath);
                compareDecodedMVTiles(decodedMvTile, decodedMvTile2);

                try{
                    System.out.printf("z:%s, x:%s, y:%s -------------------------------------------- \n", zoom, x, y);
                    var columnMapping = new ColumnMapping("name", ":", true);
                    var tileMetadata = MltConverter.createTileMetadata(decodedMvTile, Optional.of(List.of(columnMapping)), true);
                    var mlTile = MltConverter.convertMvt(decodedMvTile,
                            new ConversionConfig(true, PhysicalLevelTechnique.FAST_PFOR, true),
                            tileMetadata, Optional.of(List.of(new ColumnMapping("name", ":", true))));

                    //var decodedMlTile = MltDecoder.decodeMlTile(mlTile, tileMetadata);
                    //MltDecoderTest.compareTiles(decodedMlTile, decodedMvTile);

                    ratios += printStats(mvTile, mlTile);
                    counter++;
                }
                catch(Exception e){
                    System.out.println(e);
                }
            }
        }

        System.out.println("Total ratio: " + (ratios / counter));
    }

    public static void compareDecodedMVTiles(MapboxVectorTile mvTile1, MapboxVectorTile mvTile2){
        var mvt1Layers = mvTile1.layers();
        var mvt2Layers = mvTile2.layers();
        for(var i = 0; i < mvt2Layers.size(); i++){
            var mvt2Layer = mvt2Layers.get(i);
            var layerName = mvt2Layer.name();
            var mvt1Layer = mvt1Layers.stream().filter(l -> l.name().equals(layerName)).findFirst().get();
            var mvt1Features = mvt1Layer.features();
            var mvt2Features = mvt2Layer.features();
            for(var j = 0; j < mvt2Features.size(); j++){
                var mvt1Feature = mvt1Features.get(j);
                var mvt2Feature = mvt2Features.get(j);

                var mvt1Id = mvt2Feature.id();
                var mvt2Id = mvt1Feature.id();
                assertEquals(mvt1Id, mvt2Id);

                var mvt1Geometry = mvt1Feature.geometry();
                var mvt2Geometry = mvt2Feature.geometry();
                try{
                    //assertEquals(mvt2Geometry, mvt1Geometry);
                    assertEquals(Arrays.stream(mvt2Geometry.getCoordinates()).collect(Collectors.toList()),
                            Arrays.stream(mvt1Geometry.getCoordinates()).map(c -> new CoordinateXY(c.getX(), c.getY())).collect(Collectors.toList()));
                }
                catch(Error e){
                    System.out.println(e);
                }


                var mvt1Properties = mvt1Feature.properties();
                var mvt2Properties = mvt2Feature.properties();
                for(var mvt2Property : mvt2Properties.entrySet()){
                    var mvt1Property = mvt1Properties.get(mvt2Property.getKey());
                    assertEquals(mvt2Property.getValue(), mvt1Property);
                }
            }
        }
    }

    private static double printStats(byte[] mvTile, byte[] mlTile) throws IOException {
        var mvtGzipBuffer = EncodingUtils.gzip(mvTile);
        var mltGzipBuffer = EncodingUtils.gzip(mlTile);

        System.out.printf("MVT size: %s, Gzip MVT size: %s%n", mvTile.length, mvtGzipBuffer.length);
        System.out.printf("MLT size: %s, Gzip MLT size: %s%n", mlTile.length, mltGzipBuffer.length);
        System.out.printf("Ratio uncompressed: %s, Ratio compressed: %s%n",
                ((double)mvTile.length)/mlTile.length, ((double)mvTile.length)/mlTile.length);

        var compressionRatio = (1-(1/(((double)mvTile.length)/mlTile.length)))*100;
        var compressionRatioCompressed = (1-(1/(((double)mvtGzipBuffer.length)/mltGzipBuffer.length)))*100;
        System.out.printf("Reduction uncompressed: %s%%, Reduction compressed: %s%% %n", compressionRatio, compressionRatioCompressed);
        return compressionRatio;
    }

    /* Bing Maps Tests --------------------------------------------------------- */

    @Test
    public void convert_BingMaps_Z4Tile() throws IOException {
        var fileNames = List.of("4-8-5", "4-9-5", "4-12-6", "4-13-6");
        runBingTests(fileNames);
    }

    @Test
    public void convert_BingMaps_Z5Tiles() throws IOException {
        var fileNames = List.of("5-16-11", "5-16-9", "5-17-11", "5-17-10", "5-15-10");
        runBingTests(fileNames);
    }

    @Test
    public void convert_BingMaps_Z6Tiles() throws IOException {
        var fileNames = List.of("6-32-22", "6-33-22", "6-32-23", "6-32-21");
        runBingTests(fileNames);
    }

    @Test
    public void convert_BingMaps_Z7Tiles() throws IOException {
        var fileNames = List.of("7-65-42", "7-66-42", "7-66-43", "7-66-44", "7-69-44");
        runBingTests(fileNames);
    }


    private void runBingTests(List<String> fileNames) throws IOException {
        var compressionRatios = 0d;
        for(var fileName : fileNames){
            compressionRatios += runBingTest(fileName);
        }

        System.out.printf("Total Compression Ratio Without Gzip: %s", compressionRatios / fileNames.size());
    }

    private double runBingTest(String tileId) throws IOException {
        System.out.println(tileId + " ------------------------------------------");
        var mvtFilePath = Paths.get(BING_MVT_PATH, tileId + ".mvt" );
        var mvTile = Files.readAllBytes(mvtFilePath);
        var decodedMvTile = MvtUtils.decodeMvt(Paths.get(BING_MVT_PATH, tileId + ".mvt"));

        var columnMapping = new ColumnMapping("name", ":", true);
        var tileMetadata = MltConverter.createTileMetadata(decodedMvTile, Optional.of(List.of(columnMapping)), true);
        var mlTile = MltConverter.convertMvt(decodedMvTile,
                new ConversionConfig(true, PhysicalLevelTechnique.FAST_PFOR, true),
                tileMetadata, Optional.of(List.of(new ColumnMapping("name", ":", true))));

        var decodedMlTile = MltDecoder.decodeMlTile(mlTile, tileMetadata);
        MltDecoderTest.compareTiles(decodedMlTile, decodedMvTile);

        var compressionRatio = printStats(mvTile, mlTile);

        return compressionRatio;
    }

}
