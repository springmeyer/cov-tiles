package com.mlt.converter.encodings;

import com.mlt.converter.CollectionUtils;
import com.mlt.converter.geometry.*;
import com.mlt.decoder.IntegerDecoder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.locationtech.jts.geom.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class GeometryEncoder {

    static class GeometryColumn{
        public byte[] geometryTypes;
        public byte[] numGeometriesStream;
        public byte[] numPartsStream;
        public byte[] numRingsStream;
        public byte[] dictionaryOffsets;
        public byte[] mortonEncodedDictionaryOffsets;
        /* Plain Encoding*/
        public byte[] vertexBuffer;
        /* Vertex Dictionary Encoding */
        public byte[] hilbertVertexBuffer;
        /* Morton Encoded Vertex Dictionary */
        public byte[] mortonVertexBuffer;
    }

    private GeometryEncoder(){}

    //TODO: add selection algorithms based on statistics and sampling
    public static Triple<Integer, byte[], Integer> encodeGeometryColumn(List<Geometry> geometries, PhysicalLevelTechnique physicalLevelTechnique,
                                                                        int tileExtent, List<Long> featureIds) {
        var geometryTypes = new ArrayList<Integer>();
        var numGeometries = new ArrayList<Integer>();
        List<Integer> numParts = new ArrayList<Integer>();
        var numRings = new ArrayList<Integer>();
        var vertexBuffer = new ArrayList<Vertex>();
        for(var geometry : geometries){
            var geometryType = geometry.getGeometryType();
            switch (geometryType){
                case Geometry.TYPENAME_POINT: {
                    geometryTypes.add(GeometryType.POINT.ordinal());
                    var point = (Point) geometry;
                    var x = (int) point.getX();
                    var y = (int) point.getY();
                    vertexBuffer.add(new Vertex(x,y));
                    break;
                }
                case Geometry.TYPENAME_LINESTRING: {
                    geometryTypes.add(GeometryType.LINESTRING.ordinal());
                    var lineString = (LineString) geometry;
                    numParts.add(lineString.getCoordinates().length);
                    var vertices = flatLineString(lineString);
                    vertexBuffer.addAll(vertices);
                    break;
                }
                case Geometry.TYPENAME_POLYGON: {
                    geometryTypes.add(GeometryType.POLYGON.ordinal());
                    var polygon = (Polygon) geometry;
                    var vertices = flatPolygon(polygon, numParts, numRings);
                    vertexBuffer.addAll(vertices);
                    break;
                }
                case Geometry.TYPENAME_MULTILINESTRING: {
                    geometryTypes.add(GeometryType.MULTILINESTRING.ordinal());
                    var multiLineString = (MultiLineString) geometry;
                    var numLineStrings = multiLineString.getNumGeometries();
                    numGeometries.add(numLineStrings);
                    for (var i = 0; i < numLineStrings; i++) {
                        var lineString = (LineString) multiLineString.getGeometryN(i);
                        numParts.add(lineString.getCoordinates().length);
                        vertexBuffer.addAll(flatLineString(lineString));
                    }
                    break;
                }
                case Geometry.TYPENAME_MULTIPOLYGON: {
                    geometryTypes.add( GeometryType.MULTIPOLYGON.ordinal());
                    var multiPolygon = (MultiPolygon)geometry;
                    var numPolygons = multiPolygon.getNumGeometries();
                    numGeometries.add(numPolygons);
                    for(var i = 0; i < numPolygons; i++){
                        var polygon = (Polygon)multiPolygon.getGeometryN(i);
                        var vertices = flatPolygon(polygon, numParts, numRings);
                        vertexBuffer.addAll(vertices);
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Specified geometry type is not (yet) supported.");
            }
        }

        //TODO: get rid of that separate calculation
         /*var maxTileExtent = Collections.max(vertexBuffer.stream().flatMapToInt(v -> IntStream.of(Math.abs(v.x()), Math.abs(v.y()))).boxed().
                collect(Collectors.toList()));*/
        var minVertexValue= Collections.min(vertexBuffer.stream().flatMapToInt(v -> IntStream.of(v.x(), v.y())).boxed().
                collect(Collectors.toList()));
        var maxVertexValue = Collections.max(vertexBuffer.stream().flatMapToInt(v -> IntStream.of(v.x(), v.y())).boxed().
                collect(Collectors.toList()));

        var hilbertCurve = new HilbertCurve(minVertexValue, maxVertexValue);
        var zOrderCurve = new ZOrderCurve(minVertexValue, maxVertexValue);
        //TODO: if the ratio is lower then 2 dictionary encoding has not to be considered?
        var vertexDictionary =  addVerticesToDictionary(vertexBuffer, hilbertCurve);
        var mortonEncodedDictionary = addVerticesToMortonDictionary(vertexBuffer, zOrderCurve);
        var dictionaryOffsets = getVertexOffsets(vertexBuffer, (id) -> vertexDictionary.headMap(id).size(), hilbertCurve);
        var mortonEncodedDictionaryOffsets = getVertexOffsets(vertexBuffer, (id) -> mortonEncodedDictionary.headSet(id).size(), zOrderCurve);

        /* Test if Plain, Vertex Dictionary or Morton Encoded Vertex Dictionary is the most efficient
        * -> Plain -> convert VertexBuffer with Delta Encoding and specified Physical Level Technique
        * -> Dictionary -> convert VertexOffsets with IntegerEncoder and VertexBuffer with Delta Encoding and specified Physical Level Technique
        * -> Morton Encoded Dictionary -> convert VertexOffsets with Integer Encoder and VertexBuffer with IntegerEncoder
        * */
        var zigZagDeltaVertexBuffer = zigZagDeltaEncodeVertices(vertexBuffer);
        var zigZagDeltaVertexDictionary = zigZagDeltaEncodeVertices(vertexDictionary.values());

        //TODO: get rid of that conversions
        //TODO: should we do a potential recursive encoding again
        var encodedVertexBuffer = IntegerEncoder.encodeInt(
                Arrays.stream(zigZagDeltaVertexBuffer).boxed().collect(Collectors.toList()), physicalLevelTechnique, false);
        //TODO: should we do a potential recursive encoding again
        var encodedVertexDictionary = IntegerEncoder.encodeInt(
                Arrays.stream(zigZagDeltaVertexDictionary).boxed().collect(Collectors.toList()), physicalLevelTechnique, false);
        //var encodedMortonVertexDictionary = IntegerEncoder.encodeInt(new ArrayList<>(mortonEncodedDictionary),
        //        physicalLevelTechnique, false);
        var encodedMortonVertexDictionary = IntegerEncoder.encodeMortonCodes(new ArrayList<>(mortonEncodedDictionary)).getRight();
        var encodedDictionaryOffsets = IntegerEncoder.encodeInt(dictionaryOffsets, physicalLevelTechnique, false);
        var encodedMortonEncodedDictionaryOffsets = IntegerEncoder.encodeInt(mortonEncodedDictionaryOffsets,
                physicalLevelTechnique, false);


        //Test -------------------------------------------------------
        var sortedDictionaryOffsets = new TreeMap<Integer, Triple<List<Long>, List<Integer>, List<Integer>>>();
        List<Integer> newOffsets = null;
        if(numParts.size() == featureIds.size() && featureIds.size() > 10000){
            var partOffsetCounter = 0;
            var idCounter = 0;
            /*if(numGeometries.size() == 0){
                numGeometries.add(1);
            }*/
            //for(var numGeometry: numGeometries){

                for(var numPart : numParts){
                    var currentLinePartOffsets = new ArrayList<Integer>();
                    for(var i = 0; i < numPart; i++){
                        var offset = mortonEncodedDictionaryOffsets.get(partOffsetCounter++);
                        currentLinePartOffsets.add(offset);
                    }

                    var featureId = featureIds.get(idCounter++);
                    var minLineOffset = currentLinePartOffsets.get(0);
                    if(sortedDictionaryOffsets.containsKey(minLineOffset)){
                        var existingLineOffsets = sortedDictionaryOffsets.get(minLineOffset).getMiddle();
                        var existingIds = sortedDictionaryOffsets.get(minLineOffset).getLeft();
                        existingIds.add(featureId);
                        var existingNumParts = sortedDictionaryOffsets.get(minLineOffset).getRight();
                        existingNumParts.add(numPart);
                        sortedDictionaryOffsets.put(minLineOffset, Triple.of(existingIds,
                                Stream.concat(existingLineOffsets.stream(), currentLinePartOffsets.stream()).collect(Collectors.toList()),
                                existingNumParts));
                    }
                    else{
                        var id = new ArrayList<Long>();
                        id.add(featureId);
                        var numPartsList = new ArrayList<Integer>();
                        numPartsList.add(numPart);
                        sortedDictionaryOffsets.put(currentLinePartOffsets.get(0), Triple.of(id, currentLinePartOffsets, numPartsList));
                    }

                }
            //}

            newOffsets = sortedDictionaryOffsets.values().stream().flatMap(e -> e.getMiddle().stream()).collect(Collectors.toList());
            var newFeatureIds = sortedDictionaryOffsets.values().stream().flatMap(e -> e.getLeft().stream()).collect(Collectors.toList());
            numParts = sortedDictionaryOffsets.values().stream().flatMap(e -> e.getRight().stream()).collect(Collectors.toList());
            while(featureIds.size() > 0){
                featureIds.remove(0);
            }
            for(var f : newFeatureIds){
                featureIds.add(f);
            }

            var optimizedMortonEncodedOffsets = IntegerEncoder.encodeInt(newOffsets, PhysicalLevelTechnique.FAST_PFOR, false);
            if(geometries.size() > 10000){
                System.out.println(encodedDictionaryOffsets.encodedValues.length + " " + optimizedMortonEncodedOffsets.encodedValues.length
                        + " " + numGeometries.size() + " ------------------------------------");
            }
        }
        // -----------------------------------------------------------


        // ----------------------------------------------------------





        //TODO: check if byte rle encoding produces better results -> normally not if the ORC RLE V1 approach is used
        var encodedGeometryColumn = IntegerEncoder.encodeIntStream(geometryTypes, physicalLevelTechnique, false, StreamType.GEOMETRY_TYPES);
        var numStreams = 1;
        if(numGeometries.size() > 0){
            var encodedNumGeometries = IntegerEncoder.encodeIntStream(numGeometries, physicalLevelTechnique, false, StreamType.NUM_GEOMETRIES);
            encodedGeometryColumn = ArrayUtils.addAll(encodedGeometryColumn, encodedNumGeometries);
            numStreams++;
        }
        if(numParts.size() > 0){
            var encodedNumParts = IntegerEncoder.encodeIntStream(numParts, physicalLevelTechnique, false, StreamType.NUM_PARTS);
            encodedGeometryColumn = ArrayUtils.addAll(encodedGeometryColumn, encodedNumParts);
            numStreams++;
        }
        if(numRings.size() > 0){
            var encodedNumRings = IntegerEncoder.encodeIntStream(numRings, physicalLevelTechnique, false, StreamType.NUM_RINGS);
            encodedGeometryColumn = ArrayUtils.addAll(encodedGeometryColumn, encodedNumRings);
            numStreams++;
        }

        /* Geometry column layout -> GeometryTypes, NumGeometries, NumParts, NumRings, VertexOffsets, VertexBuffer */
        /*System.out.printf("ratio: %s, plain: %s, dictionary: %s, mortonDictionary: %s \n", (double) vertexBuffer.size() / vertexDictionary.size(),
                encodedVertexBuffer.encodedValues.length,
                encodedDictionaryOffsets.encodedValues.length + encodedVertexDictionary.encodedValues.length,
                encodedMortonEncodedDictionaryOffsets.encodedValues.length + encodedMortonVertexDictionary.encodedValues.length);*/

        if(encodedVertexBuffer.encodedValues.length <= (encodedDictionaryOffsets.encodedValues.length + encodedVertexDictionary.encodedValues.length) &&
                encodedVertexBuffer.encodedValues.length <= (encodedMortonEncodedDictionaryOffsets.encodedValues.length + encodedMortonVertexDictionary.encodedValues.length)){
            //TODO: get rid of extra conversion
            /*var vertexBufferMetadata = new StreamMetadata(StreamType.VERTEX_BUFFER, encodedVertexBuffer.technique, physicalLevelTechnique,
                    vertexBuffer.size(), encodedVertexBuffer.encodedValues.length, encodedVertexBuffer.technique == IntegerEncoder.LogicalLevelIntegerTechnique.RLE
                    || encodedVertexBuffer.technique == IntegerEncoder.LogicalLevelIntegerTechnique.DELTA_RLE? Optional.of(encodedVertexBuffer.numRuns) : Optional.empty());*/
            var encodedVertexBufferStream = IntegerEncoder.encodeIntStream(
                    Arrays.stream(zigZagDeltaVertexBuffer).boxed().collect(Collectors.toList()), physicalLevelTechnique,
                    false, StreamType.VERTEX_BUFFER);
            numStreams++;
            return Triple.of(numStreams, ArrayUtils.addAll(encodedGeometryColumn, encodedVertexBufferStream), maxVertexValue);
        }
        else if((encodedDictionaryOffsets.encodedValues.length + encodedVertexDictionary.encodedValues.length) < encodedVertexBuffer.encodedValues.length &&
                (encodedDictionaryOffsets.encodedValues.length + encodedVertexDictionary.encodedValues.length) <=
                        (encodedMortonEncodedDictionaryOffsets.encodedValues.length + encodedMortonVertexDictionary.encodedValues.length)){
            var encodedVertexOffsetStream = IntegerEncoder.encodeIntStream(dictionaryOffsets, physicalLevelTechnique, false, StreamType.VERTEX_OFFSETS);
            var encodedVertexDictionaryStream = IntegerEncoder.encodeIntStream(
                    Arrays.stream(zigZagDeltaVertexDictionary).boxed().collect(Collectors.toList()), physicalLevelTechnique, false, StreamType.VERTEX_BUFFER);
            numStreams+=2;
            return Triple.of(numStreams, CollectionUtils.concatByteArrays(encodedGeometryColumn, encodedVertexOffsetStream, encodedVertexDictionaryStream), maxVertexValue);
        }
        else{
            //var encodedMortonVertexOffsetStream = IntegerEncoder.encodeIntStream(mortonEncodedDictionaryOffsets, physicalLevelTechnique, false, StreamType.VERTEX_OFFSETS);
            var encodedMortonVertexOffsetStream = IntegerEncoder.encodeIntStream(newOffsets != null? newOffsets : mortonEncodedDictionaryOffsets,
                    physicalLevelTechnique, false, StreamType.VERTEX_OFFSETS);
            var encodedMortonEncodedVertexDictionaryStream = IntegerEncoder.encodeMortonStream(new ArrayList<>(mortonEncodedDictionary),
                    zOrderCurve.numBits(), zOrderCurve.coordinateShift());
            numStreams+=2;

            System.out.println("Use Morton VertexDictionary encoding, reduction: " +
                    ((encodedDictionaryOffsets.encodedValues.length + encodedVertexDictionary.encodedValues.length) -
                    (encodedMortonEncodedDictionaryOffsets.encodedValues.length + encodedMortonVertexDictionary.encodedValues.length)) /1000d);
            System.out.println("Morton VertexDictionary encoding size: " + encodedMortonVertexOffsetStream.length /1000d);
            return Triple.of(numStreams, CollectionUtils.concatByteArrays(encodedGeometryColumn, encodedMortonVertexOffsetStream,
                    encodedMortonEncodedVertexDictionaryStream), maxVertexValue);
       }
    }

    private static int[] zigZagDeltaEncodeVertices(Collection<Vertex> vertices){
        Vertex previousVertex = new Vertex(0, 0);
        var deltaValues = new int[vertices.size() * 2];
        var j = 0;
        for(var vertex : vertices){
            var delta = vertex.x() - previousVertex.x();
            var zigZagDelta = EncodingUtils.encodeZigZag(delta);
            deltaValues[j++] = zigZagDelta;

            delta = vertex.y() - previousVertex.y();
            zigZagDelta = EncodingUtils.encodeZigZag(delta);
            deltaValues[j++] = zigZagDelta;

            previousVertex = vertex;
        }

        return deltaValues;
    }

    private static List<Integer> getVertexOffsets(List<Vertex> vertexBuffer, Function<Integer, Integer> vertexOffsetSupplier, SpaceFillingCurve curve){
        return vertexBuffer.stream().map(vertex -> {
            var sfcId = curve.encode(vertex);
            return vertexOffsetSupplier.apply(sfcId);
        }).collect(Collectors.toList());
    }

    private static TreeMap<Integer, Vertex> addVerticesToDictionary(List<Vertex> vertices, HilbertCurve hilbertCurve){
        var vertexDictionary = new TreeMap<Integer, Vertex>();
        for(var vertex : vertices){
            var hilbertId = hilbertCurve.encode(vertex);
            vertexDictionary.put(hilbertId, vertex);
        }
        return vertexDictionary;
    }

    private static TreeSet<Integer> addVerticesToMortonDictionary(List<Vertex> vertices, ZOrderCurve zOrderCurve){
        var mortonVertexDictionary = new TreeSet<Integer>();
        for(var vertex : vertices){
            var mortonCode = zOrderCurve.encode(vertex);
            mortonVertexDictionary.add(mortonCode);
        }
        return mortonVertexDictionary;
    }

    private static List<Vertex> flatLineString(LineString lineString){
        return Arrays.stream(lineString.getCoordinates()).map(v -> new Vertex((int)v.x, (int)v.y)).collect(Collectors.toList());
    }

    private static List<Vertex> flatPolygon(Polygon polygon, List<Integer> partSize, List<Integer> ringSize) {
        var vertexBuffer = new ArrayList<Vertex>();
        var numRings = polygon.getNumInteriorRing() + 1;
        partSize.add(numRings);

        var exteriorRing = polygon.getExteriorRing();
        var shell = new GeometryFactory().createLineString(Arrays.copyOf(exteriorRing.getCoordinates(),
                exteriorRing.getCoordinates().length - 1));
        var shellVertices = flatLineString(shell);
        vertexBuffer.addAll(shellVertices);
        ringSize.add(shell.getNumPoints());

        for (var i = 0; i < polygon.getNumInteriorRing(); i++) {
            var interiorRing = polygon.getInteriorRingN(i);
            var ring = new GeometryFactory().createLineString(Arrays.copyOf(interiorRing.getCoordinates(),
                    interiorRing.getCoordinates().length - 1));

            var ringVertices = flatLineString(ring);
            vertexBuffer.addAll(ringVertices);
            ringSize.add(ring.getNumPoints());
        }

        return vertexBuffer;
    }
}
