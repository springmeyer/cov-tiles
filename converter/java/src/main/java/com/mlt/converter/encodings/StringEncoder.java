package com.mlt.converter.encodings;

import com.fsst.FsstEncoder;
import com.mlt.converter.CollectionUtils;
import com.mlt.decoder.BooleanDecoder;
import com.mlt.decoder.DecodingUtils;
import com.mlt.decoder.IntegerDecoder;
import me.lemire.integercompression.IntWrapper;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StringEncoder {

    static class StringEncodingResult{
        public IntegerEncoder.LogicalLevelIntegerTechnique technique;
        public byte[] encodedValues;
        /* If rle or delta-rle encoding is used, otherwise can be ignored */
        public int numRuns;
    }

    public enum StringEncodingTechnique{
        PLAIN,
        DICTIONARY,
        FSST,
        FSST_DICTIONARY
    }

    private StringEncoder(){}

    /**
     *
     * @param values Values to convert. If the value is not present null has to be added to the list
     */
    public static Pair<Integer, byte[]> encodeSharedDictionary(List<List<String>> values, PhysicalLevelTechnique physicalLevelTechnique) throws IOException {
        /*
         * compare single column encoding with shared dictionary encoding
         * Shared dictionary layout -> length, dictionary, present1, data1, present2, data2
         * Shared Fsst dictionary layout ->  symbol table, symbol length, dictionary/compressed corpus, length, present1, data1, present2, data2
         *
         * */
        //TODO: also compare size with plain and single encoded columns
        var lengthStream = new ArrayList<Integer>();
        //TODO: also sort dictionary for the usage in combination with Gzip?
        var dictionary = new ArrayList<String>();
        var dataStreams = new ArrayList<List<Integer>>(values.size());
        var presentStreams = new ArrayList<List<Boolean>>();
        for(var column : values){
            var presentStream = new ArrayList<Boolean>();
            presentStreams.add(presentStream);
            var dataStream = new ArrayList<Integer>();
            dataStreams.add(dataStream);
            for(var value : column){
                if(value == null){
                    presentStream.add(false);
                }
                else{
                    presentStream.add(true);

                    if(!dictionary.contains(value)){
                        dictionary.add(value);
                        var utf8EncodedData = value.getBytes(StandardCharsets.UTF_8);
                        lengthStream.add(utf8EncodedData.length);
                        var index = dictionary.size() - 1;
                        dataStream.add(index);
                    }
                    else{
                        var index = dictionary.indexOf(value);
                        dataStream.add(index);
                    }
                }
            }
        }

        var encodedSharedDictionary = encodeDictionary(dictionary, physicalLevelTechnique, false);
        var encodedSharedFsstDictionary = encodeFsstDictionary(dictionary, physicalLevelTechnique, false);
        var sharedDictionary = encodedSharedFsstDictionary.length < encodedSharedDictionary.length?
                encodedSharedFsstDictionary : encodedSharedDictionary;
        if(encodedSharedFsstDictionary.length < encodedSharedDictionary.length){
            System.out.println("Use FsstDictionary, reduction: " + (encodedSharedDictionary.length - encodedSharedFsstDictionary.length ));
        }

        for(var i = 0; i < dataStreams.size(); i++){
            var presentStream = presentStreams.get(i);
            var dataStream = dataStreams.get(i);

            var encodedFieldMetadata = EncodingUtils.encodeVarints(new long[]{2}, false, false);
            var encodedPresentStream = BooleanEncoder.encodeBooleanStream(presentStream, StreamType.PRESENT);

            var encodedDataStream = IntegerEncoder.encodeIntStream(dataStream, physicalLevelTechnique, false, StreamType.DATA_REFERENCE);
            sharedDictionary = CollectionUtils.concatByteArrays(sharedDictionary, encodedFieldMetadata, encodedPresentStream, encodedDataStream);
        }

        //TODO: make present stream optional
        var numStreams = (encodedSharedFsstDictionary.length < encodedSharedDictionary.length? 5 : 3) + values.size() * 2;
        return Pair.of(numStreams, sharedDictionary);
    }

    public static Pair<Integer, byte[]> encode(List<String> values, PhysicalLevelTechnique physicalLevelTechnique) throws IOException {
        /*
        * convert a single string column -> check if plain, dictionary, fsst or fsstDictionary
        * -> plain -> length, data
        * -> dictionary -> length, dictionary, data
        * -> fsst -> symbol table, symbol length, compressed corpus, length
        * -> fsst dictionary -> symbol table, symbol length, dictionary/compressed corpus, length, data
        * Schema selection
        * -> based on statistics if dictionary encoding is used
        * -> compare four possible encodings in size based on samples
        * */
        //TODO: add plain encoding agian
        //var plainEncodedColumn = encodePlain(values, physicalLevelTechnique);
        var dictionaryEncodedColumn = encodeDictionary(values, physicalLevelTechnique, true);
        var fsstEncodedDictionary = encodeFsstDictionary(values, physicalLevelTechnique, true);

        if(dictionaryEncodedColumn.length <= fsstEncodedDictionary.length){
            return Pair.of(3, dictionaryEncodedColumn);
        }

        return Pair.of(5, fsstEncodedDictionary);
    }

    private static byte[] encodeFsstDictionary(List<String> values, PhysicalLevelTechnique physicalLevelTechnique, boolean encodeDataStream) {
        var dataStream = new ArrayList<Integer>(values.size());
        var lengthStream = new ArrayList<Integer>();
        var dictionary = new ArrayList<String>();
        for(var value : values){
            if(!dictionary.contains(value)){
                dictionary.add(value);
                var utf8EncodedData = value.getBytes(StandardCharsets.UTF_8);
                lengthStream.add(utf8EncodedData.length);
                var index = dictionary.size() - 1;
                dataStream.add(index);
            }
            else{
                var index = dictionary.indexOf(value);
                dataStream.add(index);
            }
        }

        var symbolTable = encodeFsst(dictionary, physicalLevelTechnique);

        if(encodeDataStream == false){
            return symbolTable;
        }

        var encodedDataStream = IntegerEncoder.encodeInt(dataStream, physicalLevelTechnique, false);
        var dataStreamLogicalLevelTechnique = LogicalLevelTechnique.fromIntegerLogicalLevelTechnique(encodedDataStream.technique);
        var dataStreamMetadata = new StreamMetadata(StreamType.DATA_REFERENCE, dataStreamLogicalLevelTechnique, physicalLevelTechnique,
                encodedDataStream.physicalLevelEncodedValuesLength, encodedDataStream.encodedValues.length, dataStreamLogicalLevelTechnique == LogicalLevelTechnique.RLE ||
                dataStreamLogicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE?
                Optional.of(encodedDataStream.numRuns): Optional.empty()).encode();
        return CollectionUtils.concatByteArrays(symbolTable, dataStreamMetadata, encodedDataStream.encodedValues);
    }

    private static byte[] encodeFsst(List<String> values, PhysicalLevelTechnique physicalLevelTechnique) {
        var joinedValues = String.join("", values).getBytes(StandardCharsets.UTF_8);
        var symbolTable = FsstEncoder.encode(joinedValues);

        var encodedSymbols = symbolTable.symbols();
        var symbolLengths = Arrays.stream(symbolTable.symbolLengths()).boxed().collect(Collectors.toList());
        var encodedSymbolLengths = IntegerEncoder.encodeInt(symbolLengths, physicalLevelTechnique, false);
        var compressedCorpus = symbolTable.compressedData();

        var symbolStreamMetadata = new StreamMetadata(StreamType.SYMBOL_TABLE, LogicalLevelTechnique.NONE, PhysicalLevelTechnique.NONE,
                //TODO: numValues in this context not needed -> set 0 to save space -> only 1 byte with varint?
                symbolLengths.size(), encodedSymbols.length, Optional.empty()).encode();
        var logicalLevelTechnique = LogicalLevelTechnique.fromIntegerLogicalLevelTechnique(encodedSymbolLengths.technique);
        var symbolLengthsStreamMetadata = new StreamMetadata(StreamType.SYMBOL_LENGTH, logicalLevelTechnique, physicalLevelTechnique,
                //TODO: numValues in this context not needed -> set 0 to save space -> only 1 byte with varint?
                encodedSymbolLengths.physicalLevelEncodedValuesLength, encodedSymbolLengths.encodedValues.length, logicalLevelTechnique == LogicalLevelTechnique.RLE ||
                logicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE? Optional.of(encodedSymbolLengths.numRuns): Optional.empty()
        ).encode();
        var compressedCorpusStreamMetadata = new StreamMetadata(StreamType.DICTIONARY, LogicalLevelTechnique.NONE, PhysicalLevelTechnique.NONE,
                //TODO: numValues in this context not needed -> set 0 to save space -> only 1 byte with varint?
                values.size(), compressedCorpus.length, Optional.empty()).encode();

        var lengthStream = values.stream().map(v -> v.getBytes(StandardCharsets.UTF_8).length).collect(Collectors.toList());
        var encodedLengthStream = IntegerEncoder.encodeInt(lengthStream, physicalLevelTechnique, false);
        logicalLevelTechnique = LogicalLevelTechnique.fromIntegerLogicalLevelTechnique(encodedLengthStream.technique);
        var encodedLengthMetadata = new StreamMetadata(StreamType.LENGTH, logicalLevelTechnique, physicalLevelTechnique,
                //TODO: numValues in this context not needed -> set 0 to save space -> only 1 byte with varint?
                encodedLengthStream.physicalLevelEncodedValuesLength, encodedLengthStream.encodedValues.length,
                logicalLevelTechnique == LogicalLevelTechnique.RLE || logicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE?
                        Optional.of(encodedLengthStream.numRuns): Optional.empty()).encode();

        //TODO: how to name the streams and how to order? -> symbol_table, length, data, length
        /* SymbolLength, SymbolTable, Value Length, Compressed Corpus */
        return CollectionUtils.concatByteArrays(symbolLengthsStreamMetadata, encodedSymbolLengths.encodedValues, symbolStreamMetadata, encodedSymbols,
                encodedLengthMetadata, encodedLengthStream.encodedValues, compressedCorpusStreamMetadata, compressedCorpus);
    }

    private static byte[] encodeDictionary(List<String> values, PhysicalLevelTechnique physicalLevelTechnique, boolean encodeDataStream){
        var dataStream = new ArrayList<Integer>(values.size());
        var lengthStream = new ArrayList<Integer>();
        var dictionary = new ArrayList<String>();
        var dictionaryStream = new byte[0];
        for(var value : values){
            if(!dictionary.contains(value)){
                dictionary.add(value);
                var utf8EncodedData = value.getBytes(StandardCharsets.UTF_8);
                lengthStream.add(utf8EncodedData.length);
                var index = dictionary.size() - 1;
                dataStream.add(index);

                dictionaryStream = CollectionUtils.concatByteArrays(dictionaryStream, utf8EncodedData);
            }
            else{
                var index = dictionary.indexOf(value);
                dataStream.add(index);
            }
        }

        var encodedLengthStream = IntegerEncoder.encodeInt(lengthStream, physicalLevelTechnique, false);
        var lengthStreamLogicalLevelTechnique = LogicalLevelTechnique.fromIntegerLogicalLevelTechnique(encodedLengthStream.technique);
        var lengthStreamMetadata = new StreamMetadata(StreamType.LENGTH, lengthStreamLogicalLevelTechnique, physicalLevelTechnique,
                encodedLengthStream.physicalLevelEncodedValuesLength, encodedLengthStream.encodedValues.length, lengthStreamLogicalLevelTechnique == LogicalLevelTechnique.RLE ||
                lengthStreamLogicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE? Optional.of(encodedLengthStream.numRuns):
                Optional.empty()).encode();
        var dictionaryStreamMetadata = new StreamMetadata(StreamType.DICTIONARY, LogicalLevelTechnique.NONE, PhysicalLevelTechnique.NONE,
                dictionary.size(), dictionaryStream.length, Optional.empty()).encode();

        if(!encodeDataStream){
            return  CollectionUtils.concatByteArrays(lengthStreamMetadata, encodedLengthStream.encodedValues,
                    dictionaryStreamMetadata, dictionaryStream);
        }

        var encodedDataStream = IntegerEncoder.encodeInt(dataStream, physicalLevelTechnique, false);
        var dataStreamLogicalLevelTechnique = LogicalLevelTechnique.fromIntegerLogicalLevelTechnique(encodedDataStream.technique);
        var dataStreamMetadata = new StreamMetadata(StreamType.DATA_REFERENCE, dataStreamLogicalLevelTechnique, physicalLevelTechnique,
                encodedDataStream.physicalLevelEncodedValuesLength, encodedDataStream.encodedValues.length, dataStreamLogicalLevelTechnique == LogicalLevelTechnique.RLE ||
                dataStreamLogicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE? Optional.of(encodedDataStream.numRuns):
                Optional.empty()).encode();
        /* Length, Dictionary, Data */
        return CollectionUtils.concatByteArrays(lengthStreamMetadata, encodedLengthStream.encodedValues,
                dictionaryStreamMetadata, dictionaryStream, dataStreamMetadata,encodedDataStream.encodedValues);
    }

    private static byte[] encodePlain(List<String> values, PhysicalLevelTechnique physicalLevelTechnique) throws IOException {
        var lengthStream = new ArrayList<Integer>(values.size());
        var dataStream = new byte[0];
        for(var value : values){
            var utf8EncodedValue = value.getBytes(StandardCharsets.UTF_8);
            dataStream = CollectionUtils.concatByteArrays(dataStream, utf8EncodedValue);
            lengthStream.add(utf8EncodedValue.length);
        }

        var encodedLengthStream = IntegerEncoder.encodeInt(lengthStream, physicalLevelTechnique, false);
        var logicalLevelTechnique = LogicalLevelTechnique.fromIntegerLogicalLevelTechnique(encodedLengthStream.technique);
        var lengthStreamMetadata = new StreamMetadata(StreamType.LENGTH, logicalLevelTechnique, physicalLevelTechnique,
                encodedLengthStream.physicalLevelEncodedValuesLength, encodedLengthStream.encodedValues.length, logicalLevelTechnique == LogicalLevelTechnique.RLE ||
                logicalLevelTechnique == LogicalLevelTechnique.DELTA_RLE? Optional.of(encodedLengthStream.numRuns):
                Optional.empty()).encode();
        var dataStreamMetadata = new StreamMetadata(StreamType.DATA, LogicalLevelTechnique.NONE, PhysicalLevelTechnique.NONE,
                values.size(), dataStream.length, Optional.empty()).encode();

        return CollectionUtils.concatByteArrays(lengthStreamMetadata, encodedLengthStream.encodedValues,
                dataStreamMetadata, dataStream);
    }
}
