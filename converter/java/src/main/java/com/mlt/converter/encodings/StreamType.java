package com.mlt.converter.encodings;

public enum StreamType {
    PRESENT,
    DATA,
    /* or compressed -> for dictionaries like fsst */
    DATA_REFERENCE,
    LENGTH,
    DICTIONARY,
    SYMBOL_LENGTH,
    SYMBOL_TABLE,
    GEOMETRY_TYPES,
    NUM_GEOMETRIES,
    NUM_PARTS,
    NUM_RINGS,
    INDEX_BUFFER,
    VERTEX_OFFSETS,
    VERTEX_BUFFER,
    MORTON_VERTEX_BUFFER
}
