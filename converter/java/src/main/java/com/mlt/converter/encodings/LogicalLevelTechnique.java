package com.mlt.converter.encodings;

public enum LogicalLevelTechnique {
    NONE,
    DELTA,
    RLE,
    DELTA_RLE,
    BOOLEAN_RLE;

    public static LogicalLevelTechnique fromIntegerLogicalLevelTechnique(IntegerEncoder.LogicalLevelIntegerTechnique technique){
        switch (technique){
            case DELTA:
                return LogicalLevelTechnique.DELTA;
            case RLE:
                return LogicalLevelTechnique.RLE;
            case DELTA_RLE:
                return LogicalLevelTechnique.DELTA_RLE;
        }

        return LogicalLevelTechnique.NONE;
    }
}
