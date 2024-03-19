package com.mlt.converter;

import com.mlt.converter.encodings.PhysicalLevelTechnique;

public record ConversionConfig(boolean includeIds, PhysicalLevelTechnique physicalLevelTechnique,
                               boolean useSharedDictionaryEncoding) {
} //, List<String> nestedPropertiesNames){}
