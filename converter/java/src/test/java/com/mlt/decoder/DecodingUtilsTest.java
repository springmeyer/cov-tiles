package com.mlt.decoder;

import com.mlt.converter.encodings.EncodingUtils;
import me.lemire.integercompression.IntWrapper;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;

public class DecodingUtilsTest {

    @Test
    public void decodeLongVarint(){
        var value = (long)Math.pow(2, 54);
        var value2 = (long)Math.pow(2, 17);
        var value3 = (long)Math.pow(2, 57);
        var encodedValues = EncodingUtils.encodeVarints(new long[]{value, value2, value3}, false, false);

        var pos = new IntWrapper(0);
        var decodedValue = DecodingUtils.decodeLongVarint(encodedValues, pos, 3);

        Assert.equals(value, decodedValue[0]);
        Assert.equals(value2, decodedValue[1]);
        Assert.equals(value3, decodedValue[2]);

        Assert.equals(encodedValues.length, pos.get());
    }

    @Test
    public void decodeZigZag_LongValue(){
        var value = (long)Math.pow(2, 54);
        var value2 = (long)-Math.pow(2, 44);
        var encodedValue = EncodingUtils.encodeZigZag(value);
        var encodedValue2 = EncodingUtils.encodeZigZag(value2);

        var decodedValue = DecodingUtils.decodeZigZag(encodedValue);
        var decodedValue2 = DecodingUtils.decodeZigZag(encodedValue2);

        Assert.equals(value, decodedValue);
        Assert.equals(value2, decodedValue2);
    }

}
