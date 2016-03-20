package org.apache.kylin.measure.raw;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class RawSerializerTest {
    static {
        DataType.register("raw");
    }

    private RawSerializer rawSerializer = new RawSerializer(DataType.getType("raw"));

    @Test
    public void testPeekLength() {
        ByteBuffer out = ByteBuffer.allocate(1024 * 1024 * 128);
        int size = 127;
        List<ByteArray> input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 1, rawSerializer.peekLength(out));

        size = 128;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 2, rawSerializer.peekLength(out));

        size = 255;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 2, rawSerializer.peekLength(out));

        size = 256;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 3, rawSerializer.peekLength(out));

        size = 1024 * 63;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 3, rawSerializer.peekLength(out));

        size = 1024 * 64;
        out.clear();
        input = getValueList(size);
        rawSerializer.serialize(input, out);
        out.rewind();
        assertEquals(size * 2 + 4, rawSerializer.peekLength(out));
    }

    @Test
    public void testNormal() {
        List<ByteArray> input = getValueList(1024);
        List<ByteArray> output = doSAndD(input);
        assertEquals(input, output);
    }

    @Test
    public void testNull() {
        List<ByteArray> output = doSAndD(null);
        assertEquals(output.size(), 0);
        List<ByteArray> input = new ArrayList<ByteArray>();
        output = doSAndD(input);
        assertEquals(input, output);
    }

    @Test(expected = RuntimeException.class)
    public void testOverflow() {
        List<ByteArray> input = getValueList(512 * 1024);
        doSAndD(input);
    }

    private List<ByteArray> doSAndD(List<ByteArray> input) {
        ByteBuffer out = ByteBuffer.allocate(rawSerializer.maxLength());
        out.mark();
        rawSerializer.serialize(input, out);
        out.reset();
        return rawSerializer.deserialize(out);
    }

    private List<ByteArray> getValueList(int size) {
        if (size == -1) {
            return null;
        }
        List<ByteArray> valueList = new ArrayList<ByteArray>(size);
        for (Integer i = 0; i < size; i++) {
            ByteArray key = new ByteArray(1);
            BytesUtil.writeUnsigned(i, key.array(), 0, key.length());
            valueList.add(key);
        }
        return valueList;
    }

}
