package org.apache.kylin.aggregation.topn;

import java.nio.ByteBuffer;

import org.apache.kylin.common.datatype.DataType;
import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TopNCounterSerializerTest {

    private static TopNCounterSerializer serializer = new TopNCounterSerializer(DataType.getType("topn(10)"));

    @Test
    public void testSerialization() {
        TopNCounter<ByteArray> vs = new TopNCounter<ByteArray>(50);
        Integer[] stream = { 1, 1, 2, 9, 1, 2, 3, 7, 7, 1, 3, 1, 1 };
        for (Integer i : stream) {
            vs.offer(new ByteArray(Bytes.toBytes(i)));
        }

        ByteBuffer out = ByteBuffer.allocate(1024);
        serializer.serialize(vs, out);
        
        byte[] copyBytes = new byte[out.position()];
        System.arraycopy(out.array(), 0, copyBytes, 0, out.position());

        ByteBuffer in = ByteBuffer.wrap(copyBytes);
        TopNCounter<ByteArray> vsNew = serializer.deserialize(in);

        Assert.assertEquals(vs.toString(), vsNew.toString());

    }
    
    @Test
    public void testValueOf() {

        TopNCounter<ByteArray> origin = new TopNCounter<ByteArray>(10);
        ByteArray key = new ByteArray(1);
        ByteBuffer byteBuffer = key.asBuffer();
        BytesUtil.writeVLong(20l, byteBuffer);
        origin.offer(key, 1.0);

        byteBuffer = ByteBuffer.allocate(1024);
        byteBuffer.putInt(1);
        byteBuffer.putInt(20);
        byteBuffer.putDouble(1.0);
        TopNCounter<ByteArray> counter = serializer.valueOf(byteBuffer.array());


        Assert.assertEquals(origin.toString(), counter.toString());
    }

}
