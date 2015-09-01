package org.apache.kylin.metadata.measure.serializer;

import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.metadata.model.DataType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * 
 */
public class TopNCounterSerializerTest {

    private static TopNCounterSerializer serializer = new TopNCounterSerializer(DataType.getInstance("topn(10)"));

    @SuppressWarnings("unchecked")
    @Test
    public void testCounterSerialization() {
        TopNCounter<ByteArray> vs = new TopNCounter<ByteArray>(50);
        Integer[] stream = { 1, 1, 2, 9, 1, 2, 3, 7, 7, 1, 3, 1, 1 };
        for (Integer i : stream) {
            vs.offer(new ByteArray(Bytes.toBytes(i)));
        }

        ByteBuffer out = ByteBuffer.allocate(1024 * 1024);
        serializer.serialize(vs, out);
        
        byte[] copyBytes = new byte[out.position()];
        System.arraycopy(out.array(), 0, copyBytes, 0, out.position());

        ByteBuffer in = ByteBuffer.wrap(copyBytes);
        TopNCounter<ByteArray> vsNew = serializer.deserialize(in);

        Assert.assertEquals(vs.toString(), vsNew.toString());

    }

}
