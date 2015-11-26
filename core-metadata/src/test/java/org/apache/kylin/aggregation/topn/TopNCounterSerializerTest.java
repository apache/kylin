package org.apache.kylin.aggregation.topn;

import java.nio.ByteBuffer;

import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.topn.TopNCounterSerializer;
import org.apache.kylin.metadata.datatype.DataType;
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
        // FIXME need a good unit test for valueOf()
    }

}
