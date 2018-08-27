package org.apache.kylin.measure.auc;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.util.KryoUtils;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class AucSerializer extends DataTypeSerializer<AucCounter> {


    // called by reflection
    public AucSerializer(DataType type) {
    }


    @Override
    public void serialize(AucCounter value, ByteBuffer out) {
        byte[] tserialize = KryoUtils.serialize(value.getTruth());
        byte[] pserialize = KryoUtils.serialize(value.getPred());
        out.putInt(4 + 4 + tserialize.length);
        out.putInt(tserialize.length);
        out.put(tserialize);
        out.putInt(4 + 4 + pserialize.length);
        out.putInt(pserialize.length);
        out.put(pserialize);

    }

    @Override
    public AucCounter deserialize(ByteBuffer in) {
        int totalTruthLength = in.getInt();
        int tarrayLength = in.getInt();
        byte[] tdata = new byte[tarrayLength];
        in.get(tdata);
        List<Integer> truth = KryoUtils.deserialize(tdata, LinkedList.class);

        int totalpredLength = in.getInt();
        int parrayLength = in.getInt();
        byte[] pdata = new byte[parrayLength];
        in.get(pdata);
        List<Double> pred = KryoUtils.deserialize(pdata, LinkedList.class);
        return new AucCounter(truth, pred);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int ret = in.getInt();
        in.position(mark);
        return ret;
    }

    @Override
    public int maxLength() {
        return 8 * 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 1024;
    }
}
