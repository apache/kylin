package org.apache.kylin.storage.gridtable;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.measure.MeasureAggregator;

import java.nio.ByteBuffer;

/**
 * Created by shaoshi on 3/23/15.
 */
public class GTDictionaryCodeSystem implements IGTCodeSystem {
    private GTInfo info;

    private Dictionary[] dictionaries;

    @Override
    public void init(GTInfo info) {
        this.info = info;
    }

    @Override
    public IFilterCodeSystem<ByteArray> getFilterCodeSystem() {
        return null;
    }

    @Override
    public int codeLength(int col, ByteBuffer buf) {
        return dictionaries[col].getSize();
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        int id = dictionaries[col].getIdFromValue(value);
        BytesUtil.writeVInt(id, buf);
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        int id = BytesUtil.readVInt(buf);
        return dictionaries[col].getValueFromId(id);
    }

    @Override
    public MeasureAggregator<?> newMetricsAggregator(String aggrFunction, int col) {
        return null;
    }
}
