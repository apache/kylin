package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.measure.serializer.DataTypeSerializer;

@SuppressWarnings({ "rawtypes", "unchecked" })
/**
 * This is just for example and is INCORRECT when numbers are encoded to bytes and compared in filter.
 * 
 * A correct implementation must ensure dimension values preserve order after encoded, e.g. by using an
 * order preserving dictionary.
 * 
 * @author yangli9
 */
public class GTSampleCodeSystem implements IGTCodeSystem {

    private GTInfo info;
    private DataTypeSerializer[] serializers;
    private IGTComparator comparator;

    public GTSampleCodeSystem() {
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        this.serializers = new DataTypeSerializer[info.getColumnCount()];
        for (int i = 0; i < info.getColumnCount(); i++) {
            this.serializers[i] = DataTypeSerializer.create(info.colTypes[i]);
        }

        this.comparator = new DefaultGTComparator();
    }

    @Override
    public IGTCodeSystem trim() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int codeLength(int col, ByteBuffer buf) {
        return serializers[col].peekLength(buf);
    }

    @Override
    public int maxCodeLength(int col) {
        return serializers[col].maxLength();
    }

    @Override
    public IGTComparator getComparator() {
        return comparator;
    }

    // ============================================================================

    @Override
    public MeasureAggregator<?>[] newMetricsAggregators(ImmutableBitSet columns, String[] aggrFunctions) {
        assert columns.trueBitCount() == aggrFunctions.length;

        MeasureAggregator<?>[] result = new MeasureAggregator[aggrFunctions.length];
        for (int i = 0; i < result.length; i++) {
            int col = columns.trueBitAt(i);
            result[i] = MeasureAggregator.create(aggrFunctions[i], info.getColumnType(col).toString());
        }
        return result;
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        serializers[col].serialize(value, buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {
        // ignore rounding
        encodeColumnValue(col, value, buf);
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        return serializers[col].deserialize(buf);
    }

}
