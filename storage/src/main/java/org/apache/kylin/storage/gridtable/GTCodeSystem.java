package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.serializer.DataTypeSerializer;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class GTCodeSystem implements IGTCodeSystem {

    final private GTInfo info;
    final private DataTypeSerializer[] serializers;
    final private IFilterCodeSystem<ByteArray> filterCS;

    public GTCodeSystem(GTInfo info) {
        this.info = info;

        this.serializers = new DataTypeSerializer[info.nColumns];
        for (int i = 0; i < info.nColumns; i++) {
            this.serializers[i] = DataTypeSerializer.create(info.colTypes[i]);
        }

        this.filterCS = new IFilterCodeSystem<ByteArray>() {
            @Override
            public boolean isNull(ByteArray code) {
                return (code == null || code.length() == 0);
            }

            @Override
            public int compare(ByteArray code1, ByteArray code2) {
                return code1.compareTo(code2);
            }

            @Override
            public void serialize(ByteArray code, ByteBuffer buffer) {
                BytesUtil.writeByteArray(code.array(), code.offset(), code.length(), buffer);
            }

            @Override
            public ByteArray deserialize(ByteBuffer buffer) {
                return new ByteArray(BytesUtil.readByteArray(buffer));
            }
        };
    }

    @Override
    public int codeLength(int col, ByteBuffer buf) {
        return serializers[col].peekLength(buf);
    }

    @Override
    public IFilterCodeSystem<ByteArray> getFilterCodeSystem() {
        return filterCS;
    }

    // ============================================================================

    @Override
    public MeasureAggregator<?> newMetricsAggregator(int col) {
        return MeasureAggregator.create(info.colMetricsAggrFunc[col], info.colTypes[col].getName());
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        serializers[col].serialize(value, buf);
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        return serializers[col].deserialize(buf);
    }

}
