package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.serializer.DataTypeSerializer;

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
    private IFilterCodeSystem<ByteArray> filterCS;

    public GTSampleCodeSystem() {
    }
    
    @Override
    public void init(GTInfo info) {
        this.info = info;

        this.serializers = new DataTypeSerializer[info.nColumns];
        for (int i = 0; i < info.nColumns; i++) {
            this.serializers[i] = DataTypeSerializer.create(info.colTypes[i]);
        }

        this.filterCS = new IFilterCodeSystem<ByteArray>() {
            @Override
            public boolean isNull(ByteArray code) {
                // all 0xff is null
                byte[] array = code.array();
                for (int i = 0, j = code.offset(), n = code.length(); i < n; i++, j++) {
                    if (array[j] != (byte) 0xff)
                        return false;
                }
                return true;
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
    public MeasureAggregator<?> newMetricsAggregator(String aggrFunction, int col) {
        return MeasureAggregator.create(aggrFunction, info.colTypes[col].toString());
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
