package org.apache.kylin.storage.gridtable;

import com.google.common.collect.Maps;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.serializer.DataTypeSerializer;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Map;

/**
 * Created by shaoshi on 3/23/15.
 */
public class GTDictionaryCodeSystem implements IGTCodeSystem {
    private GTInfo info;
    private BitSet encodedColumns = null;
    private Map<Integer, Dictionary> dictionaryMaps = null; // key: column index; value: dictionary for this column;
    private Map<Integer, DataTypeSerializer> serializerMap = null; // column index; value: serializer for this column;
    private IFilterCodeSystem<ByteArray> filterCS;

    public GTDictionaryCodeSystem(Map<Integer, Dictionary> dictionaryMaps) {
        this.dictionaryMaps = dictionaryMaps;
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;
        encodedColumns = new BitSet();
        for (Integer index : dictionaryMaps.keySet()) {
            encodedColumns.set(index);
        }

        serializerMap = Maps.newHashMap();
        for (int i = 0; i < info.nColumns; i++) {
            if (!encodedColumns.get(i)) {
                serializerMap.put(i, DataTypeSerializer.create(info.colTypes[i]));
            }
        }

        this.filterCS = new IFilterCodeSystem<ByteArray>() {
            @Override
            public boolean isNull(ByteArray code) {
                // all 0xff is null
                byte[] array = code.array();
                for (int i = 0, j = code.offset(), n = code.length(); i < n; i++, j++) {
                    if (array[j] != Dictionary.NULL)
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
    public IFilterCodeSystem<ByteArray> getFilterCodeSystem() {
        return filterCS;
    }

    @Override
    public int codeLength(int col, ByteBuffer buf) {
        if (useDictionary(col))
            return dictionaryMaps.get(col).getSizeOfId();
        else
            return serializerMap.get(col).peekLength(buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        if (useDictionary(col)) {
            int id = dictionaryMaps.get(col).getIdFromValue(value);
            BytesUtil.writeUnsigned(id, dictionaryMaps.get(col).getSizeOfId(), buf);
        } else {
            serializerMap.get(col).serialize(value, buf);
        }
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {

    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        if (useDictionary(col)) {
            int id = BytesUtil.readUnsigned(buf, dictionaryMaps.get(col).getSizeOfId());
            return dictionaryMaps.get(col).getValueFromId(id);
        } else {
            return serializerMap.get(col).deserialize(buf);
        }
    }

    @Override
    public MeasureAggregator<?> newMetricsAggregator(String aggrFunction, int col) {
        return MeasureAggregator.create(aggrFunction, info.colTypes[col].toString());
    }

    private boolean useDictionary(int col) {
        return encodedColumns.get(col);
    }
}
