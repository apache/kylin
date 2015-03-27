package org.apache.kylin.storage.gridtable;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.serializer.DataTypeSerializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by shaoshi on 3/23/15.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class GTDictionaryCodeSystem implements IGTCodeSystem {
    private GTInfo info;
    private Map<Integer, Dictionary> dictionaryMaps = null; // key: column index; value: dictionary for this column;
    private IFilterCodeSystem<ByteArray> filterCS;
    private DataTypeSerializer[] serializers;

    public GTDictionaryCodeSystem(Map<Integer, Dictionary> dictionaryMaps) {
        this.dictionaryMaps = dictionaryMaps;
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        serializers = new DataTypeSerializer[info.nColumns];
        for (int i = 0; i < info.nColumns; i++) {
            if (dictionaryMaps.get(i) != null) {
                serializers[i] = new DictionarySerializer(dictionaryMaps.get(i));
            } else {
                serializers[i] = DataTypeSerializer.create(info.colTypes[i]);
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
        return serializers[col].peekLength(buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        serializers[col].serialize(value, buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {
        DataTypeSerializer serializer = serializers[col];
        if (serializer instanceof DictionarySerializer) {
            ((DictionarySerializer) serializer).serializeWithRounding(value,  roundingFlag, buf);
        } else {
            serializer.serialize(value,  buf);
        }
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        return serializers[col].deserialize(buf);
    }

    @Override
    public MeasureAggregator<?> newMetricsAggregator(String aggrFunction, int col) {
        return MeasureAggregator.create(aggrFunction, info.colTypes[col].toString());
    }

    class DictionarySerializer extends DataTypeSerializer {
        private Dictionary dictionary;

        DictionarySerializer(Dictionary dictionary) {
            this.dictionary = dictionary;
        }

        public void serializeWithRounding(Object value, int roundingFlag, ByteBuffer buf) {
            int id = dictionary.getIdFromValue(value, roundingFlag);
            BytesUtil.writeUnsigned(id, dictionary.getSizeOfId(), buf);
        }

        @Override
        public void serialize(Object value, ByteBuffer buf) {
            int id = dictionary.getIdFromValue(value);
            BytesUtil.writeUnsigned(id, dictionary.getSizeOfId(), buf);
        }

        @Override
        public Object deserialize(ByteBuffer in) {
            int id = BytesUtil.readUnsigned(in, dictionary.getSizeOfId());
            return dictionary.getValueFromId(id);
        }

        @Override
        public int peekLength(ByteBuffer in) {
            return dictionary.getSizeOfId();
        }

        @Override
        public Object valueOf(byte[] value) {
            throw new UnsupportedOperationException();
        }
    }
}
