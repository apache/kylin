package org.apache.kylin.storage.cube;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.serializer.DataTypeSerializer;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.IGTCodeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shaoshi on 3/23/15.
 * This implementation uses Dictionary to encode and decode the table; If a column doesn't have dictionary, will check
 * its data type to serialize/deserialize it;
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CubeCodeSystem implements IGTCodeSystem {
    private static final Logger logger = LoggerFactory.getLogger(CubeCodeSystem.class);

    // ============================================================================

    private GTInfo info;
    private Map<Integer, Dictionary> dictionaryMap = null; // column index ==> dictionary of column
    private Map<Integer, Integer> fixLenMap = null; // column index ==> fixed length of column
    private IFilterCodeSystem<ByteArray> filterCS;
    private DataTypeSerializer[] serializers;

    public CubeCodeSystem(Map<Integer, Dictionary> dictionaryMap) {
        this(dictionaryMap, Collections.<Integer, Integer>emptyMap());
    }
            
    public CubeCodeSystem(Map<Integer, Dictionary> dictionaryMap, Map<Integer, Integer> fixLenMap) {
        this.dictionaryMap = dictionaryMap;
        this.fixLenMap = fixLenMap;
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        serializers = new DataTypeSerializer[info.getColumnCount()];
        for (int i = 0; i < info.getColumnCount(); i++) {
            // dimension with dictionary
            if (dictionaryMap.get(i) != null) {
                serializers[i] = new DictionarySerializer(dictionaryMap.get(i));
            }
            // dimension of fixed length
            else if (fixLenMap.get(i) != null) {
                serializers[i] = new FixLenSerializer(fixLenMap.get(i));
            }
            // metrics
            else {
                serializers[i] = DataTypeSerializer.create(info.getColumnType(i));
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
                if (code == null)
                    BytesUtil.writeByteArray(null, 0, 0, buffer);
                else
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
    public int maxCodeLength(int col) {
        return serializers[col].maxLength();
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        encodeColumnValue(col, value, 0, buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {
        // this is a bit too complicated, but encoding only happens once at build time, so it is OK
        DataTypeSerializer serializer = serializers[col];
        try {
            if (serializer instanceof DictionarySerializer) {
                ((DictionarySerializer) serializer).serializeWithRounding(value, roundingFlag, buf);
            } else {
                serializer.serialize(value, buf);
            }
        } catch (ClassCastException ex) {
            // try convert string into a correct object type
            try {
                if (value instanceof String) {
                    Object converted = serializer.valueOf((String) value);
                    if ((converted instanceof String) == false) {
                        encodeColumnValue(col, converted, roundingFlag, buf);
                        return;
                    }
                }
            } catch (Throwable e) {
                logger.error("Fail to encode value '" + value + "'", e);
            }
            throw ex;
        }
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
       return serializers[col].deserialize(buf);
    }

    @Override
    public MeasureAggregator<?> newMetricsAggregator(String aggrFunction, int col) {
        return MeasureAggregator.create(aggrFunction, info.getColumnType(col).toString());
    }

    static class DictionarySerializer extends DataTypeSerializer {
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
        public int maxLength() {
            return dictionary.getSizeOfId();
        }

        @Override
        public Object valueOf(byte[] value) {
            throw new UnsupportedOperationException();
        }
    }

    static class FixLenSerializer extends DataTypeSerializer {

        private int fixLen;
        private byte[] buf;

        FixLenSerializer(int fixLen) {
            this.fixLen = fixLen;
            this.buf = new byte[fixLen];
        }

        @Override
        public void serialize(Object value, ByteBuffer out) {
            if (value == null) {
                Arrays.fill(buf, Dictionary.NULL);
                out.put(buf);
            } else {
                byte[] bytes = Bytes.toBytes(value.toString());
                if (bytes.length > fixLen) {
                    throw new IllegalStateException("Expect at most " + fixLen + " bytes, but got " + bytes.length + ", value string: " + value.toString());
                }
                out.put(bytes);
                for (int i = bytes.length; i < fixLen; i++) {
                    out.put(RowConstants.ROWKEY_PLACE_HOLDER_BYTE);
                }
            }
        }

        @Override
        public Object deserialize(ByteBuffer in) {
            in.get(buf);

            int tail = fixLen;
            while (tail > 0 && (buf[tail - 1] == RowConstants.ROWKEY_PLACE_HOLDER_BYTE || buf[tail - 1] == Dictionary.NULL)) {
                tail--;
            }
            
            if (tail == 0) {
                return buf[0] == Dictionary.NULL ? null : "";
            }
            
            return Bytes.toString(buf, 0, tail);
        }

        @Override
        public int peekLength(ByteBuffer in) {
            return fixLen;
        }

        @Override
        public int maxLength() {
            return fixLen;
        }

        @Override
        public Object valueOf(byte[] value) {
            try {
                return new String(value, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                // does not happen
                throw new RuntimeException(e);
            }
        }

    }

}
