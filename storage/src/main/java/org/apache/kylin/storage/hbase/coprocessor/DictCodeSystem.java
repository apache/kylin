package org.apache.kylin.storage.hbase.coprocessor;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.tuple.ICodeSystem;

public class DictCodeSystem implements ICodeSystem<String> {

    public static final DictCodeSystem INSTANCE = new DictCodeSystem();
    
    private DictCodeSystem() {
        // singleton
    }

    @Override
    public String encode(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object decode(String code) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(String value) {
        if (value == null)
            return true;
        
        String v = (String) value;
        for (int i = 0, n = v.length(); i < n; i++) {
            if ((byte) v.charAt(i) != Dictionary.NULL)
                return false;
        }
        return true;
    }

    @Override
    public int compare(String tupleValue, String constValue) {
        return ((String) tupleValue).compareTo((String) constValue);
    }

    @Override
    public void serialize(String value, ByteBuffer buffer) {
        BytesUtil.writeUTFString((String) value, buffer);
    }

    @Override
    public String deserialize(ByteBuffer buffer) {
        return BytesUtil.readUTFString(buffer);
    }

}
