package org.apache.kylin.storage.hbase.coprocessor;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.filter.ICodeSystem;

public class DictCodeSystem implements ICodeSystem {

    public static final DictCodeSystem INSTANCE = new DictCodeSystem();
    
    private DictCodeSystem() {
        // singleton
    }

    @Override
    public boolean isNull(Object value) {
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
    public int compare(Object tupleValue, Object constValue) {
        return ((String) tupleValue).compareTo((String) constValue);
    }

    @Override
    public void serialize(Object value, ByteBuffer buffer) {
        BytesUtil.writeUTFString((String) value, buffer);
    }

    @Override
    public Object deserialize(ByteBuffer buffer) {
        return BytesUtil.readUTFString(buffer);
    }

}
