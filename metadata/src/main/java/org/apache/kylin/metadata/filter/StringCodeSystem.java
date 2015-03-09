package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesUtil;

/**
 * A simple code system where all values are strings and conform to string comparison system.
 * 
 * @author yangli9
 */
public class StringCodeSystem implements ICodeSystem {
    
    public static final StringCodeSystem INSTANCE = new StringCodeSystem();
    
    private StringCodeSystem() {
        // singleton
    }

    @Override
    public boolean isNull(Object value) {
        return value == null;
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
