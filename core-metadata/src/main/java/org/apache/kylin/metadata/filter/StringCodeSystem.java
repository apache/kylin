package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesUtil;

/**
 * A simple code system where all values are strings and conform to string comparison system.
 * 
 * @author yangli9
 */
public class StringCodeSystem implements IFilterCodeSystem<String> {
    
    public static final StringCodeSystem INSTANCE = new StringCodeSystem();
    
    protected StringCodeSystem() {
        // singleton
    }

    @Override
    public boolean isNull(String value) {
        return value == null;
    }

    @Override
    public int compare(String tupleValue, String constValue) {
        return tupleValue.compareTo(constValue);
    }

    @Override
    public void serialize(String value, ByteBuffer buffer) {
        BytesUtil.writeUTFString( value, buffer);
    }

    @Override
    public String deserialize(ByteBuffer buffer) {
        return BytesUtil.readUTFString(buffer);
    }

}
