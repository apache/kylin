package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.tuple.ICodeSystem;

/**
 * A simple code system where all values are strings and conform to string comparison system.
 * 
 * @author yangli9
 */
public class StringCodeSystem implements ICodeSystem<String> {
    
    public static final StringCodeSystem INSTANCE = new StringCodeSystem();
    
    private StringCodeSystem() {
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
