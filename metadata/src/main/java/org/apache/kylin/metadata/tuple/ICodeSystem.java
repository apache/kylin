package org.apache.kylin.metadata.tuple;

import java.nio.ByteBuffer;

/**
 * Decides how constant values are coded and compared.
 * 
 * TupleFilter are involved in both query engine and coprocessor. In query engine, the values are strings.
 * In coprocessor, the values are dictionary IDs.
 * 
 * The type parameter is java type of code, which should be bytes. However some legacy implementation
 * stores code as String.
 * 
 * @author yangli9
 */
public interface ICodeSystem<T> {
    
    T encode(Object value);
    
    Object decode(T code);

    boolean isNull(T code);

    int compare(T code1, T code2);

    void serialize(T code, ByteBuffer buffer);

    T deserialize(ByteBuffer code);

}
