package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;

/**
 * Decides how constant values in filter are coded and compared.
 * 
 * TupleFilter are involved in both query engine and coprocessor. In query engine, the values are strings.
 * In coprocessor, the values are dictionary IDs.
 * 
 * @author yangli9
 */
public interface ICodeSystem {

    boolean isNull(Object value);

    int compare(Object tupleValue, Object constValue);

    void serialize(Object value, ByteBuffer buffer);

    Object deserialize(ByteBuffer buffer);

}
