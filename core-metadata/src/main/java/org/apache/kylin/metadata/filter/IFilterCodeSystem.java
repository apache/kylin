package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Decides how constant values are coded and compared.
 * 
 * TupleFilter are involved in both query engine and coprocessor. In query engine, the values are strings.
 * In coprocessor, the values are dictionary IDs.
 * 
 * The type parameter is the java type of code, which should be bytes. However some legacy implementation
 * stores code as String.
 * 
 * @author yangli9
 */
public interface IFilterCodeSystem<T> extends Comparator<T> {

    /** if given code represents the NULL value */
    boolean isNull(T code);

    /** compare two values by their codes */
    // int compare(T code1, T code2);

    /** write code to buffer */
    void serialize(T code, ByteBuffer buf);

    /** read code from buffer */
    T deserialize(ByteBuffer buf);

}
