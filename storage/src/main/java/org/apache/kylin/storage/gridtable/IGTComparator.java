package org.apache.kylin.storage.gridtable;

import java.util.Comparator;

import org.apache.kylin.common.util.ByteArray;

public interface IGTComparator extends Comparator<ByteArray> {

    /** if given code represents the NULL value */
    boolean isNull(ByteArray code);

    /** compare two values by their codes */
    // int compare(T code1, T code2);

}
