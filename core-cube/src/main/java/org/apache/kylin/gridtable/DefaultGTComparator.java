package org.apache.kylin.gridtable;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dict.Dictionary;

public class DefaultGTComparator implements IGTComparator {
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
}
