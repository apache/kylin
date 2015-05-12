package org.apache.kylin.storage.gridtable;

import org.apache.kylin.common.util.ByteArray;

import java.util.BitSet;

/**
 * Created by qianzhou on 5/6/15.
 */
public final class ScanKey {

    private ScanKey() {
    }

    static ByteArray makeScanKey(GTInfo info, GTRecord rec) {
        int firstPKCol = info.primaryKey.nextSetBit(0);
        if (rec == null || rec.cols[firstPKCol].array() == null)
            return null;

        BitSet selectedColumns = new BitSet();
        int len = 0;
        for (int i = info.primaryKey.nextSetBit(0); i >= 0; i = info.primaryKey.nextSetBit(i + 1)) {
            if (rec.cols[i].array() == null) {
                break;
            }
            selectedColumns.set(i);
            len += rec.cols[i].length();
        }

        ByteArray buf = ByteArray.allocate(len);
        rec.exportColumns(selectedColumns, buf);
        return buf;
    }
}
