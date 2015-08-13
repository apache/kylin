package org.apache.kylin.cube.inmemcubing;

import org.apache.kylin.gridtable.GridTable;

/**
 */
public class CuboidResult {

    public long cuboidId;
    public GridTable table;
    public int nRows;
    public long timeSpent;
    public int aggrCacheMB;

    public CuboidResult(long cuboidId, GridTable table, int nRows, long timeSpent, int aggrCacheMB) {
        this.cuboidId = cuboidId;
        this.table = table;
        this.nRows = nRows;
        this.timeSpent = timeSpent;
        this.aggrCacheMB = aggrCacheMB;
    }

}
