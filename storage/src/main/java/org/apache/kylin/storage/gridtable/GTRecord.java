package org.apache.kylin.storage.gridtable;

import java.nio.ByteBuffer;

public class GTRecord {

    final GTInfo info;
    final ByteBuffer[] cols;
    
    public GTRecord(GTInfo info) {
        this.info = info;
        this.cols = new ByteBuffer[info.nColumns];
    }

}
