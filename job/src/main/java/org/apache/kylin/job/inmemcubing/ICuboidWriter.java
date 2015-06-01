package org.apache.kylin.job.inmemcubing;

import org.apache.kylin.storage.gridtable.GTRecord;

import java.io.IOException;

/**
 */
public interface ICuboidWriter {
    void write(long cuboidId, GTRecord record) throws IOException;
}
