package org.apache.kylin.job.hadoop.cubev2;

import org.apache.kylin.storage.gridtable.GTRecord;

import java.io.IOException;

/**
 */
public interface IGTRecordWriter {
    void write(Long cuboidId, GTRecord record) throws IOException;
}
