package org.apache.kylin.job.hadoop.cubev2;

import org.apache.kylin.storage.gridtable.GTRecord;

import java.io.IOException;

/**
 * Created by shaoshi on 4/7/15.
 */
public interface IGTRecordWriter {
    void write(Long cuboidId, GTRecord record) throws IOException;
}
