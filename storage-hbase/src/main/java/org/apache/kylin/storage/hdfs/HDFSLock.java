package org.apache.kylin.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by xiefan on 17-1-17.
 */
public class HDFSLock {

    private Path rawLock;

    private static final Logger logger = LoggerFactory.getLogger(HDFSLock.class);

    protected HDFSLock(String resourceFullPath) {
        this.rawLock = new Path(resourceFullPath);
    }

    public boolean init(FileSystem fs) throws IOException, InterruptedException {
        if (!fs.isFile(rawLock)) {
            logger.info("Not support directory lock yet");
            return false;
        }
        while (!fs.createNewFile(rawLock)) {
            Thread.currentThread().sleep(1000);
        }
        return true;
    }

    public boolean release(FileSystem fs) throws IOException, InterruptedException {
        while (!fs.delete(rawLock, false)) {
            Thread.currentThread().sleep(1000);
        }
        return true;
    }
}
