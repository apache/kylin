package com.kylinolap.job.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;

/**
 * Created by honma on 8/20/14.
 */
public class HdfsOpsTest extends LocalFileMetadataTestCase {

    FileSystem fileSystem;

    @Before
    public void setup() throws Exception {

        this.createTestMetadata();

        Configuration hconf = new Configuration();

        fileSystem = FileSystem.get(hconf);
    }

    @Test
    public void TestPath() throws IOException {
        String hdfsWorkingDirectory = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        Path coprocessorDir = new Path(hdfsWorkingDirectory, "test");
        fileSystem.mkdirs(coprocessorDir);

        Path newFile = new Path(coprocessorDir, "test_file");
        newFile = newFile.makeQualified(fileSystem.getUri(), null);
        FSDataOutputStream stream = fileSystem.create(newFile);
        stream.write(new byte[] { 0, 1, 2 });
        stream.close();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }
}
