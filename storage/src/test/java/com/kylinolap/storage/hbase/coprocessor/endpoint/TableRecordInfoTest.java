package com.kylinolap.storage.hbase.coprocessor.endpoint;

import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.invertedindex.TableRecordInfo;
import com.kylinolap.cube.invertedindex.TableRecordInfoDigest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Hongbin Ma(Binmahone) on 12/3/14.
 */
public class TableRecordInfoTest extends LocalFileMetadataTestCase {
    CubeInstance cube;
    TableRecordInfo tableRecordInfo;


    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
        this.cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_ii");
        this.tableRecordInfo = new TableRecordInfo(cube.getFirstSegment());
    }

    @Test
    public void testSerialize() {
        byte[] x = TableRecordInfoDigest.serialize(this.tableRecordInfo);
        TableRecordInfoDigest d = TableRecordInfoDigest.deserialize(x);
        assertEquals(d, 9);
    }



    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }
}
