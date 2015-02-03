package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Hongbin Ma(Binmahone) on 12/3/14.
 */
public class TableRecordInfoTest extends LocalFileMetadataTestCase {
    IIInstance ii;
    TableRecordInfo tableRecordInfo;

    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii");
        this.tableRecordInfo = new TableRecordInfo(ii.getFirstSegment());
    }

    @Test
    public void testSerialize() {
        byte[] x = TableRecordInfoDigest.serialize(this.tableRecordInfo.getDigest());
        TableRecordInfoDigest d = TableRecordInfoDigest.deserialize(x);
        assertEquals(d.getColumnCount(), 25);
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }
}
