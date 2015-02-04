package org.apache.kylin.invertedindex.invertedindex;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by shaoshi on 1/30/15.
 */
public class IIDescManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() {
        this.createTestMetadata();
    }

    @After
    public void clean() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCRUD() throws IOException {
        IIDescManager mgr = IIDescManager.getInstance(getTestConfig());

        String newDescName = "Copy of test_kylin_ii_desc";

        try {
            IIDesc testRecord = mgr.getIIDesc(newDescName);
            if (testRecord != null)
                mgr.removeIIDesc(testRecord);
        } catch (IOException e) {
            // just ensure the old one is removed
        }

        Assert.assertNull(mgr.getIIDesc(newDescName));
        IIDesc desc = mgr.getIIDesc("test_kylin_ii_desc");

        desc.setName(newDescName);
        desc.setLastModified(0);

        mgr.createIIDesc(desc);


        desc = mgr.getIIDesc(newDescName);

        Assert.assertNotNull(desc);

        mgr.updateIIDesc(desc); // this will trigger cache wipe; please ignore the HTTP error in logs.

        mgr.removeIIDesc(desc);

        Assert.assertNull(mgr.getIIDesc(newDescName));


    }

    @Test
    public void testGetIIsByDesc() throws IOException {
        IIManager mgr = IIManager.getInstance(getTestConfig());

        List<IIInstance> iiInstances = mgr.getIIsByDesc("test_kylin_ii_desc");

        Assert.assertTrue(iiInstances.size() > 0);


        IIInstance instance = iiInstances.get(0);

        Dictionary dict = mgr.getDictionary(instance.getFirstSegment(), instance.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_SITE_ID"));

        Assert.assertNotNull(dict);
    }

}
