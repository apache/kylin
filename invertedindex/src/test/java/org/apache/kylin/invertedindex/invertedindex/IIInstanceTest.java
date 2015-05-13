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
 */
public class IIInstanceTest extends LocalFileMetadataTestCase {
    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void clean() {
        cleanupTestMetadata();
    }


    @Test
    public void testGetIIsByDesc() throws IOException {
        IIManager mgr = IIManager.getInstance(getTestConfig());

        List<IIInstance> iiInstances = mgr.getIIsByDesc("test_kylin_ii_left_join_desc");

        Assert.assertTrue(iiInstances.size() > 0);


        IIInstance instance = iiInstances.get(0);

        Dictionary dict = mgr.getDictionary(instance.getFirstSegment(), instance.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_SITE_ID"));

        Assert.assertNotNull(dict);
    }


    @Test
    public void testCreateIIInstance() throws IOException {


        IIDesc iiDesc = IIDescManager.getInstance(getTestConfig()).getIIDesc("test_kylin_ii_left_join_desc");

        IIInstance ii = IIInstance.create("new ii", "default", iiDesc);

        IIManager iiMgr = IIManager.getInstance(getTestConfig());

        List<IIInstance> allIIList = iiMgr.listAllIIs();

        iiMgr.createII(ii);

        Assert.assertNotNull(iiMgr.getII("new ii"));
    }


}
