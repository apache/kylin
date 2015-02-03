package com.kylinolap.invertedindex.invertedindex;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import com.kylinolap.invertedindex.IIDescManager;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.metadata.model.DataModelDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


/**
 * Created by shaoshi on 1/30/15.
 */
public class IIDescTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() {
        this.createTestMetadata();

    }

    @After
    public void clear() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetIIDesc() {

        IIDesc iiDesc = IIDescManager.getInstance(getTestConfig()).getIIDesc("test_kylin_ii_desc");
        DataModelDesc model = iiDesc.getModel();
        Assert.assertNotNull(iiDesc);
        Assert.assertNotNull(model);

    }

    @Test
    public void testSerialization() throws IOException {
        IIDesc iiDesc = IIDescManager.getInstance(getTestConfig()).getIIDesc("test_kylin_ii_desc");
        String str = JsonUtil.writeValueAsIndentString(iiDesc);
        System.out.println(str);
        @SuppressWarnings("unused")
        IIDesc desc2 = JsonUtil.readValue(str, IIDesc.class);

        Assert.assertEquals(iiDesc, desc2);
    }
}


