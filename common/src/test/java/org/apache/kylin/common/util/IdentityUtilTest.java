package org.apache.kylin.common.util;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class IdentityUtilTest {
    @Test
    public void basicTest()
    {
        String s1 = new String("hi");
        String s2 = new String("hi");

        List<String> c1 = Lists.newArrayList(s1);
        List<String> c2 = Lists.newArrayList(s2);
        List<String> c3 = Lists.newArrayList(s2);

        Assert.assertTrue(CollectionUtils.isEqualCollection(c1,c2));
        Assert.assertTrue(CollectionUtils.isEqualCollection(c3,c2));

        Assert.assertFalse(IdentityUtils.collectionReferenceEquals(c1,c2));
        Assert.assertTrue(IdentityUtils.collectionReferenceEquals(c3,c2));
    }
}
