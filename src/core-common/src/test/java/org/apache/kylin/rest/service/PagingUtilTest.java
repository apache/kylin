/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.service;

import java.util.ArrayList;

import org.apache.kylin.rest.util.PagingUtil;
import org.junit.Assert;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class PagingUtilTest {
    @Test
    public void testPageCut() {
        ArrayList<String> list = Lists.newArrayList("a", "b", "c", "d", "e");
        Assert.assertEquals(Lists.newArrayList("d"), PagingUtil.cutPage(list, 3, 1));
    }

    @Test
    public void testFuzzyMatching() {
        ArrayList<String> noAccessList = Lists.newArrayList("a1", "AB1", "Ab1", "aB1", "abc1");
        Assert.assertEquals(Lists.newArrayList("AB1", "Ab1", "aB1", "abc1"),
                PagingUtil.getIdentifierAfterFuzzyMatching("ab", false, noAccessList));
        Assert.assertEquals(Lists.newArrayList("abc1"),
                PagingUtil.getIdentifierAfterFuzzyMatching("ab", true, noAccessList));
    }

    @Test
    public void testIsInCurrentPage() {
        Assert.assertTrue(PagingUtil.isInCurrentPage(0, 0, 5));
        Assert.assertTrue(PagingUtil.isInCurrentPage(6, 1, 5));
        Assert.assertFalse(PagingUtil.isInCurrentPage(11, 1, 5));
        Assert.assertFalse(PagingUtil.isInCurrentPage(1, 1, 5));
    }
}
