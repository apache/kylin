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

package org.apache.kylin.metadata.usergroup;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class NUserGroupManagerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCRUD() {
        NUserGroupManager group = NUserGroupManager.getInstance(getTestConfig());
        group.add("g1");
        group.add("g2");
        group.add("g3");
        Assert.assertTrue(group.exists("g1"));
        Assert.assertFalse(group.exists("g4"));
        Assert.assertEquals(Lists.newArrayList("g1", "g2", "g3"), group.getAllGroupNames());
        try {
            group.add("g1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (KylinException e) {
            Assert.assertEquals("The user group \"g1\" already exists. Please check and try again.", e.getMessage());
        }

        group.delete("g1");
        Assert.assertFalse(group.exists("g1"));

        try {
            group.delete("g1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("Invalid values in parameter “group_name“. The value g1 doesn’t exist.",
                    e.getMessage());
        }
    }

    @Test
    public void testCRUDCaseInsensitive() {
        NUserGroupManager group = NUserGroupManager.getInstance(getTestConfig());
        group.add("test1");
        group.add("test2");
        group.add("test3");
        try {
            group.add("TEST1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (KylinException e) {
            Assert.assertEquals("The user group \"test1\" already exists. Please check and try again.", e.getMessage());
        }

        group.delete("Test1");
        Assert.assertFalse(group.exists("test1"));

        try {
            group.delete("test1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("Invalid values in parameter “group_name“. The value test1 doesn’t exist.",
                    e.getMessage());
        }
    }
}
