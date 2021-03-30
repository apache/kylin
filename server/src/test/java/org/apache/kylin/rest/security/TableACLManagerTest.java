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

package org.apache.kylin.rest.security;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.acl.TableACL;
import org.apache.kylin.metadata.acl.TableACLManager;
import org.apache.kylin.rest.util.MultiNodeManagerTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TableACLManagerTest extends MultiNodeManagerTestBase {
    @Test
    public void testCaseInsensitiveFromDeserializer() throws IOException {
        final TableACLManager manager = new TableACLManager(configA);
        manager.addTableACL(PROJECT, USER, "TABLE1", MetadataConstants.TYPE_USER);
        TableACL tableACL = Preconditions.checkNotNull(getStore().getResource("/table_acl/" + PROJECT, new JsonSerializer<>(TableACL.class)));
        Assert.assertEquals(1, tableACL.getNoAccessList("table1", MetadataConstants.TYPE_USER).size());
    }

    @Test
    public void test() throws Exception {
        final TableACLManager tableACLManagerA = new TableACLManager(configA);
        final TableACLManager tableACLManagerB = new TableACLManager(configB);

        Assert.assertEquals(0, tableACLManagerB.getTableACLByCache(PROJECT).size());
        tableACLManagerA.addTableACL(PROJECT, USER, TABLE, MetadataConstants.TYPE_USER);
        // if don't sleep, manager B's get method is faster than notify
        Thread.sleep(3000);
        Assert.assertEquals(1, tableACLManagerB.getTableACLByCache(PROJECT).size());

        Assert.assertEquals(1, tableACLManagerA.getTableACLByCache(PROJECT).size());
        tableACLManagerB.deleteTableACL(PROJECT, USER, TABLE, MetadataConstants.TYPE_USER);
        Thread.sleep(3000);
        Assert.assertEquals(0, tableACLManagerA.getTableACLByCache(PROJECT).size());
    }
}
