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
package org.apache.kylin.metadata.upgrade;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.aspect.Transaction;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
public class GlobalAclVersionManagerTest {

    @Test
    @Transaction
    public void testBasic() {
        GlobalAclVersion globalAclVersion = new GlobalAclVersion();
        globalAclVersion.setAclVersion(GlobalAclVersion.DATA_PERMISSION_SEPARATE);
        Assert.assertEquals("SYSTEM/acl_version", globalAclVersion.getResourcePath());
        GlobalAclVersionManager manager = new GlobalAclVersionManager(TestUtils.getTestConfig());
        UnitOfWork.doInTransactionWithRetry(() -> {
            GlobalAclVersionManager m = new GlobalAclVersionManager(TestUtils.getTestConfig());
            m.delete();
            return null;
        }, "delete");
        Assert.assertFalse(manager.exists());
        UnitOfWork.doInTransactionWithRetry(() -> {
            GlobalAclVersionManager m = new GlobalAclVersionManager(TestUtils.getTestConfig());
            m.save(globalAclVersion);
            return null;
        }, "save");
        Assert.assertTrue(manager.exists());
        UnitOfWork.doInTransactionWithRetry(() -> {
            GlobalAclVersionManager m = new GlobalAclVersionManager(TestUtils.getTestConfig());
            m.save(globalAclVersion);
            return null;
        }, "save");
        Assert.assertTrue(manager.exists());
        UnitOfWork.doInTransactionWithRetry(() -> {
            GlobalAclVersionManager m = new GlobalAclVersionManager(TestUtils.getTestConfig());
            m.delete();
            return null;
        }, "delete");
        Assert.assertFalse(manager.exists());
    }
}
