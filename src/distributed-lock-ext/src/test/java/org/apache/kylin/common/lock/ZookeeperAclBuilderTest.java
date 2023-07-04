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

package org.apache.kylin.common.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;

import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.hadoop.util.ZKUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ZookeeperAclBuilder;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.Test;

@MetadataInfo
class ZookeeperAclBuilderTest {

    @Test
    void testAclEnabled() {
        KylinConfig testConfig = KylinConfig.getInstanceFromEnv();
        testConfig.setProperty("kylin.env.zookeeper-acl-enabled", "true");
        testConfig.setProperty("kylin.env.zookeeper.zk-auth", "ADMIN:KYLIN");

        ZookeeperAclBuilder zookeeperAclBuilder = new ZookeeperAclBuilder().invoke();
        assertNotNull(zookeeperAclBuilder);
        assertTrue(zookeeperAclBuilder.isNeedAcl());

        List<ACL> zkAcls = Lists.newArrayList();
        try {
            zkAcls = ZookeeperAclBuilder.getZKAcls();
            assertFalse(zkAcls.isEmpty());
        } catch (Exception e) {
            fail("Couldn't read ACLs based on 'kylin.env.zookeeper.zk-acl' in kylin.properties");
        }

        List<ZKUtil.ZKAuthInfo> zkAuthInfo = Lists.newArrayList();
        try {
            zkAuthInfo = ZookeeperAclBuilder.getZKAuths();
            assertFalse(zkAuthInfo.isEmpty());
        } catch (Exception e) {
            fail("Couldn't read Auth based on 'kylin.env.zookeeper.zk-auth' in kylin.properties");
        }

        Builder builder = zookeeperAclBuilder.setZKAclBuilder(CuratorFrameworkFactory.builder());
        assertNotNull(builder);
        assertEquals(zkAcls, builder.getAclProvider().getDefaultAcl());
        assertNotNull(builder.getAuthInfos());
    }

    @Test
    void testAclDisabled() {
        KylinConfig testConfig = KylinConfig.getInstanceFromEnv();
        testConfig.setProperty("kylin.env.zookeeper-acl-enabled", "false");

        ZookeeperAclBuilder zookeeperAclBuilder = new ZookeeperAclBuilder().invoke();
        assertNotNull(zookeeperAclBuilder);
        assertFalse(zookeeperAclBuilder.isNeedAcl());

        Builder builder = zookeeperAclBuilder.setZKAclBuilder(CuratorFrameworkFactory.builder());
        assertNotNull(builder);
        assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, builder.getAclProvider().getDefaultAcl());
        assertNull(builder.getAuthInfos());
    }

    @Test
    void testShadedAclEnabled() {
        KylinConfig testConfig = KylinConfig.getInstanceFromEnv();
        testConfig.setProperty("kylin.env.zookeeper-acl-enabled", "true");
        testConfig.setProperty("kylin.env.zookeeper.zk-auth", "ADMIN:KYLIN");

        ZookeeperAclBuilder zookeeperAclBuilder = new ZookeeperAclBuilder().invoke();
        assertNotNull(zookeeperAclBuilder);
        assertTrue(zookeeperAclBuilder.isNeedAcl());

        List<ACL> zkAcls = Lists.newArrayList();
        try {
            zkAcls = ZookeeperAclBuilder.getZKAcls();
            assertFalse(zkAcls.isEmpty());
        } catch (Exception e) {
            fail("Couldn't read ACLs based on 'kylin.env.zookeeper.zk-acl' in kylin.properties");
        }

        List<ZKUtil.ZKAuthInfo> zkAuthInfo = Lists.newArrayList();
        try {
            zkAuthInfo = ZookeeperAclBuilder.getZKAuths();
            assertFalse(zkAuthInfo.isEmpty());
        } catch (Exception e) {
            fail("Couldn't read Auth based on 'kylin.env.zookeeper.zk-auth' in kylin.properties");
        }

        org.apache.kylin.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory.Builder builder = zookeeperAclBuilder
                .setZKAclBuilder(
                        org.apache.kylin.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory.builder());
        assertNotNull(builder);
        assertEquals(zkAcls, builder.getAclProvider().getDefaultAcl());
        assertNotNull(builder.getAuthInfos());
    }

    @Test
    void testShadedAclDisabled() {
        KylinConfig testConfig = KylinConfig.getInstanceFromEnv();
        testConfig.setProperty("kylin.env.zookeeper-acl-enabled", "false");

        ZookeeperAclBuilder zookeeperAclBuilder = new ZookeeperAclBuilder().invoke();
        assertNotNull(zookeeperAclBuilder);
        assertFalse(zookeeperAclBuilder.isNeedAcl());

        org.apache.kylin.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory.Builder builder = zookeeperAclBuilder
                .setZKAclBuilder(
                        org.apache.kylin.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory.builder());
        assertNotNull(builder);
        assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, builder.getAclProvider().getDefaultAcl());
        assertNull(builder.getAuthInfos());
    }
}
