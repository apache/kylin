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

package org.apache.kylin.rest.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.hadoop.util.ZKUtil;
import org.apache.kylin.common.util.ZookeeperAclBuilder;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.zookeeper.ZookeeperAutoConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

@MetadataInfo
class ZookeeperConfigTest {

    @Test
    @OverwriteProp.OverwriteProps({ //
            @OverwriteProp(key = "kylin.env.zookeeper-acl-enabled", value = "true"),
            @OverwriteProp(key = "kylin.env.zookeeper.zk-auth", value = "digest:ADMIN:KYLIN"),
            @OverwriteProp(key = "kylin.env.zookeeper.zk-acl", value = "world:anyone:rwcda") })
    void customize() {
        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withPropertyValues("spring.cloud.zookeeper.enabled:true")
                .withConfiguration(AutoConfigurations.of(ZookeeperAutoConfiguration.class, ZookeeperConfig.class));
        contextRunner.withUserConfiguration(ZookeeperAutoConfiguration.class, ZookeeperConfig.class).run((context) -> {
            CuratorFramework client = context.getBean(CuratorFramework.class);

            ZKUtil.ZKAuthInfo zkAuths = ZookeeperAclBuilder.getZKAuths().get(0);
            List authInfos = ((List) ReflectionTestUtils.getField(client, "authInfos"));
            assertTrue(CollectionUtils.isNotEmpty(authInfos));
            val auth = authInfos.stream().findFirst();
            assertTrue(auth.isPresent());
            AuthInfo authInfo = (AuthInfo) auth.get();
            assertEquals(zkAuths.getScheme(), authInfo.getScheme());
            assertEquals("digest", authInfo.getScheme());
            assertEquals(new String(zkAuths.getAuth(), StandardCharsets.UTF_8),
                    new String(authInfo.getAuth(), StandardCharsets.UTF_8));
            assertEquals("ADMIN:KYLIN", new String(authInfo.getAuth(), StandardCharsets.UTF_8));

            ACL zkAcl = ZookeeperAclBuilder.getZKAcls().get(0);
            ACLProvider aclProvider = ((ACLProvider) ReflectionTestUtils.getField(client, "aclProvider"));
            List<ACL> acls = aclProvider.getDefaultAcl();
            assertTrue(CollectionUtils.isNotEmpty(acls));
            val aclOptional = acls.stream().findFirst();
            assertTrue(aclOptional.isPresent());
            ACL acl = aclOptional.get();
            assertEquals(zkAcl.getId(), acl.getId());
            assertEquals(zkAcl.getPerms(), acl.getPerms());
        });
    }
}