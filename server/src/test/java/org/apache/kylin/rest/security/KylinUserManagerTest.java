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

import java.io.IOException;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.MultiNodeManagerTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.google.common.collect.Lists;

public class KylinUserManagerTest extends MultiNodeManagerTestBase {
    @Test
    public void testBasic() throws IOException, InterruptedException {
        final KylinUserManager managerA = new KylinUserManager(configA);
        final KylinUserManager managerB = new KylinUserManager(configB);
        ManagedUser u1 = new ManagedUser("u1", "skippped", false, Lists.<GrantedAuthority> newArrayList());
        managerA.update(u1);
        Thread.sleep(1000);
        ManagedUser u11 = new ManagedUser("u1", "password", false,
                Lists.<GrantedAuthority> newArrayList(new SimpleGrantedAuthority(Constant.ROLE_ANALYST)));
        managerB.update(u11);
        Thread.sleep(1000);
        Assert.assertEquals("password", managerA.get("u1").getPassword());
        Assert.assertEquals("password", managerB.get("u1").getPassword());
        managerB.delete("u1");
        Thread.sleep(1000);
        Assert.assertNull(managerA.get("u1"));
        Assert.assertNull(managerB.get("u1"));
    }
}
