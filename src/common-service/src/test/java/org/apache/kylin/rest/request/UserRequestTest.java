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

package org.apache.kylin.rest.request;

import static org.apache.kylin.metadata.user.ManagedUser.DISABLED_ROLE;
import static org.apache.kylin.rest.constant.Constant.GROUP_ALL_USERS;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.user.ManagedUser;
import org.junit.Assert;
import org.junit.Test;

public class UserRequestTest {

    @Test
    public void testUpdateManager() {
        {
            UserRequest userRequest = new UserRequest();
            userRequest.setUsername("user1");
            userRequest.setPassword("password1");
            userRequest.setDisabled(false);
            userRequest.setDefaultPassword(false);
            userRequest.setAuthorities(Lists.newArrayList(GROUP_ALL_USERS));
            ManagedUser managedUser = userRequest.updateManager(new ManagedUser());

            Assert.assertEquals(userRequest.getUsername(), managedUser.getUsername());
            Assert.assertEquals(userRequest.getPassword(), managedUser.getPassword());
            Assert.assertEquals(userRequest.getDisabled(), managedUser.isDisabled());
            Assert.assertEquals(userRequest.getDefaultPassword(), managedUser.isDefaultPassword());
            Assert.assertEquals(userRequest.transformSimpleGrantedAuthorities(), managedUser.getAuthorities());
        }

        {
            UserRequest userRequest = new UserRequest();
            ManagedUser managedUser = userRequest.updateManager(new ManagedUser());

            Assert.assertNull(managedUser.getUsername());
            Assert.assertNull(managedUser.getPassword());
            Assert.assertFalse(managedUser.isDisabled());
            Assert.assertFalse(managedUser.isDefaultPassword());
        }

        {
            UserRequest userRequest = new UserRequest();
            userRequest.setUsername("user1");
            userRequest.setPassword("password1");
            userRequest.setDisabled(true);
            userRequest.setDefaultPassword(false);
            userRequest.setAuthorities(Lists.newArrayList(DISABLED_ROLE));
            ManagedUser managedUser = userRequest.updateManager(new ManagedUser());

            Assert.assertEquals(userRequest.getUsername(), managedUser.getUsername());
            Assert.assertEquals(userRequest.getPassword(), managedUser.getPassword());
            Assert.assertTrue(managedUser.isDisabled());
            Assert.assertEquals(userRequest.getDefaultPassword(), managedUser.isDefaultPassword());
            Assert.assertFalse(managedUser.getAuthorities().isEmpty());
            Assert.assertTrue(userRequest.transformSimpleGrantedAuthorities().isEmpty());
        }
    }
}
