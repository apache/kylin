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

import io.kyligence.kap.metadata.user.ManagedUser;
import org.junit.Assert;
import org.junit.Test;

public class UserLockRuleUtilTest {

    @Test
    public void testIsLockedPermanently() {
        ManagedUser managedUser = new ManagedUser();
        managedUser.setUsername("test");
        managedUser.setWrongTime(9);

        Assert.assertFalse(UserLockRuleUtil.isLockedPermanently(managedUser));

        managedUser.setWrongTime(10);
        Assert.assertTrue(UserLockRuleUtil.isLockedPermanently(managedUser));
    }

    @Test
    public void testIsLockDurationEnded() {
        ManagedUser managedUser = new ManagedUser();
        managedUser.setUsername("test");

        managedUser.setWrongTime(3);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 30 * 1000L));

        managedUser.setWrongTime(4);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 60 * 1000L));

        managedUser.setWrongTime(5);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 5 * 60 * 1000L));

        managedUser.setWrongTime(6);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 10 * 60 * 1000L));

        managedUser.setWrongTime(7);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 30 * 60 * 1000L));

        managedUser.setWrongTime(8);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 24 * 3600 * 1000L));

        managedUser.setWrongTime(9);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertTrue(UserLockRuleUtil.isLockDurationEnded(managedUser, 72 * 3600 * 1000L));

        managedUser.setWrongTime(10);
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 20 * 1000L));
        Assert.assertFalse(UserLockRuleUtil.isLockDurationEnded(managedUser, 72 * 3600 * 1000L));
    }

    @Test
    public void testGetLockLeftSeconds() {

        ManagedUser managedUser = new ManagedUser();
        managedUser.setUsername("test");

        managedUser.setWrongTime(3);
        Assert.assertEquals(10, UserLockRuleUtil.getLockLeftSeconds(managedUser, 20 * 1000L));
        Assert.assertEquals(1, UserLockRuleUtil.getLockLeftSeconds(managedUser, 30 * 1000L - 2));
        Assert.assertEquals(1, UserLockRuleUtil.getLockLeftSeconds(managedUser, 30 * 1000L));

        managedUser.setWrongTime(9);
        Assert.assertEquals(72 * 3600 - 20, UserLockRuleUtil.getLockLeftSeconds(managedUser, 20 * 1000L));
        Assert.assertEquals(1, UserLockRuleUtil.getLockLeftSeconds(managedUser, 72 * 3600 * 1000L - 2));
        Assert.assertEquals(1, UserLockRuleUtil.getLockLeftSeconds(managedUser, 72 * 3600 * 1000L));
    }
}
