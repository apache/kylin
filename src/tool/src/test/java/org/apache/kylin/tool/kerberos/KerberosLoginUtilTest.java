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
package org.apache.kylin.tool.kerberos;

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

public class KerberosLoginUtilTest {
    private static final String validKeyTab = "src/test/resources/kerberos/valid.keytab";
    private static final String invalidKeyTab = "src/test/resources/kerberos/invalid.keytab";

    @Test
    public void checkKeyTabIsValid() {
        Assert.assertTrue(KerberosLoginUtil.checkKeyTabIsValid(validKeyTab));
    }

    @Test
    public void checkKeyTabIsInvalid() {
        Assert.assertFalse(KerberosLoginUtil.checkKeyTabIsValid(invalidKeyTab));
    }

    @Test
    public void checkKeyTabIsExist() {
        Assert.assertTrue(KerberosLoginUtil.checkKeyTabIsExist(invalidKeyTab));
    }

    @Test
    public void checkKeyTabIsMissing() {
        Assert.assertFalse(KerberosLoginUtil.checkKeyTabIsExist(invalidKeyTab + "x"));
    }

    @Test
    public void testSetJaasConfCheck() {
        testSetJaasConfCheck(null, null, null, "input loginContextName is invalid.");
        testSetJaasConfCheck("", null, null, "input loginContextName is invalid.");
        testSetJaasConfCheck("Client", null, null, "input principal is invalid.");
        testSetJaasConfCheck("Client", "", null, "input principal is invalid.");
        testSetJaasConfCheck("Client", "test", null, "input keytabFile is invalid.");
        testSetJaasConfCheck("Client", "test", "", "input keytabFile is invalid.");
    }

    void testSetJaasConfCheck(String loginContextName, String principal, String keytabFile, String message) {
        try {
            KerberosLoginUtil.setJaasConf(loginContextName, principal, keytabFile);
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertEquals(String.valueOf(message), e.getMessage());
        }
    }
}
