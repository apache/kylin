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

package org.apache.kylin.metadata.password;

import org.apache.kylin.util.PasswordEncodeFactory;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder;

public class PasswordEncodeFactoryTest extends NLocalFileMetadataTestCase {
    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testNewUserPasswordEncoder() {
        getTestConfig().setProperty("kylin.security.user-password-encoder",
                "org.springframework.security.crypto.password.Pbkdf2PasswordEncoder");
        Assert.assertTrue(PasswordEncodeFactory.newUserPasswordEncoder() instanceof Pbkdf2PasswordEncoder);
        getTestConfig().setProperty("kylin.security.user-password-encoder",
                "org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder");
        Assert.assertTrue(PasswordEncodeFactory.newUserPasswordEncoder() instanceof BCryptPasswordEncoder);
    }

}
