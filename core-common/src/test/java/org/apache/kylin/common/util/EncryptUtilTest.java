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

package org.apache.kylin.common.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EncryptUtilTest extends LocalFileMetadataTestCase {
    @BeforeEach
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @AfterEach
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    void testAESEncrypt(){
        String input = "hello world";
        String result1 = EncryptUtil.encrypt(input);
        String decrypt1 = EncryptUtil.decrypt(result1);
        Assertions.assertEquals(input, decrypt1);
        String result2 = EncryptUtil.encrypt(input);
        Assertions.assertEquals(result2, result1);

        input = "this is a long string #&$";
        result1 = EncryptUtil.encrypt(input);
        decrypt1 = EncryptUtil.decrypt(result1);
        Assertions.assertEquals(input, decrypt1);
        result2 = EncryptUtil.encrypt(input);
        Assertions.assertEquals(result2, result1);


    }

    @Test
    void testNullInput() {
        Assertions.assertNull(EncryptUtil.encrypt(null));
        Assertions.assertNull(EncryptUtil.decrypt(null));
    }

}

