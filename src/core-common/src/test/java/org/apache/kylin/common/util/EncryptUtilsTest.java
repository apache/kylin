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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class EncryptUtilsTest {

    @Test
    void testEncryptWithPrefix() {
        String text = "kylin";
        assertEquals("ENC('YeqVr9MakSFbgxEec9sBwg==')", EncryptUtil.encryptWithPrefix(text));
    }

    @Test
    void testGetDecryptedValue() {
        String text = "ENC('YeqVr9MakSFbgxEec9sBwg==')";
        assertEquals("kylin", EncryptUtil.getDecryptedValue(text));
    }

    @Test
    void testGetDecryptedValueCase2() {
        String text = "kylin";
        assertEquals("kylin", EncryptUtil.getDecryptedValue(text));
    }

    @Test
    void testGetDecryptedValueError() {
        String text = "ENC('1YeqVr9MakSFbgxEec9sBwg==')";
        assertNull(EncryptUtil.getDecryptedValue(text));
    }
}
