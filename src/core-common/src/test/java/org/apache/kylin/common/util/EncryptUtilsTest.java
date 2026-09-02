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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class EncryptUtilsTest {

    /**
     * The random key is derived from the metastore UUID, so ciphertext is not portable between
     * metastores and the built-in-key value from testGetDecryptedValue no longer decrypts.
     */
    @Test
    void testRandomEncryptKey() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.random-encrypt-key.enabled", "true");

        // default ECB mode also switches to the metastore-derived key
        String ecb = EncryptUtil.encrypt("kylin");
        assertNotEquals("YeqVr9MakSFbgxEec9sBwg==", ecb);
        assertEquals("kylin", EncryptUtil.decrypt(ecb));
        assertNull(EncryptUtil.getDecryptedValue("ENC('YeqVr9MakSFbgxEec9sBwg==')"));

        config.setProperty("kylin.gcm-encrypt.enabled", "true");
        String gcm = EncryptUtil.encrypt("kylin");
        assertEquals("kylin", EncryptUtil.decrypt(gcm));

        config.setProperty("kylin.random-encrypt-key.enabled", "false");
        assertThrows(RuntimeException.class, () -> EncryptUtil.decrypt(ecb));
    }

    @Test
    void testMetaStoreId() {
        assertNotNull(ResourceUtils.getMetaStoreId());
    }

    @Test
    void testGcmEncryptSwitch() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        // ECB is the default, and is deterministic
        assertEquals("YeqVr9MakSFbgxEec9sBwg==", EncryptUtil.encrypt("kylin"));

        config.setProperty("kylin.gcm-encrypt.enabled", "true");
        String gcm = EncryptUtil.encrypt("kylin");
        assertNotEquals("YeqVr9MakSFbgxEec9sBwg==", gcm);
        // AES/GCM uses a random IV, so the ciphertext differs every time
        assertNotEquals(gcm, EncryptUtil.encrypt("kylin"));
        assertEquals("kylin", EncryptUtil.decrypt(gcm));
    }

    @Test
    void testEncryptWithPrefix() {
        String text = "kylin";
        assertEquals("ENC('YeqVr9MakSFbgxEec9sBwg==')", EncryptUtil.encryptWithPrefix(text));
        assertEquals(text, EncryptUtil.getDecryptedValue(EncryptUtil.encryptWithPrefix(text)));

        assertEquals("ENC('YeqVr9MakSFbgxEec9sBwg==')",
                EncryptUtil.encryptWithPrefix("ENC('YeqVr9MakSFbgxEec9sBwg==')"));
    }

    @Test
    void testGetDecryptedValue() {
        // legacy AES/ECB value, must still decrypt after the switch to AES/GCM
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
