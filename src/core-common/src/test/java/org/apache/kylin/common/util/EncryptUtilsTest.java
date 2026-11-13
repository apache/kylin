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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
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

    @Test
    void testEncryptWithConfiguredKey() {
        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-specific-secret");
        try {
            String encrypted = EncryptUtil.encrypt("kylin");
            assertNotEquals("YeqVr9MakSFbgxEec9sBwg==", encrypted);
            assertEquals("kylin", EncryptUtil.decrypt(encrypted));
            assertEquals("kylin",
                    EncryptUtil.getDecryptedValue(EncryptUtil.ENC_PREFIX + encrypted + EncryptUtil.ENC_SUBFIX));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testGcmMode() {
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "GCM");
        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-specific-secret");
        try {
            String encrypted = EncryptUtil.encrypt("kylin");
            // 12-byte IV + the plaintext padded to one 16-byte block + 16-byte tag
            assertEquals(44, Base64.decodeBase64(encrypted).length);
            // random IV: the same plaintext never produces the same ciphertext
            assertNotEquals(encrypted, EncryptUtil.encrypt("kylin"));
            assertEquals("kylin", EncryptUtil.decrypt(encrypted));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testGcmLengthNeverLooksLikeEcb() {
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "GCM");
        try {
            // around the block boundaries; unpadded, 4, 20 and 36 bytes would give a whole number of blocks
            for (int length : new int[] { 0, 1, 4, 15, 16, 17, 20, 36 }) {
                String plaintext = StringUtils.repeat('x', length);
                String encrypted = EncryptUtil.encrypt(plaintext);
                assertEquals(12, Base64.decodeBase64(encrypted).length % 16);
                assertEquals(plaintext, EncryptUtil.decrypt(encrypted));
            }
            assertEquals("pässwörd", EncryptUtil.decrypt(EncryptUtil.encrypt("pässwörd")));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
        }
    }

    @Test
    void testGcmRejectsTamperedCiphertext() {
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "GCM");
        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-specific-secret");
        try {
            String encrypted = EncryptUtil.encrypt("kylin");
            char last = encrypted.charAt(encrypted.length() - 5);
            String tampered = encrypted.substring(0, encrypted.length() - 5) + (last == 'A' ? 'B' : 'A')
                    + encrypted.substring(encrypted.length() - 4);
            assertThrows(RuntimeException.class, () -> EncryptUtil.decrypt(tampered));
            assertNull(EncryptUtil.getDecryptedValue(EncryptUtil.ENC_PREFIX + tampered + EncryptUtil.ENC_SUBFIX));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testEcbValuesStayReadableInGcmMode() {
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "GCM");
        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-specific-secret");
        try {
            // written before GCM was enabled: under the configured key (5.0.5) and under the legacy key
            assertEquals("kylin", EncryptUtil.getDecryptedValue("ENC('Ygf/7Z19ZHXm0TpxVB6bbg==')"));
            assertEquals("kylin", EncryptUtil.getDecryptedValue("ENC('YeqVr9MakSFbgxEec9sBwg==')"));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testMultiBlockEcbValuesStayReadableInGcmMode() {
        String plaintext = "a-password-long-enough-to-span-multiple-aes-blocks";
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "ECB");
        try {
            String legacyCiphertext = EncryptUtil.encrypt(plaintext);
            System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-specific-secret");
            String configuredCiphertext = EncryptUtil.encryptWithPrefix(plaintext);

            System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "GCM");
            assertEquals(plaintext, EncryptUtil.getDecryptedValue(configuredCiphertext));
            assertEquals(plaintext, EncryptUtil.decrypt(legacyCiphertext));
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
            assertEquals(plaintext, EncryptUtil.decrypt(legacyCiphertext));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testLegacyKeyGcmValueStaysReadableWithConfiguredKey() {
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "GCM");
        try {
            // GCM applies to the legacy key too
            String encryptedByLegacyKey = EncryptUtil.encrypt("kylin");
            assertEquals(44, Base64.decodeBase64(encryptedByLegacyKey).length);
            System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-specific-secret");
            assertEquals("kylin", EncryptUtil.decrypt(encryptedByLegacyKey));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testGcmRejectsWrongConfiguredKey() {
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "GCM");
        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-a");
        try {
            String encrypted = EncryptUtil.encrypt("kylin");
            System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-b");
            assertThrows(RuntimeException.class, () -> EncryptUtil.decrypt(encrypted));
            assertNull(EncryptUtil.getDecryptedValue(EncryptUtil.ENC_PREFIX + encrypted + EncryptUtil.ENC_SUBFIX));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testGcmValueStaysReadableInEcbMode() {
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "GCM");
        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-specific-secret");
        try {
            String encrypted = EncryptUtil.encryptWithPrefix("kylin");
            // the mode only governs writing: a JVM that does not have it set can still read GCM values
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
            assertEquals("kylin", EncryptUtil.getDecryptedValue(encrypted));
            System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "ECB");
            assertEquals("kylin", EncryptUtil.getDecryptedValue(encrypted));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testUnsupportedMode() {
        System.setProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY, "CBC");
        try {
            assertThrows(IllegalArgumentException.class, () -> EncryptUtil.encrypt("kylin"));
            // reading does not depend on the mode
            assertEquals("kylin", EncryptUtil.decrypt("YeqVr9MakSFbgxEec9sBwg=="));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_MODE_PROPERTY);
        }
    }

    @Test
    void testLegacyCiphertextStillDecryptsWithConfiguredKey() {
        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-specific-secret");
        try {
            assertEquals("kylin", EncryptUtil.getDecryptedValue("ENC('YeqVr9MakSFbgxEec9sBwg==')"));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }

    @Test
    void testConfiguredKeysAreDeploymentScoped() {
        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-a");
        String encryptedByDeploymentA;
        try {
            encryptedByDeploymentA = EncryptUtil.encrypt("kylin");
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }

        System.setProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY, "deployment-b");
        try {
            assertNotEquals(encryptedByDeploymentA, EncryptUtil.encrypt("kylin"));
            assertNull(EncryptUtil.getDecryptedValue(
                    EncryptUtil.ENC_PREFIX + encryptedByDeploymentA + EncryptUtil.ENC_SUBFIX));
        } finally {
            System.clearProperty(EncryptUtil.ENCRYPTION_KEY_PROPERTY);
        }
    }
}
