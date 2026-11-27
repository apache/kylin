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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EncryptUtil {

    /**
     * Built-in key that older releases used for every deployment. It is kept only so that
     * configuration values encrypted by those releases remain readable. New values should be
     * encrypted with a deployment-specific key configured through {@link #ENCRYPTION_KEY_PROPERTY}
     * or {@link #ENCRYPTION_KEY_ENV}.
     */
    private static final byte[] LEGACY_KEY = { 0x74, 0x68, 0x69, 0x73, 0x49, 0x73, 0x41, 0x53, 0x65, 0x63, 0x72, 0x65,
            0x74, 0x4b, 0x65, 0x79 };

    public static final String ENCRYPTION_KEY_PROPERTY = "kylin.security.encryption-key";

    public static final String ENCRYPTION_KEY_ENV = "KYLIN_SECURITY_ENCRYPTION_KEY";

    /**
     * Cipher mode for newly encrypted values: {@code ECB} (the default, and the only one older releases can
     * read) or {@code GCM}. It only governs writing; values in either format stay readable whatever the mode.
     * Like the key, it is read from the JVM system property or the environment, not from kylin.properties, so
     * set it for every JVM that encrypts values, including the command line tools.
     */
    public static final String ENCRYPTION_MODE_PROPERTY = "kylin.security.encryption-mode";

    public static final String ENCRYPTION_MODE_ENV = "KYLIN_SECURITY_ENCRYPTION_MODE";

    public static final String ENC_PREFIX = "ENC('";
    public static final String ENC_SUBFIX = "')";

    public static final String DEC_FLAG = "DEC";

    private static final String AES_TRANSFORMATION = "AES/ECB/PKCS5Padding";
    private static final String SHA_256 = "SHA-256";
    private static final int AES_KEY_LENGTH = 16;

    private static final int AES_BLOCK_SIZE = 16;
    private static final String GCM_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_BITS = 128;
    private static final SecureRandom RANDOM = new SecureRandom();

    private static volatile boolean legacyKeyWarningLogged = false;

    public static boolean isEncrypted(String value) {
        return StringUtils.isNotEmpty(value) && value.startsWith(ENC_PREFIX) && value.endsWith(ENC_SUBFIX);
    }

    /**
     * The cipher mode is chosen by {@link #ENCRYPTION_MODE_PROPERTY}.
     */
    public static String encrypt(String strToEncrypt) {
        byte[] key = encryptionKey();
        return gcmEnabled() ? encryptGcm(strToEncrypt, key) : encryptEcb(strToEncrypt, key);
    }

    public static String encryptWithPrefix(String value) {
        return ENC_PREFIX + encrypt(value) + ENC_SUBFIX;
    }

    public static String decrypt(String strToDecrypt) {
        byte[] configuredKey = configuredKey();
        if (configuredKey != null) {
            try {
                return decrypt(strToDecrypt, configuredKey);
            } catch (Exception e) {
                // Values encrypted before the configured key was introduced still use the legacy key.
                log.debug("Failed to decrypt with the configured encryption key, retrying with the legacy key");
            }
        }
        return decrypt(strToDecrypt, LEGACY_KEY);
    }

    public static String decryptPassInKylin(String value) {
        return decrypt(value.substring(ENC_PREFIX.length(), value.length() - ENC_SUBFIX.length()));
    }

    private static void printUsage() {
        System.out.println("Usage: java org.apache.kylin.common.util <your_password>");
    }

    public static String getDecryptedValue(String value) {
        try {
            if (isEncrypted(value)) {
                return decryptPassInKylin(value);
            }
            return value;
        } catch (Exception e) {
            log.error("Get decrypted value failed, {}", value, e);
            return null;
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            printUsage();
            Unsafe.systemExit(1);
        }

        String passwordTxt = args[0];
        // for encrypt password like LDAP password
        System.out.println(EncryptUtil.encrypt(passwordTxt));
    }

    private static boolean gcmEnabled() {
        String mode = System.getProperty(ENCRYPTION_MODE_PROPERTY);
        if (StringUtils.isBlank(mode)) {
            mode = System.getenv(ENCRYPTION_MODE_ENV);
        }
        if (StringUtils.isBlank(mode) || "ECB".equalsIgnoreCase(mode.trim())) {
            return false;
        }
        if ("GCM".equalsIgnoreCase(mode.trim())) {
            return true;
        }
        // A typo must not silently downgrade to ECB.
        throw new IllegalArgumentException(
                "Unsupported " + ENCRYPTION_MODE_PROPERTY + " '" + mode + "', expected ECB or GCM");
    }

    private static byte[] encryptionKey() {
        byte[] configuredKey = configuredKey();
        if (configuredKey != null) {
            return configuredKey;
        }
        if (!legacyKeyWarningLogged) {
            log.warn("{} is not configured. Falling back to the built-in legacy encryption key, which is shared "
                    + "by every Apache Kylin deployment. Configure a deployment-specific key to protect stored "
                    + "credentials.", ENCRYPTION_KEY_PROPERTY);
            legacyKeyWarningLogged = true;
        }
        return LEGACY_KEY;
    }

    private static byte[] configuredKey() {
        String key = System.getProperty(ENCRYPTION_KEY_PROPERTY);
        if (StringUtils.isBlank(key)) {
            key = System.getenv(ENCRYPTION_KEY_ENV);
        }
        if (StringUtils.isBlank(key)) {
            return null;
        }
        return deriveKey(key);
    }

    private static byte[] deriveKey(String secret) {
        try {
            MessageDigest digest = MessageDigest.getInstance(SHA_256);
            byte[] hash = digest.digest(secret.getBytes(StandardCharsets.UTF_8));
            byte[] key = new byte[AES_KEY_LENGTH];
            System.arraycopy(hash, 0, key, 0, AES_KEY_LENGTH);
            return key;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Produces {@code base64(iv || ciphertext || tag)}. The plaintext is PKCS#7 padded to whole blocks, so the
     * decoded length is always 12 modulo 16 and can never be mistaken for AES/ECB output.
     */
    private static String encryptGcm(String strToEncrypt, byte[] key) {
        try {
            byte[] plain = strToEncrypt.getBytes(StandardCharsets.UTF_8);
            int padLength = AES_BLOCK_SIZE - plain.length % AES_BLOCK_SIZE;
            byte[] padded = Arrays.copyOf(plain, plain.length + padLength);
            Arrays.fill(padded, plain.length, padded.length, (byte) padLength);

            byte[] iv = new byte[GCM_IV_LENGTH];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(GCM_TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(GCM_TAG_BITS, iv));
            return Base64.encodeBase64String(ArrayUtils.addAll(iv, cipher.doFinal(padded)));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * The format is read off the ciphertext, not the configured mode: AES/ECB output is always a whole number
     * of blocks, while {@link #encryptGcm} output never is.
     */
    private static String decrypt(String strToDecrypt, byte[] key) {
        try {
            byte[] data = Base64.decodeBase64(strToDecrypt);
            boolean gcm = data != null && data.length % AES_BLOCK_SIZE == GCM_IV_LENGTH;
            return gcm ? decryptGcm(data, key) : decryptEcb(data, key);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String decryptGcm(byte[] data, byte[] key) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(GCM_TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"),
                new GCMParameterSpec(GCM_TAG_BITS, data, 0, GCM_IV_LENGTH));
        byte[] padded = cipher.doFinal(data, GCM_IV_LENGTH, data.length - GCM_IV_LENGTH);
        return new String(padded, 0, padded.length - padded[padded.length - 1], StandardCharsets.UTF_8);
    }

    private static String encryptEcb(String strToEncrypt, byte[] key) {
        try {
            Cipher cipher = Cipher.getInstance(AES_TRANSFORMATION);
            final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            return Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes(Charset.defaultCharset())));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String decryptEcb(byte[] data, byte[] key) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(AES_TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"));
        return new String(cipher.doFinal(data), Charset.defaultCharset());
    }
}
