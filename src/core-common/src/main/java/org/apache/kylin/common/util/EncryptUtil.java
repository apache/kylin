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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EncryptUtil {
    /**
     * thisIsAsecretKey
     */
    private static final byte[] key = { 0x74, 0x68, 0x69, 0x73, 0x49, 0x73, 0x41, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74,
            0x4b, 0x65, 0x79 };

    public static final String ENC_PREFIX = "ENC('";
    public static final String ENC_SUBFIX = "')";

    public static final String DEC_FLAG = "DEC";

    public static boolean isEncrypted(String value) {
        return StringUtils.isNotEmpty(value) && value.startsWith(ENC_PREFIX) && value.endsWith(ENC_SUBFIX);
    }

    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_BITS = 128;

    public static String encrypt(String strToEncrypt) {
        if (KylinConfig.readSystemKylinConfig().isGcmEncryptEnabled()) {
            return encryptGcm(strToEncrypt);
        }
        return encryptEcb(strToEncrypt);
    }

    private static String encryptGcm(String strToEncrypt) {
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(gcmKey(), "AES"),
                    new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] encrypted = cipher.doFinal(strToEncrypt.getBytes(Charset.defaultCharset()));
            return Base64.encodeBase64String(
                    ByteBuffer.allocate(iv.length + encrypted.length).put(iv).put(encrypted).array());
        } catch (Exception e) {
            log.error("Encrypt gcm failed: {}", e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String encryptEcb(String strToEncrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, ecbKey());
            return Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes(Charset.defaultCharset())));
        } catch (Exception e) {
            log.error("Encrypt ecb failed: {}", e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * AES-256 key derived from the key material, so a 36-char metastore UUID is a legal key length too.
     */
    private static byte[] gcmKey() {
        val config = KylinConfig.readSystemKylinConfig();
        if (config.isRandomEncryptKeyEnabled()) {
            return DigestUtils.sha256(ResourceUtils.getMetaStoreId());
        }
        return DigestUtils.sha256(key);
    }

    public static String encryptWithPrefix(String value) {
        if (isEncrypted(value)) {
            return value;
        }
        return ENC_PREFIX + encrypt(value) + ENC_SUBFIX;
    }

    public static String decrypt(String strToDecrypt) {
        byte[] raw = Base64.decodeBase64(strToDecrypt);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(gcmKey(), "AES"),
                    new GCMParameterSpec(GCM_TAG_BITS, raw, 0, GCM_IV_LENGTH));
            return new String(cipher.doFinal(raw, GCM_IV_LENGTH, raw.length - GCM_IV_LENGTH), Charset.defaultCharset());
        } catch (Exception e) {
            log.error("Decrypt gcm failed: {}", e.getMessage(), e);
            return decryptLegacyEcb(raw);
        }
    }

    /**
     * Values encrypted before the switch to AES/GCM (e.g. passwords already in kylin.properties) are still ECB.
     */
    private static String decryptLegacyEcb(byte[] raw) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, ecbKey());
            return new String(cipher.doFinal(raw), Charset.defaultCharset());
        } catch (Exception e) {
            log.error("Decrypt legacy ecb failed: {}", e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /** AES-128, so ciphertext written by older versions with the built-in key stays readable. */
    private static SecretKeySpec ecbKey() {
        byte[] material = KylinConfig.readSystemKylinConfig().isRandomEncryptKeyEnabled()
                ? Arrays.copyOf(gcmKey(), key.length)
                : key;
        return new SecretKeySpec(material, "AES");
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
}
