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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Locale;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kylin.common.KylinConfig;

import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;

public class SecretKeyUtil {
    private static final String STRING_ENCODE = "UTF-8";

    private static final String ENCRYPTION_ALGORITHM_AES = "AES";

    private static final String ENCRYPTION_ALGORITHM_MD5 = "MD5";

    @VisibleForTesting
    public static final String KG_SECRET_KEY_FILE_NAME = "kg_secret_key";

    private static SecretKey kgSecretKey = null;

    public static byte[] encryptToken(SecretKey secretKey, String token) throws Exception {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM_AES);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] byteContent = token.getBytes(STRING_ENCODE);
        return cipher.doFinal(byteContent);
    }

    public static String decryptToken(SecretKey secretKey, byte[] encryptedToken) throws Exception {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM_AES);
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] result = cipher.doFinal(encryptedToken);
        return new String(result, STRING_ENCODE);
    }

    public static byte[] encryptToken(String key, String token) throws Exception {
        MessageDigest md = MessageDigest.getInstance(ENCRYPTION_ALGORITHM_MD5);
        byte[] digest = md.digest(key.getBytes(STRING_ENCODE));
        SecretKeySpec keySpec = new SecretKeySpec(digest, ENCRYPTION_ALGORITHM_AES);
        return encryptToken(keySpec, token);
    }

    public static String decryptToken(String key, byte[] encryptedToken) throws Exception {
        MessageDigest md = MessageDigest.getInstance(ENCRYPTION_ALGORITHM_MD5);
        byte[] digest = md.digest(key.getBytes(STRING_ENCODE));
        SecretKeySpec keySpec = new SecretKeySpec(digest, ENCRYPTION_ALGORITHM_AES);
        return decryptToken(keySpec, encryptedToken);
    }

    /**
     * for KG
     * @throws IOException
     */
    public static void initKGSecretKey() throws IOException, NoSuchAlgorithmException {
        File kgSecretKeyFile = new File(KylinConfig.getKylinHome(), KG_SECRET_KEY_FILE_NAME);
        if (kgSecretKeyFile.exists()) {
            Files.delete(kgSecretKeyFile.toPath());
        }

        if (null == kgSecretKey) {
            kgSecretKey = generateNewAESKey();
        }
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(kgSecretKeyFile))) {
            bos.write(kgSecretKey.getEncoded());
        }
    }

    /**
     * for KG
     * @throws IOException
     */
    public static void checkKGSecretKeyFile() throws IOException, NoSuchAlgorithmException {
        File kgSecretKeyFile = new File(KylinConfig.getKylinHome(), KG_SECRET_KEY_FILE_NAME);
        if (kgSecretKeyFile.exists()) {
            return;
        }

        initKGSecretKey();
    }

    /**
     * for KG
     * get cached kgSecretKey
     * @return SecretKey
     */
    public static SecretKey getKGSecretKey() {
        return kgSecretKey;
    }

    /**
     * for KG
     * read KgSecretKey from file
     * @return SecretKey
     * @throws IOException
     */
    public static SecretKey readKGSecretKeyFromFile() throws IOException {
        File kgSecretKeyFile = new File(KylinConfig.getKylinHome(), KG_SECRET_KEY_FILE_NAME);
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(kgSecretKeyFile))) {
            byte[] keyBytes = new byte[16];
            if (bis.read(keyBytes) < 1) {
                throw new RuntimeException(String.format(Locale.ROOT, "%s file is empty!", KG_SECRET_KEY_FILE_NAME));
            }
            return new SecretKeySpec(keyBytes, ENCRYPTION_ALGORITHM_AES);
        }
    }

    /**
     * for KG
     * @return
     */
    public static SecretKey generateNewAESKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(ENCRYPTION_ALGORITHM_AES);
        keyGenerator.init(128, new SecureRandom());
        return keyGenerator.generateKey();
    }

    /**
     * for KG
     * @param kgSecretKey
     * @param kylinPid
     * @return
     */
    public static byte[] generateEncryptedTokenWithPid(SecretKey kgSecretKey, String kylinPid) throws Exception {
        return encryptToken(kgSecretKey, kylinPid + "_" + System.currentTimeMillis());
    }

}
