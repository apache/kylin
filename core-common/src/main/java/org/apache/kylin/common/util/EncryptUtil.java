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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class EncryptUtil {
    /**
     * thisIsAsecretKey
     */
    private static byte[] key = { 0x74, 0x68, 0x69, 0x73, 0x49, 0x73, 0x41, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x4b,
            0x65, 0x79 };

    private static final Cipher getCipher(int cipherMode) throws InvalidAlgorithmParameterException,
            InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException, UnsupportedEncodingException {
        Cipher cipher = Cipher.getInstance("AES/CFB/PKCS5Padding");
        final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
        IvParameterSpec ivSpec = new IvParameterSpec("AAAAAAAAAAAAAAAA".getBytes("UTF-8"));
        cipher.init(cipherMode, secretKey, ivSpec);
        return cipher;
    }

    public static String encrypt(String strToEncrypt) {
        if (strToEncrypt == null) {
            return null;
        }
        try {
            Cipher cipher = getCipher(Cipher.ENCRYPT_MODE);
            final String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes(
                StandardCharsets.UTF_8)));
            return encryptedString;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static String decrypt(String strToDecrypt) {
        if (strToDecrypt == null) {
            return null;
        }
        try {
            Cipher cipher = getCipher(Cipher.DECRYPT_MODE);
            final String decryptedString = new String(cipher.doFinal(Base64.decodeBase64(strToDecrypt)), StandardCharsets.UTF_8);
            return decryptedString;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java org.apache.kylin.common.util <your_password>");
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            printUsage();
            System.exit(1);
        }

        String passwordTxt = args[0];
        // for encrypt password like LDAP password
        System.out.println(EncryptUtil.encrypt(passwordTxt));
    }
}
