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

package org.apache.kylin.rest.security;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.common.KylinConfig;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Properties;

/**
 * @author xduo
 * 
 */
public class PasswordPlaceholderConfigurer extends PropertyPlaceholderConfigurer {

    private static byte[] key = { 0x74, 0x68, 0x69, 0x73, 0x49, 0x73, 0x41, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x4b, 0x65, 0x79 };

    public PasswordPlaceholderConfigurer() {
        Resource[] resources = new Resource[1];
        resources[0] = new InputStreamResource(KylinConfig.getKylinPropertiesAsInputSteam());
        this.setLocations(resources);
    }

    public static String encrypt(String strToEncrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            final String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
            return encryptedString;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static String decrypt(String strToDecrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            final String decryptedString = new String(cipher.doFinal(Base64.decodeBase64(strToDecrypt)));
            return decryptedString;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected String resolvePlaceholder(String placeholder, Properties props) {
        if (placeholder.toLowerCase().contains("password")) {
            return decrypt(props.getProperty(placeholder));
        } else {
            return props.getProperty(placeholder);
        }
    }
    
    private static void printUsage() {
        System.out.println("Usage: java org.apache.kylin.rest.security.PasswordPlaceholderConfigurer <EncryptMethod> <your_password>");
        System.out.println("EncryptMethod: AES or BCrypt");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }

        String encryptMethod = args[0];
        String passwordTxt = args[1];
        if ("AES".equalsIgnoreCase(encryptMethod)) {
            System.out.println(encryptMethod + " encrypted password is: ");
            System.out.println(encrypt(passwordTxt));
        } else if ("BCrypt".equalsIgnoreCase(encryptMethod)) {
            BCryptPasswordEncoder bCryptPasswordEncoder = new BCryptPasswordEncoder();
            System.out.println(encryptMethod + " encrypted password is: ");
            System.out.println(bCryptPasswordEncoder.encode(passwordTxt));
        } else {
            printUsage();
            System.exit(1);
        }
    }
}
