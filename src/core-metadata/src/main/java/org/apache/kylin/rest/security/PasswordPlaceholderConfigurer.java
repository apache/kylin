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

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.rest.exception.PasswordDecryptionException;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

/**
 * @author xduo
 *
 */
public class PasswordPlaceholderConfigurer extends PropertyPlaceholderConfigurer {
    private static final Set<String> passwordWhiteList = Sets.newHashSet("kylin.security.user-password-encoder");

    /**
     * The PasswordPlaceholderConfigurer will read Kylin properties as the Spring resource
     */
    public PasswordPlaceholderConfigurer() throws IOException {
        Resource[] resources = new Resource[1];
        Properties prop = getAllKylinProperties();
        String propString = null;
        try (StringBuilderWriter writer = new StringBuilderWriter()) {
            prop.store(new PrintWriter(writer), "kylin properties");
            propString = writer.getBuilder().toString();
        }

        ByteArrayResource byteArrayResource = new ByteArrayResource(propString.getBytes(Charset.defaultCharset()));
        resources[0] = byteArrayResource;
        this.setFileEncoding(Charset.defaultCharset().toString());
        this.setLocations(resources);
    }

    private static void printUsage() {
        System.out.println("Usage: java org.apache.kylin.rest.security.PasswordPlaceholderConfigurer "
                + "<EncryptMethod> <your_password>");
        System.out.println("EncryptMethod: AES or BCrypt");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            printUsage();
            Unsafe.systemExit(1);
        }

        String encryptMethod = args[0];
        String passwordTxt = args[1];
        if ("AES".equalsIgnoreCase(encryptMethod)) {
            // for encrypt password like LDAP password
            System.out.println(encryptMethod + " encrypted password is: ");
            System.out.println(EncryptUtil.encrypt(passwordTxt));
        } else if ("BCrypt".equalsIgnoreCase(encryptMethod)) {
            // for encrypt the predefined user password, like ADMIN, MODELER.
            BCryptPasswordEncoder bCryptPasswordEncoder = new BCryptPasswordEncoder();
            System.out.println(encryptMethod + " encrypted password is: ");
            System.out.println(bCryptPasswordEncoder.encode(passwordTxt));
        } else {
            printUsage();
            Unsafe.systemExit(1);
        }
    }

    public Properties getAllKylinProperties() {
        // hack to get all config properties
        Properties allProps;
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            Method getAllMethod = KylinConfigBase.class.getDeclaredMethod("getAllProperties");
            Unsafe.changeAccessibleObject(getAllMethod, true);
            allProps = (Properties) getAllMethod.invoke(kylinConfig);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
        return allProps;
    }

    @Override
    protected String resolvePlaceholder(String placeholder, Properties props) {
        if (placeholder.toLowerCase(Locale.ROOT).contains("password") && !passwordWhiteList.contains(placeholder)) {
            try {
                return EncryptUtil.decrypt(props.getProperty(placeholder));
            } catch (Exception e) {
                throw new PasswordDecryptionException(String.format(Locale.ROOT,
                        "[%s] Encrypted configuration item decryption failed, please check for errors", placeholder),
                        e.getCause());
            }
        } else {
            return props.getProperty(placeholder);
        }
    }
}
