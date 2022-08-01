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
import java.util.Properties;

import org.apache.kylin.rest.exception.PasswordDecryptionException;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
public class PasswordPlaceholderConfigurerTest {

    @Test
    public void getAllKylinProperties() throws IOException {
        PasswordPlaceholderConfigurer passwordPlaceholderConfigurer = new PasswordPlaceholderConfigurer();
        passwordPlaceholderConfigurer.getAllKylinProperties();
    }

    @Test
    public void resolvePlaceholder() throws IOException {
        String password = "123456";
        String encryptedPassword = EncryptUtil.encrypt(password);
        Properties properties = new Properties();
        properties.setProperty("kylin.security.ldap.connection-password", encryptedPassword);
        properties.setProperty("kylin.security.user-password-encoder", password);
        PasswordPlaceholderConfigurer passwordPlaceholderConfigurer = new PasswordPlaceholderConfigurer();
        Assertions.assertEquals(password,
                passwordPlaceholderConfigurer.resolvePlaceholder("kylin.security.user-password-encoder", properties));
        Assertions.assertEquals(password, passwordPlaceholderConfigurer
                .resolvePlaceholder("kylin.security.ldap.connection-password", properties));

        properties.setProperty("kylin.security.ldap.connection-password", password);
        Assertions.assertThrows(PasswordDecryptionException.class, () -> passwordPlaceholderConfigurer
                .resolvePlaceholder("kylin.security.ldap.connection-password", properties));
    }

    @Test
    public void main() {
        String[] args = new String[] { "AES", "PASSWORD" };
        PasswordPlaceholderConfigurer.main(args);
        args = new String[] { "BCrypt", "PASSWORD" };
        PasswordPlaceholderConfigurer.main(args);
    }
}
