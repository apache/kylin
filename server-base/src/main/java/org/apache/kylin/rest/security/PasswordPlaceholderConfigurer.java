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
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.EncryptUtil;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

/**
 * @author xduo
 * 
 */
public class PasswordPlaceholderConfigurer extends PropertyPlaceholderConfigurer {

    /**
     * The PasswordPlaceholderConfigurer will read Kylin properties as the Spring resource
     */
    public PasswordPlaceholderConfigurer() throws IOException {
        Resource[] resources = new Resource[1];
        //Properties prop = KylinConfig.getKylinProperties();
        Properties prop = getAllKylinProperties();
        StringWriter writer = new StringWriter();
        prop.store(new PrintWriter(writer), "kylin properties");
        String propString = writer.getBuffer().toString();
        IOUtils.closeQuietly(writer);
        InputStream is = IOUtils.toInputStream(propString, Charset.defaultCharset());
        resources[0] = new InputStreamResource(is);
        this.setLocations(resources);
    }

    public Properties getAllKylinProperties() {
        // hack to get all config properties
        Properties allProps = null;
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            Method getAllMethod = KylinConfigBase.class.getDeclaredMethod("getAllProperties");
            getAllMethod.setAccessible(true);
            allProps = (Properties) getAllMethod.invoke(kylinConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return allProps;
    }

    protected String resolvePlaceholder(String placeholder, Properties props) {
        if (placeholder.toLowerCase(Locale.ROOT).contains("password")) {
            return EncryptUtil.decrypt(props.getProperty(placeholder));
        } else {
            return props.getProperty(placeholder);
        }
    }

    private static void printUsage() {
        System.out.println(
                "Usage: ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.rest.security.PasswordPlaceholderConfigurer <EncryptMethod> <your_password>");
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
            System.exit(1);
        }
    }
}
