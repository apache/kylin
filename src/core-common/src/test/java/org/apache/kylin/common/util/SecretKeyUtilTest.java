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

import java.io.File;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SecretKeyUtilTest extends NLocalFileMetadataTestCase {

    @Test
    public void testEncryptAndDecryptToken() throws Exception {
        String originToken = "12345_1583840099000";

        SecretKey secretKey = SecretKeyUtil.generateNewAESKey();
        byte[] encryptedToken = SecretKeyUtil.encryptToken(secretKey, originToken);

        String decryptedToken = SecretKeyUtil.decryptToken(secretKey, encryptedToken);
        Assert.assertEquals(originToken, decryptedToken);

        // create key from bytes
        byte[] keyBytes = secretKey.getEncoded();
        SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, "AES");
        String decryptedToken2 = SecretKeyUtil.decryptToken(secretKeySpec, encryptedToken);
        Assert.assertEquals(originToken, decryptedToken2);
    }

    @Test
    public void testEncryptAndDecryptByStrKey() throws Exception {

        String originToken = "12345_1583840099000";

        String key = "kylin_metadata";

        byte[] encryptedToken = SecretKeyUtil.encryptToken(key, originToken);

        String decryptedToken = SecretKeyUtil.decryptToken(key, encryptedToken);

        Assert.assertEquals(originToken, decryptedToken);
    }

    @Test
    public void testInitKGSecretKey(@TempDir File tempFolder) throws Exception {
        final String mainFolder = tempFolder.getAbsolutePath();
        FileUtils.forceMkdir(new File(mainFolder));

        String sourceValue = System.getenv("KYLIN_HOME");

        overwriteSystemProp("KYLIN_HOME", mainFolder);

        SecretKeyUtil.initKGSecretKey();

        Assert.assertEquals(16, FileUtils
                .readFileToByteArray(new File(mainFolder + '/' + SecretKeyUtil.KG_SECRET_KEY_FILE_NAME)).length);

        SecretKey secretKey = SecretKeyUtil.readKGSecretKeyFromFile();
        Assert.assertEquals(16, secretKey.getEncoded().length);

        if (null != sourceValue) {
            overwriteSystemProp("KYLIN_HOME", sourceValue);
        }
        Assert.assertEquals(sourceValue, System.getenv("KYLIN_HOME"));
    }
}
