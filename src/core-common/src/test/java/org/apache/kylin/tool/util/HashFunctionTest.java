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
package org.apache.kylin.tool.util;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HashFunctionTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testChecksum() throws IOException {
        String mainDir = temporaryFolder.getRoot() + "/testChecksum";
        File checkSumFile = new File(mainDir, "a.txt");
        // do not change the data.
        FileUtils.writeStringToFile(checkSumFile, "1111");

        byte[] md5 = HashFunction.MD5.checksum(checkSumFile);
        byte[] sha1 = HashFunction.SHA1.checksum(checkSumFile);
        byte[] sha256 = HashFunction.SHA256.checksum(checkSumFile);
        byte[] sha512 = HashFunction.SHA512.checksum(checkSumFile);

        Assert.assertEquals("B59C67BF196A4758191E42F76670CEBA", DatatypeConverter.printHexBinary(md5));
        Assert.assertEquals("011C945F30CE2CBAFC452F39840F025693339C42", DatatypeConverter.printHexBinary(sha1));
        Assert.assertEquals("0FFE1ABD1A08215353C233D6E009613E95EEC4253832A761AF28FF37AC5A150C",
                DatatypeConverter.printHexBinary(sha256));
        Assert.assertEquals(
                "33275A8AA48EA918BD53A9181AA975F15AB0D0645398F5918A006D08675C1CB27D5C645DBD084EEE56E675E25BA4019F2ECEA37CA9E2995B49FCB12C096A032E",
                DatatypeConverter.printHexBinary(sha512));
    }

}
