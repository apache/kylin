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

package org.apache.kylin.tool.general;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import lombok.val;

public class CryptToolTest {
    private final PrintStream systemOut = System.out;
    private ByteArrayOutputStream output;

    @Before
    public void setup() throws Exception {
        output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output, false, Charset.defaultCharset().name()));
    }

    @After
    public void cleanup() {
        System.setOut(systemOut);
    }

    @Test
    public void testAES() throws UnsupportedEncodingException {
        val tool = new CryptTool();
        tool.execute(new String[] { "-e", "AES", "-s", "secret" });
        Assert.assertEquals("x0vzfDV1ZQ7ME4M/dO4bCw==\n", output.toString(Charset.defaultCharset().name()));
    }

    @Test
    public void testBCrypt() throws UnsupportedEncodingException {
        val tool = new CryptTool();
        tool.execute(new String[] { "-e", "BCrypt", "-s", "secret" });
        Assert.assertTrue(new BCryptPasswordEncoder().matches("secret",
                StringUtils.chomp(output.toString(Charset.defaultCharset().name()))));
    }
}
