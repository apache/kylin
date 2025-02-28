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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CreateTableFromJsonTest {

    String path = "src/test/resources/table_cc_cleanup/metadata/TABLE_INFO";
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private PrintStream originPrintStream;
    private static final String REGEX = "util.CreateTableFromJson :";

    @BeforeEach
    void setUpStreams() {
        originPrintStream = System.out;
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    void cleanUpStreams() {
        System.setOut(originPrintStream);
    }

    @Test
    void test() throws IOException {
        CreateTableFromJson.main(new String[] { path });
        String result = outContent.toString();
        if (StringUtils.isBlank(result)) {
            Assertions.assertTrue(true);
        } else if ("true".equals(System.getProperty("junit.run.local"))) {
            // with -Djunit.run.local=true, this branch is more convenient for dev
            String[] splits = result.split("\n");
            for (String str : splits) {
                String s = str.split(REGEX)[1];
                originPrintStream.println(s);
            }
        } else {
            String[] splits = result.split("\n");
            Assertions.assertTrue(splits[0].contains("create database `CAP`;"));
            Assertions.assertTrue(splits[1].contains("use `CAP`;"));
        }
    }
}
