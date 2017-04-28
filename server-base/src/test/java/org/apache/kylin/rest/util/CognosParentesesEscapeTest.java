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
package org.apache.kylin.rest.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class CognosParentesesEscapeTest {

    @Test
    public void basicTest() {
        CognosParenthesesEscape escape = new CognosParenthesesEscape();
        String data = "((a left outer join b on a.x1 = b.y1 and a.x2=b.y2 and   a.x3= b.y3) inner join c as cc on a.x1=cc.z1 ) join d dd on a.x1=d.w1 and a.x2 =d.w2 ";
        String expected = "a left outer join b on a.x1 = b.y1 and a.x2=b.y2 and   a.x3= b.y3 inner join c as cc on a.x1=cc.z1  join d dd on a.x1=d.w1 and a.x2 =d.w2 ";
        String transformed = escape.transform(data);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void advancedTest() throws IOException {
        CognosParenthesesEscape escape = new CognosParenthesesEscape();
        String query = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query01.sql"), Charset.defaultCharset());
        String expected = FileUtils.readFileToString(new File("src/test/resources/query/cognos/query01.sql.expected"), Charset.defaultCharset());
        String transformed = escape.transform(query);
        //System.out.println(transformed);
        Assert.assertEquals(expected, transformed);
    }

    @Test
    public void proguardTest() throws IOException {
        CognosParenthesesEscape escape = new CognosParenthesesEscape();
        Collection<File> files = FileUtils.listFiles(new File("../kylin-it/src/test/resources"), new String[] { "sql" }, true);
        for (File f : files) {
            System.out.println("checking " + f.getAbsolutePath());
            String query = FileUtils.readFileToString(f, Charset.defaultCharset());
            String transformed = escape.transform(query);
            Assert.assertEquals(query, transformed);
        }
    }
}
