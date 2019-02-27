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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StringUtilTest {
    @Test
    public void splitTest() {
        String normalText = "Try to make the code better";
        String[] expected = new String[] { "Try", "to", "make", "the", "code", "better" };
        Assert.assertArrayEquals(expected, StringUtil.split(normalText, " "));

        // case in http://errorprone.info/bugpattern/StringSplitter
        expected = new String[] { "" };
        Assert.assertArrayEquals(expected, StringUtil.split("", ":"));

        expected = new String[] { "", "" };
        Assert.assertArrayEquals(expected, StringUtil.split(":", ":"));

        expected = new String[] { "1", "2" };
        Assert.assertArrayEquals(expected, StringUtil.split("1<|>2", "<|>"));
    }

    @Test
    public void splitAndTrimTest() {
        String[] expected = new String[] { "foo", "bar" };
        Assert.assertArrayEquals(expected, StringUtil.splitAndTrim(" foo... bar. ", "."));
    }

    @Test
    public void splitByCommaTest() {
        String[] expected = new String[] { "Hello", "Kylin" };
        Assert.assertArrayEquals(expected, StringUtil.splitByComma("Hello,Kylin"));
    }

    @Test
    public void testJoin() {
        List<String> stringListNormal = Lists.newArrayList();
        List<String> stringListEmpty = Lists.newArrayList();

        stringListNormal.add("aaa");
        stringListNormal.add("bbb");
        stringListNormal.add("ccc");
        String joinedNormal = StringUtil.join(stringListNormal, ",");
        Assert.assertEquals("aaa,bbb,ccc", joinedNormal);

        stringListEmpty.add("");
        stringListEmpty.add("aa");
        stringListEmpty.add("");
        stringListEmpty.add("bb");
        stringListEmpty.add("");
        String joinedEmpty = StringUtil.join(stringListEmpty, ",");
        Assert.assertEquals(",aa,,bb,", joinedEmpty);
    }
}
