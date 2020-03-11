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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


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
        assertEquals("aaa,bbb,ccc", joinedNormal);

        stringListEmpty.add("");
        stringListEmpty.add("aa");
        stringListEmpty.add("");
        stringListEmpty.add("bb");
        stringListEmpty.add("");
        String joinedEmpty = StringUtil.join(stringListEmpty, ",");
        assertEquals(",aa,,bb,", joinedEmpty);
    }

  @Test
  public void testDropSuffixWithNonEmptyString() {
      assertEquals("", StringUtil.dropSuffix("Oo}T^z88/U", "Oo}T^z88/U"));
  }

  @Test
  public void testDropSuffixWithEmptyString() {
      String string = StringUtil.dropSuffix("", "^Fahs");

      assertEquals("", string);
  }

  @Test
  public void testNoBlankWithNull() {
      assertEquals("%W=U~)O|0'#?,zA", StringUtil.noBlank(null, "%W=U~)O|0'#?,zA"));
  }

  @Test
  public void testNoBlankWithNonEmptyString() {
      assertEquals("N(sg", StringUtil.noBlank("N(sg", "H=!Cp(Ed5gral0qzo"));
  }

  @Test
  public void testToUpperCaseArrayWithNonEmptyArray() {
      String[] stringArray = new String[7];
      stringArray[0] = "org.apache.kylin.common.util.StringUtil";
      StringUtil.toUpperCaseArray(stringArray, stringArray);

      assertEquals(7, stringArray.length);
      assertEquals("[ORG.APACHE.KYLIN.COMMON.UTIL.STRINGUTIL, null, null, null, null, null, null]",
              Arrays.asList(stringArray).toString()
      );
  }

  @Test
  public void testJoinReturningNonEmptyString() {
      List<String> arrayList = new ArrayList<String>();
      LinkedHashSet<String> linkedHashSet = new LinkedHashSet<String>(arrayList);
      linkedHashSet.add(")'<Mw@ZR0IYF_l%*>");
      linkedHashSet.add(null);
      String resultString = StringUtil.join(linkedHashSet, null);

      assertNotNull(resultString);
      assertEquals(")'<Mw@ZR0IYF_l%*>", resultString);

  }

  @Test
  public void testJoinOne() {
      Vector<String> vector = new Vector<>();
      vector.add(null);
      String resultString = StringUtil.join(vector, "PB");

      assertNotNull(resultString);
      assertEquals("", resultString);
  }

  @Test
  public void testJoinTwo() {
      Set<String> set = Locale.CHINESE.getUnicodeLocaleAttributes();
      String resultString = StringUtil.join(set, "Op");

      assertTrue(set.isEmpty());
      assertEquals(0, set.size());

      assertNotNull(resultString);
      assertEquals("", resultString);
  }

  @Test
  public void testJoinReturningNull() {
      String string = StringUtil.join((Iterable<String>) null, ")Y>1v&V0GU6a");

      assertNull(string);
  }

  @Test
  public void testTrimSuffixWithEmptyString() {
      String string = StringUtil.trimSuffix(" 8VKQ&I*pSVr", "");

      assertEquals(" 8VKQ&I*pSVr", string);
  }

  @Test
  public void testTrimSuffixWithNonEmptyString() {
      String string = StringUtil.trimSuffix(",", "5I;.t0F*5HV4");

      assertEquals(",", string);
  }

  @Test
  public void testFilterSystemArgsThrowsIllegalArgumentException() {
      String[] stringArray = new String[4];
      stringArray[0] = "-D";
      try {
        StringUtil.filterSystemArgs(stringArray);
        fail("Expecting exception: IllegalArgumentException");

      } catch(IllegalArgumentException e) {
      }
  }

  @Test
  public void testFilterSystemArgs() {
      String[] stringArray = new String[1];
      stringArray[0] = "J";
      String[] stringArrayTwo = StringUtil.filterSystemArgs(stringArray);

      assertFalse(stringArrayTwo.equals(stringArray));
      assertEquals(1, stringArrayTwo.length);
      assertEquals("J", stringArrayTwo[0]);
  }

}
