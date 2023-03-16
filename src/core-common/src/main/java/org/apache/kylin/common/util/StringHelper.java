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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class StringHelper {

    public static final char QUOTE = '\'';
    public static final char DOUBLE_QUOTE = '"';
    public static final char BACKTICK = '`';

    private StringHelper() {
    }

    public static String[] filterSystemArgs(String[] args) {
        List<String> whatsLeft = Lists.newArrayList();
        for (String a : args) {
            if (a.startsWith("-D")) {
                String key;
                String value;
                int cut = a.indexOf('=');
                if (cut < 0) {
                    key = a.substring(2);
                    value = "";
                } else {
                    key = a.substring(2, cut);
                    value = a.substring(cut + 1);
                }
                Unsafe.setProperty(key, value);
            } else {
                whatsLeft.add(a);
            }
        }
        return whatsLeft.toArray(new String[0]);
    }

    /**
     * Returns a substring by removing the specified suffix. If the given string
     * does not ends with the suffix, the string is returned without change.
     *
     * @param str
     * @param suffix
     * @return
     */
    public static String trimSuffix(String str, String suffix) {
        if (str.endsWith(suffix)) {
            return str.substring(0, str.length() - suffix.length());
        } else {
            return str;
        }
    }

    public static String join(Iterable<String> parts, String separator) {
        StringBuilder buf = new StringBuilder();
        for (String p : parts) {
            if (buf.length() > 0)
                buf.append(separator);
            buf.append(p);
        }
        return buf.toString();
    }

    public static void toUpperCaseArray(String[] source, String[] target) {
        if (source != null) {
            for (int i = 0; i < source.length; i++) {
                if (source[i] != null) {
                    target[i] = StringUtils.upperCase(source[i]);
                }
            }
        }
    }

    public static String noBlank(String str, String dft) {
        return StringUtils.isBlank(str) ? dft : str;
    }

    public static String dropSuffix(String str, String suffix) {
        if (str.endsWith(suffix))
            return str.substring(0, str.length() - suffix.length());
        else
            return str;
    }

    public static String dropFirstSuffix(String str, String suffix) {
        if (str.contains(suffix))
            return str.substring(0, str.indexOf(suffix));
        else
            return str;
    }

    public static String min(Collection<String> strs) {
        String min = null;
        for (String s : strs) {
            if (min == null || min.compareTo(s) > 0)
                min = s;
        }
        return min;
    }

    public static String max(Collection<String> strs) {
        String max = null;
        for (String s : strs) {
            if (max == null || max.compareTo(s) < 0)
                max = s;
        }
        return max;
    }

    public static String min(String s1, String s2) {
        if (s1 == null)
            return s2;
        else if (s2 == null)
            return s1;
        else
            return s1.compareTo(s2) < 0 ? s1 : s2;
    }

    public static String max(String s1, String s2) {
        if (s1 == null)
            return s2;
        else if (s2 == null)
            return s1;
        else
            return s1.compareTo(s2) > 0 ? s1 : s2;
    }

    public static String[] subArray(String[] array, int start, int endExclusive) {
        if (start < 0 || start > endExclusive || endExclusive > array.length)
            throw new IllegalArgumentException();
        String[] result = new String[endExclusive - start];
        System.arraycopy(array, start, result, 0, endExclusive - start);
        return result;
    }

    public static void appendWithSeparator(StringBuilder src, String append) {
        if (src == null) {
            throw new IllegalArgumentException();
        }
        if (src.length() > 0 && !src.toString().endsWith(",")) {
            src.append(",");
        }

        if (!StringUtils.isBlank(append)) {
            src.append(append);
        }
    }

    public static String[] splitAndTrim(String str, String splitBy) {
        String[] split = str.split(splitBy);
        ArrayList<String> r = new ArrayList<>(split.length);
        for (String s : split) {
            s = s.trim();
            if (!s.isEmpty())
                r.add(s);
        }
        return r.toArray(new String[0]);
    }

    public static boolean equals(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }

    public static boolean validateNumber(String s) {
        return Pattern.compile("^(0|[1-9]\\d*)$").matcher(s).matches();
    }

    public static boolean validateBoolean(String s) {
        return "true".equals(s) || "false".equals(s);
    }

    public static String[] split(String str, String splitBy) {
        return str.split(splitBy);
    }

    public static String backtickToDoubleQuote(String expression) {
        return convert(expression, StringHelper.BACKTICK, StringHelper.DOUBLE_QUOTE);
    }

    public static String doubleQuoteToBacktick(String expression) {
        return convert(expression, StringHelper.DOUBLE_QUOTE, StringHelper.BACKTICK);
    }

    private static String convert(String expression, char srcQuote, char targetQuote) {
        char[] chars = expression.toCharArray();
        List<Integer> indexList = StringHelper.findQuoteIndexes(srcQuote, expression);
        for (Integer integer : indexList) {
            chars[integer] = targetQuote;
        }
        return new String(chars);
    }

    public static String backtickQuote(String identifier) {
        String str = StringUtils.remove(identifier, StringHelper.BACKTICK);
        return StringHelper.BACKTICK + str + StringHelper.BACKTICK;
    }

    public static String doubleQuote(String identifier) {
        String str = StringUtils.remove(identifier, StringHelper.DOUBLE_QUOTE);
        return StringHelper.DOUBLE_QUOTE + str + StringHelper.DOUBLE_QUOTE;
    }

    /**
     * Search identifier quotes in the sql string.
     * @param key the char to search
     * @param str the input string
     * @return index list of {@code key}
     */
    public static List<Integer> findQuoteIndexes(char key, String str) {
        Preconditions.checkState(key == BACKTICK || key == DOUBLE_QUOTE);
        char[] chars = str.toCharArray();
        List<Integer> indexList = Lists.newArrayList();
        List<Pair<Integer, Character>> toMatchTokens = Lists.newArrayList();
        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];
            if (toMatchTokens.isEmpty()) {
                if (ch == key || ch == QUOTE) {
                    toMatchTokens.add(new Pair<>(i, ch));
                }
                continue;
            }

            // The toMatchTokens is not empty, try to collect
            Character ex = toMatchTokens.get(toMatchTokens.size() - 1).getSecond();
            if (ch == ex && ch == key) {
                toMatchTokens.add(new Pair<>(i, ex));
                Preconditions.checkState(toMatchTokens.size() == 2);
                indexList.add(toMatchTokens.get(0).getFirst());
                indexList.add(toMatchTokens.get(1).getFirst());
                toMatchTokens.clear();
            } else if (ch == ex && ch == QUOTE) {
                // There are two kind of single quote in the char array.
                // One kind has two successive single quote '', we need to clear the toMatchTokens.
                // Another kind has a form of \', just ignore it and go on match the next char.
                Preconditions.checkState(toMatchTokens.size() == 1);
                if (chars[i - 1] != '\\') {
                    toMatchTokens.clear();
                }
            }
        }
        Preconditions.checkState(indexList.size() % 2 == 0);
        return indexList;
    }
}
