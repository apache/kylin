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
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

/**
 */
public class StringUtil {

    public static String[] EMPTY_ARRAY = new String[0];

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
                    target[i] = source[i].toUpperCase(Locale.ROOT);
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
        return r.toArray(new String[r.size()]);
    }

    // calculating length in UTF-8 of Java String without actually encoding it
    public static int utf8Length(CharSequence sequence) {
        int count = 0;
        for (int i = 0, len = sequence.length(); i < len; i++) {
            char ch = sequence.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
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

}
