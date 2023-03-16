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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;

public class SizeConvertUtil {

    private static final BigDecimal ONE_KB = BigDecimal.valueOf(1024L);
    private static final BigDecimal ONE_MB = BigDecimal.valueOf(1024L * 1024);
    private static final BigDecimal ONE_GB = BigDecimal.valueOf(1024L * 1024 * 1024);
    private static final BigDecimal ONE_TB = BigDecimal.valueOf(1024L * 1024 * 1024 * 1024);

    private static final ImmutableMap<String, ByteUnit> byteSuffixes;

    private SizeConvertUtil() {
        throw new IllegalStateException("Wrong usage for utility class.");
    }

    public static String getReadableFileSize(long size) {
        if (size <= 0)
            return "0";
        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB", "PB", "EB" };
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#", DecimalFormatSymbols.getInstance(Locale.getDefault(Locale.Category.FORMAT)))
                .format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    public static long byteStringAsMb(String str) {
        return byteStringAs(str, ByteUnit.MiB);
    }

    public static long byteStringAs(String str, ByteUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
            Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);
            if (m.matches()) {
                long val = Long.parseLong(m.group(1));
                String suffix = m.group(2);
                if (suffix != null && !byteSuffixes.containsKey(suffix)) {
                    throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
                } else {
                    return unit.convertFrom(val, suffix != null ? (ByteUnit) byteSuffixes.get(suffix) : unit);
                }
            } else if (fractionMatcher.matches()) {
                throw new NumberFormatException(
                        "Fractional values are not supported. Input was: " + fractionMatcher.group(1));
            } else {
                throw new NumberFormatException("Failed to parse byte string: " + str);
            }
        } catch (NumberFormatException var8) {
            String byteError = "Size must be specified as bytes (b), kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). E.g. 50b, 100k, or 250m.";
            throw new NumberFormatException(byteError + "\n" + var8.getMessage());
        }
    }

    /**
     * calculate dataSize in front end at file src/filter/index.js
     * @param size
     * @param scale
     * @return
     */
    public static String byteCountToDisplaySize(BigDecimal size, int scale) {
        String displaySize;

        if (size.divide(ONE_TB, scale, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) >= 0) {
            displaySize = size.divide(ONE_TB, scale, RoundingMode.HALF_UP) + " TB";
        } else if (size.divide(ONE_GB, scale, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) >= 0) {
            displaySize = size.divide(ONE_GB, scale, RoundingMode.HALF_UP) + " GB";
        } else if (size.divide(ONE_MB, scale, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) >= 0) {
            displaySize = size.divide(ONE_MB, scale, RoundingMode.HALF_UP) + " MB";
        } else if (size.divide(ONE_KB, scale, RoundingMode.HALF_UP).compareTo(BigDecimal.ONE) >= 0) {
            displaySize = size.divide(ONE_KB, scale, RoundingMode.HALF_UP) + " KB";
        } else {
            displaySize = size + " B";
        }

        return displaySize;
    }

    public static String byteCountToDisplaySize(long size, int scale) {
        return byteCountToDisplaySize(BigDecimal.valueOf(size), scale);
    }

    public static String byteCountToDisplaySize(long size) {
        return byteCountToDisplaySize(size, 2);
    }

    static {
        byteSuffixes = ImmutableMap.<String, ByteUnit> builder().put("b", ByteUnit.BYTE).put("k", ByteUnit.KiB)
                .put("kb", ByteUnit.KiB).put("m", ByteUnit.MiB).put("mb", ByteUnit.MiB).put("g", ByteUnit.GiB)
                .put("gb", ByteUnit.GiB).put("t", ByteUnit.TiB).put("tb", ByteUnit.TiB).put("p", ByteUnit.PiB)
                .put("pb", ByteUnit.PiB).build();
    }

}
