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

package org.apache.kylin.job;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Joined Formatter for JoinedFlatTable
 */

public class JoinedFormatter {

    private static String REG_SEPARATOR = "\\$\\{(?<KEY>.*?)\\}";
    private static Pattern REG_PATTERN = Pattern.compile(REG_SEPARATOR,
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    //
    public static String START_DATE = "START_DATE";
    public static String END_DATE = "END_DATE";
    public static String ENV_KEY = "KEY";
    //
    private Map<String, Object> mapEnv = new HashMap<>();

    public JoinedFormatter() {
    }

    public JoinedFormatter(IJoinedFlatTableDesc flatDesc) {
        setDateEnv(flatDesc);
    }

    public void setDateEnv(IJoinedFlatTableDesc flatDesc) {
        DataModelDesc model = flatDesc.getDataModel();
        PartitionDesc partDesc = model.getPartitionDesc();
        SegmentRange segRange = flatDesc.getSegRange();
        long startInclusive = (Long) segRange.start.v;
        long endExclusive = (Long) segRange.end.v;
        //
        String startDate = "";
        String endDate = "";
        String partitionColumnDateFormat = partDesc.getPartitionDateFormat();
        if (StringUtils.isBlank(partitionColumnDateFormat)) {
            startDate = String.valueOf(startInclusive);
            endDate = String.valueOf(endExclusive);
        } else {
            startDate = DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat);
            endDate = DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat);
        }
        setKeyValue(START_DATE, startDate);
        setKeyValue(END_DATE, endDate);
    }

    public Object getValue(String key) {
        String fmtKey = StringUtils.trimToEmpty(key).toUpperCase(Locale.ROOT);
        Object value = mapEnv.get(fmtKey);
        return value == null ? "" : value;
    }

    public String formatSentense(String sentense) {
        String[] cArray = REG_PATTERN.split(sentense);
        StringBuilder sbr = new StringBuilder();
        List<String> keys = getKeys(sentense);
        int length = Math.max(cArray.length, keys.size());
        for (int i = 0; i < length; i++) {
            if (i < cArray.length) {
                sbr.append(cArray[i]);
            }
            if (i < keys.size()) {
                sbr.append(getValue(keys.get(i)));
            }
        }
        return sbr.toString();
    }

    public List<String> getKeys(String condition) {
        List<String> keys = new ArrayList<>();
        Matcher matcher = REG_PATTERN.matcher(condition);
        while (matcher.find()) {
            keys.add(matcher.group(ENV_KEY));
        }
        return keys;
    }

    public void setKeyValue(String key, Object value) {
        String fmtKey = StringUtils.trimToEmpty(key).toUpperCase(Locale.ROOT);
        mapEnv.put(fmtKey, value);
    }

    public void setStartDate(String dateStr) {
        setKeyValue(START_DATE, dateStr);
    }

    public void setEndDate(String dateStr) {
        setKeyValue(END_DATE, dateStr);
    }

    public void printEnv() {
        System.out.println(mapEnv);
    }
}
