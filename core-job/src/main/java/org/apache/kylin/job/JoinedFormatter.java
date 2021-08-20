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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;

/**
 * Joined Formatter for JoinedFlatTable
 */

public class JoinedFormatter {

    private static final String REG_SEPARATOR = "\\$\\{(?<KEY>.*?)\\}";
    private static final Pattern REG_PATTERN = Pattern.compile(REG_SEPARATOR,
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    //
    private static final String START_DATE = "START_DATE";
    private static final String END_DATE = "END_DATE";
    private static final String ENV_KEY = "KEY";
    //
    private Map<String, Object> mapEnv = new HashMap<>();

    JoinedFormatter() {
    }

    JoinedFormatter(IJoinedFlatTableDesc flatDesc) {
        setDateEnv(flatDesc);
    }

    public JoinedFormatter(Boolean validateModel) {
        // for validate model filter condition
        String start = "20190710";
        String end = "20190711";
        setKeyValue(START_DATE, start);
        setKeyValue(END_DATE, end);
    }

    private void setDateEnv(IJoinedFlatTableDesc flatDesc) {
        DataModelDesc model = flatDesc.getDataModel();
        PartitionDesc partDesc = model.getPartitionDesc();
        SegmentRange segRange = flatDesc.getSegRange();
        long startInclusive = (Long) segRange.start.v;
        long endExclusive = (Long) segRange.end.v;
        //
        String startDate = "";
        String endDate = "";
        String partitionColumnDateFormat = partDesc.getPartitionDateFormat();
        if (partDesc.getPartitionTimeColumn() == null && partDesc.getPartitionDateColumn() == null) {
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

    public String formatSentence(String sentence) {
        String[] cArray = REG_PATTERN.split(sentence);
        StringBuilder sbr = new StringBuilder();
        List<String> keys = getKeys(sentence);
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

    private List<String> getKeys(String condition) {
        List<String> keys = new ArrayList<>();
        Matcher matcher = REG_PATTERN.matcher(condition);
        while (matcher.find()) {
            keys.add(matcher.group(ENV_KEY));
        }
        return keys;
    }

    private void setKeyValue(String key, Object value) {
        String fmtKey = StringUtils.trimToEmpty(key).toUpperCase(Locale.ROOT);
        mapEnv.put(fmtKey, value);
    }

    void setStartDate(String dateStr) {
        setKeyValue(START_DATE, dateStr);
    }

    void setEndDate(String dateStr) {
        setKeyValue(END_DATE, dateStr);
    }

    void printEnv() {
        System.out.println(mapEnv);
    }
}
