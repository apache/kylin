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

package org.apache.kylin.dict;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.ReadableTable.TableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author yangli9
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DictionaryGenerator {

    private static final int DICT_MAX_CARDINALITY = getDictionaryMaxCardinality();

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGenerator.class);

    private static final String[] DATE_PATTERNS = new String[] { "yyyy-MM-dd" };

    private static int getDictionaryMaxCardinality() {
        try {
            return KylinConfig.getInstanceFromEnv().getDictionaryMaxCardinality();
        } catch (Throwable e) {
            return 2000000; // some test case does not KylinConfig setup properly
        }
    }

    public static Dictionary<?> buildDictionaryFromValueList(DataType dataType, Iterable<byte[]> values) {
        Preconditions.checkNotNull(dataType, "dataType cannot be null");
        Dictionary dict;
        int baseId = 0; // always 0 for now
        int nSamples = 5;
        ArrayList samples = new ArrayList();

        // build dict, case by data type
        if (dataType.isDateTimeFamily()) {
            if (dataType.isDate())
                dict = buildDateDict(values, baseId, nSamples, samples);
            else
                dict = new TimeStrDictionary(); // base ID is always 0
        } else if (dataType.isNumberFamily()) {
            dict = buildNumberDict(values, baseId, nSamples, samples);
        } else {
            dict = buildStringDict(values, baseId, nSamples, samples);
        }

        // log a few samples
        StringBuilder buf = new StringBuilder();
        for (Object s : samples) {
            if (buf.length() > 0) {
                buf.append(", ");
            }
            buf.append(s.toString()).append("=>").append(dict.getIdFromValue(s));
        }
        logger.debug("Dictionary value samples: " + buf.toString());
        logger.debug("Dictionary cardinality " + dict.getSize());
        if (dict instanceof TrieDictionary && dict.getSize() > DICT_MAX_CARDINALITY) {
            throw new IllegalArgumentException("Too high cardinality is not suitable for dictionary -- cardinality: " + dict.getSize());
        }
        return dict;
    }

    public static Dictionary mergeDictionaries(DataType dataType, List<DictionaryInfo> sourceDicts) {

        HashSet<byte[]> dedup = new HashSet<byte[]>();

        for (DictionaryInfo info : sourceDicts) {
            Dictionary<?> dict = info.getDictionaryObject();
            int minkey = dict.getMinId();
            int maxkey = dict.getMaxId();
            byte[] buffer = new byte[dict.getSizeOfValue()];
            for (int i = minkey; i <= maxkey; ++i) {
                int size = dict.getValueBytesFromId(i, buffer, 0);
                dedup.add(Bytes.copy(buffer, 0, size));
            }
        }

        List<byte[]> valueList = new ArrayList<byte[]>();
        valueList.addAll(dedup);

        Dictionary<?> dict = buildDictionaryFromValueList(dataType, valueList);
        return dict;
    }

    public static Dictionary<?> buildDictionary(DictionaryInfo info, ReadableTable inpTable) throws IOException {

        // currently all data types are casted to string to build dictionary
        // String dataType = info.getDataType();

        logger.debug("Building dictionary object " + JsonUtil.writeValueAsString(info));

        ArrayList<byte[]> values = loadColumnValues(inpTable, info.getSourceColumnIndex());

        Dictionary<?> dict = buildDictionaryFromValueList(DataType.getInstance(info.getDataType()), values);

        return dict;
    }

    private static Dictionary buildDateDict(Iterable<byte[]> values, int baseId, int nSamples, ArrayList samples) {
        final int BAD_THRESHOLD = 0;
        String matchPattern = null;

        for (String ptn : DATE_PATTERNS) {
            matchPattern = ptn; // be optimistic
            int badCount = 0;
            SimpleDateFormat sdf = new SimpleDateFormat(ptn);
            for (byte[] value : values) {
                if (value == null || value.length == 0)
                    continue;

                String str = Bytes.toString(value);
                try {
                    sdf.parse(str);
                    if (samples.size() < nSamples && samples.contains(str) == false)
                        samples.add(str);
                } catch (ParseException e) {
                    logger.info("Unrecognized date value: " + str);
                    badCount++;
                    if (badCount > BAD_THRESHOLD) {
                        matchPattern = null;
                        break;
                    }
                }
            }
            if (matchPattern != null) {
                return new DateStrDictionary(matchPattern, baseId);
            }
        }

        throw new IllegalStateException("Unrecognized datetime value");
    }

    private static Dictionary buildStringDict(Iterable<byte[]> values, int baseId, int nSamples, ArrayList samples) {
        TrieDictionaryBuilder builder = new TrieDictionaryBuilder(new StringBytesConverter());
        for (byte[] value : values) {
            if (value == null)
                continue;
            String v = Bytes.toString(value);
            builder.addValue(v);
            if (samples.size() < nSamples && samples.contains(v) == false)
                samples.add(v);
        }
        return builder.build(baseId);
    }

    private static Dictionary buildNumberDict(Iterable<byte[]> values, int baseId, int nSamples, ArrayList samples) {
        NumberDictionaryBuilder builder = new NumberDictionaryBuilder(new StringBytesConverter());
        for (byte[] value : values) {
            if (value == null)
                continue;
            String v = Bytes.toString(value);
            if (StringUtils.isBlank(v)) // empty string is null for numbers
                continue;

            builder.addValue(v);
            if (samples.size() < nSamples && samples.contains(v) == false)
                samples.add(v);
        }
        return builder.build(baseId);
    }

    static ArrayList<byte[]> loadColumnValues(ReadableTable inpTable, int colIndex) throws IOException {

        TableReader reader = inpTable.getReader();

        try {
            ArrayList<byte[]> result = Lists.newArrayList();
            HashSet<String> dedup = new HashSet<String>();

            while (reader.next()) {
                String[] split = reader.getRow();

                String colValue;
                // special single column file, e.g. common_indicator.txt
                if (split.length == 1) {
                    colValue = split[0];
                }
                // normal case
                else {
                    if (split.length <= colIndex) {
                        throw new ArrayIndexOutOfBoundsException("Column no. " + colIndex + " not found, line split is " + Arrays.asList(split));
                    }
                    colValue = split[colIndex];
                }

                if (dedup.contains(colValue) == false) {
                    dedup.add(colValue);
                    result.add(Bytes.toBytes(colValue));
                }
            }
            return result;

        } finally {
            reader.close();
        }
    }

}
