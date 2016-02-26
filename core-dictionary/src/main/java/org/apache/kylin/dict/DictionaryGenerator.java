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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.dimension.Dictionary;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.source.ReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @author yangli9
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DictionaryGenerator {

    private static final int DICT_MAX_CARDINALITY = getDictionaryMaxCardinality();

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGenerator.class);

    private static final String[] DATE_PATTERNS = new String[] { "yyyy-MM-dd", "yyyyMMdd" };

    private static int getDictionaryMaxCardinality() {
        try {
            return KylinConfig.getInstanceFromEnv().getDictionaryMaxCardinality();
        } catch (Throwable e) {
            return 5000000; // some test case does not KylinConfig setup properly
        }
    }

    public static Dictionary<String> buildDictionaryFromValueEnumerator(DataType dataType, IDictionaryValueEnumerator valueEnumerator) throws IOException {
        Preconditions.checkNotNull(dataType, "dataType cannot be null");
        Dictionary dict;
        int baseId = 0; // always 0 for now
        int nSamples = 5;
        ArrayList samples = new ArrayList();

        // build dict, case by data type
        if (dataType.isDateTimeFamily()) {
            if (dataType.isDate())
                dict = buildDateDict(valueEnumerator, baseId, nSamples, samples);
            else
                dict = new TimeStrDictionary(); // base ID is always 0
        } else if (dataType.isNumberFamily()) {
            dict = buildNumberDict(valueEnumerator, baseId, nSamples, samples);
        } else {
            dict = buildStringDict(valueEnumerator, baseId, nSamples, samples);
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

    public static Dictionary mergeDictionaries(DataType dataType, List<DictionaryInfo> sourceDicts) throws IOException {
        return buildDictionaryFromValueEnumerator(dataType, new MultipleDictionaryValueEnumerator(sourceDicts));
    }

    public static Dictionary<String> buildDictionary(DictionaryInfo info, ReadableTable inpTable) throws IOException {

        // currently all data types are casted to string to build dictionary
        // String dataType = info.getDataType();

        IDictionaryValueEnumerator columnValueEnumerator = null;
        try {
            logger.debug("Building dictionary object " + JsonUtil.writeValueAsString(info));

            columnValueEnumerator = new TableColumnValueEnumerator(inpTable.getReader(), info.getSourceColumnIndex());
            return buildDictionaryFromValueEnumerator(DataType.getType(info.getDataType()), columnValueEnumerator);
        } finally {
            if (columnValueEnumerator != null)
                columnValueEnumerator.close();
        }
    }

    private static Dictionary<String> buildDateDict(IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList samples) throws IOException {
        final int BAD_THRESHOLD = 0;
        String matchPattern = null;
        byte[] value;

        for (String ptn : DATE_PATTERNS) {
            matchPattern = ptn; // be optimistic
            int badCount = 0;
            SimpleDateFormat sdf = new SimpleDateFormat(ptn);
            while (valueEnumerator.moveNext()) {
                value = valueEnumerator.current();
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

    private static Dictionary buildStringDict(IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList samples) throws IOException {
        TrieDictionaryBuilder builder = new TrieDictionaryBuilder(new StringBytesConverter());
        byte[] value;
        while (valueEnumerator.moveNext()) {
            value = valueEnumerator.current();
            if (value == null)
                continue;
            String v = Bytes.toString(value);
            builder.addValue(v);
            if (samples.size() < nSamples && samples.contains(v) == false)
                samples.add(v);
        }
        return builder.build(baseId);
    }

    private static Dictionary buildNumberDict(IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList samples) throws IOException {
        NumberDictionaryBuilder builder = new NumberDictionaryBuilder(new StringBytesConverter());
        byte[] value;
        while (valueEnumerator.moveNext()) {
            value = valueEnumerator.current();
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
}
