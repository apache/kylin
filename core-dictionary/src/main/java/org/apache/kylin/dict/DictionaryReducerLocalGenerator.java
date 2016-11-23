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

import com.google.common.base.Preconditions;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.datatype.DataType;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by xiefan on 16-11-16.
 *
 * TODO:sample,mergeDict
 */
public class DictionaryReducerLocalGenerator {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DictionaryReducerLocalGenerator.class);

    private static final String[] DATE_PATTERNS = new String[] { "yyyy-MM-dd", "yyyyMMdd" };

    public static IDictionaryReducerLocalBuilder getBuilder(DataType dataType) {
        Preconditions.checkNotNull(dataType, "dataType cannot be null");

        IDictionaryReducerLocalBuilder builder;
        if (dataType.isDateTimeFamily()) {
            if (dataType.isDate())
                builder = new DateDictBuilder();
            else
                builder = new TimeDictBuilder();
        } else if (dataType.isNumberFamily()) {
            builder = new NumberDictBuilder(0);
        } else {
            builder = new StringDictBuilder(0);
        }
        return builder;
    }

    private static class DateDictBuilder implements IDictionaryReducerLocalBuilder {

        private static final String[] DATE_PATTERNS = new String[] { "yyyy-MM-dd", "yyyyMMdd" };

        private String matchPattern = null;

        private boolean isRecognizeFormat = false;

        private SimpleDateFormat sdf;

        @Override
        public Dictionary<String> build(int baseId) throws Exception {
            if (isRecognizeFormat) {
                return new DateStrDictionary(matchPattern, baseId);
            } else {
                throw new IllegalStateException("Date format not match");
            }
        }

        @Override
        public void addValue(String value) throws Exception {
            if (matchPattern == null) { //init match pattern
                for (String ptn : DATE_PATTERNS) {
                    matchPattern = ptn;
                    SimpleDateFormat sdf = new SimpleDateFormat(ptn);
                    try {
                        sdf.parse(value);
                        isRecognizeFormat = true;
                        break;
                    } catch (ParseException e) {

                    }
                }
                sdf = new SimpleDateFormat(matchPattern);
            }
            if (!isRecognizeFormat) {
                throw new IllegalStateException("Date format not match");
            }
            try {
                sdf.parse(value);
            } catch (ParseException e) {
                isRecognizeFormat = false;
                logger.info("Unrecognized date value: " + value);
            }
        }

    }

    private static class TimeDictBuilder implements IDictionaryReducerLocalBuilder {

        @Override
        public Dictionary<String> build(int baseId) {
            return new TimeStrDictionary();
        }

        @Override
        public void addValue(String value) {

        }

    }

    private static class StringDictBuilder implements IDictionaryReducerLocalBuilder {

        private TrieDictionaryForestBuilder<String> builder;

        public StringDictBuilder(int baseId) {
            builder = new TrieDictionaryForestBuilder<String>(new StringBytesConverter(), 0);
        }

        @Override
        public Dictionary<String> build(int baseId) {
            return builder.build();
        }

        @Override
        public void addValue(String value) {
            builder.addValue(value);
        }

    }

    public static class NumberDictBuilder implements IDictionaryReducerLocalBuilder {

        private NumberDictionaryForestBuilder builder;

        public NumberDictBuilder(int baseId) {
            builder = new NumberDictionaryForestBuilder(baseId);
        }

        @Override
        public Dictionary<String> build(int baseId) {
            return builder.build();
        }

        @Override
        public void addValue(String value) {
            builder.addValue(value);
        }

    }
}
