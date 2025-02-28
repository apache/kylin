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
package org.apache.kylin.hive.serde2.lazy;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDateObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.TimestampParser;
import org.apache.kylin.common.exception.KylinRuntimeException;

/**
 * This class contains ALL of our enhancement to Hive's LazySimpleSerDe,
 * for <a href="https://github.com/Kyligence/Diting/blob/release-2.0.0/insight-core/src/main/java/org/apache/hadoop/hive/serde2/lazy/LazyQuoteAwareSerDe.java">
 * a copy of this file</a> is placed into project Diting, module insight-core, to
 * ensure the consistent behavior of uploading and querying a CSV file.
 * <br/>
 * One big file is easier to copy over.
 * <br/>
 * TEST PATH (from UT to IT):
 * - LazyQuoteAwareSerDeTest
 * - CsvLocalInferUtilTest (in Diting)
 * - HiveSoftAffinityAndLocalCacheTest (in KAP)
 * - in Zen GUI, upload a CSV, make a metric, and query it
 */

@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES, serdeConstants.FIELD_DELIM,
        serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM, serdeConstants.SERIALIZATION_FORMAT,
        serdeConstants.SERIALIZATION_NULL_FORMAT, serdeConstants.SERIALIZATION_ESCAPE_CRLF,
        serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST, serdeConstants.ESCAPE_CHAR, serdeConstants.QUOTE_CHAR,
        serdeConstants.SERIALIZATION_ENCODING, LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS,
        LazySerDeParameters.SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS })
public class LazyQuoteAwareSerDe extends LazySimpleSerDe {

    public static List<String[]> parseCsvLines(List<String> lines) {
        return parseCsvLines(lines, ",", "\\", "\"");
    }

    public static List<String[]> parseCsvLines(List<String> lines, String separator, String escape, String quote) {
        if (lines.isEmpty()) {
            return Collections.emptyList();
        }

        int colCountEst = lines.stream().map(l -> l.chars().filter(c -> c == separator.charAt(0)).count() + 1)
                .max(Long::compare).orElse(0L).intValue();

        try {
            Properties tbl = new Properties();
            tbl.put("field.delim", separator);
            tbl.put("escape.delim", escape);
            tbl.put("quote.delim", quote);
            tbl.put("columns",
                    IntStream.range(0, colCountEst).boxed().map(i -> "col" + i).collect(Collectors.joining(",")));
            tbl.put("columns.types", StringUtils.repeat("string", ":", colCountEst));

            LazyQuoteAwareSerDe serde = new LazyQuoteAwareSerDe();
            serde.initialize(new Configuration(), tbl);
            StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
            List<? extends StructField> fields = soi.getAllStructFieldRefs();

            List<String[]> ret = new ArrayList<>();
            int nonNullColLen = 0;
            for (String line : lines) {
                LazyStruct struct = (LazyStruct) serde.deserialize(new Text(line));
                String[] row = new String[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    //noinspection unchecked
                    LazyPrimitive<ObjectInspector, Writable> val = (LazyPrimitive<ObjectInspector, Writable>) soi
                            .getStructFieldData(struct, fields.get(i));
                    row[i] = val == null ? null : val.getWritableObject().toString();
                    if (val != null) {
                        nonNullColLen = Math.max(nonNullColLen, i + 1);
                    }
                }
                ret.add(row);
            }

            // cut off tailing all-null columns
            if (nonNullColLen < fields.size()) {
                for (int i = 0; i < ret.size(); i++) {
                    String[] row = ret.get(i);
                    ret.set(i, Arrays.copyOf(row, nonNullColLen));
                }
            }

            return ret;

        } catch (Exception ex) {
            throw new KylinRuntimeException(ex);
        }
    }

    // =============================================

    private QuoteAwareStruct myStruct;

    public LazyQuoteAwareSerDe() throws SerDeException {
        // nothing to do
    }

    @Override
    public void initialize(Configuration job, Properties tbl) throws SerDeException {
        super.initialize(job, tbl);

        byte quote = '"';
        if (tbl.containsKey(serdeConstants.QUOTE_CHAR)) {
            quote = (byte) tbl.getProperty(serdeConstants.QUOTE_CHAR).charAt(0);
        }

        boolean numberForceRead = tbl.containsKey("number.force") && !"false".equals(tbl.getProperty("number.force"));

        // overwrite the LazyStruct field to be QuoteAwareStruct
        myStruct = new QuoteAwareStruct((LazySimpleStructObjectInspector) getObjectInspector(), quote, numberForceRead);

        try {
            FieldUtils.writeField(this, "cachedLazyStruct", myStruct, true);
        } catch (Exception e) {
            throw new KylinRuntimeException(e);
        }

    }

    public boolean hasExtraFieldWarn() {
        return myStruct.selfExtraFieldWarned;
    }

    public boolean hasMissingFieldWarn() {
        return myStruct.selfMissingFieldWarned;
    }

    public static class QuoteAwareStruct extends LazyStruct {
        private final boolean quoteEnabled;
        private final byte quote;
        private final boolean numberForceRead;
        private boolean[] isFieldQuoted;

        // copied from LazyStruct
        boolean selfParsed;
        int[] selfStartPosition;
        LazyObjectBase[] selfFields;
        boolean[] selfFieldInited;
        boolean selfMissingFieldWarned = false;
        boolean selfExtraFieldWarned = false;

        public QuoteAwareStruct(LazySimpleStructObjectInspector oi, Byte quote, boolean numberForceRead) {
            super(oi);
            this.quoteEnabled = quote != null;
            this.quote = quote == null ? 0 : quote;
            this.numberForceRead = numberForceRead;
        }

        /*
         * Called only once for each field.
         */
        @Override
        protected LazyObjectBase createLazyField(int fieldId, StructField fieldRef) {
            ObjectInspector foi = fieldRef.getFieldObjectInspector();
            if (foi instanceof LazyBooleanObjectInspector) {
                ((LazyBooleanObjectInspector) foi).setExtendedLiteral(true);
            }

            if (foi instanceof LazyStringObjectInspector) {
                return quoteEnabled ? new LazyQuoteAwareString((LazyStringObjectInspector) foi, quote)
                        : new LazyString((LazyStringObjectInspector) foi);
            } else if (foi instanceof LazyDateObjectInspector) {
                return new LazyDateEx((LazyDateObjectInspector) foi);
            } else if (foi instanceof LazyTimestampObjectInspector) {
                return new LazyTimestampEx((LazyTimestampObjectInspector) foi);
            } else if (foi instanceof LazyDoubleObjectInspector) {
                return new LazyDoubleEx((LazyDoubleObjectInspector) foi, numberForceRead);
            } else if (foi instanceof LazyFloatObjectInspector) {
                return new LazyFloatEx((LazyFloatObjectInspector) foi, numberForceRead);
            } else if (foi instanceof LazyIntObjectInspector) {
                return new LazyIntEx((LazyIntObjectInspector) foi, numberForceRead);
            } else if (foi instanceof LazyLongObjectInspector) {
                return new LazyLongEx((LazyLongObjectInspector) foi, numberForceRead);
            } else {
                return LazyFactory.createLazyObject(foi);
            }
        }

        /*
         * Resets data in this structure object. Called at the beginning of deserializing a new row.
         */
        @Override
        public void init(ByteArrayRef bytes, int start, int length) {
            super.init(bytes, start, length);
            selfParsed = false;
        }

        @Override
        public Object getField(int fieldId) {
            if (!selfParsed) {
                parse0();
            }
            return uncheckedGetField0(fieldId);
        }

        private void parse0() {

            byte separator = oi.getSeparator();
            boolean lastColumnTakesRest = oi.getLastColumnTakesRest();
            boolean isEscaped = oi.isEscaped();
            byte escapeChar = oi.getEscapeChar();

            initFieldQuoted();

            int structByteEnd = start + length;
            int fieldId = 0;
            int fieldByteBegin = start;
            int fieldByteEnd = start;
            byte[] bytes = this.bytes.getData();

            // Go through all bytes in the byte[]
            while (fieldByteEnd <= structByteEnd) {
                // check if is the end of a field, note the quote begin and end must match
                boolean isSep = checkEndField(separator, structByteEnd, fieldByteBegin, fieldByteEnd, bytes);
                if (fieldByteEnd == structByteEnd || isSep) {
                    // Reached the end of a field?
                    fieldByteEnd = getFieldByteEnd(lastColumnTakesRest, structByteEnd, fieldId, fieldByteEnd);
                    // Is quoted?
                    checkFieldQuoted(fieldId, fieldByteBegin, fieldByteEnd, bytes);
                    // Mark field boundary
                    selfStartPosition[fieldId] = fieldByteBegin;
                    fieldId++;
                    if (fieldId == selfFields.length || fieldByteEnd == structByteEnd) {
                        // All fields have been parsed, or bytes have been parsed.
                        // We need to set the startPosition of fields.length to ensure we
                        // can use the same formula to calculate the length of each field.
                        // For missing fields, their starting positions will all be the same,
                        // which will make their lengths to be -1 and uncheckedGetField will
                        // return these fields as NULLs.
                        for (int i = fieldId; i <= selfFields.length; i++) {
                            selfStartPosition[i] = fieldByteEnd + 1;
                        }
                        break;
                    }
                    fieldByteBegin = fieldByteEnd + 1;
                    fieldByteEnd++;
                } else {
                    if (isEscaped && bytes[fieldByteEnd] == escapeChar && fieldByteEnd + 1 < structByteEnd) {
                        // ignore the char after escape_char
                        fieldByteEnd += 2;
                    } else {
                        fieldByteEnd++;
                    }
                }
            }

            // Extra bytes at the end?
            checkExtraBytesAtTheEnd(structByteEnd, fieldByteEnd);

            // Missing fields?
            checkMissingFields(fieldId);

            Arrays.fill(selfFieldInited, false);
            selfParsed = true;
        }

        private int getFieldByteEnd(boolean lastColumnTakesRest, int structByteEnd, int fieldId, int fieldByteEnd) {
            if (lastColumnTakesRest && fieldId == selfFields.length - 1) {
                fieldByteEnd = structByteEnd;
            }
            return fieldByteEnd;
        }

        private void initFieldQuoted() {
            if (selfFields == null) {
                initLazyFields2(oi.getAllStructFieldRefs());
                isFieldQuoted = new boolean[selfFields.length];
            }
            Arrays.fill(isFieldQuoted, false);
        }

        private void checkFieldQuoted(int fieldId, int fieldByteBegin, int fieldByteEnd, byte[] bytes) {
            if (quoteEnabled) {
                isFieldQuoted[fieldId] = (fieldByteEnd - fieldByteBegin >= 2 && bytes[fieldByteBegin] == quote
                        && bytes[fieldByteEnd - 1] == quote);
            }
        }

        private void checkMissingFields(int fieldId) {
            if (!selfMissingFieldWarned && fieldId < selfFields.length) {
                selfMissingFieldWarned = true;
            }
        }

        private void checkExtraBytesAtTheEnd(int structByteEnd, int fieldByteEnd) {
            if (!selfExtraFieldWarned && fieldByteEnd < structByteEnd) {
                selfExtraFieldWarned = true;
            }
        }

        private boolean checkEndField(byte separator, int structByteEnd, int fieldByteBegin, int fieldByteEnd,
                byte[] bytes) {
            boolean isSep = fieldByteEnd == structByteEnd || bytes[fieldByteEnd] == separator;
            if (isSep && quoteEnabled && (fieldByteBegin < structByteEnd && bytes[fieldByteBegin] == quote)
                    && (fieldByteEnd < fieldByteBegin + 2 || bytes[fieldByteEnd - 1] != quote)) {
                // it begins with "
                // but ending is not
                isSep = false;
            }
            return isSep;
        }

        private void initLazyFields2(List<? extends StructField> fieldRefs) {
            selfFields = new LazyObjectBase[fieldRefs.size()];
            for (int i = 0; i < selfFields.length; i++) {
                try {
                    selfFields[i] = createLazyField(i, fieldRefs.get(i));
                } catch (Exception e) {
                    throw new KylinRuntimeException(e);
                }
            }
            selfFieldInited = new boolean[selfFields.length];
            // Extra element to make sure we have the same formula to compute the
            // length of each element of the array.
            selfStartPosition = new int[selfFields.length + 1];
        }

        private Object uncheckedGetField0(int fieldId) {
            if (selfFieldInited[fieldId]) {
                return selfFields[fieldId].getObject();
            }
            selfFieldInited[fieldId] = true;

            int fieldByteBegin = selfStartPosition[fieldId];
            int fieldLength = selfStartPosition[fieldId + 1] - selfStartPosition[fieldId] - 1;
            if (isFieldQuoted[fieldId]) {
                fieldByteBegin++;
                fieldLength -= 2;
            }
            if (isNull(oi.getNullSequence(), bytes, fieldByteBegin, fieldLength)) {
                selfFields[fieldId].setNull();
            } else {
                if (isFieldQuoted[fieldId] && selfFields[fieldId] instanceof LazyQuoteAwareString) {
                    ((LazyQuoteAwareString) selfFields[fieldId]).initQuoted(bytes, fieldByteBegin, fieldLength);
                } else {
                    selfFields[fieldId].init(bytes, fieldByteBegin, fieldLength);
                }
            }
            return selfFields[fieldId].getObject();
        }

        private transient List<Object> cachedList;

        @Override
        public List<Object> getFieldsAsList() {
            if (!selfParsed) {
                parse0();
            }
            if (cachedList == null) {
                cachedList = new ArrayList<>();
            } else {
                cachedList.clear();
            }
            for (int i = 0; i < selfFields.length; i++) {
                cachedList.add(uncheckedGetField0(i));
            }
            return cachedList;
        }
    }

    public static class LazyQuoteAwareString extends LazyString {
        private final byte quote;

        public LazyQuoteAwareString(LazyStringObjectInspector oi, byte quote) {
            super(oi);
            this.quote = quote;
        }

        @Override
        public void init(ByteArrayRef bytes, int start, int length) {
            if (oi.isEscaped()) {
                byte escapeChar = oi.getEscapeChar();
                byte[] inputBytes = bytes.getData();
                LazyUtils.copyAndEscapeStringDataToText(inputBytes, start, length, escapeChar, data);
            } else {
                // if the data is not escaped, simply copy the data.
                data.set(bytes.getData(), start, length);
            }
            isNull = false;
        }

        public void initQuoted(ByteArrayRef bytes, int fieldByteBegin, int fieldLength) {
            init(bytes, fieldByteBegin, fieldLength);

            // transform double quotes "" into one quote "
            transformDoubleQuotes();
        }

        private void transformDoubleQuotes() {
            if (isNull) {
                return;
            }

            byte[] bytes = data.getBytes();
            int len = data.getLength();
            if (len < 2) {
                return;
            }

            // first check if there is double quotes
            int i = 0;
            while (i < len - 1) {
                if (bytes[i] == quote && bytes[i + 1] == quote) {
                    break;
                }
                i++;
            }
            if (i == len - 1) { // no double quotes
                return; // quick return
            }

            // found double quotes, start to transform
            int target = i;
            while (i < len) {
                if (i < len - 1 && bytes[i] == quote && bytes[i + 1] == quote) {
                    i++;
                }
                bytes[target++] = bytes[i++];
            }

            // inside Text.set(), System.arraycopy will mind the copy on the same object
            data.set(bytes, 0, target);
        }
    }

    /**
     * The general contract of the LazyXxxPrimitive implementations:
     * <br/>
     * - Make the best effort to extract the primitive value, like skipping any letter to parse an integer
     * - Never throw exception
     * - In case of error, set isNull && call logExceptionMessage()
     */

    public static class LazyDateEx extends LazyDate {
        public LazyDateEx(LazyDateObjectInspector oi) {
            super(oi);
        }

        @Override
        public void init(ByteArrayRef bytes, int start, int length) {
            String s;
            try {
                s = Text.decode(bytes.getData(), start, length);
                data.set(PrimitiveParser.parseDate(s));
                isNull = false;
            } catch (Exception e) {
                isNull = true;
                logExceptionMessage(bytes, start, length, "DATE");
            }
        }
    }

    public static class LazyTimestampEx extends LazyTimestamp {
        public LazyTimestampEx(LazyTimestampObjectInspector oi) {
            super(oi);
        }

        @Override
        public void init(ByteArrayRef bytes, int start, int length) {
            String s;
            s = new String(bytes.getData(), start, length, StandardCharsets.US_ASCII);

            Timestamp t = null;
            if (s.compareTo("NULL") == 0) {
                isNull = true;
                logExceptionMessage(bytes, start, length, "TIMESTAMP");
            } else {
                try {
                    t = PrimitiveParser.parseTimestamp(s);
                    isNull = false;
                } catch (IllegalArgumentException e) {
                    isNull = true;
                    logExceptionMessage(bytes, start, length, "TIMESTAMP");
                }
            }
            data.set(t);
        }
    }

    public abstract static class LazyGenericNumber<OI extends ObjectInspector, T extends Writable>
            extends LazyPrimitive<OI, T> {
        private final boolean forceRead;

        protected LazyGenericNumber(OI oi, boolean forceRead) {
            super(oi);
            this.data = newEmptyWritable();
            this.forceRead = forceRead;
        }

        protected abstract T newEmptyWritable();

        protected abstract void parseAndSet(String str, T data, boolean force);

        @Override
        public void init(ByteArrayRef bytes, int start, int length) {
            String str;
            if (!LazyUtils.isNumberMaybe(bytes.getData(), start, length)) {
                isNull = true;
                return;
            }
            try {
                str = Text.decode(bytes.getData(), start, length);
                parseAndSet(str, data, forceRead);
                isNull = false;
            } catch (RuntimeException | CharacterCodingException e) {
                isNull = true;
                logExceptionMessage(bytes, start, length, "NUMBER");
            }
        }
    }

    public static class LazyDoubleEx extends LazyGenericNumber<LazyDoubleObjectInspector, DoubleWritable> {
        public LazyDoubleEx(LazyDoubleObjectInspector lazyDoubleObjectInspector, boolean forceRead) {
            super(lazyDoubleObjectInspector, forceRead);
        }

        @Override
        protected DoubleWritable newEmptyWritable() {
            return new DoubleWritable();
        }

        @Override
        protected void parseAndSet(String str, DoubleWritable data, boolean force) {
            data.set(PrimitiveParser.parseDouble(str, force));
        }
    }

    public static class LazyFloatEx extends LazyGenericNumber<LazyFloatObjectInspector, FloatWritable> {
        public LazyFloatEx(LazyFloatObjectInspector lazyFloatObjectInspector, boolean forceRead) {
            super(lazyFloatObjectInspector, forceRead);
        }

        @Override
        protected FloatWritable newEmptyWritable() {
            return new FloatWritable();
        }

        @Override
        protected void parseAndSet(String str, FloatWritable data, boolean force) {
            data.set(PrimitiveParser.parseFloat(str, force));
        }
    }

    public static class LazyIntEx extends LazyGenericNumber<LazyIntObjectInspector, IntWritable> {
        public LazyIntEx(LazyIntObjectInspector lazyIntegerObjectInspector, boolean forceRead) {
            super(lazyIntegerObjectInspector, forceRead);
        }

        @Override
        protected IntWritable newEmptyWritable() {
            return new IntWritable();
        }

        @Override
        protected void parseAndSet(String str, IntWritable data, boolean force) {
            data.set(PrimitiveParser.parseInteger(str, force));
        }
    }

    public static class LazyLongEx extends LazyGenericNumber<LazyLongObjectInspector, LongWritable> {
        public LazyLongEx(LazyLongObjectInspector lazyLongObjectInspector, boolean forceRead) {
            super(lazyLongObjectInspector, forceRead);
        }

        @Override
        protected LongWritable newEmptyWritable() {
            return new LongWritable();
        }

        @Override
        protected void parseAndSet(String str, LongWritable data, boolean force) {
            data.set(PrimitiveParser.parseLong(str, force));
        }
    }

    public static class PrimitiveParser {

        private PrimitiveParser() {
        }

        public static final String[] DATE_PTNS = new String[] { "yyyy-MM-dd", "yyyy/MM/dd", "MM/dd/yyyy", "yyyy.MM.dd",
                "yyyyMMdd", "yyyyMM", "yyyy-MM", "yyyy/MM" };
        static final TimestampParser dateParser = new TimestampParser(DATE_PTNS);

        public static java.sql.Date parseDate(String str) {
            if (str.length() < 6 || str.length() > 10) {
                throw new IllegalArgumentException("Invalid date value: " + str);
            }
            try {
                Timestamp ts = dateParser.parseTimestamp(str);
                return new java.sql.Date(ts.getTime());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Error date value '" + str + "'. " + "Date format must be "
                        + StringUtils.join(DATE_PTNS, " or "), e);
            }
        }

        public static final String[] TIMESTAMP_PTNS = new String[] { "yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss,SSS", "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                "yyyy/MM/dd HH:mm", "yyyy/MM/dd HH:mm:ss", "MM/dd/yyyy HH:mm", "MM/dd/yyyy HH:mm:ss", };
        static final TimestampParser timestampParser = new TimestampParser(TIMESTAMP_PTNS);

        public static java.sql.Timestamp parseTimestamp(String str) {
            if (str.length() < 12 || str.length() > 24) {
                throw new IllegalArgumentException("Invalid timestamp value: " + str);
            }
            try {
                return timestampParser.parseTimestamp(str);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Error timestamp value '" + str + "'. " + "Timestamp format must be "
                        + StringUtils.join(TIMESTAMP_PTNS, " or "));
            }
        }

        public static Boolean parseBoolean(String str) {
            if (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("t") || str.equals("1")) {
                return true;
            }

            if (str.equalsIgnoreCase("false") || str.equalsIgnoreCase("f") || str.equals("0")) {
                return false;
            }

            throw new IllegalArgumentException("Not a boolean: " + str);
        }

        /**
         * Think about a complicated number, like "$100,000.00 %"
         */
        private static final int MAX_HEAD_SKIP = 2;
        private static final int MAX_TAIL_SKIP = 2;

        private static String extractNumberPart(String str, boolean force) {
            int s = 0;
            int e = str.length() - 1;

            // find the start of number
            s = findStartIndexOfNumber(str, force, s, e);
            // find the end of number
            e = findEndIndexOfNumber(str, force, s, e);

            if (s > e)
                throw new NumberFormatException("String '" + str + "' does not contain a number");

            str = str.substring(s, e + 1);

            // filter out any ','
            for (int i = 0; i < str.length(); i++)
                if (str.charAt(i) == ',')
                    return str.replace(",", "");

            return str;
        }

        private static int findEndIndexOfNumber(String str, boolean force, int s, int e) {
            int tailSkip = 0;
            for (; s <= e; e--, tailSkip++) {
                char c = str.charAt(e);
                if (!force && ('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || tailSkip > MAX_TAIL_SKIP)) {
                    throw new NumberFormatException("String '" + str + "' does not contain a number");
                }
                if (c >= '0' && c <= '9')
                    break;
            }
            return e;
        }

        private static int findStartIndexOfNumber(String str, boolean force, int s, int e) {
            int headSkip = 0;
            for (; s <= e; s++, headSkip++) {
                char c = str.charAt(s);
                if (!force && ('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || headSkip > MAX_HEAD_SKIP)) {
                    throw new NumberFormatException("String '" + str + "' does not contain a number");
                }
                if (c == '-' || c == '+' || (c >= '0' && c <= '9')) {
                    break;
                }
            }
            return s;
        }

        public static Double parseDouble(String str) {
            return parseDouble(str, false);
        }

        public static Double parseDouble(String str, boolean force) {
            return Double.parseDouble(extractNumberPart(str, force));
        }

        public static Float parseFloat(String str) {
            return parseFloat(str, false);
        }

        public static Float parseFloat(String str, boolean force) {
            return Float.parseFloat(extractNumberPart(str, force));
        }

        public static Integer parseInteger(String str) {
            return parseInteger(str, false);
        }

        public static Integer parseInteger(String str, boolean force) {
            return Integer.parseInt(extractNumberPart(str, force));
        }

        public static Long parseLong(String str) {
            return parseLong(str, false);
        }

        public static Long parseLong(String str, boolean force) {
            return Long.parseLong(extractNumberPart(str, force));
        }
    }
}
