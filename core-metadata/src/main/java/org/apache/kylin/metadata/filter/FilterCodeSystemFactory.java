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

package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by donald.zheng on 2016/12/19.
 */
public class FilterCodeSystemFactory {
    private static final Logger logger = LoggerFactory.getLogger(FilterCodeSystemFactory.class);
    private final static HashMap<String, IFilterCodeSystem> codeSystemMap = new HashMap<>();

    static {
        codeSystemMap.put("string", StringCodeSystem.INSTANCE);
        codeSystemMap.put("integer", new IntegerCodeSystem());
        codeSystemMap.put("decimal", new DecimalCodeSystem());
        codeSystemMap.put("date", new DateTimeCodeSystem());
    }

    public static IFilterCodeSystem getFilterCodeSystem(DataType dataType) {
        if (dataType.isIntegerFamily()) {
            return codeSystemMap.get("integer");
        } else if (dataType.isNumberFamily()) {
            return codeSystemMap.get("decimal");
        } else if (dataType.isDateTimeFamily()) {
            return codeSystemMap.get("date");
        } else {
            return codeSystemMap.get("string");
        }
    }

    private static class IntegerCodeSystem implements IFilterCodeSystem {

        @Override
        public boolean isNull(Object code) {
            return code == null;
        }

        @Override
        public void serialize(Object code, ByteBuffer buf) {
            BytesUtil.writeLong(Long.parseLong(code.toString()), buf);
        }

        @Override
        public Object deserialize(ByteBuffer buf) {
            return BytesUtil.readLong(buf);
        }

        @Override
        public int compare(Object o, Object t1) {
            long l1 = Long.parseLong(o.toString());
            long l2 = Long.parseLong(t1.toString());
            return Long.compare(l1, l2);
        }
    }

    private static class DecimalCodeSystem implements IFilterCodeSystem {
        @Override
        public boolean isNull(Object code) {
            return code == null;
        }

        @Override
        public void serialize(Object code, ByteBuffer buf) {
            BytesUtil.writeUTFString(code.toString(), buf);
        }

        @Override
        public Object deserialize(ByteBuffer buf) {
            return Double.parseDouble(BytesUtil.readUTFString(buf));
        }

        @Override
        public int compare(Object o, Object t1) {
            double d1 = Double.parseDouble(o.toString());
            double d2 = Double.parseDouble(t1.toString());
            return Double.compare(d1, d2);
        }
    }

    private static class DateTimeCodeSystem implements IFilterCodeSystem<String> {

        @Override
        public boolean isNull(String code) {
            return code == null;
        }

        @Override
        public void serialize(String code, ByteBuffer buf) {
            BytesUtil.writeUTFString(code, buf);
        }

        @Override
        public String deserialize(ByteBuffer buf) {
            return BytesUtil.readUTFString(buf);
        }

        @Override
        public int compare(String o1, String o2) {
            try {
                long d1 = DateFormat.stringToMillis(o1);
                long d2 = DateFormat.stringToMillis(o2);
                return Long.compare(d1, d2);
            } catch (IllegalArgumentException e) {
                logger.error("failed to convert string[{},{}] to date, use string to compare.", o1, o2, e);
                return o1.compareTo(o2);
            }
        }
    }
}
