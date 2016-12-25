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

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dict.NumberDictionary.NumberBytesCodec;

/**
 * Created by xiefan on 16-11-2.
 */
public class NumberDictionaryForestBuilder extends TrieDictionaryForestBuilder<String> {

    public static class Number2BytesConverter implements BytesConverter<String>, java.io.Serializable {

        static final int MAX_DIGITS_BEFORE_DECIMAL_POINT = NumberDictionary.MAX_DIGITS_BEFORE_DECIMAL_POINT;
        static final transient ThreadLocal<NumberBytesCodec> LOCAL = new ThreadLocal<NumberBytesCodec>();

        static NumberBytesCodec getCodec() {
            NumberBytesCodec codec = LOCAL.get();
            if (codec == null) {
                codec = new NumberBytesCodec(MAX_DIGITS_BEFORE_DECIMAL_POINT);
                LOCAL.set(codec);
            }
            return codec;
        }
        
        @Override
        public byte[] convertToBytes(String v) {
            NumberBytesCodec codec = getCodec();
            byte[] num = Bytes.toBytes(v);
            codec.encodeNumber(num, 0, num.length);
            return Bytes.copy(codec.buf, codec.bufOffset, codec.bufLen);
        }

        @Override
        public String convertFromBytes(byte[] b, int offset, int length) {
            NumberBytesCodec codec = getCodec();
            byte[] backup = codec.buf;
            codec.buf = b;
            codec.bufOffset = offset;
            codec.bufLen = length;
            int len = codec.decodeNumber(backup, 0);
            codec.buf = backup;
            return Bytes.toString(backup, 0, len);
        }

    }

    public NumberDictionaryForestBuilder() {
        super(new Number2BytesConverter());
    }

    public NumberDictionaryForestBuilder(int baseId) {
        super(new Number2BytesConverter(), 0);
    }

    public NumberDictionaryForestBuilder(int baseId, int maxTrieSizeMB) {
        super(new Number2BytesConverter(), 0, maxTrieSizeMB);
    }
}
