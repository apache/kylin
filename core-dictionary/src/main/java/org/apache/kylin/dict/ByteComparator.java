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

import org.apache.kylin.common.util.ByteArray;

import java.util.Comparator;

/**
 * Created by xiefan on 16-10-28.
 */
public class ByteComparator<T> implements Comparator<T> {
    private BytesConverter<T> converter;

    public ByteComparator(BytesConverter<T> converter) {
        this.converter = converter;
    }

    @Override
    public int compare(T o1, T o2) {
        //return BytesUtil.safeCompareBytes(converter.convertToBytes(o1),converter.convertToBytes(o2));
        byte[] b1 = converter.convertToBytes(o1);
        byte[] b2 = converter.convertToBytes(o2);
        ByteArray ba1 = new ByteArray(b1, 0, b1.length);
        ByteArray ba2 = new ByteArray(b2, 0, b2.length);
        return ba1.compareTo(ba2);
    }
}
