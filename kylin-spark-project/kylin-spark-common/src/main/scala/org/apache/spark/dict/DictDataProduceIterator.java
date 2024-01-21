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
package org.apache.spark.dict;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

public class DictDataProduceIterator implements Iterator<byte[]> {

    ObjectIterator<Map.Entry<String,Long>> dictIterator;

    int total;
    int index = -1;
    public DictDataProduceIterator(Object2LongMap<String> dict) {
        dictIterator = dict.entrySet().iterator();
        this.total = dict.size();
    }

    @Override
    public boolean hasNext() {
        return total > 0 && index < total;
    }

    @Override
    public byte[] next() {
        byte[] next = null;
        if(index == -1){
            next = ByteBuffer.allocate(Integer.BYTES).putInt(total).array();
        }else {
            Map.Entry<String,Long> entry = dictIterator.next();
            byte[] bytes = entry.getKey().getBytes(Charset.defaultCharset());
            next = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + bytes.length)
                    .putLong(entry.getValue())
                    .putInt(bytes.length)
                    .put(bytes)
                    .array();
        }
        index++;
        return next;
    }
}
