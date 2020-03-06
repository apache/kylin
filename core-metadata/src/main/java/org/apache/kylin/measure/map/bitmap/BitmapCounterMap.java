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

package org.apache.kylin.measure.map.bitmap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.map.MapKeySerializer;

import com.google.common.collect.Maps;

public abstract class BitmapCounterMap<T> implements Serializable {

    private final MapKeySerializer<T> keySerializer;
    private Map<T, BitmapCounter> counterMap;

    BitmapCounterMap(MapKeySerializer<T> keySerializer) {
        this.keySerializer = keySerializer;
        this.counterMap = Maps.newHashMap();
    }

    private BitmapCounter getMutableBitmap(T key) {
        BitmapCounter bitmap = counterMap.get(key);
        if (bitmap == null) {
            counterMap.put(key, bitmap = newBitmapCounter());
        }
        return bitmap;
    }

    public void add(T key, int value) {
        getMutableBitmap(key).add(value);
    }

    public void clear() {
        counterMap = Maps.newHashMap();
    }

    public void orWith(BitmapCounterMap another) {
        Map<T, BitmapCounter> inputCounterMap = another.counterMap;
        for (T key : inputCounterMap.keySet()) {
            getMutableBitmap(key).orWith(inputCounterMap.get(key));
        }
    }

    public long getCount() {
        long result = 0L;
        for (BitmapCounter bitmapCounter : counterMap.values()) {
            result += bitmapCounter.getCount();
        }
        return result;
    }

    public int getMemBytes() {
        int result = 0;
        for (BitmapCounter bitmapCounter : counterMap.values()) {
            result += bitmapCounter.getMemBytes();
        }
        return result;
    }

    public int peekLength(ByteBuffer in) {
        ByteBuffer bbf = in.slice();
        try {
            getCounterMap(bbf);
            return bbf.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(ByteBuffer out) throws IOException {
        BytesUtil.writeVInt(counterMap.size(), out);
        for (T key : counterMap.keySet()) {
            keySerializer.writeKey(out, key);
            counterMap.get(key).write(out);
        }
    }

    public void readFields(ByteBuffer in) throws IOException {
        counterMap = getCounterMap(in);
    }

    private Map<T, BitmapCounter> getCounterMap(ByteBuffer in) throws IOException {
        int size = BytesUtil.readVInt(in);
        Map<T, BitmapCounter> counterMap = Maps.newHashMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            T key = keySerializer.readKey(in);
            BitmapCounter bitmapCounter = newBitmapCounter();
            bitmapCounter.readFields(in);
            counterMap.put(key, bitmapCounter);
        }
        return counterMap;
    }

    protected abstract BitmapCounter newBitmapCounter();
}
