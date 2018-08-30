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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.util.Dictionary;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public class ShrunkenDictionary<T> extends Dictionary<T> {

    private ImmutableMap<T, Integer> valueToIdMap;
    private ImmutableMap<Integer, T> idToValueMap;

    private int minId;
    private int maxId;
    private int sizeOfId;
    private int sizeOfValue;

    private ValueSerializer<T> valueSerializer;

    public ShrunkenDictionary(ValueSerializer<T> valueSerializer) { // default constructor for Writable interface
        this.valueSerializer = valueSerializer;
    }

    public ShrunkenDictionary(ValueSerializer<T> valueSerializer, int minId, int maxId, int sizeOfId, int sizeOfValue,
            Map<T, Integer> valueToIdMap) {
        this.valueSerializer = valueSerializer;

        this.minId = minId;
        this.maxId = maxId;
        this.sizeOfId = sizeOfId;
        this.sizeOfValue = sizeOfValue;

        Preconditions.checkNotNull(valueToIdMap);
        this.valueToIdMap = ImmutableMap.<T, Integer> builder().putAll(valueToIdMap).build();
    }

    @Override
    public int getMinId() {
        return minId;
    }

    @Override
    public int getMaxId() {
        return maxId;
    }

    @Override
    public int getSizeOfId() {
        return sizeOfId;
    }

    @Override
    public int getSizeOfValue() {
        return sizeOfValue;
    }

    @Override
    public boolean contains(Dictionary<?> another) {
        return false;
    }

    protected int getIdFromValueImpl(T value, int roundingFlag) {
        Integer id = valueToIdMap.get(value);
        if (id == null) {
            return -1;
        }
        return id;
    }

    protected T getValueFromIdImpl(int id) {
        if (idToValueMap == null) {
            idToValueMap = buildIdToValueMap();
        }
        return idToValueMap.get(id);
    }

    private ImmutableMap<Integer, T> buildIdToValueMap() {
        ImmutableMap.Builder<Integer, T> idToValueMapBuilder = ImmutableMap.builder();
        for (T value : valueToIdMap.keySet()) {
            idToValueMapBuilder.put(valueToIdMap.get(value), value);
        }
        return idToValueMapBuilder.build();
    }

    public void dump(PrintStream out) {
        out.println(String.format(Locale.ROOT, "Total %d values for ShrunkenDictionary", valueToIdMap.size()));
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(minId);
        out.writeInt(maxId);
        out.writeInt(sizeOfId);
        out.writeInt(sizeOfValue);

        out.writeInt(valueToIdMap.size());
        for (T value : valueToIdMap.keySet()) {
            valueSerializer.writeValue(out, value);
            out.writeInt(valueToIdMap.get(value));
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.minId = in.readInt();
        this.maxId = in.readInt();
        this.sizeOfId = in.readInt();
        this.sizeOfValue = in.readInt();

        int sizeValueMap = in.readInt();
        ImmutableMap.Builder<T, Integer> valueToIdMapBuilder = ImmutableMap.builder();
        for (int i = 0; i < sizeValueMap; i++) {
            T value = valueSerializer.readValue(in);
            int id = in.readInt();
            valueToIdMapBuilder.put(value, id);
        }
        this.valueToIdMap = valueToIdMapBuilder.build();
    }

    public interface ValueSerializer<T> {
        void writeValue(DataOutput out, T value) throws IOException;

        T readValue(DataInput in) throws IOException;
    }

    public static class StringValueSerializer implements ValueSerializer<String> {
        @Override
        public void writeValue(DataOutput out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String readValue(DataInput in) throws IOException {
            return in.readUTF();
        }
    }
}
