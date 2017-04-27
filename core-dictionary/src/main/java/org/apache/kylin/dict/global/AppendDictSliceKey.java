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

package org.apache.kylin.dict.global;

import org.apache.kylin.common.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class AppendDictSliceKey implements Comparable<AppendDictSliceKey> {
    static final AppendDictSliceKey START_KEY = AppendDictSliceKey.wrap(new byte[0]);

    byte[] key;

    public static AppendDictSliceKey wrap(byte[] key) {
        AppendDictSliceKey dictKey = new AppendDictSliceKey();
        dictKey.key = key;
        return dictKey;
    }

    @Override
    public String toString() {
        return Bytes.toStringBinary(key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof AppendDictSliceKey) {
            AppendDictSliceKey that = (AppendDictSliceKey) o;
            return Arrays.equals(this.key, that.key);
        }
        return false;
    }

    @Override
    public int compareTo(AppendDictSliceKey that) {
        return Bytes.compareTo(key, that.key);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(key.length);
        out.write(key);
    }

    public void readFields(DataInput in) throws IOException {
        key = new byte[in.readInt()];
        in.readFully(key);
    }
}
