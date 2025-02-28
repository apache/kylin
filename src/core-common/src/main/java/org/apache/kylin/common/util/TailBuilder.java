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
package org.apache.kylin.common.util;

import java.util.Arrays;
import java.util.LinkedList;

import lombok.Getter;

public class TailBuilder {

    static final int ENTRY_SIZE = 1024;

    private LinkedList<char[]> data = new LinkedList<>();

    @Getter
    private final int maxCapacity;

    // last segment left size
    private int writableSize = 0;

    private int length = 0;

    public int length() {
        return Math.min(length, maxCapacity);
    }

    public TailBuilder(int maxCapacity) {
        if (maxCapacity < 0) {
            throw new IllegalArgumentException("maxCapacity must >= 0");
        }
        this.maxCapacity = maxCapacity;
    }

    public TailBuilder append(CharSequence csq) {
        if (maxCapacity == 0) {
            return this;
        }
        if (csq == null || csq.length() == 0) {
            return this;
        }
        // only need tail part
        if (csq.length() > maxCapacity) {
            csq = csq.subSequence(csq.length() - maxCapacity, csq.length());
        }
        int size2Append = csq.length();
        char[] charArray = csq.toString().toCharArray();
        while (size2Append > 0) {
            int copySize = Math.min(writableSize, size2Append);
            System.arraycopy(charArray, charArray.length - size2Append, getWriteEntry(),
                    ENTRY_SIZE - writableSize, copySize);
            writableSize -= copySize;
            size2Append -= copySize;
            length += copySize;
        }
        return this;
    }

    private char[] getWriteEntry() {
        ensureWriteEntry();
        return data.getLast();
    }

    public TailBuilder append(char c) {
        if (maxCapacity == 0) {
            return this;
        }
        getWriteEntry()[ENTRY_SIZE - writableSize] = c;
        writableSize--;
        length++;
        return this;
    }

    /**
     * ensure the last entry having space to write
     * allocate a new entry if the last entry is full
     * we would recycle the first entry to allocate
     * if the (sizeof(data) - 1) * ENTRY_SIZE > maxCapacity so the first entry is useless
     * because data[0] is not include any char we want(remain last maxCapacity chars)
     */
    private void ensureWriteEntry() {
        if (writableSize > 0) {
            return;
        }
        char[] allocated = null;
        // recycle first entry
        if (ENTRY_SIZE * (data.size() - 1) >= maxCapacity) {
            allocated = data.removeFirst();
            Arrays.fill(allocated, (char) 0);
            length -= ENTRY_SIZE;
        }
        if (allocated == null) {
            allocated = new char[ENTRY_SIZE];
        }
        data.add(allocated);
        writableSize = ENTRY_SIZE;
    }

    /**
     * @return a String contains tail maxCapacity chars, if length < maxCapacity, return all
     */
    public String toString() {
        int totalSize = Math.min(length, maxCapacity);
        char[] result = new char[totalSize];
        int skipSize = Math.max(0, length - maxCapacity);
        int pos = 0;
        for (int i = 0; i < data.size() - 1; i++) {
            if (skipSize >= ENTRY_SIZE) {
                skipSize -= ENTRY_SIZE;
                continue;
            }
            char[] src = data.get(i);
            System.arraycopy(src, skipSize, result, pos, ENTRY_SIZE - skipSize);
            pos += ENTRY_SIZE - skipSize;
            skipSize = 0;
        }
        System.arraycopy(getWriteEntry(), skipSize, result, pos, ENTRY_SIZE - writableSize - skipSize);
        return new String(result);
    }
}
