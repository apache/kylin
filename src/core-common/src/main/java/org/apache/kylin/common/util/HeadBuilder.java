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

import lombok.Getter;

/**
 * Limit the capacity of StringBuilder to avoid OOM
 * If the capacity of StringBuilder would exceed the maxCapacity, the StringBuilder will be recreated to maxCapacity
 */
public class HeadBuilder {

    private StringBuilder builder;

    @Getter
    private final int maxCapacity;

    public HeadBuilder(int initialCapacity, int maxCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must greater than 0");
        }
        if (maxCapacity < 0) {
            throw new IllegalArgumentException("maxCapacity must greater than 0");
        }
        this.builder = new StringBuilder(Math.min(initialCapacity, maxCapacity));
        this.maxCapacity = maxCapacity;
    }

    public HeadBuilder(int maxCapacity) {
        this(16, maxCapacity);
    }

    private int ensureCapacity(int appendLen) {
        if (builder.capacity() >= maxCapacity) {
            return Math.min(builder.capacity() - builder.length(), appendLen);
        }
        if (builder.capacity() > builder.length() + appendLen) {
            return appendLen;
        }
        // expand capacity
        int newCapacity = builder.length() << 1 + 2;
        if (newCapacity > maxCapacity) {
            // create a new builder with maxCapacity to ensure maxCapacity limit
            String origin = builder.toString();
            builder = new StringBuilder(maxCapacity);
            builder.append(origin);
        } else {
            builder.ensureCapacity(Math.max(newCapacity, builder.length() + appendLen));
        }
        return Math.min(builder.capacity() - builder.length(), appendLen);
    }

    public int capacity() {
        return builder.capacity();
    }

    public int length() {
        return builder.length();
    }

    public HeadBuilder append(String str) {
        return append(str, 0, str.length());
    }

    public HeadBuilder append(CharSequence csq) {
        return append(csq, 0, csq.length());
    }

    public HeadBuilder append(CharSequence csq, int start, int end) {
        int appendSize = ensureCapacity(end - start);
        if (appendSize == 0) {
            return this;
        }
        builder.append(csq, start, start + appendSize);
        return this;
    }

    public HeadBuilder append(char c) {
        if (ensureCapacity(1) == 0) {
            return this;
        }
        builder.append(c);
        return this;
    }

    @Override
    public String toString() {
        return builder.toString();
    }

}
