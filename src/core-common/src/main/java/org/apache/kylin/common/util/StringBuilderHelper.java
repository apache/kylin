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

/**
 * slice the string head and tail
 * merge head and tail within the total size
 */
public class StringBuilderHelper {

    private final int totalSize;

    private final HeadBuilder headBuilder;

    private final TailBuilder tailBuilder;

    private int appendedSize = 0;

    public static StringBuilderHelper head(int headSize) {
        return new StringBuilderHelper(headSize, 0);
    }

    public static StringBuilderHelper tail(int tailSize) {
        return new StringBuilderHelper(0, tailSize);
    }

    public static StringBuilderHelper headTail(int headSize, int tailSize) {
        return new StringBuilderHelper(headSize, tailSize);
    }

    private StringBuilderHelper(int headSize, int tailSize) {
        this.headBuilder = new HeadBuilder(headSize);
        this.tailBuilder = new TailBuilder(tailSize);
        this.totalSize = headSize + tailSize;
    }

    public StringBuilderHelper append(CharSequence csq) {
        headBuilder.append(csq);
        tailBuilder.append(csq);
        appendedSize += csq.length();
        return this;
    }

    public StringBuilderHelper append(char c) {
        headBuilder.append(c);
        tailBuilder.append(c);
        appendedSize++;
        return this;
    }

    private String merge() {
        if (appendedSize < totalSize) {
            // concat appended string
            char[] res = new char[appendedSize];
            headBuilder.toString().getChars(0, headBuilder.length(), res, 0);
            int least = appendedSize - headBuilder.length();
            tailBuilder.toString().getChars(tailBuilder.length() - least, tailBuilder.length(),
                    res, headBuilder.length());
            return new String(res);
        } else {
            // concat head and tail
            char[] res = new char[totalSize];
            int headLength = headBuilder.length();
            headBuilder.toString().getChars(0, headLength, res, 0);
            tailBuilder.toString().getChars(0, tailBuilder.length(),
                    res, headLength);
            return new String(res);
        }
    }

    public int length() {
        return Math.min(appendedSize, totalSize);
    }

    /**
     * if appended string size <= totalSize return total string
     * if append string size > totalSize return head + tail
     */
    public String toString() {
        return merge();
    }
}
