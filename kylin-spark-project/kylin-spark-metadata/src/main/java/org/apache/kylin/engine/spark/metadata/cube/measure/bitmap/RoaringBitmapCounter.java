/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
package org.apache.kylin.engine.spark.metadata.cube.measure.bitmap;

import org.apache.kylin.common.util.ByteBufferBackedInputStream;
import org.apache.kylin.common.util.ByteBufferOutputStream;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A {@link BitmapCounter} based on roaring bitmap.
 */
public class RoaringBitmapCounter implements BitmapCounter, Serializable {

    private Roaring64NavigableMap bitmap;
    private Long counter;

    public RoaringBitmapCounter() {
        bitmap = new Roaring64NavigableMap();
    }

    RoaringBitmapCounter(Roaring64NavigableMap bitmap) {
        this.bitmap = bitmap;
    }

    RoaringBitmapCounter(long counter) {
        this.counter = counter;
    }

    @Override
    public void orWith(BitmapCounter another) {
        if (another instanceof RoaringBitmapCounter) {
            RoaringBitmapCounter input = (RoaringBitmapCounter) another;
            bitmap.or(input.bitmap);
            return;
        }
        throw new IllegalArgumentException("Unsupported type: " + another.getClass().getCanonicalName());
    }

    @Override
    public void andWith(BitmapCounter another) {
        if (another instanceof RoaringBitmapCounter) {
            RoaringBitmapCounter input = (RoaringBitmapCounter) another;
            bitmap.and(input.bitmap);
            return;
        }
        throw new IllegalArgumentException("Unsupported type: " + another.getClass().getCanonicalName());
    }

    @Override
    public void add(long value) {
        bitmap.add(value);
    }

    public void clear() {
        bitmap = new Roaring64NavigableMap();
    }

    public long getCount() {
        if (counter != null) {
            return counter;
        }

        return bitmap.getLongCardinality();
    }

    public int getMemBytes() {
        return bitmap.getSizeInBytes();
    }

    public Iterator<Long> iterator() {
        return bitmap.iterator();
    }

    public void write(ByteBuffer out) throws IOException {
        if (bitmap instanceof Roaring64NavigableMap) {
            bitmap.runOptimize();
        }

        if (out.remaining() < bitmap.getSizeInBytes()) {
            throw new BufferOverflowException();
        }
        try (DataOutputStream dos = new DataOutputStream(new ByteBufferOutputStream(out))) {
            bitmap.serialize(dos);
        }
    }

    public void write(ByteArrayOutputStream baos) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            bitmap.serialize(dos);
        }
    }

    public void readFields(ByteBuffer in) throws IOException {
        ByteBufferBackedInputStream bbi = new ByteBufferBackedInputStream(in);
        bitmap.deserialize(new DataInputStream(bbi));
    }

    public int peekLength(ByteBuffer in) {
        // The current peeklength method has no meaning
        throw new UnsupportedOperationException();
    }

    public boolean equals(Object obj) {
        return (obj instanceof RoaringBitmapCounter) && bitmap.equals(((RoaringBitmapCounter) obj).bitmap);
    }

    public int hashCode() {
        return bitmap.hashCode();
    }

    public String toString() {
        return "RoaringBitmapCounter[" + getCount() + "]";
    }
}
