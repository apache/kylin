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

package org.apache.kylin.measure.bitmap;

import org.apache.kylin.common.util.ByteBufferOutputStream;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

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

    private ImmutableRoaringBitmap bitmap;
    private Long counter;

    RoaringBitmapCounter() {
        bitmap = new MutableRoaringBitmap();
    }

    RoaringBitmapCounter(ImmutableRoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    RoaringBitmapCounter(long counter) {
        this.counter = counter;
    }


    private MutableRoaringBitmap getMutableBitmap() {
        if (bitmap instanceof MutableRoaringBitmap) {
            return (MutableRoaringBitmap) bitmap;
        }
        // convert to mutable bitmap
        MutableRoaringBitmap result = bitmap.toMutableRoaringBitmap();
        bitmap = result;
        return result;
    }

    @Override
    public void add(int value) {
        getMutableBitmap().add(value);
    }

    @Override
    public void orWith(BitmapCounter another) {
        if (another instanceof RoaringBitmapCounter) {
            RoaringBitmapCounter input = (RoaringBitmapCounter) another;
            getMutableBitmap().or(input.bitmap);
            return;
        }
        throw new IllegalArgumentException("Unsupported type: " + another.getClass().getCanonicalName());
    }

    @Override
    public void andWith(BitmapCounter another) {
        if (another instanceof RoaringBitmapCounter) {
            RoaringBitmapCounter input = (RoaringBitmapCounter) another;
            getMutableBitmap().and(input.bitmap);
            return;
        }
        throw new IllegalArgumentException("Unsupported type: " + another.getClass().getCanonicalName());
    }

    @Override
    public void clear() {
        bitmap = new MutableRoaringBitmap();
    }

    @Override
    public long getCount() {
        if (counter != null) {
            return counter;
        }

        return bitmap.getCardinality();
    }

    @Override
    public int getMemBytes() {
        return bitmap.getSizeInBytes();
    }

    @Override
    public Iterator<Integer> iterator() {
        return bitmap.iterator();
    }

    @Override
    public void write(ByteBuffer out) throws IOException {
        if (bitmap instanceof MutableRoaringBitmap) {
            getMutableBitmap().runOptimize();
        }

        if (out.remaining() < bitmap.serializedSizeInBytes()) {
            throw new BufferOverflowException();
        }
        try (DataOutputStream dos = new DataOutputStream(new ByteBufferOutputStream(out))) {
            bitmap.serialize(dos);
        }
    }

    @Override
    public void readFields(ByteBuffer in) throws IOException {
        int size = peekLength(in);
        // make a copy of the content to be safe
        byte[] dst = new byte[size];
        in.get(dst);

        // ImmutableRoaringBitmap only maps the buffer, thus faster than constructing a MutableRoaringBitmap.
        // we'll convert to MutableRoaringBitmap later when mutate is needed
        bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(dst));
    }

    @Override
    public int peekLength(ByteBuffer in) {
        // only look at the metadata of the bitmap, no deserialization happens
        ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(in);
        return bitmap.serializedSizeInBytes();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof RoaringBitmapCounter) &&
                bitmap.equals(((RoaringBitmapCounter) obj).bitmap);
    }

    @Override
    public int hashCode() {
        return bitmap.hashCode();
    }

    @Override
    public String toString() {
        return "RoaringBitmapCounter[" + getCount() + "]";
    }
}
