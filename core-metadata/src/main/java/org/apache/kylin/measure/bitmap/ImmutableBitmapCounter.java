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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A thin wrapper around {@link ImmutableRoaringBitmap}.
 */
public class ImmutableBitmapCounter implements BitmapCounter {

    protected ImmutableRoaringBitmap bitmap;

    public ImmutableBitmapCounter() {
        this(ImmutableRoaringBitmap.bitmapOf());
    }

    public ImmutableBitmapCounter(ImmutableRoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    @Override
    public long getCount() {
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
    public void serialize(ByteBuffer out) throws IOException {
        if (out.remaining() < bitmap.serializedSizeInBytes()) {
            throw new BufferOverflowException();
        }
        bitmap.serialize(new DataOutputStream(new ByteBufferOutputStream(out)));
    }

    @Override
    public BitmapCounter deserialize(ByteBuffer in) throws IOException {
        int size = peekLength(in);
        // make a copy of the content to be safe
        byte[] dst = new byte[size];
        in.get(dst);

        // just map the buffer, faster than deserialize
        ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(dst));
        return new ImmutableBitmapCounter(bitmap);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        // only look at the metadata of the bitmap, no deserialization happens
        ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(in);
        return bitmap.serializedSizeInBytes();
    }

    /**
     * Copies the content of this counter to a counter that can be modified.
     * @return a mutable counter
     */
    public MutableBitmapCounter toMutable() {
        MutableBitmapCounter mutable = new MutableBitmapCounter();
        mutable.orWith(this);
        return mutable;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof ImmutableBitmapCounter) &&
                bitmap.equals(((ImmutableBitmapCounter) obj).bitmap);
    }

    @Override
    public int hashCode() {
        return bitmap.hashCode();
    }

    @Override
    public String toString() {
        return "BitmapCounter[" + getCount() + "]";
    }
}
