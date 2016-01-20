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

import org.apache.hadoop.io.DataInputByteBuffer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by sunyerui on 15/12/1.
 */
public class BitmapCounter implements Comparable<BitmapCounter> {

    private MutableRoaringBitmap bitmap = new MutableRoaringBitmap();

    public BitmapCounter() {
    }

    public BitmapCounter(BitmapCounter another) {
        merge(another);
    }

    public void clear() {
        bitmap.clear();
    }

    public void add(int value) {
        bitmap.add(value);
    }

    public void add(byte[] value) {
        if (value == null || value.length == 0) {
            return;
        }
        try {
            int l = Integer.parseInt(new String(value));
            add(l);
        } catch (NumberFormatException e) {
            throw e;
        }
    }

    public void add(byte[] value, int offset, int length) {
        if (value == null || length == 0) {
            return;
        }
        try {
            int l = Integer.parseInt(new String(value, offset, length));
            add(l);
        } catch (NumberFormatException e) {
            throw e;
        }
    }

    public void add(String value) {
        if (value == null || value.isEmpty()) {
            return;
        }
        try {
            int l = Integer.parseInt(value);
            add(l);
        } catch (NumberFormatException e) {
            throw e;
        }
    }

    public void add(long value) {
        // TODO we need support long later
        add((int) value);
    }

    public void merge(BitmapCounter another) {
        this.bitmap.or(another.bitmap);
    }

    public long getCount() {
        return this.bitmap.getCardinality();
    }

    public int getMemBytes() {
        return this.bitmap.getSizeInBytes();
    }

    public void writeRegisters(ByteBuffer out) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bitmap.runOptimize();
        bitmap.serialize(dos);
        dos.close();
        ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
        out.put(bb);
    }

    public void readRegisters(ByteBuffer in) throws IOException {
        DataInputByteBuffer input = new DataInputByteBuffer();
        input.reset(new ByteBuffer[]{in});
        bitmap.deserialize(input);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + bitmap.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BitmapCounter other = (BitmapCounter) obj;
        return bitmap.equals(other.bitmap);
    }

    @Override
    public int compareTo(BitmapCounter o) {
        if (o == null)
            return 1;

        long e1 = this.getCount();
        long e2 = o.getCount();

        if (e1 == e2)
            return 0;
        else if (e1 > e2)
            return 1;
        else
            return -1;
    }

    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int len;

        DataInputByteBuffer input = new DataInputByteBuffer();
        input.reset(new ByteBuffer[]{in});
        RoaringBitmap bitmap = new RoaringBitmap();
        try {
            bitmap.deserialize(input);
        } catch (IOException e) {
        }

        len = in.position() - mark;
        in.position(mark);
        return len;
    }
}
