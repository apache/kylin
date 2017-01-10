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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.kylin.common.util.ByteBufferBackedInputStream;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Created by sunyerui on 15/12/1.
 */
public class BitmapCounter implements Comparable<BitmapCounter>, java.io.Serializable {

    private MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    private final int VERSION = 2;
    private Integer count;
    private ByteBuffer buffer;

    public BitmapCounter() {
    }

    public BitmapCounter(BitmapCounter another) {
        merge(another);
    }

    private MutableRoaringBitmap getBitmap() {
        if (!bitmap.isEmpty()) {
            return bitmap;
        }

        if (buffer != null) {
            @SuppressWarnings("unused")
            int version = buffer.getInt();
            @SuppressWarnings("unused")
            int size = buffer.getInt();
            count = buffer.getInt();

            try (DataInputStream is = new DataInputStream(new ByteBufferBackedInputStream(buffer))) {
                bitmap.deserialize(is);
            } catch (IOException e) {
                throw new RuntimeException("deserialize bitmap failed!");
            }

            buffer = null;
        }

        return bitmap;
    }

    public void clear() {
        getBitmap().clear();
    }

    public BitmapCounter clone() {
        BitmapCounter newCounter = new BitmapCounter();
        newCounter.bitmap = getBitmap().clone();
        return newCounter;
    }

    public void add(int value) {
        getBitmap().add(value);
        count = null;
    }

    public void add(String value) {
        if (value == null || value.isEmpty()) {
            return;
        }
        add(Integer.parseInt(value));
    }

    public void merge(BitmapCounter another) {
        getBitmap().or(another.getBitmap());
        count = null;
    }

    public void intersect(BitmapCounter another) {
        getBitmap().and(another.getBitmap());
        count = null;
    }

    public int getCount() {
        if (count != null) {
            return count;
        }

        return getBitmap().getCardinality();
    }

    public int getMemBytes() {
        return getBitmap().getSizeInBytes();
    }

    public Iterator<Integer> iterator() {
        return getBitmap().iterator();
    }

    public void writeRegisters(ByteBuffer out) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        MutableRoaringBitmap bitmap = getBitmap();
        bitmap.runOptimize();
        bitmap.serialize(dos);
        dos.close();
        ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());

        out.putInt(VERSION);
        out.putInt(bos.size() + 4 + 4 + 4);
        out.putInt(getCount());
        out.put(bb);
    }

    public void readRegisters(ByteBuffer in) throws IOException {
        int mark = in.position();
        int version = in.getInt();

        // keep forward compatibility
        if (version == VERSION) {
            int size = in.getInt();
            count = in.getInt();
            in.position(mark);
            buffer = cloneBuffer(in, size);
        } else {
            in.position(mark);
            try (DataInputStream is = new DataInputStream(new ByteBufferBackedInputStream(in))) {
                getBitmap().deserialize(is);
            }
        }
    }

    private ByteBuffer cloneBuffer(ByteBuffer src, int size) throws IOException {
        int mark = src.position();
        int limit = src.limit();

        src.limit(mark + size);
        ByteBuffer clone = ByteBuffer.allocate(size);
        clone.put(src.slice());
        clone.flip();

        src.position(mark + size);
        src.limit(limit);

        return clone;
    }

    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int len;
        int version = in.getInt();

        // keep forward compatibility
        if (version == VERSION) {
            len = in.getInt();
        } else {
            in.position(mark);
            try (DataInputStream is = new DataInputStream(new ByteBufferBackedInputStream(in))) {
                MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
                bitmap.deserialize(is);
                len = in.position() - mark;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        in.position(mark);
        return len;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + getBitmap().hashCode();
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
        return getBitmap().equals(other.getBitmap());
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
    
}
