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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

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
        add(value, 0, value.length);
    }

    public void add(byte[] value, int offset, int length) {
        if (value == null || length == 0) {
            return;
        }

        add(new String(value, offset, length));
    }

    public void add(String value) {
        if (value == null || value.isEmpty()) {
            return;
        }
        add(Integer.parseInt(value));
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

    public Iterator<Integer> iterator() {
        return bitmap.iterator();
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
        input.reset(new ByteBuffer[] { in });
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
        input.reset(new ByteBuffer[] { in });
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        try {
            bitmap.deserialize(input);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        len = in.position() - mark;
        in.position(mark);
        return len;
    }

    static class DataInputByteBuffer extends DataInputStream {
        private DataInputByteBuffer.Buffer buffers;

        public DataInputByteBuffer() {
            this(new DataInputByteBuffer.Buffer());
        }

        private DataInputByteBuffer(DataInputByteBuffer.Buffer buffers) {
            super(buffers);
            this.buffers = buffers;
        }

        public void reset(ByteBuffer... input) {
            this.buffers.reset(input);
        }

        public ByteBuffer[] getData() {
            return this.buffers.getData();
        }

        public int getPosition() {
            return this.buffers.getPosition();
        }

        public int getLength() {
            return this.buffers.getLength();
        }

        private static class Buffer extends InputStream {
            private final byte[] scratch;
            ByteBuffer[] buffers;
            int bidx;
            int pos;
            int length;

            private Buffer() {
                this.scratch = new byte[1];
                this.buffers = new ByteBuffer[0];
            }

            public int read() {
                return -1 == this.read(this.scratch, 0, 1) ? -1 : this.scratch[0] & 255;
            }

            public int read(byte[] b, int off, int len) {
                if (this.bidx >= this.buffers.length) {
                    return -1;
                } else {
                    int cur = 0;

                    do {
                        int rem = Math.min(len, this.buffers[this.bidx].remaining());
                        this.buffers[this.bidx].get(b, off, rem);
                        cur += rem;
                        off += rem;
                        len -= rem;
                    } while (len > 0 && ++this.bidx < this.buffers.length);

                    this.pos += cur;
                    return cur;
                }
            }

            public void reset(ByteBuffer[] buffers) {
                this.bidx = this.pos = this.length = 0;
                this.buffers = buffers;
                ByteBuffer[] arr$ = buffers;
                int len$ = buffers.length;

                for (int i$ = 0; i$ < len$; ++i$) {
                    ByteBuffer b = arr$[i$];
                    this.length += b.remaining();
                }

            }

            public int getPosition() {
                return this.pos;
            }

            public int getLength() {
                return this.length;
            }

            public ByteBuffer[] getData() {
                return this.buffers;
            }
        }
    }
}
