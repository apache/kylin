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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import java.util.Locale;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;

public class AppendDictSlice {
    static final byte[] HEAD_MAGIC = new byte[] { 0x41, 0x70, 0x70, 0x65, 0x63, 0x64, 0x54, 0x72, 0x69, 0x65, 0x44,
            0x69, 0x63, 0x74 }; // "AppendTrieDict"
    static final int HEAD_SIZE_I = HEAD_MAGIC.length;
    static final int BIT_IS_LAST_CHILD = 0x80;
    static final int BIT_IS_END_OF_VALUE = 0x40;

    private byte[] trieBytes;

    // non-persistent part
    transient private int headSize;
    transient private int bodyLen;
    transient private int sizeChildOffset;

    transient private int nValues;
    transient private int sizeOfId;
    // mask MUST be long, since childOffset maybe 5 bytes at most
    transient private long childOffsetMask;
    transient private int firstByteOffset;

    public AppendDictSlice(byte[] bytes) {
        this.trieBytes = bytes;
        init();
    }

    private void init() {
        if (BytesUtil.compareBytes(HEAD_MAGIC, 0, trieBytes, 0, HEAD_MAGIC.length) != 0)
            throw new IllegalArgumentException("Wrong file type (magic does not match)");

        try {
            DataInputStream headIn = new DataInputStream(
                    new ByteArrayInputStream(trieBytes, HEAD_SIZE_I, trieBytes.length - HEAD_SIZE_I));
            this.headSize = headIn.readShort();
            this.bodyLen = headIn.readInt();
            this.nValues = headIn.readInt();
            this.sizeChildOffset = headIn.read();
            this.sizeOfId = headIn.read();

            this.childOffsetMask = ~(((long) (BIT_IS_LAST_CHILD | BIT_IS_END_OF_VALUE)) << ((sizeChildOffset - 1) * 8));
            this.firstByteOffset = sizeChildOffset + 1; // the offset from begin of node to its first value byte
        } catch (Exception e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            else
                throw new RuntimeException(e);
        }
    }

    public static AppendDictSlice deserializeFrom(DataInput in) throws IOException {
        byte[] headPartial = new byte[HEAD_MAGIC.length + Short.SIZE / Byte.SIZE + Integer.SIZE / Byte.SIZE];
        in.readFully(headPartial);

        if (BytesUtil.compareBytes(HEAD_MAGIC, 0, headPartial, 0, HEAD_MAGIC.length) != 0)
            throw new IllegalArgumentException("Wrong file type (magic does not match)");

        DataInputStream headIn = new DataInputStream(//
                new ByteArrayInputStream(headPartial, HEAD_SIZE_I, headPartial.length - HEAD_SIZE_I));
        int headSize = headIn.readShort();
        int bodyLen = headIn.readInt();
        headIn.close();

        byte[] all = new byte[headSize + bodyLen];
        System.arraycopy(headPartial, 0, all, 0, headPartial.length);
        in.readFully(all, headPartial.length, all.length - headPartial.length);

        return new AppendDictSlice(all);
    }

    public byte[] getFirstValue() {
        int nodeOffset = headSize;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        while (true) {
            int valueLen = BytesUtil.readUnsigned(trieBytes, nodeOffset + firstByteOffset - 1, 1);
            bytes.write(trieBytes, nodeOffset + firstByteOffset, valueLen);
            if (checkFlag(nodeOffset, BIT_IS_END_OF_VALUE)) {
                break;
            }
            nodeOffset = headSize
                    + (int) (BytesUtil.readLong(trieBytes, nodeOffset, sizeChildOffset) & childOffsetMask);
            if (nodeOffset == headSize) {
                break;
            }
        }
        return bytes.toByteArray();
    }

    /**
     * returns a code point from [0, nValues), preserving order of value
     *
     * @param n            -- the offset of current node
     * @param inp          -- input value bytes to lookup
     * @param o            -- offset in the input value bytes matched so far
     * @param inpEnd       -- end of input
     * @param roundingFlag -- =0: return -1 if not found
     *                     -- <0: return closest smaller if not found, return -1
     *                     -- >0: return closest bigger if not found, return nValues
     */
    private int lookupSeqNoFromValue(int n, byte[] inp, int o, int inpEnd, int roundingFlag) {
        while (true) {
            // match the current node
            int p = n + firstByteOffset; // start of node's value
            int end = p + BytesUtil.readUnsigned(trieBytes, p - 1, 1); // end of node's value
            for (; p < end && o < inpEnd; p++, o++) { // note matching start from [0]
                if (trieBytes[p] != inp[o]) {
                    return -1; // mismatch
                }
            }

            // node completely matched, is input all consumed?
            boolean isEndOfValue = checkFlag(n, BIT_IS_END_OF_VALUE);
            if (o == inpEnd) {
                return p == end && isEndOfValue ? BytesUtil.readUnsigned(trieBytes, end, sizeOfId) : -1;
            }

            // find a child to continue
            int c = headSize + (int) (BytesUtil.readLong(trieBytes, n, sizeChildOffset) & childOffsetMask);
            if (c == headSize) // has no children
                return -1;
            byte inpByte = inp[o];
            int comp;
            while (true) {
                p = c + firstByteOffset;
                comp = BytesUtil.compareByteUnsigned(trieBytes[p], inpByte);
                if (comp == 0) { // continue in the matching child, reset n and loop again
                    n = c;
                    break;
                } else if (comp < 0) { // try next child
                    if (checkFlag(c, BIT_IS_LAST_CHILD))
                        return -1;
                    c = p + BytesUtil.readUnsigned(trieBytes, p - 1, 1)
                            + (checkFlag(c, BIT_IS_END_OF_VALUE) ? sizeOfId : 0);
                } else { // children are ordered by their first value byte
                    return -1;
                }
            }
        }
    }

    private boolean checkFlag(int offset, int bit) {
        return (trieBytes[offset] & bit) > 0;
    }

    public int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
        int id = lookupSeqNoFromValue(headSize, value, offset, offset + len, roundingFlag);
        return id;
    }

    public AppendDictNode rebuildTrieTree() {
        return rebuildTrieTreeR(headSize, null);
    }

    private AppendDictNode rebuildTrieTreeR(int n, AppendDictNode parent) {
        AppendDictNode root = null;
        while (true) {
            int p = n + firstByteOffset;
            int childOffset = (int) (BytesUtil.readLong(trieBytes, n, sizeChildOffset) & childOffsetMask);
            int parLen = BytesUtil.readUnsigned(trieBytes, p - 1, 1);
            boolean isEndOfValue = checkFlag(n, BIT_IS_END_OF_VALUE);

            byte[] value = new byte[parLen];
            System.arraycopy(trieBytes, p, value, 0, parLen);

            AppendDictNode node = new AppendDictNode(value, isEndOfValue);
            if (isEndOfValue) {
                int id = BytesUtil.readUnsigned(trieBytes, p + parLen, sizeOfId);
                node.id = id;
            }

            if (parent == null) {
                root = node;
            } else {
                parent.addChild(node);
            }

            if (childOffset != 0) {
                rebuildTrieTreeR(childOffset + headSize, node);
            }

            if (checkFlag(n, BIT_IS_LAST_CHILD)) {
                break;
            } else {
                n += firstByteOffset + parLen + (isEndOfValue ? sizeOfId : 0);
            }
        }
        return root;
    }

    public boolean doCheck() {
        int offset = headSize;
        HashSet<Integer> parentSet = new HashSet<>();
        boolean lastChild = false;

        while (offset < trieBytes.length) {
            if (lastChild) {
                boolean contained = parentSet.remove(offset - headSize);
                // Can't find parent, the data is corrupted
                if (!contained) {
                    return false;
                }
                lastChild = false;
            }
            int p = offset + firstByteOffset;
            int childOffset = (int) (BytesUtil.readLong(trieBytes, offset, sizeChildOffset) & childOffsetMask);
            int parLen = BytesUtil.readUnsigned(trieBytes, p - 1, 1);
            boolean isEndOfValue = checkFlag(offset, BIT_IS_END_OF_VALUE);

            // Copy value overflow, the data is corrupted
            if (trieBytes.length < p + parLen) {
                return false;
            }

            // Check id is fine
            if (isEndOfValue) {
                BytesUtil.readUnsigned(trieBytes, p + parLen, sizeOfId);
            }

            // Record it if has children
            if (childOffset != 0) {
                parentSet.add(childOffset);
            }

            // brothers done, move to next parent
            if (checkFlag(offset, BIT_IS_LAST_CHILD)) {
                lastChild = true;
            }

            // move to next node
            offset += firstByteOffset + parLen + (isEndOfValue ? sizeOfId : 0);
        }

        // ParentMap is empty, meaning all nodes has parent, the data is correct
        return parentSet.isEmpty();
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "DictSlice[firstValue=%s, values=%d, bytes=%d]",
                Bytes.toStringBinary(getFirstValue()), nValues, bodyLen);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(trieBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AppendDictSlice)) {
            return false;
        }
        AppendDictSlice that = (AppendDictSlice) o;
        return Arrays.equals(this.trieBytes, that.trieBytes);
    }
}
