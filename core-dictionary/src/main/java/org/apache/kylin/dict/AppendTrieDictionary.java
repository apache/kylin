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

package org.apache.kylin.dict;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A dictionary based on Trie data structure that maps enumerations of byte[] to
 * int IDs, used for global dictionary.
 *
 * Trie data is split into sub trees, called {@link DictSlice}, and stored in a {@link CachedTreeMap} with a configurable cache size.
 *
 * With Trie the memory footprint of the mapping is kinda minimized at the cost
 * CPU, if compared to HashMap of ID Arrays. Performance test shows Trie is
 * roughly 10 times slower, so there's a cache layer overlays on top of Trie and
 * gracefully fall back to Trie using a weak reference.
 *
 * The implementation is NOT thread-safe for now.
 *
 * TODO making it thread-safe
 *
 * @author sunyerui
 */
@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
public class AppendTrieDictionary<T> extends CacheDictionary<T> {

    public static final byte[] HEAD_MAGIC = new byte[] { 0x41, 0x70, 0x70, 0x65, 0x63, 0x64, 0x54, 0x72, 0x69, 0x65, 0x44, 0x69, 0x63, 0x74 }; // "AppendTrieDict"
    public static final int HEAD_SIZE_I = HEAD_MAGIC.length;

    public static final int BIT_IS_LAST_CHILD = 0x80;
    public static final int BIT_IS_END_OF_VALUE = 0x40;

    private static final Logger logger = LoggerFactory.getLogger(AppendTrieDictionary.class);

    transient private String baseDir;
    transient private int maxId;
    transient private int maxValueLength;
    transient private int nValues;

    volatile private TreeMap<DictSliceKey, DictSlice> dictSliceMap;

    // Constructor both for build and deserialize
    public AppendTrieDictionary() {
        enableCache();
    }

    public void initParams(String baseDir, int baseId, int maxId, int maxValueLength, int nValues, BytesConverter bytesConverter) throws IOException {
        this.baseDir = baseDir;
        this.baseId = baseId;
        this.maxId = maxId;
        this.maxValueLength = maxValueLength;
        this.nValues = nValues;
        this.bytesConvert = bytesConverter;
    }

    public void initDictSliceMap(CachedTreeMap dictMap) throws IOException {
        int maxVersions = KylinConfig.getInstanceFromEnv().getAppendDictMaxVersions();
        long versionTTL = KylinConfig.getInstanceFromEnv().getAppendDictVersionTTL();
        CachedTreeMap newDictSliceMap = CachedTreeMap.CachedTreeMapBuilder.newBuilder().maxSize(1).baseDir(baseDir).immutable(true).maxVersions(maxVersions).versionTTL(versionTTL).keyClazz(DictSliceKey.class).valueClazz(DictSlice.class).build();
        newDictSliceMap.loadEntry(dictMap);
        this.dictSliceMap = newDictSliceMap;
    }

    public byte[] writeDictMap() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        ((Writable) dictSliceMap).write(out);
        byte[] dictMapBytes = buf.toByteArray();
        buf.close();
        out.close();

        return dictMapBytes;
    }

    // The dict id starts from 1 to 2147483647 and 2147483648 to -2, leave 0 and -1 used for uninitialized state
    public static void checkValidId(int id) {
        if (id == 0 || id == -1) {
            throw new IllegalArgumentException("AppendTrieDictionary Id Overflow Unsigned Integer Size 4294967294");
        }
    }

    public static class DictSliceKey implements WritableComparable, java.io.Serializable {
        byte[] key;

        public static DictSliceKey wrap(byte[] key) {
            DictSliceKey dictKey = new DictSliceKey();
            dictKey.key = key;
            return dictKey;
        }

        @Override
        public String toString() {
            return Bytes.toStringBinary(key);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

        @Override
        public int compareTo(Object o) {
            if (!(o instanceof DictSliceKey)) {
                return -1;
            }
            DictSliceKey other = (DictSliceKey) o;
            return Bytes.compareTo(key, other.key);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(key.length);
            out.write(key);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            key = new byte[in.readInt()];
            in.readFully(key);
        }
    }

    public static class DictSlice<T> implements Writable, java.io.Serializable {
        public DictSlice() {
        }

        public DictSlice(byte[] trieBytes) {
            init(trieBytes);
        }

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

        private void init(byte[] trieBytes) {
            this.trieBytes = trieBytes;
            if (BytesUtil.compareBytes(HEAD_MAGIC, 0, trieBytes, 0, HEAD_MAGIC.length) != 0)
                throw new IllegalArgumentException("Wrong file type (magic does not match)");

            try {
                DataInputStream headIn = new DataInputStream(new ByteArrayInputStream(trieBytes, HEAD_SIZE_I, trieBytes.length - HEAD_SIZE_I));
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

        public byte[] getFirstValue() {
            int nodeOffset = headSize;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            while (true) {
                int valueLen = BytesUtil.readUnsigned(trieBytes, nodeOffset + firstByteOffset - 1, 1);
                bytes.write(trieBytes, nodeOffset + firstByteOffset, valueLen);
                if (checkFlag(nodeOffset, BIT_IS_END_OF_VALUE)) {
                    break;
                }
                nodeOffset = headSize + (int) (BytesUtil.readLong(trieBytes, nodeOffset, sizeChildOffset) & childOffsetMask);
                if (nodeOffset == headSize) {
                    break;
                }
            }
            return bytes.toByteArray();
        }

        /**
         * returns a code point from [0, nValues), preserving order of value
         *
         * @param n
         *            -- the offset of current node
         * @param inp
         *            -- input value bytes to lookup
         * @param o
         *            -- offset in the input value bytes matched so far
         * @param inpEnd
         *            -- end of input
         * @param roundingFlag
         *            -- =0: return -1 if not found
         *            -- <0: return closest smaller if not found, return -1
         *            -- >0: return closest bigger if not found, return nValues
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
                        c = p + BytesUtil.readUnsigned(trieBytes, p - 1, 1) + (checkFlag(c, BIT_IS_END_OF_VALUE) ? sizeOfId : 0);
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

        private DictNode rebuildTrieTree() {
            return rebuildTrieTreeR(headSize, null);
        }

        private DictNode rebuildTrieTreeR(int n, DictNode parent) {
            DictNode root = null;
            while (true) {
                int p = n + firstByteOffset;
                int childOffset = (int) (BytesUtil.readLong(trieBytes, n, sizeChildOffset) & childOffsetMask);
                int parLen = BytesUtil.readUnsigned(trieBytes, p - 1, 1);
                boolean isEndOfValue = checkFlag(n, BIT_IS_END_OF_VALUE);

                byte[] value = new byte[parLen];
                System.arraycopy(trieBytes, p, value, 0, parLen);

                DictNode node = new DictNode(value, isEndOfValue);
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

        public void write(DataOutput out) throws IOException {
            out.write(trieBytes);
        }

        public void readFields(DataInput in) throws IOException {
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

            init(all);
        }

        public static DictNode rebuildNodeByDeserialize(DataInput in) throws IOException {
            DictSlice slice = new DictSlice();
            slice.readFields(in);
            return slice.rebuildTrieTree();
        }

        @Override
        public String toString() {
            return String.format("DictSlice[firstValue=%s, values=%d, bytes=%d]", Bytes.toStringBinary(getFirstValue()), nValues, bodyLen);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(trieBytes);
        }

        @Override
        public boolean equals(Object o) {
            if ((o instanceof AppendTrieDictionary.DictSlice) == false) {
                logger.info("Equals return false because it's not DictInfo");
                return false;
            }
            DictSlice that = (DictSlice) o;
            return Arrays.equals(this.trieBytes, that.trieBytes);
        }
    }

    public static class DictNode implements Writable, java.io.Serializable {
        public byte[] part;
        public int id = -1;
        public boolean isEndOfValue;
        public ArrayList<DictNode> children = new ArrayList<>();

        public int nValuesBeneath;
        public DictNode parent;
        public int childrenCount = 1;

        public DictNode() {
        }

        public void clone(DictNode o) {
            this.part = o.part;
            this.id = o.id;
            this.isEndOfValue = o.isEndOfValue;
            this.children = o.children;
            for (DictNode child : o.children) {
                child.parent = this;
            }
            this.nValuesBeneath = o.nValuesBeneath;
            this.parent = o.parent;
            this.childrenCount = o.childrenCount;
        }

        DictNode(byte[] value, boolean isEndOfValue) {
            reset(value, isEndOfValue);
        }

        DictNode(byte[] value, boolean isEndOfValue, ArrayList<DictNode> children) {
            reset(value, isEndOfValue, children);
        }

        void reset(byte[] value, boolean isEndOfValue) {
            reset(value, isEndOfValue, new ArrayList<DictNode>());
        }

        void reset(byte[] value, boolean isEndOfValue, ArrayList<DictNode> children) {
            this.part = value;
            this.isEndOfValue = isEndOfValue;
            clearChild();
            for (DictNode child : children) {
                addChild(child);
            }
            this.id = -1;
        }

        void clearChild() {
            this.children.clear();
            int childrenCountDelta = this.childrenCount - 1;
            for (DictNode p = this; p != null; p = p.parent) {
                p.childrenCount -= childrenCountDelta;
            }
        }

        void addChild(DictNode child) {
            addChild(-1, child);
        }

        void addChild(int index, DictNode child) {
            child.parent = this;
            if (index < 0) {
                this.children.add(child);
            } else {
                this.children.add(index, child);
            }
            for (DictNode p = this; p != null; p = p.parent) {
                p.childrenCount += child.childrenCount;
            }
        }

        public DictNode removeChild(int index) {
            DictNode child = children.remove(index);
            child.parent = null;
            for (DictNode p = this; p != null; p = p.parent) {
                p.childrenCount -= child.childrenCount;
            }
            return child;
        }

        public DictNode duplicateNode() {
            DictNode newChild = new DictNode(part, false);
            newChild.parent = parent;
            if (parent != null) {
                int index = parent.children.indexOf(this);
                parent.addChild(index + 1, newChild);
            }
            return newChild;
        }

        public byte[] firstValue() {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DictNode p = this;
            while (true) {
                bytes.write(p.part, 0, p.part.length);
                if (p.isEndOfValue || p.children.size() == 0) {
                    break;
                }
                p = p.children.get(0);
            }
            return bytes.toByteArray();
        }

        public static DictNode splitNodeTree(DictNode splitNode) {
            if (splitNode == null) {
                return null;
            }
            DictNode current = splitNode;
            DictNode p = current.parent;
            while (p != null) {
                int index = p.children.indexOf(current);
                assert index != -1;
                DictNode newParent = p.duplicateNode();
                for (int i = p.children.size() - 1; i >= index; i--) {
                    DictNode child = p.removeChild(i);
                    newParent.addChild(0, child);
                }
                current = newParent;
                p = p.parent;
            }
            return current;
        }

        public static void mergeSingleByteNode(DictNode root, int leftOrRight) {
            DictNode current = root;
            DictNode child;
            while (!current.children.isEmpty()) {
                child = leftOrRight == 0 ? current.children.get(0) : current.children.get(current.children.size() - 1);
                if (current.children.size() > 1 || current.isEndOfValue) {
                    current = child;
                    continue;
                }
                byte[] newValue = new byte[current.part.length + child.part.length];
                System.arraycopy(current.part, 0, newValue, 0, current.part.length);
                System.arraycopy(child.part, 0, newValue, current.part.length, child.part.length);
                current.reset(newValue, child.isEndOfValue, child.children);
                current.id = child.id;
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            byte[] bytes = buildTrieBytes();
            out.write(bytes);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            DictNode root = DictSlice.rebuildNodeByDeserialize(in);
            this.clone(root);
        }

        protected byte[] buildTrieBytes() {
            Stats stats = Stats.stats(this);
            int sizeChildOffset = stats.mbpn_sizeChildOffset;
            int sizeId = stats.mbpn_sizeId;

            // write head
            byte[] head;
            try {
                ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
                DataOutputStream headOut = new DataOutputStream(byteBuf);
                headOut.write(AppendTrieDictionary.HEAD_MAGIC);
                headOut.writeShort(0); // head size, will back fill
                headOut.writeInt(stats.mbpn_footprint); // body size
                headOut.writeInt(stats.nValues);
                headOut.write(sizeChildOffset);
                headOut.write(sizeId);
                headOut.close();
                head = byteBuf.toByteArray();
                BytesUtil.writeUnsigned(head.length, head, AppendTrieDictionary.HEAD_SIZE_I, 2);
            } catch (IOException e) {
                throw new RuntimeException(e); // shall not happen, as we are
            }

            byte[] trieBytes = new byte[stats.mbpn_footprint + head.length];
            System.arraycopy(head, 0, trieBytes, 0, head.length);

            LinkedList<DictNode> open = new LinkedList<DictNode>();
            IdentityHashMap<DictNode, Integer> offsetMap = new IdentityHashMap<DictNode, Integer>();

            // write body
            int o = head.length;
            offsetMap.put(this, o);
            o = build_writeNode(this, o, true, sizeChildOffset, sizeId, trieBytes);
            if (this.children.isEmpty() == false)
                open.addLast(this);

            while (open.isEmpty() == false) {
                DictNode parent = open.removeFirst();
                build_overwriteChildOffset(offsetMap.get(parent), o - head.length, sizeChildOffset, trieBytes);
                for (int i = 0; i < parent.children.size(); i++) {
                    DictNode c = parent.children.get(i);
                    boolean isLastChild = (i == parent.children.size() - 1);
                    offsetMap.put(c, o);
                    o = build_writeNode(c, o, isLastChild, sizeChildOffset, sizeId, trieBytes);
                    if (c.children.isEmpty() == false)
                        open.addLast(c);
                }
            }

            if (o != trieBytes.length)
                throw new RuntimeException();
            return trieBytes;
        }

        private void build_overwriteChildOffset(int parentOffset, int childOffset, int sizeChildOffset, byte[] trieBytes) {
            int flags = (int) trieBytes[parentOffset] & (TrieDictionary.BIT_IS_LAST_CHILD | TrieDictionary.BIT_IS_END_OF_VALUE);
            BytesUtil.writeUnsigned(childOffset, trieBytes, parentOffset, sizeChildOffset);
            trieBytes[parentOffset] |= flags;
        }

        private int build_writeNode(DictNode n, int offset, boolean isLastChild, int sizeChildOffset, int sizeId, byte[] trieBytes) {
            int o = offset;

            // childOffset
            if (isLastChild)
                trieBytes[o] |= TrieDictionary.BIT_IS_LAST_CHILD;
            if (n.isEndOfValue)
                trieBytes[o] |= TrieDictionary.BIT_IS_END_OF_VALUE;
            o += sizeChildOffset;

            // nValueBytes
            if (n.part.length > 255)
                throw new RuntimeException("Value length is " + n.part.length + " and larger than 255: " + Bytes.toStringBinary(n.part));
            BytesUtil.writeUnsigned(n.part.length, trieBytes, o, 1);
            o++;

            // valueBytes
            System.arraycopy(n.part, 0, trieBytes, o, n.part.length);
            o += n.part.length;

            if (n.isEndOfValue) {
                checkValidId(n.id);
                BytesUtil.writeUnsigned(n.id, trieBytes, o, sizeId);
                o += sizeId;
            }

            return o;
        }

        @Override
        public String toString() {
            return String.format("DictNode[root=%s, nodes=%d, firstValue=%s]", Bytes.toStringBinary(part), childrenCount, Bytes.toStringBinary(firstValue()));
        }
    }

    public static class Stats {
        public interface Visitor {
            void visit(DictNode n, int level);
        }

        private static void traverseR(DictNode node, Visitor visitor, int level) {
            visitor.visit(node, level);
            for (DictNode c : node.children)
                traverseR(c, visitor, level + 1);
        }

        private static void traversePostOrderR(DictNode node, Visitor visitor, int level) {
            for (DictNode c : node.children)
                traversePostOrderR(c, visitor, level + 1);
            visitor.visit(node, level);
        }

        public int nValues; // number of values in total
        public int nValueBytesPlain; // number of bytes for all values
        // uncompressed
        public int nValueBytesCompressed; // number of values bytes in Trie
        // (compressed)
        public int maxValueLength; // size of longest value in bytes

        // the trie is multi-byte-per-node
        public int mbpn_nNodes; // number of nodes in trie
        public int mbpn_trieDepth; // depth of trie
        public int mbpn_maxFanOut; // the maximum no. children
        public int mbpn_nChildLookups; // number of child lookups during lookup
        // every value once
        public int mbpn_nTotalFanOut; // the sum of fan outs during lookup every
        // value once
        public int mbpn_sizeValueTotal; // the sum of value space in all nodes
        public int mbpn_sizeNoValueBytes; // size of field noValueBytes
        public int mbpn_sizeChildOffset; // size of field childOffset, points to
        // first child in flattened array
        public int mbpn_sizeId; // size of id value, always be 4
        public int mbpn_footprint; // MBPN footprint in bytes

        /**
         * out print some statistics of the trie and the dictionary built from it
         */
        public static Stats stats(DictNode root) {
            // calculate nEndValueBeneath
            traversePostOrderR(root, new Visitor() {
                @Override
                public void visit(DictNode n, int level) {
                    n.nValuesBeneath = n.isEndOfValue ? 1 : 0;
                    for (DictNode c : n.children)
                        n.nValuesBeneath += c.nValuesBeneath;
                }
            }, 0);

            // run stats
            final Stats s = new Stats();
            final ArrayList<Integer> lenAtLvl = new ArrayList<Integer>();
            traverseR(root, new Visitor() {
                @Override
                public void visit(DictNode n, int level) {
                    if (n.isEndOfValue)
                        s.nValues++;
                    s.nValueBytesPlain += n.part.length * n.nValuesBeneath;
                    s.nValueBytesCompressed += n.part.length;
                    s.mbpn_nNodes++;
                    if (s.mbpn_trieDepth < level + 1)
                        s.mbpn_trieDepth = level + 1;
                    if (n.children.size() > 0) {
                        if (s.mbpn_maxFanOut < n.children.size())
                            s.mbpn_maxFanOut = n.children.size();
                        int childLookups = n.nValuesBeneath - (n.isEndOfValue ? 1 : 0);
                        s.mbpn_nChildLookups += childLookups;
                        s.mbpn_nTotalFanOut += childLookups * n.children.size();
                    }

                    if (level < lenAtLvl.size())
                        lenAtLvl.set(level, n.part.length);
                    else
                        lenAtLvl.add(n.part.length);
                    int lenSoFar = 0;
                    for (int i = 0; i <= level; i++)
                        lenSoFar += lenAtLvl.get(i);
                    if (lenSoFar > s.maxValueLength)
                        s.maxValueLength = lenSoFar;
                }
            }, 0);

            // flatten trie footprint calculation, case of Multi-Byte-Per-DictNode
            s.mbpn_sizeId = 4;
            s.mbpn_sizeValueTotal = s.nValueBytesCompressed + s.nValues * s.mbpn_sizeId;
            s.mbpn_sizeNoValueBytes = 1;
            s.mbpn_sizeChildOffset = 5;
            s.mbpn_footprint = s.mbpn_sizeValueTotal + s.mbpn_nNodes * (s.mbpn_sizeNoValueBytes + s.mbpn_sizeChildOffset);
            while (true) { // minimize the offset size to match the footprint
                int t = s.mbpn_sizeValueTotal + s.mbpn_nNodes * (s.mbpn_sizeNoValueBytes + s.mbpn_sizeChildOffset - 1);
                // *4 because 2 MSB of offset is used for isEndOfValue & isEndChild flag
                // expand t to long before *4, avoiding exceed Integer.MAX_VALUE
                if (BytesUtil.sizeForValue((long) t * 4) <= s.mbpn_sizeChildOffset - 1) {
                    s.mbpn_sizeChildOffset--;
                    s.mbpn_footprint = t;
                } else
                    break;
            }

            return s;
        }

        /**
         * out print trie for debug
         */
        public void print(DictNode root) {
            print(root, System.out);
        }

        public void print(DictNode root, final PrintStream out) {
            traverseR(root, new Visitor() {
                @Override
                public void visit(DictNode n, int level) {
                    try {
                        for (int i = 0; i < level; i++)
                            out.print("  ");
                        out.print(new String(n.part, "UTF-8"));
                        out.print(" - ");
                        if (n.nValuesBeneath > 0)
                            out.print(n.nValuesBeneath);
                        if (n.isEndOfValue)
                            out.print("* [" + n.id + "]");
                        out.print("\n");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }, 0);
        }
    }

    public static class Builder<T> {
        private static ConcurrentHashMap<String, Pair<Integer, Builder>> builderInstanceAndCountMap = new ConcurrentHashMap();

        public static Builder getInstance(String resourcePath) throws IOException {
            return getInstance(resourcePath, null);
        }

        public synchronized static Builder getInstance(String resourcePath, AppendTrieDictionary dict) throws IOException {
            Pair<Integer, Builder> entry = builderInstanceAndCountMap.get(resourcePath);
            if (entry == null) {
                entry = new Pair<>(0, createNewBuilder(resourcePath, dict));
                builderInstanceAndCountMap.put(resourcePath, entry);
            }
            entry.setFirst(entry.getFirst() + 1);
            return entry.getSecond();
        }

        // return true if entry still in map
        private synchronized static boolean releaseInstance(String resourcePath) {
            Pair<Integer, Builder> entry = builderInstanceAndCountMap.get(resourcePath);
            if (entry != null) {
                entry.setFirst(entry.getFirst() - 1);
                if (entry.getFirst() <= 0) {
                    builderInstanceAndCountMap.remove(resourcePath);
                    return false;
                }
                return true;
            }
            return false;
        }

        public static Builder createNewBuilder(String resourcePath, AppendTrieDictionary existDict) throws IOException {
            String dictDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "resources/GlobalDict" + resourcePath + "/";

            AppendTrieDictionary dictToUse = existDict;
            if (dictToUse == null) {
                // Try to load the existing dict from cache, making sure there's only the same one object in memory
                NavigableSet<String> dicts = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getStore().listResources(resourcePath);
                ArrayList<String> appendDicts = new ArrayList<>();
                if (dicts != null && !dicts.isEmpty()) {
                    for (String dict : dicts) {
                        DictionaryInfo info = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getStore().getResource(dict, DictionaryInfo.class, DictionaryInfoSerializer.INFO_SERIALIZER);
                        if (info.getDictionaryClass().equals(AppendTrieDictionary.class.getName())) {
                            appendDicts.add(dict);
                        }
                    }
                }
                if (appendDicts.isEmpty()) {
                    dictToUse = null;
                } else if (appendDicts.size() == 1) {
                    dictToUse = (AppendTrieDictionary) DictionaryManager.getInstance(KylinConfig.getInstanceFromEnv()).getDictionary(appendDicts.get(0));
                } else {
                    throw new IllegalStateException(String.format("GlobalDict %s should have 0 or 1 append dict but %d", resourcePath, appendDicts.size()));
                }
            }

            AppendTrieDictionary.Builder<String> builder;
            if (dictToUse == null) {
                logger.info("GlobalDict {} is empty, create new one", resourcePath);
                builder = new Builder<>(resourcePath, null, dictDir, 0, 0, 0, new StringBytesConverter(), null);
            } else {
                logger.info("GlobalDict {} exist, append value", resourcePath);
                builder = new Builder<>(resourcePath, dictToUse, dictToUse.baseDir, dictToUse.maxId, dictToUse.maxValueLength, dictToUse.nValues, dictToUse.bytesConvert, dictToUse.writeDictMap());
            }

            return builder;
        }

        private final String resourcePath;
        private final String baseDir;
        private int maxId;
        private int maxValueLength;
        private int nValues;
        private final BytesConverter<T> bytesConverter;

        private final AppendTrieDictionary dict;

        private final TreeMap<DictSliceKey, DictNode> mutableDictSliceMap;
        private int MAX_ENTRY_IN_SLICE = 10_000_000;
        private static final double MAX_ENTRY_OVERHEAD_FACTOR = 1.0;

        private int processedCount = 0;

        // Constructor for a new Dict
        private Builder(String resourcePath, AppendTrieDictionary dict, String baseDir, int maxId, int maxValueLength, int nValues, BytesConverter<T> bytesConverter, byte[] dictMapBytes) throws IOException {
            this.resourcePath = resourcePath;
            if (dict == null) {
                this.dict = new AppendTrieDictionary<T>();
            } else {
                this.dict = dict;
            }
            this.baseDir = baseDir;
            this.maxId = maxId;
            this.maxValueLength = maxValueLength;
            this.nValues = nValues;
            this.bytesConverter = bytesConverter;

            MAX_ENTRY_IN_SLICE = KylinConfig.getInstanceFromEnv().getAppendDictEntrySize();
            int maxVersions = KylinConfig.getInstanceFromEnv().getAppendDictMaxVersions();
            long versionTTL = KylinConfig.getInstanceFromEnv().getAppendDictVersionTTL();
            // create a new cached map with baseDir
            mutableDictSliceMap = CachedTreeMap.CachedTreeMapBuilder.newBuilder().maxSize(1).baseDir(baseDir).maxVersions(maxVersions).versionTTL(versionTTL).keyClazz(DictSliceKey.class).valueClazz(DictNode.class).immutable(false).build();
            if (dictMapBytes != null) {
                ((Writable) mutableDictSliceMap).readFields(new DataInputStream(new ByteArrayInputStream(dictMapBytes)));
            }
        }

        public void addValue(T value) {
            addValue(bytesConverter.convertToBytes(value));
        }

        private synchronized void addValue(byte[] value) {
            if (++processedCount % 1_000_000 == 0) {
                logger.debug("add value count " + processedCount);
            }
            maxValueLength = Math.max(maxValueLength, value.length);

            if (mutableDictSliceMap.isEmpty()) {
                DictNode root = new DictNode(new byte[0], false);
                mutableDictSliceMap.put(DictSliceKey.wrap(new byte[0]), root);
            }
            DictSliceKey sliceKey = mutableDictSliceMap.floorKey(DictSliceKey.wrap(value));
            if (sliceKey == null) {
                sliceKey = mutableDictSliceMap.firstKey();
            }
            DictNode root = mutableDictSliceMap.get(sliceKey);
            addValueR(root, value, 0);
            if (root.childrenCount > MAX_ENTRY_IN_SLICE * MAX_ENTRY_OVERHEAD_FACTOR) {
                mutableDictSliceMap.remove(sliceKey);
                DictNode newRoot = splitNodeTree(root);
                DictNode.mergeSingleByteNode(root, 1);
                DictNode.mergeSingleByteNode(newRoot, 0);
                mutableDictSliceMap.put(DictSliceKey.wrap(root.firstValue()), root);
                mutableDictSliceMap.put(DictSliceKey.wrap(newRoot.firstValue()), newRoot);
            }
        }

        private DictNode splitNodeTree(DictNode root) {
            DictNode parent = root;
            DictNode splitNode;
            int childCountToSplit = (int) (MAX_ENTRY_IN_SLICE * MAX_ENTRY_OVERHEAD_FACTOR / 2);
            while (true) {
                List<DictNode> children = parent.children;
                if (children.size() == 0) {
                    splitNode = parent;
                    break;
                } else if (children.size() == 1) {
                    parent = children.get(0);
                    continue;
                } else {
                    for (int i = children.size() - 1; i >= 0; i--) {
                        parent = children.get(i);
                        if (childCountToSplit > children.get(i).childrenCount) {
                            childCountToSplit -= children.get(i).childrenCount;
                        } else {
                            childCountToSplit--;
                            break;
                        }
                    }
                }
            }
            return DictNode.splitNodeTree(splitNode);
        }

        private int createNextId() {
            int id = ++maxId;
            checkValidId(id);
            nValues++;
            return id;
        }

        // Only used for test
        public void setMaxId(int id) {
            this.maxId = id;
        }

        // When add a new node, the value part maybe over 255 bytes, need split it into a sub tree
        private DictNode addNodeMaybeOverflow(byte[] value, int start, int end) {
            DictNode head = null;
            DictNode current = null;
            for (; start + 255 < end; start += 255) {
                DictNode c = new DictNode(BytesUtil.subarray(value, start, start + 255), false);
                if (head == null) {
                    head = c;
                    current = c;
                } else {
                    current.addChild(c);
                    current = c;
                }
            }
            DictNode last = new DictNode(BytesUtil.subarray(value, start, end), true);
            last.id = createNextId();
            if (head == null) {
                head = last;
            } else {
                current.addChild(last);
            }
            return head;
        }

        private void addValueR(DictNode node, byte[] value, int start) {
            // match the value part of current node
            int i = 0, j = start;
            int n = node.part.length, nn = value.length;
            int comp = 0;
            for (; i < n && j < nn; i++, j++) {
                comp = BytesUtil.compareByteUnsigned(node.part[i], value[j]);
                if (comp != 0)
                    break;
            }

            if (j == nn) {
                // if value fully matched within the current node
                if (i == n) {
                    // if equals to current node, just mark end of value
                    if (!node.isEndOfValue) {
                        // if the first match, assign an Id to nodt
                        node.id = createNextId();
                    }
                    node.isEndOfValue = true;
                } else {
                    // otherwise, split the current node into two
                    DictNode c = new DictNode(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);
                    c.id = node.id;
                    node.reset(BytesUtil.subarray(node.part, 0, i), true);
                    node.addChild(c);
                    node.id = createNextId();
                }
                return;
            }

            // if partially matched the current, split the current node, add the new
            // value, make a 3-way
            if (i < n) {
                DictNode c1 = new DictNode(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);
                c1.id = node.id;
                DictNode c2 = addNodeMaybeOverflow(value, j, nn);
                node.reset(BytesUtil.subarray(node.part, 0, i), false);
                if (comp < 0) {
                    node.addChild(c1);
                    node.addChild(c2);
                } else {
                    node.addChild(c2);
                    node.addChild(c1);
                }
                return;
            }

            // out matched the current, binary search the next byte for a child node
            // to continue
            byte lookfor = value[j];
            int lo = 0;
            int hi = node.children.size() - 1;
            int mid = 0;
            boolean found = false;
            comp = 0;
            while (!found && lo <= hi) {
                mid = lo + (hi - lo) / 2;
                DictNode c = node.children.get(mid);
                comp = BytesUtil.compareByteUnsigned(lookfor, c.part[0]);
                if (comp < 0)
                    hi = mid - 1;
                else if (comp > 0)
                    lo = mid + 1;
                else
                    found = true;
            }
            if (found) {
                // found a child node matching the first byte, continue in that child
                addValueR(node.children.get(mid), value, j);
            } else {
                // otherwise, make the value a new child
                DictNode c = addNodeMaybeOverflow(value, j, nn);
                node.addChild(comp <= 0 ? mid : mid + 1, c);
            }
        }

        public synchronized AppendTrieDictionary<T> build(int baseId) throws IOException {
            boolean keepAppend = releaseInstance(resourcePath);
            CachedTreeMap dictSliceMap = (CachedTreeMap) mutableDictSliceMap;
            dict.initParams(baseDir, baseId, maxId, maxValueLength, nValues, bytesConverter);
            dict.flushIndex(dictSliceMap, keepAppend);
            dict.initDictSliceMap(dictSliceMap);

            return dict;
        }
    }

    @Override
    protected int getIdFromValueBytesWithoutCache(byte[] value, int offset, int len, int roundingFlag) {
        if (dictSliceMap.isEmpty()) {
            return -1;
        }
        byte[] tempVal = new byte[len];
        System.arraycopy(value, offset, tempVal, 0, len);
        DictSliceKey sliceKey = dictSliceMap.floorKey(DictSliceKey.wrap(tempVal));
        if (sliceKey == null) {
            sliceKey = dictSliceMap.firstKey();
        }
        DictSlice slice = dictSliceMap.get(sliceKey);
        int id = slice.getIdFromValueBytesImpl(value, offset, len, roundingFlag);
        return id;
    }

    @Override
    protected byte[] getValueBytesFromIdWithoutCache(int id) {
        throw new UnsupportedOperationException("AppendTrieDictionary can't retrive value from id");
    }

    @Override
    public int getMinId() {
        return baseId;
    }

    @Override
    public int getMaxId() {
        return maxId;
    }

    @Override
    public int getSizeOfId() {
        return 4;
    }

    @Override
    public int getSizeOfValue() {
        return maxValueLength;
    }

    public void flushIndex(CachedTreeMap dictSliceMap, boolean keepAppend) throws IOException {
        try (FSDataOutputStream indexOut = dictSliceMap.openIndexOutput()) {
            indexOut.writeInt(baseId);
            indexOut.writeInt(maxId);
            indexOut.writeInt(maxValueLength);
            indexOut.writeInt(nValues);
            indexOut.writeUTF(bytesConvert.getClass().getName());
            dictSliceMap.write(indexOut);
            dictSliceMap.commit(keepAppend);
        }
    }

    @Override
    public AppendTrieDictionary copyToAnotherMeta(KylinConfig srcConfig, KylinConfig dstConfig) throws IOException {
        //copy appendDict
        Path base = new Path(baseDir);
        FileSystem srcFs = HadoopUtil.getFileSystem(base);
        Path srcPath = CachedTreeMap.getLatestVersion(HadoopUtil.getCurrentConfiguration(), srcFs, base);
        Path dstPath = new Path(srcPath.toString().replaceFirst(srcConfig.getHdfsWorkingDirectory(), dstConfig.getHdfsWorkingDirectory()));
        logger.info("Copy appendDict from {} to {}", srcPath, dstPath);

        FileSystem dstFs = HadoopUtil.getFileSystem(dstPath);
        if (dstFs.exists(dstPath)) {
            logger.info("Delete existing AppendDict {}", dstPath);
            dstFs.delete(dstPath, true);
        }
        FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, true, HadoopUtil.getCurrentConfiguration());

        // init new AppendTrieDictionary
        AppendTrieDictionary newDict = new AppendTrieDictionary();
        newDict.initParams(baseDir.replaceFirst(srcConfig.getHdfsWorkingDirectory(), dstConfig.getHdfsWorkingDirectory()), baseId, maxId, maxValueLength, nValues, bytesConvert);
        newDict.initDictSliceMap((CachedTreeMap) dictSliceMap);

        return newDict;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(baseDir);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String baseDir = in.readUTF();
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        try (FSDataInputStream input = CachedTreeMap.openLatestIndexInput(conf, baseDir)) {
            int baseId = input.readInt();
            int maxId = input.readInt();
            int maxValueLength = input.readInt();
            int nValues = input.readInt();
            String converterName = input.readUTF();
            BytesConverter converter = null;
            if (converterName.isEmpty() == false) {
                try {
                    converter = ClassUtil.forName(converterName, BytesConverter.class).newInstance();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
            initParams(baseDir, baseId, maxId, maxValueLength, nValues, converter);

            // Create instance for deserialize data, and update to map in dict
            CachedTreeMap dictMap = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir).immutable(true).keyClazz(DictSliceKey.class).valueClazz(DictSlice.class).build();
            dictMap.readFields(input);
            initDictSliceMap(dictMap);
        }
    }

    @Override
    public void dump(PrintStream out) {
        out.println("Total " + nValues + " values, " + (dictSliceMap == null ? 0 : dictSliceMap.size()) + " slice");
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(baseDir);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof AppendTrieDictionary) {
            AppendTrieDictionary that = (AppendTrieDictionary) o;
            return Objects.equals(this.baseDir, that.baseDir);
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("AppendTrieDictionary(%s)", baseDir);
    }

    @Override
    public boolean contains(Dictionary other) {
        return false;
    }

}
