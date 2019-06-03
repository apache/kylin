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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedList;

import java.util.Locale;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.AppendTrieDictionary;
import org.apache.kylin.dict.TrieDictionary;

public class AppendDictNode {
    public byte[] part;
    public int id = -1;
    public boolean isEndOfValue;
    public ArrayList<AppendDictNode> children = new ArrayList<>();

    public int nValuesBeneath;
    public AppendDictNode parent;
    public int childrenCount = 1;

    AppendDictNode(byte[] value, boolean isEndOfValue) {
        reset(value, isEndOfValue);
    }

    AppendDictNode(byte[] value, boolean isEndOfValue, ArrayList<AppendDictNode> children) {
        reset(value, isEndOfValue, children);
    }

    void reset(byte[] value, boolean isEndOfValue) {
        reset(value, isEndOfValue, new ArrayList<AppendDictNode>());
    }

    void reset(byte[] value, boolean isEndOfValue, ArrayList<AppendDictNode> children) {
        this.part = value;
        this.isEndOfValue = isEndOfValue;
        clearChild();
        for (AppendDictNode child : children) {
            addChild(child);
        }
        this.id = -1;
    }

    void clearChild() {
        this.children.clear();
        int childrenCountDelta = this.childrenCount - 1;
        for (AppendDictNode p = this; p != null; p = p.parent) {
            p.childrenCount -= childrenCountDelta;
        }
    }

    void addChild(AppendDictNode child) {
        addChild(-1, child);
    }

    void addChild(int index, AppendDictNode child) {
        child.parent = this;
        if (index < 0) {
            this.children.add(child);
        } else {
            this.children.add(index, child);
        }
        for (AppendDictNode p = this; p != null; p = p.parent) {
            p.childrenCount += child.childrenCount;
        }
    }

    private AppendDictNode removeChild(int index) {
        AppendDictNode child = children.remove(index);
        child.parent = null;
        for (AppendDictNode p = this; p != null; p = p.parent) {
            p.childrenCount -= child.childrenCount;
        }
        return child;
    }

    private AppendDictNode duplicateNode() {
        AppendDictNode newChild = new AppendDictNode(part, false);
        newChild.parent = parent;
        if (parent != null) {
            int index = parent.children.indexOf(this);
            parent.addChild(index + 1, newChild);
        }
        return newChild;
    }

    public byte[] firstValue() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        AppendDictNode p = this;
        while (true) {
            bytes.write(p.part, 0, p.part.length);
            if (p.isEndOfValue || p.children.isEmpty()) {
                break;
            }
            p = p.children.get(0);
        }
        return bytes.toByteArray();
    }

    public static AppendDictNode splitNodeTree(final AppendDictNode splitNode) {
        if (splitNode == null) {
            return null;
        }
        AppendDictNode current = splitNode;
        AppendDictNode p = current.parent;
        while (p != null) {
            int index = p.children.indexOf(current);
            assert index != -1;
            AppendDictNode newParent = p.duplicateNode();
            for (int i = p.children.size() - 1; i >= index; i--) {
                AppendDictNode child = p.removeChild(i);
                newParent.addChild(0, child);
            }
            current = newParent;
            p = p.parent;
        }
        return current;
    }

    public byte[] buildTrieBytes() {
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

        LinkedList<AppendDictNode> open = new LinkedList<AppendDictNode>();
        IdentityHashMap<AppendDictNode, Integer> offsetMap = new IdentityHashMap<AppendDictNode, Integer>();

        // write body
        int o = head.length;
        offsetMap.put(this, o);
        o = build_writeNode(this, o, true, sizeChildOffset, sizeId, trieBytes);
        if (this.children.isEmpty() == false)
            open.addLast(this);

        while (open.isEmpty() == false) {
            AppendDictNode parent = open.removeFirst();
            build_overwriteChildOffset(offsetMap.get(parent), o - head.length, sizeChildOffset, trieBytes);
            for (int i = 0; i < parent.children.size(); i++) {
                AppendDictNode c = parent.children.get(i);
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
        int flags = (int) trieBytes[parentOffset]
                & (TrieDictionary.BIT_IS_LAST_CHILD | TrieDictionary.BIT_IS_END_OF_VALUE);
        BytesUtil.writeUnsigned(childOffset, trieBytes, parentOffset, sizeChildOffset);
        trieBytes[parentOffset] |= flags;
    }

    private int build_writeNode(AppendDictNode n, int offset, boolean isLastChild, int sizeChildOffset, int sizeId,
            byte[] trieBytes) {
        int o = offset;

        // childOffset
        if (isLastChild)
            trieBytes[o] |= TrieDictionary.BIT_IS_LAST_CHILD;
        if (n.isEndOfValue)
            trieBytes[o] |= TrieDictionary.BIT_IS_END_OF_VALUE;
        o += sizeChildOffset;

        // nValueBytes
        if (n.part.length > 255)
            throw new RuntimeException(
                    "Value length is " + n.part.length + " and larger than 255: " + Bytes.toStringBinary(n.part));
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

    // The dict id starts from 1 to 2147483647 and 2147483648 to -2, leave 0 and -1 used for uninitialized state
    private void checkValidId(int id) {
        if (id == 0 || id == -1) {
            throw new IllegalArgumentException("AppendTrieDictionary Id Overflow Unsigned Integer Size 4294967294");
        }
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "DictNode[root=%s, nodes=%d, firstValue=%s]", Bytes.toStringBinary(part),
                childrenCount, Bytes.toStringBinary(firstValue()));
    }

    static class Stats {
        public interface Visitor {
            void visit(AppendDictNode n, int level);
        }

        private static void traverseR(AppendDictNode node, Visitor visitor, int level) {
            visitor.visit(node, level);
            for (AppendDictNode c : node.children)
                traverseR(c, visitor, level + 1);
        }

        private static void traversePostOrderR(AppendDictNode node, Visitor visitor, int level) {
            for (AppendDictNode c : node.children)
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
        public static Stats stats(AppendDictNode root) {
            // calculate nEndValueBeneath
            traversePostOrderR(root, new Visitor() {
                @Override
                public void visit(AppendDictNode n, int level) {
                    n.nValuesBeneath = n.isEndOfValue ? 1 : 0;
                    for (AppendDictNode c : n.children)
                        n.nValuesBeneath += c.nValuesBeneath;
                }
            }, 0);

            // run stats
            final Stats s = new Stats();
            final ArrayList<Integer> lenAtLvl = new ArrayList<Integer>();
            traverseR(root, new Visitor() {
                @Override
                public void visit(AppendDictNode n, int level) {
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
            s.mbpn_footprint = s.mbpn_sizeValueTotal
                    + s.mbpn_nNodes * (s.mbpn_sizeNoValueBytes + s.mbpn_sizeChildOffset);
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
        public void print(AppendDictNode root) {
            print(root, System.out);
        }

        public void print(AppendDictNode root, final PrintStream out) {
            traverseR(root, new Visitor() {
                @Override
                public void visit(AppendDictNode n, int level) {
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
}
