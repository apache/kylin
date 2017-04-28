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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedList;

import org.apache.kylin.common.util.BytesUtil;

/**
 * Builds a dictionary using Trie structure. All values are taken in byte[] form
 * and organized in a Trie with ordering. Then numeric IDs are assigned in
 * sequence.
 *
 * @author yangli9
 */
public class TrieDictionaryBuilder<T> {

    private static final int _2GB = 2000000000;

    public static class Node {
        public byte[] part;
        public boolean isEndOfValue;
        public ArrayList<Node> children;

        public int nValuesBeneath; // only present after stats()

        Node(byte[] value, boolean isEndOfValue) {
            reset(value, isEndOfValue);
        }

        Node(byte[] value, boolean isEndOfValue, ArrayList<Node> children) {
            reset(value, isEndOfValue, children);
        }

        void reset(byte[] value, boolean isEndOfValue) {
            reset(value, isEndOfValue, new ArrayList<Node>());
        }

        void reset(byte[] value, boolean isEndOfValue, ArrayList<Node> children) {
            this.part = value;
            this.isEndOfValue = isEndOfValue;
            this.children = children;
        }
    }

    public static interface Visitor {
        void visit(Node n, int level);
    }

    // ============================================================================

    private Node root;
    protected BytesConverter<T> bytesConverter;

    private boolean hasValue = false;

    public TrieDictionaryBuilder(BytesConverter<T> bytesConverter) {
        this.root = new Node(new byte[0], false);
        this.bytesConverter = bytesConverter;
    }

    public void addValue(T value) {
        addValue(bytesConverter.convertToBytes(value));
    }

    // add a converted value (given in byte[] format), use with care, for internal only
    void addValue(byte[] value) {
        addValueR(root, value, 0);
    }

    private void addValueR(Node node, byte[] value, int start) {
        hasValue = true;
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
                node.isEndOfValue = true;
            } else {
                // otherwise, split the current node into two
                Node c = new Node(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);
                node.reset(BytesUtil.subarray(node.part, 0, i), true);
                node.children.add(c);
            }
            return;
        }

        // if partially matched the current, split the current node, add the new value, make a 3-way
        if (i < n) {
            Node c1 = new Node(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);
            Node c2 = new Node(BytesUtil.subarray(value, j, nn), true);
            node.reset(BytesUtil.subarray(node.part, 0, i), false);
            if (comp < 0) {
                node.children.add(c1);
                node.children.add(c2);
            } else {
                node.children.add(c2);
                node.children.add(c1);
            }
            return;
        }

        // out matched the current, binary search the next byte for a child node to continue
        byte lookfor = value[j];
        int lo = 0;
        int hi = node.children.size() - 1;
        int mid = 0;
        boolean found = false;
        comp = 0;
        while (!found && lo <= hi) {
            mid = lo + (hi - lo) / 2;
            comp = BytesUtil.compareByteUnsigned(lookfor, node.children.get(mid).part[0]);
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
            Node c = new Node(BytesUtil.subarray(value, j, nn), true);
            node.children.add(comp <= 0 ? mid : mid + 1, c);
        }
    }

    public void traverse(Visitor visitor) {
        traverseR(root, visitor, 0);
    }

    private void traverseR(Node node, Visitor visitor, int level) {
        visitor.visit(node, level);
        for (Node c : node.children)
            traverseR(c, visitor, level + 1);
    }

    public void traversePostOrder(Visitor visitor) {
        traversePostOrderR(root, visitor, 0);
    }

    private void traversePostOrderR(Node node, Visitor visitor, int level) {
        for (Node c : node.children)
            traversePostOrderR(c, visitor, level + 1);
        visitor.visit(node, level);
    }

    public static class Stats {
        public int nValues; // number of values in total
        public int nValueBytesPlain; // number of bytes for all values uncompressed
        public int nValueBytesCompressed; // number of values bytes in Trie (compressed)
        public int maxValueLength; // size of longest value in bytes

        // the trie is multi-byte-per-node
        public int mbpn_nNodes; // number of nodes in trie
        public int mbpn_trieDepth; // depth of trie
        public int mbpn_maxFanOut; // the maximum no. children
        public long mbpn_nChildLookups; // number of child lookups during lookup every value once
        public long mbpn_nTotalFanOut; // the sum of fan outs during lookup every value once
        public int mbpn_sizeValueTotal; // the sum of value space in all nodes
        public int mbpn_sizeNoValueBytes; // size of field noValueBytes
        public int mbpn_sizeNoValueBeneath; // size of field noValuesBeneath, depends on cardinality
        public int mbpn_sizeChildOffset; // size of field childOffset, points to first child in flattened array
        public long mbpn_footprint; // MBPN footprint in bytes

        // stats for one-byte-per-node as well, so there's comparison
        public int obpn_sizeValue; // size of value per node, always 1
        public int obpn_sizeNoValuesBeneath; // size of field noValuesBeneath, depends on cardinality
        public int obpn_sizeChildCount; // size of field childCount, enables binary search among children
        public int obpn_sizeChildOffset; // size of field childOffset, points to first child in flattened array
        public int obpn_nNodes; // no. nodes in OBPN trie
        public long obpn_footprint; // OBPN footprint in bytes

        public void print() {
            PrintStream out = System.out;
            out.println("============================================================================");
            out.println("No. values:             " + nValues);
            out.println("No. bytes raw:          " + nValueBytesPlain);
            out.println("No. bytes in trie:      " + nValueBytesCompressed);
            out.println("Longest value length:   " + maxValueLength);

            // flatten trie footprint calculation, case of One-Byte-Per-Node
            out.println("----------------------------------------------------------------------------");
            out.println("OBPN node size:  " + (obpn_sizeValue + obpn_sizeNoValuesBeneath + obpn_sizeChildCount + obpn_sizeChildOffset) + " = " + obpn_sizeValue + " + " + obpn_sizeNoValuesBeneath + " + " + obpn_sizeChildCount + " + " + obpn_sizeChildOffset);
            out.println("OBPN no. nodes:  " + obpn_nNodes);
            out.println("OBPN trie depth: " + maxValueLength);
            out.println("OBPN footprint:  " + obpn_footprint + " in bytes");

            // flatten trie footprint calculation, case of Multi-Byte-Per-Node
            out.println("----------------------------------------------------------------------------");
            out.println("MBPN max fan out:       " + mbpn_maxFanOut);
            out.println("MBPN no. child lookups: " + mbpn_nChildLookups);
            out.println("MBPN total fan out:     " + mbpn_nTotalFanOut);
            out.println("MBPN average fan out:   " + (double) mbpn_nTotalFanOut / mbpn_nChildLookups);
            out.println("MBPN values size total: " + mbpn_sizeValueTotal);
            out.println("MBPN node size:         " + (mbpn_sizeNoValueBytes + mbpn_sizeNoValueBeneath + mbpn_sizeChildOffset) + " = " + mbpn_sizeNoValueBytes + " + " + mbpn_sizeNoValueBeneath + " + " + mbpn_sizeChildOffset);
            out.println("MBPN no. nodes:         " + mbpn_nNodes);
            out.println("MBPN trie depth:        " + mbpn_trieDepth);
            out.println("MBPN footprint:         " + mbpn_footprint + " in bytes");
        }
    }

    public boolean isHasValue() {
        return hasValue;
    }

    /**
     * out print some statistics of the trie and the dictionary built from it
     */
    public Stats stats() {
        // calculate nEndValueBeneath
        traversePostOrder(new Visitor() {
            @Override
            public void visit(Node n, int level) {
                n.nValuesBeneath = n.isEndOfValue ? 1 : 0;
                for (Node c : n.children)
                    n.nValuesBeneath += c.nValuesBeneath;
            }
        });

        // run stats
        final Stats s = new Stats();
        final ArrayList<Integer> lenAtLvl = new ArrayList<Integer>();
        traverse(new Visitor() {
            @Override
            public void visit(Node n, int level) {
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
        });

        // flatten trie footprint calculation, case of One-Byte-Per-Node
        s.obpn_sizeValue = 1;
        s.obpn_sizeNoValuesBeneath = BytesUtil.sizeForValue(s.nValues);
        s.obpn_sizeChildCount = 1;
        s.obpn_sizeChildOffset = 5; // MSB used as isEndOfValue flag
        s.obpn_nNodes = s.nValueBytesCompressed; // no. nodes is the total number of compressed bytes in OBPN
        s.obpn_footprint = s.obpn_nNodes * (long) (s.obpn_sizeValue + s.obpn_sizeNoValuesBeneath + s.obpn_sizeChildCount + s.obpn_sizeChildOffset);
        while (true) { // minimize the offset size to match the footprint
            long t = s.obpn_nNodes * (long) (s.obpn_sizeValue + s.obpn_sizeNoValuesBeneath + s.obpn_sizeChildCount + s.obpn_sizeChildOffset - 1);
            if (BytesUtil.sizeForValue(t * 2) <= s.obpn_sizeChildOffset - 1) { // *2 because MSB of offset is used for isEndOfValue flag
                s.obpn_sizeChildOffset--;
                s.obpn_footprint = t;
            } else
                break;
        }

        // flatten trie footprint calculation, case of Multi-Byte-Per-Node
        s.mbpn_sizeValueTotal = s.nValueBytesCompressed;
        s.mbpn_sizeNoValueBytes = 1;
        s.mbpn_sizeNoValueBeneath = BytesUtil.sizeForValue(s.nValues);
        s.mbpn_sizeChildOffset = 5;
        s.mbpn_footprint = s.mbpn_sizeValueTotal + s.mbpn_nNodes * (long) (s.mbpn_sizeNoValueBytes + s.mbpn_sizeNoValueBeneath + s.mbpn_sizeChildOffset);
        while (true) { // minimize the offset size to match the footprint
            long t = s.mbpn_sizeValueTotal + s.mbpn_nNodes * (long) (s.mbpn_sizeNoValueBytes + s.mbpn_sizeNoValueBeneath + s.mbpn_sizeChildOffset - 1);
            if (BytesUtil.sizeForValue(t * 4) <= s.mbpn_sizeChildOffset - 1) { // *4 because 2 MSB of offset is used for isEndOfValue & isEndChild flag
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
    public void print() {
        print(System.out);
    }

    public void print(final PrintStream out) {
        traverse(new Visitor() {
            @Override
            public void visit(Node n, int level) {
                try {
                    for (int i = 0; i < level; i++)
                        out.print("  ");
                    out.print(new String(n.part, "UTF-8"));
                    out.print(" - ");
                    if (n.nValuesBeneath > 0)
                        out.print(n.nValuesBeneath);
                    if (n.isEndOfValue)
                        out.print("*");
                    out.print("\n");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private CompleteParts completeParts = new CompleteParts();

    private class CompleteParts {
        byte[] data = new byte[4096];
        int current = 0;

        public void append(byte[] part) {
            while (current + part.length > data.length)
                expand();

            System.arraycopy(part, 0, data, current, part.length);
            current += part.length;
        }

        public void withdraw(int size) {
            current -= size;
        }

        public byte[] retrieve() {
            return Arrays.copyOf(data, current);
        }

        private void expand() {
            byte[] temp = new byte[2 * data.length];
            System.arraycopy(data, 0, temp, 0, data.length);
            data = temp;
        }
    }

    // there is a 255 limitation of length for each node's part.
    // we interpolate nodes to satisfy this when a node's part becomes
    // too long(overflow)
    private void checkOverflowParts(Node node) {
        LinkedList<Node> childrenCopy = new LinkedList<Node>(node.children);
        for (Node child : childrenCopy) {
            if (child.part.length > 255) {
                byte[] first255 = Arrays.copyOf(child.part, 255);

                completeParts.append(node.part);
                completeParts.append(first255);
                byte[] visited = completeParts.retrieve();
                this.addValue(visited);
                completeParts.withdraw(255);
                completeParts.withdraw(node.part.length);
            }
        }

        completeParts.append(node.part); // by here the node.children may have been changed
        for (Node child : node.children) {
            checkOverflowParts(child);
        }
        completeParts.withdraw(node.part.length);
    }

    /**
     * Flatten the trie into a byte array for a minimized memory footprint.
     * Lookup remains fast. Cost is inflexibility to modify (becomes immutable).
     * <p>
     * Flattened node structure is HEAD + NODEs, for each node:
     * - o byte, offset to child node, o = stats.mbpn_sizeChildOffset
     *   - 1 bit, isLastChild flag, the 1st MSB of o
     *   - 1 bit, isEndOfValue flag, the 2nd MSB of o
     * - c byte, number of values beneath, c = stats.mbpn_sizeNoValueBeneath
     * - 1 byte, number of value bytes
     * - n byte, value bytes
     */
    public TrieDictionary<T> build(int baseId) {
        byte[] trieBytes = buildTrieBytes(baseId);
        TrieDictionary<T> r = new TrieDictionary<T>(trieBytes);
        return r;
    }

    protected byte[] buildTrieBytes(int baseId) {
        checkOverflowParts(this.root);

        Stats stats = stats();
        int sizeNoValuesBeneath = stats.mbpn_sizeNoValueBeneath;
        int sizeChildOffset = stats.mbpn_sizeChildOffset;

        if (stats.mbpn_footprint <= 0) // must never happen, but let us be cautious
            throw new IllegalStateException("Too big dictionary, dictionary cannot be bigger than 2GB");
        if (stats.mbpn_footprint > _2GB)
            throw new RuntimeException("Too big dictionary, dictionary cannot be bigger than 2GB");

        // write head
        byte[] head;
        try {
            ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
            DataOutputStream headOut = new DataOutputStream(byteBuf);
            headOut.write(TrieDictionary.MAGIC);
            headOut.writeShort(0); // head size, will back fill
            headOut.writeInt((int) stats.mbpn_footprint); // body size
            headOut.write(sizeChildOffset);
            headOut.write(sizeNoValuesBeneath);
            headOut.writeShort(baseId);
            headOut.writeShort(stats.maxValueLength);
            headOut.writeUTF(bytesConverter == null ? "" : bytesConverter.getClass().getName());
            headOut.close();
            head = byteBuf.toByteArray();
            BytesUtil.writeUnsigned(head.length, head, TrieDictionary.MAGIC_SIZE_I, 2);
        } catch (IOException e) {
            throw new RuntimeException(e); // shall not happen, as we are writing in memory
        }

        byte[] trieBytes = new byte[(int) stats.mbpn_footprint + head.length];
        System.arraycopy(head, 0, trieBytes, 0, head.length);

        LinkedList<Node> open = new LinkedList<Node>();
        IdentityHashMap<Node, Integer> offsetMap = new IdentityHashMap<Node, Integer>();

        // write body
        int o = head.length;
        offsetMap.put(root, o);
        o = build_writeNode(root, o, true, sizeNoValuesBeneath, sizeChildOffset, trieBytes);
        if (root.children.isEmpty() == false)
            open.addLast(root);

        while (open.isEmpty() == false) {
            Node parent = open.removeFirst();
            build_overwriteChildOffset(offsetMap.get(parent), o - head.length, sizeChildOffset, trieBytes);
            for (int i = 0; i < parent.children.size(); i++) {
                Node c = parent.children.get(i);
                boolean isLastChild = (i == parent.children.size() - 1);
                offsetMap.put(c, o);
                o = build_writeNode(c, o, isLastChild, sizeNoValuesBeneath, sizeChildOffset, trieBytes);
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

    private int build_writeNode(Node n, int offset, boolean isLastChild, int sizeNoValuesBeneath, int sizeChildOffset, byte[] trieBytes) {
        int o = offset;
        if (o > _2GB)
            throw new IllegalStateException();

        // childOffset
        if (isLastChild)
            trieBytes[o] |= TrieDictionary.BIT_IS_LAST_CHILD;
        if (n.isEndOfValue)
            trieBytes[o] |= TrieDictionary.BIT_IS_END_OF_VALUE;
        o += sizeChildOffset;

        // nValuesBeneath
        BytesUtil.writeUnsigned(n.nValuesBeneath, trieBytes, o, sizeNoValuesBeneath);
        o += sizeNoValuesBeneath;

        // nValueBytes
        if (n.part.length > 255)
            throw new RuntimeException();
        BytesUtil.writeUnsigned(n.part.length, trieBytes, o, 1);
        o++;

        // valueBytes
        System.arraycopy(n.part, 0, trieBytes, o, n.part.length);
        o += n.part.length;

        return o;
    }

}
