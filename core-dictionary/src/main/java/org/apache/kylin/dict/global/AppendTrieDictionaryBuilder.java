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

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.AppendTrieDictionary;
import org.apache.kylin.dict.BytesConverter;
import org.apache.kylin.dict.StringBytesConverter;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkState;

public class AppendTrieDictionaryBuilder {
    private final String baseDir;
    private final String workingDir;
    private final int maxEntriesPerSlice;
    private final boolean isAppendDictGlobal;

    private GlobalDictStore store;
    private int maxId;
    private int maxValueLength;
    private int nValues;
    private BytesConverter bytesConverter;
    private TreeMap<AppendDictSliceKey, String> sliceFileMap = new TreeMap<>(); // slice key -> slice file name

    private AppendDictSliceKey curKey;
    private AppendDictNode curNode;

    public AppendTrieDictionaryBuilder(String baseDir, int maxEntriesPerSlice, boolean isAppendDictGlobal) throws IOException {
        this.baseDir = baseDir;
        this.workingDir = baseDir + "working";
        this.maxEntriesPerSlice = maxEntriesPerSlice;
        this.isAppendDictGlobal = isAppendDictGlobal;
        init();
    }

    public synchronized void init() throws IOException {
        this.store = new GlobalDictHDFSStore(baseDir);
        store.prepareForWrite(workingDir, isAppendDictGlobal);

        Long[] versions = store.listAllVersions();

        if (versions.length == 0 || !isAppendDictGlobal) { // build dict for the first time
            this.maxId = 0;
            this.maxValueLength = 0;
            this.nValues = 0;
            this.bytesConverter = new StringBytesConverter();

        } else { // append values to last version
            GlobalDictMetadata metadata = store.getMetadata(versions[versions.length - 1]);
            this.maxId = metadata.maxId;
            this.maxValueLength = metadata.maxValueLength;
            this.nValues = metadata.nValues;
            this.bytesConverter = metadata.bytesConverter;
            this.sliceFileMap = new TreeMap<>(metadata.sliceFileMap);
        }
    }

    @SuppressWarnings("unchecked")
    public void addValue(String value) throws IOException {
        byte[] valueBytes = bytesConverter.convertToBytes(value);

        if (sliceFileMap.isEmpty()) {
            curNode = new AppendDictNode(new byte[0], false);
            sliceFileMap.put(AppendDictSliceKey.START_KEY, null);
        }
        checkState(sliceFileMap.firstKey().equals(AppendDictSliceKey.START_KEY), "first key should be \"\", but got \"%s\"", sliceFileMap.firstKey());

        AppendDictSliceKey nextKey = sliceFileMap.floorKey(AppendDictSliceKey.wrap(valueBytes));

        if (curKey != null && !nextKey.equals(curKey)) {
            // you may suppose nextKey>=curKey, but nextKey<curKey could happen when a node splits.
            // for example, suppose we have curNode [1-10], and add value "2" triggers split:
            //   first half [1-5] is flushed out, make second half [6-10] curNode and "6" curKey.
            // then value "3" is added, now we got nextKey "1" smaller than curKey "6", surprise!
            // in this case, we need to flush [6-10] and read [1-5] back.
            flushCurrentNode();
            curNode = null;
        }
        if (curNode == null) { // read next slice
            AppendDictSlice slice = store.readSlice(workingDir, sliceFileMap.get(nextKey));
            curNode = slice.rebuildTrieTree();
        }
        curKey = nextKey;

        addValueR(curNode, valueBytes, 0);

        // split slice if it's too large
        if (curNode.childrenCount > maxEntriesPerSlice) {
            AppendDictNode newRoot = splitNodeTree(curNode);
            flushCurrentNode();
            curNode = newRoot;
            curKey = AppendDictSliceKey.wrap(newRoot.firstValue());
            sliceFileMap.put(curKey, null);
        }

        maxValueLength = Math.max(maxValueLength, valueBytes.length);
    }

    public AppendTrieDictionary build(int baseId) throws IOException {
        if (curNode != null) {
            flushCurrentNode();
        }

        GlobalDictMetadata metadata = new GlobalDictMetadata(baseId, this.maxId, this.maxValueLength, this.nValues, this.bytesConverter, sliceFileMap);
        store.commit(workingDir, metadata, isAppendDictGlobal);

        AppendTrieDictionary dict = new AppendTrieDictionary();
        dict.init(this.baseDir);
        return dict;
    }

    private void flushCurrentNode() throws IOException {
        String newSliceFile = store.writeSlice(workingDir, curKey, curNode);
        String oldSliceFile = sliceFileMap.put(curKey, newSliceFile);
        if (oldSliceFile != null) {
            store.deleteSlice(workingDir, oldSliceFile);
        }
    }

    private void addValueR(AppendDictNode node, byte[] value, int start) {
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
                // on first match, mark end of value and assign an ID
                if (!node.isEndOfValue) {
                    node.id = createNextId();
                    node.isEndOfValue = true;
                }
            } else {
                // otherwise, split the current node into two
                AppendDictNode c = new AppendDictNode(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);
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
            AppendDictNode c1 = new AppendDictNode(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);
            c1.id = node.id;
            AppendDictNode c2 = addNodeMaybeOverflow(value, j, nn);
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
            AppendDictNode c = node.children.get(mid);
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
            AppendDictNode c = addNodeMaybeOverflow(value, j, nn);
            node.addChild(comp <= 0 ? mid : mid + 1, c);
        }
    }

    private int createNextId() {
        int id = ++maxId;
        checkValidId(id);
        nValues++;
        return id;
    }

    // The dict id starts from 1 to 2147483647 and 2147483648 to -2, leave 0 and -1 used for uninitialized state
    private void checkValidId(int id) {
        if (id == 0 || id == -1) {
            throw new IllegalArgumentException("AppendTrieDictionary Id Overflow Unsigned Integer Size 4294967294");
        }
    }

    // When add a new node, the value part maybe over 255 bytes, need split it into a sub tree
    private AppendDictNode addNodeMaybeOverflow(byte[] value, int start, int end) {
        AppendDictNode head = null;
        AppendDictNode current = null;
        for (; start + 255 < end; start += 255) {
            AppendDictNode c = new AppendDictNode(BytesUtil.subarray(value, start, start + 255), false);
            if (head == null) {
                head = c;
                current = c;
            } else {
                current.addChild(c);
                current = c;
            }
        }
        AppendDictNode last = new AppendDictNode(BytesUtil.subarray(value, start, end), true);
        last.id = createNextId();
        if (head == null) {
            head = last;
        } else {
            current.addChild(last);
        }
        return head;
    }

    private AppendDictNode splitNodeTree(AppendDictNode root) {
        AppendDictNode parent = root;
        int childCountToSplit = (int) (maxEntriesPerSlice * 1.0 / 2);
        while (true) {
            List<AppendDictNode> children = parent.children;
            if (children.size() == 0) {
                break;
            }
            if (children.size() == 1) {
                parent = children.get(0);
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
        return AppendDictNode.splitNodeTree(parent);
    }

    // Only used for test
    public void setMaxId(int id) {
        this.maxId = id;
    }
}
