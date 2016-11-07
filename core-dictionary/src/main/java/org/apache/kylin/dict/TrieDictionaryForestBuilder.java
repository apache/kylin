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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;


public class TrieDictionaryForestBuilder<T> {

    public static int DEFAULT_MAX_TRIE_TREE_SIZE_MB = 500;

    //public static int MaxTrieTreeSize = 1024;//1k

    private BytesConverter<T> bytesConverter;

    private int curTreeSize = 0;

    private TrieDictionaryBuilder<T> trieBuilder;

    private ArrayList<TrieDictionary<T>> trees = new ArrayList<>();

    private ArrayList<ByteArray> valueDivide = new ArrayList<>(); //find tree

    private ArrayList<Integer> accuOffset = new ArrayList<>();  //find tree

    private ByteArray previousValue = null;  //value use for remove duplicate

    private static final Logger logger = LoggerFactory.getLogger(TrieDictionaryForestBuilder.class);

    private int baseId;

    private int curOffset;

    private int maxTrieTreeSize;

    private boolean isOrdered = true;


    public TrieDictionaryForestBuilder(BytesConverter<T> bytesConverter) {
        this(bytesConverter, 0);
    }

    public TrieDictionaryForestBuilder(BytesConverter<T> bytesConverter, int baseId) {
        this.bytesConverter = bytesConverter;
        this.trieBuilder = new TrieDictionaryBuilder<T>(bytesConverter);
        this.baseId = baseId;
        curOffset = 0;
        int maxTrieTreeSizeMB = getMaxTrieSizeInMB();
        this.maxTrieTreeSize = maxTrieTreeSizeMB * 1024 * 1024;
        logger.info("maxTrieSize is set to:" + maxTrieTreeSize + "B");
        //System.out.println("max trie size:"+maxTrieTreeSize);
        //stringComparator = new ByteComparator<>(new StringBytesConverter());
    }

    public TrieDictionaryForestBuilder(BytesConverter<T> bytesConverter, int baseId, int maxTrieTreeSizeMB) {
        this.bytesConverter = bytesConverter;
        this.trieBuilder = new TrieDictionaryBuilder<T>(bytesConverter);
        this.baseId = baseId;
        curOffset = 0;
        this.maxTrieTreeSize = maxTrieTreeSizeMB * 1024 * 1024;
        logger.info("maxTrieSize is set to:" + maxTrieTreeSize + "B");
    }


    public void addValue(T value) {
        if (value == null) return;
        byte[] valueBytes = bytesConverter.convertToBytes(value);
        addValue(new ByteArray(valueBytes, 0, valueBytes.length));
    }

    public void addValue(byte[] value) {
        if (value == null) return;
        ByteArray array = new ByteArray(value, 0, value.length);
        addValue(array);
    }

    public void addValue(ByteArray value) {
        //System.out.println("value length:"+value.length);
        if (value == null) return;
        //logger.info("going to add value:" + new String(value.array()));
        if (previousValue == null) {
            previousValue = value;
        } else {
            int comp = previousValue.compareTo(value);
            if (comp == 0) {
                //logger.info("find duplicate value:" + new String(value.array()));
                return; //duplicate value
            }
            if (comp > 0 && isOrdered) {
                logger.info("values not in ascending order:" + new String(value.array()));
                isOrdered = false;
                //System.out.println(".");
            }
        }
        this.trieBuilder.addValue(value.array());
        previousValue = value;
        this.curTreeSize += value.length();
        if (curTreeSize >= this.maxTrieTreeSize) {
            TrieDictionary<T> tree = trieBuilder.build(0);
            addTree(tree);
            reset();
        }
    }

    public TrieDictionaryForest<T> build() {
        if (curTreeSize != 0) {  //last tree
            TrieDictionary<T> tree = trieBuilder.build(0);
            addTree(tree);
            reset();
        }
        TrieDictionaryForest<T> forest = new TrieDictionaryForest<T>(this.trees,
                this.valueDivide, this.accuOffset, this.bytesConverter, baseId);

        //log
        logger.info("tree num:" + forest.getTrees().size());
        StringBuilder sb = new StringBuilder();
        for (ByteArray ba : valueDivide) {
            sb.append(new String(ba.array()) + " ");
        }
        logger.info("value divide:" + sb.toString());
        /*
        If input values are not in ascending order and tree num>1,TrieDictionaryForest can not work correctly.
         */
        if (forest.getTrees().size() > 1 && !isOrdered) {
            throw new IllegalStateException("Invalid input data.Unordered data can not be split into multi trees");
        }

        return forest;
    }

    public int getMaxTrieTreeSize() {
        return maxTrieTreeSize;
    }

    public void setMaxTrieTreeSize(int maxTrieTreeSize) {
        this.maxTrieTreeSize = maxTrieTreeSize;
        logger.info("maxTrieSize is set to:" + maxTrieTreeSize + "B");
    }

    private void addTree(TrieDictionary<T> tree) {
        trees.add(tree);
        int minId = tree.getMinId();
        accuOffset.add(curOffset);
        byte[] valueBytes = tree.getValueBytesFromId(minId);
        valueDivide.add(new ByteArray(valueBytes, 0, valueBytes.length));
        curOffset += (tree.getMaxId() + 1);
        //System.out.println(" curOffset:"+ curOffset);
    }

    private void reset() {
        curTreeSize = 0;
        trieBuilder = new TrieDictionaryBuilder<T>(bytesConverter);
    }

    public static int getMaxTrieSizeInMB() {
        KylinConfig config = null;
        try {
            config = KylinConfig.getInstanceFromEnv();
        } catch (RuntimeException e) {
            logger.info("can not get KylinConfig from env.Use default setting:" + DEFAULT_MAX_TRIE_TREE_SIZE_MB + "MB");
        }
        int maxTrieTreeSizeMB;
        if (config != null) {
            maxTrieTreeSizeMB = config.getTrieDictionaryForestMaxTrieSizeMB();
        } else {
            maxTrieTreeSizeMB = DEFAULT_MAX_TRIE_TREE_SIZE_MB;
        }
        return maxTrieTreeSizeMB;
    }

}
