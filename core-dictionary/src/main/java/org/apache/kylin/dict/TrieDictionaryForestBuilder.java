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

import org.apache.kylin.common.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;


public class TrieDictionaryForestBuilder<T> {

    public static int MaxTrieTreeSize = 1024 * 1024;//1M

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


    public TrieDictionaryForestBuilder(BytesConverter<T> bytesConverter) {
        this(bytesConverter, 0);
    }

    public TrieDictionaryForestBuilder(BytesConverter<T> bytesConverter, int baseId) {
        this.bytesConverter = bytesConverter;
        this.trieBuilder = new TrieDictionaryBuilder<T>(bytesConverter);
        this.baseId = baseId;
        curOffset = 0;
        //stringComparator = new ByteComparator<>(new StringBytesConverter());
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
        if (previousValue == null) {
            previousValue = value;
        } else {
            int comp = previousValue.compareTo(value);
            if (comp == 0) return; //duplicate value
            if (comp > 0) {
                //logger.info("values not in ascending order");
                //System.out.println(".");
            }
        }
        this.trieBuilder.addValue(value.array());
        previousValue = value;
        this.curTreeSize += value.length();
        if (curTreeSize >= MaxTrieTreeSize) {
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
        return forest;
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

}
