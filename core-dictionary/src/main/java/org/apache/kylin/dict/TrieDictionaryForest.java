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
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * use trie forest to optimize trie dictionary
 * <p>
 * the input data must in an increase order(sort by org.apache.kylin.dict.ByteComparator)
 * <p>
 * Created by xiefan on 16-10-26.
 */
public class TrieDictionaryForest<T> extends Dictionary<T> {

    private static final Logger logger = LoggerFactory.getLogger(TrieDictionaryForest.class);

    private ArrayList<TrieDictionary<T>> trees;

    //private ArrayList<byte[]> valueDivide; //find tree

    private ArrayList<ByteArray> valueDivide;

    private ArrayList<Integer> accuOffset;  //find tree

    private BytesConverter<T> bytesConvert;

    private int baseId;

    /*public AtomicLong getValueIndexTime = new AtomicLong(0);

    public AtomicLong getValueTime = new AtomicLong(0);

    public AtomicLong binarySearchTime = new AtomicLong(0);

    public AtomicLong copyTime = new AtomicLong(0);

    public AtomicLong getValueIndexTime2 = new AtomicLong(0);

    public AtomicLong getValueTime2 = new AtomicLong(0);*/

    public TrieDictionaryForest() { // default constructor for Writable interface

    }

    public TrieDictionaryForest(ArrayList<TrieDictionary<T>> trees,
                                ArrayList<ByteArray> valueDivide, ArrayList<Integer> accuOffset, BytesConverter<T> bytesConverter, int baseId) {
        this.trees = trees;
        this.valueDivide = valueDivide;
        this.accuOffset = accuOffset;
        this.bytesConvert = bytesConverter;
        this.baseId = baseId;
    }


    @Override
    public int getMinId() {
        if (trees.isEmpty()) return -1;
        return trees.get(0).getMinId() + baseId;
    }

    @Override
    public int getMaxId() {
        if (trees.isEmpty()) return -1;
        int index = trees.size() - 1;
        int id = accuOffset.get(index) + trees.get(index).getMaxId() + baseId;
        return id;
    }

    @Override
    public int getSizeOfId() {
        if (trees.isEmpty()) return -1;
        int maxOffset = accuOffset.get(accuOffset.size() - 1);
        TrieDictionary<T> lastTree = trees.get(trees.size() - 1);
        int sizeOfId = BytesUtil.sizeForValue(baseId + maxOffset + lastTree.getMaxId() + 1);
        return sizeOfId;
    }

    @Override
    public int getSizeOfValue() {
        int maxValue = -1;
        for (TrieDictionary<T> tree : trees)
            maxValue = Math.max(maxValue, tree.getSizeOfValue());
        return maxValue;
    }

    //value --> id
    @Override
    protected int getIdFromValueImpl(T value, int roundingFlag)
            throws IllegalArgumentException {
        byte[] valueBytes = bytesConvert.convertToBytes(value);
        return getIdFromValueBytesImpl(valueBytes, 0, valueBytes.length, roundingFlag);
    }


    //id = tree_inner_offset + accumulate_offset + baseId
    @Override
    protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag)
            throws IllegalArgumentException {

        //long startTime = System.currentTimeMillis();
        ByteArray search = new ByteArray(value, offset, len);
        //copyTime.addAndGet(System.currentTimeMillis() - startTime);
        int index = findIndexByValue(search);
        //int index = findIndexByValue(value);
        //binarySearchTime.addAndGet(System.currentTimeMillis() - startTime);
        if (index < 0) {
            //System.out.println("value divide:"+valueDivide.size()+" "+valueDivide);
            throw new IllegalArgumentException("Tree Not Found. index < 0.Value:" + new String(Arrays.copyOfRange(value, offset, len)));
        }
        TrieDictionary<T> tree = trees.get(index);
        //getValueIndexTime.addAndGet(System.currentTimeMillis() - startTime);
        //startTime = System.currentTimeMillis();
        int id = tree.getIdFromValueBytes(value, offset, len, roundingFlag);
        id = id + accuOffset.get(index);
        id += baseId;
        //getValueTime.addAndGet(System.currentTimeMillis() - startTime);
        return id;
    }

    //id --> value
    @Override
    protected T getValueFromIdImpl(int id) throws IllegalArgumentException {
        //System.out.println("here");
        byte[] data = getValueBytesFromIdImpl(id);
        if (data != null) {
            return bytesConvert.convertFromBytes(data, 0, data.length);
        } else {
            return null;
        }
    }

    @Override
    protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset)
            throws IllegalArgumentException {
        //long startTime = System.currentTimeMillis();
        int index = findIndexById(id);
        int treeInnerOffset = getTreeInnerOffset(id, index);
        TrieDictionary<T> tree = trees.get(index);
        //getValueIndexTime2.addAndGet(System.currentTimeMillis() - startTime);
        //startTime = System.currentTimeMillis();
        int size = tree.getValueBytesFromIdImpl(treeInnerOffset, returnValue, offset);
        //getValueTime2.addAndGet(System.currentTimeMillis() - startTime);
        return size;
    }


    @Override
    protected byte[] getValueBytesFromIdImpl(int id) throws IllegalArgumentException {
        int index = findIndexById(id); //lower bound
        if (index < 0) {
            throw new IllegalArgumentException("Tree Not Found. index < 0");
        }
        int treeInnerOffset = getTreeInnerOffset(id, index);
        TrieDictionary<T> tree = trees.get(index);
        byte[] result = tree.getValueBytesFromId(treeInnerOffset);
        return result;
    }


    private int getTreeInnerOffset(int id, int index) {
        id -= baseId;
        id = id - accuOffset.get(index);
        return id;
    }

    @Override
    public void dump(PrintStream out) {
        for (int i = 0; i < trees.size(); i++) {
            System.out.println("----tree " + i + "--------");
            trees.get(i).dump(out);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeHead(out);
        writeBody(out);
    }

    private void writeHead(DataOutput out) throws IOException {
        ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
        DataOutputStream headOut = new DataOutputStream(byteBuf);
        headOut.writeInt(baseId);
        headOut.writeUTF(bytesConvert == null ? "" : bytesConvert.getClass().getName());
        //write accuOffset
        headOut.writeInt(accuOffset.size());
        for (int i = 0; i < accuOffset.size(); i++)
            headOut.writeInt(accuOffset.get(i));
        //write valueDivide
        headOut.writeInt(valueDivide.size());
        for (int i = 0; i < valueDivide.size(); i++) {
            ByteArray ba = valueDivide.get(i);
            byte[] byteStr = ba.toBytes();
            headOut.writeInt(byteStr.length);
            headOut.write(byteStr);
        }
        //write tree size
        headOut.writeInt(trees.size());
        headOut.close();
        byte[] head = byteBuf.toByteArray();
        //output
        out.writeInt(head.length);
        out.write(head);
    }


    private void writeBody(DataOutput out) throws IOException {
        for (int i = 0; i < trees.size(); i++) {
            TrieDictionary<T> tree = trees.get(i);
            tree.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            int headSize = in.readInt();
            this.baseId = in.readInt();
            String converterName = in.readUTF();
            if (converterName.isEmpty() == false)
                this.bytesConvert = ClassUtil.forName(converterName, BytesConverter.class).newInstance();
            //init accuOffset
            int accuSize = in.readInt();
            this.accuOffset = new ArrayList<>();
            for (int i = 0; i < accuSize; i++) {
                accuOffset.add(in.readInt());
            }
            //init valueDivide
            int valueDivideSize = in.readInt();
            this.valueDivide = new ArrayList<>();
            for (int i = 0; i < valueDivideSize; i++) {
                int length = in.readInt();
                byte[] buffer = new byte[length];
                in.readFully(buffer);
                valueDivide.add(new ByteArray(buffer, 0, buffer.length));
            }
            int treeSize = in.readInt();
            this.trees = new ArrayList<>();
            for (int i = 0; i < treeSize; i++) {
                TrieDictionary<T> dict = new TrieDictionary<>();
                dict.readFields(in);
                trees.add(dict);
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            else
                throw new RuntimeException(e);
        }

    }

    @Override
    public boolean contains(Dictionary other) {
        if (other.getSize() > this.getSize()) {
            return false;
        }

        for (int i = other.getMinId(); i <= other.getMaxId(); ++i) {
            T v = (T) other.getValueFromId(i);
            if (!this.containsValue(v)) {
                return false;
            }
        }
        return true;
    }

    public List<TrieDictionary<T>> getTrees() {
        return Collections.unmodifiableList(this.trees);
    }

    private boolean onlyOneTree() {
        return trees.size() == 1;
    }

    private int findIndexByValue(T value) {
        byte[] valueBytes = bytesConvert.convertToBytes(value);
        return findIndexByValue(new ByteArray(valueBytes, 0, valueBytes.length));
    }

    private int findIndexByValue(ByteArray value) {
        int index = lowerBound(value, new Comparator<ByteArray>() {
            @Override
            public int compare(ByteArray o1, ByteArray o2) {
                return o1.compareTo(o2);
            }
        }, this.valueDivide);
        return index;
    }

    private int findIndexById(Integer id) {
        id -= baseId;
        int index = lowerBound(id, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        }, this.accuOffset);
        return index;
    }


    private static <K> int lowerBound(K lookfor, Comparator<K> comparator, ArrayList<K> list) {
        if (list == null || list.isEmpty())
            return 0; //return the first tree
        int left = 0;
        int right = list.size() - 1;
        int mid = 0;
        boolean found = false;
        int comp = 0;
        while (!found && left <= right) {
            mid = left + (right - left) / 2;
            comp = comparator.compare(lookfor, list.get(mid));
            if (comp < 0)
                right = mid - 1;
            else if (comp > 0)
                left = mid + 1;
            else
                found = true;
        }
        if (found) {
            //System.out.println("look for:"+lookfor+" index:"+mid);
            return mid;
        } else {
            //System.out.println("look for:"+lookfor+" index:"+Math.max(left,right));
            return Math.min(left, right);  //value may be bigger than the right tree
        }
    }

    public static void main(String[] args) {
        /*ArrayList<Integer> list = new ArrayList<>();
        list.add(3);
        list.add(10);
        list.add(15);
        Comparator<Integer> comp = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        };
        int[] nums = {-1,0,1,2,3,4,13,15,16};
        for(int i : nums){
            System.out.println("found value:"+i+" index:"+lowerBound(i,comp,list));
        }*/
        ArrayList<String> list = new ArrayList<>();
        list.add("一");
        list.add("二");
        list.add("三");
        list.add("");
        list.add("part");
        list.add("par");
        list.add("partition");
        list.add("party");
        list.add("parties");
        list.add("paint");
        Collections.sort(list);
        for (String str : list) {
            System.out.println("found value:" + str + " index:" + lowerBound(str, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }
            }, list));
        }
        //System.out.println(BytesUtil.safeCompareBytes("二".getBytes(),"三".getBytes()));
    }

    public BytesConverter<T> getBytesConvert() {
        return bytesConvert;
    }
}
