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

package org.apache.kylin.stream.core.storage.columnar.compress;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.stream.core.storage.columnar.ColumnDataWriter;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.stream.core.storage.columnar.GeneralColumnDataWriter;

public class RunLengthCompressedColumnWriter implements ColumnDataWriter {
    private int valLen;
    private int numValInBlock;
    private int blockCnt;

    private ByteBuffer writeBuffer;
    private DataOutputStream dataOutput;
    private byte[] previousVal;
    private int numOfVals = 1;
    private int blockWriteCnt;
    private int totalWriteCntBeforeTheEntry;
    private int entryNum;
    private List<Integer> entryIndex;
    private GeneralColumnDataWriter blockDataWriter;

    public RunLengthCompressedColumnWriter(int valLen, int rowCnt, int compressBlockSize, OutputStream output) {
        this.valLen = valLen;
        this.numValInBlock = compressBlockSize / valLen;
        this.blockCnt = rowCnt / numValInBlock;
        if (rowCnt % numValInBlock != 0) {
            blockCnt++;
        }
        this.writeBuffer = ByteBuffer.allocate(numValInBlock * (valLen + 8) + 4);
        this.dataOutput = new DataOutputStream(output);
        this.blockDataWriter = new GeneralColumnDataWriter(blockCnt, dataOutput);
        this.entryIndex = Lists.newArrayListWithCapacity(512);
    }

    public void write(byte[] valBytes) throws IOException {
        blockWriteCnt++;
        boolean lastVal = (blockWriteCnt == numValInBlock);
        if (previousVal == null) {
            previousVal = valBytes;
            if (lastVal) {
                writeEntry(numOfVals, previousVal);
                writeBlockData();
            }
        } else {
            boolean same = Bytes.compareTo(previousVal, valBytes) == 0;
            if (same) {
                numOfVals++;
            } else {
                writeEntry(numOfVals, previousVal);
            }
            previousVal = valBytes;
            if (lastVal) {
                if (same) {
                    writeEntry(numOfVals, previousVal);
                } else {
                    writeEntry(1, valBytes);
                }

                writeBlockData();
            }
        }
    }

    private void writeEntry(int numOfVals, byte[] val) {
        writeBuffer.putInt(numOfVals);
        writeBuffer.put(val);
        totalWriteCntBeforeTheEntry += numOfVals;
        entryIndex.add(totalWriteCntBeforeTheEntry - 1);
        entryNum++;
        this.numOfVals = 1;
    }

    private void writeBlockData() throws IOException {
        writeEntriesIndex();
        blockDataWriter.write(writeBuffer.array(), 0, writeBuffer.position());

        writeBuffer.rewind();
        previousVal = null;
        numOfVals = 1;
        blockWriteCnt = 0;
        totalWriteCntBeforeTheEntry = 0;
        entryNum = 0;
        entryIndex.clear();
    }

    private void writeEntriesIndex() {
        for (int indexVal : entryIndex) {
            writeBuffer.putInt(indexVal);
        }
        writeBuffer.putInt(entryNum);
    }

    public void flush() throws IOException {
        if (blockWriteCnt > 0) {
            writeEntry(numOfVals, previousVal);
            writeBlockData();
        }
        blockDataWriter.flush();
        dataOutput.writeInt(numValInBlock);
        dataOutput.writeInt(valLen);
        dataOutput.flush();
        writeBuffer = null;
    }
}
