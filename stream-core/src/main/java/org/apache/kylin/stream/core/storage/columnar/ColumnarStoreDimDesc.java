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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.stream.core.storage.columnar.compress.Compression;
import org.apache.kylin.stream.core.storage.columnar.compress.FSInputLZ4CompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.FSInputNoCompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.FSInputRLECompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.LZ4CompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.LZ4CompressedColumnWriter;
import org.apache.kylin.stream.core.storage.columnar.compress.NoCompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.NoCompressedColumnWriter;
import org.apache.kylin.stream.core.storage.columnar.compress.RunLengthCompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.RunLengthCompressedColumnWriter;
import org.apache.kylin.dimension.TimeDerivedColumnType;

public class ColumnarStoreDimDesc {
    private int fixLen;
    private Compression compression;

    public static ColumnarStoreDimDesc getDefaultCStoreDimDesc(CubeDesc cubeDesc, String dimName,
            DimensionEncoding encoding) {
        // for time dimension and the dimension at the first using rle compression
        if (TimeDerivedColumnType.isTimeDerivedColumn(dimName)) {
            return new ColumnarStoreDimDesc(encoding.getLengthOfEncoding(), Compression.RUN_LENGTH);
        }
        if (cubeDesc.getRowkey().getRowKeyColumns()[0].getColumn().equals(dimName)) {
            return new ColumnarStoreDimDesc(encoding.getLengthOfEncoding(), Compression.RUN_LENGTH);
        }
        return new ColumnarStoreDimDesc(encoding.getLengthOfEncoding(), Compression.LZ4);
    }

    public ColumnarStoreDimDesc(int fixLen, Compression compression) {
        this.fixLen = fixLen;
        this.compression = compression;
    }

    public Compression getCompression() {
        return compression;
    }

    public ColumnDataWriter getDimWriter(OutputStream output, int rowCnt) {
        if (compression == Compression.LZ4) {
            return new LZ4CompressedColumnWriter(fixLen, rowCnt, LZ4CompressedColumnWriter.DEF_BLOCK_SIZE, output);
        } else if (compression == Compression.RUN_LENGTH) {
            return new RunLengthCompressedColumnWriter(fixLen, rowCnt, LZ4CompressedColumnWriter.DEF_BLOCK_SIZE, output);
        }
        return new NoCompressedColumnWriter(output);
    }

    public ColumnDataReader getDimReader(ByteBuffer dataBuffer, int columnDataStartOffset, int columnDataLength,
            int rowCount) {
        if (compression == Compression.LZ4) {
            return new LZ4CompressedColumnReader(dataBuffer, columnDataStartOffset, columnDataLength, rowCount);
        } else if (compression == Compression.RUN_LENGTH) {
            return new RunLengthCompressedColumnReader(dataBuffer, columnDataStartOffset, columnDataLength, rowCount);
        }
        return new NoCompressedColumnReader(dataBuffer, columnDataStartOffset, columnDataLength / rowCount, rowCount);
    }

    public ColumnDataReader getDimReaderFromFSInput(FileSystem fs, Path file, int columnDataStartOffset,
                                                    int columnDataLength, int rowCount) throws IOException {
        if (compression == Compression.LZ4) {
            return new FSInputLZ4CompressedColumnReader(fs, file, columnDataStartOffset, columnDataLength, rowCount);
        } else if (compression == Compression.RUN_LENGTH) {
            return new FSInputRLECompressedColumnReader(fs, file, columnDataStartOffset, columnDataLength, rowCount);
        }
        return new FSInputNoCompressedColumnReader(fs, file, columnDataStartOffset, columnDataLength / rowCount,
                rowCount);
    }
}
