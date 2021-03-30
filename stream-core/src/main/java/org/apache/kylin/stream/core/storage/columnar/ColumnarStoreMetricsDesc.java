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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.stream.core.storage.columnar.compress.Compression;
import org.apache.kylin.stream.core.storage.columnar.compress.FSInputLZ4CompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.FSInputNoCompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.LZ4CompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.LZ4CompressedColumnWriter;
import org.apache.kylin.stream.core.storage.columnar.compress.NoCompressedColumnReader;
import org.apache.kylin.stream.core.storage.columnar.compress.NoCompressedColumnWriter;

public class ColumnarStoreMetricsDesc {
    private int fixLen = -1;
    private Compression compression;

    public static ColumnarStoreMetricsDesc getDefaultCStoreMetricsDesc(ColumnarMetricsEncoding metricsEncoding) {
        return new ColumnarStoreMetricsDesc(metricsEncoding, Compression.LZ4);
    }

    public ColumnarStoreMetricsDesc(ColumnarMetricsEncoding metricsEncoding, Compression compression) {
        if (metricsEncoding.isFixLength()) {
            this.fixLen = metricsEncoding.getFixLength();
        }
        this.compression = compression;
    }

    public Compression getCompression() {
        return compression;
    }

    public ColumnDataWriter getMetricsWriter(OutputStream output, int rowCnt) {
        if (compression == Compression.LZ4 && fixLen != -1) {
            return new LZ4CompressedColumnWriter(fixLen, rowCnt, LZ4CompressedColumnWriter.DEF_BLOCK_SIZE, output);
        }

        if (fixLen != -1) {
            return new NoCompressedColumnWriter(output);
        }
        return new GeneralColumnDataWriter(rowCnt, new DataOutputStream(output));
    }

    public ColumnDataReader getMetricsReader(ByteBuffer dataBuffer, int columnDataStartOffset, int columnDataLength,
            int rowCount) {
        if (Compression.LZ4 == compression && fixLen != -1) {
            return new LZ4CompressedColumnReader(dataBuffer, columnDataStartOffset, columnDataLength, rowCount);
        }
        if (fixLen != -1) {
            return new NoCompressedColumnReader(dataBuffer, columnDataStartOffset, columnDataLength / rowCount,
                    rowCount);
        }
        return new GeneralColumnDataReader(dataBuffer, columnDataStartOffset, columnDataLength);
    }

    public ColumnDataReader getMetricsReaderFromFSInput(FileSystem fs, Path filePath, int columnDataStartOffset,
                                                        int columnDataLength, int rowCount) throws IOException {
        if (Compression.LZ4 == compression && fixLen != -1) {
            return new FSInputLZ4CompressedColumnReader(fs, filePath, columnDataStartOffset, columnDataLength, rowCount);
        }
        if (fixLen != -1) {
            return new FSInputNoCompressedColumnReader(fs, filePath, columnDataStartOffset, columnDataLength / rowCount,
                    rowCount);
        }
        return new FSInputGeneralColumnDataReader(fs, filePath, columnDataStartOffset, columnDataLength);
    }

}
