/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.cube.invertedindex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HadoopUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;

/**
 * @author yangli9
 * 
 */
public class InvertedIndexCLI {

    public static void main(String[] args) throws IOException {
        Configuration hconf = HadoopUtil.getDefaultConfiguration();
        CubeManager mgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

        String cubeName = args[0];
        CubeInstance cube = mgr.getCube(cubeName);

        String path = args[1];
        System.out.println("Reading from " + path + " ...");

        TableRecordInfo info = new TableRecordInfo(cube.getFirstSegment());
        IIKeyValueCodec codec = new IIKeyValueCodec(info);
        int count = 0;
        for (Slice slice : codec.decodeKeyValue(readSequenceKVs(hconf, path))) {
            for (TableRecordBytes rec : slice) {
                System.out.println((TableRecord)rec);
                count++;
            }
        }
        System.out.println("Total " + count + " records");
    }

    public static Iterable<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> readSequenceKVs(Configuration hconf, String path) throws IOException {
        final Reader reader = new Reader(hconf, SequenceFile.Reader.file(new Path(path)));
        return new Iterable<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>() {
            @Override
            public Iterator<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> iterator() {
                return new Iterator<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>() {
                    ImmutableBytesWritable k = new ImmutableBytesWritable();
                    ImmutableBytesWritable v = new ImmutableBytesWritable();
                    Pair<ImmutableBytesWritable, ImmutableBytesWritable> pair = new Pair<ImmutableBytesWritable, ImmutableBytesWritable>(k, v);

                    @Override
                    public boolean hasNext() {
                        boolean hasNext = false;
                        try {
                            hasNext = reader.next(k, v);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            if (hasNext == false) {
                                IOUtils.closeQuietly(reader);
                            }
                        }
                        return hasNext;
                    }

                    @Override
                    public Pair<ImmutableBytesWritable, ImmutableBytesWritable> next() {
                        return pair;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
