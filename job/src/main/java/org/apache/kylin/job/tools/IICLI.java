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

package org.apache.kylin.job.tools;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.RawTableRecord;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.invertedindex.model.IIRow;

/**
 * @author yangli9
 */
public class IICLI {

    public static void main(String[] args) throws IOException {
        Configuration hconf = HadoopUtil.getCurrentConfiguration();
        IIManager mgr = IIManager.getInstance(KylinConfig.getInstanceFromEnv());

        String iiName = args[0];
        IIInstance ii = mgr.getII(iiName);

        String path = args[1];
        System.out.println("Reading from " + path + " ...");

        TableRecordInfo info = new TableRecordInfo(ii.getFirstSegment());
        IIKeyValueCodec codec = new IIKeyValueCodec(info.getDigest());
        int count = 0;
        for (Slice slice : codec.decodeKeyValue(readSequenceKVs(hconf, path))) {
            for (RawTableRecord rec : slice) {
                System.out.printf(new TableRecord(rec, info).toString());
                count++;
            }
        }
        System.out.println("Total " + count + " records");
    }

    public static Iterable<IIRow> readSequenceKVs(Configuration hconf, String path) throws IOException {
        final Reader reader = new Reader(hconf, SequenceFile.Reader.file(new Path(path)));
        return new Iterable<IIRow>() {
            @Override
            public Iterator<IIRow> iterator() {
                return new Iterator<IIRow>() {
                    ImmutableBytesWritable k = new ImmutableBytesWritable();
                    ImmutableBytesWritable v = new ImmutableBytesWritable();
                    IIRow pair = new IIRow(k, v, null);

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
                    public IIRow next() {
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
