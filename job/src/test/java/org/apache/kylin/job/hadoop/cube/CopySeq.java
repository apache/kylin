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

package org.apache.kylin.job.hadoop.cube;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author yangli9
 * 
 */
public class CopySeq {

    public static void main(String[] args) throws IOException {
        copyTo64MB(args[0], args[1]);
    }

    public static void copyTo64MB(String src, String dst) throws IOException {
        Configuration hconf = new Configuration();
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);

        FileSystem fs = FileSystem.get(hconf);
        long srcSize = fs.getFileStatus(srcPath).getLen();
        int copyTimes = (int) (67108864 / srcSize); // 64 MB
        System.out.println("Copy " + copyTimes + " times");

        Reader reader = new Reader(hconf, SequenceFile.Reader.file(srcPath));
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), hconf);
        Text value = new Text();

        Writer writer = SequenceFile.createWriter(hconf, Writer.file(dstPath), Writer.keyClass(key.getClass()), Writer.valueClass(Text.class), Writer.compression(CompressionType.BLOCK, getLZOCodec(hconf)));

        int count = 0;
        while (reader.next(key, value)) {
            for (int i = 0; i < copyTimes; i++) {
                writer.append(key, value);
                count++;
            }
        }

        System.out.println("Len: " + writer.getLength());
        System.out.println("Rows: " + count);

        reader.close();
        writer.close();
    }

    static CompressionCodec getLZOCodec(Configuration hconf) {
        CompressionCodecFactory factory = new CompressionCodecFactory(hconf);
        return factory.getCodecByClassName("org.apache.hadoop.io.compress.LzoCodec");
    }
}
