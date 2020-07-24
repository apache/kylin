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

package org.apache.kylin.engine.mr.streaming;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.DictionarySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableMap;
import org.apache.kylin.shaded.com.google.common.collect.ImmutableSet;

public class ColumnarSplitDictReader extends ColumnarSplitReader {
    private static Logger logger = LoggerFactory.getLogger(ColumnarSplitDictReader.class);
    private Iterator<Map.Entry<String, Dictionary>> itr;
    private DictsReader dictsReader;

    private Text currentKey;
    private Text currentValue;
    private AtomicInteger readCount;

    public ColumnarSplitDictReader(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        super(split, context);
        init(split, context);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return -1;
    }

    public void init(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FileSplit fSplit = (FileSplit) split;
        Path path = fSplit.getPath();

        dictsReader = new DictsReader(path, fs);
        ImmutableMap<String, Dictionary> dimensionDictMap = dictsReader.readDicts();
        ImmutableSet<Map.Entry<String, Dictionary>> set = dimensionDictMap.entrySet();
        itr = set.iterator();
        readCount = new AtomicInteger(0);

        logger.info("Reader for dictionary reader initialized. ");
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!itr.hasNext()) {
            return false;
        }
        Map.Entry<String, Dictionary> dimensionDict = itr.next();
        currentKey = new Text(dimensionDict.getKey());
        byte[] dictBytes = DictionarySerializer.serialize(dimensionDict.getValue()).array();
        currentValue = new Text(dictBytes);
        if (readCount.get() % 3 == 0) {
            logger.debug("3 more dict read. ");
        }
        readCount.getAndIncrement();
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public void close() throws IOException {
        dictsReader.close();
    }

}
