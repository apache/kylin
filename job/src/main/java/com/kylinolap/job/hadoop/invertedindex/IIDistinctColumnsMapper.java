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
package com.kylinolap.job.hadoop.invertedindex;

import com.kylinolap.cube.common.BytesSplitter;
import com.kylinolap.cube.common.SplittedBytes;
import com.kylinolap.job.constant.BatchConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangli9
 */
public class IIDistinctColumnsMapper<KEYIN> extends Mapper<KEYIN, Text, ShortWritable, Text> {

    private String[] columns;
    private int delim;
    private BytesSplitter splitter;

    private ShortWritable outputKey = new ShortWritable();
    private Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        this.columns = conf.get(BatchConstants.TABLE_COLUMNS).split(",");
        String inputDelim = conf.get(BatchConstants.INPUT_DELIM);
        this.delim = inputDelim == null ? -1 : inputDelim.codePointAt(0);
        this.splitter = new BytesSplitter(200, 4096);
    }

    @Override
    public void map(KEYIN key, Text value, Context context) throws IOException, InterruptedException {
        if (delim == -1) {
            delim = splitter.detectDelim(value, columns.length);
        }

        int nParts = splitter.split(value.getBytes(), value.getLength(), (byte) delim);
        SplittedBytes[] parts = splitter.getSplitBuffers();

        if (nParts != columns.length) {
            throw new RuntimeException("Got " + parts.length + " from -- " + value.toString()
                    + " -- but only " + columns.length + " expected");
        }

        for (short i = 0; i < nParts; i++) {
            outputKey.set(i);
            outputValue.set(parts[i].value, 0, parts[i].length);
            context.write(outputKey, outputValue);
        }
    }

}
