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

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

/**
 * @author yangli9
 */
public class IIDistinctColumnsMapper<KEYIN> extends Mapper<KEYIN, HCatRecord, ShortWritable, Text> {

//    private String[] columns;
//    private int delim;
//    private BytesSplitter splitter;

    private ShortWritable outputKey = new ShortWritable();
    private Text outputValue = new Text();
    private HCatSchema schema = null;
    private int columnSize = 0;

    @Override
    protected void setup(Context context) throws IOException {
//        Configuration conf = context.getConfiguration();
//        this.columns = conf.get(BatchConstants.TABLE_COLUMNS).split(",");
//        String inputDelim = conf.get(BatchConstants.INPUT_DELIM);
//        this.delim = inputDelim == null ? -1 : inputDelim.codePointAt(0);
//        this.splitter = new BytesSplitter(200, 4096);
        
        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        columnSize = schema.getFields().size();
    }
    
    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {
        /*
        if (delim == -1) {
            delim = splitter.detectDelim(value, columns.length);
        }

        int nParts = splitter.split(value.getBytes(), value.getLength(), (byte) delim);
        SplittedBytes[] parts = splitter.getSplitBuffers();

        if (nParts != columns.length) {
            throw new RuntimeException("Got " + parts.length + " from -- " + value.toString() + " -- but only " + columns.length + " expected");
        }

        for (short i = 0; i < nParts; i++) {
            outputKey.set(i);
            outputValue.set(parts[i].value, 0, parts[i].length);
            context.write(outputKey, outputValue);
        }
        */
        
        HCatFieldSchema fieldSchema = null;
        for (short i = 0; i < columnSize; i++) {
            outputKey.set(i);
            fieldSchema = schema.get(i);
            Object fieldValue = record.get(fieldSchema.getName(), schema);
            if (fieldValue == null)
                fieldValue = "NULL";
            byte[] bytes = Bytes.toBytes(fieldValue.toString());
            outputValue.set(bytes, 0, bytes.length);
            context.write(outputKey, outputValue);
        }
        
    }

}
