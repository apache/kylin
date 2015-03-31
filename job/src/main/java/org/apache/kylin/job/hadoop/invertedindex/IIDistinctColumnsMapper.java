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

package org.apache.kylin.job.hadoop.invertedindex;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import org.apache.kylin.common.mr.KylinMapper;

/**
 * @author yangli9
 */
public class IIDistinctColumnsMapper<KEYIN> extends KylinMapper<KEYIN, HCatRecord, ShortWritable, Text> {

    private ShortWritable outputKey = new ShortWritable();
    private Text outputValue = new Text();
    private HCatSchema schema = null;
    private int columnSize = 0;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        columnSize = schema.getFields().size();
    }

    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {

        HCatFieldSchema fieldSchema = null;
        for (short i = 0; i < columnSize; i++) {
            outputKey.set(i);
            fieldSchema = schema.get(i);
            Object fieldValue = record.get(fieldSchema.getName(), schema);
            if (fieldValue == null)
                continue;
            byte[] bytes = Bytes.toBytes(fieldValue.toString());
            outputValue.set(bytes, 0, bytes.length);
            context.write(outputKey, outputValue);
        }

    }

}
