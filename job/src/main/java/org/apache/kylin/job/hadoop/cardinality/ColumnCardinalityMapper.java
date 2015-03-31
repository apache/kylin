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

package org.apache.kylin.job.hadoop.cardinality;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.cube.kv.RowConstants;

/**
 * @author Jack
 * 
 */
public class ColumnCardinalityMapper<T> extends KylinMapper<T, HCatRecord, IntWritable, BytesWritable> {

    private Map<Integer, HyperLogLogPlusCounter> hllcMap = new HashMap<Integer, HyperLogLogPlusCounter>();
    public static final String DEFAULT_DELIM = ",";

    private int counter = 0;
    
    private HCatSchema schema = null;
    private int columnSize = 0;
    
    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        columnSize = schema.getFields().size();
    }

    @Override
    public void map(T key, HCatRecord value, Context context) throws IOException, InterruptedException {

        HCatFieldSchema field;
        Object fieldValue;
        for (int m = 0; m < columnSize; m++) {
            field = schema.get(m);
            fieldValue = value.get(field.getName(), schema);
            if (fieldValue == null)
                fieldValue = "NULL";
            
            if (counter < 5 && m < 10) {
                System.out.println("Get row " + counter + " column '" + field.getName() + "'  value: " + fieldValue);
            }

            if (fieldValue != null)
                getHllc(m).add(Bytes.toBytes(fieldValue.toString()));
        }

        counter++;
    }

    private HyperLogLogPlusCounter getHllc(Integer key) {
        if (!hllcMap.containsKey(key)) {
            hllcMap.put(key, new HyperLogLogPlusCounter());
        }
        return hllcMap.get(key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<Integer> it = hllcMap.keySet().iterator();
        while (it.hasNext()) {
            int key = it.next();
            HyperLogLogPlusCounter hllc = hllcMap.get(key);
            ByteBuffer buf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
            buf.clear();
            hllc.writeRegisters(buf);
            buf.flip();
            context.write(new IntWritable(key), new BytesWritable(buf.array(), buf.limit()));
        }
    }

}
