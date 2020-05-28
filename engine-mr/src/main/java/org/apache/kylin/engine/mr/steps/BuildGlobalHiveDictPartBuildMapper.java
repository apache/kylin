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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildGlobalHiveDictPartBuildMapper<KEYIN, Object> extends KylinMapper<KEYIN, Object, Text, NullWritable> {
    private static final Logger logger = LoggerFactory.getLogger(BuildGlobalHiveDictPartBuildMapper.class);

    private Integer colIndex;
    private ByteBuffer tmpbuf;
    private Text outputKey = new Text();
    private long count = 0L;

    @Override
    protected void doSetup(Context context) throws IOException, InterruptedException {
        tmpbuf = ByteBuffer.allocate(64);

        KylinConfig config;
        try {
            config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String[] dicCols = config.getMrHiveDictColumnsExcludeRefColumns();
        logger.info("kylin.dictionary.mr-hive.columns: exclude ref cols {}", dicCols);

        //eg: /user/kylin/warehouse/db/kylin_intermediate_kylin_sales_cube_mr_6222c210_ce2d_e8ce_dd0f_f12c38fa9115__group_by/dict_column=KYLIN_SALES_SELLER_ID/part-000
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //eg: dict_column=KYLIN_SALES_SELLER_ID
        String name = fileSplit.getPath().getParent().getName();
        logger.info("this map file name :{}", name);

        //eg: KYLIN_SALES_SELLER_ID
        String colName = name.split("=")[1];
        logger.info("this map build col name :{}", colName);

        for (int i = 0; i < dicCols.length; i++) {
            if (dicCols[i].equalsIgnoreCase(colName)) {
                colIndex = i;
            }
        }
        if (colIndex < 0 || colIndex > 127) {
            logger.error("kylin.dictionary.mr-hive.columns colIndex :{} error ", colIndex);
            logger.error("kylin.dictionary.mr-hive.columns set error,mr-hive columns's count should less than 128");
        }
        logger.info("this map build col index :{}", colIndex);

    }

    @Override
    public void doMap(KEYIN key, Object record, Context context) throws IOException, InterruptedException {
        count++;
        writeFieldValue(context, key.toString());
    }


    private void writeFieldValue(Context context, String value)
            throws IOException, InterruptedException {
        tmpbuf.clear();
        byte[] valueBytes = Bytes.toBytes(value);
        int size = valueBytes.length + 1;
        if (size >= tmpbuf.capacity()) {
            tmpbuf = ByteBuffer.allocate(countNewSize(tmpbuf.capacity(), size));
        }
        tmpbuf.put(colIndex.byteValue());//colIndex should less than 128
        tmpbuf.put(valueBytes);
        outputKey.set(tmpbuf.array(), 0, tmpbuf.position());
        context.write(outputKey, NullWritable.get());
        if (count < 10) {
            logger.info("colIndex:{},input key:{}", colIndex, value);
        }
    }

    private int countNewSize(int oldSize, int dataSize) {
        int newSize = oldSize * 2;
        while (newSize < dataSize) {
            newSize = newSize * 2;
        }
        return newSize;
    }
}
