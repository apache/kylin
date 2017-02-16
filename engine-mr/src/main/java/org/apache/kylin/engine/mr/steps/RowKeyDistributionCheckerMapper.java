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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.mr.KylinMapper;

/**
 * @author ysong1
 * 
 */
public class RowKeyDistributionCheckerMapper extends KylinMapper<Text, Text, Text, LongWritable> {

    String rowKeyStatsFilePath;
    byte[][] splitKeys;
    Map<Text, Long> resultMap;
    List<Text> keyList;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        rowKeyStatsFilePath = context.getConfiguration().get("rowKeyStatsFilePath");
        splitKeys = this.getSplits(context.getConfiguration(), new Path(rowKeyStatsFilePath));

        resultMap = new HashMap<Text, Long>();
        keyList = new ArrayList<Text>();
        for (int i = 0; i < splitKeys.length; i++) {
            Text key = new Text(splitKeys[i]);
            resultMap.put(key, 0L);
            keyList.add(new Text(splitKeys[i]));
        }
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        for (Text t : keyList) {
            if (key.compareTo(t) < 0) {
                Long v = resultMap.get(t);
                long length = (long)key.getLength() + value.getLength();
                v += length;
                resultMap.put(t, v);
                break;
            }
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        LongWritable outputValue = new LongWritable();
        for (Entry<Text, Long> kv : resultMap.entrySet()) {
            outputValue.set(kv.getValue());
            context.write(kv.getKey(), outputValue);
        }
    }

    @SuppressWarnings("deprecation")
    public byte[][] getSplits(Configuration conf, Path path) {
        List<byte[]> rowkeyList = new ArrayList<byte[]>();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(HadoopUtil.getFileSystem(path, conf), path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                byte[] tmp = ((Text) key).copyBytes();
                if (rowkeyList.contains(tmp) == false) {
                    rowkeyList.add(tmp);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }

        byte[][] retValue = rowkeyList.toArray(new byte[rowkeyList.size()][]);

        return retValue;
    }
}
