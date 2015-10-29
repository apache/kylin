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

package org.apache.kylin.engine.mr.invertedindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;

/**
 * @author yangli9
 */
public class IIDistinctColumnsMapper<KEYIN> extends KylinMapper<KEYIN, Object, ShortWritable, Text> {

    private ShortWritable outputKey = new ShortWritable();
    private Text outputValue = new Text();

    protected IMRInput.IMRTableInputFormat flatTableInputFormat;
    
    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String iiName = conf.get(BatchConstants.CFG_II_NAME);
        IIInstance iiInstance = IIManager.getInstance(config).getII(iiName);
        IISegment seg = iiInstance.getFirstSegment();
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(seg).getFlatTableInputFormat();
    }

    @Override
    public void map(KEYIN key, Object record, Context context) throws IOException, InterruptedException {

        String[] row = flatTableInputFormat.parseMapperInput(record);
        
        for (short i = 0; i < row.length; i++) {
            outputKey.set(i);
            if (row[i] == null)
                continue;
            byte[] bytes = Bytes.toBytes(row[i].toString());
            outputValue.set(bytes, 0, bytes.length);
            context.write(outputKey, outputValue);
        }

    }

}
