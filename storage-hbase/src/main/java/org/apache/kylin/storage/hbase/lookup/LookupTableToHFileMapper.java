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

package org.apache.kylin.storage.hbase.lookup;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.lookup.HBaseLookupRowEncoder.HBaseRow;

public class LookupTableToHFileMapper<KEYIN> extends KylinMapper<KEYIN, Object, ImmutableBytesWritable, KeyValue> {
    ImmutableBytesWritable outputKey = new ImmutableBytesWritable();

    private String cubeName;
    private CubeDesc cubeDesc;
    private String tableName;
    private int shardNum;
    private IMRTableInputFormat lookupTableInputFormat;
    private long timestamp = 0;
    private HBaseLookupRowEncoder encoder;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        tableName = context.getConfiguration().get(BatchConstants.CFG_TABLE_NAME);
        shardNum = Integer.parseInt(context.getConfiguration().get(BatchConstants.CFG_SHARD_NUM));
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeManager cubeMgr = CubeManager.getInstance(config);
        cubeDesc = cubeMgr.getCube(cubeName).getDescriptor();
        DataModelDesc modelDesc = cubeDesc.getModel();
        TableDesc tableDesc = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(
                tableName, cubeDesc.getProject());
        TableRef lookupTableRef = null;
        for (TableRef tableRef : modelDesc.getLookupTables()) {
            if (tableRef.getTableIdentity().equalsIgnoreCase(tableName)) {
                lookupTableRef = tableRef;
                break;
            }
        }
        JoinDesc joinDesc = modelDesc.getJoinByPKSide(lookupTableRef);
        TblColRef[] keyColRefs = joinDesc.getPrimaryKeyColumns();
        String[] keyColumns = new String[keyColRefs.length];
        for (int i = 0; i < keyColRefs.length; i++) {
            keyColumns[i] = keyColRefs[i].getName();
        }
        encoder = new HBaseLookupRowEncoder(tableDesc, keyColumns, shardNum);
        Configuration conf = context.getConfiguration();
        lookupTableInputFormat = MRUtil.getTableInputFormat(tableDesc, conf.get(BatchConstants.ARG_CUBING_JOB_ID));
    }

    @Override
    public void doMap(KEYIN key, Object value, Context context) throws IOException, InterruptedException {
        Collection<String[]> rowCollection = lookupTableInputFormat.parseMapperInput(value);
        for (String[] row : rowCollection) {
            HBaseRow hBaseRow = encoder.encode(row);

            byte[] rowKey = hBaseRow.getRowKey();
            Map<byte[], byte[]> qualifierValMap = hBaseRow.getQualifierValMap();
            outputKey.set(rowKey);
            for (Entry<byte[], byte[]> qualifierValEntry : qualifierValMap.entrySet()) {
                KeyValue outputValue = createKeyValue(rowKey, qualifierValEntry.getKey(), qualifierValEntry.getValue());
                context.write(outputKey, outputValue);
            }
        }
    }

    private KeyValue createKeyValue(byte[] keyBytes, byte[] qualifier, byte[] value) {
        return new KeyValue(keyBytes, 0, keyBytes.length, //
                HBaseLookupRowEncoder.CF, 0, HBaseLookupRowEncoder.CF.length, //
                qualifier, 0, qualifier.length, //
                timestamp, KeyValue.Type.Put, //
                value, 0, value.length);
    }

}
