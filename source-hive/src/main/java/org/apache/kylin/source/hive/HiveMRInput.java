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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatSplit;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.TableDesc;

public class HiveMRInput extends HiveInputBase implements IMRInput {

    @Override
    public IBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new HiveMRBatchCubingInputSide(flatDesc);
    }

    @Override
    public IBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new IMRBatchMergeInputSide() {
            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                // doing nothing
            }
        };
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table, String uuid) {
        return new HiveTableInputFormat(getTableNameForHCat(table, uuid));
    }

    public static class HiveTableInputFormat implements IMRTableInputFormat {
        final String dbName;
        final String tableName;

        /**
         * Construct a HiveTableInputFormat to read hive table.
         * @param fullQualifiedTableName "databaseName.tableName"
         */
        public HiveTableInputFormat(String fullQualifiedTableName) {
            String[] parts = HadoopUtil.parseHiveTableName(fullQualifiedTableName);
            dbName = parts[0];
            tableName = parts[1];
        }

        @Override
        public void configureJob(Job job) {
            try {
                job.getConfiguration().addResource("hive-site.xml");

                HCatInputFormat.setInput(job, dbName, tableName);
                job.setInputFormatClass(HCatInputFormat.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String[]> parseMapperInput(Object mapperInput) {
            return Collections.singletonList(HiveTableReader.getRowAsStringArray((HCatRecord) mapperInput));
        }

        @Override
        public String getInputSplitSignature(InputSplit inputSplit) {
            FileSplit baseSplit = (FileSplit) ((HCatSplit) inputSplit).getBaseSplit();
            //file name(for intermediate table) + start pos + length
            return baseSplit.getPath().getName() + "_" + baseSplit.getStart() + "_" + baseSplit.getLength();
        }
    }

    public static class HiveMRBatchCubingInputSide extends BaseBatchCubingInputSide implements IMRBatchCubingInputSide {

        public HiveMRBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            super(flatDesc);
        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            return new HiveMRInput.HiveTableInputFormat(getIntermediateTableIdentity());
        }
    }

    /**
     * When build job is created by kylin version 2.4.x or below, the step class name is an inner class of {@link HiveMRInput},
     * to avoid the ClassNotFoundException in {@link ExecutableManager#newExecutable(java.lang.String)} , delegate the OLD class to the new one
     *
     * @since 2.5.0
     * @deprecated For backwards compatibility.
     */
    @Deprecated
    public static class RedistributeFlatHiveTableStep
            extends org.apache.kylin.source.hive.RedistributeFlatHiveTableStep {

    }

    /**
     * When build job is created by kylin version 2.4.x or below, the step class name is an inner class of {@link HiveMRInput},
     * to avoid the ClassNotFoundException in {@link ExecutableManager#newExecutable(java.lang.String)} , delegate the OLD class to the new one
     *
     * @since 2.5.0
     * @deprecated For backwards compatibility.
     */
    @Deprecated
    public static class GarbageCollectionStep extends org.apache.kylin.source.hive.GarbageCollectionStep {

    }
}
