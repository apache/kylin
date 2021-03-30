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
package org.apache.kylin.source.jdbc.extensible;

import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.source.hive.HiveMRInput;

public class JdbcHiveMRInput extends JdbcHiveInputBase implements IMRInput {

    private final JdbcConnector dataSource;

    JdbcHiveMRInput(JdbcConnector dataSource) {
        this.dataSource = dataSource;
    }

    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new JdbcMRBatchCubingInputSide(flatDesc, dataSource);
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
        return new HiveMRInput.HiveTableInputFormat(getTableNameForHCat(table, uuid));
    }

    public static class JdbcMRBatchCubingInputSide extends JDBCBaseBatchCubingInputSide implements IMRInput.IMRBatchCubingInputSide {

        public JdbcMRBatchCubingInputSide(IJoinedFlatTableDesc flatDesc, JdbcConnector dataSource) {
            super(flatDesc, dataSource);
        }

        @Override
        public IMRInput.IMRTableInputFormat getFlatTableInputFormat() {
            return new HiveMRInput.HiveTableInputFormat(getIntermediateTableIdentity());
        }
    }

}
