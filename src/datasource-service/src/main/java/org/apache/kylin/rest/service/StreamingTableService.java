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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.RELOAD_TABLE_FAILED;
import static org.apache.kylin.metadata.datatype.DataType.DECIMAL;
import static org.apache.kylin.metadata.datatype.DataType.DOUBLE;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.schema.ReloadTableContext;
import org.apache.kylin.metadata.streaming.DataParserManager;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.TableUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;

@Component("streamingTableService")
public class StreamingTableService extends TableService {

    @Autowired
    private AclEvaluate aclEvaluate;

    @Transaction(project = 0, retry = 1)
    public void reloadTable(String project, TableDesc tableDesc, TableExtDesc extDesc) {
        aclEvaluate.checkProjectWritePermission(project);
        innerReloadTable(project, tableDesc, extDesc);
    }

    @Transaction(project = 0)
    List<String> innerReloadTable(String project, TableDesc tableDesc, TableExtDesc extDesc) {
        val tableManager = getManager(NTableMetadataManager.class, project);
        String tableIdentity = tableDesc.getIdentity();
        val originTable = tableManager.getTableDesc(tableIdentity);
        Preconditions.checkNotNull(originTable,
                String.format(Locale.ROOT, MsgPicker.getMsg().getTableNotFound(), tableIdentity));
        List<String> jobs = Lists.newArrayList();
        val context = new ReloadTableContext();
        context.setTableDesc(tableDesc);
        context.setTableExtDesc(extDesc);

        mergeTable(project, context, false);
        return jobs;
    }

    @Transaction(project = 0)
    public void createKafkaConfig(String project, KafkaConfig kafkaConfig) {
        aclEvaluate.checkProjectWritePermission(project);
        getManager(KafkaConfigManager.class, project).createKafkaConfig(kafkaConfig);

        DataParserManager manager = getManager(DataParserManager.class, project);
        manager.initDefault();
        val info = manager.getDataParserInfo(kafkaConfig.getParserName());
        val copyInfo = manager.copyForWrite(info);
        copyInfo.getStreamingTables().add(kafkaConfig.resourceName());
        manager.updateDataParserInfo(copyInfo);
    }

    @Transaction(project = 0)
    public void updateKafkaConfig(String project, KafkaConfig kafkaConfig) {
        aclEvaluate.checkProjectWritePermission(project);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KafkaConfigManager.getInstance(kylinConfig, project).updateKafkaConfig(kafkaConfig);
    }

    /**
     * StreamingTable decimal convert to double in StreamingRequest
     */
    @Transaction(project = 0)
    public void decimalConvertToDouble(String project, StreamingRequest streamingRequest) {
        aclEvaluate.checkProjectWritePermission(project);
        Arrays.stream(streamingRequest.getTableDesc().getColumns()).forEach(column -> {
            if (StringUtils.equalsIgnoreCase(DECIMAL, column.getDatatype())) {
                column.setDatatype(DOUBLE);
            }
        });
    }

    public void checkColumns(StreamingRequest streamingRequest) {
        String batchTableName = streamingRequest.getKafkaConfig().getBatchTable();
        String project = streamingRequest.getProject();
        if (!org.apache.commons.lang.StringUtils.isEmpty(batchTableName)) {
            TableDesc batchTableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getTableDesc(batchTableName);
            if (!checkColumnsMatch(batchTableDesc.getColumns(), streamingRequest.getTableDesc().getColumns())) {
                throw new KylinException(RELOAD_TABLE_FAILED, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getBatchStreamTableNotMatch(), batchTableName));
            }
            TableUtils.checkTimestampColumn(batchTableDesc);
            streamingRequest.getTableDesc().setColumns(batchTableDesc.getColumns().clone());
        } else {
            TableUtils.checkTimestampColumn(streamingRequest.getTableDesc());
        }
    }

    private boolean checkColumnsMatch(ColumnDesc[] batchColumnDescs, ColumnDesc[] streamColumnDescs) {
        if (batchColumnDescs.length != streamColumnDescs.length) {
            return false;
        }

        List<String> batchColumns = Arrays.stream(batchColumnDescs).map(ColumnDesc::getName).sorted()
                .collect(Collectors.toList());
        List<String> streamColumns = Arrays.stream(streamColumnDescs).map(ColumnDesc::getName).sorted()
                .collect(Collectors.toList());
        return batchColumns.equals(streamColumns);
    }

}
