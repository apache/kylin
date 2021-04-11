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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.request.StreamingRequestV2;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.StreamingV2Service;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.stats.ClusterState;
import org.apache.kylin.stream.core.model.stats.CubeRealTimeState;
import org.apache.kylin.stream.core.model.stats.ReceiverStats;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * StreamingController is defined as Restful API entrance for UI.
 *
 */
@Controller
@RequestMapping(value = "/streaming_v2")
public class StreamingV2Controller extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(StreamingV2Controller.class);

    @Autowired
    private StreamingV2Service streamingService;

    @Autowired
    private CubeService cubeMgmtService;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @RequestMapping(value = "/getConfig", method = { RequestMethod.GET })
    @ResponseBody
    public List<StreamingSourceConfig> getStreamings(@RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset) {
        try {
            // query all streaming config or query one streaming config
            if (!Strings.isEmpty(table) && !Strings.isEmpty(project)) {
                // check the table metadata
                if (tableService.getTableDescByName(table, false, project) == null) {
                    // the table metadata doesn't exist
                    throw new InternalErrorException(String.format(Locale.ROOT,
                            "The table %s of project %s doesn't exist, please make the stream table exists",
                            table, project));
                }
            }
            return streamingService.getStreamingConfigs(table, project, limit, offset);
        } catch (IOException e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    /**
     *
     * create Streaming Schema
     * 
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.POST })
    @ResponseBody
    public StreamingRequestV2 saveStreamingConfig(@RequestBody StreamingRequestV2 streamingRequest) {
        String project = streamingRequest.getProject();
        TableDesc tableDesc = deserializeTableDesc(streamingRequest);
        StreamingSourceConfig streamingSourceConfig = deserializeStreamingConfig(streamingRequest.getStreamingConfig());

        validateInput(tableDesc, streamingSourceConfig);

        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to saveStreamingConfig with table Identity {}", user, tableDesc.getIdentity());

        boolean saveStreamingSuccess = false, saveTableSuccess = false;
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ProjectInstance projectInstance = ProjectManager.getInstance(kylinConfig).getProject(project);

        InternalErrorException shouldThrow = null;
        try {
            try {
                tableDesc.setUuid(UUID.randomUUID().toString());
                if (tableService.getTableDescByName(tableDesc.getIdentity(), false, project) != null) {
                    throw new IOException(String.format(Locale.ROOT,
                            "The table %s of project %s exists",
                            tableDesc.getIdentity(), project));
                }
                tableService.loadTableToProject(tableDesc, null, project);
                saveTableSuccess = true;
            } catch (IOException e) {
                throw new BadRequestException("Failed to add streaming table, because of " + e.getMessage());
            }
            try {
                streamingSourceConfig.setName(tableDesc.getIdentity());
                streamingSourceConfig.setUuid(UUID.randomUUID().toString());
                streamingService.createStreamingConfig(streamingSourceConfig, projectInstance);
                saveStreamingSuccess = true;
            } catch (IOException e) {
                logger.error("Failed to save StreamingSourceConfig:" + e.getLocalizedMessage(), e);
                throw new InternalErrorException("Failed to save StreamingSourceConfig: " + e.getLocalizedMessage());
            }

        } finally {
            if (!saveTableSuccess || !saveStreamingSuccess) {
                if (saveTableSuccess) {
                    try {
                        // just drop the table metadata and don't drop the stream source config info
                        tableService.unloadHiveTable(tableDesc.getIdentity(), project, false);
                    } catch (IOException e) {
                        shouldThrow = new InternalErrorException(
                                "Action failed and failed to rollback the create table " + e.getLocalizedMessage(), e);
                    }
                }
                if (saveStreamingSuccess) {
                    try {
                        streamingService.dropStreamingConfig(streamingSourceConfig);
                    } catch (IOException e) {
                        shouldThrow = new InternalErrorException(
                                "Action failed and failed to rollback the created streaming config: "
                                        + e.getLocalizedMessage(),
                                e);
                    }
                }
            }
        }

        if (null != shouldThrow) {
            throw shouldThrow;
        }

        streamingRequest.setSuccessful(true);
        return streamingRequest;
    }

    private void validateInput(TableDesc tableDesc, StreamingSourceConfig streamingSourceConfig) {
        if (StringUtils.isEmpty(tableDesc.getIdentity()) || StringUtils.isEmpty(streamingSourceConfig.getName())) {
            logger.error("streamingSourceConfig name should not be empty.");
            throw new BadRequestException("streamingSourceConfig name should not be empty.");
        }

        // validate the compatibility for input table schema and the underline hive table schema
        if (tableDesc.getSourceType() == ISourceAware.ID_KAFKA_HIVE) {
            List<FieldSchema> fields;
            List<FieldSchema> partitionFields;
            String db = tableDesc.getDatabase();
            try {
                HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(new HiveConf());
                fields = metaStoreClient.getFields(db, tableDesc.getName());
                partitionFields = metaStoreClient.getTable(db, tableDesc.getName()).getPartitionKeys();
                logger.info("Checking the {} in {}", tableDesc.getName(), db);
            } catch (NoSuchObjectException noObjectException) {
                logger.info("table not exist in hive meta store for table:" + tableDesc.getIdentity(),
                        noObjectException);
                throw new BadRequestException(
                        "table doesn't exist in hive meta store for table:" + tableDesc.getIdentity(),
                        ResponseCode.CODE_UNDEFINED, noObjectException);
            } catch (Exception e) {
                logger.error("error when get metadata from hive meta store for table:" + tableDesc.getIdentity(), e);
                throw new BadRequestException("error when connect hive meta store", ResponseCode.CODE_UNDEFINED, e);
            }
            // check the data type compatibility for each column
            Map<String, FieldSchema> fieldSchemaMap = Maps.newHashMap();
            for (FieldSchema field : fields) {
                fieldSchemaMap.put(field.getName().toUpperCase(Locale.ROOT), field);
            }
            // partition column
            for (FieldSchema field : partitionFields) {
                fieldSchemaMap.put(field.getName().toUpperCase(Locale.ROOT), field);
            }
            List<String> incompatibleMsgs = Lists.newArrayList();
            for (ColumnDesc columnDesc : tableDesc.getColumns()) {
                FieldSchema fieldSchema = fieldSchemaMap.get(columnDesc.getName().toUpperCase(Locale.ROOT));
                if (fieldSchema == null) {
                    incompatibleMsgs.add("Column not exist in hive table:" + columnDesc.getName());
                    continue;
                }
                if (!checkHiveTableFieldCompatible(fieldSchema, columnDesc)) {
                    String msg = String.format(Locale.ROOT,
                            "column:%s defined in hive type:%s is incompatible with the column definition:%s",
                            columnDesc.getName(), fieldSchema.getType(), columnDesc.getDatatype());
                    incompatibleMsgs.add(msg);
                }
            }
            if (!incompatibleMsgs.isEmpty()) {
                logger.info("incompatible for hive and input table schema:{}", incompatibleMsgs);
                throw new BadRequestException(
                        "incompatible for hive schema and input table schema:" + incompatibleMsgs);
            }
        }
    }

    private static Map<String, Set<String>> COMPATIBLE_MAP = Maps.newHashMap();
    static {
        COMPATIBLE_MAP.put("float", Sets.newHashSet("double"));
        COMPATIBLE_MAP.put("string", Sets.newHashSet("varchar", "char", "varchar(256)"));
        COMPATIBLE_MAP.put("varchar", Sets.newHashSet("string", "char"));
        COMPATIBLE_MAP.put("varchar(256)", Sets.newHashSet("string", "char", "varchar"));
        COMPATIBLE_MAP.put("long", Sets.newHashSet("bigint", "int", "smallint", "integer"));
        COMPATIBLE_MAP.put("bigint", Sets.newHashSet("long", "int", "smallint", "integer"));
        COMPATIBLE_MAP.put("int", Sets.newHashSet("smallint", "integer"));
    }

    private boolean checkHiveTableFieldCompatible(FieldSchema fieldSchema, ColumnDesc columnDesc) {
        DataType normalized = DataType.getType(columnDesc.getDatatype());
        String normalizedDataType = normalized == null ? columnDesc.getDatatype() : normalized.toString();
        if (fieldSchema.getType().equals(normalizedDataType)) {
            return true;
        }
        Set<String> compatibleSet = COMPATIBLE_MAP.get(fieldSchema.getType());
        if (compatibleSet != null && compatibleSet.contains(normalizedDataType)) {
            return true;
        }
        return false;
    }

    @RequestMapping(value = "/updateConfig", method = { RequestMethod.PUT })
    @ResponseBody
    public StreamingRequest updateStreamingConfig(@RequestBody StreamingRequest streamingRequest)
            throws JsonProcessingException {
        StreamingSourceConfig streamingSourceConfig = deserializeStreamingConfig(streamingRequest.getStreamingConfig());

        if (streamingSourceConfig == null) {
            return streamingRequest;
        }

        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to updateStreamingConfig.", user);
        try {
            streamingService.updateStreamingConfig(streamingSourceConfig);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this StreamingSourceConfig.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
        streamingRequest.setSuccessful(true);

        return streamingRequest;
    }

    @Deprecated
    @RequestMapping(value = "/{project}/{configName}", method = { RequestMethod.DELETE }, produces = {
            "application/json" })
    @ResponseBody
    public void deleteConfig(@PathVariable String project, @PathVariable String configName) throws IOException {
        // This method will never be called by the frontend.
        StreamingSourceConfig config = streamingService.getStreamingManagerV2().getConfig(configName, project);
        if (null == config) {
            throw new NotFoundException("StreamingSourceConfig with name " + configName + " not found..");
        }

        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to delete config: {}", user, configName);

        try {
            streamingService.dropStreamingConfig(config);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(
                    "Failed to delete StreamingSourceConfig. " + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/parserTemplate", method = { RequestMethod.GET })
    @ResponseBody
    public String getParserTemplate(@RequestParam(value = "sourceType") int sourceType,
            @RequestParam(value = "streamingConfig") String streamingConfigStr) {
        StreamingSourceConfig streamingSourceConfig = deserializeStreamingConfig(streamingConfigStr);
        return streamingService.getParserTemplate(sourceType, streamingSourceConfig);
    }

    @RequestMapping(value = "/cubeAssignments", method = { RequestMethod.GET })
    @ResponseBody
    public List<CubeAssignment> getCubeAssignments(@RequestParam(value = "cube", required = false) String cube) {
        CubeInstance cubeInstance = null;
        if (cube != null) {
            cubeInstance = cubeMgmtService.getCubeManager().getCube(cube);
        }
        return streamingService.getStreamingCubeAssignments(cubeInstance);
    }

    @RequestMapping(value = "/rsAssignments", method = { RequestMethod.GET })
    @ResponseBody
    public Map<Integer, Map<String, List<Partition>>> getReplicaSetAssignments(
            @RequestParam(value = "replicaSetID", required = false) Integer replicaSetID) {
        return streamingService.getStreamingReplicaSetAssignments(replicaSetID);
    }

    @RequestMapping(value = "/balance/recommend", method = { RequestMethod.GET })
    @ResponseBody
    public Map<Integer, Map<String, List<Partition>>> reBalanceRecommend() {
        return streamingService.reBalancePlan();
    }

    @RequestMapping(value = "/balance", method = { RequestMethod.POST })
    @ResponseBody
    public void reBalance(@RequestBody String reBalancePlanStr) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to do reBalance.", user);
        streamingService.reBalance(deserializeRebalancePlan(reBalancePlanStr));
    }

    private Map<Integer, Map<String, List<Partition>>> deserializeRebalancePlan(String reBalancePlanStr) {
        TypeReference<Map<Integer, Map<String, List<Partition>>>> typeRef = new TypeReference<Map<Integer, Map<String, List<Partition>>>>() {
        };
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(reBalancePlanStr, typeRef);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @RequestMapping(value = "/cubeAssignments/{cubeName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void removeCubeAssignment(@PathVariable String cubeName) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to remove CubeAssignment {}", user, cubeName);
        streamingService.removeCubeAssignment();
    }

    @RequestMapping(value = "/cubes", method = { RequestMethod.GET })
    @ResponseBody
    public List<String> getStreamingCubes() {
        return streamingService.getStreamingCubes();
    }

    @RequestMapping(value = "/cubes/{cubeName}/consumeState", method = { RequestMethod.GET })
    @ResponseBody
    public String getStreamingCubeConsumeState(@PathVariable String cubeName) {
        return streamingService.getStreamingCubeConsumeState(cubeName).toString();
    }

    @RequestMapping(value = "/cubes/{cubeName}/assign", method = { RequestMethod.PUT })
    @ResponseBody
    public void assignStreamingCube(@PathVariable String cubeName) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to assign cube {}", user, cubeName);
        CubeInstance cube = cubeMgmtService.getCubeManager().getCube(cubeName);
        streamingService.assignCube(cube);
    }

    @RequestMapping(value = "/cubes/{cubeName}/unAssign", method = { RequestMethod.PUT })
    @ResponseBody
    public void unAssignStreamingCube(@PathVariable String cubeName) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to unAssign cube {}", user, cubeName);
        CubeInstance cube = cubeMgmtService.getCubeManager().getCube(cubeName);
        streamingService.unAssignCube(cube);
    }

    @RequestMapping(value = "/cubes/{cubeName}/reAssign", method = { RequestMethod.POST })
    @ResponseBody
    public void reAssignStreamingCube(@PathVariable String cubeName, @RequestBody CubeAssignment newAssignment) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to reAssign cube {}", user, cubeName);
        streamingService.reAssignCube(cubeName, newAssignment);
    }

    @RequestMapping(value = "/receivers", method = { RequestMethod.GET })
    @ResponseBody
    public List<Node> getStreamingReceivers() {
        return streamingService.getReceivers();
    }

    @RequestMapping(value = "/receivers/{receiverID:.+}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void removeStreamingReceiver(@PathVariable String receiverID) {
        Node receiver = Node.fromNormalizeString(receiverID);
        streamingService.removeReceiver(receiver);
    }

    @RequestMapping(value = "/replicaSet", method = { RequestMethod.POST })
    @ResponseBody
    public void createReplicaSet(@RequestBody ReplicaSet rs) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to create ReplicaSet {}", user, rs.getReplicaSetID());
        streamingService.createReplicaSet(rs);
    }

    @RequestMapping(value = "/replicaSet/{replicaSetID}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void removeReplicaSet(@PathVariable Integer replicaSetID) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to remove ReplicaSet {}", user, replicaSetID);
        streamingService.removeReplicaSet(replicaSetID);
    }

    @RequestMapping(value = "/replicaSets", method = { RequestMethod.GET })
    @ResponseBody
    public List<ReplicaSet> getReplicaSets() {
        return streamingService.getReplicaSets();
    }

    @RequestMapping(value = "/replicaSet/{replicaSetID}/{nodeID:.+}", method = { RequestMethod.PUT })
    @ResponseBody
    public void addNodeToReplicaSet(@PathVariable Integer replicaSetID, @PathVariable String nodeID) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to add Node {} To ReplicaSet {}", user, nodeID, replicaSetID);
        streamingService.addNodeToReplicaSet(replicaSetID, nodeID);
    }

    @RequestMapping(value = "/replicaSet/{replicaSetID}/{nodeID:.+}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void removeNodeFromReplicaSet(@PathVariable Integer replicaSetID, @PathVariable String nodeID) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to remove Node {} from ReplicaSet {}", user, nodeID, replicaSetID);
        streamingService.removeNodeFromReplicaSet(replicaSetID, nodeID);
    }

    @RequestMapping(value = "/cubes/{cubeName}/suspendConsume", method = { RequestMethod.PUT })
    @ResponseBody
    public void pauseCubeConsume(@PathVariable String cubeName) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to pause Consumers for cube {}", user, cubeName);
        CubeInstance cube = cubeMgmtService.getCubeManager().getCube(cubeName);
        streamingService.pauseConsumers(cube);
    }

    @RequestMapping(value = "/cubes/{cubeName}/resumeConsume", method = { RequestMethod.PUT })
    @ResponseBody
    public void resumeCubeConsume(@PathVariable String cubeName) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} try to resume Consumers for cube {}", user, cubeName);
        CubeInstance cube = cubeMgmtService.getCubeManager().getCube(cubeName);
        streamingService.resumeConsumers(cube);
    }

    @RequestMapping(value = "/cubes/{cubeName}/stats", method = { RequestMethod.GET })
    @ResponseBody
    public CubeRealTimeState getCubeRealTimeState(@PathVariable String cubeName) {
        CubeInstance cube = cubeMgmtService.getCubeManager().getCube(cubeName);
        return streamingService.getCubeRealTimeState(cube);
    }

    @RequestMapping(value = "/receivers/{receiverID:.+}/stats", method = { RequestMethod.GET })
    @ResponseBody
    public ReceiverStats getReceiverStats(@PathVariable String receiverID) {
        Node receiver = Node.fromNormalizeString(receiverID);
        return streamingService.getReceiverStats(receiver);
    }

    @RequestMapping(value = "/cluster/state", method = { RequestMethod.GET })
    @ResponseBody
    public ClusterState getClusterState() {
        return streamingService.getClusterState();
    }

    private TableDesc deserializeTableDesc(StreamingRequestV2 streamingRequest) {
        TableDesc desc = null;
        try {
            logger.debug("Saving TableDesc " + streamingRequest.getTableData());
            desc = JsonUtil.readValue(streamingRequest.getTableData(), TableDesc.class);
        } catch (JsonParseException e) {
            logger.error("The TableDesc definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data TableDesc definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }

        Preconditions.checkNotNull(desc, "Failed to deserialize from TableDesc definition");
        String[] dbTable = HadoopUtil.parseHiveTableName(desc.getName());
        desc.setName(dbTable[1]);
        desc.setDatabase(dbTable[0]);
        desc.getIdentity();
        return desc;
    }

    private StreamingSourceConfig deserializeStreamingConfig(String streamingConfigStr) {
        try {
            logger.debug("Saving StreamingSourceConfig " + streamingConfigStr);
            return JsonUtil.readValue(streamingConfigStr, StreamingSourceConfig.class);
        } catch (Exception e) {
            logger.error("The StreamingSourceConfig definition is invalid.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
    }

    private void updateRequest(StreamingRequestV2 request, boolean success, String message) {
        request.setSuccessful(success);
        request.setMessage(message);
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeMgmtService = cubeService;
    }

}
