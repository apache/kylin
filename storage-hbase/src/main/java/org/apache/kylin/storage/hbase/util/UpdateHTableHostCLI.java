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

package org.apache.kylin.storage.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 1/18/16.
 */
public class UpdateHTableHostCLI {
    private static final Logger logger = LoggerFactory.getLogger(UpdateHTableHostCLI.class);

    private List<String> updatedResources = Lists.newArrayList();
    private List<String> errorMsgs = Lists.newArrayList();

    private List<String> htables;
    private Admin hbaseAdmin;
    private KylinConfig kylinConfig;
    private String oldHostValue;

    public UpdateHTableHostCLI(List<String> htables, String oldHostValue) throws IOException {
        this.htables = htables;
        this.oldHostValue = oldHostValue;
        Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create());
        hbaseAdmin = conn.getAdmin();
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsageAndExit();
        }

        List<String> tableNames = getHTableNames(KylinConfig.getInstanceFromEnv());
        if (!args[0].toLowerCase().equals("-from")) {
            printUsageAndExit();
        }
        String oldHostValue = args[1].toLowerCase();
        String filterType = args[2].toLowerCase();
        if (filterType.equals("-table")) {
            tableNames = filterByTables(tableNames, Arrays.asList(args).subList(3, args.length));
        } else if (filterType.equals("-cube")) {
            tableNames = filterByCubes(tableNames, Arrays.asList(args).subList(3, args.length));
        } else if (!filterType.equals("-all")) {
            printUsageAndExit();
        }
        logger.info("These htables are needed to be updated: " + StringUtils.join(tableNames, ","));

        UpdateHTableHostCLI updateHTableHostCLI = new UpdateHTableHostCLI(tableNames, oldHostValue);
        updateHTableHostCLI.execute();

        logger.info("=================================================================");
        logger.info("Run UpdateHTableHostCLI completed;");

        if (!updateHTableHostCLI.updatedResources.isEmpty()) {
            logger.info("Following resources are updated successfully:");
            for (String s : updateHTableHostCLI.updatedResources) {
                logger.info(s);
            }
        } else {
            logger.warn("No resource updated.");
        }

        if (!updateHTableHostCLI.errorMsgs.isEmpty()) {
            logger.info("Here are the error/warning messages, you may need to check:");
            for (String s : updateHTableHostCLI.errorMsgs) {
                logger.warn(s);
            }
        } else {
            logger.info("No error or warning messages; The update succeeds.");
        }

        logger.info("=================================================================");
    }

    private static void printUsageAndExit() {
        logger.info("Usage: exec -from oldHostValue -all|-cube cubeA,cubeB|-table tableA,tableB");
        System.exit(0);
    }

    private static List<String> getHTableNames(KylinConfig config) {
        CubeManager cubeMgr = CubeManager.getInstance(config);

        ArrayList<String> result = new ArrayList<String>();
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments(SegmentStatusEnum.READY)) {
                String tableName = seg.getStorageLocationIdentifier();
                if (!StringUtils.isBlank(tableName)) {
                    result.add(tableName);
                    System.out.println("added new table: " + tableName);
                }
            }
        }

        return result;
    }

    private static List<String> filterByCubes(List<String> allTableNames, List<String> cubeNames) {
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<String> result = Lists.newArrayList();
        for (String c : cubeNames) {
            c = c.trim();
            if (c.endsWith(","))
                c = c.substring(0, c.length() - 1);

            CubeInstance cubeInstance = cubeManager.getCube(c);
            for (CubeSegment segment : cubeInstance.getSegments()) {
                String tableName = segment.getStorageLocationIdentifier();
                if (allTableNames.contains(tableName)) {
                    result.add(tableName);
                }
            }
        }
        return result;
    }

    private static List<String> filterByTables(List<String> allTableNames, List<String> tableNames) {
        List<String> result = Lists.newArrayList();
        for (String t : tableNames) {
            t = t.trim();
            if (t.endsWith(","))
                t = t.substring(0, t.length() - 1);

            if (allTableNames.contains(t)) {
                result.add(t);
            }
        }
        return result;
    }

    private void updateHtable(String tableName) throws IOException {
        HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
        if (oldHostValue.equals(desc.getValue(IRealizationConstants.HTableTag))) {
            desc.setValue(IRealizationConstants.HTableTag, kylinConfig.getMetadataUrlPrefix());
            hbaseAdmin.disableTable(TableName.valueOf(tableName));
            hbaseAdmin.modifyTable(TableName.valueOf(tableName), desc);
            hbaseAdmin.enableTable(TableName.valueOf(tableName));

            updatedResources.add(tableName);
        }
    }

    public void execute() {
        for (String htable : htables) {
            try {
                updateHtable(htable);
            } catch (IOException ex) {
                ex.printStackTrace();
                errorMsgs.add("Update HTable[" + htable + "] failed: " + ex.getMessage());
            }
        }
    }
}
