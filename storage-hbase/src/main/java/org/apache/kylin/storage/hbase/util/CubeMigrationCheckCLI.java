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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * <p/>
 * This tool serves for the purpose of
 * checking the "KYLIN_HOST" property to be consistent with the dst's MetadataUrlPrefix
 * for all of cube segments' corresponding HTables after migrating a cube
 * <p/>
 */
@Deprecated
public class CubeMigrationCheckCLI {

    private static final Logger logger = LoggerFactory.getLogger(CubeMigrationCheckCLI.class);

    private static final Option OPTION_FIX = OptionBuilder.withArgName("fix").hasArg().isRequired(false).withDescription("Fix the inconsistent cube segments' HOST").create("fix");

    private static final Option OPTION_DST_CFG_URI = OptionBuilder.withArgName("dstCfgUri").hasArg().isRequired(false).withDescription("The KylinConfig of the cube’s new home").create("dstCfgUri");

    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(false).withDescription("The name of cube migrated").create("cube");

    private KylinConfig dstCfg;
    private Admin hbaseAdmin;

    private List<String> issueExistHTables;
    private List<String> inconsistentHTables;

    private boolean ifFix = false;

    public static void main(String[] args) throws ParseException, IOException {
        logger.warn("org.apache.kylin.storage.hbase.util.CubeMigrationCheckCLI is deprecated, use org.apache.kylin.tool.CubeMigrationCheckCLI instead");

        OptionsHelper optionsHelper = new OptionsHelper();

        Options options = new Options();
        options.addOption(OPTION_FIX);
        options.addOption(OPTION_DST_CFG_URI);
        options.addOption(OPTION_CUBE);

        boolean ifFix = false;
        String dstCfgUri;
        String cubeName;
        logger.info("jobs args: " + Arrays.toString(args));
        try {

            optionsHelper.parseOptions(options, args);

            logger.info("options: '" + options.toString() + "'");
            logger.info("option value 'fix': '" + optionsHelper.getOptionValue(OPTION_FIX) + "'");
            ifFix = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_FIX));

            logger.info("option value 'dstCfgUri': '" + optionsHelper.getOptionValue(OPTION_DST_CFG_URI) + "'");
            dstCfgUri = optionsHelper.getOptionValue(OPTION_DST_CFG_URI);

            logger.info("option value 'cube': '" + optionsHelper.getOptionValue(OPTION_CUBE) + "'");
            cubeName = optionsHelper.getOptionValue(OPTION_CUBE);

        } catch (ParseException e) {
            optionsHelper.printUsage(CubeMigrationCheckCLI.class.getName(), options);
            throw e;
        }

        KylinConfig kylinConfig;
        if (dstCfgUri == null) {
            kylinConfig = KylinConfig.getInstanceFromEnv();
        } else {
            kylinConfig = KylinConfig.createInstanceFromUri(dstCfgUri);
        }

        CubeMigrationCheckCLI checkCLI = new CubeMigrationCheckCLI(kylinConfig, ifFix);
        checkCLI.execute(cubeName);
    }

    public void execute() throws IOException {
        execute(null);
    }

    public void execute(String cubeName) throws IOException {
        if (cubeName == null) {
            checkAll();
        } else {
            checkCube(cubeName);
        }
        fixInconsistent();
        printIssueExistingHTables();
    }

    public CubeMigrationCheckCLI(KylinConfig kylinConfig, Boolean isFix) throws IOException {
        this.dstCfg = kylinConfig;
        this.ifFix = isFix;

        Connection conn = HBaseConnection.get(kylinConfig.getStorageUrl());
        hbaseAdmin = conn.getAdmin();
        issueExistHTables = Lists.newArrayList();
        inconsistentHTables = Lists.newArrayList();
    }

    public void checkCube(String cubeName) {
        List<String> segFullNameList = Lists.newArrayList();

        CubeInstance cube = CubeManager.getInstance(dstCfg).getCube(cubeName);
        addHTableNamesForCube(cube, segFullNameList);

        check(segFullNameList);
    }

    public void checkAll() {
        List<String> segFullNameList = Lists.newArrayList();

        CubeManager cubeMgr = CubeManager.getInstance(dstCfg);
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            addHTableNamesForCube(cube, segFullNameList);
        }

        check(segFullNameList);
    }

    public void addHTableNamesForCube(CubeInstance cube, List<String> segFullNameList) {
        for (CubeSegment seg : cube.getSegments()) {
            String tableName = seg.getStorageLocationIdentifier();
            segFullNameList.add(tableName + "," + cube.getName());
        }
    }

    public void check(List<String> segFullNameList) {
        issueExistHTables = Lists.newArrayList();
        inconsistentHTables = Lists.newArrayList();

        for (String segFullName : segFullNameList) {
            String[] sepNameList = segFullName.split(",");
            try {
                HTableDescriptor hTableDescriptor = hbaseAdmin.getTableDescriptor(TableName.valueOf(sepNameList[0]));
                String host = hTableDescriptor.getValue(IRealizationConstants.HTableTag);
                if (!dstCfg.getMetadataUrlPrefix().equalsIgnoreCase(host)) {
                    inconsistentHTables.add(segFullName);
                }
            } catch (IOException e) {
                issueExistHTables.add(segFullName);
                continue;
            }
        }
    }

    public void fixInconsistent() throws IOException {
        if (ifFix == true) {
            for (String segFullName : inconsistentHTables) {
                String[] sepNameList = segFullName.split(",");
                HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(sepNameList[0]));
                logger.info("Change the host of htable " + sepNameList[0] + "belonging to cube " + sepNameList[1] + " from " + desc.getValue(IRealizationConstants.HTableTag) + " to " + dstCfg.getMetadataUrlPrefix());
                hbaseAdmin.disableTable(TableName.valueOf(sepNameList[0]));
                desc.setValue(IRealizationConstants.HTableTag, dstCfg.getMetadataUrlPrefix());
                hbaseAdmin.modifyTable(TableName.valueOf(sepNameList[0]), desc);
                hbaseAdmin.enableTable(TableName.valueOf(sepNameList[0]));
            }
        } else {
            logger.info("------ Inconsistent HTables Needed To Be Fixed ------");
            for (String hTable : inconsistentHTables) {
                String[] sepNameList = hTable.split(",");
                logger.info(sepNameList[0] + " belonging to cube " + sepNameList[1]);
            }
            logger.info("----------------------------------------------------");
        }
    }

    public void printIssueExistingHTables() {
        logger.info("------ HTables exist issues in hbase : not existing, metadata broken ------");
        for (String segFullName : issueExistHTables) {
            String[] sepNameList = segFullName.split(",");
            logger.error(sepNameList[0] + " belonging to cube " + sepNameList[1] + " has some issues and cannot be read successfully!!!");
        }
        logger.info("----------------------------------------------------");
    }
}
