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

package org.apache.kylin.storage.hbase.steps;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Preconditions;

/**
 */
public class HbaseImporter {

    private static final Log logger = LogFactory.getLog(HbaseImporter.class);

    public static void importHBaseData(String hbaseTarLocation, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        if (System.getenv("JAVA_HOME") == null) {
            logger.error("Didn't find $JAVA_HOME, this will cause HBase data import failed. Please set $JAVA_HOME.");
            logger.error("Skipping table import...");
            return;
        }

        File exportFile = new File(hbaseTarLocation);
        if (!exportFile.exists()) {
            logger.error("Didn't find the export achieve file on " + exportFile.getAbsolutePath());
            return;
        }

        File folder = File.createTempFile("hbase-import", "tmp");
        if (folder.exists()) {
            FileUtils.forceDelete(folder);
        }
        folder.mkdirs();
        FileUtils.forceDeleteOnExit(folder);

        //TarGZUtil.uncompressTarGZ(exportFile, folder);
        FileUtil.unTar(exportFile, folder);
        String[] child = folder.list();
        Preconditions.checkState(child.length == 1);
        String backupFolderName = child[0];
        File backupFolder = new File(folder, backupFolderName);
        String[] tableNames = backupFolder.list();

        for (String table : tableNames) {

            if (!(table.equalsIgnoreCase(HBaseMiniclusterHelper.TEST_METADATA_TABLE) || table.startsWith(HBaseMiniclusterHelper.SHARED_STORAGE_PREFIX))) {
                continue;
            }

            // create the htable; otherwise the import will fail.
            if (table.startsWith(HBaseMiniclusterHelper.II_STORAGE_PREFIX)) {
                HBaseConnection.createHTableIfNeeded(KylinConfig.getInstanceFromEnv().getStorageUrl(), table, "f");
            } else if (table.startsWith(HBaseMiniclusterHelper.CUBE_STORAGE_PREFIX)) {
                HBaseConnection.createHTableIfNeeded(KylinConfig.getInstanceFromEnv().getStorageUrl(), table, "F1", "F2");
            }

            // directly import from local fs, no need to copy to hdfs
            String importLocation = "file://" + backupFolder.getAbsolutePath() + "/" + table;
            String[] args = new String[] { table, importLocation };
            boolean result = runImport(args, conf);
            logger.info("importing table '" + table + "' with result:" + result);

            if (!result)
                break;
        }

    }

    private static boolean runImport(String[] args, Configuration configuration) throws IOException, InterruptedException, ClassNotFoundException {
        // need to make a copy of the configuration because to make sure different temp dirs are used.
        GenericOptionsParser opts = new GenericOptionsParser(new Configuration(configuration), args);
        Configuration newConf = opts.getConfiguration();
        args = opts.getRemainingArgs();
        Job job = Import.createSubmittableJob(newConf, args);
        job.waitForCompletion(false);
        return job.isSuccessful();
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 1) {
            logger.error("Usage: HbaseImporter hbase_tar_lcoation");
            System.exit(-1);
        }

        logger.info("The KylinConfig being used:");
        logger.info("=================================================");
        KylinConfig.getInstanceFromEnv().printProperties();
        logger.info("=================================================");

        importHBaseData(args[0], HBaseConfiguration.create());
    }
}
