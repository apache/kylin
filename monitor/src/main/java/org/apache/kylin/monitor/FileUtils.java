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

package org.apache.kylin.monitor;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * Created by jiazhong on 2015/6/18.
 */
public class FileUtils {

    final static Logger logger = Logger.getLogger(FileUtils.class);

    public static boolean pathCheck(String filePath) throws IOException {
        logger.info("checking file:" + filePath);
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(conf);
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filePath);
            if (!fs.exists(path)) {
                fs.create(path);
                fs.close();
                return false;
            }
        } catch (Exception e) {
            fs.close();
            logger.info("Failed to init:", e);
        }
        return true;
    }

    /*
    * write parse result to hdfs
    */
    public static void clearHdfsFile(String dPath) throws IOException {
        OutputStreamWriter writer = null;
        FileSystem fs = null;
        try {
            fs = getHdfsFileSystem();
            org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
            writer = new OutputStreamWriter(fs.create(resultStorePath, true));

        } catch (Exception e) {
            logger.info("Exception", e);
        } finally {
            writer.close();
            fs.close();
        }
    }

    /*
    * write parse result to hdfs
    */
    public static void appendResultToHdfs(String dPath, String[] record) throws IOException {
        OutputStreamWriter writer = null;
        CSVWriter cwriter = null;
        FileSystem fs = null;
        try {
            fs = getHdfsFileSystem();
            org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
            writer = new OutputStreamWriter(fs.append(resultStorePath));
            cwriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER);

            cwriter.writeNext(record);

        } catch (Exception e) {
            logger.info("Exception", e);
        } finally {
            writer.close();
            cwriter.close();
            fs.close();
        }
    }

    /*
     * get hdfs fileSystem
     */
    public static FileSystem getHdfsFileSystem() throws IOException {
        Configuration conf = new Configuration();
        //        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        FileSystem fs = null;
        try {
            fs = FileSystem.newInstance(conf);
        } catch (IOException e) {
            fs.close();
            logger.info("Failed to get hdfs FileSystem", e);
        }
        return fs;
    }

}
