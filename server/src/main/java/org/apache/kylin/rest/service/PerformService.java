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

import au.com.bytecode.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author jiazhong
 */
@Component("performService")
public class PerformService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(PerformService.class);

    /*
     * @return all query user
     */
    public  List<String[]> getTotalQueryUser() throws IOException {
        String filePath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()+"/performance/metadata/total_query_user.csv";
        List<String[]> res = readHdfsFile(filePath);
        logger.info("Total Query User:"+res.get(0)[0]);
       return  res;
    }

     /*
     * @return last 30 daily query num
     */
    public  List<String[]> dailyQueryCount() throws IOException {
        String filePath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()+"/performance/metadata/last_30_daily_query_count.csv";
        List<String[]> res = readHdfsFile(filePath);
       return  res;
    }

    /*
     * @return average query count every day
     */
    public  List<String[]> avgDayQuery() throws IOException {
        String filePath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()+"/performance/metadata/avg_day_query.csv";
        List<String[]> res = readHdfsFile(filePath);
        logger.info("Avg Day Query:"+res.get(0)[0]);
        return  res;
    }

    /*
     *@return average latency every day
     */
    public List<String[]> last30DayPercentile() throws IOException {
        String filePath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()+"/performance/metadata/last_30_day_90_percentile_latency.csv";
        List<String[]> res = readHdfsFile(filePath);
        return res;
    }

    /*
     *@return average latency for every cube
     */
    public List<String[]> eachDayPercentile() throws IOException {
        String filePath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()+"/performance/metadata/each_day_90_95_percentile_latency.csv";
        List<String[]> res = readHdfsFile(filePath);
        return res;
    }

    /*
     *@return average latency for every cube
     */
    public List<String[]> projectPercentile() throws IOException {
        String filePath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()+"/performance/metadata/project_90_95_percentile_latency.csv";
        List<String[]> res = readHdfsFile(filePath);
        return res;
    }


    private List<String[]> readHdfsFile(String filePath) throws IOException {
        List<String[]> allRows = null;
        CSVReader reader = null;
        FileSystem fs = null;
        Configuration conf = new Configuration();

        try {
            fs = FileSystem.newInstance(conf);
            FSDataInputStream inputStream = fs.open(new Path(filePath));
            reader = new CSVReader(new InputStreamReader (inputStream),'|');

            //Read all rows at once
            allRows = reader.readAll();


        } catch (IOException e) {
            logger.info("failed to read hdfs file:",e);
        }
        finally {
            fs.close();
        }
        return allRows;
    }
}
