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

package org.apache.kylin.engine.mr.steps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildGlobalHiveDictTotalBuildMapper<KEYIN, KEYOUT> extends KylinMapper<KEYIN, Text, Text, LongWritable> {
    private static final Logger logger = LoggerFactory.getLogger(BuildGlobalHiveDictTotalBuildMapper.class);

    private MultipleOutputs mos;
    private Integer colIndex = null;
    private String colName = null;
    private Long start = 0L;//start index
    private String[] cols = null;

    @Override
    protected void doSetup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        mos = new MultipleOutputs(context);

        KylinConfig config;
        try {
            config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        cols = config.getMrHiveDictColumnsExcludeRefColumns();


        String statPath = conf.get("partition.statistics.path");

        // get the input file name ,the file name format by colIndex-part-partitionNum, eg: 1-part-000019
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String[] arr = fileSplit.getPath().getName().split("-");
        int partitionNum = Integer.parseInt(arr[2]);
        colIndex = Integer.parseInt(arr[0]);
        colName = cols[colIndex];
        logger.info("Input fileName:{}, colIndex:{}, colName:{}, partitionNum:{}", fileSplit.getPath().getName(), colIndex, colName, partitionNum);

        //last max dic value per column
        String lastMaxValuePath = conf.get("last.max.dic.value.path");
        logger.info("last.max.dic.value.path:" + lastMaxValuePath);
        long lastMaxDictValue = this.getLastMaxDicValue(conf, lastMaxValuePath);
        logger.info("last.max.dic.value.path:" + lastMaxValuePath + ",value=" + lastMaxDictValue);

        // Calculate the starting position of this file, the starting position of this file = sum (count) of all previous numbers + last max dic value of the column
        Map<Integer, TreeMap<Integer, Long>> allStats = getPartitionsCount(conf, statPath); //<colIndex,<reduceNum,count>>
        TreeMap<Integer, Long> partitionStats = allStats.get(colIndex);
        if (partitionNum != 0) {
            SortedMap<Integer, Long> subStat = partitionStats.subMap(0, true, partitionNum, false);
            subStat.forEach((k, v) -> {
                logger.info("Split num:{} and it's count:{}", k, v);
                start += v;
            });
        }
        start += lastMaxDictValue;
        logger.info("global dic.{}.split.num.{} build dict start offset is {}", colName, partitionNum, start);
    }

    @Override
    public void doMap(KEYIN key, Text record, Context context) throws IOException, InterruptedException {
        long inkey = Long.parseLong(key.toString());
        mos.write(colIndex + "", record, new LongWritable(start + inkey), "dict_column=" + colName + "/" + colIndex);
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    private Map<Integer, TreeMap<Integer, Long>> getPartitionsCount(Configuration conf, String partitionStatPath) throws IOException {
        StringBuffer sb = new StringBuffer();
        String temp = null;

        String[] fileNameArr = null;
        String[] statsArr = null;

        //colStats key is last step reduce num,value is last step that reduce item count
        TreeMap<Integer, Long> colStats = null;
        //allStats key is colIndex,value is colStats(that col statistics info)
        Map<Integer, TreeMap<Integer, Long>> allStats = new HashMap<>();

        Path path = new Path(partitionStatPath);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path) && fs.isDirectory(path)) {
            for (FileStatus status : fs.listStatus(path)) {
                //fileNameArr[0] is globaldict colIndex
                fileNameArr = status.getPath().getName().split("-");
                colStats = allStats.get(Integer.parseInt(fileNameArr[0]));
                if (colStats == null) {
                    colStats = new TreeMap<>();
                }
                temp = cat(status.getPath(), fs);
                logger.info("partitionStatPath:{},content:{}", partitionStatPath, temp);
                if (temp != null) {
                    statsArr = temp.split("\t");
                    colStats.put(Integer.parseInt(statsArr[1]), Long.parseLong(statsArr[0]));
                    allStats.put(Integer.parseInt(fileNameArr[0]), colStats);
                }
            }
        }

        allStats.forEach((k, v) -> {
            v.forEach((k1, v1) -> {
                logger.info("allStats.colIndex:{},this split num:{},this split num's count:{}", k, k1, v1);
            });
        });

        return allStats;
    }

    private String cat(Path remotePath, FileSystem fs) throws IOException {
        FSDataInputStream in = null;
        BufferedReader buffer = null;
        StringBuffer stat = new StringBuffer();
        try {
            in = fs.open(remotePath);
            buffer = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            String line = null;
            while ((line = buffer.readLine()) != null) {
                stat.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (buffer != null) {
                buffer.close();
            }
            if (in != null) {
                in.close();
            }
        }
        return stat.toString();
    }

    /**
     * @param lastMaxDicValuePath eg: /user/kylin/warehouse/db/kylin_intermediate_kylin_sales_cube_mr_6222c210_ce2d_e8ce_dd0f_f12c38fa9115__group_by/dict_column=KYLIN_MAX_DISTINCT_COUNT/part-00000-450ee120-39ff-4806-afaf-ed482ceffc68-c000
     *                            remotePath content is dict column stats info of per column: dic column name,extract distinct value count,last max dic value
     * @return this colIndex's last max dic value
     */
    private long getLastMaxDicValue(Configuration conf, String lastMaxDicValuePath) throws IOException {
        StringBuffer sb = new StringBuffer();
        Map<Integer, Long> map = null;
        Path path = new Path(lastMaxDicValuePath);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path) && fs.isDirectory(path)) {
            for (FileStatus status : fs.listStatus(path)) {
                logger.info("start buildMaxCountMap :");
                map = buildMaxCountMap(status.getPath(), fs);
                logger.info("end buildMaxCountMap :");
            }
        }
        if (map == null) {
            return 0L;
        } else {
            return map.get(colIndex) == null ? 0L : map.get(colIndex);
        }
    }

    /**
     * @param remotePath , eg: /user/kylin/warehouse/db/kylin_intermediate_kylin_sales_cube_mr_6222c210_ce2d_e8ce_dd0f_f12c38fa9115__group_by/dict_column=KYLIN_MAX_DISTINCT_COUNT/part-00000-450ee120-39ff-4806-afaf-ed482ceffc68-c000
     *                   remotePath content is dict column stats info of per column: dic column name,extract distinct value count,last max dic value
     * @return Map<>,key is colIndex, value is last max dict value
     */
    private Map<Integer, Long> buildMaxCountMap(Path remotePath, FileSystem fs) throws IOException {
        FSDataInputStream in = null;
        BufferedReader buffer = null;
        String[] arr = null;
        Map<Integer, Long> map = new HashMap<>();
        try {
            in = fs.open(remotePath);
            buffer = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line = null;
            while ((line = buffer.readLine()) != null) {
                arr = line.split(",");
                logger.info("line=" + line + ",arr.length:" + arr.length);
                if (arr.length == 3) {
                    for (int i = 0; i < cols.length; i++) {
                        if (cols[i].equalsIgnoreCase(arr[0])) {
                            map.put(i, Long.parseLong(arr[2]));
                            logger.info("col.{}.maxValue={}", cols[i], Long.parseLong(arr[2]));
                            break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (buffer != null) {
                buffer.close();
            }
            if (in != null) {
                in.close();
            }
        }
        logger.info("BuildMaxCountMap map=" + map);
        return map;
    }
}
