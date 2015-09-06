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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * @author jiazhong
 */
public class QueryParser {

    final static Logger logger = Logger.getLogger(QueryParser.class);
    final static Charset ENCODING = StandardCharsets.UTF_8;
    final static String QUERY_LOG_FILE_PATTERN = "kylin_query.log.(\\d{4}-\\d{2}-\\d{2})$";
    final static String QUERY_LOG_PARSE_RESULT_FILENAME = "kylin_query_log.csv";
    static String QUERY_PARSE_RESULT_PATH = null;
    static String DEPLOY_ENV;

    final static String[] KYLIN_QUERY_CSV_HEADER = { "REQ_TIME", "REQ_DATE", "SQL", "USER", "IS_SUCCESS", "LATENCY", "PROJECT", "REALIZATION NAMES", "CUBOID IDS", "TOTAL SCAN COUNT", "RESULT ROW COUNT", "ACCEPT PARTIAL", "IS PARTIAL RESULT", "HIT CACHE", "MESSAGE", "DEPLOY_ENV" };

    private ConfigUtils monitorConfig;

    public QueryParser() {
        monitorConfig = ConfigUtils.getInstance();
        try {
            monitorConfig.loadMonitorParam();
            DEPLOY_ENV = monitorConfig.getDeployEnv();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * will parse kylin query log files ,and append result to csv on hdfs
     * files read will be marked,will not read again
     */

    public void start() throws IOException, ParseException {
        QueryParser.QUERY_PARSE_RESULT_PATH = ConfigUtils.getInstance().getQueryLogParseResultDir() + QUERY_LOG_PARSE_RESULT_FILENAME;
        this.parseQueryInit();

        //get query file has been read
        String[] hasReadFiles = MonitorMetaManager.getReadQueryLogFileList();

        //get all log files
        List<File> files = this.getQueryLogFiles();

        for (File file : files) {
            if (!Arrays.asList(hasReadFiles).contains(file.getName())) {
                this.parseQueryLog(file.getPath(), QueryParser.QUERY_PARSE_RESULT_PATH);
                MonitorMetaManager.markQueryFileAsRead(file.getName());
            }
        }
    }

    public void parseQueryInit() throws IOException {
        logger.info("parse query initializing...");
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(conf);
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(QueryParser.QUERY_PARSE_RESULT_PATH);
            if (!fs.exists(path)) {
                fs.create(path);
                fs.close(); //need to close before get FileSystem again
                this.writeResultToHdfs(QueryParser.QUERY_PARSE_RESULT_PATH, QueryParser.KYLIN_QUERY_CSV_HEADER);
            }
        } catch (IOException e) {
            if(fs != null) {
                fs.close();
            }
            logger.info("Failed to init:", e);
        }
    }

    //parse query log and convert to csv file to hdfs
    public void parseQueryLog(String filePath, String dPath) throws ParseException, IOException {

        logger.info("Start parsing file " + filePath + " !");

        //        writer config init
        FileSystem fs = this.getHdfsFileSystem();
        org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
        OutputStreamWriter writer = new OutputStreamWriter(fs.append(resultStorePath));
        CSVWriter cwriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        Pattern p_query_start = Pattern.compile("^\\[.*\\]:\\[(.*),.*\\]\\[.*\\]\\[.*QueryService.logQuery.*\\].*");
        Pattern p_query_end = Pattern.compile("^Message:(.*)$");
        Pattern p_query_body = Pattern.compile("^\\[.*\\]:\\[((\\d{4}-\\d{2}-\\d{2}).*)\\]\\[.*\\]\\[.*\\].*\n^=+\\[QUERY\\]=+\n^SQL:(.*)\n^User:(.*)\n^Success:(.*)\n^Duration:(.*)\n^Project:(.*)\n^(Realization Names|Cube Names): \\[(.*)\\]\n^Cuboid Ids: \\[(.*)\\]\n^Total scan count:(.*)\n^Result row count:(.*)\n^Accept Partial:(.*)\n(^Is Partial Result:(.*)\n)?^Hit Cache:(.*)\n^Message:(.*)", Pattern.MULTILINE);
        Matcher m_query_start = p_query_start.matcher("");
        Matcher m_query_end = p_query_end.matcher("");
        Matcher m_query_body = p_query_body.matcher("");

        boolean query_start = false;
        StringBuffer query_body = new StringBuffer("");
        Path path = Paths.get(filePath);
        try {
            BufferedReader reader = Files.newBufferedReader(path, ENCODING);
            String line = null;
            while ((line = reader.readLine()) != null) {
                m_query_start.reset(line); //reset the input
                m_query_end.reset(line);

                // set start flag ,clear StringBuffer
                if (m_query_start.find()) {
                    query_start = true;
                    query_body = new StringBuffer("");
                }
                if (query_start) {
                    query_body.append(line + "\n");
                }
                if (m_query_end.find()) {
                    query_start = false;
                    m_query_body.reset(query_body);
                    logger.info("parsing query...");
                    logger.info(query_body);
                    //                    skip group(8) and group(14)
                    if (m_query_body.find()) {
                        ArrayList<String> groups = new ArrayList<String>();
                        int grp_count = m_query_body.groupCount();
                        for (int i = 1; i <= grp_count; i++) {
                            if (i != 8 && i != 14) {
                                String grp_item = m_query_body.group(i);
                                grp_item = grp_item == null ? "" : grp_item.trim();
                                groups.add(grp_item);
                            }
                        }

                        long start_time = format.parse(groups.get(0)).getTime() - (int) (Double.parseDouble(groups.get(5)) * 1000);
                        groups.set(0, format.format(new Date(start_time)));
                        groups.add(DEPLOY_ENV);
                        String[] recordArray = groups.toArray(new String[groups.size()]);
                        //                        write to hdfs
                        cwriter.writeNext(recordArray);

                    }

                }

            }
        } catch (IOException ex) {
            logger.info("Failed to write to hdfs:", ex);
        } finally {
            if(writer != null) {
                writer.close();
            }
            if(cwriter != null) {
                cwriter.close();
            }
            if(fs != null) {
                fs.close();
            }
        }

        logger.info("Finish parsing file " + filePath + " !");

    }

    /*
     * write parse result to hdfs
     */
    public void writeResultToHdfs(String dPath, String[] record) throws IOException {
        OutputStreamWriter writer = null;
        CSVWriter cwriter = null;
        FileSystem fs = null;
        try {
            fs = this.getHdfsFileSystem();
            org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
            writer = new OutputStreamWriter(fs.append(resultStorePath));
            cwriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER);

            cwriter.writeNext(record);

        } catch (IOException e) {
            logger.info("Exception", e);
        } finally {
            if(writer != null) {
                writer.close();
            }
            if(cwriter != null) {
                cwriter.close();
            }
            if(fs != null) {
                fs.close();
            }
        }
    }

    /*
     * get all query log files
     */
    public List<File> getQueryLogFiles() {
        List<File> logFiles = new ArrayList<File>();

        List<String> query_log_dir_list = monitorConfig.getLogBaseDir();
        FileFilter filter = new RegexFileFilter(QUERY_LOG_FILE_PATTERN);

        for (String path : query_log_dir_list) {
            logger.info("fetching query log file from path:" + path);
            File query_log_dir = new File(path);
            File[] query_log_files = query_log_dir.listFiles(filter);
            if (query_log_files == null) {
                logger.warn("no query log file found under path" + path);
                continue;
            }

            Collections.addAll(logFiles, query_log_files);
        }
        return logFiles;
    }

    /*
     * get hdfs fileSystem
     */
    public FileSystem getHdfsFileSystem() throws IOException {
        Configuration conf = new Configuration();
        //        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            if(fs != null) {
                fs.close();
            }
            logger.info("Failed to get hdfs FileSystem", e);
        }
        return fs;
    }

}