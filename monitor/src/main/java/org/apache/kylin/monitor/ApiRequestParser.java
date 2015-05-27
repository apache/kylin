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

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jiazhong
 */
public class ApiRequestParser {

    final static Logger logger = Logger.getLogger(ApiRequestParser.class);
    final static Charset ENCODING = StandardCharsets.UTF_8;
    static String REQUEST_PARSE_RESULT_PATH = null;
    final static String REQUEST_LOG_FILE_PATTERN = "kylin_request.log.(\\d{4}-\\d{2}-\\d{2})$";
    final static String REQUEST_LOG_PARSE_RESULT_FILENAME = "kylin_request_log.csv";
    static String DEPLOY_ENV;


    final static String[] KYLIN_REQUEST_CSV_HEADER = {"REQUESTER", "REQ_TIME","REQ_DATE", "URI", "METHOD", "QUERY_STRING", "PAYLOAD", "RESP_STATUS", "TARGET", "ACTION","DEPLOY_ENV"};

    private ConfigUtils monitorConfig;

    public ApiRequestParser() {
        monitorConfig = ConfigUtils.getInstance();
        try {
            monitorConfig.loadMonitorParam();
            DEPLOY_ENV = monitorConfig.getDeployEnv();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException, ParseException {
        ApiRequestParser.REQUEST_PARSE_RESULT_PATH = ConfigUtils.getInstance().getRequestLogParseResultDir() + REQUEST_LOG_PARSE_RESULT_FILENAME;
        this.parseRequestInit();

        //get api req log files have been read
        String[] hasReadFiles = MonitorMetaManager.getReadApiReqLogFileList();

        List<File> files = this.getRequestLogFiles();
        for (File file : files) {
            if (!Arrays.asList(hasReadFiles).contains(file.getName())) {
                this.parseRequestLog(file.getPath(), ApiRequestParser.REQUEST_PARSE_RESULT_PATH);
                MonitorMetaManager.markApiReqLogFileAsRead(file.getName());
            }
        }
    }

    public void parseRequestInit() throws IOException {
        logger.info("parse api request initializing...");
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(conf);
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(ApiRequestParser.REQUEST_PARSE_RESULT_PATH);
            if (!fs.exists(path)) {
                fs.create(path);

                //need to close before get FileSystem again
                fs.close();
                this.writeResultToHdfs(ApiRequestParser.REQUEST_PARSE_RESULT_PATH, ApiRequestParser.KYLIN_REQUEST_CSV_HEADER);
            }
        } catch (Exception e) {
            fs.close();
            logger.info("Failed to init:", e);
        }
    }

    //parse query log and convert to csv file to hdfs
    public void parseRequestLog(String filePath, String dPath) throws ParseException, IOException {

        logger.info("Start parsing kylin api request file " + filePath + " !");

//        writer config init
        FileSystem fs = this.getHdfsFileSystem();
        org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
        OutputStreamWriter writer = new OutputStreamWriter(fs.append(resultStorePath));
        CSVWriter cwriter = new CSVWriter(writer,'|',CSVWriter.NO_QUOTE_CHARACTER);

        Pattern p_available = Pattern.compile("/kylin/api/(cubes|user)+.*");
        Pattern p_request = Pattern.compile("^.*\\[.*KylinApiFilter.logRequest.*\\].*REQUEST:.*REQUESTER=(.*);REQ_TIME=(\\w+ (\\d{4}-\\d{2}-\\d{2}).*);URI=(.*);METHOD=(.*);QUERY_STRING=(.*);PAYLOAD=(.*);RESP_STATUS=(.*);$");
        Pattern p_uri = Pattern.compile("/kylin/api/(\\w+)(/.*/)*(.*)$");
        Matcher m_available = p_available.matcher("");
        Matcher m_request = p_request.matcher("");
        Matcher m_uri = p_uri.matcher("");

        Path path = Paths.get(filePath);
        try {
            BufferedReader reader = Files.newBufferedReader(path, ENCODING);
            String line = null;
            while ((line = reader.readLine()) != null) {
                //reset the input
                m_available.reset(line);
                m_request.reset(line);

                //filter unnecessary info
                if (m_available.find()) {
                    //filter GET info
                    if (m_request.find() && !m_request.group(5).equals("GET")) {

                        List<String> groups = new ArrayList<String>();
                        for (int i = 1; i <= m_request.groupCount(); i++) {
                            groups.add(m_request.group(i));
                        }

                        String uri = m_request.group(4);
                        m_uri.reset(uri);
                        if (m_uri.find()) {

                            //add target
                            groups.add(m_uri.group(1));

                            //add action
                            if (m_uri.group(1).equals("cubes")) {
                                switch (m_request.group(5)) {
                                    case "DELETE":
                                        groups.add("drop");
                                        break;
                                    case "POST":
                                        groups.add("save");
                                        break;
                                    default:
                                        //add parse action
                                        groups.add(m_uri.group(3));
                                        break;
                                }
                            }

                        }
                        groups.add(DEPLOY_ENV);
                        String[] recordArray = groups.toArray(new String[groups.size()]);
                        //write to hdfs
                        cwriter.writeNext(recordArray);
                    }
                }


            }
        } catch (IOException ex) {
            logger.info("Failed to write to hdfs:", ex);
        } finally {
            writer.close();
            cwriter.close();
            fs.close();
        }

        logger.info("Finish parsing file " + filePath + " !");
    }

    public void writeResultToHdfs(String dPath, String[] record) throws IOException {
        OutputStreamWriter writer = null;
        CSVWriter cwriter = null;
        FileSystem fs = null;
        try {

            fs = this.getHdfsFileSystem();
            org.apache.hadoop.fs.Path resultStorePath = new org.apache.hadoop.fs.Path(dPath);
            writer = new OutputStreamWriter(fs.append(resultStorePath));
            cwriter = new CSVWriter(writer,'|',CSVWriter.NO_QUOTE_CHARACTER);
            cwriter.writeNext(record);

        } catch (IOException e) {
            logger.info("Exception", e);
        } finally {
            writer.close();
            cwriter.close();
            fs.close();
        }
    }

    public List<File> getRequestLogFiles() {
        List<File> logFiles = new ArrayList<File>();

//        String request_log_file_pattern = monitorConfig.getRequestLogFilePattern();

        List<String> request_log_dir_list = monitorConfig.getLogBaseDir();
        FileFilter filter = new RegexFileFilter(REQUEST_LOG_FILE_PATTERN);

        for (String path : request_log_dir_list) {
            logger.info("fetch api request log file from path:" + path);
            File request_log_dir = new File(path);
            File[] request_log_files = request_log_dir.listFiles(filter);
            if (request_log_files == null) {
                logger.warn("no api request log file found under path" + path);
                break;
            }
            Collections.addAll(logFiles, request_log_files);
        }

        return logFiles;
    }

    public FileSystem getHdfsFileSystem() throws IOException {
        Configuration conf = new Configuration();
//        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            fs.close();
            logger.info("Failed to get hdfs FileSystem", e);
        }
        return fs;
    }

}
