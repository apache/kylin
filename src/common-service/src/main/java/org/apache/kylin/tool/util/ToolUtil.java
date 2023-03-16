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
package org.apache.kylin.tool.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.query.util.ExtractFactory;
import org.apache.spark.sql.SparderEnv;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ToolUtil {

    private ToolUtil() {
    }

    public static void dumpKylinJStack(File outputFile) throws IOException, ShellException {
        String jstackDumpCmd = String.format(Locale.ROOT, "jstack -l %s", getKylinPid());
        val result = new CliCommandExecutor().execute(jstackDumpCmd, null);
        FileUtils.writeStringToFile(outputFile, result.getCmd(), Charset.defaultCharset());
    }

    public static String getKylinPid() {
        File pidFile = new File(getKylinHome(), "pid");
        if (pidFile.exists()) {
            try {
                return FileUtils.readFileToString(pidFile, Charset.defaultCharset());
            } catch (IOException e) {
                throw new IllegalStateException("Error reading KYLIN PID file.", e);
            }
        }
        throw new IllegalStateException("Cannot find KYLIN PID file.");
    }

    public static String getKylinHome() {
        String path = System.getProperty(KylinConfig.KYLIN_CONF);
        if (StringUtils.isNotEmpty(path)) {
            return path;
        }
        path = KylinConfig.getKylinHome();
        if (StringUtils.isNotEmpty(path)) {
            return path;
        }
        throw new IllegalStateException("Cannot find KYLIN_HOME.");
    }

    public static String getBinFolder() {
        final String BIN = "bin";
        return getKylinHome() + File.separator + BIN;
    }

    public static String getLogFolder() {
        final String LOG = "logs";
        return getKylinHome() + File.separator + LOG;
    }

    public static String getConfFolder() {
        final String CONF = "conf";
        return getKylinHome() + File.separator + CONF;
    }

    public static String getHadoopConfFolder() {
        final String HADOOP_CONF = "hadoop_conf";
        return getKylinHome() + File.separator + HADOOP_CONF;
    }

    public static String getMetaStoreId() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ResourceStore store = ResourceStore.getKylinMetaStore(kylinConfig);
        return store.getMetaStoreUUID();
    }

    public static String getHostName() {
        String hostname = System.getenv("COMPUTERNAME");
        if (StringUtils.isEmpty(hostname)) {
            try {
                InetAddress address = InetAddress.getLocalHost();
                hostname = address.getHostName();
                if (StringUtils.isEmpty(hostname)) {
                    hostname = AddressUtil.getLocalHostExactAddress();
                }
            } catch (UnknownHostException uhe) {
                String host = uhe.getMessage(); // host = "hostname: hostname"
                if (host != null) {
                    int colon = host.indexOf(':');
                    if (colon > 0) {
                        return host.substring(0, colon);
                    }
                }
                hostname = "Unknown";
            }
        }
        return hostname;
    }

    private static String getHdfsPrefix() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        return kylinConfig.getHdfsWorkingDirectory();
    }

    public static String getSparderLogsDir() {
        final String SPARDER_LOG = "_sparder_logs";
        return getHdfsPrefix() + File.separator + SPARDER_LOG;
    }

    public static String getSparkLogsDir(String project) {
        Preconditions.checkArgument(!StringUtils.isBlank(project));

        final String SPARK_LOG = "spark_logs";
        return getHdfsPrefix() + File.separator + project + File.separator + SPARK_LOG;
    }

    public static String getJobTmpDir(String project, String jobId) {
        Preconditions.checkArgument(!StringUtils.isBlank(project) && !StringUtils.isBlank(jobId));

        final String JOB_TMP = "job_tmp";
        return getHdfsPrefix() + File.separator + project + File.separator + JOB_TMP + File.separator + jobId;
    }

    public static boolean waitForSparderRollUp() {
        val extractor = ExtractFactory.create();
        String check = SparderEnv.rollUpEventLog();
        if (StringUtils.isBlank(check)) {
            log.info("Failed to roll up eventLog because the spader is closed.");
            return false;
        }
        String logDir = extractor.getSparderEvenLogDir();
        ExecutorService es = Executors.newSingleThreadExecutor();
        FileSystem fs = HadoopUtil.getFileSystem(logDir);
        try {
            Future<Boolean> task = es.submit(() -> {
                while (true) {
                    if (fs.exists(new Path(logDir, check))) {
                        return true;
                    }
                    Thread.sleep(1000);
                }
            });
            if (Boolean.TRUE.equals(task.get(10, TimeUnit.SECONDS))) {
                fs.delete(new Path(logDir, check), false);
                return true;
            }
        } catch (InterruptedException e) {
            log.warn("Sparder eventLog rollUp failed.", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Sparder eventLog rollUp failed.", e);
        } finally {
            es.shutdown();
        }
        return false;
    }

    public static boolean isPortAvailable(String ip, int port) {
        boolean isAvailable;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ip, port), 1000);
            isAvailable = socket.isConnected();
        } catch (Exception e) {
            log.warn("Connect failed", e);
            isAvailable = false;
        }
        return isAvailable;
    }

    public static String getHdfsJobTmpDir(String project) {
        Preconditions.checkArgument(!StringUtils.isBlank(project));

        final String JOB_TMP = "job_tmp";
        return getHdfsPrefix() + File.separator + project + File.separator + JOB_TMP;
    }
}
