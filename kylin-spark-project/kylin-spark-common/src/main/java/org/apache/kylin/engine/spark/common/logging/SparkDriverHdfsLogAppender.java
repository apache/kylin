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

package org.apache.kylin.engine.spark.common.logging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.util.List;

public class SparkDriverHdfsLogAppender extends AbstractHdfsLogAppender {

    private String logPath;

    // kerberos
    private boolean kerberosEnable = false;
    private String kerberosPrincipal;
    private String kerberosKeytab;

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }

    public boolean isKerberosEnable() {
        return kerberosEnable;
    }

    public void setKerberosEnable(boolean kerberosEnable) {
        this.kerberosEnable = kerberosEnable;
    }

    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public void setKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
    }

    public String getKerberosKeytab() {
        return kerberosKeytab;
    }

    public void setKerberosKeytab(String kerberosKeytab) {
        this.kerberosKeytab = kerberosKeytab;
    }

    @Override
    public void init() {
        LogLog.warn("spark.driver.log4j.appender.hdfs.File -> " + getLogPath());
        LogLog.warn("kerberosEnable -> " + isKerberosEnable());
        if (isKerberosEnable()) {
            LogLog.warn("kerberosPrincipal -> " + getKerberosPrincipal());
            LogLog.warn("kerberosKeytab -> " + getKerberosKeytab());
        }
    }

    @Override
    String getAppenderName() {
        return "SparkDriverHdfsLogAppender";
    }

    @Override
    public boolean isSkipCheckAndFlushLog() {
        return false;
    }

    @Override
    public void doWriteLog(int eventSize, List<LoggingEvent> transaction)
            throws IOException, InterruptedException {
        if (!isWriterInited()) {
            Configuration conf = new Configuration();
            if (isKerberosEnable()) {
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(getKerberosPrincipal(), getKerberosKeytab());
            }
            if (!initHdfsWriter(new Path(getLogPath()), conf)) {
                LogLog.error("init the hdfs writer failed!");
            }
        }

        while (eventSize > 0) {
            LoggingEvent loggingEvent = getLogBufferQue().take();
            transaction.add(loggingEvent);
            writeLogEvent(loggingEvent);
            eventSize--;
        }
    }
}
