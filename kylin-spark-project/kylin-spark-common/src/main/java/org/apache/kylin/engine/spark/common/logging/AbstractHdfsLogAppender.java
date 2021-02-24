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

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public abstract class AbstractHdfsLogAppender extends AppenderSkeleton {
    private final Object flushLogLock = new Object();
    private final Object initWriterLock = new Object();
    private final Object closeLock = new Object();
    private final Object fileSystemLock = new Object();

    private FSDataOutputStream outStream = null;
    private BufferedWriter bufferedWriter = null;

    private FileSystem fileSystem = null;

    private ExecutorService appendHdfsService = null;

    private BlockingDeque<LoggingEvent> logBufferQue = null;
    private static final double QUEUE_FLUSH_THRESHOLD = 0.2;

    //configurable
    private int logQueueCapacity = 8192;
    private int flushInterval = 5000;
    private String hdfsWorkingDir;

    public int getLogQueueCapacity() {
        return logQueueCapacity;
    }

    public void setLogQueueCapacity(int logQueueCapacity) {
        this.logQueueCapacity = logQueueCapacity;
    }

    public BlockingDeque<LoggingEvent> getLogBufferQue() {
        return logBufferQue;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public String getHdfsWorkingDir() {
        return hdfsWorkingDir;
    }

    public void setHdfsWorkingDir(String hdfsWorkingDir) {
        this.hdfsWorkingDir = hdfsWorkingDir;
    }

    public FileSystem getFileSystem() {
        if (null == fileSystem) {
            return getFileSystem(new Configuration());
        }
        return fileSystem;
    }

    private FileSystem getFileSystem(Configuration conf) {
        synchronized (fileSystemLock) {
            if (null == fileSystem) {
                try {
                    fileSystem = new Path(hdfsWorkingDir).getFileSystem(conf);
                } catch (IOException e) {
                    LogLog.error("Failed to create the file system, ", e);
                    throw new RuntimeException(e);
                }
            }
        }
        return fileSystem;
    }

    public boolean isWriterInited() {
        synchronized (initWriterLock) {
            return null != bufferedWriter;
        }
    }

    abstract void init();

    abstract String getAppenderName();

    /**
     * init the load resource.
     */
    @Override
    public void activateOptions() {
        LogLog.warn(String.format(Locale.ROOT, "%s starting ...", getAppenderName()));
        LogLog.warn("hdfsWorkingDir -> " + getHdfsWorkingDir());

        init();

        logBufferQue = new LinkedBlockingDeque<>(getLogQueueCapacity());
        appendHdfsService = Executors.newSingleThreadExecutor();
        appendHdfsService.execute(this::checkAndFlushLog);
        Runtime.getRuntime().addShutdownHook(new Thread(this::closing));

        LogLog.warn(String.format(Locale.ROOT, "%s started ...", getAppenderName()));
    }

    @Override
    public void append(LoggingEvent loggingEvent) {
        try {
            boolean offered = logBufferQue.offer(loggingEvent, 10, TimeUnit.SECONDS);
            if (!offered) {
                LogLog.error("LogEvent cannot put into the logBufferQue, log event content:");
                printLoggingEvent(loggingEvent);
            }
        } catch (InterruptedException e) {
            LogLog.warn("Append logging event interrupted!", e);
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
    }

    /**
     * flush log when shutdowning
     */
    public void closing() {
        LogLog.warn(String.format(Locale.ROOT, "%s flush log when shutdown ...",
                getAppenderName()));
        synchronized (closeLock) {
            if (!this.closed) {
                List<LoggingEvent> transaction = Lists.newArrayList();
                try {
                    flushLog(getLogBufferQue().size(), transaction);
                } catch (Exception e) {
                    transaction.forEach(this::printLoggingEvent);
                    try {
                        while (!getLogBufferQue().isEmpty()) {
                            printLoggingEvent(getLogBufferQue().take());
                        }
                    } catch (Exception ie) {
                        LogLog.error("clear the logging buffer queue failed!", ie);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        LogLog.warn(String.format(Locale.ROOT, "%s attempt to closing ...",
                getAppenderName()));
        synchronized (closeLock) {
            if (!this.closed) {
                this.closed = true;
                List<LoggingEvent> transaction = Lists.newArrayList();
                try {
                    flushLog(getLogBufferQue().size(), transaction);
                    if (appendHdfsService != null && !appendHdfsService.isShutdown()) {
                        appendHdfsService.shutdownNow();
                    }
                    closeWriter();
                } catch (Exception e) {
                    transaction.forEach(this::printLoggingEvent);
                    try {
                        while (!getLogBufferQue().isEmpty()) {
                            printLoggingEvent(getLogBufferQue().take());
                        }
                    } catch (Exception ie) {
                        LogLog.error("clear the logging buffer queue failed!", ie);
                    }
                    LogLog.error(String.format(Locale.ROOT, "close %s failed!", getAppenderName()), e);
                }
                LogLog.warn(String.format(Locale.ROOT, "%s closed ...", getAppenderName()));
            }
        }
    }

    private void closeWriter() {
        IOUtils.closeQuietly(bufferedWriter);
        IOUtils.closeQuietly(outStream);
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    /**
     * some times need to wait the component init ok.
     *
     * @return
     */
    abstract boolean isSkipCheckAndFlushLog();

    /**
     * clear the log buffer queue when it was full.
     */
    private void clearLogBufferQueueWhenBlocked() {
        if (logBufferQue.size() >= getLogQueueCapacity()) {
            int removeNum = getLogQueueCapacity() / 5;
            while (removeNum > 0) {
                try {
                    LoggingEvent loggingEvent = logBufferQue.take();
                    printLoggingEvent(loggingEvent);
                } catch (Exception ex) {
                    LogLog.error("Take event interrupted!", ex);
                }
                removeNum--;
            }
        }
    }

    /**
     * print the logging event to stderr
     * @param loggingEvent
     */
    private void printLoggingEvent(LoggingEvent loggingEvent) {
        try {
            String log = getLayout().format(loggingEvent);
            LogLog.error(log.endsWith("\n") ? log.substring(0, log.length() - 1) : log);
            if (null != loggingEvent.getThrowableStrRep()) {
                for (String stack : loggingEvent.getThrowableStrRep()) {
                    LogLog.error(stack);
                }
            }
        } catch (Exception e) {
            LogLog.error("print logging event failed!", e);
        }
    }

    /**
     * flush the log to hdfs when conditions are satisfied.
     */
    protected void checkAndFlushLog() {
        long start = System.currentTimeMillis();
        do {
            List<LoggingEvent> transaction = Lists.newArrayList();
            try {
                if (isSkipCheckAndFlushLog()) {
                    continue;
                }

                int eventSize = getLogBufferQue().size();
                if (eventSize > getLogQueueCapacity() * QUEUE_FLUSH_THRESHOLD
                        || System.currentTimeMillis() - start > getFlushInterval()) {
                    // update start time before doing flushLog to avoid exception when flushLog
                    start = System.currentTimeMillis();
                    flushLog(eventSize, transaction);
                } else {
                    Thread.sleep(getFlushInterval() / 100);
                }
            } catch (Exception e) {
                transaction.forEach(this::printLoggingEvent);
                clearLogBufferQueueWhenBlocked();
                LogLog.error("Error occurred when consume event", e);
            }
        } while (!closed);
    }

    /**
     * init the hdfs writer and create the hdfs file with outPath.
     * need kerberos authentic, so fileSystem init here.
     *
     * @param outPath
     */
    protected boolean initHdfsWriter(Path outPath, Configuration conf) {
        synchronized (initWriterLock) {
            closeWriter();
            bufferedWriter = null;
            outStream = null;

            int retry = 10;
            while (retry-- > 0) {
                try {
                    fileSystem = getFileSystem(conf);
                    outStream = fileSystem.create(outPath, true);
                    break;
                } catch (Exception e) {
                    LogLog.error("fail to create stream for path: " + outPath, e);
                }

                try {
                    initWriterLock.wait(1000);//waiting for acl to turn to current user
                } catch (InterruptedException e) {
                    LogLog.warn("Init writer interrupted!", e);
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                }
            }
            if (null != outStream) {
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));
                return true;
            }
        }

        return false;
    }

    /**
     * write the data into the buffer.
     *
     * @param message
     * @throws IOException
     */
    protected void write(String message) throws IOException {
        if (isWriterInited()) {
            bufferedWriter.write(message);
        }
    }

    /**
     * write the error stack info into buffer
     *
     * @param loggingEvent
     * @throws IOException
     */
    protected void writeLogEvent(LoggingEvent loggingEvent) throws IOException {
        if (null != loggingEvent) {
            write(getLayout().format(loggingEvent));

            if (null != loggingEvent.getThrowableStrRep()) {
                for (String message : loggingEvent.getThrowableStrRep()) {
                    write(message);
                    write("\n");
                }
            }
        }
    }

    /**
     * do write log to the buffer.
     *
     * @param eventSize
     * @throws IOException
     * @throws InterruptedException
     */
    abstract void doWriteLog(int eventSize, List<LoggingEvent> transaction) throws IOException, InterruptedException;

    /**
     * flush the buffer data to HDFS.
     *
     * @throws IOException
     */
    private void flush() throws IOException {
        if (isWriterInited()) {
            bufferedWriter.flush();
            outStream.hsync();
        }
    }

    /**
     * take the all events from queue and write into the HDFS immediately.
     *
     * @param eventSize
     * @throws IOException
     * @throws InterruptedException
     */
    protected void flushLog(int eventSize, List<LoggingEvent> transaction) throws IOException, InterruptedException {
        if (eventSize <= 0) {
            return;
        }

        synchronized (flushLogLock) {
            if (eventSize > getLogBufferQue().size()) {
                eventSize = getLogBufferQue().size();
            }

            doWriteLog(eventSize, transaction);

            flush();
        }
    }
}
