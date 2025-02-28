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
package org.apache.kylin.spark.common.logging;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.spark.utils.SparkHadoopUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

public abstract class AbstractHdfsLogAppender
        extends AbstractOutputStreamAppender<AbstractHdfsLogAppender.HdfsManager> {

    public static final long ROLLING_BYTE_SIZE_DEFAULT = 524_288_000L;
    private static final double QUEUE_FLUSH_THRESHOLD = 0.2;
    private final Object flushLogLock = new Object();
    private final Object initWriterLock = new Object();
    private final Object closeLock = new Object();
    private final Object fileSystemLock = new Object();
    private volatile FSDataOutputStream outStream = null;
    private volatile FileSystem fileSystem = null;
    private ExecutorService appendHdfsService = null;
    @Getter
    private BlockingDeque<LogEvent> logBufferQue = null;
    //configurable
    @Getter
    @Setter
    private int logQueueCapacity = 8192;
    @Getter
    @Setter
    private int flushInterval = 5000;

    @Getter
    @Setter
    private String workingDir;

    private AtomicBoolean finished = new AtomicBoolean(false);
    private long start = System.currentTimeMillis();

    protected AbstractHdfsLogAppender(String name, Layout<? extends Serializable> layout, Filter filter,
            boolean ignoreExceptions, boolean immediateFlush, Property[] properties, HdfsManager manager) {
        super(name, layout, filter, ignoreExceptions, immediateFlush, properties, manager);
        StatusLogger.getLogger().warn("{} starting ...", getAppenderName());
        StatusLogger.getLogger().warn("hdfsWorkingDir -> {}", getWorkingDir());

        init();

        logBufferQue = new LinkedBlockingDeque<>(getLogQueueCapacity());
        final ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("logger-thread-%d")
                .build();
        appendHdfsService = Executors.newSingleThreadExecutor(factory);
        appendHdfsService.submit(this::checkAndFlushLog);

        ShutdownHookManager.get().addShutdownHook(() -> {
            stop();
            closeWriter();
        }, FileSystem.SHUTDOWN_HOOK_PRIORITY * 2);
        StatusLogger.getLogger().warn("{} started ...", getAppenderName());
    }

    public FileSystem getFileSystem() {
        if (null == fileSystem) {
            return getFileSystem(SparkHadoopUtils.newConfiguration());
        }
        return fileSystem;
    }

    private FileSystem getFileSystem(Configuration conf) {
        synchronized (fileSystemLock) {
            if (null == fileSystem) {
                try {
                    if (Objects.isNull(workingDir) || workingDir.isEmpty()) {
                        workingDir = System.getProperty("kylin.hdfs.working.dir");
                        StatusLogger.getLogger().warn("hdfsWorkingDir -> " + getWorkingDir());
                    }

                    // Use FileSystem.newInstance(...) to prevent contamination of global file system cache
                    fileSystem = FileSystem.newInstance(new Path(workingDir).toUri(), conf);

                } catch (IOException e) {
                    StatusLogger.getLogger().error("Failed to create the file system, ", e);
                    throw new KylinRuntimeException("Failed to create the file system, ", e);
                }
            }
        }
        return fileSystem;
    }

    public boolean isWriterInited() {
        synchronized (initWriterLock) {
            return null != outStream;
        }
    }

    abstract void init();

    abstract String getAppenderName();

    /**
     * init the load resource.
     */

    @Override
    public void append(LogEvent loggingEvent) {
        try {
            boolean offered = logBufferQue.offer(loggingEvent, 10, TimeUnit.SECONDS);
            if (!offered) {
                StatusLogger.getLogger().error("LogEvent cannot put into the logBufferQue, log event content:");
                printLoggingEvent(loggingEvent);
            }
        } catch (InterruptedException e) {
            StatusLogger.getLogger().warn("Append logging event interrupted!", e);
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop() {
        synchronized (closeLock) {
            if (!this.isStopped()) {
                finished.set(true);

                try {
                    if (appendHdfsService != null && !appendHdfsService.isShutdown()) {
                        ExecutorServiceUtil.shutdownGracefully(appendHdfsService, 60);
                    }
                } catch (Exception e) {
                    try {
                        while (!getLogBufferQue().isEmpty()) {
                            printLoggingEvent(getLogBufferQue().take());
                        }
                    } catch (Exception ie) {
                        StatusLogger.getLogger().error("clear the logging buffer queue failed!", ie);
                    }
                    StatusLogger.getLogger().error(String.format(Locale.ROOT, "close %s failed!", getAppenderName()),
                            e);
                }
                StatusLogger.getLogger().warn("{} closed ...", getAppenderName());
            }
        }
    }

    private void closeWriter() {
        getManager().close();
        outStream = null;
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
                    LogEvent loggingEvent = logBufferQue.take();
                    printLoggingEvent(loggingEvent);
                } catch (Exception ex) {
                    StatusLogger.getLogger().error("Take event interrupted!", ex);
                }
                removeNum--;
            }
        }
    }

    /**
     * print the logging event to stderr
     *
     * @param loggingEvent
     */
    private void printLoggingEvent(LogEvent loggingEvent) {
        try {
            getLayout().encode(loggingEvent, getManager());
            if (null != loggingEvent.getThrown()) {
                for (val stack : loggingEvent.getThrown().getStackTrace()) {
                    StatusLogger.getLogger().error(stack);
                }
            }
        } catch (Exception e) {
            StatusLogger.getLogger().error("print logging event failed!", e);
        }
    }

    /**
     * flush the log to hdfs when conditions are satisfied.
     */
    protected void checkAndFlushLog() {
        do {
            List<LogEvent> transaction = Lists.newArrayList();
            try {
                if (isSkipCheckAndFlushLog()) {
                    continue;
                }

                int eventSize = getLogBufferQue().size();

                //finished to flush
                if (finished.get()) {
                    if (eventSize > 0) {
                        flushLog(eventSize, transaction);
                    }
                    break;
                }

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
                StatusLogger.getLogger().error("Error occurred when consume event", e);
            }
        } while (!isStopped());
    }

    /**
     * init the hdfs writer and create the hdfs file with outPath.
     * need kerberos authentic, so fileSystem init here.
     */
    protected boolean initHdfsWriter(Path outPath, Configuration conf) {
        synchronized (initWriterLock) {
            StatusLogger.getLogger().warn("init hdfs writer...");
            closeWriter();

            int retry = 10;
            while (retry-- > 0) {
                try {
                    fileSystem = getFileSystem(conf);
                    outStream = fileSystem.exists(outPath) ? fileSystem.append(outPath, 8192)
                            : fileSystem.create(outPath, false);
                    break;
                } catch (Exception e) {
                    StatusLogger.getLogger().error("fail to create stream for path: " + outPath, e);
                }

                try {
                    initWriterLock.wait(1000);
                } catch (InterruptedException e) {
                    StatusLogger.getLogger().warn("Init writer interrupted!", e);
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                }
            }
            if (null != outStream) {
                getManager().setOutputStream(outStream);
                return true;
            }
        }

        return false;
    }

    /**
     * write the error stack info into buffer
     *
     * @param loggingEvent
     * @throws IOException
     */
    protected void writeLogEvent(LogEvent loggingEvent) {
        if (null != loggingEvent) {
            getLayout().encode(loggingEvent, getManager());
        }
    }

    /**
     * do write log to the buffer.
     *
     * @param eventSize
     * @throws IOException
     * @throws InterruptedException
     */
    abstract void doWriteLog(int eventSize, List<LogEvent> transaction) throws IOException, InterruptedException;

    /**
     * flush the buffer data to HDFS.
     *
     * @throws IOException
     */
    private void flush() {
        getManager().flush();
    }

    /**
     * take the all events from queue and write into the HDFS immediately.
     *
     * @param eventSize
     * @throws IOException
     * @throws InterruptedException
     */
    protected void flushLog(int eventSize, List<LogEvent> transaction) throws IOException, InterruptedException {
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

    protected boolean needRollingFile(String logPath, Long rollingMaxSize) throws IOException {
        Path pathProcess = new Path(logPath);
        FileSystem fs = getFileSystem();

        StatusLogger.getLogger().debug("log file path {}, {}", logPath, fs.exists(pathProcess));
        if (!fs.exists(pathProcess)) {
            return false;
        }
        ContentSummary contentSummary = fs.getContentSummary(pathProcess);
        long length = contentSummary.getLength();
        StatusLogger.getLogger().debug("log file length {}", length);

        return length > rollingMaxSize;

    }

    protected String updateOutPutPath(String logPath) throws IOException {
        synchronized (initWriterLock) {
            StatusLogger.getLogger().debug("start rolling log file {}", logPath);
            Path pathProcess = new Path(logPath);
            Path pathDone = new Path(getLogPathRollingDone(logPath));
            getFileSystem().rename(pathProcess, pathDone);
            String currentProcessPath = getLogPathAfterRolling(logPath);
            outStream = null;
            StatusLogger.getLogger().debug("end rolling log file {}", currentProcessPath);
            return currentProcessPath;
        }
    }

    abstract String getLogPathAfterRolling(String logPath);

    abstract String getLogPathRollingDone(String logPath);

    public static class HdfsManager extends OutputStreamManager {

        protected HdfsManager(String streamName, Layout<?> layout) {
            super(null, streamName, layout, false);
        }

        private OutputStream getOutputStreamQuietly() {
            try {
                return getOutputStream();
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        protected synchronized void flushDestination() {
            // access volatile field only once per method
            final OutputStream stream = getOutputStreamQuietly();
            if (stream != null) {
                try {
                    if (stream instanceof HdfsDataOutputStream) {
                        ((HdfsDataOutputStream) stream).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
                        return;
                    }
                    ((FSDataOutputStream) stream).hflush();
                } catch (final IOException ex) {
                    throw new AppenderLoggingException("Error flushing stream " + getName(), ex);
                }
            }
        }

        @Override
        protected synchronized boolean closeOutputStream() {
            super.flush();
            final OutputStream stream = getOutputStreamQuietly();
            if (stream == null || stream == System.out || stream == System.err) {
                return true;
            }
            try {
                ((FSDataOutputStream) stream).hsync();
                stream.close();
                LOGGER.debug("OutputStream closed");
            } catch (final IOException ex) {
                logError("Unable to close stream", ex);
                return false;
            }
            return true;
        }

        @Override
        public void setOutputStream(OutputStream os) {
            super.setOutputStream(os);
        }
    }
}
