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

package org.apache.kylin.job.execution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.notify.NotificationContext;
import org.apache.kylin.common.notify.NotificationTransmitter;
import org.apache.kylin.common.notify.util.MailNotificationUtil;
import org.apache.kylin.common.notify.util.NotificationConstants;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.impl.threadpool.DefaultContext;
import org.apache.kylin.job.impl.threadpool.IJobRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kylin.shaded.com.google.common.base.MoreObjects;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public abstract class AbstractExecutable implements Executable, Idempotent {

    public static final Integer DEFAULT_PRIORITY = 10;

    public static final String CUBE_NAME = "cubeName";
    protected static final String SUBMITTER = "submitter";
    protected static final String START_TIME = "startTime";
    protected static final String END_TIME = "endTime";
    protected static final String INTERRUPT_TIME = "interruptTime";
    protected static final String BUILD_INSTANCE = "buildInstance";
    protected static final String PROJECT_INSTANCE_NAME = "projectName";

    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);
    public static final String NO_NEED_TO_SEND_EMAIL_USER_LIST_IS_EMPTY = "no need to send email, user list is empty";
    protected int retry = 0;

    private KylinConfig config;
    private String name;
    private String id;
    private AbstractExecutable parentExecutable = null;
    private Map<String, String> params = Maps.newHashMap();
    protected Integer priority;

    public AbstractExecutable() {
        setId(RandomUtil.randomUUID().toString());
    }

    protected void initConfig(KylinConfig config) {
        Preconditions.checkState(this.config == null || this.config == config);
        this.config = config;
    }

    protected KylinConfig getConfig() {
        return config;
    }

    protected ExecutableManager getManager() {
        return ExecutableManager.getInstance(config);
    }

    protected void onExecuteStart(ExecutableContext executableContext) {
        Map<String, String> info = Maps.newHashMap();
        info.put(START_TIME, Long.toString(System.currentTimeMillis()));
        getManager().updateJobOutput(getId(), ExecutableState.RUNNING, info, null);
    }

    public KylinConfig getCubeSpecificConfig() {
        String cubeName = getCubeName();
        CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = manager.getCube(cubeName);
        return cube.getConfig();
    }

    private void onExecuteFinishedWithRetry(ExecuteResult result, ExecutableContext executableContext)
            throws ExecuteException {
        Throwable exception;
        int nRetry = 0;
        do {
            nRetry++;
            exception = null;
            try {
                onExecuteFinished(result, executableContext);
            } catch (Exception e) {
                logger.error(nRetry + "th retries for onExecuteFinished fails due to {}", e);
                if (isMetaDataPersistException(e, 5)) {
                    exception = e;
                    try {
                        Thread.sleep(1000L * (long) Math.pow(4, nRetry));
                    } catch (InterruptedException e1) {
                        throw new IllegalStateException(e1);
                    }
                } else {
                    throw e;
                }
            }
        } while (exception != null && nRetry <= executableContext.getConfig().getJobMetadataPersistRetry());

        if (exception != null) {
            handleMetadataPersistException(executableContext, exception);
            throw new ExecuteException(exception);
        }
    }

    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        setEndTime(System.currentTimeMillis());
        if (!isDiscarded() && !isRunnable()) {
            if (result.succeed()) {
                getManager().updateJobOutput(getId(), ExecutableState.SUCCEED, null, result.output());
            } else {
                getManager().updateJobOutput(getId(), ExecutableState.ERROR, null, result.output());
            }
        }
    }

    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        if (!isDiscarded()) {
            getManager().addJobInfo(getId(), END_TIME, Long.toString(System.currentTimeMillis()));
            String output = null;
            if (exception != null) {
                final StringWriter out = new StringWriter();
                exception.printStackTrace(new PrintWriter(out));
                output = out.toString();
            }
            getManager().updateJobOutput(getId(), ExecutableState.ERROR, null, output);
        }
    }

    @Override
    public final ExecuteResult execute(ExecutableContext executableContext, IJobRunner jobRunner) throws ExecuteException {

        logger.info("Executing AbstractExecutable ({})", this.getName());

        Preconditions.checkArgument(executableContext instanceof DefaultContext);
        ExecuteResult result = null;

        try {
            onExecuteStart(executableContext);
            Throwable catchedException;
            Throwable realException;
            do {
                if (retry > 0) {
                    pauseOnRetry();
                    logger.info("Begin to retry, retry time: {}", retry);
                }
                catchedException = null;
                result = null;
                try {
                    result = doWork(executableContext, jobRunner);
                } catch (Throwable e) {
                    logger.error("error running Executable: {}", this.toString());
                    catchedException = e;
                }
                retry++;
                realException = catchedException != null ? catchedException
                        : (result.getThrowable() != null ? result.getThrowable() : null);

                //don't invoke retry on ChainedExecutable
            } while (needRetry(this.retry, realException)); //exception in ExecuteResult should handle by user itself.

            // If after doWork finishes and the job lock is lost, it should do short circuit
            if (!jobRunner.acquireJobLock()) {
                logger.warn("fail to acquire lock for {} after finishing doWork", id);
                return ExecuteResult.createSucceed();
            }

            //check exception in result to avoid retry on ChainedExecutable(only need to retry on subtask actually)
            if (realException != null) {
                onExecuteError(realException, executableContext);
                throw new ExecuteException(realException);
            }

            onExecuteFinishedWithRetry(result, executableContext);
        } catch (ExecuteException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecuteException(e);
        }
        return result;
    }

    protected void handleMetadataPersistException(ExecutableContext context, Throwable exception) {
        final String[] adminDls = context.getConfig().getAdminDls();
        if (adminDls == null || adminDls.length < 1) {
            logger.warn(NO_NEED_TO_SEND_EMAIL_USER_LIST_IS_EMPTY);
            return;
        }
        List<String> users = Lists.newArrayList(adminDls);

        Map<String, Object> dataMap = Maps.newHashMap();
        dataMap.put("job_name", getName());
        dataMap.put("env_name", context.getConfig().getDeployEnv());
        dataMap.put(SUBMITTER, StringUtil.noBlank(getSubmitter(), "missing submitter"));
        dataMap.put("job_engine", MailNotificationUtil.getLocalHostName());
        dataMap.put("error_log",
                Matcher.quoteReplacement(StringUtil.noBlank(exception.getMessage(), "no error message")));

        Pair<String[], Map<String, Object>> mapPair = Pair.newPair(new String[]{
                "METADATA PERSIST",
                "FAIL",
                context.getConfig().getDeployEnv()
        }, dataMap);

        Map<String, List<String>> receivers = new HashMap<>();
        receivers.put(NotificationConstants.NOTIFY_EMAIL_LIST, users);
        new NotificationTransmitter(new NotificationContext(context.getConfig(), receivers, NotificationConstants.JOB_METADATA_PERSIST_FAIL, mapPair)).sendNotification();
    }

    protected abstract ExecuteResult doWork(ExecutableContext context, IJobRunner jobRunner) throws ExecuteException, PersistentException;

    @Override
    public void cleanup() throws ExecuteException {

    }

    public static boolean isMetaDataPersistException(Exception e, final int maxDepth) {
        if (e instanceof PersistentException) {
            return true;
        }

        Throwable t = e.getCause();
        int depth = 0;
        while (t != null && depth < maxDepth) {
            depth++;
            if (t instanceof PersistentException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    @Override
    public boolean isRunnable() {
        return this.getStatus() == ExecutableState.READY;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getId() {
        return this.id;
    }

    public final void setId(String id) {
        this.id = id;
    }

    @Override
    public ExecutableState getStatus() {
        ExecutableManager manager = getManager();
        return manager.getOutput(this.getId()).getState();
    }

    @Override
    public final Map<String, String> getParams() {
        return this.params;
    }

    public final String getParam(String key) {
        return this.params.get(key);
    }

    public final void setParam(String key, String value) {
        this.params.put(key, value);
    }

    public final void setParams(Map<String, String> params) {
        this.params.putAll(params);
    }

    public final long getLastModified() {
        return getOutput().getLastModified();
    }

    public final void setSubmitter(String submitter) {
        setParam(SUBMITTER, submitter);
    }

    public final List<String> getNotificationList(String key) {
        final String str = getParam(key);
        if (str != null) {
            return Lists.newArrayList(StringUtils.split(str, ","));
        } else {
            return Collections.emptyList();
        }
    }

    public final void setNotificationList(String key, String notifications) {
        setParam(key, notifications);
    }

    public final void setNotificationList(String key, List<String> notifications) {
        setNotificationList(key, StringUtils.join(notifications, ","));
    }

    protected Pair<String[], Map<String, Object>>  formatNotifications(ExecutableContext executableContext, ExecutableState state) {
        return null;
    }

    protected final void notifyUserStatusChange(ExecutableContext context, ExecutableState state) {
        try {
            Map<String, List<String>> users = getAllNofificationUsers(config);
            if (users.isEmpty()) {
                logger.debug(NO_NEED_TO_SEND_EMAIL_USER_LIST_IS_EMPTY);
                return;
            }
            final Pair<String[], Map<String, Object>> notification = formatNotifications(context, state);
            doSendNotification(new NotificationContext(config, users, state.name(), notification));
        } catch (Exception e) {
            logger.error("error send notification", e);
        }
    }

    private Map<String, List<String>> getAllNofificationUsers(KylinConfig kylinConfig) {
        Map<String, List<String>> users = Maps.newHashMap();
        users.put(NotificationConstants.NOTIFY_EMAIL_LIST, getNotificationList(NotificationConstants.NOTIFY_EMAIL_LIST));
        users.put(NotificationConstants.NOTIFY_DINGTALK_LIST, getNotificationList(NotificationConstants.NOTIFY_DINGTALK_LIST));
        final String[] adminDls = kylinConfig.getAdminDls();
        if (null != adminDls) {
            for (String adminDl : adminDls) {
                users.get(NotificationConstants.NOTIFY_EMAIL_LIST).add(adminDl);
            }
        }
        return users;
    }

    private void doSendNotification(NotificationContext notificationContext) {
        logger.info("prepare to send notify to:{}", notificationContext.getReceivers());
        logger.info("job name:{}", getName());
        logger.info("submitter:{}", getSubmitter());
        new NotificationTransmitter(notificationContext).sendNotification();
    }

    public final String getSubmitter() {
        return getParam(SUBMITTER);
    }

    public final String getCubeName() {
        return getParam(CUBE_NAME);
    }

    @Override
    public final Output getOutput() {
        return getManager().getOutput(getId());
    }

    protected long getExtraInfoAsLong(String key, long defaultValue) {
        return getExtraInfoAsLong(getOutput(), key, defaultValue);
    }

    public static String getBuildInstance(Output output) {
        final String str = output.getExtra().get(BUILD_INSTANCE);
        if (str != null) {
            return str;
        }
        return "unknown";
    }

    public static long getStartTime(Output output) {
        return getExtraInfoAsLong(output, START_TIME, 0L);
    }

    public static long getEndTime(Output output) {
        return getExtraInfoAsLong(output, END_TIME, 0L);
    }

    public static long getInterruptTime(Output output) {
        return getExtraInfoAsLong(output, INTERRUPT_TIME, 0L);
    }

    public static long getDuration(long startTime, long endTime, long interruptTime) {
        if (startTime == 0) {
            return 0;
        }
        if (endTime == 0) {
            return System.currentTimeMillis() - startTime - interruptTime;
        } else {
            return endTime - startTime - interruptTime;
        }
    }

    public AbstractExecutable getParentExecutable() {
        return parentExecutable;
    }
    public void setParentExecutable(AbstractExecutable parentExecutable) {
        this.parentExecutable = parentExecutable;
    }

    public static long getExtraInfoAsLong(Output output, String key, long defaultValue) {
        final String str = output.getExtra().get(key);
        if (str != null) {
            return Long.parseLong(str);
        } else {
            return defaultValue;
        }
    }

    public final void addExtraInfo(String key, String value) {
        getManager().addJobInfo(getId(), key, value);
    }

    public final String getExtraInfo(String key) {
        return getExtraInfo().get(key);
    }

    protected final Map<String, String> getExtraInfo() {
        return getOutput().getExtra();
    }

    public final void setStartTime(long time) {
        addExtraInfo(START_TIME, time + "");
    }

    public final void setEndTime(long time) {
        addExtraInfo(END_TIME, time + "");
    }

    public final void setInterruptTime(long time) {
        addExtraInfo(INTERRUPT_TIME, time + "");
    }

    public final long getStartTime() {
        return getExtraInfoAsLong(START_TIME, 0L);
    }

    public final long getEndTime() {
        return getExtraInfoAsLong(END_TIME, 0L);
    }

    public final long getInterruptTime() {
        return getExtraInfoAsLong(INTERRUPT_TIME, 0L);
    }

    public final long getDuration() {
        return getDuration(getStartTime(), getEndTime(), getInterruptTime());
    }

    public boolean isReady() {
        final Output output = getManager().getOutput(id);
        return output.getState() == ExecutableState.READY;
    }

    /**
     * The larger the value, the higher priority
     * */
    public int getDefaultPriority() {
        return DEFAULT_PRIORITY;
    }

    public Integer getPriority() {
        return priority == null ? getDefaultPriority() : priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    /**
     * The different jobs have different default priorities.
     * */
    public void setPriorityBasedOnPriorityOffset(Integer priorityOffset) {
        this.priority = getDefaultPriority() + (priorityOffset == null ? 0 : priorityOffset);
    }

    /*
    * discarded is triggered by JobService, the Scheduler is not awake of that
    *
    * */
    protected final boolean isDiscarded() {
        final ExecutableState status = getOutput().getState();
        return status == ExecutableState.DISCARDED;
    }

    protected final boolean isPaused() {
        final ExecutableState status = getOutput().getState();
        return status == ExecutableState.STOPPED;
    }

    // Retry will happen in below cases:
    // 1) if property "kylin.job.retry-exception-classes" is not set or is null, all jobs with exceptions will retry according to the retry times.
    // 2) if property "kylin.job.retry-exception-classes" is set and is not null, only jobs with the specified exceptions will retry according to the retry times.
    public boolean needRetry(int retry, Throwable t) {
        if (retry > KylinConfig.getInstanceFromEnv().getJobRetry() || t == null
                || (this instanceof DefaultChainedExecutable)) {
            return false;
        } else {
            return isRetryableException(t.getClass().getName());
        }
    }

    // pauseOnRetry should only works when retry has been triggered
    public void pauseOnRetry() {
        int interval = KylinConfig.getInstanceFromEnv().getJobRetryInterval();
        logger.info("Pause {} milliseconds before retry", interval);
        try {
            TimeUnit.MILLISECONDS.sleep(interval);
        } catch (InterruptedException e) {
            logger.error("Job retry was interrupted, details: {}", e);
            Thread.currentThread().interrupt();
        }
    }

    private static boolean isRetryableException(String exceptionName) {
        String[] jobRetryExceptions = KylinConfig.getInstanceFromEnv().getJobRetryExceptions();
        return ArrayUtils.isEmpty(jobRetryExceptions) || ArrayUtils.contains(jobRetryExceptions, exceptionName);
    }

    @Override
    public String toString() {
        ExecutableState state = null;
        try {
            state = getStatus();
        } catch (RuntimeException e) {
            logger.error("failed to get job status:" + getId(), e);
        }
        return MoreObjects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", state)
                .toString();
    }

    public String getProjectName() {
        return getParam(PROJECT_INSTANCE_NAME);
    }

    public void setProjectName(String name) {
        setParam(PROJECT_INSTANCE_NAME, name);
    }
}
