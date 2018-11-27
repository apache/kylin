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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.impl.threadpool.DefaultContext;
import org.apache.kylin.job.util.MailNotificationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public abstract class AbstractExecutable implements Executable, Idempotent {

    public static final Integer DEFAULT_PRIORITY = 10;

    protected static final String SUBMITTER = "submitter";
    protected static final String NOTIFY_LIST = "notify_list";
    protected static final String START_TIME = "startTime";
    protected static final String END_TIME = "endTime";
    protected static final String INTERRUPT_TIME = "interruptTime";

    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);
    public static final String NO_NEED_TO_SEND_EMAIL_USER_LIST_IS_EMPTY = "no need to send email, user list is empty";
    protected int retry = 0;

    private KylinConfig config;
    private String name;
    private String id;
    private AbstractExecutable parentExecutable = null;
    private Map<String, String> params = Maps.newHashMap();

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
    public final ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException {

        logger.info("Executing AbstractExecutable ({})", this.getName());

        Preconditions.checkArgument(executableContext instanceof DefaultContext);
        ExecuteResult result = null;

        try {
            onExecuteStart(executableContext);
            Throwable catchedException;
            Throwable realException;
            do {
                if (retry > 0) {
                    logger.info("Retry {}", retry);
                }
                catchedException = null;
                result = null;
                try {
                    result = doWork(executableContext);
                } catch (Throwable e) {
                    logger.error("error running Executable: {}", this.toString());
                    catchedException = e;
                }
                retry++;
                realException = catchedException != null ? catchedException
                        : (result.getThrowable() != null ? result.getThrowable() : null);

                //don't invoke retry on ChainedExecutable
            } while (needRetry(this.retry, realException)); //exception in ExecuteResult should handle by user itself.

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

        String content = MailNotificationUtil.getMailContent(MailNotificationUtil.METADATA_PERSIST_FAIL, dataMap);
        String title = MailNotificationUtil.getMailTitle("METADATA PERSIST", "FAIL",
                context.getConfig().getDeployEnv());

        new MailService(context.getConfig()).sendMail(users, title, content);
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException, PersistentException;

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
    public final String getId() {
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

    public final List<String> getNotifyList() {
        final String str = getParam(NOTIFY_LIST);
        if (str != null) {
            return Lists.newArrayList(StringUtils.split(str, ","));
        } else {
            return Collections.emptyList();
        }
    }

    public final void setNotifyList(String notifications) {
        setParam(NOTIFY_LIST, notifications);
    }

    public final void setNotifyList(List<String> notifications) {
        setNotifyList(StringUtils.join(notifications, ","));
    }

    protected Pair<String, String> formatNotifications(ExecutableContext executableContext, ExecutableState state) {
        return null;
    }

    protected final void notifyUserStatusChange(ExecutableContext context, ExecutableState state) {
        try {
            List<String> users = getAllNofifyUsers(config);
            if (users.isEmpty()) {
                logger.debug(NO_NEED_TO_SEND_EMAIL_USER_LIST_IS_EMPTY);
                return;
            }
            final Pair<String, String> email = formatNotifications(context, state);
            doSendMail(config, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }

    private List<String> getAllNofifyUsers(KylinConfig kylinConfig) {
        List<String> users = Lists.newArrayList();
        users.addAll(getNotifyList());
        final String[] adminDls = kylinConfig.getAdminDls();
        if (null != adminDls) {
            for (String adminDl : adminDls) {
                users.add(adminDl);
            }
        }
        return users;
    }

    private void doSendMail(KylinConfig kylinConfig, List<String> users, Pair<String, String> email) {
        if (email == null) {
            logger.warn("no need to send email, content is null");
            return;
        }
        logger.info("prepare to send email to:{}", users);
        logger.info("job name:{}", getName());
        logger.info("submitter:{}", getSubmitter());
        logger.info("notify list:{}", users);
        new MailService(kylinConfig).sendMail(users, email.getFirst(), email.getSecond());
    }

    protected void sendMail(Pair<String, String> email) {
        try {
            List<String> users = getAllNofifyUsers(config);
            if (users.isEmpty()) {
                logger.debug(NO_NEED_TO_SEND_EMAIL_USER_LIST_IS_EMPTY);
                return;
            }
            doSendMail(config, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }

    public final String getSubmitter() {
        return getParam(SUBMITTER);
    }

    @Override
    public final Output getOutput() {
        return getManager().getOutput(getId());
    }

    protected long getExtraInfoAsLong(String key, long defaultValue) {
        return getExtraInfoAsLong(getOutput(), key, defaultValue);
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

    private static boolean isRetryableException(String exceptionName) {
        String[] jobRetryExceptions = KylinConfig.getInstanceFromEnv().getJobRetryExceptions();
        return ArrayUtils.isEmpty(jobRetryExceptions) || ArrayUtils.contains(jobRetryExceptions, exceptionName);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus())
                .toString();
    }
}
