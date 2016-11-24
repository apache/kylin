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
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.impl.threadpool.DefaultContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public abstract class AbstractExecutable implements Executable, Idempotent {

    protected static final String SUBMITTER = "submitter";
    protected static final String NOTIFY_LIST = "notify_list";
    protected static final String START_TIME = "startTime";
    protected static final String END_TIME = "endTime";

    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);
    protected int retry = 0;

    private KylinConfig config;
    private String name;
    private String id;
    private Map<String, String> params = Maps.newHashMap();

    public AbstractExecutable() {
        setId(UUID.randomUUID().toString());
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

        logger.info("Executing AbstractExecutable (" + this.getName() + ")");

        Preconditions.checkArgument(executableContext instanceof DefaultContext);
        ExecuteResult result = null;
        try {
            onExecuteStart(executableContext);
            Throwable exception;
            do {
                if (retry > 0) {
                    logger.info("Retry " + retry);
                }
                exception = null;
                result = null;
                try {
                    result = doWork(executableContext);
                } catch (Throwable e) {
                    logger.error("error running Executable: " + this.toString());
                    exception = e;
                }
                retry++;
            } while (((result != null && result.succeed() == false) || exception != null) && needRetry() == true);

            if (exception != null) {
                onExecuteError(exception, executableContext);
                throw new ExecuteException(exception);
            }

            onExecuteFinished(result, executableContext);
        } catch (Exception e) {
            if (isMetaDataPersistException(e)) {
                handleMetaDataPersistException(e);
            }
            if (e instanceof ExecuteException) {
                throw e;
            } else {
                throw new ExecuteException(e);
            }
        }
        return result;
    }

    protected void handleMetaDataPersistException(Exception e) {
        // do nothing.
    }

    private boolean isMetaDataPersistException(Exception e) {
        if (e instanceof PersistentException) {
            return true;
        }

        Throwable t = e.getCause();
        int depth = 0;
        while (t != null && depth < 5) {
            depth++;
            if (t instanceof PersistentException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException;

    @Override
    public void cleanup() throws ExecuteException {

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
    public final ExecutableState getStatus() {
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
                logger.warn("no need to send email, user list is empty");
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
        logger.info("prepare to send email to:" + users);
        logger.info("job name:" + getName());
        logger.info("submitter:" + getSubmitter());
        logger.info("notify list:" + users);
        new MailService(kylinConfig).sendMail(users, email.getLeft(), email.getRight());
    }

    protected void sendMail(Pair<String, String> email) {
        try {
            List<String> users = getAllNofifyUsers(config);
            if (users.isEmpty()) {
                logger.warn("no need to send email, user list is empty");
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

    public static long getDuration(long startTime, long endTime) {
        if (startTime == 0) {
            return 0;
        }
        if (endTime == 0) {
            return System.currentTimeMillis() - startTime;
        } else {
            return endTime - startTime;
        }
    }

    public static long getExtraInfoAsLong(Output output, String key, long defaultValue) {
        final String str = output.getExtra().get(key);
        if (str != null) {
            return Long.parseLong(str);
        } else {
            return defaultValue;
        }
    }

    protected final void addExtraInfo(String key, String value) {
        getManager().addJobInfo(getId(), key, value);
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

    public final long getStartTime() {
        return getExtraInfoAsLong(START_TIME, 0L);
    }

    public final long getEndTime() {
        return getExtraInfoAsLong(END_TIME, 0L);
    }

    public final long getDuration() {
        return getDuration(getStartTime(), getEndTime());
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

    protected boolean needRetry() {
        return this.retry <= config.getJobRetry();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus()).toString();
    }
}
