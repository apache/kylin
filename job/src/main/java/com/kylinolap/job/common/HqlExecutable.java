package com.kylinolap.job.common;

import com.google.common.collect.Lists;
import com.kylinolap.common.util.HiveClient;
import com.kylinolap.job.dao.JobPO;
import com.kylinolap.job.exception.ExecuteException;
import com.kylinolap.job.execution.ExecutableContext;
import com.kylinolap.job.execution.ExecuteResult;
import com.kylinolap.job.impl.threadpool.AbstractExecutable;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * Created by qianzhou on 1/15/15.
 */
public class HqlExecutable extends AbstractExecutable {

    private static final String HQL = "hql";

    public HqlExecutable() {
    }

    public HqlExecutable(JobPO job) {
        super(job);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            HiveClient hiveClient = new HiveClient();
            for (String hql: getHqls()) {
                hiveClient.executeHQL(hql);
            }
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        } catch (Exception e) {
            logger.error("error run hive query:" + getHqls(), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    public void setHqls(List<String> hqls) {
        setParam(HQL, StringUtils.join(hqls, ","));
    }

    private List<String> getHqls() {
        final String hqls = getParam(HQL);
        if (hqls != null) {
            return Lists.newArrayList(StringUtils.split(hqls, ","));
        } else {
            return Collections.emptyList();
        }
    }

}
