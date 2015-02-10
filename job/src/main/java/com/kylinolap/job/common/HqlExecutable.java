package com.kylinolap.job.common;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.datanucleus.store.types.backed.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.kylinolap.common.util.HiveClient;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.job.exception.ExecuteException;
import com.kylinolap.job.execution.AbstractExecutable;
import com.kylinolap.job.execution.ExecutableContext;
import com.kylinolap.job.execution.ExecuteResult;

/**
 * Created by qianzhou on 1/15/15.
 */
public class HqlExecutable extends AbstractExecutable {

    private static final String HQL = "hql";
    private static final String HIVE_CONFIG = "hive-config";

    public HqlExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Map<String, String> configMap = getConfiguration();
            HiveClient hiveClient = new HiveClient(configMap);
            
            for (String hql: getHqls()) {
                hiveClient.executeHQL(hql);
            }
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        } catch (Exception e) {
            logger.error("error run hive query:" + getHqls(), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }
    
    public void setConfiguration(Map<String, String> configMap) {
        if(configMap != null) {
            String configStr = "";
            try {
                configStr = JsonUtil.writeValueAsString(configMap);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            setParam(HIVE_CONFIG, configStr);
        }
    }


    @SuppressWarnings("unchecked")
    private Map<String, String> getConfiguration() {
        String configStr = getParam(HIVE_CONFIG);
        Map<String, String> result = null;
        if(configStr != null) {
            try {
                result = JsonUtil.readValue(configStr, HashMap.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        return result;
    }
    
    public void setHqls(List<String> hqls) {
        setParam(HQL, StringUtils.join(hqls, ";"));
    }

    private List<String> getHqls() {
        final String hqls = getParam(HQL);
        if (hqls != null) {
            return Lists.newArrayList(StringUtils.split(hqls, ";"));
        } else {
            return Collections.emptyList();
        }
    }

}
