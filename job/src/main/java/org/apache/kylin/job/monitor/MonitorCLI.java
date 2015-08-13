package org.apache.kylin.job.monitor;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 */
public class MonitorCLI {

    private static final Logger logger = LoggerFactory.getLogger(MonitorCLI.class);

    public static void main(String[] args) {
        Preconditions.checkArgument(args[0].equals("monitor"));

        int i = 1;
        List<String> receivers = null;
        String host = null;
        String tableName = null;
        String authorization = null;
        String cubeName = null;
        String projectName = "default";
        while (i < args.length) {
            String argName = args[i];
            switch (argName) {
            case "-receivers":
                receivers = Lists.newArrayList(StringUtils.split(args[++i], ";"));
                break;
            case "-host":
                host = args[++i];
                break;
            case "-tableName":
                tableName = args[++i];
                break;
            case "-authorization":
                authorization = args[++i];
                break;
            case "-cubeName":
                cubeName = args[++i];
                break;
            case "-projectName":
                projectName = args[++i];
                break;
            default:
                throw new RuntimeException("invalid argName:" + argName);
            }
            i++;
        }
        Preconditions.checkArgument(receivers != null && receivers.size() > 0);
        final StreamingMonitor streamingMonitor = new StreamingMonitor();
        if (tableName != null) {
            logger.info(String.format("check query tableName:%s host:%s receivers:%s", tableName, host, StringUtils.join(receivers, ";")));
            Preconditions.checkNotNull(host);
            Preconditions.checkNotNull(authorization);
            Preconditions.checkNotNull(tableName);
            streamingMonitor.checkCountAll(receivers, host, authorization, projectName, tableName);
        }
        if (cubeName != null) {
            logger.info(String.format("check cube cubeName:%s receivers:%s", cubeName, StringUtils.join(receivers, ";")));
            streamingMonitor.checkCube(receivers, cubeName);
        }
        System.exit(0);
    }
}
