package org.apache.kylin.rest.util;

import org.apache.kylin.rest.service.PerformanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jiazhong on 2015/4/13.
 */

public class PerformanceMonitorTask {

    @Autowired
    private PerformanceService performanceService;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    @Scheduled(fixedRate = 3600000)
    public void reportCurrentTime() {
        System.out.println("The time is now " + dateFormat.format(new Date()));
    }


    public void setPerformanceService(PerformanceService performanceService) {this.performanceService = performanceService;}
}
