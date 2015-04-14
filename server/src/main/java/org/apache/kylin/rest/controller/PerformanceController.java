package org.apache.kylin.rest.controller;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jiazhong on 2015/4/13.
 */

@Controller
@RequestMapping(value = "/performance")
public class PerformanceController extends BasicController {

}
