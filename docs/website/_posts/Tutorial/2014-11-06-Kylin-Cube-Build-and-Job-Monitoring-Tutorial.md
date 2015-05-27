---
layout: post
title:  "Kylin Cube Build and Job Monitoring Tutorial"
date:   2014-11-06
author: Kejia-Wang
categories: tutorial
---

### Cube Build
First of all, make sure that you have authority of the cube you want to build.

1. In `Cubes` page, click the `Action` drop down button in the right of a cube column and select operation `Build`.

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/1 action-build.png)

2. There is a pop-up window after the selection. 

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/2 pop-up.png)

3. Click `END DATE` input box to choose end date of this incremental cube build.

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/3 end-date.png)

4. Click `Submit` to send request. 

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/4 submit.png)

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/4.1 success.png)

   After submit the request successfully, you will see the job just be created in the `Jobs` page.

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/5 jobs-page.png)

5. To discard this job, just click the `Discard` button.

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/6 discard.png)

### Job Monitoring
In the `Jobs` page, click the job detail button to see detail information show in the right side.

![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/7 job-steps.png)

The detail information of a job provides a step-by-step record to trace a job. You can hover a step status icon to see the basic status and information.

![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/8 hover-step.png)

Click the icon button show in each step to see the details: `Parameters`, `Log`, `MRJob`, `EagleMonitoring`.

* Parameters

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 parameters.png)

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 parameters-d.png)

* Log
        
   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 log.png)

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 log-d.png)

* MRJob(MapReduce Job)

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 mrjob.png)

   ![](/images/Kylin-Cube-Build-and-Job-Monitoring-Tutorial/9 mrjob-d.png)
