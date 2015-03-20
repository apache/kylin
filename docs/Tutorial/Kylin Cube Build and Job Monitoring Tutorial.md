Kylin Cube Build and Job Monitoring Tutorial
===

### Cube Build
First of all, make sure that you have authority of the cube you want to build.

1. In `Cubes` page, click the `Action` drop down button in the right of a cube column and select operation `Build`.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/1%20action-build.png)

2. There is a pop-up window after the selection. 

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/2%20pop-up.png)

3. Click `END DATE` input box to choose end date of this incremental cube build.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/3%20end-date.png)

4. Click `Submit` to send request. 

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/4%20submit.png)

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/4.1%20success.png)

   After submit the request successfully, you will see the job just be created in the `Jobs` page.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/5%20jobs-page.png)

5. To discard this job, just click the `Discard` button.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/6%20discard.png)

### Job Monitoring
In the `Jobs` page, click the job detail button to see detail information show in the right side.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/7%20job-steps.png)

The detail information of a job provides a step-by-step record to trace a job. You can hover a step status icon to see the basic status and information.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/8%20hover-step.png)

Click the icon button show in each step to see the details: `Parameters`, `Log`, `MRJob`, `EagleMonitoring`.

* Parameters

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/9%20parameters.png)

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/9%20parameters-d.png)

* Log
        
   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/9%20log.png)

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/9%20log-d.png)

* MRJob(MapReduce Job)

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/9%20mrjob.png)

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/9%20mrjob-d.png)
   
#### What's next

After cube being built, you might need to:

1. [Query the cube via web interface](Kylin Web Tutorial.md)
2. [Query the cube via ODBC](Kylin ODBC Driver Tutorial.md)
3. [Grant permission to cubes](Kylin Cube Permission Grant Tutorial.md)