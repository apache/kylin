Advanced settings of Kylin environment
----

### A. Enabling LZO compression

LZO compression can be leveraged to compress the output of MR jobs, as well as hbase table storage, reducing the storage overhead. By default we do not enable LZO compression in Kylin because hadoop sandbox venders tend to not include LZO in their distributions due to license(GPL) issues.

To enable LZO in Kylin, there are three steps:

#### Make sure LZO is working in your environment

we have a simple tool to test whether LZO is well installed and configured in your environment(we only test it on the hadoop CLI that you deployed Kylin), Just run

`hbase org.apache.hadoop.util.RunJar kylin-job-latest.jar com.kylinolap.job.tools.LZOSupportnessChecker`

If the program prints "LZO supported by current env? true", you're good to go. Otherwise you'll need to first install LZO properly.

#### Modify kylin_job_conf.xml

You'll need to stop Kylin first by running `${KYLIN_HOME}/bin/kylin.sh stop`, and then modify /etc ${KYLIN_HOME}/conf/kylin_job_conf.xml by uncommenting some configuration entries related to LZO compression. 

#### export KYLIN_LD_LIBRARY_PATH to carry your native library paths

Before starting Kylin again, you'll need to uncomment KYLIN_LD_LIBRARY_PATH in ${KYLIN_HOME}/bin/setenv.sh to carry your LZO native library paths. Here's an example for hdp 2.1:

	export KYLIN_LD_LIBRARY_PATH=::/usr/lib/hadoop/lib/native/Linux-amd64-64:/usr/lib/hadoop/lib/native

After exporting, you need to run `${KYLIN_HOME}/bin/kylin.sh start` to start Kylin again. Now Kylin will use LZO to compress MR outputs and hbase tables.