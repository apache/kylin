package com.kylinolap.job.hadoop.invertedindex;

import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.invertedindex.IIDescManager;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JoinedFlatTable;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.hadoop.hive.IIJoinedFlatTableDesc;
import com.kylinolap.job.hadoop.hive.IJoinedFlatTableDesc;

/**
 * Created by Hongbin Ma(Binmahone) on 12/30/14.
 */
public class IIFlatenHiveJob extends AbstractHadoopJob {

    protected static final Logger log = LoggerFactory.getLogger(InvertedIndexJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        try {
            options.addOption(OPTION_II_NAME);
            parseOptions(options, args);

            String iiname = getOptionValue(OPTION_II_NAME);
            KylinConfig config = KylinConfig.getInstanceFromEnv();

            IIDesc iidesc = IIDescManager.getInstance(config).getIIDesc(iiname);

            String jobUUID = "00bf87b5-c7b5-4420-a12a-07f6b37b3187";
            JobEngineConfig engineConfig = new JobEngineConfig(config);
            IJoinedFlatTableDesc intermediateTableDesc = new IIJoinedFlatTableDesc(iidesc);
            String dropTableHql = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, jobUUID);
            String createTableHql = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, //
                    JobInstance.getJobWorkingDir(jobUUID, engineConfig.getHdfsWorkingDirectory()), jobUUID);
            String insertDataHql = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, jobUUID, engineConfig);

            StringBuffer buf = new StringBuffer();
            buf.append("hive -e \"");
            buf.append(dropTableHql + "\n");
            buf.append(createTableHql + "\n");
            buf.append(insertDataHql + "\n");
            buf.append("\"");

            System.out.println(buf.toString());

            return 0;
        } catch (Exception e) {
            printUsage(options);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }
    }

    public static void main(String[] args) throws Exception {
        IIFlatenHiveJob job = new IIFlatenHiveJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
