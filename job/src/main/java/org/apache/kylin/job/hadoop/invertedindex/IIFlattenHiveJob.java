package org.apache.kylin.job.hadoop.invertedindex;

import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.cmd.ICommandOutput;
import org.apache.kylin.job.cmd.ShellCmd;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.job.hadoop.hive.IJoinedFlatTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.hadoop.hive.IIJoinedFlatTableDesc;

/**
 * Created by Hongbin Ma(Binmahone) on 12/30/14.
 */
public class IIFlattenHiveJob extends AbstractHadoopJob {

    protected static final Logger log = LoggerFactory.getLogger(InvertedIndexJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        try {
            options.addOption(OPTION_II_NAME);
            parseOptions(options, args);

            String iiname = getOptionValue(OPTION_II_NAME);
            KylinConfig config = KylinConfig.getInstanceFromEnv();

            IIInstance iiInstance = IIManager.getInstance(config).getII(iiname);
            IIDesc iidesc = IIDescManager.getInstance(config).getIIDesc(iiInstance.getDescName());

            String jobUUID = "00bf87b5-c7b5-4420-a12a-07f6b37b3187";
            JobEngineConfig engineConfig = new JobEngineConfig(config);
            IJoinedFlatTableDesc intermediateTableDesc = new IIJoinedFlatTableDesc(iidesc);
            String dropTableHql = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, jobUUID);
            String createTableHql = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, //
                    JobInstance.getJobWorkingDir(jobUUID, engineConfig.getHdfsWorkingDirectory()), jobUUID);
            String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, jobUUID, engineConfig);

            StringBuffer buf = new StringBuffer();
            buf.append("hive -e \"");
            buf.append(dropTableHql + "\n");
            buf.append(createTableHql + "\n");
            buf.append(insertDataHqls + "\n");
            buf.append("\"");
            
            System.out.println(buf.toString());
            System.out.println("========================");

            ShellCmd cmd = new ShellCmd(buf.toString(), null, null, null, false);
            ICommandOutput output = cmd.execute();
            System.out.println(output.getOutput());
            System.out.println(output.getExitCode());
            
            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        IIFlattenHiveJob job = new IIFlattenHiveJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
