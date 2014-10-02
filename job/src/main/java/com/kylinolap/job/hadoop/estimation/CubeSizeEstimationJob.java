package com.kylinolap.job.hadoop.estimation;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by honma on 9/1/14.
 */
public class CubeSizeEstimationJob extends AbstractHadoopJob {

    private int returnCode = 0;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_CUBE_NAME);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            KylinConfig config = KylinConfig.getInstanceFromEnv();

        } catch (Exception e) {
            printUsage(options);
            e.printStackTrace(System.err);
            log.error(e.getLocalizedMessage(), e);
            returnCode = 2;
        }

        return returnCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CubeSizeEstimationJob(), args);
        System.exit(exitCode);
    }

}
