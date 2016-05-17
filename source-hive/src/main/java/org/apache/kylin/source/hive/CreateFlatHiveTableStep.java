package org.apache.kylin.source.hive;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import java.io.IOException;
import java.io.InputStream;

/**
 */
public class CreateFlatHiveTableStep extends AbstractExecutable {
    private final BufferedLogger stepLogger = new BufferedLogger(logger);

    private long readRowCountFromFile(Path file) throws IOException {
        FileSystem fs = FileSystem.get(file.toUri(), HadoopUtil.getCurrentConfiguration());
        InputStream in = fs.open(file);
        try {
            String content = IOUtils.toString(in);
            return Long.valueOf(content.trim()); // strip the '\n' character

        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    private int determineNumReducer(KylinConfig config) throws IOException {
        Path rowCountFile = new Path(getRowCountOutputDir(), "000000_0");
        long rowCount = readRowCountFromFile(rowCountFile);
        int mapperInputRows = config.getHadoopJobMapperInputRows();

        int numReducers = Math.round(rowCount / ((float) mapperInputRows));
        numReducers = Math.max(1, numReducers);

        stepLogger.log("total input rows = " + rowCount);
        stepLogger.log("expected input rows per mapper = " + mapperInputRows);
        stepLogger.log("reducers for RedistributeFlatHiveTableStep = " + numReducers);

        return numReducers;
    }

    private void createFlatHiveTable(KylinConfig config, int numReducers) throws IOException {
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement(getInitStatement());
        hiveCmdBuilder.addStatement("set mapreduce.job.reduces=" + numReducers + ";\n");
        hiveCmdBuilder.addStatement("set hive.merge.mapredfiles=false;\n"); //disable merge
        hiveCmdBuilder.addStatement(getCreateTableStatement());
        final String cmd = hiveCmdBuilder.toString();

        stepLogger.log("Create and distribute table, cmd: ");
        stepLogger.log(cmd);

        Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger);
        if (response.getFirst() != 0) {
            throw new RuntimeException("Failed to create flat hive table, error code " + response.getFirst());
        }
    }

    private KylinConfig getCubeSpecificConfig() {
        String cubeName = CubingExecutableUtil.getCubeName(getParams());
        CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = manager.getCube(cubeName);
        return cube.getConfig();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = getCubeSpecificConfig();
        try {

            int numReducers = determineNumReducer(config);
            createFlatHiveTable(config, numReducers);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
        }
    }

    public void setInitStatement(String sql) {
        setParam("HiveInit", sql);
    }

    public String getInitStatement() {
        return getParam("HiveInit");
    }

    public void setCreateTableStatement(String sql) {
        setParam("HiveRedistributeData", sql);
    }

    public String getCreateTableStatement() {
        return getParam("HiveRedistributeData");
    }

    public void setRowCountOutputDir(String rowCountOutputDir) {
        setParam("rowCountOutputDir", rowCountOutputDir);
    }

    public String getRowCountOutputDir() {
        return getParam("rowCountOutputDir");
    }
}
