package com.kylinolap.job;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.job.common.HadoopShellExecutable;
import com.kylinolap.job.common.MapReduceExecutable;
import com.kylinolap.job.common.ShellExecutable;
import com.kylinolap.job.constant.ExecutableConstants;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.cube.CubingJob;
import com.kylinolap.job.execution.ExecutableState;
import com.kylinolap.job.hadoop.cube.*;
import com.kylinolap.job.hadoop.dict.CreateDictionaryJob;
import com.kylinolap.job.hadoop.hbase.BulkLoadJob;
import com.kylinolap.job.hadoop.hbase.CreateHTableJob;
import com.kylinolap.job.impl.threadpool.AbstractExecutable;
import com.kylinolap.job.service.ExecutableManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.cube.CubeDescUpgrader;
import com.kylinolap.metadata.MetadataConstances;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.metadata.project.ProjectManager;
import com.kylinolap.metadata.project.RealizationEntry;
import com.kylinolap.metadata.realization.RealizationType;

/**
 * This is the utility class to migrate the Kylin metadata format from v1 to v2;
 * 
 * @author shaoshi
 *
 */
public class CubeMetadataUpgrade {

    private KylinConfig config = null;
    private ResourceStore store;

    private List<String> updatedResources = Lists.newArrayList();
    private List<String> errorMsgs = Lists.newArrayList();

    private static final Log logger = LogFactory.getLog(CubeMetadataUpgrade.class);

    public CubeMetadataUpgrade(String newMetadataUrl) {
        KylinConfig.destoryInstance();
        System.setProperty(KylinConfig.KYLIN_CONF, newMetadataUrl);
        KylinConfig.getInstanceFromEnv().setMetadataUrl(newMetadataUrl);

        
        config = KylinConfig.getInstanceFromEnv();
        store = getStore();
    }
    
    public void upgrade() {

//        upgradeTableDesc();
//        upgradeTableDesceExd();
//        upgradeCubeDesc();
//        upgradeProjectInstance();
        upgradeJobInstance();
        
    }

    private List<String> listResourceStore(String pathRoot) {
        List<String> paths = null;
        try {
            paths = store.collectResourceRecursively(pathRoot, MetadataConstances.FILE_SURFIX);
        } catch (IOException e1) {
            e1.printStackTrace();
            errorMsgs.add("Get IOException when scan resource store at: " + ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        }

        return paths;
    }

    private void upgradeCubeDesc() {
        logger.info("Reloading Cube Metadata from folder " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));

        List<String> paths = listResourceStore(ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        for (String path : paths) {

            try {
                CubeDescUpgrader upgrade = new CubeDescUpgrader(path);
                CubeDesc ndesc = upgrade.upgrade();
                ndesc.setSignature(ndesc.calculateSignature());
                
                getStore().putResource(ndesc.getModel().getResourcePath(), ndesc.getModel(), MetadataManager.MODELDESC_SERIALIZER);
                getStore().putResource(ndesc.getResourcePath(), ndesc, CubeDescManager.CUBE_DESC_SERIALIZER);
                updatedResources.add(ndesc.getResourcePath());
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade CubeDesc at '" + path + "' failed: " + e.getLocalizedMessage());
            }
        }

    }

    private void upgradeTableDesc() {
        List<String> paths = listResourceStore(ResourceStore.TABLE_RESOURCE_ROOT);
        for (String path : paths) {
            TableDesc t;
            try {
                t = store.getResource(path, TableDesc.class, MetadataManager.TABLE_SERIALIZER);
                t.init();

                // if it only has 1 "." in the path, delete the old resource if it exists
                if (path.substring(path.indexOf(".")).length() == MetadataConstances.FILE_SURFIX.length()) {
                    getStore().deleteResource(path);
                    // the new source will be new;
                    t.setLastModified(0);
                    getStore().putResource(t.getResourcePath(), t, MetadataManager.TABLE_SERIALIZER);
                    updatedResources.add(t.getResourcePath());
                }
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade TableDesc at '" + path + "' failed: " + e.getLocalizedMessage());
            }

        }

    }

    private void upgradeTableDesceExd() {

        List<String> paths = listResourceStore(ResourceStore.TABLE_EXD_RESOURCE_ROOT);
        for (String path : paths) {
            Map<String, String> attrs = Maps.newHashMap();

            InputStream is = null;
            try {
                is = store.getResource(path);
                if (is == null) {
                    continue;
                }
                try {
                    attrs.putAll(JsonUtil.readValue(is, HashMap.class));
                } finally {
                    if (is != null)
                        is.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade TableDescExd at '" + path + "' failed: " + e.getLocalizedMessage());
            }

            // parse table identity from file name
            String file = path;
            if (file.indexOf("/") > -1) {
                file = file.substring(file.lastIndexOf("/") + 1);
            }
            String tableIdentity = file.substring(0, file.length() - MetadataConstances.FILE_SURFIX.length()).toUpperCase();

            // for metadata upgrade, convert resource path to new pattern (<DB>.<TABLE>.json)
            if (tableIdentity.indexOf(".") < 0) {
                tableIdentity = appendDBName(tableIdentity);
                try {
                    getMetadataManager().saveTableExd(tableIdentity, attrs);
                    //delete old resoruce if it exists;
                    getStore().deleteResource(path);
                    updatedResources.add(path);
                } catch (IOException e) {
                    e.printStackTrace();
                    errorMsgs.add("Upgrade TableDescExd at '" + path + "' failed: " + e.getLocalizedMessage());
                }

            }

        }

    }

    public String appendDBName(String table) {

        if (table.indexOf(".") > 0)
            return table;

        Map<String, TableDesc> map = this.getMetadataManager().getAllTablesMap();

        int count = 0;
        String result = null;
        for (TableDesc t : map.values()) {
            if (t.getName().equalsIgnoreCase(table)) {
                result = t.getIdentity();
                count++;
            }
        }

        if (count == 1)
            return result;

        if (count > 1) {
            errorMsgs.add("There are more than 1 table named with '" + table + "' in different database; The program couldn't determine, randomly pick '" + result + "'");
        }
        return result;
    }

    private void upgradeProjectInstance() {
        List<String> paths = listResourceStore(ResourceStore.PROJECT_RESOURCE_ROOT);
        for (String path : paths) {
            try {
                com.kylinolap.cube.model.v1.ProjectInstance oldPrj = store.getResource(path, com.kylinolap.cube.model.v1.ProjectInstance.class, new JsonSerializer<com.kylinolap.cube.model.v1.ProjectInstance>(com.kylinolap.cube.model.v1.ProjectInstance.class));

                ProjectInstance newPrj = new ProjectInstance();
                newPrj.setUuid(oldPrj.getUuid());
                newPrj.setName(oldPrj.getName());
                newPrj.setOwner(oldPrj.getOwner());
                newPrj.setDescription(oldPrj.getDescription());
                newPrj.setLastModified(oldPrj.getLastModified());
                newPrj.setLastUpdateTime(oldPrj.getLastUpdateTime());
                newPrj.setCreateTime(oldPrj.getCreateTime());
                newPrj.setStatus(oldPrj.getStatus());
                List<RealizationEntry> realizationEntries = Lists.newArrayList();
                for (String cube : oldPrj.getCubes()) {
                    RealizationEntry entry = new RealizationEntry();
                    entry.setType(RealizationType.CUBE);
                    entry.setRealization(cube);
                    realizationEntries.add(entry);
                }
                newPrj.setRealizationEntries(realizationEntries);
                newPrj.getCreateTimeUTC();

                Set<String> tables = Sets.newHashSet();
                for (String table : oldPrj.getTables()) {
                    tables.add(this.appendDBName(table));
                }
                newPrj.setTables(tables);

                store.putResource(newPrj.getResourcePath(), newPrj, ProjectManager.PROJECT_SERIALIZER);
                updatedResources.add(path);
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade Project at '" + path + "' failed: " + e.getLocalizedMessage());
            }
        }

    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

    private ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(config);
    }

    private void upgradeJobInstance() {
        try {
            List<String> paths = getStore().collectResourceRecursively(ResourceStore.JOB_PATH_ROOT, "");
            for (String path: paths) {
                upgradeJobInstance(path);
            }
        } catch (IOException ex) {
            errorMsgs.add("upgrade job failed" + ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }

    private ExecutableState parseState(JobStatusEnum state) {
        switch (state) {
            case NEW:
            case PENDING:
                return ExecutableState.READY;
            case RUNNING:
                return ExecutableState.RUNNING;
            case FINISHED:
                return ExecutableState.SUCCEED;
            case ERROR:
                return ExecutableState.ERROR;
            case DISCARDED:
                return ExecutableState.DISCARDED;
            default:
                return ExecutableState.DISCARDED;
        }
    }

    private ExecutableState parseState(JobStepStatusEnum state) {
        switch (state) {
            case NEW:
            case PENDING:
            case WAITING:
                return ExecutableState.READY;
            case RUNNING:
                return ExecutableState.RUNNING;
            case FINISHED:
                return ExecutableState.SUCCEED;
            case ERROR:
                return ExecutableState.ERROR;
            case DISCARDED:
                return ExecutableState.DISCARDED;
            default:
                return ExecutableState.DISCARDED;
        }

    }

    private void upgradeJobInstance(String path) throws IOException {
        JobInstance job = getStore().getResource(path, JobInstance.class, new JsonSerializer<JobInstance>(JobInstance.class));
        CubingJob cubingJob = new CubingJob();
        cubingJob.setId(job.getId());
        cubingJob.setName(job.getName());
        cubingJob.setSubmitter(job.getSubmitter());
        for (JobInstance.JobStep step: job.getSteps()) {
            final AbstractExecutable executable = parseToExecutable(step);
            cubingJob.addTask(executable);
        }
        getExecutableManager().addJob(cubingJob);

        cubingJob.setStartTime(job.getExecStartTime());
        cubingJob.setEndTime(job.getExecEndTime());
        cubingJob.setMapReduceWaitTime(job.getMrWaiting());
        getExecutableManager().resetJobOutput(cubingJob.getId(), parseState(job.getStatus()), job.getStatus().toString());

        for (int i = 0, size = job.getSteps().size(); i < size; ++i) {
            final JobInstance.JobStep jobStep = job.getSteps().get(i);
            final InputStream inputStream = getStore().getResource(ResourceStore.JOB_OUTPUT_PATH_ROOT + "/" + job.getId() + "." + i);
            String output = null;
            if (inputStream != null) {
                JsonElement json = new JsonParser().parse(new InputStreamReader(inputStream));
                if (json instanceof JsonObject) {
                    final JsonElement element = ((JsonObject) json).get("output");
                    if (element != null) {
                        output = element.getAsString();
                    } else {
                        output = json.getAsString();
                    }
                } else {
                    output = json.getAsString();
                }
            }
            updateJobStepOutput(jobStep, output, cubingJob.getTasks().get(i));
        }
    }

    private void updateJobStepOutput(JobInstance.JobStep step, String output, AbstractExecutable task) {
        task.setStartTime(step.getExecStartTime());
        task.setEndTime(step.getExecEndTime());
        if (task instanceof MapReduceExecutable) {
            ((MapReduceExecutable) task).setMapReduceWaitTime(step.getExecWaitTime() * 1000);
        }
        getExecutableManager().resetJobOutput(task.getId(), parseState(step.getStatus()), output);
    }

    private AbstractExecutable parseToExecutable(JobInstance.JobStep step) {
        AbstractExecutable result;
        switch (step.getCmdType()) {
            case SHELL_CMD_HADOOP: {
                ShellExecutable executable = new ShellExecutable();
                executable.setCmd(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADOOP_FACTDISTINCT: {
                MapReduceExecutable executable = new MapReduceExecutable();
                executable.setMapReduceJobClass(FactDistinctColumnsJob.class);
                executable.setMapReduceParams(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADOOP_BASECUBOID: {
                MapReduceExecutable executable = new MapReduceExecutable();
                executable.setMapReduceJobClass(BaseCuboidJob.class);
                executable.setMapReduceParams(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADOOP_NDCUBOID: {
                MapReduceExecutable executable = new MapReduceExecutable();
                executable.setMapReduceJobClass(NDCuboidJob.class);
                executable.setMapReduceParams(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION: {
                MapReduceExecutable executable = new MapReduceExecutable();
                executable.setMapReduceJobClass(RangeKeyDistributionJob.class);
                executable.setMapReduceParams(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADOOP_CONVERTHFILE: {
                MapReduceExecutable executable = new MapReduceExecutable();
                executable.setMapReduceJobClass(CubeHFileJob.class);
                executable.setMapReduceParams(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADOOP_MERGECUBOID: {
                MapReduceExecutable executable = new MapReduceExecutable();
                executable.setMapReduceJobClass(MergeCuboidJob.class);
                executable.setMapReduceParams(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADOOP_NO_MR_DICTIONARY: {
                HadoopShellExecutable executable = new HadoopShellExecutable();
                executable.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
                executable.setJobClass(CreateDictionaryJob.class);
                executable.setJobParams(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE: {
                HadoopShellExecutable executable = new HadoopShellExecutable();
                executable.setJobClass(CreateHTableJob.class);
                executable.setJobParams(step.getExecCmd());
                result = executable;
                break;
            }
            case JAVA_CMD_HADOOP_NO_MR_BULKLOAD: {
                HadoopShellExecutable executable = new HadoopShellExecutable();
                executable.setJobClass(BulkLoadJob.class);
                executable.setJobParams(step.getExecCmd());
                result = executable;
                break;
            }
            default:
                throw new RuntimeException("invalid step type:" + step.getCmdType());
        }
        result.setName(step.getName());
        return result;
    }

    public static void main(String[] args) {

        if (!(args != null && args.length == 1)) {
            System.out.println("Usage: java CubeMetadataUpgrade <metadata_export_folder>; e.g, /export/kylin/meta");
            return;
        }

        String exportFolder = args[0];
        
        File oldMetaFolder = new File(exportFolder);
        if(!oldMetaFolder.exists()) {
            System.out.println("Provided folder doesn't exist: '" + exportFolder + "'");
            return;
        }
        
        if(!oldMetaFolder.isDirectory()) {
            System.out.println("Provided folder is not a directory: '" + exportFolder + "'");
            return;
        }

        
        String newMetadataUrl = oldMetaFolder.getAbsolutePath() + "_v2";
        try {
            FileUtils.deleteDirectory(new File(newMetadataUrl));
            FileUtils.copyDirectory(oldMetaFolder, new File(newMetadataUrl));
        } catch (IOException e) {
            e.printStackTrace();
        }

        CubeMetadataUpgrade instance = new CubeMetadataUpgrade(newMetadataUrl);

        instance.upgrade();
        
        logger.info("Run CubeMetadataUpgrade completed, check the following messages.");
        logger.info("The following resources have been successfully updated in : " + newMetadataUrl);
        for (String s : instance.updatedResources) {
            logger.info(s);
        }

        if (instance.errorMsgs.size() > 0) {
            logger.info("Here are the error/warning messages, you may need check:");
            for (String s : instance.errorMsgs) {
                logger.warn(s);
            }
        } else {
            logger.info("No error or warning messages; Looks all good.");
        }
    }
}
