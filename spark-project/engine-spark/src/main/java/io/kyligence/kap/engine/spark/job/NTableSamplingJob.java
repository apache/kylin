/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.Set;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NTableSamplingJob extends DefaultChainedExecutable {

    public String getTableIdentity() {
        return getParam(NBatchConstants.P_TABLE_NAME);
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String table = getParam(NBatchConstants.P_TABLE_NAME);
        final TableDesc tableDesc = NTableMetadataManager.getInstance(config, getProject()).getTableDesc(table);
        final ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(this.getProject());
        return Sets.newHashSet(tableDesc.getResourcePath(), projectInstance.getResourcePath());
    }

    public static NTableSamplingJob create(TableDesc tableDesc, String project, String submitter, int rows) {
        Preconditions.checkArgument(tableDesc != null, //
                "Create table sampling job failed for table not exist!");

        log.info("start creating a table sampling job on table {}", tableDesc.getIdentity());
        NTableSamplingJob job = new NTableSamplingJob();
        job.setId(UUID.randomUUID().toString());
        job.setName(JobTypeEnum.TABLE_SAMPLING.toString());
        job.setProject(project);
        job.setJobType(JobTypeEnum.TABLE_SAMPLING);
        job.setTargetSubject(tableDesc.getIdentity());

        job.setSubmitter(submitter);
        job.setParam(NBatchConstants.P_PROJECT_NAME, project);
        job.setParam(NBatchConstants.P_JOB_ID, job.getId());
        job.setParam(NBatchConstants.P_TABLE_NAME, tableDesc.getIdentity());
        job.setParam(NBatchConstants.P_SAMPLING_ROWS, String.valueOf(rows));

        JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT);
        JobStepFactory.addStep(job, JobStepType.SAMPLING);
        log.info("sampling job create success on table {}", tableDesc.getIdentity());
        return job;
    }

    @Override
    public String getTargetSubjectAlias() {
        return getTableIdentity();
    }

    @Override
    public boolean checkSuicide() {
        return null == NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getTableDesc(getTableIdentity());
    }

    NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    SamplingStep getSamplingStep() {
        return getTask(SamplingStep.class);
    }

    public static class SamplingStep extends NSparkExecutable {

        // called by reflection
        public SamplingStep() {
        }

        SamplingStep(String sparkSubmitClassName) {
            this.setSparkSubmitClassName(sparkSubmitClassName);
            this.setName(ExecutableConstants.STEP_NAME_TABLE_SAMPLING);
        }

        private String getTableIdentity() {
            return getParam(NBatchConstants.P_TABLE_NAME);
        }

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            ExecuteResult result = super.doWork(context);
            if (!result.succeed()) {
                return result;
            }
            UnitOfWork.doInTransactionWithRetry(() -> {
                if (checkSuicide()) {
                        log.info(
                                "This Table Sampling job seems meaningless now, quit before mergeRemoteMetaAfterSampling()");
                        return null;
                }
                mergeRemoteMetaAfterSampling();
                return null;
            }, getProject());
            return result;
        }

        private void mergeRemoteMetaAfterSampling() {
            try (val remoteStore = ExecutableUtils.getRemoteStore(KylinConfig.getInstanceFromEnv(), this)) {
                val remoteTblMgr = NTableMetadataManager.getInstance(remoteStore.getConfig(), getProject());
                val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
                localTblMgr.mergeAndUpdateTableExt(localTblMgr.getOrCreateTableExt(getTableIdentity()),
                        remoteTblMgr.getOrCreateTableExt(getTableIdentity()));

                // use create time of sampling job to update the create time of TableExtDesc
                final TableDesc tableDesc = localTblMgr.getTableDesc(getTableIdentity());
                final TableExtDesc tableExt = localTblMgr.getTableExtIfExists(tableDesc);
                TableExtDesc copyForWrite = localTblMgr.copyForWrite(tableExt);
                copyForWrite.setCreateTime(this.getCreateTime());
                localTblMgr.saveTableExt(copyForWrite);
            }
        }

        @Override
        protected Set<String> getMetadataDumpList(KylinConfig config) {

            final Set<String> dumpList = Sets.newHashSet();
            // dump project
            ProjectInstance instance = NProjectManager.getInstance(config).getProject(getProject());
            dumpList.add(instance.getResourcePath());

            // dump table & table ext
            final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
            final TableExtDesc tableExtDesc = tableMetadataManager
                    .getTableExtIfExists(tableMetadataManager.getTableDesc(getTableIdentity()));
            if (tableExtDesc != null) {
                dumpList.add(tableExtDesc.getResourcePath());
            }
            final TableDesc table = tableMetadataManager.getTableDesc(getTableIdentity());
            if (table != null) {
                dumpList.add(table.getResourcePath());
            }

            return dumpList;
        }
    }

    @Override
    protected void afterUpdateOutput(String jobId) {
        val job = getExecutableManager(getProject()).getJob(jobId);
        long duration = job.getDuration();
        long endTime = job.getEndTime();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        long startOfDay = TimeUtil.getDayStart(endTime);

        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(kylinConfig, getProject());
        jobStatisticsManager.updateStatistics(startOfDay, duration, 0);
    }

}
