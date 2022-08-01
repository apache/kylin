<template>
  <div id="streamingJobListPage">
    <el-alert :title="$t('adminTips')" type="tip" class="admin-tips" v-if="isShowAdminTips" @close="closeTips" show-icon></el-alert>
    <div class="streaming-jobs-list ksd-mrl-24">
      <div class="ksd-title-page ksd-mt-32">{{$t('streamingJobsList')}}</div>
      <el-row :gutter="20" class="jobs_tools_row ksd-mtb-8">
        <el-col :span="16">
          <div class="action_groups ksd-btn-group-minwidth ksd-fleft" v-if="monitorActions.includes('jobActions')">
            <el-button type="primary" text size="medium" icon="el-ksd-icon-play_with_border_22" :disabled="!batchBtnsEnabled.start" :loading="startLoading" @click="batchStart">{{$t('jobStart')}}</el-button>
            <el-button type="primary" text size="medium" icon="el-ksd-icon-resure_22" class="ksd-ml-2" :disabled="!batchBtnsEnabled.restart" :loading="restartLoading" @click="batchRestart">{{$t('jobRestart')}}</el-button>
            <el-button type="primary" text size="medium" icon="el-ksd-icon-stop_with_border_22" class="ksd-ml-2" :disabled="!batchBtnsEnabled.stop" :loading="stopLoading" @click="batchStop(false)">{{$t('jobStop')}}</el-button>
            <el-button type="primary" text size="medium" icon="el-ksd-icon-stop_with_border_22" class="ksd-ml-2" :disabled="!batchBtnsEnabled.stopImme" :loading="stopImmeLoading" @click="batchStop(true)">{{$t('stopJobImme')}}</el-button>
          </div>
        </el-col>
        <el-col :span="8">
          <el-button type="primary" text size="medium" class="ksd-ml-2 ksd-fright" icon="el-ksd-icon-refresh_22" @click="manualRefreshJobs">{{$t('refreshList')}}</el-button><el-input
          :placeholder="$t('pleaseSearch')" v-model="filter.model_name" v-global-key-event.enter.debounce="filterChange" @clear="filterChange()" class="show-search-btn ksd-fright" size="medium" prefix-icon="el-icon-search">
          </el-input>
        </el-col>
      </el-row>
      <el-row class="filter-status-list" v-show="filterTags.length">
        <div class="tag-layout">
          <el-tag closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{`${$t(item.source)}${$t('kylinLang.common.colon')}${item.label}`}}</el-tag><span class="clear-all-tags" @click="handleClearAllTags">{{$t('clearAll')}}</span>
        </div>
        <span class="filter-job-size">{{$t('filteredTotalSize', {totalSize: jobTotal})}}</span>
      </el-row>
      <el-row :gutter="10" id="listBox">
        <el-col :span="showStep?18:24" id="leftStreamingTableBox" :class="{'show-steps': showStep}">
          <el-table class="ksd-el-table streaming-jobs-table"
            tooltip-effect="dark"
            v-scroll-shadow
            ref="streamingJobsTable"
            :data="jobsList"
            highlight-current-row
            :default-sort = "{prop: 'create_time', order: 'descending'}"
            :empty-text="emptyText"
            @sort-change="sortJobList"
            @selection-change="handleSelectionChange"
            @cell-click="showLineSteps"
            :row-class-name="tableRowClassName"
            :key="$store.state.project.isAllProject">
            <el-table-column
              class-name="icon-column"
              :selectable="setJobSelectable"
              :checkbox-disable-tooltip="(row) => disableStartJobTips(row)"
              checkbox-disable-tooltip-placement="top"
              type="selection" align="center" width="32"></el-table-column>
            <el-table-column class-name="icon-column" align="center" width="32" prop="icon" v-if="monitorActions.includes('jobActions')">
              <template slot-scope="scope">
                <i class="ksd-fs-22" :class="{
                  'el-ksd-icon-arrow_table_right_22': scope.row.uuid !== selectedJob.uuid || !showStep,
                  'el-ksd-icon-arrow_table_down_22': scope.row.uuid == selectedJob.uuid && showStep}"></i>
              </template>
            </el-table-column>
            <el-table-column
              :label="$t('TargetObject')"
              sortable="custom"
              min-width="140"
              :show-search-input="true"
              :placeholder="$t('pleaseSearch')"
              :emptyFilterText="$t('kylinLang.common.noData')"
              :filter-filters-change="(v) => loadModelObjectList(v)"
              customFilterClass="filter-submitter"
              :filters="modelFilteArr.map(item => ({text: item, value: item}))"
              :filtered-value="filter.model_names"
              filter-icon="el-ksd-icon-filter_22"
              :show-multiple-footer="false"
              :filter-change="(v) => filterContent(v, 'model_names')"
              show-overflow-tooltip
              prop="model_alias">
              <template slot-scope="scope">
                <a class="link" @click="gotoModelList(scope.row)">{{scope.row.model_alias}}</a>
              </template>
            </el-table-column>
            <el-table-column
              :filters="jobTypeFilteArr.map(item => ({text: $t(item), value: item}))"
              :filtered-value="filter.job_types"
              :label="$t('JobType')"
              filter-icon="el-ksd-icon-filter_22"
              :show-multiple-footer="false"
              :filter-change="(v) => filterContent(v, 'job_types')"
              prop="job_type"
              width="164">
              <template slot-scope="scope">
                {{$t(scope.row.job_type)}}
              </template>
            </el-table-column>
            <el-table-column v-if="$store.state.project.isAllProject"
              :label="$t('project')"
              sortable='custom'
              :width="120"
              show-overflow-tooltip
              prop="project">
            </el-table-column>
            <el-table-column
              width="180"
              :filters="allStatus.map(item => ({text: $t(item), value: item}))"
              :filtered-value="filter.status" :label="$t('status')"
              filter-icon="el-ksd-icon-filter_22"
              :show-multiple-footer="false"
              :filter-change="(v) => filterContent(v, 'status')">
              <template slot-scope="scope">
                <common-tip :content="scope.row.launching_error?$t('errorStautsTips2'):$t('errorStautsTips')" :disabled="scope.row.job_status!=='ERROR'">
                <el-tag :type="jobStatus(scope.row.job_status)" size="small">{{$t(scope.row.job_status)}}</el-tag>
                </common-tip>
              </template>
            </el-table-column>
            <el-table-column
              width="140"
              sortable="custom"
              prop="data_latency"
              :label="$t('dataDuration')">
              <template slot-scope="scope">
                {{scope.row.data_latency | timeSize }}
              </template>
            </el-table-column>
            <el-table-column
              width="218"
              :label="$t('lastStatusChangeTime')"
              show-overflow-tooltip
              prop="last_modified"
              sortable="custom">
              <template slot-scope="scope">
                {{transToGmtTime(scope.row.last_modified)}}
              </template>
            </el-table-column>
            <el-table-column
              width="218"
              :label="$t('lastStatusDuration')"
              show-overflow-tooltip
              prop="last_status_duration"
              sortable="custom">
              <template slot-scope="scope">
                {{scope.row.last_status_duration | timeSize }}
              </template>
            </el-table-column>
            <el-table-column
              :label="$t('Actions')"
              v-if="monitorActions.includes('jobActions')"
              class-name="job-fc-icon"
              width="96">
              <template slot-scope="scope">
                <common-tip :content="$t('jobDiagnosis')" v-if="monitorActions.includes('diagnostic')">
                  <i class="el-icon-ksd-ostin_diagnose ksd-fs-14 ksd-ml-4" @click.stop="showDiagnosisDetail(scope.row.uuid)"></i>
                </common-tip>
                <common-tip :content="$t('configurations')">
                  <i class="el-ksd-icon-controller_22 ksd-fs-22 ksd-ml-4" @click.stop="configJob(scope.row)"></i>
                </common-tip>
                <common-tip :content="$t('kylinLang.common.moreActions')" v-if="(!scope.row.launching_error&&scope.row.job_status!=='STARTING')||scope.row.yarn_app_url" >
                   <el-dropdown trigger="click">
                    <span class="el-dropdown-link" @click.stop>
                      <common-tip :content="$t('kylinLang.common.moreActions')">
                        <i class="el-icon-ksd-table_others ksd-fs-16"></i>
                      </common-tip>
                    </span>
                    <el-dropdown-menu slot="dropdown">
                      <el-dropdown-item v-if="!scope.row.launching_error&&scope.row.job_status!=='STARTING'" @click.native="clickFile(scope.row)">{{$t('logInfoTip')}}</el-dropdown-item>
                      <el-dropdown-item v-if="scope.row.yarn_app_url" @click.native="skipToSparkDetail(scope.row)">{{$t('sparkTaskDetails')}}<i class="el-icon-ksd-export ksd-ml-4"></i></el-dropdown-item>
                    </el-dropdown-menu>
                   </el-dropdown>
                </common-tip>
              </template>
            </el-table-column>
          </el-table>
          <kylin-pager :totalSize="jobTotal" :curPage="filter.page_offset+1"  v-on:handleCurrentChange='currentChange' ref="jobPager" :refTag="pageRefTags.streamingJobPager" class="ksd-mtb-16 ksd-center" ></kylin-pager>
        </el-col>
        <el-col :span="6" v-if="showStep" id="rightDetail" :class="{'is-admin-tips': $store.state.user.isShowAdminTips&&isAdminRole}">
          <el-tabs v-model="jobDetailTab" @tab-click="handleChangeTab">
            <el-tab-pane :label="$t('dataStatistics')" name="statistics">
              <el-select class="ksd-fright" size="small" v-model="time_filter">
                <el-option
                  v-for="item in filterOptions"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
                </el-option>
              </el-select>
              <div v-if="!isNoStatisticsData">
                <div class="data-consumption-chart">
                  <div class="chart-title">{{$t('consumptionRate')}}</div>
                  <div id="consumption-chart" class="data-chart"></div>
                </div>
                <div class="data-latency-chart ksd-mt-16">
                  <div class="chart-title">{{$t('dataLatency')}}</div>
                  <div id="latency-chart" class="data-chart"></div>
                </div>
                <div class="data-processing-chart ksd-mt-16">
                  <div class="chart-title">{{$t('processingTime')}}</div>
                  <div id="processing-chart" class="data-chart"></div>
                </div>
              </div>
              <kylin-nodata v-else :content="$t('isNoStatisticsData')"></kylin-nodata>
            </el-tab-pane>
            <el-tab-pane :label="$t('recordStatus')" name="records">
              <div class="ksd-list" v-if="selectedJobRecords.length">
                <p class="list" v-for="r in selectedJobRecords" :key="r.create_time">
                  <span class="label">{{$t(r.action)}}</span><span class="text ksd-ml-24">{{transToGmtTime(r.create_time)}}</span>
                </p>
              </div>
              <kylin-nodata v-else :content="$t('isNoRecords')"></kylin-nodata>
            </el-tab-pane>
          </el-tabs>
          <div class='job-btn' id="jobDetailBtn" v-show="isShowBtn" :class="{'is-filter-list': filterTags.length}" @click='showStep=false'><i class='el-ksd-icon-arrow_right_16' aria-hidden='true'></i></div>
        </el-col>
      </el-row>
    </div>
    <el-dialog
      :title="$t('configurations')"
      append-to-body
      class="configurations-dialog"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :visible.sync="showConfigurations"
      @close="handleConfigurationsClose"
      width="600px">
      <div>
        <el-row :gutter="10">
          <el-col :span="14"><span class="title">{{$t('key')}}</span></el-col>
          <el-col :span="6"><span class="title">{{$t('value')}}</span></el-col>
          <el-col :span="4"></el-col>
        </el-row>
        <el-row :gutter="10" class="ksd-mt-10" v-for="(item, index) in paramsConfigs" :key="index">
          <el-col :span="14">
            <el-input v-model.trim="paramsConfigs[index][0]"
              :disabled="paramsConfigs[index] && !!paramsConfigs[index][2].isDefault"
              :class="{'is-mul-key': paramsConfigs[index][2].isMulParamsKey, 'is-empty': paramsConfigs[index][2].isEmpty&&!paramsConfigs[index][0]}"
              @change="handleValidateParamsKey()"
              :placeholder="$t('pleaseInputKey')"></el-input>
            <div class="error-msg" v-if="paramsConfigs[index][2].isMulParamsKey&&!paramsConfigs[index][2].isDefault">{{$t('mulParamsKeyTips')}}</div>
          </el-col>
          <el-col :span="6">
            <el-input v-number2="paramsConfigs[index][1]"
              v-model.trim="paramsConfigs[index][1]"
              :class="{'is-empty': paramsConfigs[index][2].isEmpty&&!paramsConfigs[index][1]}"
              :placeholder="$t('pleaseInputValue')"
              @change="handleValidateParamsKey()"
              v-if="numberParams.indexOf(item[0]) !== -1"></el-input>
            <el-input v-else v-model.trim="paramsConfigs[index][1]" :class="{'is-empty': paramsConfigs[index][2].isEmpty&&!paramsConfigs[index][1]}" :placeholder="$t('pleaseInputValue')" />
          </el-col>
          <el-col :span="4">
            <span class="action-btns ksd-ml-5">
              <el-button type="primary" icon="el-ksd-icon-add_22" plain circle size="small" @click="addParamsConfigs()"></el-button>
              <el-button icon="el-ksd-icon-minus_22" class="ksd-ml-5" plain circle size="small" :disabled="[...buildDefaultParams, ...mergeDefaultParams].indexOf(item[0]) !== -1" @click="removeParamsConfigs(index)"></el-button>
            </span>
          </el-col>
        </el-row>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="handleConfigurationsClose">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="loadingSetting" @click="saveSettings">{{$t('kylinLang.common.save')}}</el-button>
      </div>
    </el-dialog>
    <el-dialog
      id="show-diagnos"
      limited-area
      :title="$t('output')"
      :visible.sync="dialogVisible"
      @close="handleCloseOutputDialog"
      :close-on-press-escape="false"
      :close-on-click-modal="false">
      <job_dialog :stepDetail="outputDetail" :showOutputJob="showOutputJob"></job_dialog>
      <span slot="footer" class="dialog-footer">
        <el-button plain size="medium" @click="dialogVisible = false">{{$t('kylinLang.common.close')}}</el-button>
      </span>
    </el-dialog>
    <diagnostic v-if="showDiagnostic" @close="showDiagnostic = false" :jobId="diagnosticId"/>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations } from 'vuex'
import { cacheLocalStorage, objectClone, indexOfObjWithSomeKey, countObjWithSomeKey } from 'util/index'
import { handleError, handleSuccess, transToGmtTime, kylinConfirm } from 'util/business'
import { pageRefTags, bigPageCount } from 'config'
import $ from 'jquery'
import locales from './locales'
import { handleSuccessAsync } from 'util'
import charts from 'util/charts'
import echarts from 'echarts'
import jobDialog from '../job_dialog'
import Diagnostic from 'components/admin/Diagnostic/index'
@Component({
  methods: {
    ...mapActions({
      loadStreamingJobsList: 'LOAD_STREAMING_JOBS_LIST',
      getStreamingJobRecords: 'GET_STREAMING_JOB_RECORDS',
      updateStreamingConfig: 'UPDATE_STREAMING_CONFIGURATIONS',
      updateStreamingJobs: 'UPDATE_STREAMING_JOBS',
      getStreamingChartData: 'GET_STREAMING_CHART_DATA',
      getModelObjectList: 'GET_MODEL_OBJECT_LIST',
      getJobSimpleLog: 'GET_JOB_SIMPLE_LOG'
    }),
    ...mapMutations({
      setProject: 'SET_PROJECT'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  components: {
    'job_dialog': jobDialog,
    Diagnostic
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'monitorActions',
      'isAdminRole'
    ])
  },
  locales
})
export default class StreamingJobsList extends Vue {
  pageRefTags = pageRefTags
  transToGmtTime = transToGmtTime
  filter = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.streamingJobPager) || bigPageCount,
    job_types: [],
    model_names: [],
    sort_by: 'create_time',
    reverse: true,
    status: [],
    model_name: '',
    isAuto: false
  }
  startLoading = false
  restartLoading = false
  stopLoading = false
  stopImmeLoading = false
  batchBtnsEnabled = {
    start: false,
    restart: false,
    stop: false,
    stopImme: false
  }
  stCycle = null
  showStep = false
  jobDetailTab = 'statistics'
  isNoStatisticsData = false
  paramsConfigs = []
  filterTags = []
  jobsList = []
  jobTotal = 0
  multipleSelection = []
  isPausePolling = false
  selectedJob = {}
  settingJob = null
  selectedJobRecords = []
  modelFilteArr = []
  filterOptions = [
    {value: '30', label: this.$t('30m')},
    {value: '60', label: this.$t('1h')},
    {value: '360', label: this.$t('6h')},
    {value: '720', label: this.$t('12h')},
    {value: '1440', label: this.$t('1d')},
    {value: '4320', label: this.$t('3d')},
    {value: '10080', label: this.$t('7d')}
  ]
  jobTypeFilteArr = ['STREAMING_BUILD', 'STREAMING_MERGE']
  allStatus = ['STARTING', 'RUNNING', 'STOPPING', 'ERROR', 'STOPPED']
  buildDefaultParams = ['spark.master', 'spark.driver.memory', 'spark.executor.instances', 'spark.executor.cores', 'spark.executor.memory', 'spark.sql.shuffle.partitions', 'kylin.streaming.duration', 'kylin.streaming.job-retry-enabled', 'kylin.streaming.kafka-conf.maxOffsetsPerTrigger']
  mergeDefaultParams = ['spark.master', 'spark.driver.memory', 'spark.executor.instances', 'spark.executor.cores', 'spark.executor.memory', 'spark.sql.shuffle.partitions', 'kylin.streaming.segment-max-size', 'kylin.streaming.segment-merge-threshold', 'kylin.streaming.job-retry-enabled']
  numberParams = ['spark.executor.cores', 'kylin.streaming.duration', 'spark.executor.instances', 'kylin.streaming.segment-merge-threshold', 'kylin.streaming.kafka-conf.maxOffsetsPerTrigger']
  showConfigurations = false
  loadingSetting = false
  consumpChart = null
  latencyChart = null
  processingChart = null
  time_filter = '30'
  isShowBtn = true
  isMulParamsKey = false
  dialogVisible = false
  showOutputJob = null
  outputDetail = ''
  showDiagnostic = false
  diagnosticId = ''
  get isShowAdminTips () {
    return this.$store.state.user.isShowAdminTips && this.isAdminRole && !this.$store.state.system.isShowGlobalAlter
  }
  get emptyText () {
    return this.filter.key || this.filter.job_types.length || this.filter.status.length ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  // 模型无索引时不让启动任务
  setJobSelectable (row) {
    return row.model_indexes && row.model_indexes > 0 && !row.model_broken && !!row.partition_desc && !!row.partition_desc.partition_date_column
  }

  disableStartJobTips (row) {
    if (row.model_broken) {
      return this.$t('borkenModelDisableStartJobTips', {modelName: row.model_alias})
    } else if (!(row.partition_desc && row.partition_desc.partition_date_column)) {
      return this.$t('noPartitonColumnDisableStartJobTips')
    } else {
      return this.$t('disableStartJobTips')
    }
  }

  @Watch('$store.state.project.isAllProject')
  selectAllProject (curVal) {
    if (curVal) {
      this.jobsList = []
      this.$nextTick(() => {
        this.manualRefreshJobs()
        this.loadModelObjectList()
      })
    }
  }

  closeTips () {
    this.$store.state.user.isShowAdminTips = false
    cacheLocalStorage('isHideAdminTips', true)
  }
  async batchStart () {
    if (!this.batchBtnsEnabled.start) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      try {
        this.startLoading = true
        const data = {project: this.currentSelectedProject, action: 'START', job_ids: this.idsArr}
        if (this.$store.state.project.isAllProject) {
          delete data.project
        }
        const res = await this.updateStreamingJobs(data)
        await handleSuccessAsync(res)
        this.startLoading = false
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        this.manualRefreshJobs()
      } catch (e) {
        handleError(e)
        this.startLoading = false
      }
    }
  }
  async batchRestart () {
    if (!this.batchBtnsEnabled.restart) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      try {
        this.restartLoading = true
        const data = {project: this.currentSelectedProject, action: 'RESTART', job_ids: this.idsArr}
        if (this.$store.state.project.isAllProject) {
          delete data.project
        }
        const res = await this.updateStreamingJobs(data)
        await handleSuccessAsync(res)
        this.restartLoading = false
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        this.manualRefreshJobs()
      } catch (e) {
        handleError(e)
        this.restartLoading = false
      }
    }
  }
  async stopJob (data, isStopImme) {
    const loadingType = isStopImme ? 'stopImmeLoading' : 'stopLoading'
    try {
      this[loadingType] = true
      const res = await this.updateStreamingJobs(data)
      await handleSuccessAsync(res)
      this[loadingType] = false
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
      this.manualRefreshJobs()
    } catch (e) {
      handleError(e)
      this[loadingType] = false
    }
  }
  async batchStop (isStopImme) {
    if (!isStopImme && !this.batchBtnsEnabled.stop) return
    if (isStopImme && !this.batchBtnsEnabled.stopImme) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      const msg = isStopImme ? this.$t('stopStreamingJobImmeTips') : this.$t('stopStreamingJobTips')
      const stopJobType = isStopImme ? this.$t('stopJobImme') : this.$t('stopJob')
      await kylinConfirm(msg, {confirmButtonText: stopJobType}, stopJobType)
      const data = {project: this.currentSelectedProject, action: isStopImme ? 'FORCE_STOP' : 'STOP', job_ids: this.idsArr}
      // const isSubmit = await this.callGlobalDetailDialog({
      //   msg: this.$t('stopStreamingJobTips'),
      //   title: this.$t('stopJob'),
      //   dialogType: '',
      //   wid: '600px',
      //   showDetailBtn: false,
      //   isSubSubmit: true,
      //   submitSubText: this.$t('stopJob'),
      //   submitText: this.$t('stopJobImme')
      // })
      // data.action = isSubmit.isOnlySave ? 'STOP' : 'FORCE_STOP'
      if (this.$store.state.project.isAllProject) {
        delete data.project
      }
      this.stopJob(data, isStopImme)
    }
  }
  filterChange (val) {
    this.searchLoading = true
    this.filter.page_offset = 0
    this.manualRefreshJobs()
    this.showStep = false
  }
  resetSelection () {
    this.multipleSelection = []
    this.$refs.streamingJobsTable && this.$refs.streamingJobsTable.clearSelection && this.$refs.streamingJobsTable.clearSelection()
    this.idsArrCopy = []
    this.idsArr = []
  }
  configJob (rows) {
    this.settingJob = rows
    const defaultParams = rows.job_type === 'STREAMING_BUILD' ? this.buildDefaultParams : this.mergeDefaultParams
    this.paramsConfigs = Object.entries(rows.params).map(it => ([...it, {isDefault: defaultParams.indexOf(it[0]) !== -1}]))
    this.showConfigurations = true
  }
  handleConfigurationsClose (isSubmit) {
    this.showConfigurations = false
    if (isSubmit) {
      this.loadList()
    }
  }
  async clickFile (row) {
    try {
      const res = await this.getJobSimpleLog({ project: row.project, job_id: row.uuid })
      const data = await handleSuccessAsync(res)
      this.outputDetail = data.cmd_output
      this.showOutputJob = row
      this.dialogVisible = true
    } catch (e) {
      handleError(e)
      this.dialogVisible = false
      this.showOutputJob = null
    }
  }
  skipToSparkDetail (row) {
    window.open(row.yarn_app_url)
  }
  handleCloseOutputDialog () {
    this.showOutputJob = null
  }
  currentChange (size, count) {
    this.filter.page_offset = size
    this.filter.page_size = count
    this.resetSelection()
    this.getJobsList()
    this.closeIt()
  }
  closeIt () {
    if (this.showStep) {
      this.showStep = false
    }
  }
  // 增加配置
  addParamsConfigs () {
    this.paramsConfigs.unshift(['', '', {isDefault: false}])
  }
  // 删减配置
  removeParamsConfigs (index) {
    this.paramsConfigs.splice(index, 1)
    this.handleValidateParamsKey()
  }
  // 检测key是否重复
  handleValidateParamsKey () {
    this.isMulParamsKey = false
    this.paramsConfigs.forEach((p, index) => {
      if (countObjWithSomeKey(this.paramsConfigs, 0, p[0]) > 1) {
        this.$set(this.paramsConfigs[index][2], 'isMulParamsKey', true)
        this.isMulParamsKey = true
      } else {
        this.$set(this.paramsConfigs[index][2], 'isMulParamsKey', false)
      }
      if (!p[0] || !p[1]) {
        this.$set(this.paramsConfigs[index][2], 'isEmpty', true)
      }
    })
  }
  saveSettings () {
    this.handleValidateParamsKey()
    const params = {}
    let isEmptyValue
    this.paramsConfigs.forEach((item, index) => {
      if (!item[0] || !item[1]) {
        isEmptyValue = true
      }
      params[item[0]] = item[1]
    })
    if (isEmptyValue || this.isMulParamsKey) return // 默认参数有空值不能提交，key值重复不能提交
    this.loadingSetting = true
    this.updateStreamingConfig({project: this.currentSelectedProject, job_id: this.settingJob.uuid, params}).then(() => {
      this.loadingSetting = false
      this.handleConfigurationsClose(true)
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }).catch((e) => {
      handleError(e)
      this.loadingSetting = false
    })
  }
  // 删除单个筛选条件
  handleClose (tag) {
    const index = this.filter[tag.key].indexOf(tag.label)
    index > -1 && this.filter[tag.key].splice(index, 1)
    this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    this.filter.page_offset = 0
    this.manualRefreshJobs()
  }
  // 清除所有的tags
  handleClearAllTags () {
    this.filter.page_offset = 0
    this.filter.model_names.splice(0, this.filter.model_names.length)
    this.filter.job_types.splice(0, this.filter.job_types.length)
    this.filter.status.splice(0, this.filter.status.length)
    this.filterTags = []
    this.manualRefreshJobs()
  }
  sortJobList ({ column, prop, order }) {
    if (order === 'ascending') {
      this.filter.reverse = false
    } else {
      this.filter.reverse = true
    }
    this.filter.sort_by = prop
    this.filter.page_offset = 0
    this.manualRefreshJobs()
  }
  handleSelectionChange (val) {
    if (val && val.length) {
      this.multipleSelection = val
      this.isPausePolling = true
      const selectedStatus = this.multipleSelection.map((item) => {
        return item.job_status
      })
      this.getBatchBtnStatus(selectedStatus)
      this.idsArr = this.multipleSelection.map((item) => {
        return item.uuid
      })
    } else {
      this.isPausePolling = false
      this.multipleSelection = []
      this.batchBtnsEnabled = {
        start: false,
        restart: false,
        stop: false,
        stopImme: false
      }
      this.idsArr = []
    }
  }
  jobStatus (status) {
    switch (status) {
      case 'STARTING': return ''
      case 'STOPPING': return ''
      case 'STOPPED': return 'warning'
      case 'ERROR': return 'danger'
      default: return ''
    }
  }
  getBatchBtnStatus (statusArr) {
    const batchBtns = {
      start: ['ERROR', 'STOPPED'],
      restart: ['RUNNING'],
      stop: ['RUNNING'],
      stopImme: ['STARTING', 'RUNNING', 'STOPPING']
    }
    $.each(batchBtns, (key, item) => {
      this.batchBtnsEnabled[key] = this.isContain(item, statusArr)
    })
  }
  isContain (arr1, arr2) {
    for (let i = arr2.length - 1; i >= 0; i--) {
      if (!arr1.includes(arr2[i])) {
        return false
      }
    }
    return true
  }
  showLineSteps (row, column, cell) {
    if (column.property === 'icon') {
      var needShow = false
      if (row.uuid !== this.selectedJob.uuid) {
        needShow = true
      } else {
        needShow = !this.showStep
      }
      this.showStep = needShow
      this.selectedJob = row
      // this.jobDetailTab = 'statistics'
      if (this.jobDetailTab === 'records') {
        this.selectedJobRecords = []
        this.getRecordsData()
      } else {
        this.getChartData()
      }
    }
  }
  initLineChart (data) {
    if (!this.showStep) return
    if (data.create_time.length > 0) {
      this.isNoStatisticsData = false
      setTimeout(() => {
        const consumptionId = document.getElementById('consumption-chart')
        const latencyId = document.getElementById('latency-chart')
        const processingId = document.getElementById('processing-chart')
        const xDates = data.create_time && data.create_time.reverse().map(it => {
          const dateSplit = transToGmtTime(+it).split(' ')
          return dateSplit[0] + '\n' + dateSplit[1] + ' ' + dateSplit[2]
        })
        const yConsumpVol = data.consumption_rate_hist.reverse()
        const yLatencyVol = data.data_latency_hist.reverse()
        const yProcessingVol = data.processing_time_hist.reverse()
        const consumpDatas = {}
        const latencyDatas = {}
        const processingDatas = {}
        data.create_time && data.create_time.forEach((k, index) => {
          consumpDatas[k] = data.consumption_rate_hist[index]
          latencyDatas[k] = data.data_latency_hist[index]
          processingDatas[k] = data.processing_time_hist[index]
        })
        this.consumpChart = echarts.init(consumptionId)
        const consumpOption = charts.line(this, xDates, yConsumpVol, consumpDatas)
        consumpOption.grid.left = 38
        consumpOption.yAxis.axisLabel.formatter = (val) => {
          if (val > 0 && val < 1) {
            return ''
          } else if (val < 1000) {
            return `${val}`
          } else {
            return `${val / 1000}K`
          }
        }
        consumpOption.yAxis.name = this.$t('consumptionUnit')
        consumpOption.tooltip.trigger = 'axis'
        consumpOption.tooltip.formatter = (vals) => {
          return `<div>${vals[0].name}</div><div>${vals[0].data} ${this.$t('consumptionUnit')}</div>`
        }
        this.consumpChart.setOption(consumpOption)
        this.latencyChart = echarts.init(latencyId)
        const latencyOption = charts.line(this, xDates, yLatencyVol, latencyDatas)
        latencyOption.yAxis.axisLabel.formatter = (val) => {
          if (val / 1000 < 1000) {
            return `${val / 1000}`
          } else {
            return `${val / 1000 / 1000}K`
          }
        }
        latencyOption.grid.left = 38
        latencyOption.yAxis.name = this.$t('latencyUnit')
        latencyOption.tooltip.trigger = 'axis'
        latencyOption.tooltip.formatter = (vals) => {
          return `<span>${vals[0].name}: ${vals[0].data / 1000} ${this.$t('latencyUnit')}</span>`
        }
        this.latencyChart.setOption(latencyOption)
        this.processingChart = echarts.init(processingId)
        const processingOption = charts.line(this, xDates, yProcessingVol, processingDatas)
        processingOption.yAxis.axisLabel.formatter = (val) => {
          if (val / 1000 < 1000) {
            return `${val / 1000}`
          } else {
            return `${val / 1000 / 1000}K`
          }
        }
        processingOption.grid.left = 38
        processingOption.yAxis.name = this.$t('processingUnit')
        processingOption.tooltip.trigger = 'axis'
        processingOption.tooltip.formatter = (vals) => {
          return `<span>${vals[0].name}: ${vals[0].data / 1000} ${this.$t('processingUnit')}</span>`
        }
        this.processingChart.setOption(processingOption)
      })
    } else {
      this.isNoStatisticsData = true
    }
  }
  handleChangeTab (tab) {
    if (tab.name === 'records') {
      this.getRecordsData()
    } else {
      this.getChartData()
    }
  }
  tableRowClassName ({row, rowIndex}) {
    if (row.uuid === this.selectedJob.uuid && this.showStep) {
      return 'current-row2'
    }
  }
  gotoModelList (item) {
    // 暂停轮询，清掉计时器
    clearTimeout(this.stCycle)
    this.isPausePolling = true
    // 如果是全 project 模式，需要先改变当前 project 选中值
    if (this.$store.state.project.isAllProject) {
      this.setProject(item.project)
    }
    this.$router.push({name: 'ModelList', params: { modelAlias: item.model_alias }})
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      job_types: 'JobType',
      status: 'ProgressStatus',
      model_names: 'TargetObject'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        let label
        if (type === 'model_names') {
          label = item
        } else {
          label = this.$t(item)
        }
        this.filterTags.push({label: label, source: maps[type], key: type})
      }
    })
    this.filter[type] = val
    this.filter.page_offset = 0
    this.manualRefreshJobs()
  }
  getRecordsData () {
    this.getStreamingJobRecords({project: this.selectedJob.project, job_id: this.selectedJob.uuid}).then((res) => {
      handleSuccess(res, (data) => {
        this.selectedJobRecords = data
      }, (resError) => {
        handleError(resError)
      })
    })
  }
  getChartData () {
    this.getStreamingChartData({project: this.selectedJob.project, job_id: this.selectedJob.uuid, time_filter: this.time_filter}).then((res) => {
      handleSuccess(res, (data) => {
        this.initLineChart(data)
      })
    }, (resError) => {
      handleError(resError)
    })
  }
  getJobsList () {
    return new Promise((resolve, reject) => {
      if (!this.currentSelectedProject) return reject()
      let data = {}
      const statuses = this.filter.status.join(',')
      Object.keys(this.filter).forEach(key => key !== 'status' && (data[key] = this.filter[key]))
      this.loadStreamingJobsList({...data, statuses}).then((res) => {
        handleSuccess(res, (data) => {
          if (data.total_size) {
            this.jobsList = data.value
            if (this.selectedJob) {
              const selectedIndex = indexOfObjWithSomeKey(this.jobsList, 'uuid', this.selectedJob.uuid)
              if (selectedIndex !== -1) {
                this.selectedJob = this.jobsList[selectedIndex]
                if (this.jobDetailTab === 'records') {
                  this.getRecordsData()
                } else {
                  this.getChartData()
                }
              }
            }
            if (this.multipleSelection.length) {
              const cloneSelections = objectClone(this.multipleSelection)
              this.multipleSelection = []
              cloneSelections.forEach((m) => {
                const index = indexOfObjWithSomeKey(this.jobsList, 'uuid', m.uuid)
                if (index !== -1) {
                  this.$nextTick(() => {
                    this.$refs.streamingJobsTable.toggleRowSelection(this.jobsList[index])
                  })
                }
              })
            }
            this.jobTotal = data.total_size
          } else {
            this.jobsList = []
            this.jobTotal = 0
          }
          this.searchLoading = false
        })
        resolve()
      }, (res) => {
        handleError(res)
        this.searchLoading = false
        reject()
      })
    })
  }
  loadList () {
    if (this.$store.state.project.isAllProject) {
      delete this.filter.project
    } else {
      this.filter.project = this.currentSelectedProject
    }
    return this.getJobsList()
  }
  manualRefreshJobs () {
    // 手动刷新部分，接口skip session 设为false
    this.filter.isAuto = false
    // this.waitingFilter.isAuto = false
    this.resetSelection()
    this.loadList()
  }
  refreshJobs () {
    if (!this.isPausePolling) {
      return this.loadList()
    } else {
      return new Promise((resolve) => {
        resolve()
      })
    }
  }
  autoFilter () {
    if (this.stCycle) {
      this.filter.isAuto = true
    }
    clearTimeout(this.stCycle)
    this.stCycle = setTimeout(() => {
      this.refreshJobs().then((res) => {
        handleSuccess(res, (data) => {
          if (this._isDestroyed) {
            return
          }
          this.autoFilter()
        })
      }, (res) => {
        handleError(res)
      })
    }, 5000)
  }
  async loadModelObjectList (filterValue) {
    try {
      const data = { project: this.currentSelectedProject, model_name: filterValue, page_size: 100 }
      if (this.$store.state.project.isAllProject) {
        delete data.project
      }
      const res = await this.getModelObjectList(data)
      this.modelFilteArr = await handleSuccessAsync(res)
    } catch (e) {
      handleError(e)
    }
  }
  created () {
    // const { modelAlias, jobStatus } = this.$route.query
    // modelAlias && (this.filter.subject = modelAlias)
    // jobStatus && (this.filter.status = jobStatus)
    // this.selectedJob = {} // 防止切换project时，发一个不存在该项目jobId的jobDetail的请求
    this.filter.project = this.currentSelectedProject
    if (this.currentSelectedProject) {
      this.autoFilter()
      this.getJobsList()
      this.loadModelObjectList()
    }
  }
  isShowJobBtn (e) {
    if (!e) return
    const sTop = e.target.scrollTop
    const rightDetailH = document.getElementById('rightDetail') && document.getElementById('rightDetail').clientHeight
    let jobBtn = document.getElementById('jobDetailBtn')
    if (jobBtn) {
      const top = sTop > (rightDetailH - 20) ? rightDetailH - 70 : sTop
      jobBtn.style.cssText = `top: ${top + 20}px;`
    }
  }
  mounted () {
    if (document.getElementById('scrollContent')) {
      document.getElementById('scrollContent').addEventListener('scroll', this.isShowJobBtn, false)
    }
  }
  beforeDestroy () {
    clearTimeout(this.stCycle)
    if (document.getElementById('scrollContent')) {
      document.getElementById('scrollContent').removeEventListener('scroll', this.isShowJobBtn, false)
    }
  }
  showDiagnosisDetail (id) {
    this.diagnosticId = id
    this.showDiagnostic = true
  }
}
</script>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  #streamingJobListPage {
    .jobs_tools_row {
      font-size: 0px;
    }
    .show-search-btn {
      width: 248px;
    }
    .streaming-jobs-table {
      .icon-column {
        .cell {
          padding-left: 5px;
          padding-right: 5px;
        }
      }
      .el-ksd-icon-arrow_table_right_22,
      .el-ksd-icon-arrow_table_down_22 {
        color: @text-disabled-color;
      }
      .link{
        text-decoration: underline;
        color:@base-color;
      }
      .el-ksd-icon-controller_22,
      .el-ksd-icon-log_22 {
        cursor: pointer;
      }
    }
    #rightDetail {
      width: 410px;
      border-radius: 0;
      box-shadow: -3px 3px 5px @ke-color-secondary;
      border: 0;
      padding: 16px 0 16px 16px !important;
      min-height: calc(~'100vh - 168px');
      position: relative;
      &.is-admin-tips {
        min-height: calc(~'100vh - 205px');
      }
      .el-tabs__content {
        overflow: visible;
        min-height: calc(~'100vh - 260px');
        .el-radio-group {
          position: absolute;
          right: 0px;
        }
      }
      .job-btn {
        position: absolute;
        // right: 345px;
        left: -9px;
        top: 20px;
        height: 24px;
        width: 24px;
        border-radius: 100%;
        box-shadow: 0px 2px 8px rgba(50, 73, 107, 0.24);
        cursor: pointer;
        text-align: center;
        background-color: @fff;
        z-index: 10;
        &.is-filter-list {
          top: 270px;
        }
        i {
          font-size: 16px;
          margin-top: 2px;
        }
        &:hover {
          background-color: @ke-color-primary-hover;
          i {
            color: @fff;
          }
        }
        &:active {
          background-color: @ke-color-primary-active;
        }
      }
      .chart-title {
        color: @text-title-color;
        font-size: 14px;
        line-height: 22px;
        margin-bottom: 12px;
      }
      .data-chart {
        height: 125px;
      }
      .record-title {
        color: @text-normal-color;
        font-size: 14px;
        line-height: 22px;
        font-weight: @font-medium;
        margin-top: 24px;
      }
    }
    #leftStreamingTableBox {
      width: 100%;
      &.show-steps {
        width: calc(~'100% - 410px');
      }
    }
  }
  .configurations-dialog {
    .error-msg {
      color: @ke-color-danger;
    }
    .is-mul-key .el-input__inner,
    .is-empty .el-input__inner{
      border-color: #E03B3B;
      box-shadow: 0px 0px 0px 2px rgba(218, 8, 8, 0.1);
    }
  }
</style>
