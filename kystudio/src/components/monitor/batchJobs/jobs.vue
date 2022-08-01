<template>
  <div id="jobListPage">
  <el-alert :title="$t('adminTips')" type="tip" class="admin-tips" v-if="isShowAdminTips" @close="closeTips" show-icon></el-alert>
  <div class="jobs_list ksd-mrl-24">
    <div class="ksd-title-page ksd-mt-32">{{$t('jobsList')}}</div>
    <el-row :gutter="20" class="jobs_tools_row ksd-mt-10 ksd-mb-10">
      <el-col :span="16">
        <div class="action_groups ksd-btn-group-minwidth ksd-fleft" v-if="monitorActions.includes('jobActions')">
          <el-button type="primary" text size="medium" icon="el-ksd-icon-play_outline_22" :disabled="!batchBtnsEnabled.resume" @click="batchResume">{{$t('jobResume')}}</el-button>
          <el-button type="primary" text size="medium" icon="el-ksd-icon-resure_22" class="ksd-ml-2" :disabled="!batchBtnsEnabled.restart" @click="batchRestart">{{$t('jobRestart')}}</el-button>
          <el-button type="primary" text size="medium" icon="el-ksd-icon-pause_outline_22" class="ksd-ml-2" :disabled="!batchBtnsEnabled.pause" @click="batchPause">{{$t('jobPause')}}</el-button>
          <el-button type="primary" text size="medium" icon="el-ksd-icon-close_22" class="ksd-ml-2" :disabled="!batchBtnsEnabled.discard" @click="batchDiscard">{{$t('jobDiscard')}}</el-button>
          <el-button type="primary" text size="medium" icon="el-ksd-icon-table_delete_22" class="ksd-ml-2" :disabled="!batchBtnsEnabled.drop" @click="batchDrop">{{$t('jobDrop')}}</el-button>
        </div>
      </el-col>
      <el-col :span="8">
        <el-button type="primary" text size="medium" class="ksd-ml-2 ksd-fright" icon="el-ksd-icon-refresh_22" @click="manualRefreshJobs">{{$t('refreshList')}}</el-button><el-input
        :placeholder="$t('pleaseSearch')" v-model="filter.key" v-global-key-event.enter.debounce="filterChange" @clear="filterChange()" class="show-search-btn ksd-fright" size="medium" prefix-icon="el-icon-search">
        </el-input>
      </el-col>
    </el-row>
    <el-row class="filter-status-list" v-show="filterTags.length">
      <div class="tag-layout">
        <el-tag closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{`${$t(item.source)}${$t('kylinLang.common.colon')}${$t(item.label)}`}}</el-tag><span class="clear-all-tags" @click="handleClearAllTags">{{$t('clearAll')}}</span>
      </div>
      <span class="filter-job-size">{{$t('filteredTotalSize', {totalSize: jobTotal})}}</span>
    </el-row>
    <transition name="fade">
      <div class="selectLabel" v-if="isSelectAllShow&&!$store.state.project.isAllProject&&filter.status.length">
        <span>{{$t('selectedJobs', {selectedNumber: selectedNumber})}}</span>
        <el-checkbox v-model="isSelectAll" @change="selectAllChange">{{$t('selectAll')}}</el-checkbox>
      </div>
    </transition>
    <el-row :gutter="10" id="listBox">
      <el-col :span="showStep?19:24" id="leftTableBox" :class="{'show-steps': showStep}">
        <el-table class="ksd-el-table jobs-table"
          tooltip-effect="dark"
          v-scroll-shadow
          ref="jobsTable"
          :data="jobsList"
          highlight-current-row
          :default-sort = "{prop: 'create_time', order: 'descending'}"
          :empty-text="emptyText"
          @sort-change="sortJobList"
          @selection-change="handleSelectionChange"
          @select="handleSelect"
          @select-all="handleSelectAll"
          @cell-click="showLineSteps"
          :row-class-name="tableRowClassName"
          :key="$store.state.project.isAllProject">
          <el-table-column class-name="icon-column" type="selection" align="center" width="32"></el-table-column>
          <el-table-column class-name="icon-column" align="center" width="32" prop="icon" v-if="monitorActions.includes('jobActions')">
            <template slot-scope="scope">
              <i class="ksd-fs-22" :class="{
                'el-ksd-icon-arrow_table_right_22': scope.row.id !== selectedJob.id || !showStep,
                'el-ksd-icon-arrow_table_down_22': scope.row.id == selectedJob.id && showStep}"></i>
            </template>
          </el-table-column>
          <el-table-column
            :filters="jobTypeFilteArr.map(item => ({text: $t(item), value: item}))"
            :filtered-value="filter.job_names"
            :label="$t('JobType')"
            filter-icon="el-ksd-icon-filter_22"
            :show-multiple-footer="false"
            :filter-change="(v) => filterContent(v, 'job_names')"
            show-overflow-tooltip prop="job_name" width="184">
            <template slot-scope="scope">
              {{$t(scope.row.job_name)}}
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
            :label="$t('TargetSubject')"
            sortable="custom"
            min-width="140"
            prop="target_subject">
            <template slot-scope="scope">
              <p class="target-subject-title" :title="['TABLE_SAMPLING'].includes(scope.row.job_name) || scope.row.target_subject_error ? getTargetSubject(scope.row) : scope.row.target_subject">
                <span :class="{'is-disabled': scope.row.target_subject_error}" v-if="['TABLE_SAMPLING'].includes(scope.row.job_name) || scope.row.target_subject_error">{{getTargetSubject(scope.row)}}</span>
                <common-tip :content="$t('snapshotDisableTips')" v-if="['SNAPSHOT_BUILD', 'SNAPSHOT_REFRESH'].includes(scope.row.job_name) && !scope.row.target_subject_error && !$store.state.project.snapshot_manual_management_enabled">
                  <span class="is-disabled">{{scope.row.target_subject}}</span>
                </common-tip>
                <a class="link" v-if="['SNAPSHOT_BUILD', 'SNAPSHOT_REFRESH'].includes(scope.row.job_name) && $store.state.project.snapshot_manual_management_enabled&&!scope.row.target_subject_error" @click="gotoSnapshotList(scope.row)">{{scope.row.target_subject}}</a>
                <a class="link" v-if="!tableJobTypes.includes(scope.row.job_name)&&!scope.row.target_subject_error" @click="gotoModelList(scope.row)">{{scope.row.target_subject}}</a>
              </p>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('dataRange')"
            min-width="180"
            show-overflow-tooltip>
            <template slot-scope="scope">
              <template v-if="scope.row.job_name !== 'SNAPSHOT_REFRESH' && scope.row.job_name !== 'SNAPSHOT_BUILD'">
                <span v-if="scope.row.data_range_end==9223372036854776000">{{$t('fullLoad')}}</span>
                <span v-else>{{scope.row.data_range_start | toServerGMTDate}} - {{scope.row.data_range_end | toServerGMTDate}}</span>
              </template>
              <template v-else>
                <span v-if="scope.row.snapshot_data_range === 'FULL'">{{$t('fullLoad')}}</span>
                <span v-else-if="scope.row.snapshot_data_range === 'INC'">{{$t('increamLoad')}}</span>
                <span v-else>{{scope.row.snapshot_data_range ? JSON.parse(scope.row.snapshot_data_range).splice(0, 10).join(', ') : ''}}</span>
              </template>
            </template>
          </el-table-column>
          <el-table-column
            width="180"
            :filters="allStatus.map(item => ({text: $t(item), value: item}))" :filtered-value="filter.status" :label="$t('ProgressStatus')" filter-icon="el-ksd-icon-filter_22" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'status')">
            <template slot-scope="scope">
              <kylin-progress :percent="scope.row.step_ratio * 100 | number(0)" :status="scope.row.job_status"></kylin-progress>
            </template>
          </el-table-column>
          <el-table-column
            width="218"
            :label="$t('startTime')"
            show-overflow-tooltip
            prop="create_time"
            sortable="custom">
            <template slot-scope="scope">
              {{transToGmtTime(scope.row.create_time)}}
            </template>
          </el-table-column>
          <el-table-column
            width="140"
            sortable="custom"
            prop="duration"
            :label="$t('totalDuration')">
            <template slot-scope="scope">
              {{scope.row.duration/60/1000 | number(2) }}  mins
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('Actions')"
            v-if="monitorActions.includes('jobActions')"
            class-name="job-fc-icon"
            width="96">
            <template slot-scope="scope">
              <common-tip :content="$t('jobPause')" v-if="(scope.row.job_status=='RUNNING'|| scope.row.job_status=='PENDING')">
                <i class="el-icon-ksd-pause ksd-fs-14" @click.stop="pause([scope.row.id], scope.row.project, '', scope.row)"></i>
              </common-tip><common-tip
              :content="$t('jobResume')" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED'">
                <i class="el-icon-ksd-table_resume ksd-fs-14" @click.stop="resume([scope.row.id], scope.row.project, '', scope.row)"></i>
              </common-tip><common-tip
              :content="$t('jobRestart')" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED' || scope.row.job_status=='RUNNING'">
                <i class="el-icon-ksd-restart ksd-fs-14" @click.stop="restart([scope.row.id], scope.row.project, '', scope.row)"></i>
              </common-tip><common-tip
              :content="$t('jobDiscard')" v-if="scope.row.job_status=='PENDING'">
                <i class="el-icon-ksd-error_02 ksd-fs-14" @click.stop="discard([scope.row.id], scope.row.project, '', scope.row)"></i>
              </common-tip><common-tip
              :content="$t('jobDiagnosis')" v-if="monitorActions.includes('diagnostic') && (scope.row.job_status =='FINISHED' || scope.row.job_status == 'DISCARDED' || scope.row.job_status=='PENDING')">
                <i class="el-icon-ksd-ostin_diagnose ksd-fs-14" @click.stop="showDiagnosisDetail(scope.row.id)"></i>
              </common-tip><common-tip :content="$t('jobDrop')" v-if="scope.row.job_status=='DISCARDED' || scope.row.job_status=='FINISHED'">
                <i class="el-icon-ksd-table_delete ksd-fs-14" @click.stop="drop([scope.row.id], scope.row.project, '', scope.row)"></i>
              </common-tip><common-tip
              :content="$t('kylinLang.common.moreActions')">
                <el-dropdown trigger="click">
                  <span class="el-dropdown-link" @click.stop>
                    <common-tip :content="$t('kylinLang.common.moreActions')">
                      <i class="el-icon-ksd-table_others ksd-fs-16" v-if="scope.row.job_status !=='FINISHED' && scope.row.job_status !== 'DISCARDED' && scope.row.job_status!=='PENDING'"></i>
                    </common-tip>
                  </span>
                  <el-dropdown-menu slot="dropdown">
                    <el-dropdown-item @click.native="discard([scope.row.id], scope.row.project, '', scope.row)" v-if="(scope.row.job_status=='RUNNING' || scope.row.job_status=='ERROR' || scope.row.job_status=='STOPPED')">{{$t('jobDiscard')}}</el-dropdown-item>
                    <el-dropdown-item @click.native="showDiagnosisDetail(scope.row.id)" v-if="monitorActions.includes('diagnostic')">{{$t('jobDiagnosis')}}</el-dropdown-item>
                  </el-dropdown-menu>
                </el-dropdown>
              </common-tip>
            </template>
          </el-table-column>
        </el-table>
        <kylin-pager :totalSize="jobTotal" :curPage="filter.page_offset+1"  v-on:handleCurrentChange='currentChange' ref="jobPager" :refTag="pageRefTags.jobPager" class="ksd-mtb-16 ksd-center" ></kylin-pager>
      </el-col>
      <el-col :span="5" v-if="showStep" id="rightDetail">
        <el-card v-show="showStep" class="card-width job-step" :class="{'is-admin-tips': $store.state.user.isShowAdminTips&&isAdminRole}" id="stepList">
          <div class="timeline-item">
            <div class="timeline-body">
              <div class="ksd-list">
                <p class="list">
                  <span class="label">{{$t('jobId')}}</span>
                  <span class="text single-line greyd0">
                    {{selectedJob.id}}
                  </span>
                </p>
                <p class="list">
                  <span class="label">{{$t('TargetSubject')}}: </span>
                  <span class="text">
                    <span class="is-disabled" v-if="['TABLE_SAMPLING'].includes(selectedJob.job_name) || selectedJob.target_subject_error">{{getTargetSubject(selectedJob)}}</span>
                    <common-tip :content="$t('snapshotDisableTips')" v-if="['SNAPSHOT_BUILD', 'SNAPSHOT_REFRESH'].includes(selectedJob.job_name) && !selectedJob.target_subject_error && !$store.state.project.snapshot_manual_management_enabled">
                      <span class="is-disabled">{{selectedJob.target_subject}}</span>
                    </common-tip>
                    <a class="link" v-if="['SNAPSHOT_BUILD', 'SNAPSHOT_REFRESH'].includes(selectedJob.job_name) && $store.state.project.snapshot_manual_management_enabled&&!selectedJob.target_subject_error" @click="gotoSnapshotList(selectedJob)">{{selectedJob.target_subject}}</a>
                    <a class="link" v-if="!tableJobTypes.includes(selectedJob.job_name)&&!selectedJob.target_subject_error" @click="gotoModelList(selectedJob)">{{selectedJob.target_subject}}</a>
                  </span>
                </p>
                <p class="list">
                  <span class="label">{{$t('kylinLang.common.status')}}: </span>
                  <span class="text">
                    <el-tag
                      size="small"
                      :type="getJobStatusTag">
                      {{selectedJob.job_status}}
                    </el-tag>
                  </span>
                </p>
                <p class="list">
                  <span class="label">{{$t('waiting')}}: </span>
                  <span class="text">{{formatTime(selectedJob.wait_time)}}</span>
                </p>
                <p class="list">
                  <span class="label">{{$t('duration')}}: </span>
                  <span class="text greyd0">{{formatTime(selectedJob.duration)}}</span>
                </p>
              </div>
            </div>
          </div>
          <p class="time-hd">
            {{$t('jobDetails')}}
          </p>
          <ul class="timeline">

            <li v-for="(step, index) in selectedJob.details" :key="index" :class="{'finished' : step.step_status=='FINISHED'}">
              <el-tooltip placement="bottom" :content="getStepStatusTips(step.step_status)">
                <i class="fa"
                  v-if="step.step_status=='SKIP' || step.step_status === 'ERROR_STOP'"
                  :class="{
                    'el-ksd-icon-skip_16' : step.step_status=='SKIP',
                    'el-ksd-icon-wrong_fill_16': step.step_status === 'ERROR_STOP'
                  }">
                </i>
                <el-icon v-else :class="['job-status', {'is-running': step.step_status=='RUNNING'}]" :name="getStatusIcons(step)" type="mult"></el-icon>
              </el-tooltip>
              <div class="timeline-item timer-line">
                <div class="timeline-header ">
                  <p class="stepname single-line">
                    <span>{{getStepLineName(step.name)}}</span>
                    <common-tip :content="$t('logInfoTip')">
                      <i name="file" v-if="step.step_status!='PENDING'" class="action-icon el-ksd-icon-log_16 ksd-fs-16 ksd-mr-4 ksd-mb-2" @click="clickFile(step)"></i>
                    </common-tip>
                    <common-tip :content="$t('jobParams')">
                      <i v-if="step.info && 'job_params' in step.info" class="action-icon el-ksd-icon-controller_16 ksd-fs-16 ksd-mr-4 ksd-mb-2" @click="showJobParams(step)"></i>
                    </common-tip>
                    <common-tip :content="$t('sparkJobTip')" v-if="step.info">
                      <a :href="step.info.yarn_application_tracking_url" target="_blank" v-if="!$store.state.config.platform">
                          <i name="tasks" v-if="step.info.yarn_application_tracking_url" class="el-ksd-icon-export_16 ksd-fs-16 ksd-mr-4 ksd-mb-4"></i>
                      </a>
                    </common-tip>
                  </p>
                </div>
                <div class="timeline-body">
                  <el-alert class="ksd-mb-8" type="error" show-icon v-if="step.step_status === 'ERROR'" :closable="false">
                    <p slot="title">
                      <p class="err-msg">{{getErrorReson(step)}}</p>
                      <el-button nobg-text size="small" v-if="step.failed_stack" @click="viewErrorDetails(step)">{{$t('viewDetails')}}</el-button>
                    </p>
                  </el-alert>
                  <el-alert class="ksd-mb-8" type="tip" show-icon v-if="step.step_status !== 'ERROR' && 'segment_sub_stages' in step && step.segment_sub_stages && Object.keys(step.segment_sub_stages).length > 1" :closable="false">
                    <p slot="title">{{$t('buildSegmentTips', {segments: Object.keys(step.segment_sub_stages).length, successLen: getSegmentStatusLen(step, 'FINISHED'), pendingLen: getSegmentStatusLen(step, 'PENDING'), runningLen: getSegmentStatusLen(step, 'RUNNING')})}}
                      <el-button nobg-text size="small" @click="viewSegmentDetails(step, step.id)">{{$t('viewDetails')}}</el-button>
                    </p>
                  </el-alert>

                  <div v-if="step.info && step.info.hdfs_bytes_written">
                    <span class="jobActivityLabel">Data Size: </span>
                    <span>{{step.info.hdfs_bytes_written|dataSize}}</span>
                  </div>
                  <div>
                    <span class="jobActivityLabel">{{$t('waiting')}}: </span>
                    <span v-if="step.wait_time">{{formatTime(step.wait_time)}}</span>
                    <span v-else>0</span>
                  </div>
                  <div>
                    <span class="jobActivityLabel">{{$t('duration')}}: </span>
                    <span v-if="step.duration">{{formatTime(step.duration)}}</span>
                    <span v-else>0</span>
                    <el-tooltip placement="bottom" triggle="hover">
                      <div slot="content">
                        <div><span>{{$t('durationStart')}}</span><span>{{step.exec_start_time!== 0 ? transToGmtTime(step.exec_start_time) : '-'}}</span></div>
                        <div><span>{{$t('durationEnd')}}</span><span>{{step.exec_end_time!== 0 ? transToGmtTime(step.exec_end_time) : '-'}}</span></div>
                      </div>
                      <span class="duration-details" v-show="step.step_status !== 'PENDING'">{{$t('durationDetails')}}</span>
                    </el-tooltip>
                  </div>
                  <div>
                    <span class="active-nodes jobActivityLabel">{{$t('jobNodes')}}: </span>
                    <span v-if="step.info">{{step.info.node_info || $t('unknow')}}</span>
                    <br />
                  </div>
                  <div class="sub-tasks" v-if="'sub_stages' in step && step.sub_stages && step.sub_stages.length > 0">
                    <ul v-for="sub in step.sub_stages" :key="sub.id">
                      <li>
                        <el-tooltip placement="bottom" :content="getStepStatusTips(step.step_status === 'STOPPED' && sub.step_status !== 'SKIP' ? 'STOPPED' : sub.step_status)">
                          <span :class="[step.step_status === 'STOPPED' && sub.step_status !== 'FINISHED' && sub.step_status !== 'SKIP' ? 'sub-tasks-status is-stop' : getSubTaskStatus(sub)]"><i class="el-icon-loading" v-if="sub.step_status === 'RUNNING'"></i></span>
                        </el-tooltip>
                        <span class="sub-tasks-name">{{getSubTasksName(sub.name)}}</span>
                        <span class="sub-tasks-layouts" v-if="sub.name === 'Build indexes by layer'"><span class="success-layout-count">{{sub.success_index_count}}</span>{{`/${sub.index_count}`}}</span>
                      </li>
                      <li><span class="list-details" v-if="sub.duration">{{$t('duration')}}: {{formatTime(sub.duration)}}</span><span class="list-details" v-else>{{$t('duration')}}: -</span></li>
                    </ul>
                  </div>
                </div>
              </div>
            </li>
          </ul>
          <div class='job-btn' id="jobDetailBtn" v-show="isShowBtn" :class="{'is-filter-list': filterTags.length}" @click='showStep=false'><i class='el-ksd-icon-arrow_right_16' aria-hidden='true'></i>
          </div>
        </el-card>
      </el-col>
    </el-row>
    <el-dialog
      id="show-diagnos"
      width="600px"
      limited-area
      :title="stepAttrToShow == 'cmd' ? $t('parameters') : $t('output')"
      :visible.sync="dialogVisible"
      :close-on-press-escape="false"
      :close-on-click-modal="false">
      <job_dialog :stepDetail="outputDetail" :stepId="stepId" :jobId="selectedJob.id" :targetProject="selectedJob.project"></job_dialog>
      <span slot="footer" class="dialog-footer">
      </span>
    </el-dialog>
    <diagnostic v-if="showDiagnostic" @close="showDiagnostic = false" :jobId="diagnosticId"/>
    <!-- segment 构建步骤详情 -->
    <el-dialog
      :visible="true"
      width="600px"
      @close="closeSegmentDetail"
      v-if="showSegmentDetails"
      :close-on-click-modal="false">
      <span slot="title">{{$t('segmentDetail')}}</span>
      <build-segment-detail :segmentTesks="segmentDetails" :jobStatus="currentJobStatus" />
      <span slot="footer" class="dialog-footer">
      </span>
    </el-dialog>
    <job-error-detail v-if="showErrorDetails" @close="showErrorDetails = false" :currentErrorJob="currentErrorJob"/>
  </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations } from 'vuex'
import locales from './locales'
import jobDialog from '../job_dialog'
import TWEEN from '@tweenjs/tween.js'
import $ from 'jquery'
import { pageRefTags, bigPageCount } from '../../../config'
import { transToGmtTime, handleError, handleSuccess } from '../../../util/business'
import { cacheLocalStorage, indexOfObjWithSomeKey, objectClone, transToServerGmtTime } from '../../../util/index'
import Diagnostic from 'components/admin/Diagnostic/index'
import BuildSegmentDetail from './buildSegmentDetail.vue'
import jobErrorDetail from './jobErrorDetail.vue'
import { getSubTasksName, getSubTaskStatus, formatTime, getStepStatusTips, getStepLineName } from './handler'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    getSubTaskStatus: getSubTaskStatus,
    formatTime: formatTime,
    ...mapActions({
      loadJobsList: 'LOAD_JOBS_LIST',
      getJobDetail: 'GET_JOB_DETAIL',
      loadStepOutputs: 'LOAD_STEP_OUTPUTS',
      removeJob: 'REMOVE_JOB',
      removeJobForAll: 'ROMOVE_JOB_FOR_ALL',
      pauseJob: 'PAUSE_JOB',
      restartJob: 'RESTART_JOB',
      resumeJob: 'RESUME_JOB',
      discardJob: 'DISCARD_JOB'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapMutations({
      setProject: 'SET_PROJECT'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'monitorActions',
      'isAdminRole'
    ])
  },
  components: {
    'job_dialog': jobDialog,
    Diagnostic,
    BuildSegmentDetail,
    jobErrorDetail
  },
  locales
})

export default class JobsList extends Vue {
  pageRefTags = pageRefTags
  getSubTasksName = (name) => getSubTasksName(this, name)
  getStepStatusTips = (status) => getStepStatusTips(this, status)
  getStepLineName = (name) => getStepLineName(this, name)
  project = localStorage.getItem('selected_project')
  filterName = ''
  filterStatus = []
  lockST = null
  scrollST = null
  stCycle = null
  showStep = false
  selectedJob = {}
  dialogVisible = false
  outputDetail = ''
  stepAttrToShow = ''
  beforeScrollPos = 0
  multipleSelection = []
  isPausePolling = false
  isSelectAllShow = false
  isSelectAll = false
  selectedNumber = 0
  idsArr = []
  idsArrCopy = []
  filter = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.jobPager) || bigPageCount,
    time_filter: 4,
    job_names: [],
    sort_by: 'create_time',
    reverse: true,
    status: [],
    key: '',
    isAuto: false
  }
  showSegmentDetails = false
  showErrorDetails = false
  segmentDetails = {}
  expandSegmentDetailId = ''
  currentJobStatus = ''
  currentErrorJob = {}
  waittingJobsFilter = {
    offset: 0,
    limit: 10
  }
  jobsList = []
  jobTotal = 0
  allStatus = ['PENDING', 'RUNNING', 'FINISHED', 'ERROR', 'DISCARDED', 'STOPPED']
  jobTypeFilteArr = ['INDEX_REFRESH', 'INDEX_MERGE', 'INDEX_BUILD', 'INC_BUILD', 'TABLE_SAMPLING', 'SNAPSHOT_BUILD', 'SNAPSHOT_REFRESH', 'SUB_PARTITION_BUILD', 'SUB_PARTITION_REFRESH']
  tableJobTypes = ['TABLE_SAMPLING', 'SNAPSHOT_BUILD', 'SNAPSHOT_REFRESH']
  targetId = ''
  searchLoading = false
  batchBtnsEnabled = {
    resume: false,
    restart: false,
    pause: false,
    discard: false,
    drop: false
  }
  waitingJobListVisibel = false
  waitingJob = {modelName: '', jobsList: [], jobsSize: 0}
  waittingJobModels = {size: 0, data: null}
  stepId = ''
  filterTags = []
  showDiagnostic = false
  diagnosticId = ''
  isShowBtn = true

  get emptyText () {
    return this.filter.key || this.filter.job_names.length || this.filter.status.length ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  get isShowAdminTips () {
    return this.$store.state.user.isShowAdminTips && this.isAdminRole && !this.$store.state.system.isShowGlobalAlter
  }

  getErrorReson (step) {
    return `${step.failed_code ?? ''}${step.failed_reason ?? (step.failed_step_name ? this.$t('errorStepTips', {name: this.getSubTasksName(step.failed_step_name) || this.getStepLineName(step.name)}) : this.$t('errorStepTips', {name: this.getStepLineName(step.name)}))}`
  }

  @Watch('$store.state.project.isAllProject')
  selectAllProject (curVal) {
    if (curVal) {
      delete this.filter.project
      this.jobsList = []
      this.$nextTick(() => {
        this.loadJobsList(this.filter)
      })
    }
  }
  closeTips () {
    this.$store.state.user.isShowAdminTips = false
    cacheLocalStorage('isHideAdminTips', true)
  }
  handleCommand (uuid) {
    this.waitingJobListVisibel = true
    this.waittingJobsFilter.project = this.currentSelectedProject
    this.waittingJobsFilter.model = uuid
    this.waitingJob.modelName = this.waittingJobModels.data[uuid].model_alias
  }

  getSegmentStatusLen (step, type) {
    const { segment_sub_stages } = step
    const segmentTasks = Object.values(segment_sub_stages)
    switch (type) {
      case 'FINISHED':
        return segmentTasks.filter(it => it.stage.length === it.stage.filter(item => item.step_status === 'FINISHED' || item.step_status === 'SKIP').length).length
      case 'PENDING':
        return segmentTasks.filter(it => it.stage.length === it.stage.filter(item => item.step_status === 'PENDING' || item.step_status === 'SKIP').length).length
      case 'RUNNING':
        return segmentTasks.filter(it => it.stage.filter(item => item.step_status === 'RUNNING').length > 0).length
      default:
        return ''
    }
  }

  // segment 构建详情
  viewSegmentDetails (step, id) {
    this.showSegmentDetails = true
    this.segmentDetails = this.definitionStopErrorStatus(step)
    this.expandSegmentDetailId = id
    this.currentJobStatus = step.step_status
  }

  closeSegmentDetail () {
    this.showSegmentDetails = false
    this.expandSegmentDetailId = ''
  }

  getStatusIcons (step) {
    const status = {
      'PENDING': 'el-ksd-icon-pending_16',
      'STOPPED': 'el-ksd-icon-stopped_16',
      'WAITING,RUNNING': 'el-ksd-icon-running_16',
      'FINISHED': 'el-ksd-icon-finished_16',
      'ERROR': 'el-ksd-icon-error_16',
      'DISCARDED': 'el-ksd-icon-discarded_16'
    }
    const currentStatus = Object.keys(status).filter(item => item.split(',').includes(step.step_status))
    if (currentStatus.length > 0) {
      return status[currentStatus[0]]
    } else {
      return ''
    }
  }

  // 展示任务参数
  showJobParams (step) {
    const { job_params } = step.info
    if (!job_params) return
    try {
      const jobParams = JSON.parse(job_params)
      const data = Object.keys(jobParams).map(item => ({key: item, value: jobParams[item]}))
      this.$msgbox({
        width: '600px',
        message: <el-table data={data} ref="jobParamRef" style="width: 100%" key={step.id} class="job-param-table" height="300">
          <el-table-column width="300" label={this.$t('paramKey')}>
            {
              scope => <div class="params-item" title={scope.row.key}><span class="params-value">{scope.row.key}</span></div>
            }
          </el-table-column>
          <el-table-column label={this.$t('paramValue')}>
            {
              scope => <div class="params-item" title={scope.row.value}><span class="params-value">{scope.row.value}</span></div>
            }
          </el-table-column>
        </el-table>,
        title: this.$t('jobParams'),
        showConfirmButton: false,
        dangerouslyUseHTMLString: true,
        closeOnClickModal: false,
        closeOnPressEscape: false
      })
      setTimeout(() => {
        this.$refs.jobParamRef && this.$refs.jobParamRef.doLayout()
      }, 100)
    } catch (e) {
      console.error(e)
      return
    }
  }

  getTargetSubject (row) {
    if (row.target_subject === 'The snapshot is deleted') {
      return this.$t('snapshotIsDeleted')
    } else if (row.target_subject === 'The model is deleted') {
      return this.$t('modelIsDeleted')
    } else {
      return row.target_subject
    }
  }
  getBatchBtnStatus (statusArr) {
    const batchBtns = {
      resume: ['ERROR', 'STOPPED'],
      restart: ['ERROR', 'STOPPED', 'RUNNING'],
      pause: ['PENDING', 'RUNNING'],
      discard: ['PENDING', 'RUNNING', 'ERROR', 'STOPPED'],
      drop: ['DISCARDED', 'FINISHED']
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
  gotoModelList (item) {
    // 暂停轮询，清掉计时器
    clearTimeout(this.stCycle)
    this.isPausePolling = true
    // 如果是全 project 模式，需要先改变当前 project 选中值
    if (this.$store.state.project.isAllProject) {
      this.setProject(item.project)
    }
    this.$router.push({name: 'ModelList', params: { modelAlias: item.target_subject }})
  }
  gotoSnapshotList (item) {
    // 暂停轮询，清掉计时器
    clearTimeout(this.stCycle)
    this.isPausePolling = true
    // 如果是全 project 模式，需要先改变当前 project 选中值
    if (this.$store.state.project.isAllProject) {
      this.setProject(item.project)
    }
    this.$router.push({name: 'Snapshot', params: { table: item.target_subject }})
  }
  // 清除所有的tags
  handleClearAllTags () {
    this.filter.page_offset = 0
    this.filter.job_names.splice(0, this.filter.job_names.length)
    this.filter.status.splice(0, this.filter.status.length)
    this.filterTags = []
    this.manualRefreshJobs()
  }
  autoFilter () {
    if (this.stCycle) {
      this.filter.isAuto = true
      // this.waitingFilter.isAuto = true
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
  created () {
    const { modelAlias, jobStatus } = this.$route.query
    modelAlias && (this.filter.subject = modelAlias)
    jobStatus && (this.filter.status = jobStatus)
    this.selectedJob = {} // 防止切换project时，发一个不存在该项目jobId的jobDetail的请求
    this.filter.project = this.currentSelectedProject
    if (this.currentSelectedProject) {
      this.autoFilter()
      this.getJobsList()
    }
  }
  destroyed () {
    clearTimeout(this.stCycle)
  }
  mounted () {
    if (document.getElementById('scrollContent')) {
      document.getElementById('scrollContent').addEventListener('scroll', this.isShowJobBtn, false)
    }
  }
  beforeDestroy () {
    window.clearTimeout(this.stCycle)
    window.clearTimeout(this.scrollST)
    if (document.getElementById('scrollContent')) {
      document.getElementById('scrollContent').removeEventListener('scroll', this.isShowJobBtn, false)
    }
  }
  getJobsList () {
    return new Promise((resolve, reject) => {
      if (!this.currentSelectedProject) return reject()
      let data = {}
      const statuses = this.filter.status.join(',')
      Object.keys(this.filter).forEach(key => key !== 'status' && (data[key] = this.filter[key]))
      this.loadJobsList({...data, statuses}).then((res) => {
        handleSuccess(res, (data) => {
          if (data.total_size) {
            this.jobsList = data.value
            if (this.selectedJob) {
              const selectedIndex = indexOfObjWithSomeKey(this.jobsList, 'id', this.selectedJob.id)
              if (selectedIndex !== -1) {
                this.getJobDetail({project: this.selectedJob.project, job_id: this.selectedJob.id}).then((res) => {
                  handleSuccess(res, (data) => {
                    const result = data.map(it => ({...it, sub_stages: it.sub_stages ?? [], segment_sub_stages: it.segment_sub_stages ?? []}))
                    this.selectedJob = this.jobsList[selectedIndex]
                    this.selectedJob['details'] = result.map(item => ({...item, step_status: this.exChangeStatus(item, result), sub_stages: item.sub_stages && item.sub_stages.length > 0 ? this.definitionSubTaskStopErrorStatus(item.sub_stages, result) : item.sub_stages}))
                    if (this.expandSegmentDetailId) {
                      const jobDetail = result.filter(it => it.id === this.expandSegmentDetailId)
                      const detail = this.definitionStopErrorStatus(jobDetail[0])
                      this.segmentDetails = detail
                      this.currentJobStatus = jobDetail[0].step_status
                    }
                  })
                }, (resError) => {
                  handleError(resError)
                })
              }
            }
            if (this.multipleSelection.length) {
              const cloneSelections = objectClone(this.multipleSelection)
              this.multipleSelection = []
              cloneSelections.forEach((m) => {
                const index = indexOfObjWithSomeKey(this.jobsList, 'id', m.id)
                if (index !== -1) {
                  this.$nextTick(() => {
                    this.$refs.jobsTable.toggleRowSelection(this.jobsList[index])
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

  // 大步骤状态 ERROR_STOP 转换
  exChangeStatus (currentStep, steps) {
    const hasErrorTask = steps.filter(it => it.step_status === 'ERROR')
    if (hasErrorTask.length && !['DISCARDED', 'ERROR', 'FINISHED', 'STOPPED', 'SKIP'].includes(currentStep.step_status)) {
      return 'ERROR_STOP'
    } else {
      if (this.selectedJob.job_status === 'STOPPED') {
        return 'STOPPED'
      }
      return currentStep.step_status
    }
  }

  // 非多 segment 时，子步骤状态处理
  definitionSubTaskStopErrorStatus (subTask, data) {
    const tasks = subTask
    const hasErrorSteps = data.filter(it => it.step_status === 'ERROR')
    const hasErrorTask = tasks.filter(it => it.step_status === 'ERROR')
    if (hasErrorTask.length || hasErrorSteps.length) {
      tasks.forEach(tk => {
        if (!['DISCARDED', 'ERROR', 'FINISHED', 'STOPPED', 'SKIP'].includes(tk.step_status)) {
          tk.step_status = 'ERROR_STOP'
        }
      })
    }
    return tasks
  }

  // segment 步骤详情状态处理
  definitionStopErrorStatus (steps) {
    const segmentSteps = steps.segment_sub_stages
    const hasErrorSegments = Object.keys(segmentSteps).filter(it => segmentSteps[it].stage.filter(item => item.step_status === 'ERROR').length > 0)
    if (hasErrorSegments.length) {
      Object.keys(segmentSteps).forEach(seg => {
        segmentSteps[seg].stage = segmentSteps[seg].stage.map(it => ({...it, step_status: !['DISCARDED', 'ERROR', 'FINISHED', 'STOPPED', 'SKIP'].includes(it.step_status) ? 'ERROR_STOP' : it.step_status}))
      })
    }
    return segmentSteps
  }

  get getJobStatusTag () {
    if (this.selectedJob.job_status === 'PENDING') {
      return 'gray'
    }
    if (this.selectedJob.job_status === 'RUNNING') {
      return ''
    }
    if (this.selectedJob.job_status === 'FINISHED') {
      return 'success'
    }
    if (this.selectedJob.job_status === 'ERROR') {
      return 'danger'
    }
    if (this.selectedJob.job_status === 'DISCARDED') {
      return 'info'
    }
    if (this.selectedJob.job_status === 'STOPPED') {
      return ''
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
  animatedNum (newValue, oldValue) {
    new TWEEN.Tween({
      number: oldValue
    }).to({
      number: newValue
    }, 500).onUpdate(tween => {
      this.selectedNumber = tween.number.toFixed(0)
    }).start()
    function animate () {
      if (TWEEN.update()) {
        requestAnimationFrame(animate)
      }
    }
    animate()
  }
  reCallPolling () {
    this.isPausePolling = false
    this.loadList()
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
        return item.id
      })
    } else {
      this.isPausePolling = false
      this.multipleSelection = []
      this.batchBtnsEnabled = {
        resume: false,
        discard: false,
        pause: false,
        drop: false
      }
      this.idsArr = []
    }
  }
  handleSelectAll (val) {
    if (this.jobTotal > this.filter.page_size && this.filter.status !== '') {
      this.isSelectAllShow = !this.isSelectAllShow
      this.isSelectAll = false
      this.animatedNum(this.jobsList.length, 0)
    }
  }
  handleSelect (val) {
    if (this.jobTotal > this.filter.page_size && this.filter.status !== '') {
      if (this.multipleSelection.length < this.filter.page_size) {
        this.isSelectAllShow = false
      } else {
        this.isSelectAllShow = true
      }
    }
  }
  selectAllChange (val) {
    if (val) {
      this.selectAll()
    } else {
      this.cancelSelectAll()
    }
  }
  selectAll () {
    this.idsArrCopy = this.idsArr
    this.idsArr = []
    this.animatedNum(this.jobTotal, this.jobsList.length)
  }
  cancelSelectAll () {
    this.idsArr = this.idsArrCopy
    this.animatedNum(this.jobsList.length, this.jobTotal)
  }
  getJobIds () {
    const jobIds = this.multipleSelection.map((item) => {
      return item.id
    })
    return jobIds
  }
  getJobNames () {
    const jobNames = this.multipleSelection.map((item) => {
      return item.name
    })
    return jobNames
  }
  batchResume () {
    if (!this.batchBtnsEnabled.resume) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.resume([], this.currentSelectedProject, 'batchAll', null, this.filter.status)
      } else {
        const jobIds = this.getJobIds()
        this.resume(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  batchRestart () {
    if (!this.batchBtnsEnabled.restart) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.restart([], this.currentSelectedProject, 'batchAll', null, this.filter.status)
      } else {
        const jobIds = this.getJobIds()
        this.restart(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  batchPause () {
    if (!this.batchBtnsEnabled.pause) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.pause([], this.currentSelectedProject, 'batchAll', null, this.filter.status)
      } else {
        const jobIds = this.getJobIds()
        this.pause(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  batchDiscard () {
    if (!this.batchBtnsEnabled.discard) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.discard([], this.currentSelectedProject, 'batchAll', null, this.filter.status)
      } else {
        const jobIds = this.getJobIds()
        this.discard(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  batchDrop () {
    if (!this.batchBtnsEnabled.drop) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.drop([], this.currentSelectedProject, 'batchAll', null, this.filter.status)
      } else {
        const jobIds = this.getJobIds()
        this.drop(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  resetSelection () {
    this.isSelectAllShow = false
    this.isSelectAll = false
    this.multipleSelection = []
    this.$refs.jobsTable && this.$refs.jobsTable.clearSelection && this.$refs.jobsTable.clearSelection()
    this.idsArrCopy = []
    this.idsArr = []
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
  filterChange (val) {
    this.searchLoading = true
    this.filter.page_offset = 0
    this.manualRefreshJobs()
    this.showStep = false
  }
  tableRowClassName ({row, rowIndex}) {
    if (row.id === this.selectedJob.id && this.showStep) {
      return 'current-row2'
    }
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
  async resume (jobIds, project, isBatch, row, status) {
    // 全选时不显示具体选中的joblist
    const targetJobs = row ? [row] : (isBatch && isBatch === 'batchAll') ? [] : this.multipleSelection
    const msg = this.$t('resumeJob', {count: (isBatch && isBatch === 'batchAll') ? this.selectedNumber : jobIds.length})
    await this.callGlobalDetail(targetJobs, msg, this.$t('resumeJobTitle'), 'tip', this.$t('jobResume'))
    const resumeData = {job_ids: jobIds, project: project, action: 'RESUME'}
    if (this.$store.state.project.isAllProject && isBatch) {
      delete resumeData.project
    }
    this.isSelectAll && status && (resumeData.statuses = status)
    this.resumeJob(resumeData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = []
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  async restart (jobIds, project, isBatch, row, status) {
    // 全选时不显示具体选中的joblist
    const targetJobs = row ? [row] : (isBatch && isBatch === 'batchAll') ? [] : this.multipleSelection
    const msg = this.$t('restartJob', {count: (isBatch && isBatch === 'batchAll') ? this.selectedNumber : jobIds.length})
    await this.callGlobalDetail(targetJobs, msg, this.$t('restartJobTitle'), 'tip', this.$t('jobRestart'))
    const restartData = {job_ids: jobIds, project: project, action: 'RESTART'}
    if (this.$store.state.project.isAllProject && isBatch) {
      delete restartData.project
    }
    this.isSelectAll && status && (restartData.statuses = status)
    this.restartJob(restartData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = []
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  async pause (jobIds, project, isBatch, row, status) {
    // 全选时不显示具体选中的joblist
    const targetJobs = row ? [row] : (isBatch && isBatch === 'batchAll') ? [] : this.multipleSelection
    const msg = this.$t('pauseJob', {count: (isBatch && isBatch === 'batchAll') ? this.selectedNumber : jobIds.length})
    await this.callGlobalDetail(targetJobs, msg, this.$t('pauseJobTitle'), 'tip', this.$t('jobPause'))
    const pauseData = {job_ids: jobIds, project: project, action: 'PAUSE'}
    if (this.$store.state.project.isAllProject && isBatch) {
      delete pauseData.project
    }
    this.isSelectAll && status && (pauseData.statuses = status)
    this.pauseJob(pauseData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = []
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  async discard (jobIds, project, isBatch, row, status) {
    let isHaveHoleWarning = false
    // 全选时不显示具体选中的joblist
    const targetJobs = row ? [row] : (isBatch && isBatch === 'batchAll') ? [] : this.multipleSelection
    targetJobs.forEach((job) => {
      if (!job.discard_safety) {
        isHaveHoleWarning = true
      }
    })
    const msg = isHaveHoleWarning ? this.$t('discardJobWarning') : this.$t('discardJob')
    await this.callGlobalDetail(targetJobs, msg, this.$t('discardJobTitle'), 'warning', this.$t('jobDiscard'), true)
    const pauseData = {job_ids: jobIds, project: project, action: 'DISCARD'}
    if (this.$store.state.project.isAllProject && isBatch) {
      delete pauseData.project
    }
    this.isSelectAll && status && (pauseData.statuses = status)
    this.discardJob(pauseData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = []
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  async callGlobalDetail (targetJobs, msg, title, type, submitText, isShowHighlight) {
    const tableData = []
    targetJobs.forEach((job) => {
      const obj = {}
      obj['job_name'] = this.$t(job.job_name)
      obj['target_subject'] = this.getTargetSubject(job)
      obj['data_range'] = job.data_range_end === 9223372036854776000 ? this.$t('fullLoad') : transToServerGmtTime(job.data_range_start) + '-' + transToServerGmtTime(job.data_range_end)
      obj['highlight'] = isShowHighlight && !job.discard_safety
      tableData.push(obj)
    })
    await this.callGlobalDetailDialog({
      msg: msg,
      title: title,
      detailTableData: tableData,
      detailColumns: [
        {column: 'job_name', label: this.$t('JobType')},
        {column: 'target_subject', label: this.$t('TargetSubject')},
        {column: 'data_range', label: this.$t('dataRange'), minWidth: '180'}
      ],
      dialogType: type,
      showDetailBtn: false,
      submitText: submitText
    })
  }
  async drop (jobIds, project, isBatch, row, status) {
    // 全选时不显示具体选中的joblist
    const targetJobs = row ? [row] : (isBatch && isBatch === 'batchAll') ? [] : this.multipleSelection
    const msg = this.$t('dropJob', {count: (isBatch && isBatch === 'batchAll') ? this.selectedNumber : jobIds.length})
    await this.callGlobalDetail(targetJobs, msg, this.$t('dropJobTitle'), 'warning', this.$t('jobDrop'))
    const dropData = {job_ids: jobIds, project: project}
    let removeJobType = 'removeJob'
    if (this.$store.state.project.isAllProject && isBatch) {
      delete dropData.project
      removeJobType = 'removeJobForAll'
    }
    this.isSelectAll && status && (dropData.statuses = status.join(','))
    this[removeJobType](dropData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = []
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.delSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  showLineSteps (row, column, cell) {
    if (column.property === 'icon') {
      var needShow = false
      if (row.id !== this.selectedJob.id) {
        needShow = true
      } else {
        needShow = !this.showStep
      }
      this.showStep = needShow
      this.selectedJob = row
      this.getJobDetail({project: this.selectedJob.project, job_id: row.id}).then((res) => {
        handleSuccess(res, (data) => {
          this.$nextTick(() => {
            this.$set(this.selectedJob, 'details', data.map(item => ({...item, step_status: this.exChangeStatus(item, data), sub_stages: item.sub_stages && item.sub_stages.length > 0 ? this.definitionSubTaskStopErrorStatus(item.sub_stages, data) : (item.sub_stages ?? []), segment_sub_stages: item.segment_sub_stages ?? []})))
            this.isShowJobBtn()
          })
        }, (resError) => {
          handleError(resError)
        })
      })
    }
  }
  clickFile (step) {
    this.stepAttrToShow = 'output'
    this.dialogVisible = true
    this.outputDetail = this.$t('load')
    this.stepId = step.id
    this.loadStepOutputs({jobId: this.selectedJob.id, stepId: step.id, project: this.selectedJob.project}).then((res) => {
      handleSuccess(res, (data) => {
        this.outputDetail = data.cmd_output
      })
    }).catch((resError) => {
      this.outputDetail = this.$t('cmdOutput')
    })
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      job_names: 'JobType',
      status: 'ProgressStatus'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        this.filterTags.push({label: item, source: maps[type], key: type})
      }
    })
    this.filter[type] = val
    this.filter.page_offset = 0
    this.manualRefreshJobs()
  }
  // 删除单个筛选条件
  handleClose (tag) {
    const index = this.filter[tag.key].indexOf(tag.label)
    index > -1 && this.filter[tag.key].splice(index, 1)
    this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    this.filter.page_offset = 0
    this.manualRefreshJobs()
  }
  showDiagnosisDetail (id) {
    this.diagnosticId = id
    this.showDiagnostic = true
  }
  // 显示错误详情
  viewErrorDetails (step) {
    this.showErrorDetails = true
    this.currentErrorJob = {...step, segment_sub_stages: this.definitionStopErrorStatus(step)}
  }
}
</script>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  .jobs_list {
    #show-diagnos .el-textarea__inner:focus {
      border-color: @line-border-color;
    }
    .jobs_tools_row {
      font-size: 0px;
    }
    .show-search-btn {
      width: 260px;
    }
    .fade-enter-active, .fade-leave-active {
      transition: opacity .5s;
    }
    .fade-enter, .fade-leave-to /* .fade-leave-active below version 2.1.8 */ {
      opacity: 0;
    }
    .selectLabel {
      background-color: @base-color-9;
      height: 32px;
      line-height: 32px;
      margin: 10px 0;
      padding-left: 10px;
      color: @text-title-color;
      .el-checkbox__input {
        font-size: inherit;
        .el-checkbox__inner {
          vertical-align: middle;
        }
      }
      .el-checkbox__label {
        color: @text-title-color;
      }
    }
    .action_groups {
      vertical-align: top;
    }
    .waiting-jobs {
      color: @text-title-color;
      &:hover {
        .el-icon-arrow-down {
          color: @base-color;
        }
      }
      .el-button {
        &:hover {
          background-color: @fff;
        }
        &.is-disabled {
          .el-icon-arrow-down {
            color: inherit;
            cursor: not-allowed;
          }
          &:hover {
            background-color: @background-disabled-color;
            .el-icon-arrow-down {
              color: inherit;
            }
          }
        }
      }
    }
    .el-progress-bar__innerText {
      top: -1px;
      position: relative;
    }
    #rightDetail {
      width: 348px;
      position: relative;
    }
    #leftTableBox {
      width: 100%;
      &.show-steps {
        width: calc(~'100% - 348px');
      }
    }
    .job-step {
      min-height: calc(~'100vh - 167px');
      box-sizing: border-box;
      position: relative;
      overflow: initial;
      .is-disabled {
        color: @text-disabled-color;
      }
      &.is-admin-tips {
        min-height: calc(~'100vh - 181px');
      }
        &.el-card {
          border-radius: 0;
          box-shadow: -3px 3px 5px @ke-color-secondary;
          border: 0;
          padding: 16px;
          .el-card__body {
            padding: 0;
          }
        }
      .table-bordered {
        border: 1px solid @border-color-base;
        tr{
           td {
              border-bottom: 1px solid @border-color-base;
              word-break: break-word;
              a {
                color: @base-color;
                &.link{
                  text-decoration: underline;
                }
              }
            }
          &:last-child {
            td {
              border-bottom: 0;
            }
          }
          &:nth-child(odd) {
            td {
              background-color: @table-stripe-color;
            }
          }
          td:first-child{
            width: 25%;
            font-weight: @font-medium;
            color:@text-normal-color;
          }
        }
      }
      .job-nodes-msg {
        width: calc(100%);
        overflow: hidden;
        text-overflow: ellipsis;
        display: -webkit-box;
        -webkit-line-clamp: 3;
        /*! autoprefixer: off */
        -webkit-box-orient: vertical;
        /* autoprefixer: on */
      }
      .time-hd {
        height:22px;
        line-height:22px;
        margin:16px 0;
        font-size: 14px;
        font-weight: @font-medium;
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
      .timeline {
        position: relative;
        margin: 0px 10px 30px 0px;
        list-style: none;
        font-size: 12px;
        clear: both;

        .job-status {
          font-size: 16px;
          float: left;
          margin-top: 2px;
          &.is-running {
            animation: rotating 2s linear infinite;
            font-size: 15px;
          }
        }

        .jobPoplayer.el-popover[x-placement^=left] .popper__arrow{
          border-left-color: #333;
          &:after{
           border-left-color:#393e53;
         }
        }
        > li.time-label > span {
          padding: 5px;
          display: inline-block;
          border-radius: 4px;
          color: #fff;
        }
        > li {
          &:last-child:before,
          &:last-child:after{
            display: none;
          }
          &.finished:before,
          &.finished:after {
            background: @color-success;
          }
          position: relative;
          margin-right: 10px;
          .timeline-item {
            position: relative;
            margin-left: 20px;
            border-radius: 3px;
            .time {
              float: right;
              padding: 10px;
              color: #999;
              font-size: 12px;
            }
            .timeline-header {
              margin: 0;
              padding: 0 10px 0 0;
              .single-line.stepname {
                max-width: 300px;
                word-wrap: break-word;
                word-break: normal;
                font-size:14px;
                font-weight: @font-medium;
              }
              .action-icon {
                cursor: pointer;
              }
              i {
                color: @text-normal-color;
                &:hover {
                  color: @ke-color-primary;
                }
              }
            }
            .timeline-footer {
              padding: 0 10px 4px 0;
              i {
                font-size: 16px;
                color: @color-primary;
                margin-left: 5px;
                &:first-child {
                  margin-left: 0;
                }
                &:hover {
                  background-color: @base-color-9;
                }
              }
            }
            .timeline-body {
              padding: 8px 0px 16px 0;
              line-height: 1.8;
              .steptime {
                height:20px;
                line-height:20px;
              }
              .jobActivityLabel {
                color: @text-normal-color;
              }
              .sub-tasks {
                &-name {
                  color: @text-normal-color;
                }
                &-status {
                  width: 6px;
                  height: 6px;
                  display: inline-block;
                  border-radius: 100%;
                  margin-right: 8px;
                  &.is-finished {
                    background: @ke-color-success;
                  }
                  &.is-pending {
                    background: @ke-color-info-secondary;
                    position: relative;
                    &::after {
                      content: '';
                      width: 3px;
                      height: 3px;
                      border-radius: 100%;
                      background: #fff;
                      position: absolute;
                      top: 50%;
                      left: 50%;
                      transform: translate(-50%, -50%);
                    }
                  }
                  &.is-error {
                    background: @ke-color-danger;
                  }
                  &.is-error-stop {
                    background: @text-placeholder-color;
                  }
                  &.is-stop {
                    background: @ke-color-primary;
                  }
                }
                &-layouts {
                  color: @text-normal-color;
                  .success-layout-count {
                    color: @ke-color-primary;
                  }
                }
                .running {
                  margin-right: 8px;
                  font-size: 8px;
                  margin-left: -1px;
                  line-height: initial;
                  .el-icon-loading {
                    line-height: normal\0;
                  }
                }
                .list-details {
                  padding-left: 15px;
                  color: @text-disabled-color;
                }
                .icons {
                  transform: scale(0.6);
                  margin-left: -2px;
                  margin-right: 3px;
                  color: #A5B2C5;
                }
              }
              .err-msg {
                display: -webkit-box;
                -webkit-box-orient: vertical;
                -webkit-line-clamp: 3;
                overflow: hidden;
                position: relative;
                word-break: break-all;
              }
              .duration-details {
                color: @ke-color-primary;
                cursor: pointer;
              }
            }
          }
          > span > .fa, > .fa {
            line-height: 20px;
            position: absolute;
            border-radius: 50%;
            text-align: center;
            font-size: 16px;
            color: @color-info;
            &.el-ksd-icon-skip_16 {
              color: @text-normal-color;
            }
            &.el-ksd-icon-wrong_fill_16 {
              color: @ke-color-info-secondary;
            }
            &.el-icon-ksd-good_health {
              color: @ke-color-success-hover;
            }
            &.el-icon-ksd-error_01 {
              color: @ke-color-danger-hover;
            }
            &.el-ksd-icon-loading_22 {
              color: @color-primary;
              font-size: 14px;
            }
          }
        }
        li:last-child {
          position: relative;
        }
      }
    }
    .jobs-table {
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
      span.is-disabled {
        color: @text-disabled-color;
      }
      .link{
        text-decoration: underline;
        color:@base-color;
      }
      .el-ksd-icon-filter_22 {
        // position: relative;
        // font-size: 17px;
        // top: 2px;
        // left: 5px;
        &:hover,
        &.filter-open {
          color: @base-color;
        }
      }
      th .el-dropdown {
        padding: 0;
        line-height: 0;
        position: relative;
        left: 5px;
        top: 2px;
        .el-ksd-icon-filter_22 {
          float: none;
          position: relative;
          left: 0px;
        }
      }
      .el-icon-ksd-dock_to_right_return,
      .el-icon-ksd-dock_to_right,
      .el-icon-ksd-table_delete,
      .el-icon-ksd-restart,
      .el-icon-ksd-table_resume,
      .el-icon-ksd-pause {
        &:hover {
          color: @base-color;
        }
      }
      tr.current-row2 > td{
        background: @base-color-9;
      }
      tr .el-icon-arrow-right, tr .el-icon-arrow-down {
        position:absolute;
        left:15px;
        top:50%;
        transform:translate(0,-50%);
        font-size:12px;
        &:hover{
          color:@base-color;
        }
      }
      tr .el-icon-arrow-down{
        color:@base-color;
      }
      .job-fc-icon {
        .tip_box {
          margin-left: 10px;
          &:first-child {
            margin-left: 0;
          }
        }
      }
      .target-subject-title {
        display: block;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
    }
  }
  .jobs-dropdown {
    min-width: 80px;
    .el-dropdown-menu__item {
      &:focus {
        background-color: inherit;
        color: inherit;
      }
      &.active {
        background-color: @base-color-9;
        color: @base-color-2;
      }
    }
  }
  .filter-status-list {
    margin-bottom: 8px;
    box-sizing: border-box;
    .tag-layout {
      width: calc(~'100% - 80px');
      display: inline-block;
      .el-tag {
        margin-right: 5px;
        margin-top: 5px;
      }
    }
    .clear-all-tags {
      font-size: 12px;
      color: @base-color;
      cursor: pointer;
      margin-left: 8px;
    }
    .filter-job-size {
      position: absolute;
      top: 8px;
      right: 10px;
    }
  }
  .job-param-table {
    .params-item {
      overflow: hidden;
      text-overflow: ellipsis;
      .params-value {
        white-space: nowrap;
      }
    }
  }
</style>
