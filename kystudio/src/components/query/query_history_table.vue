<template>
  <div id="queryHistoryTable">
    <div class="ksd-title-page ksd-mb-16">{{$t('kylinLang.menu.queryhistory')}}</div>
    <div class="clearfix ksd-mb-10">
      <div class="btn-group ksd-fleft export-btn">
        <el-dropdown
          split-button
          class="ksd-fleft"
          :class="{'is-disabled': !queryHistoryTotalSize}"
          type="primary"
          size="medium"
          id="exportSql"
          btn-icon="el-ksd-icon-export_22"
          placement="bottom-start"
          @click="exportHistory(false)">{{$t('kylinLang.query.export')}}
          <el-dropdown-menu slot="dropdown" class="model-actions-dropdown">
            <el-dropdown-item
              :disabled="!queryHistoryTotalSize"
              @click="exportHistory(true)">
              {{$t('kylinLang.query.exportSql')}}
            </el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </div>
      <div class="ksd-fright ksd-inline searchInput ksd-ml-10">
        <el-input v-model="filterData.sql" v-global-key-event.enter.debounce="onSqlFilterChange" @clear="onSqlFilterChange()" prefix-icon="el-ksd-icon-search_22" :placeholder="$t('searchSQL')" size="medium"></el-input>
      </div>
    </div>
    <div class="filter-tags" v-show="filterTags.length">
      <div class="filter-tags-layout"><el-tag closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{$t(item.source) + '：'}}{{['query_status', 'realization'].includes(item.key) ? $t(item.label) : item.label}}</el-tag><span class="clear-all-filters" @click="clearAllTags">{{$t('clearAll')}}</span></div>
      <span class="filter-queries-size">{{$t('filteredTotalSize', {totalSize: queryHistoryTotalSize})}}</span>
    </div>
    <el-table
      :data="queryHistoryData"
      v-scroll-shadow
      class="history-table"
      v-loading="isLoadingHistory"
      :empty-text="emptyText"
      :expand-row-keys="toggleExpandId"
      @expand-change="expandChange"
      :row-key="val => val.query_id"
      ref="queryHistoryTable"
      style="width: 100%">
      <el-table-column type="expand" width="34">
        <template slot-scope="props">
          <!-- <div class="detail-title">
            <span class="ksd-fleft ksd-fs-14">{{$t('queryDetails')}}</span>
          </div> -->
          <div class="detail-content">
            <el-row :gutter="16" type="flex">
              <el-col :span="15" :style="{height: props.row.flexHeight + 'px'}">
                <div class="loading" v-if="currentExpandId === props.row.query_id"><i class="el-icon-loading"></i></div>
                <kylin-editor
                  width="100%"
                  :height="props.row.editorH"
                  lang="sql"
                  theme="chrome"
                  v-if="props.row.editorH"
                  :ref="'historySqlEditor' + props.row.query_id"
                  :key="props.row.query_id"
                  :readOnly="true"
                  :needFormater="true"
                  :dragable="false"
                  :isAbridge="true"
                  :value="props.row.sql_text">
                </kylin-editor>
              </el-col>
              <el-col :span="9">
                <div class="ksd-list history_detail_table" :id="'detailTable_' + props.row.query_id">
                  <p class="list">
                    <span class="label">{{$t('kylinLang.query.query_id')}}:</span>
                    <span class="text">{{props.row.query_id}}</span>
                  </p>
                  <p class="list">
                    <span class="label">{{$t('kylinLang.query.duration')}}:</span>
                    <span class="text">
                      <el-popover
                        placement="bottom"
                        :width="$lang === 'en' ? 400 : 320"
                        v-if="props.row.query_steps.length&&props.row.query_status==='SUCCEEDED'"
                        popper-class="duration-popover"
                        trigger="hover">
                        <el-row v-for="(step, index) in props.row.query_steps" :key="step.name" v-show="!step.group || (step.group === 'PREPARATION' && isShowDetail_PREPARATION) ||  (step.group === 'JOB_EXECUTION' && isShowDetail_JOB_EXECUTION)">
                          <el-col :span="14">
                            <span class="step-name"
                              :class="{'font-medium': index === 0, 'sub-step': ['PREPARATION', 'JOB_EXECUTION'].includes(step.group)}">{{$t(step.name)}}</span>
                            <i class="el-icon-ksd-more_01"
                              :class="{'up': (step.name === 'PREPARATION' && isShowDetail_PREPARATION) ||  (step.group === 'JOB_EXECUTION' && isShowDetail_JOB_EXECUTION)}"
                              v-if="['PREPARATION', 'JOB_EXECUTION'].includes(step.name)"
                              @click.stop="toggleDetail(step.name)"></i>
                          </el-col>
                          <el-col :span="4">
                            <span class="step-duration ksd-fright" :class="{'font-medium': index === 0, 'sub-step': ['PREPARATION', 'JOB_EXECUTION'].includes(step.group)}">{{Math.round(step.duration / 1000 * 100) / 100}}s</span>
                          </el-col>
                          <el-col :span="6" v-if="props.row.query_steps&&props.row.query_steps[0].duration>0">
                            <el-progress :stroke-width="6" :percentage="getProgress(step.duration, props.row.query_steps[0].duration)" color="#A6D6F6" :show-text="false"></el-progress>
                          </el-col>
                        </el-row>
                        <span slot="reference" class="duration">{{Math.round(props.row.duration / 1000 * 100) / 100}}s</span>
                      </el-popover>
                      <span v-else>{{Math.round(props.row.duration / 1000 * 100) / 100}}s</span>
                    </span>
                  </p>
                  <template v-if="props.row.query_status === 'SUCCEEDED'">
                    <p class="list" :class="{'active': props.row.hightlight_realizations}" v-if="!(props.row.engine_type === 'NATIVE'&&!(props.row.realizations && getRealizations2(props.row.realizations).length))">
                      <span class="label">{{$t('kylinLang.query.answered_by')}}:</span>
                      <span class="text">
                        <span class="realization-tags" v-if="props.row.realizations && getRealizations2(props.row.realizations).length">
                          <span v-for="(item, index) in getRealizations2(props.row.realizations)" :key="item.modelId">
                            <template v-if="'visible' in item && !item.visible">
                              <span @click="openAuthorityDialog(item)" class="no-authority-model"><i class="el-icon-ksd-lock"></i>{{item.modelAlias}}</span><span>{{`${index !== getRealizations2(props.row.realizations).length - 1 ? $t('kylinLang.common.comma') : ''}`}}</span>
                            </template>
                            <template v-else>
                              <span @click="openIndexDialog(item, getRealizations2(props.row.realizations))" :class="{'model-tag': item.valid, 'disable': !item.valid || item.indexType === 'Table Snapshot'}">{{item.modelAlias}}</span><span>{{`${index !== getRealizations2(props.row.realizations).length - 1 ? $t('kylinLang.common.comma') : ''}`}}</span>
                            </template>
                          </span>
                        </span>
                        <span v-else class="realization-tags"><el-tag type="warning" size="small" v-if="props.row.engine_type">{{props.row.engine_type}}</el-tag></span>
                      </span>
                    </p>
                    <p class="list" v-if="props.row.realizations && getRealizations(props.row.realizations).length && getLayoutIds(props.row.realizations)">
                      <span class="label">{{$t('kylinLang.query.index_id')}}:</span>
                      <span class="text">
                        <span :class="['realizations-layout-id', {'is-disabled': !item.layoutExist}]" v-for="(item, index) in getRealizations(props.row.realizations)" :key="item.layoutId">
                          <el-tooltip placement="top" :content="$t('unExistLayoutTip')" :disabled="item.layoutExist">
                            <span @click="openLayoutDetails(item)">{{item.layoutId}}</span>
                          </el-tooltip>
                          <el-tag size="mini" v-if="item.streamingLayout" class="ksd-ml-2" style="cursor:default">{{$t('streamingTag')}}</el-tag>
                          <el-tooltip placement="top" :content="$t('secStorage')">
                            <el-icon v-if="item.secondStorage" class="ksd-fs-22" name="el-ksd-icon-tieredstorage_22" type="mult"></el-icon>
                          </el-tooltip><span>{{`${index !== getRealizations(props.row.realizations).length - 1 ? $t('kylinLang.common.comma') : ''}`}}</span>
                        </span>
                      </span>
                    </p>
                    <p class="list" v-if="props.row.realizations && props.row.realizations.length && getSnapshots(props.row.realizations)">
                      <span class="label">{{$t('kylinLang.query.snapshot')}}:</span>
                      <span class="text">{{getSnapshots(props.row.realizations)}}</span>
                    </p>
                    <p class="list">
                      <span class="label">{{$t('kylinLang.query.total_scan_count')}}:</span>
                      <span class="text">{{props.row.total_scan_count | filterNumbers}}</span>
                    </p>
                    <p class="list">
                      <span class="label">{{$t('kylinLang.query.total_scan_bytes')}}:</span>
                      <span class="text">{{props.row.total_scan_bytes | filterNumbers}}</span>
                    </p>
                    <p class="list">
                      <span class="label">{{$t('kylinLang.query.result_row_count')}}:</span>
                      <span class="text">{{props.row.result_row_count | filterNumbers}}</span>
                    </p>
                    <p class="list">
                      <span class="label">{{$t('kylinLang.query.cache_hit')}}:</span>
                      <span class="text">{{props.row.cache_hit}}</span>
                    </p>
                    <p class="list" v-if="props.row.query_history_info && !!props.row.query_history_info.cache_type">
                      <span class="label">{{$t('kylinLang.query.cache_type')}}:</span>
                      <span class="text">{{props.row.query_history_info.cache_type}}</span>
                    </p>
                  </template>
                  <el-alert
                    v-else
                    type="error"
                    :closable="false"
                    class="ksd-mt-8"
                    show-icon>
                    <span slot="title">{{$t('queryError')}}<span class="ky-a-like" @click="openErrorDialog(props.row.query_history_info.query_msg)">{{$t('viewDetails')}}</span></span>
                  </el-alert>
                </div>
              </el-col>
            </el-row>
          </div>
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="query_time" width="218">
        <template slot-scope="props">
          {{transToGmtTime(props.row.query_time)}}
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn2" prop="duration" align="right" width="100">
        <template slot-scope="props">
          <span v-if="props.row.duration < 1000 && props.row.query_status === 'SUCCEEDED'">&lt; 1s</span>
          <span v-if="props.row.duration >= 1000 && props.row.query_status === 'SUCCEEDED'">{{props.row.duration / 1000 | fixed(2)}}s</span>
          <!-- <span v-if="props.row.query_status === 'FAILED'">Failed</span> -->
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.query_id')" prop="query_id" width="120" show-overflow-tooltip>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql_limit" min-width="125">
        <template slot-scope="props">
          <el-popover
            ref="sql-popover"
            placement="top"
            trigger="hover"
            popper-class="col-sql-popover">
            <div class="sql-column" slot="reference" @click="handleExpandType(props)">{{props.row.sql_limit}}</div>
            <template>
              <div class="sql-container">
                <p class="popover-sql-content">{{props.row.sql_limit}}</p>
                <div class="sql-tip" v-if="sqlOverLimit(props.row.sql_limit)">{{$t('sqlDetailTip')}}</div>
              </div>
            </template>
          </el-popover>
        </template>
      </el-table-column>
      <el-table-column
        :filters="realFilteArr"
        :filters2="allHitModels"
        :show-search-input="true"
        :filtered-value="filterData.realization"
        :label="$t('kylinLang.query.realization_th')"
        filter-icon="el-ksd-icon-filter_22"
        :placeholder="$t('searchAnsweredBy')"
        :emptyFilterText="$t('kylinLang.common.noData')"
        :show-multiple-footer="false"
        :filter-change="(v) => filterContent(v, 'realization')"
        :filter-filters-change="(v) => fiterList('loadFilterHitModelsList', v)"
        customFilterClass="filter-realization"
        prop="realizations"
        width="250">
        <template slot-scope="props">
          <div class="tag-ellipsis" :class="{'hasMore': checkIsShowMore(props.row.realizations)}">
            <template v-if="props.row.realizations && getRealizations2(props.row.realizations).length">
              <span v-for="(item, index) in getRealizations2(props.row.realizations)" :key="item.modelId">
                <el-tag :class="{'disabled': 'visible' in item && !item.visible}" v-if="index < checkShowCount(getRealizations2(props.row.realizations))" :type="'visible' in item && !item.visible ? 'info' : item.valid ? 'success' : 'info'" size="small">
                  <i class="el-icon-ksd-lock" v-if="'visible' in item && !item.visible"></i>{{item.modelAlias}}
                </el-tag>
              </span>>
              <a v-if="checkIsShowMore(getRealizations2(props.row.realizations))" href="javascript:;" @click="handleExpandType(props, true)" class="showMore el-tag el-tag--small">{{$t('showDetail', {count: getRealizations2(props.row.realizations).length - checkShowCount(getRealizations2(props.row.realizations))})}}</a>
            </template>
            <template v-else>
              <el-tag type="warning" size="small" v-if="props.row.engine_type&&props.row.engine_type!=='NATIVE'">{{props.row.engine_type}}</el-tag>
            </template>
          </div>
        </template>
      </el-table-column>
      <el-table-column :filters="statusList.map(item => ({text: $t(item), value: item}))" :filtered-value="filterData.query_status" :label="$t('kylinLang.query.query_status')" filter-icon="el-ksd-icon-filter_22" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'query_status')" show-overflow-tooltip prop="query_status" width="130">
        <template slot-scope="scope">
          {{$t('kylinLang.query.' + scope.row.query_status)}}
        </template>
      </el-table-column>
      <el-table-column :filterMultiple="false" :show-all-select-option="false" :filters="queryNodes.map(item => ({text: item, value: item}))" :filtered-value="filterData.server" :label="$t('kylinLang.query.queryNode')" filter-icon="el-ksd-icon-filter_22" :filter-change="(v) => filterContent(v, 'server')"  show-overflow-tooltip prop="server" width="145">
      </el-table-column>
      <el-table-column
        :label="$t('kylinLang.query.submitter')"
        :filters="submitterFilter.map(item => ({text: item, value: item}))"
        :show-search-input="true"
        :filtered-value="filterData.submitter"
        filter-icon="el-ksd-icon-filter_22"
        :show-multiple-footer="false"
        :placeholder="$t('searchSubmitter')"
        :emptyFilterText="$t('kylinLang.common.noData')"
        :filter-change="(v) => filterContent(v, 'submitter')"
        :filter-filters-change="(v) => fiterList('loadFilterSubmitterList', v)"
        customFilterClass="filter-submitter"
        prop="submitter"
        v-if="queryHistoryFilter.includes('filterActions')"
        show-overflow-tooltip
        width="110">
      </el-table-column>
      <el-table-column
        :label="$t('kylinLang.query.submitter')"
        prop="submitter"
        v-if="!queryHistoryFilter.includes('filterActions')"
        show-overflow-tooltip
        width="110">
      </el-table-column>
      <el-table-column
        v-if="(queryHistoryFilter.includes('filterActions') || isNonAdminGenQueryDiagPackage)"
        :label="$t('kylinLang.common.action')"
        width='70'>
        <template slot-scope="scope">
          <common-tip :content="$t('downloadQueryDiagnosticPackage')" >
            <i class="el-icon-ksd-ostin_diagnose ksd-fs-14 ksd-ml-4" @click.stop="openQueryDialog(scope.row)"></i>
          </common-tip>
        </template>
      </el-table-column>
    </el-table>
    <index-details :index-detail-title="indexDetailTitle" :detail-type="detailType" :cuboid-data="cuboidData" @close="closeDetailDialog" v-if="indexDetailShow" />
    <diagnostic
      v-if="showDiagnostic"
      @close="showDiagnostic = false"
      :jobId='queryId'
      :queryServer='queryServer'
      :jobStatus='jobStatus'
    />
    <el-dialog
      :visible.sync="queryErrorVisible"
      width="600px"
      class="query-error-dialog"
      status-icon="el-ksd-icon-error_24"
      :close-on-click-modal="false">
      <span slot="title">{{$t('errorTitle')}}</span>
      <pre class="error-block">{{errinfo}}</pre>
    </el-dialog>
  </div>
</template>

<script>
import { transToGmtTime, getStringLength, handleError } from '../../util/business'
import { handleSuccessAsync } from '../../util'
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
// import $ from 'jquery'
import { sqlRowsLimit, sqlStrLenLimit } from '../../config/index'
import sqlFormatter from 'sql-formatter'
import IndexDetails from '../studio/StudioModel/ModelList/ModelAggregate/indexDetails'
import Diagnostic from 'components/admin/Diagnostic/index'
@Component({
  name: 'QueryHistoryTable',
  props: ['queryHistoryData', 'queryHistoryTotalSize', 'queryNodes', 'filterDirectData', 'isLoadingHistory'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      markFav: 'MARK_FAV',
      fetchHitModelsList: 'FETCH_HIT_MODELS_LIST',
      fetchSubmitterList: 'FETCH_SUBMITTER_LIST',
      loadAllIndex: 'LOAD_ALL_INDEX'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'briefMenuGet',
      'queryHistoryFilter',
      'isNonAdminGenQueryDiagPackage'
    ])
  },
  components: {
    IndexDetails,
    Diagnostic
  },
  locales: {
    'en': {
      queryDetails: 'Query Details',
      ruleDesc: 'Favorite Condition:<br/>Query Frequency (default by daily);<br/>Query Duration;<br/>From user/ user group;<br/>Pushdown Query.',
      searchSQL: 'Search one keyword or query ID',
      noSpaceTips: 'Invalide entering: cannot search space',
      sqlDetailTip: 'Please click sql to get more informations',
      taskStatus: 'Task Status',
      realization: 'Query',
      clearAll: 'Clear All',
      showDetail: 'More {count}',
      SUCCEEDED: 'SUCCEEDED',
      FAILED: 'FAILED',
      pushdown: 'Pushdown',
      modelName: 'Model',
      totalDuration: 'Total Duration',
      PREPARATION: 'Preparation',
      SQL_TRANSFORMATION: 'SQL transformation',
      SQL_PARSE_AND_OPTIMIZE: 'SQL parser optimization',
      HTTP_RECEPTION: 'Reception',
      GET_ACL_INFO: 'ACL Checking',
      MODEL_MATCHING: 'Model matching',
      PREPARE_AND_SUBMIT_JOB: 'Creating and Submitting Spark job',
      WAIT_FOR_EXECUTION: 'Waiting for resources',
      EXECUTION: 'Executing',
      FETCH_RESULT: 'Receiving result',
      SPARK_JOB_EXECUTION: 'Spark Job Execution',
      JOB_EXECUTION: 'Spark Job Execution',
      SQL_PUSHDOWN_TRANSFORMATION: 'SQL pushdown transformation',
      CONSTANT_QUERY: 'Constant query',
      HIT_CACHE: 'Cache hit',
      allModels: 'All Models',
      searchAnsweredBy: 'Search by model name',
      searchSubmitter: 'Search by submitter',
      aggDetailTitle: 'Aggregate Detail',
      tabelDetailTitle: 'Table Index Detail',
      unExistLayoutTip: 'This index has been deleted',
      filteredTotalSize: '{totalSize} result(s)',
      secStorage: 'Tiered Storage',
      streamingTag: 'streaming',
      downloadQueryDiagnosticPackage: 'Download Query Diagnostic Package',
      queryError: 'Query error.',
      viewDetails: 'View Details',
      errorTitle: 'Error Details'
    }
  },
  filters: {
    filterNumbers (num) {
      if (num >= 0) return num
    }
  }
})
export default class QueryHistoryTable extends Vue {
  datetimerange = ''
  startSec = 0
  endSec = 10
  latencyFilterPopoverVisible = false
  realFilteArr = []
  submitterFilter = []
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    submitter: [],
    server: [],
    sql: '',
    query_status: []
  }
  timer = null
  showCopyStatus = false
  currentExpandId = ''
  toggleExpandId = []
  sqlLimitRows = 20 * 10
  statusList = ['SUCCEEDED', 'FAILED']
  filterTags = []
  isShowDetail_PREPARATION = false // 展开查询步骤详情
  isShowDetail_JOB_EXECUTION = false // 展开spark任务执行步骤详情
  cuboidData = {}
  indexDetailTitle = ''
  indexDetailShow = false
  detailType = ''
  showDiagnostic = false
  queryId = ''
  queryServer = ''
  jobStatus = ''
  queryErrorVisible = false
  errinfo = ''

  openErrorDialog (errinfo) {
    this.errinfo = errinfo
    this.queryErrorVisible = true
  }

  @Watch('queryHistoryData')
  onQueryHistoryDataChange (val) {
    val.forEach(element => {
      const sql = element.sql_text
      const sql_limit = this.sqlOverLimit(sql) ? `${sql.slice(0, this.sqlLimitRows)}...` : sql
      const sqlTextArr = sql.split('\n') // 换行符超过一个，说明用户查询行自定义过format格式，则保留
      element['sql_limit'] = sqlTextArr.length > 1 ? sql_limit : sqlFormatter.format(sql_limit)
      element['server'] = [element['server']]
      element['flexHeight'] = 0
      element['editorH'] = 0
      element['query_steps'] = element.query_history_info && this.getStepData(element.query_history_info.traces) || []
    })
    this.toggleExpandId = []
  }

  @Watch('filterDirectData.startTimeFrom')
  onInitFilterData (v) {
    this.initFilterData()
  }

  get isHasFilterValue () {
    return this.filterData.sql || this.filterData.startTimeFrom || this.filterData.startTimeTo || this.filterData.latencyFrom || this.filterData.latencyTo || this.filterData.realization.length || this.filterData.query_status.length || this.filterData.server.length || this.filterData.submitter.length
  }

  get emptyText () {
    return this.isHasFilterValue ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  get allHitModels () {
    return [{text: this.$t('allModels'), value: 'modelName', icon: 'el-icon-ksd-cube'}]
  }

  // 排除击中 snapshot 的查询对象
  getRealizations (row) {
    return row.filter(item => item.indexType !== 'Table Snapshot' && item.layoutId !== 0 && item.layoutId !== -1)
  }

  getRealizations2 (row) {
    return row.filter(item => !((item.layoutId === 0 || item.layoutId === -1 || item.layoutId === null) && item.indexType !== null))
  }

  dateRangeChange () {
    if (this.datetimerange) {
      this.filterData.startTimeFrom = new Date(this.datetimerange[0]).getTime()
      this.filterData.startTimeTo = new Date(this.datetimerange[1]).getTime()
      this.clearDatetimeRange()
      this.filterTags.push({label: `${this.transToGmtTime(this.filterData.startTimeFrom)} To ${this.transToGmtTime(this.filterData.startTimeTo)}`, source: 'kylinLang.query.startTime_th', key: 'datetimerange'})
    } else {
      this.filterData.startTimeFrom = null
      this.filterData.startTimeTo = null
      this.clearDatetimeRange()
    }
  }

  initFilterData () {
    const { startTimeFrom, startTimeTo } = JSON.parse(JSON.stringify(this.filterDirectData))
    if (!startTimeFrom || !startTimeTo) return
    this.datetimerange = [startTimeFrom, startTimeTo]
    this.dateRangeChange()
    this.filterList()
  }

  fiterList (type, filterValue) {
    this.timer = setTimeout(() => {
      this[type](filterValue)
    }, 200)
  }

  async loadFilterHitModelsList (filterValue) {
    try {
      const res = await this.fetchHitModelsList({ project: this.currentSelectedProject, model_name: filterValue, page_size: 100 })
      const data = await handleSuccessAsync(res)
      this.realFilteArr = data.map((d) => {
        if (d === 'HIVE') {
          return { text: d, value: d, icon: 'el-icon-ksd-hive' }
        } else if (d === 'CONSTANTS') {
          return { text: d, value: d, icon: 'el-icon-ksd-contants' }
        } else if (d === 'OBJECT STORAGE') {
          return { text: d, value: d, icon: 'el-icon-ksd-data_source' }
        } else {
          return { text: d, value: d, icon: 'el-icon-ksd-model' }
        }
      })
    } catch (e) {
      handleError(e)
    }
  }

  async loadFilterSubmitterList (filterValue) {
    try {
      const res = await this.fetchSubmitterList({ project: this.currentSelectedProject, submitter: filterValue, page_size: 100 })
      this.submitterFilter = await handleSuccessAsync(res)
    } catch (e) {
      handleError(e)
    }
  }

  created () {
    if (this.queryHistoryFilter.includes('filterActions')) {
      this.loadFilterSubmitterList()
    }
    this.loadFilterHitModelsList() // 普通用户也支持筛选查询对象
  }

  // 清除查询开始事件筛选项
  clearDatetimeRange () {
    if (this.filterTags.filter(item => item.key === 'datetimerange').length) {
      let idx = null
      for (let index in this.filterTags) {
        if (this.filterTags[index].key === 'datetimerange') {
          idx = index
          break
        }
      }
      this.filterTags.splice(idx, 1)
    }
  }

  getProgress (duration, totalDuration) {
    const popoverWidth = this.$lang === 'en' ? 340 : 320
    const progressWidth = (popoverWidth - 30) * 0.25 // 减去padding宽度, span为6
    const miniWidthRat = 0.5 / progressWidth
    const dur = Math.round(duration / 1000 * 100) / 100 // 根据精确度保留两位来计算比例
    let stepRat = Math.round(dur / (totalDuration / 1000) * 100) / 100
    stepRat = stepRat > 1 ? 1 : stepRat // 精度问题导致有大于1的情况
    if (stepRat < miniWidthRat) {
      return miniWidthRat * 100
    } else {
      return stepRat * 100
    }
  }

  getStepData (steps) {
    if (steps && steps.length) {
      let renderSteps = [
        {name: 'totalDuration', duration: 0},
        {name: 'PREPARATION', duration: 0}
      ]
      let preStepNum = 0
      let jobExecutionIndex = -1
      steps.forEach((s) => {
        renderSteps[0].duration = renderSteps[0].duration + s.duration
        if (s.group === 'PREPARATION') {
          preStepNum++
          let preparationIndex = renderSteps.findIndex(item => item.name === 'PREPARATION')
          renderSteps[preparationIndex].duration = renderSteps[preparationIndex].duration + s.duration
          renderSteps.push(s)
        } else if (s.group === 'JOB_EXECUTION') {
          jobExecutionIndex = renderSteps.findIndex(item => item.name === 'JOB_EXECUTION')
          if (jobExecutionIndex === -1) {
            renderSteps.push({name: 'JOB_EXECUTION', duration: 0})
            jobExecutionIndex = renderSteps.length - 1
          }
          renderSteps[jobExecutionIndex].duration = renderSteps[jobExecutionIndex].duration + s.duration
          renderSteps.push(s)
        } else if (s.name === 'HTTP_RECEPTION') {
          renderSteps.splice(1, 0, {name: 'HTTP_RECEPTION', duration: s.duration})
        } else {
          renderSteps.push(s)
        }
      })
      if (preStepNum === 0) {
        renderSteps.splice(1, 1) // 击中缓存没有查询前置步骤
      }
      return renderSteps
    } else {
      return []
    }
  }

  // 清除查询耗时筛选项
  clearLatencyRange () {
    if (this.filterTags.filter(item => item.key === 'latency').length) {
      let idx = null
      for (let index in this.filterTags) {
        if (this.filterTags[index].key === 'latency') {
          idx = index
          break
        }
      }
      idx !== null && this.filterTags.splice(idx, 1)
    }
  }

  // 控制是否显示查看更多
  checkIsShowMore (arr) {
    let str = ''
    if (arr && arr.length) {
      // 如果将全部的模型合并后，超过21个字符，就肯定要显示更多
      for (let i = 0; i < arr.length; i++) {
        let item = arr[i]
        str = str + item.modelAlias
      }
      return arr.length > 1 && getStringLength(str) >= 21
    } else {
      return false
    }
  }

  checkShowCount (arr) {
    let str = ''
    let idx = 1
    if (arr && arr.length) {
      for (let i = 0; i < arr.length; i++) {
        let item = arr[i]
        str = str + item.modelAlias
        // 如果第一个就超21个字符了，就直接返回下标1
        if (i === 0 && getStringLength(str) >= 21) {
          idx = 1
          break
        } else {
          if (arr.length > 1 && getStringLength(str) >= 21) {
            idx = i
            break
          } else { // 如果每次加上名字后，还是可以放得下，就让下标+1，保证都正常渲染
            idx = i + 1
          }
        }
      }
    }
    return idx
  }

  sqlOverLimit (sql) {
    return sql && sql.length > this.sqlLimitRows
  }

  toggleDetail (group) {
    this[`isShowDetail_${group}`] = !this[`isShowDetail_${group}`]
  }

  expandChange (e) {
    this.isShowDetail_PREPARATION = false // 每次展开重置查询详情为隐藏
    this.isShowDetail_JOB_EXECUTION = false
    if (this.toggleExpandId.includes(e.query_id)) {
      const index = this.toggleExpandId.indexOf(e.query_id)
      this.toggleExpandId.splice(index, 1)
      e.hightlight_realizations = false
      return
    }
    this.currentExpandId = e.query_id
    e.flexHeight = 0
    e.editorH = 0
    this.toggleExpandId.push(e.query_id)
    this.$nextTick(() => {
      // const tableHeigth = $('#detailTable_' + e.query_id) && $('#detailTable_' + e.query_id).height()
      const tableHeigth = document.getElementById(`detailTable_${e.query_id}`) && document.getElementById(`detailTable_${e.query_id}`).offsetHeight
      if (tableHeigth) {
        e.flexHeight = e.flexHeight + tableHeigth
        let showLimitTip = false
        let sqlTextArr = e.sql_text.split('\n')
        // 要手动传入高度
        if ((sqlTextArr.length > 0 && sqlTextArr.length > sqlRowsLimit) || (sqlTextArr.length === 0 && e.sql_text.length > sqlStrLenLimit)) {
          showLimitTip = true
        }
        e.editorH = showLimitTip ? (e.flexHeight - 32) : (e.flexHeight - 2)
      }
      this.currentExpandId = ''
    })
  }

  handleExpandType (props, realizations) {
    // 点击的时候根据当前是展开还是收起，进行展开收起
    let flag = this.toggleExpandId.indexOf(props.row.query_id) === -1
    if (realizations) {
      props.row.hightlight_realizations = flag
    }
    this.$refs.queryHistoryTable.toggleRowExpansion(props.row, flag)
  }

  onCopy () {
    this.showCopyStatus = true
    setTimeout(() => {
      this.showCopyStatus = false
    }, 3000)
  }

  onError () {
    this.$message(this.$t('kylinLang.common.copyfail'))
  }

  getLayoutIds (realizations) {
    if (realizations && realizations.length) {
      let filterIds = []
      for (let i of realizations) {
        if (i.layoutId !== -1 && i.layoutId !== null && i.layoutId !== 0) {
          filterIds.push({layoutId: i.layoutId, streamingLayout: i.streamingLayout, secondStorage: i.secondStorage})
        }
      }
      return filterIds.join(', ')
    } else {
      return ''
    }
  }
  getSnapshots (realizations) {
    if (realizations && realizations.length) {
      let filterSnapshot = []
      for (let i of realizations) {
        if (i.snapshots && i.snapshots.length) {
          filterSnapshot = [...filterSnapshot, ...i.snapshots]
        }
      }
      filterSnapshot = [...new Set(filterSnapshot)]
      return filterSnapshot.join(', ')
    } else {
      return ''
    }
  }

  onSqlFilterChange () {
    if (this.filterData.sql.trim().match(/\s/)) {
      this.$message({
        message: this.$t('noSpaceTips'),
        type: 'warning',
        duration: 10000,
        showClose: true
      })
    }
    this.filterList()
  }

  exportHistory (isExportSqlOnly) {
    if (!this.queryHistoryTotalSize) return
    this.$emit('exportHistory', isExportSqlOnly)
  }

  filterList () {
    this.toggleExpandId = []
    this.$emit('loadFilterList', {...this.filterData, server: this.filterData.server.join('')})
  }

  openIndexDialog (realization, rows) {
    if (!realization.valid || realization.indexType === 'Table Snapshot') return
    this.$emit('openIndexDialog', realization, rows)
  }
  renderColumn (h) {
    if (this.filterData.startTimeFrom && this.filterData.startTimeTo) {
      const startTime = transToGmtTime(this.filterData.startTimeFrom)
      const endTime = transToGmtTime(this.filterData.startTimeTo)
      return (<span onClick={e => (e.stopPropagation())}>
        <span>{this.$t('kylinLang.query.startTime_th')}</span>
        <el-tooltip placement="top">
          <div slot="content">
            <span>
              <i class='el-icon-time'></i>
              <span> {startTime} To {endTime}</span>
            </span>
          </div>
          <el-date-picker
            value={this.datetimerange}
            onInput={this.handleInputDateRange}
            type="datetimerange"
            popper-class="table-filter-datepicker"
            toggle-icon="el-ksd-icon-data_range_old isFilter"
            is-only-icon={true}>
          </el-date-picker>
        </el-tooltip>
      </span>)
    } else {
      return (<span onClick={e => (e.stopPropagation())}>
        <span>{this.$t('kylinLang.query.startTime_th')}</span>
        <el-date-picker
          value={this.datetimerange}
          onInput={this.handleInputDateRange}
          popper-class="table-filter-datepicker"
          type="datetimerange"
          toggle-icon="el-ksd-icon-data_range_old"
          is-only-icon={true}>
        </el-date-picker>
      </span>)
    }
  }
  handleInputDateRange (val) {
    this.datetimerange = val
    this.dateRangeChange()
    this.filterList()
  }
  resetLatency () {
    this.startSec = 0
    this.endSec = 10
    this.filterData.latencyFrom = null
    this.filterData.latencyTo = null
    this.latencyFilterPopoverVisible = false
    this.clearLatencyRange()
    this.filterList()
  }
  saveLatencyRange () {
    this.filterData.latencyFrom = this.startSec
    if (this.startSec > this.endSec) {
      this.filterData.latencyTo = this.endSec = this.startSec
    } else {
      this.filterData.latencyTo = this.endSec
    }
    this.latencyFilterPopoverVisible = false
    this.clearLatencyRange()
    this.filterTags.push({label: `${this.startSec}s To ${this.endSec}s`, source: 'kylinLang.query.latency_th', key: 'latency'})
    this.filterList()
  }

  renderColumn2 (h) {
    if (this.filterData.latencyTo) {
      return (<span>
        <span style="margin-right:5px;">{this.$t('kylinLang.query.latency_th')}</span>
        <el-tooltip placement="top">
          <div slot="content">
            <span>
              <i class='el-icon-time'></i>
              <span> {this.filterData.latencyFrom}s To {this.filterData.latencyTo}s</span>
            </span>
          </div>
          <el-popover
            ref="latencyFilterPopover"
            placement="bottom"
            width="315"
            value={this.latencyFilterPopoverVisible}
            onInput={val => (this.latencyFilterPopoverVisible = val)}>
            <div class="latency-filter-pop">
              <el-input-number
                size="small"
                min={0}
                value={this.startSec}
                onInput={val1 => (this.startSec = val1)}></el-input-number>
              <span>&nbsp;S&nbsp;&nbsp;To</span>
              <el-input-number
                size="small"
                min={this.startSec}
                class="ksd-ml-10"
                value={this.endSec}
                onInput={val2 => (this.endSec = val2)}></el-input-number>
              <span>&nbsp;S</span>
            </div>
            <div class="latency-filter-footer">
              <el-button size="small" onClick={this.resetLatency}>{this.$t('kylinLang.query.clear')}</el-button>
              <el-button type="primary" onClick={this.saveLatencyRange} size="small">{this.$t('kylinLang.common.save')}</el-button>
            </div>
            <i class="el-ksd-icon-data_range_old isFilter" onClick={e => (e.stopPropagation())} slot="reference"></i>
          </el-popover>
        </el-tooltip>
      </span>)
    } else {
      return (<span>
        <span style="margin-right:5px;">{this.$t('kylinLang.query.latency_th')}</span>
        <el-popover
          ref="latencyFilterPopover"
          placement="bottom"
          width="315"
          value={this.latencyFilterPopoverVisible}
          onInput={val => (this.latencyFilterPopoverVisible = val)}>
          <div class="latency-filter-pop">
            <el-input-number
              size="small"
              value={this.startSec}
              min={0}
              onInput={val1 => (this.startSec = val1)}></el-input-number>
            <span>&nbsp;S&nbsp;&nbsp;To</span>
            <el-input-number
              size="small"
              class="ksd-ml-10"
              value={this.endSec}
              min={this.startSec}
              onInput={val2 => (this.endSec = val2)}></el-input-number>
            <span>&nbsp;S</span>
          </div>
          <div class="latency-filter-footer">
            <el-button size="small" onClick={this.resetLatency}>{this.$t('kylinLang.query.clear')}</el-button>
            <el-button type="primary" onClick={this.saveLatencyRange} size="small">{this.$t('kylinLang.common.save')}</el-button>
          </div>
          <i class="el-ksd-icon-data_range_old" onClick={e => (e.stopPropagation())} slot="reference"></i>
        </el-popover>
      </span>)
    }
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      realization: 'kylinLang.query.answered_by',
      query_status: 'taskStatus',
      server: 'kylinLang.query.queryNode',
      submitter: 'kylinLang.query.submitter'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        this.filterTags.push({label: item === 'modelName' ? 'allModels' : item, source: maps[type], key: type})
      }
    })
    this.filterData[type] = val
    this.filterList()
  }
  // 删除单个筛选条件
  handleClose (tag) {
    if (tag.key === 'datetimerange') {
      this.datetimerange = ''
      this.dateRangeChange()
    } else if (tag.key === 'latency') {
      this.filterData.latencyFrom = null
      this.filterData.latencyTo = null
      this.clearLatencyRange()
    } else if (tag.key === 'server') {
      this.filterData.server.splice(0, 1)
      const index = this.filterTags.map(item => item.key).indexOf('server')
      this.filterTags.splice(index, 1)
    } else {
      const index = this.filterData[tag.key].indexOf(tag.label)
      index > -1 && this.filterData[tag.key].splice(index, 1)
      this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    }
    this.filterList()
  }
  // 清除所有筛选条件
  clearAllTags () {
    this.filterData.query_status.splice(0, this.filterData.query_status.length)
    this.filterData.realization.splice(0, this.filterData.realization.length)
    this.filterData.server.splice(0, this.filterData.server.length)
    this.filterData.submitter.splice(0, this.filterData.submitter.length)
    this.filterData.latencyFrom = null
    this.filterData.latencyTo = null
    this.datetimerange = ''
    this.filterTags = []
    this.dateRangeChange()
    this.filterList()
  }
  openAuthorityDialog (item) {
    const { unauthorized_tables, unauthorized_columns, modelAlias } = item
    let details = []
    if (unauthorized_tables && unauthorized_tables.length) {
      details.push({title: `Table (${unauthorized_tables.length})`, list: unauthorized_tables})
    }
    if (unauthorized_columns && unauthorized_columns.length) {
      details.push({title: `Columns (${unauthorized_columns.length})`, list: unauthorized_columns})
    }
    this.callGlobalDetailDialog({
      theme: 'plain-mult',
      title: this.$t('kylinLang.model.authorityDetail'),
      msg: this.$t('kylinLang.model.authorityMsg', {modelName: modelAlias}),
      showCopyBtn: true,
      showIcon: false,
      showDetailDirect: true,
      details,
      showDetailBtn: false,
      dialogType: 'error',
      customClass: 'no-acl-model',
      showCopyTextLeftBtn: true
    })
  }
  // 展示 layout 详情
  async openLayoutDetails (item) {
    if (!item.layoutExist) return
    const {modelId, layoutId} = item
    try {
      const res = await this.loadAllIndex({
        project: this.currentSelectedProject,
        model: modelId,
        key: layoutId,
        page_offset: 0,
        page_size: 10,
        sort_by: '',
        reverse: '',
        sources: [],
        status: []
      })
      const data = await handleSuccessAsync(res)
      let row = data.value[0]
      this.cuboidData = row
      let idStr = (row.id !== undefined) && (row.id !== null) && (row.id !== '') ? ' [' + row.id + ']' : ''
      this.detailType = row.source.indexOf('AGG') >= 0 ? 'aggDetail' : 'tabelIndexDetail'
      this.indexDetailTitle = row.source.indexOf('AGG') >= 0 ? this.$t('aggDetailTitle') + idStr : this.$t('tabelDetailTitle') + idStr
      this.indexDetailShow = true
      // this.indexLoading = false
    } catch (e) {
      handleError(e)
    }
  }

  // 关闭 layout 详情
  closeDetailDialog () {
    this.indexDetailShow = false
  }
  openQueryDialog (row) {
    this.queryId = row.query_id
    this.queryServer = row.server[0]
    this.jobStatus = row.job_status
    this.showDiagnostic = true
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #queryHistoryTable {
    margin-top: 32px;
    /* table.ksd-table{
      tr:nth-child(odd){
        background: @table-stripe-color;
      }
    } */
    .el-table__expanded-cell {
      padding: 24px;
      .copy-btn {
        margin-right: 9%;
        .copyStatusMsg {
          display: inline-block;
          color: @text-normal-color;
          .el-icon-circle-check {
            color: @normal-color-1;
          }
        }
      }
      .detail-title {
        /* border-bottom: 1px solid @line-border-color; */
        overflow: hidden;
        padding-bottom: 10px;
        span:first-child {
          line-height: 18px;
          font-weight: @font-medium;
        }
        span:last-child {
          color: @text-normal-color;
        }
      }
      .detail-content {
        line-height: 1.8;
        .el-col {
          position: relative;
          .history_detail_table{
            .duration {
              color: @base-color;
              cursor: pointer;
            }
            tr.active{
              background-color: @base-color-9;
            }
          }
        }
      }
    }
    .searchInput {
      width: 260px;
    }
    .export-btn .is-disabled .el-button-group .el-button{
      color: @text-disabled-color;
      cursor: not-allowed;
      background-image: none;
      background-color: @line-border-color4;
      border-color: @line-border-color3;
    }
    .history-table {
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
      .sql-column {
        cursor: pointer;
        width: calc(100%);
        overflow: hidden;
        text-overflow: ellipsis;
        display: -webkit-box;
        -webkit-line-clamp: 1;
        /*! autoprefixer: off */
        -webkit-box-orient: vertical;
        /* autoprefixer: on */
        white-space: nowrap\0;
        &:hover {
          color: @base-color;
        }
      }
      .ksd-table th {
        width: 140px;
        color:@text-normal-color;
      }
      .tag-ellipsis {
        width: 100%;
        text-overflow: ellipsis;
        overflow: hidden;
        font-size: 0;
        line-height: 1;
        display:flex;
        align-items: center;
        /* position:relative; */
        &.hasMore{
          .el-tag{
            max-width: calc(~"100% - 85px");
            &.showMore{
              padding-left: 7px;
              width:auto;
              max-width: 80px;
              min-width: 60px;
            }
          }
        }
        .el-tag:not(:last-child) {
          margin-right: 5px;
        }
        .el-tag{
          max-width: 100%;
          overflow: hidden;
          text-overflow: ellipsis;
          &.disabled {
            border: solid 1px @text-placeholder-color;
            background-color: @background-disabled-color;
            color: @text-disabled-color;
            .el-icon-ksd-lock {
              margin-right: 3px;
            }
          }
        }
        .showMore{
          width:auto;
          max-width: 80px;
          min-width: 60px;
          padding-left: 7px;
          text-overflow: ellipsis;
          font-size:12px;
          line-height: 20px;
          height:20px;
          border-radius: 10px;
          box-sizing: border-box;
          display:inline-block;
          border: none;
          &:hover{
            text-decoration: none;
            cursor:pointer;
          }
        }
        /* .el-tag.realizationsNumTag{
          &:hover{
            cursor:pointer;
          }
        } */
      }
      .realization-tags {
        // display: flex;
        // flex-wrap: wrap;
        /* .el-tag {
          margin: 2.5px 10px 2.5px 0;
          border: none;
          background: none;
          padding: 0;
          height: 16px;
          line-height: 16px;
          &.model-tag {
            cursor: pointer;
          }
        } */
        .model-tag {
          color: @base-color;
          cursor: pointer;
        }
        .disable{
          color: @text-disabled-color;
          cursor: default;
        }
        .split{
          margin-right:10px;
        }
        .no-authority-model {
          color: @text-disabled-color;
          cursor: pointer;
          &:hover {
            color: @base-color;
          }
          .el-icon-ksd-lock {
            margin-right: 3px;
          }
        }
      }
      .realizations-layout-id {
        align-items: center;
        display: inline-flex;
        line-height: 16px;
        .el-tag__text {
          cursor: default !important;
        }
        span:first-child {
          color: @base-color;
          cursor: pointer;
        }
        &.is-disabled {
          color: @text-normal-color;
          cursor: default;
        }
      }
      .el-date-editor {
        line-height: 1;
        padding: 0;
        position: relative;
        top: 3px;
        left: 5px;
      }
      .el-ksd-icon-data_range_old {
        position: relative;
        top: 1px;
        &.isFilter,
        &:hover {
          color: @base-color;
        }
      }
      .el-icon-ksd-negative {
        color: @text-normal-color;
        font-size: 20px;
        &:hover {
          color: @base-color;
        }
      }
      .status-icon {
        font-size: 20px;
        &.el-icon-ksd-acclerate_all,
        &.el-icon-ksd-acclerate_portion {
          color: @normal-color-1;
        }
      }
      .el-ksd-icon-filter_22 {
        // position: relative;
        // font-size: 17px;
        // top: 1px;
        // left: 5px;
        &:hover,
        &.filter-open {
          color: @base-color;
        }
      }
    }
  }
  .query-error-dialog {
    .error-block {
      height: 200px;
      overflow-y: auto;
      background-color: @ke-background-color-secondary;
      border: 1px solid @ke-border-secondary;
      padding: 10px;
      border-radius: 4px;
    }
    .el-dialog__body {
      padding-bottom: 32px !important;
    }
  }
  .duration-popover {
    line-height: 1.5 !important;
    .step-name {
      color: @text-normal-color;
      &.sub-step {
        color: @color-text-placeholder;
      }
    }
    .step-duration {
      color: @color-text-primary;
      &.sub-step {
        color: @color-text-placeholder;
      }
    }
    .el-icon-ksd-more_01 {
      transform: scale(0.6);
      &.up {
        -webkit-transform: rotate(180deg) scale(0.6);
        -moz-transform: rotate(180deg) scale(0.6);
        -o-transform: rotate(180deg) scale(0.6);
        -ms-transform: rotate(180deg) scale(0.6);
        transform: rotate(180deg) scale(0.6);
      }
    }
    .el-progress {
      top: 3px;
    }
    .el-progress-bar__outer {
      border-radius: 0;
      background-color: transparent;
      position: relative;
      top: 5px;
      width: 90%;
      margin-left: 10px;
    }
    .el-progress-bar__inner {
      border-radius: 0;
    }
  }
  .el-popover.col-sql-popover {
    max-width: 400px;
    box-sizing: border-box;
    padding: 0;
    .sql-container {
      max-height: 300px;
      overflow-y: auto;
      padding: 16px;
    }
    .popover-sql-content {
      white-space: pre-wrap;
      word-break: break-all;
    }
  }
  .sql-tip {
    text-align: center;
    margin-top: 5px;
  }
  &.el-icon-ksd-acclerate_all,
  &.el-icon-ksd-acclerate_portion {
    color: @normal-color-1;
  }
  .to_acce {
    font-size: 12px;
    line-height: 1.5;
    color: @text-title-color;
  }
  .latency-filter-pop {
    display: inline-flex;
    align-items: center;
    .el-input-number--medium {
      width: 120px;
      margin-left: 10px;
      &:first-child {
        margin-left: 0;
      }
    }
  }
  .latency-filter-footer {
    border-top: 1px solid @line-split-color;
    padding: 10px 10px 0;
    margin: 10px -10px 0;
    text-align: right;
  }
  .filter-groups .el-checkbox {
    display: block;
    margin-bottom: 8px;
    margin-left: 0px !important;
    &:last-child {
      margin-bottom: 0;
    }
  }
  .el-popover.history-filter {
    min-width: 130px;
    box-sizing: border-box;
  }
  .loading {
    width: calc(~'100% - 13px');
    height: 254px;
    position: absolute;
    z-index: 10;
    background: @fff;
    .el-icon-loading {
      font-size: 20px;
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%)
    }
  }
  .filter-tags {
    margin-bottom: 10px;
    box-sizing: border-box;
    position: relative;
    .filter-tags-layout {
      display: inline-block;
      width: calc(~'100% - 80px');
      .clear-all-filters {
        color: @base-color;
        margin-left: 4px;
        position: relative;
        top: 0px;
        display: inline-block;
        cursor: pointer;
        font-size: 12px;
      }
    }
    .el-tag {
      margin-right: 4px;
      margin-top: 6px;
    }
    .filter-queries-size {
      position: absolute;
      top: 8px;
      right: 8px;
      font-size: 12px;
      color: @text-title-color;
    }
  }
  .filter-realization, .filter-submitter {
    .el-checkbox-group {
      max-height: 205px;
      overflow: auto;
    }
    i {
      margin-right: 5px;
      color: @text-normal-color;
    }
    .el-checkbox__input.is-checked+.el-checkbox__label i {
      color: @base-color;
    }
  }
</style>
