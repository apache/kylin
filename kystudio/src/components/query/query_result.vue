<template>
  <div class="result_box">
    <div class="ksd-title-label ksd-mb-8">{{$t('queryResults')}}</div>
    <el-alert
      :title="noModelRangeTips"
      type="warning"
      class="ksd-mb-10"
      v-if="isShowNotModelRangeTips"
      show-icon>
    </el-alert>
    <div class="resultTipsLine">
      <el-row :gutter="24">
        <el-col :span="12">
          <div class="resultTips left">
            <p class="resultText" v-if="extraoption.queryId">
              <span class="label">{{$t('kylinLang.query.query_id')}}: </span>
              <span class="text">{{extraoption.queryId}}</span>
              <common-tip :content="$t('linkToSpark')" v-if="extraoption.appMasterURL && insightActions.includes('viewAppMasterURL')">
                <a target="_blank" :href="extraoption.appMasterURL"><i class="el-ksd-icon-spark_link_16"></i></a>
              </common-tip>
            </p>
            <p class="resultText" :class="{'guide-queryAnswerBy': isWorkspace}" v-if="!(extraoption.engineType==='NATIVE'&&!(realizations2 && realizations2.length))">
              <span class="label">{{$t('kylinLang.query.answered_by')}}: </span>
              <span class="text">
                <span class="realization-tags" v-if="realizations2 && realizations2.length">
                  <span v-for="(item, index) in realizations2" :key="item.modelId">
                    <template v-if="'visible' in item && !item.visible">
                      <span @click="openAuthorityDialog(item)" class="no-authority-model"><i class="el-icon-ksd-lock"></i>{{item.modelAlias}}</span><span>{{`${index !== realizations2.length - 1 ? $t('kylinLang.common.comma') : ''}`}}</span>
                    </template>
                    <template v-else>
                      <span @click="openIndexDialog(item, realizations2)" :class="{'model-tag': item.valid, 'disable': !item.valid || item.indexType === 'Table Snapshot'}">{{item.modelAlias}}</span><span>{{`${index !== realizations2.length - 1 ? $t('kylinLang.common.comma') : ''}`}}</span>
                    </template>
                  </span>
                </span>
                <span v-else class="realization-tags">{{extraoption.engineType}}</span>
              </span>
            </p>
            <p class="resultText" v-if="realizations.length">
              <span class="label">{{$t('kylinLang.query.index_id')}}: </span>
              <span class="text">
                <span class="realizations-layout-id" v-for="(item, index) in realizations" :key="item.layoutId">
                  <span class="layout-id">
                    <span @click="openLayoutDetails(item)">{{item.layoutId}}</span>
                    <el-tag size="mini" v-if="item.streamingLayout" class="ksd-ml-2">{{$t('streamingTag')}}</el-tag>
                  </span><span>{{`${index !== realizations.length - 1 ? $t('kylinLang.common.comma') : ''}`}}</span>
                </span>
              </span>
            </p>
            <p class="resultText" v-if="snapshots">
              <span class="label">{{$t('kylinLang.query.snapshot')}}: </span>
              <span class="text" :title="snapshots">{{snapshots}}</span>
            </p>
            <p class="resultText">
              <span class="label">{{$t('kylinLang.query.queryNode')}}: </span>
              <span class="text">{{extraoption.server}}</span>
            </p>
          </div>
        </el-col>
        <el-col :span="12">
          <div class="resultTips right">
            <p class="resultText">
              <span class="label">{{$t('kylinLang.query.duration')}}: </span>
              <span class="text" v-if="querySteps.length">
                <el-popover
                  placement="right"
                  :width="$lang === 'en' ? 400 : 320"
                  popper-class="duration-popover"
                  trigger="hover">
                  <el-row v-for="(step, index) in querySteps" :key="step.name" v-show="step.group !== 'PREPARATION' || (step.group === 'PREPARATION' && isShowDetail)">
                    <el-col :span="14">
                      <span class="step-name" :class="{'font-medium': index === 0, 'sub-step': step.group === 'PREPARATION'}">{{$t(step.name)}}</span>
                      <i class="el-icon-ksd-more_01" :class="{'up': isShowDetail}" v-if="step.name==='PREPARATION'" @click.stop="isShowDetail = !isShowDetail"></i>
                    </el-col>
                    <el-col :span="4">
                      <span class="step-duration ksd-fright" :class="{'font-medium': index === 0, 'sub-step': step.group === 'PREPARATION'}">{{Math.round(step.duration / 1000 * 100) / 100}}s</span>
                    </el-col>
                    <el-col :span="6" v-if="querySteps&&querySteps[0].duration>0">
                      <el-progress :stroke-width="6" :percentage="getProgress(step.duration, querySteps[0].duration)" color="#A6D6F6" :show-text="false"></el-progress>
                    </el-col>
                  </el-row>
                  <span slot="reference" class="duration">{{Math.round(extraoption.duration / 1000 * 100)/100||0.00}}s</span>
                </el-popover>
              </span>
              <span class="text" v-else>{{Math.round(extraoption.duration / 1000 * 100) / 100 || 0.00}}s</span>
            </p>
            <p class="resultText" v-if="!extraoption.pushDown">
              <span class="label">{{$t('kylinLang.query.total_scan_count')}}: </span>
              <span class="text">{{extraoption.totalScanRows | filterNumbers}}</span>
            </p>
            <p class="resultText" v-if="!extraoption.pushDown">
              <span class="label">{{$t('kylinLang.query.result_row_count')}}: </span>
              <span class="text">{{extraoption.resultRowCount | filterNumbers}}</span>
            </p>
          </div>
        </el-col>
      </el-row>
    </div>
    <div v-show="!isStop" class="result-block">
      <el-tabs v-model="activeResultType" class="ksd-mt-16" type="button" :class="{'en-model': $lang==='en'}" @tab-click="changeDataType">
          <el-tab-pane :label="$t('dataBtn')" name="data">
            <div class="grid-box narrowTable">
              <template v-if="!isStop">
                <el-table
                  :data="pagerTableData"
                  v-scroll-shadow
                  ref="tableLayout"
                  style="width: 100%;">
                  <el-table-column v-for="(value, index) in tableMeta" :key="index"
                    :prop="''+index"
                    :min-width="value.label&&value.label.length > 100 ? 52+8*(value.label&&value.label.length || 0) : 52+10*(value.label&&value.label.length || 0)"
                    show-overflow-tooltip
                    :label="value.label">
                    <template slot-scope="props">
                      <span class="table-cell-text">{{props.row[index]}}</span>
                    </template>
                  </el-table-column>
                </el-table>

                <kylin-pager v-on:handleCurrentChange='pageSizeChange' :curPage="currentPage+1" class="ksd-center ksd-mtb-16" ref="pager" :refTag="pageRefTags.queryResultPager" :perPageSize="pageSize" :totalSize="modelsTotal"></kylin-pager>
              </template>
            </div>
            <form name="export" class="exportTool" action="/kylin/api/query/format/csv" method="post">
              <input type="hidden" name="sql" v-model="sql"/>
              <input type="hidden" name="project" v-model="project"/>
              <input type="hidden" name="limit" v-model="limit" v-if="limit"/>
            </form>
          </el-tab-pane>
          <el-tab-pane :label="$t('visualizationBtn')" name="visualization">
            <div class="chart-headers" v-if="charts.dimension && charts.measure">
              <el-row class="ksd-mt-10" :gutter="5">
                <el-col :span="4" class="title">{{$t('chartType')}}</el-col>
                <el-col :span="10" class="title">{{$t('chartDimension')}}</el-col>
                <el-col :span="10" class="title">{{$t('chartMeasure')}}</el-col>
              </el-row>
              <el-row :gutter="5">
                <el-col :span="4" class="content">
                  <el-select v-model="charts.type" :placeholder="$t('pleaseSelect')" @change="changeChartType">
                    <el-option v-for="item in chartTypeOptions" :label="$t(item.text)" :disabled="item.isDisabled" :key="item.value" :value="item.value"></el-option>
                  </el-select>
                </el-col>
                <el-col :span="10" class="content">
                  <el-select v-model="charts.dimension" :placeholder="$t('pleaseSelect')" @change="changeChartDimension">
                    <el-option v-for="item in chartDimensionOptions.map(it => ({text: it, value: it}))" :label="item.text" :value="item.value" :key="item.value"></el-option>
                  </el-select>
                </el-col>
                <el-col :span="10" class="content">
                  <el-select v-model="charts.measure" :placeholder="$t('pleaseSelect')" @change="changeChartMeasure">
                    <el-option v-for="item in chartMeasureList.map(v => ({text: v, value: v}))" :label="item.text" :value="item.value" :key="item.value"></el-option>
                  </el-select>
                </el-col>
              </el-row>
            </div>
            <div class="chart-contains ksd-mt-10">
              <el-tooltip :content="$t('overSizeTips')" effect="dark" placement="top" v-if="displayOverSize">
                <i class="el-icon-ksd-info ksd-fs-15 tips"></i>
              </el-tooltip>
              <div :id="`charts_${tabsItem.name}`" class="chart-layout" v-show="charts.dimension && charts.measure"></div>
              <p class="no-fill-data" v-if="!charts.dimension || !charts.measure">{{$t('noDimensionOrMeasureData')}}</p>
            </div>
          </el-tab-pane>
      </el-tabs>
      <div class="query-data-options" v-if="activeResultType === 'data'">
        <span class="resultOperator">
          <el-input :placeholder="$t('kylinLang.common.pleaseFilter')" v-model="resultFilter" class="show-search-btn ksd-inline" size="small" prefix-icon="el-icon-search">
          </el-input>
        </span><el-button v-if="showExportCondition" :loading="hasClickExportBtn" class="ksd-ml-8" size="small" @click.native="exportData">
          {{$t('exportCSV')}}
        </el-button>
      </div>
    </div>
    <index-details :index-detail-title="indexDetailTitle" :detail-type="detailType" :cuboid-data="cuboidData" @close="closeDetailDialog" v-if="indexDetailShow" />
    <el-dialog
      :title="$t('indexOverview')"
      top="10vh"
      limited-area
      :visible.sync="aggDetailVisible"
      class="agg-dialog"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      width="1200px">
      <ModelAggregate
        v-if="aggDetailVisible"
        :model="model"
        :project-name="currentSelectedProject"
        :layout-id="aggIndexLayoutId"
        :is-show-aggregate-action="false"
        :isShowEditAgg="datasourceActions.includes('editAggGroup')"
        :isShowBulidIndex="datasourceActions.includes('buildIndex')"
        :isShowTableIndexActions="datasourceActions.includes('tableIndexActions')">
      </ModelAggregate>
    </el-dialog>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { scToFloat, showNull, handleSuccessAsync } from '../../util/index'
import { hasRole, transToGmtTime, handleError } from '../../util/business'
import { pageRefTags, pageCount } from 'config'
import { getOptions, compareDataSize } from './handler'
import IndexDetails from '../studio/StudioModel/ModelList/ModelAggregate/indexDetails'
import ModelAggregate from '../studio/StudioModel/ModelList/ModelAggregate/index.vue'
import echarts from 'echarts'
@Component({
  props: ['extraoption', 'isWorkspace', 'queryExportData', 'isStop', 'tabsItem'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      query: 'QUERY_BUILD_TABLES',
      postToExportCSV: 'EXPORT_CSV',
      loadAllIndex: 'LOAD_ALL_INDEX'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'insightActions',
      'datasourceActions'
    ])
  },
  components: {
    IndexDetails,
    ModelAggregate
  },
  locales: {
    'en': {
      username: 'Username',
      role: 'Role',
      analyst: 'Analyst',
      modeler: 'Modeler',
      admin: 'Admin',
      save: 'Save',
      restore: 'Restore',
      lineChart: 'Line chart',
      barChart: 'Bar chart',
      pieChart: 'Pie chart',
      traceUrl: 'Trace Url:',
      extraoptionrmation: 'Query Information',
      queryResults: 'Query Results',
      exportCSV: 'Export to CSV',
      linkToSpark: 'Jump to Spark Web UI',
      noModelRangeTips: 'The query is out of the data range for serving queries. Please add segments accordingly.',
      noModelRangeTips2: 'The query is out of the data range for serving queries. Please add segments or subpartitions accordingly.',
      dataBtn: 'Data',
      visualizationBtn: 'Visualization',
      chartType: 'Chart Types',
      chartDimension: 'Dimensions',
      chartMeasure: 'Measures',
      noDimensionOrMeasureData: 'Visualization unavailable for current dataset.',
      overSizeTips: 'The dataset exceeds the maximum limit of the chart. A sampling of 1,000 rows is occurred.',
      overTenYearTips: 'Your data is too large, data sampling with 36,500 rows has been occurred.',
      totalDuration: 'Total Duration',
      PREPARATION: 'Preparation',
      SQL_TRANSFORMATION: 'SQL transformation',
      SQL_PARSE_AND_OPTIMIZE: 'SQL parser optimization',
      HTTP_RECEPTION: 'Reception',
      GET_ACL_INFO: 'ACL checking',
      MODEL_MATCHING: 'Model matching',
      PREPARE_AND_SUBMIT_JOB: 'Creating and submitting Spark job',
      WAIT_FOR_EXECUTION: 'Waiting for resources',
      EXECUTION: 'Executing',
      FETCH_RESULT: 'Receiving result',
      SPARK_JOB_EXECUTION: 'Spark Job Execution',
      SQL_PUSHDOWN_TRANSFORMATION: 'SQL pushdown transformation',
      CONSTANT_QUERY: 'Constant query',
      HIT_CACHE: 'Cache hit',
      pleaseSelect: 'Please select',
      aggDetailTitle: 'Aggregate Detail',
      tabelDetailTitle: 'Table Index Detail',
      streamingTag: 'streaming',
      indexOverview: 'Index Overview',
      refreshManual: 'No data, please click to refresh',
      loading: 'Loading...',
      refreshLater: 'No results, please try again later',
      fetchError: 'Can\'t get the result as the record is missing'
    }
  },
  filters: {
    filterNumbers (num) {
      if (num >= 0) return num
    }
  }
})
export default class queryResult extends Vue {
  pageRefTags = pageRefTags
  resultFilter = ''
  tableData = []
  tableMeta = []
  tableMetaBackup = []
  pagerTableData = []
  graphType = 'line'
  sql = ''
  project = ''
  limit = ''
  showDetail = false
  pageSize = +localStorage.getItem(this.pageRefTags.queryResultPager) || pageCount
  currentPage = 0
  modelsTotal = this.extraoption.results && this.extraoption.results.length
  timer = null
  pageX = 0
  pageSizeX = 30
  hasClickExportBtn = false
  isShowDetail = false
  activeResultType = 'data'
  charts = {
    type: 'lineChart',
    dimension: '',
    measure: ''
  }
  chartDimensionOptions = []
  dateTypes = [91, 92, 93]
  stringTypes = [-1, 1, 12]
  numberTypes = [-7, -6, -5, 3, 4, 5, 6, 7, 8]
  chartLayout = null
  chartTypeOptions = [
    {text: 'lineChart', value: 'lineChart', isDisabled: false},
    {text: 'barChart', value: 'barChart', isDisabled: false},
    {text: 'pieChart', value: 'pieChart', isDisabled: false}
  ]
  indexDetailShow = false
  aggDetailVisible = false
  aggIndexLayoutId = ''
  model = {
    uuid: ''
  }
  // 增加可视化按钮
  get insightBtnGroups () {
    return [
      {text: this.$t('dataBtn'), value: 'data'},
      {text: this.$t('visualizationBtn'), value: 'visualization'}
    ]
  }
  // 动态获取维度
  chartDimensionList (type) {
    const dimensionList = []
    const chartType = type || this.charts.type
    if (chartType === 'lineChart') {
      this.tableMetaBackup.forEach(item => {
        if (this.dateTypes.includes(item.columnType)) {
          dimensionList.push(item.label)
        }
      })
    } else {
      this.tableMetaBackup.forEach(item => {
        dimensionList.push(item.label)
      })
    }
    this.charts.dimension = dimensionList[0] || ''
    this.chartDimensionOptions = dimensionList
    return dimensionList
  }
  // 动态获取度量
  get chartMeasureList () {
    const measureList = []
    this.tableMetaBackup.forEach(item => {
      if (this.numberTypes.includes(item.columnType)) {
        measureList.push(item.label)
      }
    })
    this.charts.measure = measureList[measureList.length - 1] || ''
    return measureList
  }
  // 默认 disabled 不可选的图表类型
  disabledChartType () {
    if (!this.chartMeasureList.length) {
      this.chartTypeOptions = this.chartTypeOptions.map(it => ({...it, isDisabled: true}))
    } else {
      this.chartTypeOptions.forEach(it => {
        if (!this.chartDimensionList(it.value).length) {
          it.isDisabled = true
        }
      })
    }
    this.charts.type = this.chartTypeOptions.filter(it => !it.isDisabled).length ? this.chartTypeOptions.filter(it => !it.isDisabled)[0].value : ''
  }

  // 切换数据展示效果
  changeDataType (item) {
    if (item.name === 'visualization') {
      this.disabledChartType()
      this.chartDimensionList()
      this.$nextTick(() => {
        this.initChartOptions()
      })
    }
  }
  // 初始化 echarts 配置
  initChartOptions () {
    if (!this.charts.type) return
    this.chartLayout = echarts.init(this.$el.querySelector(`#charts_${this.tabsItem.name}`))
    this.chartLayout.setOption(getOptions(this))
  }
  changeChartType (v) {
    this.charts.type = v
    this.chartDimensionList()
    this.resetEcharts()
  }
  // 更改维度
  changeChartDimension (v) {
    this.resetEcharts()
  }
  // 更改度量
  changeChartMeasure () {
    this.resetEcharts()
  }
  resetEcharts () {
    this.chartLayout && this.chartLayout.dispose()
    this.$nextTick(() => {
      this.initChartOptions()
    })
  }
  // 更改 charts 大小
  resetChartsPosition () {
    this.chartLayout && this.chartLayout.resize()
  }
  // 是否显示数量过多 icon 提示
  get displayOverSize () {
    const result = compareDataSize(this)
    if (this.charts.type === 'pieChart') {
      return result.xData.length > 1000
    }
  }
  exportData () {
    // 区别于3x中，导出所需的参数，存在props 传进来的 queryExportData 这个对象中，不再一起放在 extraoption 中
    this.sql = this.queryExportData.sql
    this.project = this.currentSelectedProject
    this.limit = this.queryExportData.limit
    this.$nextTick(() => {
      this.hasClickExportBtn = false
      this.$el.querySelectorAll('.exportTool').length && this.$el.querySelectorAll('.exportTool')[0].submit()
    })
  }
  transDataForGrid (data) {
    var columnMeata = this.extraoption.columnMetas
    var lenOfMeta = columnMeata && columnMeata.length
    for (var i = 0; i < lenOfMeta; i++) {
      this.tableMeta.push(columnMeata[i])
    }
    this.tableMetaBackup = this.tableMeta
    this.tableMeta = this.tableMetaBackup.slice(0, (this.pageX + 1) * this.pageSizeX)
    this.pageSizeChange(0)
  }
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  openIndexDialog ({indexType, modelId, modelAlias, layoutId, valid}, totalList) {
    if (!valid || indexType === 'Table Snapshot') return
    this.model.uuid = modelId
    let aggLayoutId = totalList.filter(it => it.modelAlias === modelAlias && it.layoutId).map(item => item.layoutId).join(',')
    this.aggIndexLayoutId = aggLayoutId
    this.aggDetailVisible = true
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

  // 查询对象显示跟索引ID显示的逻辑不一样
  get realizations2 () {
    if (this.extraoption.realizations && this.extraoption.realizations.length) {
      let realizations = []
      for (let i of this.extraoption.realizations) {
        if (!((i.layoutId === -1 || i.layoutId === null || i.layoutId === 0) && i.indexType !== null)) {
          realizations.push(i)
        }
      }
      return realizations
    } else {
      return []
    }
  }
  get realizations () {
    if (this.extraoption.realizations && this.extraoption.realizations.length) {
      let realizations = []
      for (let i of this.extraoption.realizations) {
        if (i.layoutId !== -1 && i.layoutId !== null && i.layoutId !== 0) {
          realizations.push(i)
        }
      }
      return realizations
    } else {
      return []
    }
  }
  get snapshots () {
    if (this.extraoption.realizations && this.extraoption.realizations.length) {
      let filterSnapshot = []
      for (let i of this.extraoption.realizations) {
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
  get isShowNotModelRangeTips () {
    let isAnyNull = false
    if (this.extraoption.realizations && this.extraoption.realizations.length) {
      for (let i in this.extraoption.realizations) {
        if (this.extraoption.realizations[i].layoutId === -1 && this.extraoption.realizations[i].indexType === null) {
          isAnyNull = true
          break
        }
      }
    }
    return isAnyNull
  }
  get noModelRangeTips () {
    return this.$store.state.project.multi_partition_enabled ? this.$t('noModelRangeTips2') : this.$t('noModelRangeTips')
  }

  // 展示 layout 详情
  async openLayoutDetails (item) {
    const {modelId, layoutId} = item
    try {
      const res = await this.loadAllIndex({
        project: this.currentSelectedProject,
        model: modelId,
        ids: layoutId,
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
    } catch (e) {
      handleError(e)
    }
  }

  // 关闭 layout 详情
  closeDetailDialog () {
    this.indexDetailShow = false
  }
  filterTableData () {
    if (this.resultFilter) {
      const filteredData = this.extraoption.results && this.extraoption.results.filter((item) => {
        return item.toString().toLocaleUpperCase().indexOf(this.resultFilter.toLocaleUpperCase()) !== -1
      })
      this.modelsTotal = filteredData.length
      return filteredData
    } else {
      this.modelsTotal = this.extraoption.results && this.extraoption.results.length
      return this.extraoption.results
    }
  }
  pageSizeChange (currentPage, pageSize) {
    this.currentPage = currentPage
    if (pageSize) {
      this.pageSize = pageSize
    }
    const filteredData = this.filterTableData()
    this.tableData = filteredData && filteredData.slice(currentPage * this.pageSize, (currentPage + 1) * this.pageSize)
    var len = this.tableData && this.tableData.length
    for (let i = 0; i < len; i++) {
      var innerLen = this.tableData[i].length
      for (var m = 0; m < innerLen; m++) {
        var cur = this.tableData[i][m]
        var colType = this.extraoption.columnMetas[m].columnTypeName
        // char varchar 类型列不进行科学计算转数字显示的转换
        var trans = cur
        if (colType.toLocaleLowerCase().indexOf('char') === -1) {
          trans = scToFloat(cur)
        }
        this.tableData[i][m] = showNull(trans)
      }
    }
    this.pagerTableData = Object.assign([], this.tableData)
    this.$nextTick(() => {
      this.$refs.tableLayout && this.$refs.tableLayout.doLayout()
    })
  }
  getMoreData () {
    if (this.$refs.tableLayout.scrollPosition === 'right') {
      this.tableMeta = this.tableMetaBackup.slice(0, (++this.pageX + 1) * this.pageSizeX)
    }
  }
  @Watch('resultFilter')
  onResultFilterChange (val) {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.pageSizeChange(0)
    }, 500)
  }
  getProgress (duration, totalDuration, isShow) {
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
      steps.forEach((s) => {
        renderSteps[0].duration = renderSteps[0].duration + s.duration
        if (s.group === 'PREPARATION') {
          preStepNum++
          let preparationIndex = renderSteps.findIndex(item => item.name === 'PREPARATION')
          renderSteps[preparationIndex].duration = renderSteps[preparationIndex].duration + s.duration
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
  get querySteps () {
    return this.getStepData(this.extraoption['traces']) || []
  }
  @Watch('extraoption')
  onExtraoptionChange (val) {
    this.tableData = []
    this.tableMeta = []
    this.pagerTableData = []
    this.transDataForGrid()
  }
  created () {
    this.transDataForGrid()
  }
  get showExportCondition () {
    return this.$store.state.system.allowAdminExport === 'true' && this.isAdmin || this.$store.state.system.allowNotAdminExport === 'true' && !this.isAdmin
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }

  mounted () {
    if (!this.$refs.tableLayout || !this.$refs.tableLayout.bodyWrapper) return
    this.$refs.tableLayout.bodyWrapper.addEventListener('scroll', this.getMoreData)
    window.addEventListener('resize', this.resetChartsPosition)
  }

  beforeDestory () {
    this.$refs.tableLayout.bodyWrapper.removeEventListener('scroll', this.getMoreData)
    window.removeEventListener('resize', this.resetChartsPosition)
  }
}
</script>
<style  lang="less">
  @import '../../assets/styles/variables.less';
  .narrowTable{
    .el-table td, .el-table th{
      height: 30px;
    }
  }
  .ksd-header {
    margin-top: 26px;
  }
  .result-title {
    position: relative;
    top: 6px;
    &.result-title-float {
      float: left;
      top: 2px;
    }
  }
  .resultTipsLine{
    font-size: 12px;
    padding: 16px;
    background-color: @ke-background-color-secondary;
    line-height: 16px;
    position: relative;
    .show-more-btn {
      position: absolute;
      right: 10px;
      top: 16px;
    }
    .resultTips{
      align-items: center;
      flex-wrap: wrap;
      .resultText{
        color:@text-normal-color;
        margin-top: 8px;
        display: flex;
        .el-ksd-icon-refresh_16 {
          color: @ke-color-primary;
          font-size: 16px;
          position: relative;
          top: -2px;
          margin-right: 2px;
        }
        .refresh-loading {
          color: @text-normal-color;
        }
        .refresh-label {
          color: @text-normal-color;
          cursor: pointer;
          &:hover {
            color: @ke-color-primary;
          }
        }
        .refresh-error {
          color: @ke-color-danger;
        }
        &:first-child {
          margin-top: 0px;
        }
        .label {
          font-weight: normal;
          flex: none;
        }
        .text{
          color:@color-text-primary;
          word-break: break-all;
          margin-left: 8px;
          .duration {
            color: @ke-color-primary;
            cursor: pointer;
          }
          .realization-tags {
            .model-tag {
              color: @ke-color-primary;
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
            .layout-id {
              position: relative;
              color: @ke-color-primary;
              cursor: pointer;
            }
            .mutiple-color-icon {
              position: absolute;
              right: 0px;
              top: -3px;
            }
          }
        }
        a {
          color: @color-text-primary;
          &:hover {
            color: @base-color;
          }
        }
        &.query-obj {
          .text {
            width: calc(~'100% - 85px');
            display: inline-block;
            overflow: hidden;
            text-overflow: ellipsis;
            position: absolute;
            margin-left: 5px;
            white-space: nowrap;
          }
        }
      }
    }
  }
  .result_box{
    .result-block {
      position: relative;
      .query-data-options {
        position: absolute;
        right: 0px;
        top: 0px;
      }
    }
    .el-table .cell{
       word-break: break-all!important;
    }
    .resultOperator {
      .el-input {
        width: 245px;
      }
    }
    // .result-layout-btns {
    //   margin-top: 16px;
    //   .el-button.active {
    //     color: #5c5c5c;
    //     background: #f4f4f4;
    //     border: 1px solid #cccccc;
    //     box-shadow: inset 1px 1px 2px 0 #ddd;
    //   }
    // }
    .chart-headers {
      .title {
        font-weight: bold;
      }
      .content {
        .el-select {
          width: 100%;
          margin-top: 5px;
        }
      }
    }
    .chart-contains {
      width: 100%;
      height: 400px;
      position: relative;
      .chart-layout {
        width: 100%;
        height: 100%;
      }
      .no-fill-data {
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
      }
      .tips {
        position: absolute;
        right: 0;
        z-index: 10;
        color: @color-warning;
      }
    }
  }
  .table-cell-text{
    // word-wrap: break-word;
    // word-break: break-all;
    white-space: pre;
    color: @text-normal-color;
    font-family: Lato,"Noto Sans S Chinese",sans-serif;
  }
</style>
