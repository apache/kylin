<template>
  <div id="queryHistory">
    <query_history_table
      :isLoadingHistory="isLoadingHistory"
      :queryHistoryData="queryHistoryData.query_histories"
      :queryHistoryTotalSize="queryHistoryData.size"
      :filterDirectData="filterDirectData"
      :queryNodes="queryNodes"
      v-on:openIndexDialog="openIndexDialog"
      v-on:loadFilterList="loadFilterList"
      v-on:exportHistory="exportHistory"></query_history_table>
    <kylin-pager ref="queryHistoryPager" :refTag="pageRefTags.queryHistoryPager" class="ksd-center ksd-mtb-16" :curPage="queryCurrentPage" :totalSize="queryHistoryData.size"  v-on:handleCurrentChange='pageCurrentChange'></kylin-pager>
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

    <el-dialog
      :title="$t('kylinLang.model.tableIndex')"
      top="10vh"
      limited-area
      :visible.sync="tabelIndexVisible"
      class="agg-dialog"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      width="1200px">
      <TableIndex
        v-if="tabelIndexVisible"
        :model-desc="model"
        :layout-id="tabelIndexLayoutId"
        :is-hide-edit="true">
      </TableIndex>
    </el-dialog>

    <el-dialog class="export-sql-dialog"
      width="480px"
      :title="exportTitle"
      :close-on-click-modal="false"
      :append-to-body="true"
      :visible.sync="exportSqlDialogVisible"
      :close-on-press-escape="false">
      <el-alert show-icon class="ksd-mb-10" :closable="false" type="warning" v-if="queryHistoryData.size>queryDownloadMaxSize">
        <span slot="title">{{$t('exportHistoryTips', {maxLength: queryDownloadMaxSize})}}<a class="ky-a-like" :href="$t('manualUrl')" target="_blank">{{$t('userManual')}}</a>{{$t('exportHistoryTips2')}}</span>
      </el-alert>
      <div v-html="exportMsg"></div>
      <span slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="exportSqlDialogVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <span class="ksd-ml-10" @click="exportSqlDialogVisible = false">
          <a class="el-button el-button--primary el-button--medium" v-if="!isExportSqlOnly" download :href="apiUrl + 'query/download_query_histories' + exportUrl()">{{$t('kylinLang.query.export')}}</a>
          <a class="el-button el-button--primary el-button--medium" v-else download :href="apiUrl + 'query/download_query_histories_sql' + exportUrl()">{{$t('kylinLang.query.export')}}</a>
        </span>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleError, handleSuccessAsync } from '../../util/index'
import queryHistoryTable from './query_history_table'
import ModelAggregate from '../studio/StudioModel/ModelList/ModelAggregate/index.vue'
import TableIndex from '../studio/StudioModel/TableIndex/index.vue'
import { pageRefTags, apiUrl, bigPageCount } from 'config'
@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      if (from.name && from.name === 'Dashboard' && to.params.source && to.params.source === 'homepage-history') {
        let tm = new Date(new Date().toDateString()).getTime()
        vm.filterDirectData.startTimeFrom = tm - 1000 * 60 * 60 * 24 * 7
        vm.filterDirectData.startTimeTo = tm
        return
      }
      vm.currentSelectedProject && vm.loadHistoryList()
    })
  },
  methods: {
    ...mapActions({
      getHistoryList: 'GET_HISTORY_LIST',
      loadOnlineQueryNodes: 'LOAD_ONLINE_QUERY_NODES'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions',
      'queryDownloadMaxSize'
    ])
  },
  components: {
    'query_history_table': queryHistoryTable,
    ModelAggregate,
    TableIndex
  },
  locales: {
    'en': {
      indexOverview: 'Index Overview',
      exportHistoryTitle: 'Export Query History',
      exportSqlTitle: 'Export SQL',
      exportHistoryTips: '{maxLength} query histories could be exported at a time. If you want to export more. Check ',
      userManual: 'User Manual',
      manualUrl: 'https://docs.kyligence.io/books/v4.5/en/Designers-Guide/query/history.en.html',
      exportHistoryTips2: ' for details.',
      exportSqlConfirm: '<b>{historyTotal}</b> SQL(s) will be exported as a .txt file. Are you sure you want to export?',
      exportHistoryConfirm: '<b>{historyTotal}</b> query historie(s) will be exported as a .csv file. Are you sure you want to export?'
    }
  }
})
export default class QueryHistory extends Vue {
  pageRefTags = pageRefTags
  apiUrl = apiUrl
  aggDetailVisible = false
  tabelIndexVisible = false
  tabelIndexLayoutId = ''
  aggIndexLayoutId = ''
  queryCurrentPage = 1
  queryHistoryData = {}
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    submitter: [],
    server: '',
    sql: '',
    query_status: []
  }
  filterDirectData = {
    startTimeFrom: null,
    startTimeTo: null
  }
  model = {
    uuid: ''
  }
  queryNodes = []
  pageSize = +localStorage.getItem(this.pageRefTags.queryHistoryPager) || bigPageCount
  isExportSqlOnly = false
  exportSqlDialogVisible = false
  isLoadingHistory = false

  get exportMsg () {
    // 最多导出10万条查询历史
    const totalSize = this.queryHistoryData.size > this.queryDownloadMaxSize ? this.queryDownloadMaxSize : this.queryHistoryData.size
    return this.isExportSqlOnly ? this.$t('exportSqlConfirm', {historyTotal: totalSize}) : this.$t('exportHistoryConfirm', {historyTotal: totalSize})
  }

  get exportTitle () {
    return this.isExportSqlOnly ? this.$t('exportSqlTitle') : this.$t('exportHistoryTitle')
  }

  exportUrl () {
    // 后端无法解析 5.5 时区，所以传分钟回去
    let timezone = -(new Date().getTimezoneOffset()) / 60
    let exportInfo = {
      project: this.currentSelectedProject,
      start_time_from: this.filterData.startTimeFrom || '',
      start_time_to: this.filterData.startTimeTo || '',
      latency_from: this.filterData.latencyFrom === null ? '' : this.filterData.latencyFrom,
      latency_to: this.filterData.latencyTo === null ? '' : this.filterData.latencyTo,
      realization: this.filterData.realization.join(','),
      submitter: this.filterData.submitter.join(','),
      server: this.filterData.server,
      sql: this.filterData.sql,
      query_status: this.filterData.query_status.join(','),
      timezone_offset_hour: timezone,
      language: this.$lang
    }
    let arr = []
    for (let prop in exportInfo) {
      arr.push(prop + '=' + exportInfo[prop])
    }
    return '?' + arr.join('&')
  }

  exportHistory (isExportSqlOnly) {
    this.isExportSqlOnly = isExportSqlOnly
    this.exportSqlDialogVisible = true
  }

  async openIndexDialog ({indexType, modelId, modelAlias, layoutId}, totalList) {
    this.model.uuid = modelId
    let aggLayoutId = totalList.filter(it => it.modelAlias === modelAlias && it.layoutId).map(item => item.layoutId).join(',')
    this.aggIndexLayoutId = aggLayoutId
    this.aggDetailVisible = true
  }
  async loadHistoryList (pageIndex) {
    try {
      this.isLoadingHistory = true
      const resData = {
        project: this.currentSelectedProject || null,
        limit: this.pageSize || 20,
        offset: pageIndex || 0,
        start_time_from: this.filterData.startTimeFrom,
        start_time_to: this.filterData.startTimeTo,
        latency_from: this.filterData.latencyFrom,
        latency_to: this.filterData.latencyTo,
        realization: this.filterData.realization,
        submitter: this.filterData.submitter,
        server: this.filterData.server,
        sql: this.filterData.sql,
        query_status: this.filterData.query_status
      }
      const res = await this.getHistoryList(resData)
      const data = await handleSuccessAsync(res)
      this.isLoadingHistory = false
      this.queryHistoryData = data
    } catch (e) {
      this.isLoadingHistory = false
      handleError(e)
    }
  }

  loadFilterList (data) {
    this.filterData = data
    this.pageCurrentChange(0, this.pageSize)
  }

  async created () {
    const res = await this.loadOnlineQueryNodes()
    this.queryNodes = await handleSuccessAsync(res)
  }

  pageCurrentChange (offset, pageSize) {
    this.pageSize = pageSize
    this.queryCurrentPage = offset + 1
    this.loadHistoryList(offset)
  }
}
</script>

<style lang="less">
@import '../../assets/styles/variables.less';
#queryHistory {
  padding: 0 24px 50px 24px;
}
.export-sql-dialog .dialog-footer{
  a {
    color: @fff;
    &:hover {
      text-decoration: none;
    }
  }
}
</style>
