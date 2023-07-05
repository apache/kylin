<template>
  <el-dialog class="edit-exclude-column-dialog"
    width="800px"
    :title="$t(excludeColumntitle)"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="handleClose">
    <div class="ksd-mb-16" v-html="$t('excludeDesc')"></div>
    <div class="ksd-title-label-mini ksd-mb-8">{{$t('selectTable')}}</div>
    <el-select
      class="exclude_rule-select ksd-mb-16"
      v-model="excluded_table"
      @change="getExcludeColumns"
      filterable
      :disabled="!!excludeTable"
      remote
      :remote-method="filterExcludeTables"
      :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
      style="width: 100%"
      :loading="loading">
      <el-option
        v-for="item in excludeRuleOptions"
        :key="item.value"
        :label="item.label"
        :value="item.value">
        <span>{{item.label}}<i v-if="item.fact" class="el-icon-ksd-fact_table ksd-ml-5"></i></span>
      </el-option>
      <p class="limit-excluded-tables-msg" v-if="showIimitExcludedTableMsg">{{$t('limitExcludedTablesTip')}}</p>
    </el-select>
    <div class="ksd-title-label-mini columns-header ksd-mb-8">
      <span>{{$t('selectColumns')}}</span>
      <el-input
        class="filter-input"
        prefix-icon="el-ksd-icon-search_22"
        size="small"
        v-model="filterText"
        @input="filterChange"
        :placeholder="$t('filterByColumns')">
      </el-input>
    </div>
    <el-table
      :data="columns"
      :empty-text="emptyText"
      class="column-table"
      @selection-change="handleSelectionChange"
      style="width: 100%">
      <el-table-column type="selection" align="center" width="42"></el-table-column>
      <el-table-column
        type="index"
        label="ID"
        width="64"
        show-overflow-tooltip
        :index="startIndex">
      </el-table-column>
      <el-table-column
        prop="name"
        min-width="300"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.columnName')">
      </el-table-column>
      <el-table-column
        prop="datatype"
        min-width="120"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.dataType')">
      </el-table-column>
      <el-table-column
        prop="comment"
        min-width="100"
        show-overflow-tooltip
        :render-header="renderCommentHeader">
        <template slot-scope="scope">
          <span :title="scope.row.comment">{{scope.row.comment}}</span>
        </template>
      </el-table-column>
    </el-table>
    <kylin-pager
      class="ksd-center ksd-mt-16" ref="columnPager"
      :refTag="pageRefTags.exclusionColumnsPager"
      :totalSize="columnsTotalSize"
      :curPage="pagination.page_offset + 1"
      :perPageSize="pagination.page_size"
      @handleCurrentChange="handleCurrentChange">
    </kylin-pager>
    <div slot="footer" class="dialog-footer">
      <el-popover
        ref="popover"
        placement="top"
        width="200"
        v-model="confirmSubmitVisible"
        trigger="click">
        <div class="popover-title">
          <el-icon name="el-ksd-n-icon-warning-color-filled" class="ksd-fs-16" type="mult"></el-icon><span class="ksd-title-label-mini">{{isDelExcludeMode ? $t('delAllColumnTitle') : $t('addAllColumnTitle')}}</span>
        </div>
        <div class="ksd-center ksd-mt-16">
          <el-button style="width: 96px" size="small" @click="confirmSubmitVisible = false">{{$t('kylinLang.common.cancel')}}</el-button><el-button
          type="primary" size="small" style="width: 96px" @click="confirmSubmit">{{$t('kylinLang.common.ok')}}</el-button>
        </div>
      </el-popover>

      <el-button type="primary" text v-popover:popover v-show="columnsTotalSize&&!loadingSubmit">{{isDelExcludeMode ? $t('delAllColumnBtn') : $t('addAllColumnBtn')}}</el-button>
      <el-button size="medium" @click="handleClose">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" :loading="loadingSubmit" :disabled="!selectCols.length" @click="submit(false)">{{isDelExcludeMode ? $t('kylinLang.common.delete') : $t('kylinLang.common.add')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import { handleError, handleSuccessAsync, ArrayFlat } from 'util'
import { pageRefTags, pageCount } from 'config'

import store from './store'
import locales from './locales'
import vuex, { actionTypes } from '../../../store'

vuex.registerModule(['modals', 'ExcludeColumnsDialog'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    ...mapState('ExcludeColumnsDialog', {
      isShow: state => state.isShow,
      excludeTable: state => state.excludeTable,
      excludeColumntitle: state => state.excludeColumntitle,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapActions({
      fetchDBandTables: 'FETCH_DB_AND_TABLES',
      loadExcludeColumns: 'LOAD_EXCLUDE_COLUMNS',
      updateExcludeColumns: 'UPDATE_EXCLUDE_CLUMNS'
    }),
    ...mapMutations('ExcludeColumnsDialog', {
      setModalForm: actionTypes.SET_MODAL_FORM,
      setModal: actionTypes.SET_MODAL,
      hideModal: actionTypes.HIDE_MODAL,
      initModal: actionTypes.INIT_MODAL
    })
  },
  locales
})
export default class ExcludeColumnsDialog extends Vue {
  pageRefTags = pageRefTags
  loadingSubmit = false
  excludeRuleOptions = []
  showIimitExcludedTableMsg = false
  filterExcludeTablesTimer = null
  dbInfoFilter = {
    project_name: '',
    source_type: 9,
    page_offset: 0,
    page_size: 10,
    table: ''
  }
  excluded_table = ''
  loading = false
  pagination = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.exclusionColumnsPager) || pageCount
  }
  filterText = ''
  columns = []
  columnsTotalSize = 0
  confirmSubmitVisible = false
  selectCols = []

  get startIndex () {
    const { page_offset, page_size } = this.pagination
    return page_offset * page_size + 1
  }

  get emptyText () {
    return this.filterText ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  filterChange () {
    this.pagination.page_offset = 0
    this.getExcludeColumns(this.excluded_table)
  }

  reset () {
    this.loadingSubmit = false
    this.excludeRuleOptions = []
    this.showIimitExcludedTableMsg = false
    this.excluded_table = ''
    this.filterText = ''
    this.columns = []
    this.columnsTotalSize = 0
    this.confirmSubmitVisible = false
    this.selectCols = []
  }

  handleClose (isSubmit = false) {
    this.reset()
    this.hideModal()
    this.callback && this.callback(isSubmit)
  }

  handleSelectionChange (cols) {
    this.selectCols = cols
  }

  async submit (isExcludedAll) {
    try {
      this.loadingSubmit = true
      const data = {
        project: this.currentSelectedProject,
        canceled_tables: !!isExcludedAll && this.isDelExcludeMode ? [this.excluded_table] : [],
        excluded_tables: [
          {
            table: this.excluded_table,
            excluded: !!isExcludedAll && !this.isDelExcludeMode,
            added_columns: !isExcludedAll && !this.isDelExcludeMode ? this.selectCols.map((c) => {
              return c.name
            }) : [],
            removed_columns: !isExcludedAll && this.isDelExcludeMode ? this.selectCols.map((c) => {
              return c.name
            }) : []
          }
        ]
      }
      await this.updateExcludeColumns(data)
      this.callback && this.callback(true)
      this.hideModal()
    } catch (e) {
      this.loadingSubmit = false
      handleError(e)
    }
  }
  confirmSubmit () {
    this.confirmSubmitVisible = false
    this.submit(true)
  }

  get isDelExcludeMode () {
    return this.excludeColumntitle === 'delExcludeColumns'
  }

  async getExcludeColumns (val) {
    try {
      if (!val) return
      // excluded_col_type 展示的屏蔽列类型，可选，默认值 0代表待屏蔽列，1代表屏蔽列，其他值无效
      const res = await this.loadExcludeColumns({ ...this.pagination, project: this.currentSelectedProject, table: val, key: this.filterText, col_type: this.isDelExcludeMode ? 1 : 0 })
      const { admitted_columns, excluded_columns, total_size } = await handleSuccessAsync(res)
      this.columns = this.isDelExcludeMode ? excluded_columns : admitted_columns
      this.columnsTotalSize = total_size
    } catch (e) {
      handleError(e)
    }
  }

  handleCurrentChange (page_offset, pageSize) {
    this.pagination.page_offset = page_offset
    this.pagination.page_size = pageSize
    this.getExcludeColumns(this.excluded_table)
  }

  renderCommentHeader (h, { column, $index }) {
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('kylinLang.dataSource.comment')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('commentTip')}>
        <span class='el-ksd-icon-more_info_22 ksd-fs-22'></span>
      </common-tip>
    </span>)
  }

  // 获取 database 和 tables 信息（显示前 50 条记录）
  async getDbAndTablesInfo () {
    try {
      this.dbInfoFilter.project_name = this.currentSelectedProject
      const response = await this.fetchDBandTables(this.dbInfoFilter)
      const results = await handleSuccessAsync(response)
      const { databases } = results
      let dbList = databases ? ArrayFlat(databases.map(item => item.tables)) : []
      if (dbList.length) {
        this.excludeRuleOptions = dbList.map(it => ({label: `${it.database}.${it.name}`, value: `${it.database}.${it.name}`, fact: it.root_fact})).slice(0, 50)
        this.showIimitExcludedTableMsg = dbList.length > 50
      }
    } catch (e) {
      handleError(e)
    }
  }

  // 过滤 db 或 table
  filterExcludeTables (name) {
    clearTimeout(this.filterExcludeTablesTimer)
    this.filterExcludeTablesTimer = setTimeout(() => {
      this.dbInfoFilter.table = name
      this.getDbAndTablesInfo()
    }, 500)
  }

  @Watch('isShow')
  onModalShow (val) {
    if (val) {
      // 新增一张表的屏蔽列时，要拉取数据源表数据
      if (this.excludeColumntitle === 'addExcludeColumns') {
        this.getDbAndTablesInfo()
      }
      if (this.excludeTable) {
        this.excluded_table = this.excludeTable
        this.getExcludeColumns(this.excluded_table)
      }
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.edit-exclude-column-dialog {
  .el-dialog__body {
    padding: 20px;
    .columns-header span{
      display: inline-block;
      height: 30px;
      line-height: 30px;
    }
    .filter-input {
      width: 210px;
      float: right;
    }
  }
}
</style>
