<template>
  <div class="table-columns">
    <div class="columns-header">
      <div class="left total-rows">
        <span v-if="totalDataRows">{{$t('total') + totalDataRows}}</span>
      </div>
      <div class="right">
        <el-input
          class="filter-input"
          prefix-icon="el-ksd-icon-search_22"
          size="small"
          v-model="filterText"
          @input="filterChange"
          :placeholder="$t('filterByColumns')">
        </el-input>
      </div>
    </div>
    <el-table class="columns-body" ref="tableColumns" v-scroll-shadow :data="currentColumns" :empty-text="emptyText" @sort-change="onSortChange">
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
        prop="cardinality"
        sortable="custom"
        align="right"
        header-align="right"
        min-width="105"
        v-if="table.datasource!==1"
        show-overflow-tooltip
        :render-header="renderCardinalityHeader">
        <span slot-scope="scope">
          <template v-if="scope.row.cardinality === null"><i class="no-data_placeholder">NULL</i></template>
          <template v-else>{{ scope.row.cardinality }}</template>
        </span>
      </el-table-column>
      <el-table-column
        prop="min_value"
        align="right"
        header-align="right"
        min-width="105"
        v-if="table.datasource!==1"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.minimal')">
        <span slot-scope="scope">
          <template v-if="scope.row.min_value === null"><i class="no-data_placeholder">NULL</i></template>
          <template v-else>{{ scope.row.min_value }}</template>
        </span>
      </el-table-column>
      <el-table-column
        prop="max_value"
        align="right"
        header-align="right"
        min-width="105"
        v-if="table.datasource!==1"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.maximum')">
        <span slot-scope="scope">
          <template v-if="scope.row.max_value === null"><i class="no-data_placeholder">NULL</i></template>
          <template v-else>{{ scope.row.max_value }}</template>
        </span>
      </el-table-column>
      <el-table-column
        prop="comment"
        v-if="table.datasource!==1"
        show-overflow-tooltip
        :render-header="renderCommentHeader">
        <template slot-scope="scope">
          <span :title="scope.row.comment">{{scope.row.comment}}</span>
        </template>
      </el-table-column>
    </el-table>
    <kylin-pager
      class="ksd-center ksd-mt-16" ref="pager"
      :refTag="pageRefTags.tableColumnsPager"
      :totalSize="columns.length"
      :curPage="pagination.page_offset + 1"
      :perPageSize="pagination.pageSize"
      @handleCurrentChange="handleCurrentChange">
    </kylin-pager>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { pageRefTags } from 'config'
import locales from './locales'
import { pageCount } from '../../../../config'

@Component({
  props: {
    table: {
      type: Object
    }
  },
  locales
})
export default class TableColumns extends Vue {
  pageRefTags = pageRefTags
  filterText = ''
  pagination = {
    page_offset: 0,
    pageSize: +localStorage.getItem(this.pageRefTags.tableColumnsPager) || pageCount
  }
  renderCardinalityHeader (h, { column, $index }) {
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('kylinLang.dataSource.cardinality')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('cardinalityTip')}>
        <span class='el-ksd-icon-more_info_22 ksd-fs-22'></span>
      </common-tip>
    </span>)
  }
  renderCommentHeader (h, { column, $index }) {
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('kylinLang.dataSource.comment')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('commentTip')}>
        <span class='el-ksd-icon-more_info_22 ksd-fs-22'></span>
      </common-tip>
    </span>)
  }
  get emptyText () {
    return this.filterText ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get startIndex () {
    const { page_offset, pageSize } = this.pagination
    return page_offset * pageSize + 1
  }
  get totalDataRows () {
    return this.table.totalRecords
  }
  get columns () {
    return this.table.columns
      .filter(column => column.name.toUpperCase().includes(this.filterText.toUpperCase()))
  }
  get currentColumns () {
    const { page_offset, pageSize } = this.pagination
    return this.columns.slice(page_offset * pageSize, page_offset * pageSize + pageSize)
  }
  filterChange () {
    this.pagination.page_offset = 0
  }
  handleCurrentChange (page_offset, pageSize) {
    this.pagination.page_offset = page_offset
    this.pagination.pageSize = pageSize
  }
  onSortChange ({ column, prop, order }) {
    if (order === 'ascending') {
      this.table.columns.sort((a, b) => {
        return a[prop] - b[prop]
      })
    } else {
      this.table.columns.sort((a, b) => {
        return b[prop] - a[prop]
      })
    }
    this.handleCurrentChange(0, this.pagination.pageSize)
  }
  mounted () {
    this.$nextTick(() => {
      this.$refs.tableColumns && this.$refs.tableColumns.doLayout()
    })
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-columns {
  padding: 0 0 20px 0;
  .columns-header {
    margin-bottom: 10px;
    white-space: nowrap;
    .total-rows {
      font-size: 14px;
      line-height: 22px;
      // position: relative;
      // bottom: 4px;
    }
  }
  .columns-body {
    width: 100%;
    .no-data_placeholder {
      color: @text-placeholder-color;
      font-size: 12px;
    }
  }
  .left, .right {
    display: inline-block;
    vertical-align: bottom;
    width: 49.79%;
  }
  .right {
    text-align: right;
  }
  .filter-input {
    width: 210px;
  }
  .cell {
    white-space: nowrap;
  }
}
</style>
