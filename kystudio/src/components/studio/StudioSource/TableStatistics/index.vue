<template>
  <div class="table-statistics">
    <div class="columns-header">
      <div class="left font-medium">
        {{$t('total') + columns.length}}
      </div>
      <div class="right">
        <el-input
          class="filter-input"
          size="small"
          prefix-icon="el-ksd-icon-search_22"
          v-model="filterText"
          :placeholder="$t('kylinLang.common.pleaseFilter')">
        </el-input>
      </div>
    </div>
    <el-table class="columns-body" :empty-text="emptyText" :data="currentColumns" border>
      <el-table-column
        type="index"
        label="ID"
        align="center"
        width="58px"
        :index="startIndex">
      </el-table-column>
      <el-table-column
        prop="name"
        align="left"
        :label="$t('kylinLang.dataSource.columnName')">
      </el-table-column>
      <el-table-column
        prop="cardinality"
        align="right"
        header-align="right"
        min-width="120px"
        :label="$t('kylinLang.dataSource.cardinality')">
      </el-table-column>
      <el-table-column
        prop="max_value"
        width="100px"
        show-overflow-tooltip
        :label="$t('kylinLang.dataSource.maximum')">
      </el-table-column>
      <el-table-column
        prop="min_value"
        show-overflow-tooltip
        width="100px"
        :label="$t('kylinLang.dataSource.minimal')">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        prop="max_length_value"
        :label="$t('kylinLang.dataSource.maxLengthVal')">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        prop="min_length_value"
        :label="$t('kylinLang.dataSource.minLengthVal')">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        width="120px"
        prop="null_count"
        :label="$t('kylinLang.dataSource.nullCount')">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        width="120px"
        prop="exceed_precision_max_length_value"
        :label="$t('kylinLang.dataSource.exceedPrecisionMaxLengthValue')">
      </el-table-column>
    </el-table>
    <kylin-pager
      class="ksd-center ksd-mtb-10" ref="pager"
      :refTag="pageRefTags.statisticsPager"
      :totalSize="columns.length"
      :perPageSize="pagination.pageSize"
      @handleCurrentChange="handleCurrentChange">
    </kylin-pager>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { pageRefTags, pageCount } from 'config'
import locales from './locales'

@Component({
  props: {
    table: {
      type: Object
    }
  },
  locales
})
export default class TableStatistics extends Vue {
  pageRefTags = pageRefTags
  filterText = ''
  pagination = {
    page_offset: 0,
    pageSize: +localStorage.getItem(this.pageRefTags.statisticsPager) || pageCount
  }
  get emptyText () {
    return this.filterText ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get startIndex () {
    const { page_offset, pageSize } = this.pagination
    return page_offset * pageSize + 1
  }
  get columns () {
    return this.table.columns
      .filter(column => column.name.toUpperCase().includes(this.filterText.toUpperCase()))
  }
  get currentColumns () {
    const { page_offset, pageSize } = this.pagination
    return this.columns.slice(page_offset * pageSize, page_offset * pageSize + pageSize)
  }
  handleCurrentChange (page_offset, pageSize) {
    this.pagination.page_offset = page_offset
    this.pagination.pageSize = pageSize
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-statistics {
  padding: 0 0 20px 0;
  .columns-header {
    margin-bottom: 8px;
    white-space: nowrap;
  }
  .columns-body {
    width: 100%;
  }
  .left, .right {
    display: inline-block;
    vertical-align: middle;
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
  .el-pagination {
    margin-top: 30px;
  }
}
</style>
