<template>
  <div class="table-sample">
    <div v-if="table.last_build_job_id || pagedColumns.length">
      <div class="columns-header">
        <div class="left font-medium">
          {{$t('version')}}{{table.create_time | toGMTDate}}
        </div>
        <div class="right">
          <el-input size="small" clearable class="filter-input ksd-ml-10" :placeholder="$t('filterColumns')"
            prefix-icon="el-ksd-icon-search_22" v-bind:value="filterText" @input="filterColumns"></el-input>
          <el-select size="medium" class="filter-input column-filter-input" value-key="range" v-model="samplePageRange" v-if="pagerSampleOptions.length">
            <el-option v-for="pager in pagerSampleOptions" :key="pager.label" :value="pager" :label="$t('pageRange', {pageRange: pager.label})"></el-option>
          </el-select>
        </div>
      </div>
      <el-table class="columns-body" ref="sampleTable" :data="pagedColumns" v-scroll-shadow :emptyText="$t('noSamplingData')" v-if="pagedHeaders.length">
        <el-table-column
          align="left"
          show-overflow-tooltip
          v-for="(headerText, index) in pagedHeaders"
          :key="headerText"
          :prop="String(index)"
          :min-width="getStringWidth(headerText)"
          :label="headerText">
        </el-table-column>
      </el-table>
    </div>
    <kylin-empty-data v-if="!table.last_build_job_id&&!pagedColumns.length" :content="$t('noSampling')">
    </kylin-empty-data>
    <kylin-nodata v-if="(table.last_build_job_id || pagedColumns.length)&&!headers.length&&this.filterText" :content="$t('kylinLang.common.noResults')"></kylin-nodata>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'

@Component({
  props: {
    table: {
      type: Object
    }
  },
  locales
})
export default class TableSamples extends Vue {
  filterText = ''
  currentPage = 1
  perPageSize = 50
  samplePageRange = ''
  ST = null
  get headers () {
    return this.table.__data.columns && this.table.__data.columns.length ? this.table.__data.columns
      .map(column => column.name)
      .filter(column => column.toUpperCase().includes(this.filterText.toUpperCase())) : []
  }
  get columnCount () {
    return this.columns[0] ? this.columns[0].length : 0
  }
  get columns () {
    let columns = []
    if (this.table.sampling_rows) {
      columns = this.table.sampling_rows
      Array.isArray(columns) && columns.forEach((item, index) => {
        Array.isArray(item) && item.forEach((it, idx) => {
          Object.prototype.toString.call(it) === '[object Null]' && (columns[index][idx] = '<NULL>')
        })
      })
      const headerIdxs = []
      this.table.__data.columns.forEach((column, index) => {
        if (this.headers.includes(column.name)) {
          headerIdxs.push(index)
        }
      })
      columns = columns.map(column => column.filter((value, index) => headerIdxs.includes(index)))
    }
    return columns
  }
  getStringWidth (string = '') {
    return 15 * string.length + 20
  }
  pageCurrentChange (curPage) {
    this.currentPage = curPage
  }
  filterColumns (val) {
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      this.filterText = val
    }, 200)
  }
  get pagerSampleOptions () {
    let result = []
    if (this.headers && this.headers.length) {
      let headerCount = this.headers.length
      let perPageCount = this.perPageSize
      let count = parseInt(headerCount / perPageCount)
      let residueCount = headerCount % perPageCount
      if (count) {
        for (let i = 0; i < count; i++) {
          result.push({
            label: (perPageCount * i + 1) + '-' + (i + 1) * perPageCount,
            range: [perPageCount * i + 1, (i + 1) * perPageCount]
          })
        }
        if (residueCount) {
          result.push({
            label: count * perPageCount + 1 === headerCount ? headerCount : (count * perPageCount + 1) + '-' + headerCount,
            range: [count * perPageCount + 1, headerCount]
          })
        }
      }
    }
    this.samplePageRange = result[0]
    return result
  }
  get pagedColumns () {
    let result = []
    if (this.samplePageRange && this.samplePageRange.range) {
      result = this.columns.map((row) => {
        return row.slice(this.samplePageRange.range[0] - 1, this.samplePageRange.range[1])
      })
    } else {
      result = this.columns
    }
    this.$nextTick(() => {
      if (this.$refs.sampleTable) {
        this.$refs.sampleTable.bodyWrapper.scrollLeft = 0
      }
    })
    return result
  }
  get pagedHeaders () {
    if (this.samplePageRange) {
      return this.headers.slice(this.samplePageRange.range[0] - 1, this.samplePageRange.range[1])
    }
    return this.headers
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-sample {
  padding: 0 0 20px 0;
  .columns-header {
    margin-bottom: 10px;
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
  .left {
    vertical-align: bottom;
    font-size: 12px;
    font-weight: normal;
    height: 14px;
    line-height: 14px;
  }
  .right {
    text-align: right;
  }
  .filter-input {
    width: 210px;
    float: right;
  }
  .column-filter-input {
    width: 180px;
  }
  .cell {
    white-space: nowrap;
  }
  .el-pagination {
    margin-top: 30px;
  }
}
</style>
