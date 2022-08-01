<template>
  <el-dialog class="batch-measure-modal" width="960px"
    :title="$t('batchMeasure')"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    limited-area
    append-to-body
    ref="batchMeasures"
    top="5vh"
    v-event-stop
    @close="isShow && handleClose(false)">
    <template v-if="isFormShow">
      <div class="batch-des">{{$t('batchMeasureDes')}}</div>
      <div class="ksd-mb-10 ksd-right">
        <el-button @click="changeSyncName" class="sync-btn">{{!syncComment ? $t('syncName') : $t('resetSyncName')}}</el-button>
        <el-input :placeholder="$t('searchColumn')" style="width:230px;" @input="changeSearchVal" v-model="searchChar">
          <i slot="prefix" class="el-input__icon el-ksd-icon-search_22"></i>
        </el-input>
      </div>
      <div v-if="!searchChar">
        <!-- 事实表 -->
        <div v-for="(table, index) in factTable" class="ksd-mb-10 scroll-table-item" :key="index">
          <div @click="toggleTableShow(table)" class="table-header clearfix">
            <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
            <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
            <span class="ksd-ml-2 table-icon">
              <i class="el-icon-ksd-fact_table"></i>
            </span>
            <div class="table-title"><span class="measure-header-tip-layout">{{table.alias}}</span> <span>({{table.meaColNum}}/{{table.columns.length}})</span></div>
          </div>
          <el-table
            v-if="table.show || isGuideMode"
            :data="table.columns"
            :ref="table.guid">
            <!-- <el-table-column show-overflow-tooltip prop="name" :label="$t('name')"></el-table-column> -->
            <el-table-column show-overflow-tooltip prop="column" :label="$t('column')"></el-table-column>
            <el-table-column show-overflow-tooltip prop="datatype" width="110px" :label="$t('dataType')"></el-table-column>
            <el-table-column prop="SUM" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.SUM.value" @change="handleChange(scope.row, table, 'SUM')" :disabled="scope.row.SUM.isShouldDisable"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MIN" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MIN.value" @change="handleChange(scope.row, table, 'MIN')" :disabled="scope.row.MIN.isShouldDisable"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MAX" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MAX.value" @change="handleChange(scope.row, table, 'MAX')" :disabled="scope.row.MAX.isShouldDisable"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="COUNT" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.COUNT.value" @change="handleChange(scope.row, table, 'COUNT')" :disabled="scope.row.COUNT.isShouldDisable"></el-checkbox>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <!-- 维度表 -->
        <div v-for="(table, index) in lookupTable" :class="['ksd-mb-10', 'scroll-table-item']" :key="index">
          <div @click="toggleTableShow(table)" class="table-header clearfix">
            <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
            <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
            <span class="ksd-ml-2 table-icon">
              <i class="el-icon-ksd-lookup_table"></i>
            </span>
            <div class="table-title">
              <span class="measure-header-tip-layout">{{table.alias}}</span> <span>({{table.meaColNum}}/{{table.columns.length}})</span>
              <common-tip placement="top" v-if="flattenLookupTables.includes(table.alias)" :content="$t('lockLookupMeasureTableTip')">
                <i class="el-icon-ksd-what ksd-ml-5"></i>
              </common-tip>
            </div>
          </div>
          <el-table
            v-if="table.show || isGuideMode"
            :class="{'disabled-checkbox': flattenLookupTables.includes(table.alias)}"
            :data="table.columns"
            :ref="table.guid">
            <el-table-column show-overflow-tooltip prop="name" :label="$t('name')"></el-table-column>
            <el-table-column show-overflow-tooltip prop="column" :label="$t('column')"></el-table-column>
            <el-table-column show-overflow-tooltip prop="datatype" width="110px" :label="$t('dataType')"></el-table-column>
            <el-table-column prop="SUM" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.SUM.value" @change="handleChange(scope.row, table, 'SUM')" :disabled="scope.row.SUM.isShouldDisable || flattenLookupTables.includes(table.alias)"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MIN" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MIN.value" @change="handleChange(scope.row, table, 'MIN')" :disabled="scope.row.MIN.isShouldDisable || flattenLookupTables.includes(table.alias)"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MAX" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MAX.value" @change="handleChange(scope.row, table, 'MAX')" :disabled="scope.row.MAX.isShouldDisable || flattenLookupTables.includes(table.alias)"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="COUNT" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.COUNT.value" @change="handleChange(scope.row, table, 'COUNT')" :disabled="scope.row.COUNT.isShouldDisable || flattenLookupTables.includes(table.alias)"></el-checkbox>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <!-- 可计算列 -->
        <template v-if="ccTable.columns.length">
          <div class="ksd-mb-10 scroll-table-item" v-for="ccTable in [ccTable]" :key="ccTable.guid">
            <div @click="toggleTableShow(ccTable)" class="table-header">
              <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!ccTable.show"></i>
              <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
              <span class="ksd-ml-2">
                <i class="el-ksd-icon-auto_computed_column_old"></i>
              </span>
              <span class="table-title">{{$t('computedColumns')}} <span>({{ccTable.meaColNum}}/{{ccTable.columns.length}})</span></span>
              <common-tip placement="top" v-if="unflattenComputedColumns.length" :content="$t('useCCBylockLookupTableTip')">
                <i class="el-icon-ksd-what ksd-ml-5"></i>
              </common-tip>
            </div>
            <el-table
              v-show="ccTable.show || isGuideMode"
              :data="ccTable.columns"
              :ref="ccTable.guid">
              <el-table-column show-overflow-tooltip prop="name" :label="$t('column')"></el-table-column>
              <el-table-column show-overflow-tooltip prop="datatype" width="110px" :label="$t('dataType')"></el-table-column>
              <el-table-column prop="SUM" :renderHeader="(h, obj) => {return renderColumn(h, obj, ccTable)}" align="center">
                <template slot-scope="scope">
                  <el-checkbox v-model="scope.row.SUM.value" @change="handleChange(scope.row, ccTable, 'SUM')" :disabled="scope.row.SUM.isShouldDisable || unflattenComputedColumns.includes(scope.row.columnName)"></el-checkbox>
                </template>
              </el-table-column>
              <el-table-column prop="MIN" :renderHeader="(h, obj) => {return renderColumn(h, obj, ccTable)}" align="center">
                <template slot-scope="scope">
                  <el-checkbox v-model="scope.row.MIN.value" @change="handleChange(scope.row, ccTable, 'MIN')" :disabled="scope.row.MIN.isShouldDisable || unflattenComputedColumns.includes(scope.row.columnName)"></el-checkbox>
                </template>
              </el-table-column>
              <el-table-column prop="MAX" :renderHeader="(h, obj) => {return renderColumn(h, obj, ccTable)}" align="center">
                <template slot-scope="scope">
                  <el-checkbox v-model="scope.row.MAX.value" @change="handleChange(scope.row, ccTable, 'MAX')" :disabled="scope.row.MAX.isShouldDisable || unflattenComputedColumns.includes(scope.row.columnName)"></el-checkbox>
                </template>
              </el-table-column>
              <el-table-column prop="COUNT" :renderHeader="(h, obj) => {return renderColumn(h, obj, ccTable)}" align="center">
                <template slot-scope="scope">
                  <el-checkbox v-model="scope.row.COUNT.value" @change="handleChange(scope.row, ccTable, 'COUNT')" :disabled="scope.row.COUNT.isShouldDisable || unflattenComputedColumns.includes(scope.row.columnName)"></el-checkbox>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </template>
      </div>
      <div v-else>
        <div v-for="searchTable in pagerSearchTable()" class="scroll-table-item" :key="searchTable.guid">
          {{searchTable.table_alias}}
          <el-table
            :empty-text="emptyText"
            :key="searchTable.guid"
            :ref="searchTable.guid"
            :data="searchTable.columns">
            <el-table-column show-overflow-tooltip prop="name" :label="$t('column')"></el-table-column>
            <el-table-column show-overflow-tooltip prop="table_alias" :label="$t('table')"></el-table-column>
            <el-table-column prop="SUM" :renderHeader="(h, obj) => {return renderColumn(h, obj, searchTable)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.SUM.value" @change="handleChange(scope.row, searchTable, 'SUM')" :disabled="scope.row.SUM.isShouldDisable || flattenLookupTables.includes(scope.row.table_alias)"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MIN" :renderHeader="(h, obj) => {return renderColumn(h, obj, searchTable)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MIN.value" @change="handleChange(scope.row, searchTable, 'MIN')" :disabled="scope.row.MIN.isShouldDisable || flattenLookupTables.includes(scope.row.table_alias)"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MAX" :renderHeader="(h, obj) => {return renderColumn(h, obj, searchTable)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MAX.value" @change="handleChange(scope.row, searchTable, 'MAX')" :disabled="scope.row.MAX.isShouldDisable || flattenLookupTables.includes(scope.row.table_alias)"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="COUNT" :renderHeader="(h, obj) => {return renderColumn(h, obj, searchTable)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.COUNT.value" @change="handleChange(scope.row, searchTable, 'COUNT')" :disabled="scope.row.COUNT.isShouldDisable || flattenLookupTables.includes(scope.row.table_alias)"></el-checkbox>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <kylin-pager class="ksd-center ksd-mtb-10" ref="pager" :perPageSize="filterArgs.pageSize" :refTag="pageRefTags.batchMeasurePager" :curPage="filterArgs.pageOffset+1" :totalSize="searchTotalSize"  v-on:handleCurrentChange='pageCurrentChange'></kylin-pager>
      </div>
    </template>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { objectClone, sampleGuid } from '../../../../util'
import { pageCount, measuresDataType, measureSumAndTopNDataType, pageRefTags } from '../../../../config'
vuex.registerModule(['modals', 'BatchMeasureModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isGuideMode'
    ]),
    // Store数据注入
    ...mapState('BatchMeasureModal', {
      isShow: state => state.isShow,
      modelDesc: state => state.modelDesc,
      tables: state => objectClone(state.modelDesc && state.modelDesc.tables),
      usedColumns: state => state.modelDesc.all_measures,
      callback: state => state.callback
    }),
    ...mapState({
      otherColumns: state => state.model.otherColumns
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('BatchMeasureModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  locales
})
export default class BatchMeasureModal extends Vue {
  pageRefTags = pageRefTags
  isLoading = false
  isFormShow = false
  factTable = []
  lookupTable = []
  searchChar = ''
  expressions = ['SUM', 'MIN', 'MAX', 'COUNT']
  ST = null
  ccTable = {columns: []}
  filterArgs = {
    pageOffset: 0,
    pageSize: +localStorage.getItem(this.pageRefTags.batchMeasurePager) || pageCount
  }
  scrollDom = null
  tableHeaderTops = []
  scrollTableList = []
  targetFixedTable = null
  syncComment = false

  get flattenLookupTables () {
    return this.modelDesc.anti_flatten_lookups
  }

  get unflattenComputedColumns () {
    return this.modelDesc.anti_flatten_cc.map(it => it.columnName)
  }

  checkHasSameName (arr, val, column) {
    let flag = false
    for (let i = 0; i < arr.length; i++) {
      if (arr[i].name === val && (arr[i].table_guid || arr[i].guid) !== column.table_guid) {
        flag = true
        break
      }
    }
    return flag
  }
  submit () {
    let allMeasureArr = [...this.modelDesc.all_measures]
    let columns = []
    this.factTable.forEach((t) => {
      columns.push(...t.columns)
    })
    this.lookupTable.forEach((t) => {
      columns.push(...t.columns)
    })
    columns.push(...this.ccTable.columns)
    if (this.syncComment) {
      allMeasureArr.forEach(item => {
        if (item.parameter_value.length > 0) {
          const column = columns.filter(it => it.full_colname === item.parameter_value[0].value)
          column.length > 0 && column[0].comment && (item.comment = column[0].comment)
        }
      })
    }
    columns.forEach((column) => {
      if (column.isMeasureCol) {
        if (column.SUM.value && !column.SUM.isShouldDisable) {
          // 如果存在同名的，添加上表别名，如果不同名，就是列名+函数
          const measure = {
            name: this.checkHasSameName(allMeasureArr, column.name + '_SUM', column) ? column.name + '_SUM_' + column.table_alias : column.name + '_SUM',
            guid: sampleGuid(),
            expression: 'SUM',
            parameter_value: [{type: 'column', value: column.table_alias + '.' + (column.column ?? column.columnName)}],
            table_guid: column.table_guid,
            comment: this.syncComment ? column.comment : ''
          }
          allMeasureArr.push(measure)
        }
        if (column.MIN.value && !column.MIN.isShouldDisable) {
          const measure = {
            name: this.checkHasSameName(allMeasureArr, column.name + '_MIN', column) ? column.name + '_MIN_' + column.table_alias : column.name + '_MIN',
            guid: sampleGuid(),
            expression: 'MIN',
            parameter_value: [{type: 'column', value: column.table_alias + '.' + (column.column ?? column.columnName)}],
            table_guid: column.table_guid,
            comment: this.syncComment ? column.comment : ''
          }
          allMeasureArr.push(measure)
        }
        if (column.MAX.value && !column.MAX.isShouldDisable) {
          const measure = {
            name: this.checkHasSameName(allMeasureArr, column.name + '_MAX', column) ? column.name + '_MAX_' + column.table_alias : column.name + '_MAX',
            guid: sampleGuid(),
            expression: 'MAX',
            parameter_value: [{type: 'column', value: column.table_alias + '.' + (column.column ?? column.columnName)}],
            table_guid: column.table_guid,
            comment: this.syncComment ? column.comment : ''
          }
          allMeasureArr.push(measure)
        }
        if (column.COUNT.value && !column.COUNT.isShouldDisable) {
          const measure = {
            name: this.checkHasSameName(allMeasureArr, column.name + '_COUNT', column) ? column.name + '_COUNT_' + column.table_alias : column.name + '_COUNT',
            guid: sampleGuid(),
            expression: 'COUNT',
            parameter_value: [{type: 'column', value: column.table_alias + '.' + (column.column ?? column.columnName)}],
            table_guid: column.table_guid,
            comment: this.syncComment ? column.comment : ''
          }
          allMeasureArr.push(measure)
        }
      }
    })
    this.$set(this.modelDesc, 'all_measures', allMeasureArr)
    this.$emit('betchMeasures', allMeasureArr)
    this.handleClose(true)
  }

  // 整理没有选中做 measures 的列，同步注释时要用到
  getOtherColumns (columns) {
    let result = []
    columns.forEach((col) => {
      if (!col.isMeasureCol) {
        result.push({ name: col.name, column: `${col.table_alias}.${col.column}`, datatype: col.datatype })
      }
    })
    return result
  }

  handleClose (isSubmit, data) {
    this.hideModal()
    this.syncComment = false
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: data
      })
    }, 300)
  }
  changeSearchVal (val) {
    this.$nextTick(() => {
      this.searchChar = val && val.replace(/^\s+|\s+$/, '') || ''
      this.filterArgs.pageOffset = 0
      if (!this.searchChar) {
        this.factTable.forEach((t) => {
          t.meaColNum = t.columns.filter((col) => {
            return col.isMeasureCol
          }).length
        })
        this.lookupTable.forEach((t) => {
          t.meaColNum = t.columns.filter((col) => {
            return col.isMeasureCol
          }).length
        })
        this.ccTable.meaColNum = this.ccTable.columns.filter((col) => {
          return col.isMeasureCol
        }).length
      }
    })
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      this.searchChar = ''
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
      this.getRenderMeasureData()
      this.$nextTick(() => {
        this.listenEvent()
      })
    } else {
      this.scrollDom && this.scrollDom.removeEventListener('scroll', this.addScrollEvent)
      setTimeout(() => {
        this.isFormShow = false
      }, 200)
    }
  }
  @Watch('searchChar')
  onSearchCharChange (newVal, oldVal) {
    if (!oldVal && newVal) {
      this.$nextTick(() => {
        this.getTableTops()
      })
    } else if (!newVal && oldVal) {
      this.$nextTick(() => {
        this.getTableTops()
      })
    }
  }
  get emptyText () {
    return this.searchChar ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  // 分页搜索table渲染数据
  pagerSearchTable () {
    return [{
      guid: sampleGuid(),
      columns: this.pagerSearchMeasureList,
      show: true,
      nums: {sumNum: 0, minNum: 0, maxNum: 0, countNum: 0}
    }]
  }
  filterMeasureColumns (table) {
    let columns
    if (this.searchChar) {
      let searchReg = new RegExp(this.searchChar, 'i')
      columns = table.columns && table.columns.filter((col) => {
        return searchReg.test(col.name) || searchReg.test(col.column) // column for cc list search
      })
    } else {
      columns = table.columns
    }
    return columns
  }
  // 全量获取搜索columns
  get searchColumns () {
    let columns = []
    this.factTable.forEach((t) => {
      columns.push(...this.filterMeasureColumns(t))
    })
    this.lookupTable.forEach((t) => {
      columns.push(...this.filterMeasureColumns(t))
    })
    columns.push(...this.filterMeasureColumns(this.ccTable))
    return columns
  }
  // 分页获取搜索columns
  get pagerSearchMeasureList () {
    return this.searchColumns.slice(this.filterArgs.pageOffset * this.filterArgs.pageSize, (this.filterArgs.pageOffset + 1) * this.filterArgs.pageSize)
  }
  pageCurrentChange (size, count) {
    this.filterArgs.pageOffset = size
    this.filterArgs.pageSize = count
  }
  // 总搜索条数
  get searchTotalSize () {
    return this.searchColumns.length
  }
  // 渲染之前选过的可计算列measure
  getRenderCCData () {
    this.ccTable = {}
    this.$set(this.ccTable, 'show', false)
    this.$set(this.ccTable, 'guid', sampleGuid())
    this.ccTable.columns = objectClone(this.modelDesc.computed_columns) || []
    let meaColNum = 0
    const nums = {sumNum: 0, minNum: 0, maxNum: 0, countNum: 0}
    let filterColumns = []
    this.ccTable.columns.forEach((col) => {
      const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
      const returnValue = returnRegex.exec(col.datatype)
      this.$set(col, 'SUM', {isShouldDisable: false, value: false})
      this.$set(col, 'MIN', {isShouldDisable: false, value: false})
      this.$set(col, 'MAX', {isShouldDisable: false, value: false})
      this.$set(col, 'COUNT', {isShouldDisable: false, value: false})
      this.$set(col, 'isAllSelected', false)
      this.$set(col, 'isMeasureCol', false)
      this.$set(col, 'table_guid', this.factTable[0].guid)
      this.$set(col, 'table_alias', this.factTable[0].alias)
      this.$set(col, 'name', col.columnName)
      const len = this.usedColumns.length
      for (let i = 0; i < len; i++) {
        let d = this.usedColumns[i]
        if (this.expressions.indexOf(d.expression) !== -1 && d.parameter_value[0].value === col.table_alias + '.' + col.name) {
          col[d.expression].value = true
          col[d.expression].isShouldDisable = true
          this.$set(col, 'isMeasureCol', true)
          nums[d.expression.toLowerCase() + 'Num']++
        }
      }
      if (measureSumAndTopNDataType.indexOf(returnValue[1].toLocaleLowerCase()) === -1) {
        col.SUM.isShouldDisable = true
      }
      if (col.isMeasureCol) {
        meaColNum++
      }
      filterColumns.push(col)
    })
    this.ccTable.columns = filterColumns
    this.$set(this.ccTable, 'nums', nums)
    this.$set(this.ccTable, 'meaColNum', meaColNum)
  }
  // 获取所有的table columns，并渲染已经选择过的measure
  getRenderMeasureData () {
    this.factTable = []
    this.lookupTable = []
    Object.values(this.tables).forEach((table) => {
      if (table.kind === 'FACT') {
        this.factTable.push(table)
      } else {
        this.lookupTable.push(table)
      }
      this.$set(table, 'show', false)
      let meaColNum = 0
      const nums = {sumNum: 0, minNum: 0, maxNum: 0, countNum: 0}
      // 将已经选上的measure回显到界面上
      let filterColumns = []
      table.columns && table.columns.forEach((col) => {
        const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
        const returnValue = returnRegex.exec(col.datatype)
        if (measuresDataType.indexOf(returnValue[1]) >= 0) {
          this.$set(col, 'SUM', {isShouldDisable: false, value: false})
          this.$set(col, 'MIN', {isShouldDisable: false, value: false})
          this.$set(col, 'MAX', {isShouldDisable: false, value: false})
          this.$set(col, 'COUNT', {isShouldDisable: false, value: false})
          this.$set(col, 'isAllSelected', false)
          this.$set(col, 'isMeasureCol', false)
          this.$set(col, 'table_guid', table.guid)
          this.$set(col, 'table_alias', table.alias)
          const len = this.usedColumns.length
          const selectedColumns = this.usedColumns.filter(it => this.expressions.indexOf(it.expression) !== -1).map(item => item.parameter_value[0].value)
          let others = this.otherColumns.length ? this.otherColumns : this.modelDesc.all_named_columns.filter(item => !selectedColumns.includes(item.column))
          for (let i = 0; i < len; i++) {
            let d = this.usedColumns[i]
            if (this.expressions.indexOf(d.expression) !== -1 && d.parameter_value[0].value === table.alias + '.' + col.column) {
              let regxt = new RegExp(`\\_${d.expression}$`)
              let name = d.name.trim() ? d.name.replace(regxt, '') : col.name
              col[d.expression].value = true
              col[d.expression].isShouldDisable = true
              this.$set(col, 'isMeasureCol', true)
              this.$set(col, 'name', name)
              nums[d.expression.toLowerCase() + 'Num']++
            }
          }
          for (let it of others) {
            if (`${col.table_alias}.${col.column}` === it.column && !selectedColumns.includes(it.column)) {
              this.$set(col, 'name', it.name.trim() ? it.name : col.column)
              break
            }
          }
          if (measureSumAndTopNDataType.indexOf(returnValue[1]) === -1) {
            col.SUM.isShouldDisable = true
          }
          if (col.isMeasureCol) {
            meaColNum++
          }
          filterColumns.push(col)
        }
      })
      table.columns = filterColumns
      this.$set(table, 'nums', nums)
      this.$set(table, 'meaColNum', meaColNum)
    })
    this.getRenderCCData()
  }
  toggleTableShow (table) {
    table.show = !table.show
    this.$nextTick(() => {
      this.getTableTops()
    })
  }
  handleChange (row, table, property) {
    if (!(row.SUM.isShouldDisable && row.MIN.isShouldDisable && row.MAX.isShouldDisable && row.COUNT.isShouldDisable)) {
      if (row[property].value) {
        const cloneRow = objectClone(row)
        cloneRow[property].value = !cloneRow[property].value
        if (!(cloneRow.SUM.value || cloneRow.MIN.value || cloneRow.MAX.value || cloneRow.COUNT.value)) {
          table.meaColNum++
          row.isMeasureCol = true
        }
      } else {
        if (!(row.SUM.value || row.MIN.value || row.MAX.value || row.COUNT.value)) {
          table.meaColNum--
          row.isMeasureCol = false
        }
      }
    }
  }
  renderColumn (h, { column, store }, table) {
    table = table || {}
    let totalNums = 0
    const len = store.states.data.length
    for (let i = 0; i < len; i++) {
      let d = store.states.data[i]
      if (d[column.property].value) {
        totalNums++
      }
    }
    this.$set(column, 'totalNums', totalNums)
    if (totalNums === len) {
      column.isAllSelected = true
    }
    const toggleAllMeasures = (val) => {
      table.columns.forEach((d) => {
        if (val) {
          if (!d[column.property].isShouldDisable && !this.flattenLookupTables.includes(d.table_alias) && !this.unflattenComputedColumns.includes(d.columnName)) {
            d[column.property] = {value: val, isShouldDisable: d[column.property].isShouldDisable}
            d.isMeasureCol = true
          }
        } else {
          if (!d[column.property].isShouldDisable && !this.flattenLookupTables.includes(d.table_alias) && !this.unflattenComputedColumns.includes(d.columnName)) {
            d[column.property] = {value: val, isShouldDisable: d[column.property].isShouldDisable}
          }
          if (!(d.SUM.value || d.MIN.value || d.MAX.value || d.COUNT.value)) {
            d.isMeasureCol = false
          }
        }
      })
      column.isAllSelected = !column.isAllSelected
      const numArr = table.columns.filter(it => it.isMeasureCol).length
      this.$set(table, 'meaColNum', numArr)
    }
    totalNums === 0 && (column.isAllSelected = false)
    return (<span class="layout">
      <el-checkbox
        disabled={ store.states.data && store.states.data.length === 0 }
        indeterminate={ totalNums > 0 && len > totalNums}
        onChange={ toggleAllMeasures }
        value={ column.isAllSelected }></el-checkbox> <span class="title">{ column.property }({column.totalNums}/{len})</span>
    </span>)
  }
  getTableTops () {
    this.scrollTableList = this.$el.querySelectorAll('.scroll-table-item')
    this.tableHeaderTops = []
    if (this.scrollTableList.length) {
      for (let item of this.scrollTableList) {
        this.tableHeaderTops.push([item.offsetTop, item.offsetHeight])
      }
    }
  }
  listenEvent () {
    if (!this.$refs.batchMeasures.$el.children.length) return
    for (let item of this.$refs.batchMeasures.$el.children[0].children) {
      item.classList[0].indexOf('el-dialog__body') > -1 && (item.addEventListener('scroll', this.addScrollEvent), this.scrollDom = item)
    }
    this.getTableTops()
  }
  addScrollEvent (e) {
    try {
      const top = e.target.scrollTop
      const list = this.tableHeaderTops.filter(it => it[0] < top)
      const overCurrentHeightList = !this.searchChar ? this.tableHeaderTops.filter(it => it[0] + it[1] < top + 80) : []

      if (this.scrollTableList.length) {
        if (!list.length) {
          this.targetFixedTable && this.targetFixedTable.length && (this.targetFixedTable[0].style.cssText = '', this.targetFixedTable[0].nextSibling.style.cssText = '', this.targetFixedTable = null)
          return
        }
        const scrollTable = document.querySelectorAll('.scroll-table-item')

        for (var index = 0; index < this.scrollTableList.length; index++) {
          const target = scrollTable[index].getElementsByClassName('el-table__header-wrapper')

          if (!target.length) continue

          if (index === list.length - 1 && !overCurrentHeightList[index]) {
            this.targetFixedTable = target
            target[0].style.cssText = 'position:fixed;top:calc(5vh + 47px);z-index:10;'
            target[0].nextSibling.style.cssText = 'margin-top:36px;'
          } else {
            target[0].style.cssText = ''
            target[0].nextSibling.style.cssText = ''
          }
        }
      }
    } catch (e) {
      console.error(e)
    }
  }
  // 同步注释到名称
  changeSyncName () {
    // this.factTable.forEach((item, index) => {
    //   item.columns.forEach((it, idx) => {
    //     if (!this.syncComment) {
    //       it.name = it.comment && it.comment.trim() ? it.comment.slice(0, 100) : it.name
    //     } else {
    //       this.usedColumns.filter(useColumn => this.expressions.indexOf(useColumn.expression) !== -1 && useColumn.parameter_value[0].value === item.alias + '.' + it.column).length === 0 && (it.name = it.column || '')
    //     }
    //     this.$set(this.factTable[index].columns[idx], 'name', it.name)
    //   })
    // })
    // this.lookupTable.forEach((item, index) => {
    //   item.columns.forEach((it, idx) => {
    //     if (!this.syncComment) {
    //       it.name = it.comment && it.comment.trim() ? it.comment.slice(0, 100) : it.name
    //     } else {
    //       this.usedColumns.filter(useColumn => this.expressions.indexOf(useColumn.expression) !== -1 && useColumn.parameter_value[0].value === item.alias + '.' + it.column).length === 0 && (it.name = it.column || '')
    //     }
    //     this.$set(this.lookupTable[index].columns[idx], 'name', it.name)
    //   })
    // })
    this.syncComment = !this.syncComment
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
  .batch-measure-modal {
    .el-table.disabled-checkbox {
      .el-table__header {
        .el-checkbox {
          pointer-events: none;
          .el-checkbox__inner {
            background-color: @background-disabled-color;
            border-color: @line-border-color3;
          }
        }
      }
    }
    .el-table__header {
      .layout {
        font-size: 0;
        .title {
          vertical-align: middle;
          line-height: 23px;
          font-size: 14px;
          margin-left: 5px;
        }
      }
    }
    .batch-des {
      color: @text-title-color;
      font-size: 14px;
    }
    .sync-btn {
      vertical-align: top;
    }
    .table-title {
      font-size: 14px;
      margin-left: 5px;
      font-weight: @font-medium;
    }
    .table-header {
      padding-left:15px;
      background-color: @regular-background-color;
      &:hover {
        color: @base-color;
        .right-icon {
          color:@base-color-2!important;
          cursor: pointer;
        }
      }
      height:40px;
      line-height:40px;
      cursor:pointer;
      .right-icon{
        margin-right:20px;
      }
    }
    .table-icon {
      float: left;
    }
    .measure-header-tip-layout {
      max-width: calc(~'100% - 140px');
      display: inline-block;
      text-overflow: ellipsis;
      overflow: hidden;
      float: left;
      margin-left: 10px;
      margin-right: 5px;
    }
  }
</style>
