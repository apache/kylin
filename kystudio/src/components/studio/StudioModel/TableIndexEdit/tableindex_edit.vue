<template>
  <!-- tableindex的添加和编辑 -->
  <el-dialog :title="tableIndexModalTitle" append-to-body limited-area top="5vh" class="table-edit-dialog" width="880px" v-if="isShow" :visible="true" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <!-- <el-form :model="tableIndexMeta" :rules="rules" ref="tableIndexForm" label-position="top">
        <el-form-item :label="$t('tableIndexName')" prop="name">
          <el-input v-focus="isShow" v-model="tableIndexMeta.name" auto-complete="off" placeholder="" size="medium" style="width:500px"></el-input>
        </el-form-item>
      </el-form> -->
      <!-- <div class="ky-line ksd-mtb-10"></div> -->
      <div class="table-index-list">
        <div class="ksd-mb-10" v-if="modelInstance.model_type === 'HYBRID'">
          <h4>
            <span class="is-required">*</span>
            {{$t('indexTimeRange')}}
            <common-tip :content="$t('indexTimeRangeTips')"><i class="el-ksd-icon-more_info_16 ksd-fs-16"></i></common-tip>
          </h4>
          <el-radio-group v-model="tableIndexMeta.index_range" :disabled="tableIndexMeta.id !== ''" @change="changeTableIndexType">
            <el-tooltip placement="top" :disabled="indexUpdateEnabled" :content="$t('refuseAddIndexTip')">
              <el-radio :label="'HYBRID'" :disabled="!indexUpdateEnabled">{{$t('kylinLang.common.HYBRID')}}</el-radio>
            </el-tooltip>
            <el-radio :label="'BATCH'">{{$t('kylinLang.common.BATCH')}}</el-radio>
            <el-tooltip placement="top" :disabled="indexUpdateEnabled" :content="$t('refuseAddIndexTip')">
              <el-radio :label="'STREAMING'" :disabled="!indexUpdateEnabled">{{$t('kylinLang.common.STREAMING')}}</el-radio>
            </el-tooltip>
          </el-radio-group>
        </div>
        <!-- <el-button type="primary" plain size="medium" @click="selectAll">{{$t('selectAllColumns')}}</el-button><el-button plain size="medium" @click="clearAll">{{$t('clearAll')}}</el-button> -->
        <div class="header">
          <h4 class="ksd-left" v-if="modelInstance.model_type === 'HYBRID'">{{$t('includeColumns')}}</h4>
          <el-alert
            :title="$t('tableIndexShardByTips')"
            type="info"
            :closable="false"
            :show-background="false"
            show-icon>
          </el-alert>
          <template v-if="modelInstance.model_type !== 'HYBRID' || modelInstance.model_type === 'HYBRID' && tableIndexMeta.index_range">
            <p class="anit-table-tips" v-if="hasManyToManyAndAntiTable">{{$t('manyToManyAntiTableTip')}}</p>
            <el-tooltip :content="$t('excludeTableCheckboxTip')" effect="dark" placement="top"><el-checkbox class="ksd-mr-5" v-if="showExcludedTableCheckBox" v-model="displayExcludedTables">{{$t('excludeTableCheckbox')}}</el-checkbox></el-tooltip>
            <el-input v-model="searchColumn" size="medium" prefix-icon="el-ksd-icon-search_22" style="width:200px" :placeholder="$t('filterByColumns')"></el-input>
          </template>
        </div>
        <div class="no-index-range" v-if="modelInstance.model_type === 'HYBRID' && !tableIndexMeta.index_range">
          <span>{{$t('noIndexRangeByHybrid')}}</span>
        </div>
        <div class="ky-simple-table" v-else>
          <el-row class="table-header table-row ksd-mt-10">
            <el-col :span="1"><el-checkbox v-model="isSelectAllTableIndex" :indeterminate="getSelectedColumns.length !== 0 && allColumns.length > getSelectedColumns.length" @change="selectAllTableIndex" size="small" /></el-col>
            <el-col :span="14" class="column-name">{{$t('kylinLang.model.columnName')}}</el-col>
            <el-col :span="3" class="cardinality-item">{{$t('cardinality')}}</el-col>
            <el-col :span="3">ShardBy</el-col>
            <el-col :span="3">{{$t('order')}}</el-col>
          </el-row>
          <div class="table-content table-index-layout" v-scroll.observe.reactive @scroll-bottom="scrollLoad">
            <transition-group name="flip-list" tag="div">
                <el-row v-for="(col, index) in searchAllColumns" :key="col.fullName" class="table-row">
                  <el-col :span="1"><el-checkbox size="small" :disabled="getDisabledTableType(col)" v-model="col.isUsed" @change="(status) => selectTableIndex(status, col)" /></el-col>
                  <el-col :span="14" class="column-name" :title="col.fullName">{{col.fullName}}<el-tooltip :content="$t('excludedTableIconTip')" effect="dark" placement="top"><i class="excluded_table-icon el-icon-ksd-exclude" v-if="isExistExcludeTable(col) && displayExcludedTables"></i></el-tooltip></el-col>
                  <el-col :span="3" class="cardinality-item">
                    <template v-if="col.cardinality === null"><i class="no-data_placeholder">NULL</i></template>
                    <template v-else>{{ col.cardinality }}</template>
                  </el-col>
                  <el-col :span="3" @click.native="toggleShard(col)">
                     <i class="el-icon-success" v-if="col.isUsed" :class="{active: col.isShared}"></i>
                  </el-col>
                  <el-col :span="3" class="order-actions">
                    <template  v-if="col.isUsed">
                      <el-tooltip :content="$t('moveTop')" effect="dark" placement="top">
                        <span :class="['icon', 'el-icon-ksd-move_to_top', {'is-disabled': index === 0 && !searchColumn}]" @click="topRow(col)"></span>
                      </el-tooltip>
                      <el-tooltip :content="$t('moveUp')" effect="dark" placement="top">
                        <span :class="['icon', 'el-icon-ksd-move_up', {'is-disabled': index === 0}]" @click="upRow(col)"></span>
                      </el-tooltip>
                      <el-tooltip :content="$t('moveDown')" effect="dark" placement="top">
                        <span :class="['icon', 'el-icon-ksd-move_down', {'is-disabled': !searchAllColumns[index + 1] || !searchAllColumns[index + 1].isUsed}]" @click="downRow(col)"></span>
                      </el-tooltip>
                    </template>
                  </el-col>
                </el-row>
              </transition-group>
          </div>
       </div>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <!-- <el-checkbox v-model="tableIndexMeta.load_data" :label="true" class="ksd-fleft ksd-mt-8">{{$t('catchup')}}</el-checkbox> -->
        <el-button :type="onlyBatchType ? 'primary' : ''" :text="onlyBatchType" @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button :type="!onlyBatchType ? 'primary' : ''" :loading="btnLoading" size="medium" @click="submit(false)" :disabled="saveBtnDisable">{{$t('kylinLang.common.save')}}</el-button>
        <el-button v-if="onlyBatchType" type="primary" :loading="btnLoading" size="medium" @click="submit(true)" :disabled="saveBtnDisable">{{$t('saveAndBuild')}}</el-button>
      </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../store'
  import { NamedRegex } from 'config'
  import { BuildIndexStatus } from 'config/model'
  import { handleError, handleSuccess, kylinConfirm } from 'util/business'
  import { objectClone, changeObjectArrProperty, indexOfObjWithSomeKey, filterObjectArray } from 'util/index'
  import locales from './locales'
  import store, { types } from './store'

  vuex.registerModule(['modals', 'TableIndexEditModal'], store)

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('TableIndexEditModal', {
        isShow: state => state.isShow,
        isHybridBatch: state => state.isHybridBatch,
        modelInstance: state => state.form.data.modelInstance,
        tableIndexDesc: state => objectClone(state.form.data.tableIndexDesc),
        indexUpdateEnabled: state => state.form.data.indexUpdateEnabled,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        editTableIndex: 'EDIT_TABLE_INDEX',
        addTableIndex: 'ADD_TABLE_INDEX'
      }),
      ...mapMutations('TableIndexEditModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class TableIndexEditModal extends Vue {
    btnLoading = false
    openShared = false
    searchColumn = ''
    allColumns = []
    currentPager = 1
    pagerSize = 50
    pager = 0
    tableIndexMetaStr = JSON.stringify({
      id: '',
      col_order: [],
      sort_by_columns: [],
      shard_by_columns: [],
      load_data: false,
      index_range: ''
    })
    tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
    rules = {
      name: [
        {validator: this.checkName, trigger: 'blur'}
      ]
    }
    cloneMeta = ''
    isSelectAllTableIndex = false
    displayExcludedTables = false

    @Watch('searchColumn')
    changeSearchColumn (val) {
      const dom = document.querySelector('.table-index-layout .scroll-content')
      dom && (dom.style = 'transform: translate3d(0px, 0px, 0px);')
    }

    get getSelectedColumns () {
      return this.allColumns.filter(it => it.isUsed)
    }

    get onlyBatchType () {
      return (this.modelInstance.model_type === 'HYBRID' && this.tableIndexMeta.index_range !== 'STREAMING') || (this.modelInstance.model_type !== 'STREAMING' && this.modelInstance.model_type !== 'HYBRID')
    }

    get showExcludedTableCheckBox () {
      return this.allColumns.length ? this.allColumns.filter(it => typeof it.depend_lookup_table !== 'undefined' && it.depend_lookup_table).length > 0 : false
    }
    topRow (col) {
      let index = this.getRowIndex(col, 'fullName')
      this.allColumns.splice(0, 0, col)
      this.allColumns.splice(index + 1, 1)
    }
    upRow (col) {
      let i = this.getRowIndex(col, 'fullName')
      this.allColumns.splice(i - 1, 0, col)
      this.allColumns.splice(i + 1, 1)
    }
    downRow (col) {
      let i = this.getRowIndex(col, 'fullName')
      this.allColumns.splice(i + 2, 0, col)
      this.allColumns.splice(i, 1)
    }
    getRowIndex (t, key) {
      return indexOfObjWithSomeKey(this.allColumns, key, t[key])
    }
    toggleShard (t) {
      let shardStatus = t.isShared
      changeObjectArrProperty(this.allColumns, '*', 'isShared', false)
      t.isShared = !shardStatus
    }
    scrollLoad () {
      if (this.searchAllColumns && this.searchAllColumns.length !== this.filterResult.length) {
        this.currentPager += 1
      }
    }
    // 是否存在多对多且被屏蔽的表
    get hasManyToManyAndAntiTable () {
      let flag = false
      for (let item of this.allColumns) {
        if (this.getDisabledTableType(item)) {
          flag = true
          break
        }
      }
      return flag
    }
    // 当表为屏蔽表且表关联关系为多对多时，不能作为维度添加到索引中
    getDisabledTableType (col) {
      if (!col) return false
      const [currentTable] = col.fullName.split('.')
      const { join_tables } = this.modelInstance
      const manyToManyTables = join_tables.filter(it => it.join_relation_type === 'MANY_TO_MANY').map(item => item.alias)
      return manyToManyTables.includes(currentTable) && this.isExistExcludeTable(col)
    }
    // 是否为屏蔽表的 column
    isExistExcludeTable (col) {
      return typeof col.depend_lookup_table !== 'undefined' ? col.depend_lookup_table : false
    }
    get filterResult () {
      if (!this.isShow) {
        return []
      }
      return this.allColumns.filter((col) => {
        if (this.displayExcludedTables) {
          return !this.searchColumn || col.fullName.toUpperCase().indexOf(this.searchColumn.toUpperCase()) >= 0
        } else {
          return (!this.searchColumn || col.fullName.toUpperCase().indexOf(this.searchColumn.toUpperCase()) >= 0) && !this.isExistExcludeTable(col)
        }
      })
    }
    get searchAllColumns () {
      if (!this.isShow) {
        return []
      }
      return this.filterResult.slice(0, this.pagerSize * this.currentPager)
    }
    getAllColumns () {
      this.allColumns = []
      // let result = []
      let result = this.modelInstance.selected_columns.map((c) => {
        return { fullName: c.column, cardinality: c.cardinality, depend_lookup_table: typeof c.depend_lookup_table !== 'undefined' ? c.depend_lookup_table : true }
      })
      // let modelUsedTables = this.modelInstance && this.modelInstance.getTableColumns() || []
      // modelUsedTables.forEach((col) => {
      //   result.push(col.full_colname)
      // })
      if (this.tableIndexMeta.col_order.length) {
        const selected = this.tableIndexMeta.col_order.map(item => {
          const index = result.findIndex(it => it.fullName === item)
          return {fullName: item, cardinality: result[index].cardinality, depend_lookup_table: result[index].depend_lookup_table}
        })
        const unSort = result.filter(item => !this.tableIndexMeta.col_order.includes(item.fullName))
        result = [...selected, ...unSort]
      }
      // cc列也要放到这里
      // let ccColumns = this.modelInstance && this.modelInstance.computed_columns || []
      // ccColumns.forEach((col) => {
      //   result.push(col.tableAlias + '.' + col.columnName)
      // })
      result.forEach((ctx, index) => {
        let obj = {fullName: ctx.fullName, cardinality: ctx.cardinality, depend_lookup_table: ctx.depend_lookup_table, isUsed: false, isShared: false, colorful: false}
        if (this.tableIndexMeta.col_order.indexOf(ctx.fullName) >= 0) {
          obj.isUsed = true
        }
        if (this.tableIndexMeta.shard_by_columns.indexOf(ctx.fullName) >= 0) {
          obj.isShared = true
        }
        this.allColumns.push(obj)
      })
      // 初始判断是否为全选状态
      this.isSelectAllTableIndex = this.allColumns.length && this.allColumns.filter(it => it.isUsed).length === this.allColumns.length
    }
    get saveBtnDisable () {
      return filterObjectArray(this.allColumns, 'isUsed', true).length === 0 || this.cloneMeta === JSON.stringify(this.allColumns)
    }
    @Watch('isShow')
    initTableIndex (val) {
      if (val) {
        if (this.tableIndexDesc) {
          // col_order 数据结构改成object list了，这里重置成原来的column list
          if (this.tableIndexDesc.col_order) {
            this.tableIndexDesc.col_order = this.tableIndexDesc.col_order.map((col) => {
              return col.key
            })
          }
          Object.assign(this.tableIndexMeta, this.tableIndexDesc)
        }
        this.getAllColumns()
        this.cloneMeta = JSON.stringify(this.allColumns)
      } else {
        this.tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
      }
    }
    pagerChange (pager) {
      this.pager = pager
    }
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    }
    clearAll () {
      this.allColumns.forEach((col) => {
        col.isUsed = false
        col.isShared = false
      })
    }
    changeTableIndexType () {
      this.isSelectAllTableIndex = false
      this.clearAll()
    }
    selectAll () {
      this.allColumns.forEach((col) => {
        col.isUsed = true
      })
    }
    closeModal (isSubmit) {
      this.hideModal()
      this.btnLoading = false
      // this.tableIndexMeta.name = ''
      this.searchColumn = ''
      this.isSelectAllTableIndex = false
      // this.$refs.tableIndexForm.resetFields()
      setTimeout(() => {
        this.callback && this.callback({
          isSubmit: isSubmit
        })
        this.resetModalForm()
      }, 200)
    }
    get tableIndexModalTitle () {
      return this.tableIndexMeta.id !== '' ? this.$t('editTableIndexTitle') : this.$t('addTableIndexTitle')
    }
    handleBuildIndexTip (data) {
      let tipMsg = this.$t('kylinLang.model.saveIndexSuccess', {indexType: this.$t('kylinLang.model.tableIndex')})
      if (this.tableIndexMeta.load_data) {
        if (data.type === BuildIndexStatus.NORM_BUILD) {
          tipMsg += ' ' + this.$t('kylinLang.model.buildIndexSuccess1', {indexType: this.$t('kylinLang.model.tableIndex')})
          this.$message({
            type: 'success',
            dangerouslyUseHTMLString: true,
            duration: 10000,
            showClose: true,
            message: (
              <div>
                <span>{tipMsg}</span>
                <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
              </div>
            )
          })
          return
        }
        if (data.type === BuildIndexStatus.NO_LAYOUT) {
          tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.tableIndex')})
          this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
        } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
          tipMsg += '<br/>' + this.$t('kylinLang.model.buildIndexFail1', {modelName: this.modelInstance.name})
          this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'success', dangerouslyUseHTMLString: true})
        }
      } else {
        this.$message({message: tipMsg, type: 'success'})
      }
    }
    confirmSubmit (isLoadData) {
      this.btnLoading = true
      let successCb = (res) => {
        handleSuccess(res, (data) => {
          this.handleBuildIndexTip(data)
        })
        this.closeModal(true)
        this.btnLoading = false
        // 该字段只有在保存并构建时才会用到，纯流模型是屏蔽保存并构建的
        const isHaveBatchSegment = this.modelInstance.model_type === 'HYBRID' ? this.modelInstance.batch_segments.length > 0 : this.modelInstance.segments.length > 0
        if (isLoadData && !isHaveBatchSegment) {
          this.$emit('openBuildDialog', this.modelInstance, true)
        }
        // 保存并增量构建时，需弹出segment list选择构建区域
        if (isLoadData && isHaveBatchSegment > 0 && this.modelInstance.partition_desc && this.modelInstance.partition_desc.partition_date_column) {
          this.$emit('openComplementAllIndexesDialog', this.modelInstance)
        }
      }
      let errorCb = (res) => {
        this.btnLoading = false
        handleError(res)
      }
      // 按照sort选中列的顺序对col_order进行重新排序
      this.tableIndexMeta.col_order = []
      // this.tableIndexMeta.sort_by_columns = []
      this.tableIndexMeta.shard_by_columns = []
      this.allColumns.forEach((col) => {
        if (col.isUsed) {
          this.tableIndexMeta.col_order.push(col.fullName)
        }
        if (col.isShared) {
          this.tableIndexMeta.shard_by_columns.push(col.fullName)
        }
        // if (col.isSorted) {
        //   this.tableIndexMeta.sort_by_columns.push(col.fullName)
        // }
      })
      this.tableIndexMeta.project = this.currentSelectedProject
      this.tableIndexMeta.model_id = this.isHybridBatch ? this.modelInstance.batch_id : this.modelInstance.uuid
      'name' in this.tableIndexMeta && delete this.tableIndexMeta.name
      if (this.tableIndexMeta.id) {
        this.editTableIndex({...this.tableIndexMeta, index_range: this.tableIndexMeta.index_range || 'EMPTY'}).then(successCb, errorCb)
      } else {
        this.addTableIndex({...this.tableIndexMeta, index_range: this.tableIndexMeta.index_range || 'EMPTY'}).then(successCb, errorCb)
      }
    }
    async submit (isLoadData) {
      const { status } = this.tableIndexDesc || {}
      // 该字段只有在保存并构建时才会用到，纯流模型是屏蔽保存并构建的
      const isHaveBatchSegment = this.modelInstance.model_type === 'HYBRID' ? this.modelInstance.batch_segments.length > 0 : this.modelInstance.segments.length > 0
      // 保存并全量构建时，可以直接提交构建任务，保存并增量构建时，需弹出segment list选择构建区域
      if (isLoadData && isHaveBatchSegment && (!this.modelInstance.partition_desc || this.modelInstance.partition_desc && !this.modelInstance.partition_desc.partition_date_column) || !isLoadData) {
        this.tableIndexMeta.load_data = isLoadData
      }
      if (status && status !== 'EMPTY' && status === 'ONLINE') {
        kylinConfirm(this.$t('cofirmEditTableIndex'), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('kylinLang.common.submit'), type: 'warning'}).then(() => {
          this.confirmSubmit(isLoadData)
        })
      } else {
        this.confirmSubmit(isLoadData)
      }
    }
    selectAllTableIndex (v) {
      this.isSelectAllTableIndex = v
      this.allColumns.forEach(item => {
        if (v && this.getDisabledTableType(item)) return
        item.isUsed = v
        // item.isSorted = v
      })
    }
    selectTableIndex (status, col) {
      const selectedColumns = this.getSelectedColumns
      const unSelected = this.allColumns.filter(it => !it.isUsed)
      // col.isSorted = status
      this.allColumns = [...selectedColumns, ...unSelected]
      selectedColumns.length === this.allColumns.length && (this.isSelectAllTableIndex = true)
      unSelected.length === this.allColumns.length && (this.isSelectAllTableIndex = false)
    }
    // 跳转至job页面
    jumpToJobs () {
      this.$router.push('/monitor/job')
    }
  }
</script>
<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .table-edit-dialog {
    .el-dialog {
      min-width: 600px;
      .header {
        text-align: right;
        .el-alert--nobg {
          text-align: left;
        }
        .anit-table-tips {
          font-size: 12px;
          text-align: left;
          margin-left: 22px;
          margin-bottom: 10px;
        }
        .exclude-table-checkbox {
          display: inline-block;
          font-size: 0;
          top: -4px;
          .el-checkbox__inner {
            vertical-align: middle;
          }
        }
      }
      .table-index-list {
        .is-required{
          color: @color-danger;
        }
      }
      .no-index-range {
        text-align: center;
        padding: 16px 8px;
        color: @text-disabled-color;
      }
    }
    .flip-list-move {
      transition: transform .5s;
    }
    .action-list {
      position:relative;
      .up-down {
        position: absolute;
        left: 61%;
        display: none;
        i {
          color:@base-color;
        }
      }
      &:hover {
        .up-down {
          display: inline-block;
        }
      }
    }
    .row-colorful {
      background:@lighter-color-tip!important;
    }
    .el-icon-success {
      cursor:pointer;
      &.active{
        color:@color-success;
      }
      color:@text-placeholder-color;
    }
    .ky-dot-tag {
      cursor:pointer;
    }
    .no-sorted {
      background:@text-placeholder-color;
    }
    .sub-title {
      margin-top:60px;
    }
    .show-pagers{
      width: 42px;
      position: absolute;
      top: 100px;
      right: -16px;
      ul {
        li {
          border:solid 1px #ccc;
          margin-bottom:12px;
          text-align:center;
        }
      }
    }
    .show-more-block {
      width:120px;
      height:20px;
      line-height:20px;
      text-align:center
    }
    .sort-icon {
      .ky-square-box(32px, 32px);
      background: @text-secondary-color;
      display: inline-block;
      color:@fff;
      vertical-align: baseline;
    }
    .table-index-columns {
      li {
        margin-top:20px;
        height:32px;
      }
    }
    .ky-simple-table {
      .el-col {
        position: relative;
        &:first-child {
          text-overflow: initial;
        }
      }
      .order-actions {
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .table-row {
        .column-name {
          text-align: left;
        }
        .cardinality-item {
          text-align: right;
          .no-data_placeholder {
            color: @text-placeholder-color;
            font-size: 12px;
          }
        }
        .icon {
          cursor: pointer;
          margin-left: 10px;
          &:first-child {
            margin-left: 0;
          }
          &.is-disabled {
            pointer-events: none;
            color: @text-disabled-color;
          }
        }
      }
      .excluded_table-icon {
        position: absolute;
        right: 10px;
        line-height: 32px;
      }
    }
  }
</style>
