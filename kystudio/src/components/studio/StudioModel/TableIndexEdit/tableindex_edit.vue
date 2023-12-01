<template>
  <!-- tableindex的添加和编辑 -->
  <el-dialog :title="tableIndexModalTitle" append-to-body limited-area class="table-edit-dialog" width="880px" v-if="isShow" :visible="true" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
    <div class="table-index-list">
      <transfer-data
        :allModelColumns="allColumns"
        isShowNum
        draggable
        topColAble
        sharedByAble
        isSortAble
        isAllSelect
        :selectedColumns="selectedColumns"
        :rightTitle="$t('tableIndex')"
        :rightTitleTip="$t('tableIndexTips')"
        isTextRecognition
        :isEdit="this.tableIndexMeta.id !== ''"
        :alertTips="alertTips"
        @handleTableIndexRecognize="handleTableIndexRecognize"
        @setSelectedColumns="(v) => setSelectedColumns(v)"
        @setShardbyCol="(label) => setShardbyCol(label)">
        <template slot="left-footer" v-if="showExcludedTableCheckBox">
          <el-checkbox v-model="displayExcludedTables" @change="isShowExcludedTablesCols" class="exclude-checkbox" size="small">
            <span>{{$t('excludeTableCheckbox')}}</span>
            <el-tooltip effect="dark" placement="top" :maxHeight="200">
              <div slot="content" class="ksd-fs-12">
                <div>{{ $t('excludeTableCheckboxTip1') }}</div>
                <li>• {{ $t('excludeTableCheckboxTip2') }}</li>
                <li>• {{ $t('excludeTableCheckboxTip3') }}</li>
                <div v-html="$t('excludeTableCheckboxTip4')"></div>
              </div>
              <i class="el-icon-ksd-what ksd-fs-14"></i>
            </el-tooltip>
          </el-checkbox>
        </template>
        <template slot="help">
          <el-popover
            ref="help"
            placement="top"
            width="366"
            popper-class="table-index-help"
            :title="$t('kylinLang.common.help')"
            v-model="isShowHelp">
            <i class="el-ksd-n-icon-close-L-outlined" @click="isShowHelp = false"></i>
            <div class="sugession-blocks">
              <p>
                <el-tag type="info" size="mini" is-light>{{ $t('sugessionLabel1') }}</el-tag>
                <span class="sugession">{{ $t('sugession1') }}</span>
              </p>
              <p class="ksd-mt-16">
                <el-tag type="info" size="mini" is-light>{{ $t('sugessionLabel2') }}</el-tag>
                <span class="sugession">{{ $t('sugession2') }}
                  <span class="tips">{{ $t('tips') }}<a class="ky-a-like" @click="goToDataSource">{{ $t('goToDataSource') }}</a></span>
                </span>
              </p>
            </div>
            <div class="footer">
              <span class="info ksd-mr-16">{{ $t('knowMore') }}<a :href="$t('shardbyManal')">{{ $t('userManual') }}</a></span>
            </div>
          </el-popover>
          <el-tooltip effect="dark" :content="$t('help')" placement="top">
            <i class="el-ksd-n-icon-help-circle-outlined" v-popover:help></i>
          </el-tooltip>
        </template>
      </transfer-data>
      <div class="ksd-mt-16" v-if="modelInstance.model_type === 'HYBRID'">
        <div class="ksd-title-label-mini ksd-mb-8">
          {{$t('indexTimeRange')}}
          <span class="is-required">*</span>
        </div>
        <el-radio-group v-model="tableIndexMeta.index_range" size="small" :disabled="tableIndexMeta.id !== ''">
          <el-tooltip placement="top" :disabled="indexUpdateEnabled" :content="$t('refuseAddIndexTip')">
            <el-radio :label="'HYBRID'" :disabled="!indexUpdateEnabled">
              {{$t('kylinLang.common.HYBRID')}}<el-tooltip effect="dark" :content="$t('indexTimeRangeTips')" placement="top">
                <i class="el-icon-ksd-what ksd-ml-5 ksd-fs-14"></i>
              </el-tooltip>
            </el-radio>
          </el-tooltip>
          <el-radio class="ksd-ml-16" :label="'BATCH'">{{$t('kylinLang.common.BATCH')}}</el-radio>
          <el-tooltip placement="top" :disabled="indexUpdateEnabled" :content="$t('refuseAddIndexTip')">
            <el-radio class="ksd-ml-16" :label="'STREAMING'" :disabled="!indexUpdateEnabled">{{$t('kylinLang.common.STREAMING')}}</el-radio>
          </el-tooltip>
        </el-radio-group>
        <div v-if="indexRangeReqiured" class="is-required ksd-fs-12 ksd-mt-8">{{ $t('indexRangeReqiured') }}</div>
      </div>
    </div>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button :type="onlyBatchType ? 'primary' : ''" :text="onlyBatchType" @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button :type="!onlyBatchType ? 'primary' : ''" :loading="btnLoading&&!isLoadDataLoading" size="medium" @click="submit(false)" :disabled="saveBtnDisable || btnLoading&&isLoadDataLoading">{{$t('kylinLang.common.save')}}</el-button>
      <el-button v-if="onlyBatchType" type="primary" :loading="btnLoading&&isLoadDataLoading" size="medium" @click="submit(true)" :disabled="saveBtnDisable || btnLoading&&!isLoadDataLoading">{{$t('saveAndBuild')}}</el-button>
    </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../store'
  import { BuildIndexStatus } from 'config/model'
  import { handleSuccess, kylinConfirm } from 'util/business'
  import { objectClone, indexOfObjWithSomeKey } from 'util/index'
  import TransferData from '../../../common/CustomTransferData/TransferData'
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
        isShowHelpDefault: state => state.form.data.isShowHelp,
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
      }),
      ...mapActions('RecognizeAggregateModal', {
        callRecognizeAggregateModal: types.CALL_MODAL
      })
    },
    components: {
      TransferData
    },
    locales
  })
  export default class TableIndexEditModal extends Vue {
    isShowHelp = false
    btnLoading = false
    allColumnList = []
    allColumns = []
    selectedColumns = []
    tableIndexMetaStr = JSON.stringify({
      id: '',
      col_order: [],
      sort_by_columns: [],
      shard_by_columns: [],
      load_data: false,
      index_range: ''
    })
    tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
    cloneMeta = ''
    displayExcludedTables = false
    isLoadDataLoading = false
    alertTips = []
    indexRangeReqiured = false

    get onlyBatchType () {
      return (this.modelInstance.model_type === 'HYBRID' && this.tableIndexMeta.index_range !== 'STREAMING') || (this.modelInstance.model_type !== 'STREAMING' && this.modelInstance.model_type !== 'HYBRID')
    }

    get showExcludedTableCheckBox () {
      return this.modelInstance.selected_columns.length ? this.modelInstance.selected_columns.filter(it => typeof it.excluded !== 'undefined' && it.excluded).length > 0 : false
    }

    goToDataSource () {
      this.$router.push('/studio/source')
    }
  
    setSelectedColumns (selectedColumns) {
      this.selectedColumns = selectedColumns
      this.tableIndexMeta.col_order = []
      this.selectedColumns.forEach((key) => {
        const i = indexOfObjWithSomeKey(this.allColumns, 'key', key)
        i !== -1 && this.tableIndexMeta.col_order.push(this.allColumns[i].label)
      })
      this.removeTips('error')
    }
    setShardbyCol (label) {
      this.tableIndexMeta.shard_by_columns = []
      label && this.tableIndexMeta.shard_by_columns.push(label)
      this.removeTips('error')
    }
    removeTips (type) {
      const index = indexOfObjWithSomeKey(this.alertTips, 'type', type)
      index !== -1 && this.alertTips.splice(index, 1)
    }
    // 当表为屏蔽表且表关联关系为多对多时，不能作为维度添加到索引中
    getDisabledTableType (col) {
      if (!col) return false
      const [currentTable] = col.column.split('.')
      const { join_tables } = this.modelInstance
      const manyToManyTables = join_tables.filter(it => it.join_relation_type === 'MANY_TO_MANY').map(item => item.alias)
      return manyToManyTables.includes(currentTable) && this.isExistExcludeTable(col)
    }
    // 是否为屏蔽表的 column
    isExistExcludeTable (col) {
      return typeof col.excluded !== 'undefined' ? col.excluded : false
    }
    initColumns () {
      this.allColumns = this.modelInstance.selected_columns.filter(col => !this.displayExcludedTables && !this.isExistExcludeTable(col) || this.displayExcludedTables).map((col) => {
        const isShardby = this.tableIndexMeta.shard_by_columns.indexOf(col.column) >= 0
        const disabled = this.getDisabledTableType(col)
        const isSelected = this.tableIndexMeta.col_order.indexOf(col.column) >= 0
        if (disabled) {
          this.alertTips = [{ text: this.$t('manyToManyAntiTableTip'), type: 'warning' }]
        }
        return { key: col.id, label: col.column, name: col.name, disabled: disabled, cardinality: col.cardinality, type: col.type, comment: col.comment, excluded: typeof col.excluded !== 'undefined' ? col.excluded : true, selected: isSelected, isShared: isShardby }
      })
      this.selectedColumns = this.tableIndexMeta.col_order.map(item => {
        const index = this.allColumns.findIndex(it => it.label === item)
        return this.allColumns[index].key
      })
      setTimeout(() => {
        this.isShowHelp = this.isShowHelpDefault
      }, 200)
    }
    isShowExcludedTablesCols () {
      this.allColumns = this.modelInstance.selected_columns.filter(col => !this.displayExcludedTables && !this.isExistExcludeTable(col) || this.displayExcludedTables).map((col) => {
        const isShardby = this.tableIndexMeta.shard_by_columns.indexOf(col.column) >= 0
        const disabled = this.getDisabledTableType(col)
        if (disabled) {
          this.alertTips = [{ text: this.$t('manyToManyAntiTableTip'), type: 'warning' }]
        }
        return { key: col.id, label: col.column, name: col.name, disabled: disabled, cardinality: col.cardinality, type: col.type, comment: col.comment, excluded: typeof col.excluded !== 'undefined' ? col.excluded : true, selected: false, isShared: isShardby }
      })
      if (!this.displayExcludedTables) {
        this.removeTips('warning')
      }
      this.selectedColumns = this.tableIndexMeta.col_order.map(item => {
        const index = this.allColumns.findIndex(it => it.label === item)
        return this.allColumns[index].key
      })
    }
    get saveBtnDisable () {
      return this.cloneMeta === JSON.stringify(this.selectedColumns)
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
        this.initColumns()
        this.cloneMeta = JSON.stringify(this.selectedColumns)
      } else {
        this.tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
      }
    }
    closeModal (isSubmit) {
      this.hideModal()
      this.btnLoading = false
      this.displayExcludedTables = false
      this.alertTips = []
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
              <div class="el-message__content">
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
    async handleTableIndexRecognize () {
      const selectedColumns = await this.callRecognizeAggregateModal({
        type: 'TABLE_INDEX',
        allColumns: this.allColumns,
        model: this.modelInstance
      })
      selectedColumns.forEach((col) => {
        const index = indexOfObjWithSomeKey(this.allColumns, 'label', col)
        if (index !== -1) {
          const selectedIndex = this.selectedColumns.indexOf(this.allColumns[index].key)
          selectedIndex === -1 && this.selectedColumns.push(this.allColumns[index].key)
        }
      })
      this.setSelectedColumns(this.selectedColumns)
    }
    confirmSubmit (isLoadData) {
      this.isLoadDataLoading = isLoadData
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
        // handleError(res)
        this.removeTips('error')
        this.alertTips.push({ text: res.body.msg, type: 'error' })
      }
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
      if (this.modelInstance.model_type === 'HYBRID' && !this.tableIndexMeta.index_range) {
        this.indexRangeReqiured = true
        return
      }
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
      .exclude-checkbox {
        margin: 6px 8px;
      }
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
  .table-index-help {
    position: relative;
    font-size: 12px;
    color: @text-normal-color;
    .el-ksd-n-icon-close-L-outlined {
      position: absolute;
      top: 16px;
      right: 16px;
      cursor: pointer;
    }
    .el-popover__title {
      color: @text-normal-color;
      font-weight: @font-medium;
    }
    .sugession-blocks {
      margin-bottom: 32px;
      p {
        display: flex;
        align-items: flex-start;
        .sugession {
          margin-left: 8px;
          line-height: 18px;
        }
        .tips {
          color: @text-disabled-color;
          display: block;
          margin-top: 4px;
        }
      }
    }
    .footer {
      height: 32px;
      line-height: 32px;
      text-align: right;
      background-color: @ke-background-color-secondary;
      position: absolute;
      bottom: 0px;
      left: 0px;
      width: 100%;
      border-bottom-left-radius: 6px;
      border-bottom-right-radius: 6px;
      border-top: 1px solid @ke-border-secondary;
      color: @text-placeholder-color;
      a {
        color: @text-normal-color;
        &:hover {
          color: @ke-color-primary;
        }
      }
    }
    &.el-popper[x-placement^=top] .popper__arrow::after {
      border-top-color: @ke-background-color-secondary;
    }
  }
</style>
