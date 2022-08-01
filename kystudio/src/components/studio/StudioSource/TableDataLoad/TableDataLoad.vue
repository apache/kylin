<template>
  <div class="table-data-load">
    <div class="info-group">
      <div class="info-row">
        <span class="info-label font-medium">{{$t('tableName')}}</span>
        <span class="info-value">{{table.name}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('partitionKey')}}</span>
        <span class="info-value ky-no-br-space">
          <el-select
            filterable
            size="medium"
            v-bind:value="table.partitionColumn"
            @change="handleChangePartition">
            <el-option :label="$t('noPartition')" value=""></el-option>
            <el-option
              v-for="item in table.dateTypeColumns"
              :key="item.name"
              :label="item.name"
              :value="item.name">
              <span style="float: left">{{ item.name }}</span>
              <span class="ky-option-sub-info">{{ item.datatype }}</span>
            </el-option>
          </el-select>
        </span>
      </div>
      <div class="info-row" v-show="table.partitionColumn">
        <span class="info-label font-medium">{{$t('dateFormat')}}</span>
        <span class="info-value">
          <el-select
            filterable
            size="medium"
            :disabled="isLoadingFormat"
            @change="handleChangePartitionFormat"
            :placeholder="$t('kylinLang.common.pleaseSelect')"
            v-bind:value="table.format">
            <el-option
              v-for="item in dateFormats"
              :key="item.label"
              :label="item.label"
              :value="item.value">
            </el-option>
          </el-select>
          <el-tooltip effect="dark" :content="$t('detectFormat')" placement="top">
            <div style="display: inline-block;">
              <el-button
                size="medium"
                class="ksd-ml-10"
                :loading="isLoadingFormat"
                v-if="table.partitionColumn&&$store.state.project.projectPushdownConfig"
                icon="el-ksd-icon-data_range_search_old"
                @click="handleLoadFormat">
              </el-button>
            </div>
          </el-tooltip>
        </span>
      </div>
    </div>
    <div class="hr inner"></div>
    <div class="info-group">
      <div class="info-row">
        <span class="info-label font-medium">{{$t('storageType')}}</span>
        <span class="info-value" v-if="table.storageType">{{$t('kylinLang.dataSource.' + table.storageType)}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('storageSize')}}</span>
        <span class="info-value" v-if="table.storageSize !== null">{{table.storageSize | dataSize}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row" v-if="!~['incremental'].indexOf(table.storageType)">
        <span class="info-label font-medium">{{$t('totalRecords')}}</span>
        <span class="info-value" v-if="table.storageType">{{table.totalRecords}} {{$t('rows')}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
    </div>
    <template v-if="table.partitionColumn">
      <div class="hr inner"></div>
      <div class="info-group">
      <div class="info-row">
        <span class="info-label font-medium">{{$t('loadRange')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('kylinLang.common.startTime1')}}</span>
        <span class="info-value" v-if="table.startTime !== undefined">{{table.startTime | toServerGMTDate}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
      <div class="info-row">
        <span class="info-label font-medium">{{$t('kylinLang.common.endTime1')}}</span>
        <span class="info-value" v-if="table.endTime !== undefined">{{table.endTime | toServerGMTDate}}</span>
        <span class="info-value empty" v-else>{{$t('notLoadYet')}}</span>
      </div>
    </div>
    </template>
    <div class="hr"></div>
    <div class="ksd-mt-15 ksd-ml-10 ky-no-br-space">
      <el-button type="primary" size="medium" v-if="(~['incremental', 'full'].indexOf(table.storageType) || table.partitionColumn)&&isShowLoadData" @click="handleLoadData()">{{$t('loadData')}}</el-button>
      <el-button v-if="~['incremental'].indexOf(table.storageType) || table.partitionColumn" size="medium" @click="handleRefreshData">{{$t('refreshData')}}</el-button>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { _getPartitionInfo, _getFullLoadInfo, _getRefreshFullLoadInfo } from './handler'
import { handleSuccessAsync, handleError } from '../../../../util'
import { getAffectedModelsType } from '../../../../config'

@Component({
  props: {
    project: {
      type: Object
    },
    table: {
      type: Object
    },
    isShowLoadData: {
      type: Boolean,
      default: true
    }
  },
  methods: {
    ...mapActions('SourceTableModal', {
      callSourceTableModal: 'CALL_MODAL'
    }),
    ...mapActions({
      saveTablePartition: 'SAVE_TABLE_PARTITION',
      fetchRelatedModels: 'FETCH_RELATED_MODELS',
      fetchChangeTypeInfo: 'FETCH_CHANGE_TYPE_INFO',
      fetchRelatedModelStatus: 'FETCH_RELATED_MODEL_STATUS',
      fetchFullLoadInfo: 'FETCH_FULL_LOAD_INFO',
      fetchFreshInfo: 'FETCH_RANGE_FRESH_INFO',
      freshDataRange: 'FRESH_RANGE_DATA',
      fetchPartitionFormat: 'FETCH_PARTITION_FORMAT'
    })
  },
  locales
})
export default class TableDataLoad extends Vue {
  isLoadingFormat = false
  dateFormats = [
    {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
    {label: 'yyyyMMdd', value: 'yyyyMMdd'},
    {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
    {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'},
    {label: 'yyyy/MM/dd', value: 'yyyy/MM/dd'}
    // {label: 'yyyy-MM', value: 'yyyy-MM'},
    // {label: 'yyyyMM', value: 'yyyyMM'}
  ]
  async handleLoadFormat () {
    try {
      this.isLoadingFormat = true
      const res = await this.fetchPartitionFormat({ project: this.project.name, table: this.table.fullName, partition_column: this.table.partitionColumn })
      const data = await handleSuccessAsync(res)
      this.handleChangePartitionFormat(data)
      this.isLoadingFormat = false
    } catch (e) {
      this.isLoadingFormat = false
      handleError(e)
    }
  }
  async handleLoadData (isChangePartition) {
    try {
      if (isChangePartition) {
        delete this.table.startTime // 切换partition key的时候选择范围设置为空
        delete this.table.endTime
      }
      const { project, table } = this
      if (table.partitionColumn) {
        const isSubmit = await this.callSourceTableModal({ editType: 'loadData', project, table, format: this.table.format })
        isSubmit && this.$emit('fresh-tables')
        // 如果是改变partition拉起的渲染range弹窗，关闭的时候给提示
        if (!isSubmit && isChangePartition === true) {
          this.$message({
            message: this.$t('suggestSetLoadRangeTip'),
            type: 'warning'
          })
        }
      } else {
        await this.handleLoadFullData()
        this.$emit('fresh-tables')
      }
    } catch (e) {
      handleError(e)
    }
  }
  async handleRefreshData () {
    const { project, table } = this
    const isSubmit = await this.callSourceTableModal({ editType: 'refreshData', project, table, format: this.table.format })
    isSubmit && this.$emit('fresh-tables')
  }
  async handleLoadFullData () {
    const { modelCount, modelSize } = await this._getAffectedModelCountAndSize(getAffectedModelsType.RELOAD_ROOT_FACT)
    if (modelCount || modelSize) {
      await this._showFullDataLoadConfirm({ modelSize })
    }
    const submitData = _getFullLoadInfo(this.project, this.table)
    const response = await this.fetchFreshInfo(_getRefreshFullLoadInfo(this.project, this.table))
    const result = await handleSuccessAsync(response)
    submitData.affected_start = result.affected_start
    submitData.affected_end = result.affected_end
    await this.freshDataRange(submitData)
    this.$emit('fresh-tables')
    this.$message({ type: 'success', message: this.$t('loadSuccessTip') })
  }
  async handleChangePartition (value) {
    try {
      // const { modelCount, modelSize } = await this._getAffectedModelCountAndSize(getAffectedModelsType.TOGGLE_PARTITION)
      // if (modelCount || modelSize) {
      //   await this._showPartitionConfirm({ modelSize, partitionKey: value })
      // }
      await this._changePartitionKey(value, this.table.format)
      // TODO HA 模式时 post 等接口需要等待同步完去刷新列表
      // await handleWaiting()
      if (value) {
        this.table.partitionColumn = value
        // await this.handleLoadData(true)
      }
      this.$emit('fresh-tables')
    } catch (e) {
      handleError(e)
    }
  }
  async handleChangePartitionFormat (value) {
    try {
      await this._changePartitionKey(this.table.partitionColumn, value)
      if (value) {
        this.table.format = value
      }
      this.$emit('fresh-tables')
    } catch (e) {
      handleError(e)
    }
  }
  _showFullDataLoadConfirm ({ modelSize }) {
    const storageSize = Vue.filter('dataSize')(modelSize)
    const tableName = this.table.name
    const contentVal = { tableName, storageSize }
    const confirmTitle = this.$t('fullLoadDataTitle')
    const confirmMessage1 = this.$t('fullLoadDataContent1', contentVal)
    const confirmMessage2 = this.$t('fullLoadDataContent2', contentVal)
    const confirmMessage = _render(this.$createElement)
    const confirmButtonText = this.$t('kylinLang.common.submit')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })

    function _render (h) {
      return (
        <div>
          <p class="break-all">{confirmMessage1}</p>
          <p>{confirmMessage2}</p>
        </div>
      )
    }
  }
  _showPartitionConfirm ({ modelSize, partitionKey }) {
    const storageSize = Vue.filter('dataSize')(modelSize)
    const tableName = this.table.name
    const oldPartitionKey = this.table.partitionColumn || this.$t('noPartition')
    const newPartitionKey = partitionKey || this.$t('noPartition')
    const confirmTitle = this.$t('changePartitionTitle')
    const contentVal = { tableName, newPartitionKey, oldPartitionKey, storageSize }
    const confirmMessage1 = this.$t('changePartitionContent1', contentVal)
    const confirmMessage2 = this.$t('changePartitionContent2', contentVal)
    const confirmMessage3 = this.$t('changePartitionContent3', contentVal)
    const confirmMessage = _render(this.$createElement)
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })

    function _render (h) {
      return (
        <div>
          <p class="break-all">{confirmMessage1}</p>
          <p>{confirmMessage2}</p>
          <p>{confirmMessage3}</p>
        </div>
      )
    }
  }
  _changePartitionKey (column, format) {
    const submitData = _getPartitionInfo(this.project, this.table, column, format)
    return this.saveTablePartition(submitData)
  }
  async _getAffectedModelCountAndSize (affectedType) {
    const projectName = this.project.name
    const tableName = this.table.fullName
    // const isSelectFact = !this.table.__data.increment_loading
    const response = await this.fetchChangeTypeInfo({ projectName, tableName, affectedType })
    const result = await handleSuccessAsync(response)
    return { modelCount: result.models.length, modelSize: result.byte_size }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.table-data-load {
  .info-group {
    padding: 15px 0;
    &:first-child {
      padding-top: 0;
    }
  }
  .info-label {
    display: inline-block;
    width: 116px;
  }
  .info-value.empty {
    color: @text-disabled-color;
  }
  .info-row {
    padding: 0 10px;
  }
  .info-row:not(:last-child) {
    margin-bottom: 10px;
  }
  // .hr.dashed {
  //   height: 1px;
  //   border: none;
  //   background-image: linear-gradient(to right, @line-border-color 0%, @line-border-color 50%, transparent 50%);
  //   background-size: 20px 1px;
  //   background-repeat: repeat-x;
  // }
  .hr {
    height: auto;
    border-bottom: 1px solid @line-split-color;
    background-image: none;
    &.inner{
      margin: 0 10px;
    }
  }
}
</style>
