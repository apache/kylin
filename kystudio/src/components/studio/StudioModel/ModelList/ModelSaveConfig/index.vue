<template>
  <el-dialog
    :title="partitionTitle"
    width="600px"
    append-to-body
    limited-area
    :visible="isShow"
    class="model-partition-dialog"
    @close="isShow && handleClose(false)"
    :close-on-press-escape="false"
    :close-on-click-modal="false">
    <div class="partition-set" v-if="mode === 'saveModel'">
      <el-alert
        :title="cannotSaveModelTips"
        type="error"
        :closable="false"
        class="ksd-mb-10"
        v-if="isShowSecondStoragePartitionTips"
        show-icon>
      </el-alert>
      <el-alert
        :title="$t('changeBuildTypeTips')"
        type="warning"
        :closable="false"
        class="ksd-mb-10"
        v-if="isShowWarning"
        show-icon>
      </el-alert>
      <el-tabs v-model="buildType" class="buildType-switch" type="button" @tab-click="handChangeBuildType">
        <el-tab-pane name="incremental" :disabled="!datasourceActions.includes('changeBuildType')">
          <span slot="label">
            <el-tooltip :content="$t('incrementalTips')" placement="top">
              <span>{{$t('incremental')}}<el-tag class="ksd-ml-5" size="mini" type="success">{{$t('recommend')}}</el-tag></span>
            </el-tooltip>
          </span>
        </el-tab-pane>
        <el-tab-pane :disabled="isNotBatchModel || !datasourceActions.includes('changeBuildType')" name="fullLoad">
          <span slot="label">
            <el-tooltip :content="!isNotBatchModel&&datasourceActions.includes('changeBuildType') ? $t('fullLoadTips', {storageSize: dataSize(modelDesc.storage)}) : this.$t('isNotBatchModel')" placement="top">
              <span>{{$t('fullLoad')}}</span>
            </el-tooltip>
          </span>
        </el-tab-pane>
      </el-tabs>
    </div>
    <el-form v-if="mode === 'saveModel'&&buildType=== 'incremental'" :model="partitionMeta" ref="partitionForm" :rules="partitionRules"  label-width="85px" label-position="top">
      <!-- 新建流数据、融合数据模型时提示 -->
      <el-alert
        class="ksd-mb-8"
        :title="$t('notBatchModelPartitionTips')"
        type="tip"
        v-if="isNotBatchModel&&!modelDesc.uuid"
        :closable="false"
        show-icon>
      </el-alert>
      <el-form-item :label="$t('partitionDateTable')" class="clearfix">
        <el-row :gutter="5">
          <el-col :span="24">
            <el-tooltip effect="dark" :content="$t('disableChangePartitionTips')" :disabled="!(isNotBatchModel&&!!modelDesc.uuid&&isAlreadyHavePartition)" placement="bottom">
              <el-select :disabled="isLoadingNewRange||(isNotBatchModel&&!!modelDesc.uuid&&isAlreadyHavePartition)" v-model="partitionMeta.table" @change="partitionTableChange" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" style="width:100%">
                <el-option :label="t.alias" :value="t.alias" v-for="t in partitionTables" :key="t.alias">{{t.alias}}</el-option>
              </el-select>
            </el-tooltip>
          </el-col>
        </el-row>
      </el-form-item>
      <el-form-item  :label="$t('partitionDateColumn')" v-if="partitionMeta.table">
        <el-row :gutter="5">
          <el-col :span="24" v-if="partitionMeta.table">
            <el-form-item prop="column">
              <el-tooltip effect="dark" :content="$t('disableChangePartitionTips')" :disabled="!(isNotBatchModel&&!!modelDesc.uuid&&isAlreadyHavePartition)" placement="bottom">
                <el-select
                  :disabled="isLoadingNewRange || (isNotBatchModel&&!!modelDesc.uuid&&isAlreadyHavePartition)"
                  v-model="partitionMeta.column"
                  :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                  filterable
                  class="partition-column"
                  popper-class="js_partition-column"
                  @change="changeColumn('column')"
                  style="width:100%">
                <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!partitionMeta.column.length"></i>
                  <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
                    <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                    <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                  </el-option>
                </el-select>
              </el-tooltip>
            </el-form-item>
          </el-col>
        </el-row>
      </el-form-item>
      <el-form-item  :label="$t('dateFormat')"  :class="{'is-error': errorFormat}" v-if="partitionMeta.table">
        <el-row :gutter="5">
          <el-col :span="partitionMeta.column && $store.state.project.projectPushdownConfig && factTableType !== 1 ? 22 : 24">
            <el-tooltip effect="dark" :content="$t('disableChangePartitionTips')" :disabled="!(isNotBatchModel&&!!modelDesc.uuid&&isAlreadyHavePartition)" placement="bottom">
              <el-select
                :disabled="isLoadingFormat || (isNotBatchModel&&!!modelDesc.uuid&&isAlreadyHavePartition)"
                style="width:100%"
                filterable
                allow-create
                default-first-option
                v-model="partitionMeta.format"
                class="partition-column-format"
                popper-class="js_partition-column-format"
                :placeholder="$t('pleaseInputColumnFormat')"
                @change="val => changeColumn('format', val)"
              >
                <el-option-group>
                  <el-option v-if="dateFormatsOptions.map(it => it.value).indexOf(prevPartitionMeta.format) === -1 && prevPartitionMeta.format" :label="prevPartitionMeta.format" :value="prevPartitionMeta.format"></el-option>
                  <el-option :label="f.label" :value="f.value" v-for="f in dateFormatsOptions" :key="f.label"></el-option>
                </el-option-group>
              </el-select>
            </el-tooltip>
          </el-col>
          <el-col :span="2" v-if="partitionMeta.column && $store.state.project.projectPushdownConfig && factTableType !== 1">
            <el-tooltip effect="dark" :content="$t('detectFormat')" placement="top">
              <div style="display: inline-block;">
                <el-button
                  size="medium"
                  :loading="isLoadingFormat"
                  icon="el-ksd-icon-data_range_search_old"
                  @click="handleLoadFormat">
                </el-button>
              </div>
            </el-tooltip>
          </el-col>
        </el-row>
        <div class="error-format" v-if="errorFormat">{{errorFormat}}</div>
        <div class="pre-format" v-if="formatedDate">{{$t('previewFormat')}}{{formatedDate}}</div>
        <div class="format">{{$t('formatRule')}}
          <span v-if="isExpandFormatRule" @click="isExpandFormatRule = false">{{$t('viewDetail')}}<i class="el-ksd-icon-arrow_up_16 arrow ksd-fs-16"></i></span>
          <span v-else @click="isExpandFormatRule = true">{{$t('viewDetail')}}<i class="el-ksd-icon-arrow_down_16 arrow ksd-fs-16"></i></span>
        </div>
        <div class="detail-content" v-if="isExpandFormatRule">
          <p><span class="ksd-mr-2">1. </span><span>{{$t('rule1')}}</span></p>
          <p><span class="ksd-mr-2">2. </span><span>{{$t('rule2')}}</span></p>
          <p><span class="ksd-mr-2">3. </span><span>{{$t('rule3')}}</span></p>
        </div>
        <span style="position:absolute;width:1px; height:0" v-if="partitionMeta.format"></span>
      </el-form-item>
      <el-form-item v-if="((!modelDesc.multi_partition_desc && $store.state.project.multi_partition_enabled) || modelDesc.multi_partition_desc) && partitionMeta.table && !isNotBatchModel">
        <span slot="label">
          <span>{{$t('multilevelPartition')}}</span>
          <el-tooltip effect="dark" :content="$t('multilevelPartitionDesc')" placement="right">
            <i class="el-icon-ksd-what ksd-fs-14"></i>
          </el-tooltip>
        </span>
        <el-row>
          <el-col :span="24">
           <el-select
              :disabled="isLoadingNewRange"
              v-model="partitionMeta.multiPartition"
              :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
              filterable
              class="partition-multi-partition"
              popper-class="js_multi-partition"
              style="width:100%"
              @change="changeColumn('multiPartition')"
            >
              <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!partitionMeta.multiPartition.length"></i>
              <el-option :label="$t('noPartition')" value=""></el-option>
              <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
                <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
              </el-option>
            </el-select>
          </el-col>
        </el-row>
      </el-form-item>
    </el-form>
    <template v-if="mode === 'saveModel'">
      <div v-if="modelInstance && modelInstance.status !== 'BROKEN' && !isStreamModel && !this.isHaveNoDimMeas && !modelDesc.with_second_storage && !(modelInstance.has_base_table_index && modelInstance.has_base_agg_index)">
        <div class="divide-block">
          <div class="divider">
            <span>{{$t('indexSetting')}}</span>
          </div>
        </div>
        <div class="base-index-block ksd-ptb-16">
          <el-checkbox-group v-model="source_types">
            <el-checkbox label="BASE_AGG_INDEX" size="small" v-if="!modelInstance.has_base_agg_index">
              <span>{{$t('addBaseAggIndexCheckBox')}}</span>
              <el-tooltip effect="dark" :content="$t('baseAggIndexTips')" placement="top">
                <i class="el-icon-ksd-what ksd-fs-14"></i>
              </el-tooltip>
            </el-checkbox>
            <el-checkbox size="small" label="BASE_TABLE_INDEX" :disabled="modelDesc.with_second_storage" v-if="!modelInstance.has_base_table_index">
              <span>{{$t('addBaseTableIndexCheckBox')}}</span>
              <el-tooltip effect="dark" :content="$t('baseTableIndexTips')" placement="top">
                <i class="el-icon-ksd-what ksd-fs-14"></i>
              </el-tooltip>
            </el-checkbox>
          </el-checkbox-group>
        </div>
      </div>
      <div class="divide-block advance-setting">
        <div class="divider">
          <span v-if="isExpand" @click="toggleShowPartition">{{$t('advanceSetting')}}<i class="el-ksd-icon-arrow_up_16 arrow ksd-fs-16"></i></span>
          <span v-else @click="toggleShowPartition">{{$t('advanceSetting')}}<i class="el-ksd-icon-arrow_down_16 arrow ksd-fs-16"></i></span>
        </div>
      </div>
      <div v-show="isExpand">
        <div class="second-setting ksd-mb-16" v-if="$store.state.project.second_storage_enabled">
          <el-alert v-if="modelDesc.with_second_storage" show-icon type="warning" class="ksd-mb-8" :closable="false">
            <span v-html="$t('forbidenComputedColumnTips')" class="ksd-fs-12"></span>
          </el-alert>
          <!-- 已有模型提示 开始 -->
          <el-alert
            :title="$t('openSecStorageTips2')"
            type="tip"
            :closable="false"
            class="ksd-mb-8"
            v-if="isShowSecStorageTips2 && modelDesc.uuid && !isNotBatchModel"
            show-icon>
          </el-alert>
          <!-- 已有模型提示 结束 -->
          <el-alert
            :title="$t('openSecStorageTips')"
            type="tip"
            :closable="false"
            class="ksd-mb-8"
            v-if="modelDesc.simplified_dimensions.length >= 20 && !isNotBatchModel"
            show-icon>
          </el-alert>
          <el-alert
            :title="$t('secStorageTips')"
            type="warning"
            :closable="false"
            class="ksd-mb-8"
            v-if="isShowSecStorageTips"
            show-icon>
          </el-alert>
          <span class="ksd-title-label-mini">
            <span>{{$t('secStorage')}}</span>
            <el-tooltip effect="dark" placement="right">
              <span slot="content" v-html="$t('secStorageDesc')"></span>
              <i class="el-icon-ksd-what ksd-fs-14"></i>
            </el-tooltip>
          </span>
          <span class="sec-switch">
            <common-tip :content="disableSecStorageTips" :disabled="!isNotBatchModel && !isHaveNoDimMeas">
              <el-switch
                :disabled="isNotBatchModel || isHaveNoDimMeas"
                v-model="modelDesc.with_second_storage"
                @change="val => handleSecStorageEnabled(val)"
                :active-text="$t('kylinLang.common.OFF')"
                :inactive-text="$t('kylinLang.common.ON')">
              </el-switch>
            </common-tip>
          </span>
        </div>
        <div class="data-filter ksd-title-label-mini ksd-mb-8">
          {{$t('dataFilterCond')}}
          <el-tooltip effect="dark" :content="$t('dataFilterCondTips')" placement="right">
            <i class="el-icon-ksd-what ksd-fs-14"></i>
          </el-tooltip>
        </div>
        <el-alert
          :title="$t('filterCondTips')"
          type="warning"
          :closable="false"
          class="ksd-mb-8"
          show-icon>
        </el-alert>
        <kylin-editor ref="dataFilterCond" :key="isShow" :placeholder="$t('filterPlaceholder')" height="95" width="99.6%" lang="sql" theme="chrome" v-model="filterCondition"></kylin-editor>
      </div>
      <div class="error-msg-box ksd-mt-10" v-if="filterErrorMsg">
        <div class="error-tag">{{$t('errorMsg')}}</div>
        <div v-html="filterErrorMsg"></div>
      </div>
    </template>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" @click="isShow && handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" v-if="isShow" :disabled="isLoadingNewRange||disabledSave" :loading="isLoadingSave" @click="savePartitionConfirm" size="medium">{{mode === 'saveModel' ? $t('modelSaveSet') : $t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../../store'
import locales from './locales'
import store, { types } from './store'
import { timeDataType, dateFormats, timestampFormats, dateTimestampFormats } from '../../../../../config'
import NModel from '../../ModelEdit/model.js'
import { objectClone, isSubPartitionType, indexOfObjWithSomeKey, isStreamingPartitionType } from '../../../../../util'
import { handleSuccess, transToUTCMs } from 'util/business'
import { handleSuccessAsync, handleError } from 'util/index'
vuex.registerModule(['modals', 'ModelSaveConfig'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions'
    ]),
    // Store数据注入
    ...mapState('ModelSaveConfig', {
      isShow: state => state.isShow,
      mode: state => state.form.mode,
      modelDesc: state => state.form.modelDesc,
      modelInstance: state => state.form.modelInstance || state.form.modelDesc && new NModel(state.form.modelDesc) || null,
      allDimension: state => state.form.allDimension,
      isChangeModelLayout: state => state.form.isChangeModelLayout,
      exchangeJoinTableList: state => state.form.exchangeJoinTableList,
      callback: state => state.callback
    }),
    ...mapState({
      otherColumns: state => state.model.otherColumns
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('ModelSaveConfig', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    ...mapMutations({
      resetOtherColumns: 'RESET_OTHER_COLUMNS'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    // 后台接口请求
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      loadHiveInProject: 'LOAD_HIVE_IN_PROJECT',
      saveKafka: 'SAVE_KAFKA',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      saveSampleData: 'SAVE_SAMPLE_DATA',
      setModelPartition: 'MODEL_PARTITION_SET',
      fetchNewestModelRange: 'GET_MODEL_NEWEST_RANGE',
      fetchPartitionFormat: 'FETCH_PARTITION_FORMAT',
      checkFilterConditon: 'CHECK_FILTER_CONDITION',
      fetchSegments: 'FETCH_SEGMENTS',
      validateDateFormat: 'VALIDATE_DATE_FORMAT'
    })
  },
  locales
})
export default class ModelPartitionModal extends Vue {
  isLoading = false
  isFormShow = false
  isLoadingNewRange = false
  isLoadingFormat = false
  isLoadingSave = false
  partitionMeta = {
    table: '',
    column: '',
    format: '',
    multiPartition: ''
  }
  timeDataType = timeDataType
  rules = {
    dataRangeVal: [{
      validator: this.validateRange, trigger: 'blur'
    }]
  }
  partitionRules = {
    column: [{validator: this.validateBrokenColumn, trigger: 'change'}]
  }
  filterErrorMsg = ''
  prevPartitionMeta = {
    table: '',
    column: '',
    format: '',
    multiPartition: ''
  }
  buildType = 'incremental'
  isShowWarning = false
  importantChange = false
  isExpand = false
  defaultBuildType = 'incremental'
  source_types = []
  isShowSecStorageTips = false
  isShowSecStorageTips2 = false
  isShowSecondStoragePartitionTips = false
  isAlreadyHavePartition = false
  filterCondition = ''
  originFilterCondition = ''
  dateFormats = dateFormats
  timestampFormats = timestampFormats
  dateTimestampFormats = dateTimestampFormats
  isExpandFormatRule = false
  formatedDate = ''
  errorFormat = ''

  handleSecStorageEnabled (val) {
    this.isShowSecStorageTips2 = val
    if (!val && this.modelDesc.second_storage_size > 0) {
      this.isShowSecStorageTips = true
    } else {
      this.isShowSecStorageTips = false
    }
  }

  toggleShowPartition () {
    this.isExpand = !this.isExpand
  }
  get disabledSave () {
    if (this.buildType === 'incremental' && this.partitionMeta.table && this.partitionMeta.column && this.partitionMeta.format || this.buildType === 'fullLoad') {
      return false
    } else {
      return true
    }
  }

  get factTableType () {
    const obj = this.modelInstance.getFactTable()
    return obj.source_type
  }

  dataSize (storage) {
    return Vue.filter('dataSize')(storage)
  }

  handChangeBuildType (val) {
    this.isShowWarning = typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0 && (this.defaultBuildType !== this.buildType || this.isChangePartition)
    if (val === 'incremental' && !this.partitionMeta.table) {
      this.partitionMeta.table = this.partitionTables[0].alias
    }
  }
  validateRange (rule, value, callback) {
    const [ startValue, endValue ] = value
    if ((startValue && endValue && transToUTCMs(startValue) < transToUTCMs(endValue)) || !startValue && !endValue) {
      callback()
    } else {
      callback(new Error(this.$t('invaildDate')))
    }
  }
  validateBrokenColumn (rule, value, callback) {
    if (value) {
      if (this.checkIsBroken(this.brokenPartitionColumns, value)) {
        return callback(new Error(this.$t('noColumnFund')))
      }
    }
    if (!value && this.partitionMeta.table) {
      return callback(new Error(this.$t('pleaseInputColumn')))
    }
    callback()
  }
  // 增量加载下，更改分区设置列
  async changeColumn (type, val) {
    this.formatedDate = ''
    this.errorFormat = ''
    this.isNotBatchModel && (this.partitionMeta.format = this.dateFormatsOptions[0].value)
    if (type === 'format' && val || this.isNotBatchModel && type === 'column') { // 非批数据模型分区列默认选中第一个分区列格式，并且调用一下预览
      try {
        const res = await this.validateDateFormat({partition_date_column: this.partitionMeta.column, partition_date_format: this.partitionMeta.format})
        this.formatedDate = await handleSuccessAsync(res)
      } catch (e) {
        this.errorFormat = e.body.msg
        this.formatedDate = ''
      }
    }
    if (JSON.stringify(this.prevPartitionMeta) !== JSON.stringify(this.partitionMeta)) {
      if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
        this.isShowWarning = true
      }
    } else {
      this.isShowWarning = false
    }
  }
  modelBuildMeta = {
    dataRangeVal: [],
    isLoadExisted: false
  }
  checkIsBroken (brokenKeys, key) {
    if (key) {
      return ~brokenKeys.indexOf(key)
    }
    return false
  }
  async handleLoadFormat () {
    try {
      this.isLoadingFormat = true
      const ccColumns = this.modelInstance.getComputedColumns()
      const index = indexOfObjWithSomeKey(ccColumns, 'columnName', this.partitionMeta.column)
      const data = { project: this.currentSelectedProject, table: this.selectedTable.name, partition_column: this.partitionMeta.column }
      if (index !== -1) { // 分区列是CC列
        data.expression = ccColumns[index].innerExpression
      }
      const response = await this.fetchPartitionFormat(data)
      this.partitionMeta.format = await handleSuccessAsync(response)
      this.changeColumn('format', this.partitionMeta.format)
      this.isLoadingFormat = false
    } catch (e) {
      this.isLoadingFormat = false
      handleError(e)
    }
  }
  get partitionTitle () {
    if (this.mode === 'saveModel') {
      return this.$t('saveModel')
    } else {
      return this.$t('modelPartitionSet')
    }
  }
  get partitionTables () {
    let result = []
    if (this.isShow && this.modelInstance) {
      Object.values(this.modelInstance.tables).forEach((nTable) => {
        if (nTable.kind === 'FACT') {
          result.push(nTable)
        }
      })
    }
    return result
  }

  // 获取破损的partition keys
  get brokenPartitionColumns () {
    if (this.partitionMeta.table) {
      let ntable = this.modelInstance.getTableByAlias(this.partitionMeta.table)
      if (ntable) {
        return this.modelInstance.getBrokenModelLinksKeys(ntable.guid, [this.partitionMeta.column])
      } else {
        return []
      }
    }
    return []
  }
  get selectedTable () {
    if (this.partitionMeta.table) {
      for (let i = 0; i < this.partitionTables.length; i++) {
        if (this.partitionTables[i].alias === this.partitionMeta.table) {
          return this.partitionTables[i]
        }
      }
    }
  }
  get disableSecStorageTips () {
    if (this.isNotBatchModel) {
      return this.$t('disableSecStorageActionTips')
    }
    if (this.isHaveNoDimMeas) {
      return this.$t('disableSecStorageActionTips2')
    }
  }
  get isHaveNoDimMeas () {
    return this.modelDesc.simplified_dimensions.length === 0 && this.modelDesc.simplified_measures.length === 1 && this.modelDesc.simplified_measures[0].name === 'COUNT_ALL' // 没有设置维度，只有默认度量
  }
  get isStreamModel () {
    const factTable = this.modelInstance.getFactTable()
    return factTable.source_type ? (factTable.source_type === 1 && !factTable.batch_table_identity) : this.modelInstance.model_type === 'STREAMING'
  }
  get isNotBatchModel () {
    const factTable = this.modelInstance.getFactTable()
    return factTable.source_type ? factTable.source_type === 1 : ['STREAMING', 'HYBRID'].includes(this.modelInstance.model_type)
  }
  get isHybridModel () {
    const factTable = this.modelInstance.getFactTable()
    return factTable.batch_table_identity
  }
  get dateFormatsOptions () {
    return this.isNotBatchModel ? timestampFormats : dateFormats
  }
  get columns () {
    if (!this.isShow || this.partitionMeta.table === '') {
      return []
    }
    let result = []
    let factTable = this.modelInstance.getFactTable()
    if (factTable) {
      factTable.columns.forEach((x) => {
        if (this.isNotBatchModel && isStreamingPartitionType(x.datatype)) {
          result.push(x)
        } else if (!this.isNotBatchModel && isSubPartitionType(x.datatype)) {
          result.push(x)
        }
      })
    }
    // 暂不支持CC列做分区列
    let ccColumns = this.modelInstance.getComputedColumns()
    let cloneCCList = objectClone(ccColumns)
    cloneCCList.forEach((x) => {
      let cc = {
        name: x.columnName,
        datatype: x.datatype
      }
      result.push(cc)
    })
    return result
  }
  getColumnInfo (column) {
    if (this.selectedTable) {
      let len = this.selectedTable.columns && this.selectedTable.columns.length || 0
      for (let i = 0; i < len; i++) {
        const col = this.selectedTable.columns[i]
        if (col.name === column) {
          return col
        }
      }
    }
  }
  @Watch('isShow')
  initModeDesc () {
    if (this.isShow) {
      this.modelBuildMeta.dataRangeVal = []
      // this.$nextTick(() => {
      //   this.$refs.partitionForm && this.$refs.partitionForm.validate()
      // })
      const partition_desc = this.modelDesc.partition_desc
      this.isExpand = !this.modelDesc.uuid && !this.isNotBatchModel
      // 新建模型是，默认添加基础索引。编辑时已添加基础索引不显示，未添加索引是未勾选状态，用户可选择勾选
      if (!this.modelDesc.uuid) {
        this.source_types = ['BASE_TABLE_INDEX', 'BASE_AGG_INDEX']
      }
      if (this.modelDesc.uuid && !(partition_desc && partition_desc.partition_date_column) && !this.isNotBatchModel) {
        this.buildType = 'fullLoad'
        this.defaultBuildType = 'fullLoad'
      }
      if (this.modelDesc && partition_desc && partition_desc.partition_date_column) {
        this.isAlreadyHavePartition = true
        let named = partition_desc.partition_date_column.split('.')
        this.partitionMeta.table = this.prevPartitionMeta.table = named[0]
        this.partitionMeta.column = this.prevPartitionMeta.column = named[1]
        this.partitionMeta.format = this.prevPartitionMeta.format = partition_desc.partition_date_format
        this.partitionMeta.multiPartition = this.prevPartitionMeta.multiPartition = this.modelDesc.multi_partition_desc && this.modelDesc.multi_partition_desc.columns[0] && this.modelDesc.multi_partition_desc.columns[0].split('.')[1] || ''
      } else {
        this.partitionMeta.table = this.partitionTables[0].alias // 默认增量构建选择事实表
      }
      this.filterCondition = this.modelDesc.filter_condition
      this.originFilterCondition = this.modelDesc.filter_condition
      this.$nextTick(() => {
        this.setAutoCompleteData()
      })
    } else {
      this.resetForm()
    }
  }
  @Watch('filterCondition')
  filterConditionChange (val, oldVal) {
    if (val !== oldVal) {
      this.resetMsg()
    }
  }
  resetMsg () {
    this.filterErrorMsg = ''
  }
  partitionTableChange () {
    this.partitionMeta.column = ''
    this.partitionMeta.format = ''
    this.partitionMeta.multiPartition = ''
  }
  resetForm () {
    this.partitionMeta = {
      table: '',
      column: '',
      format: '',
      multiPartition: ''
    }
    this.prevPartitionMeta = { table: '', column: '', format: '', multiPartition: '' }
    this.filterCondition = ''
    this.isLoadingSave = false
    this.isLoadingFormat = false
    this.isShowWarning = false
    this.defaultBuildType = 'incremental'
    this.isShowSecStorageTips = false
    this.isShowSecStorageTips2 = false
    this.isShowSecondStoragePartitionTips = false
    this.isExpandFormatRule = false
  }

  get isChangeToFullLoad () {
    return this.prevPartitionMeta.table && this.buildType === 'fullLoad'
  }

  get isChangePartition () {
    return (this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format || this.prevPartitionMeta.multiPartition !== this.partitionMeta.multiPartition) && this.buildType === 'incremental'
  }

  get cannotSaveModelTips () {
    if (this.isHybridModel) {
      return this.$t('streamSecStoragePartitionTips')
    } else {
      return this.$t('secondStoragePartitionTips')
    }
  }

  async savePartitionConfirm () {
    await (this.$refs.rangeForm && this.$refs.rangeForm.validate()) || Promise.resolve()
    await (this.$refs.partitionForm && this.$refs.partitionForm.validate()) || Promise.resolve()
    // 开启了分层存储或者融合数据模型，时间分区列必须选做维度列
    if (this.partitionMeta.table && this.partitionMeta.column && this.buildType === 'incremental' && (this.modelDesc.with_second_storage || this.isHybridModel)) {
      const partitionColumn = this.partitionMeta.table + '.' + this.partitionMeta.column
      const index = indexOfObjWithSomeKey(this.allDimension, 'column', partitionColumn)
      if (index === -1) {
        this.isShowSecondStoragePartitionTips = true
        return
      }
    }
    let isOnlySave = true
    if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
      if (this.isChangeToFullLoad || this.isChangePartition) {
        this.importantChange = true
        const res = await this.callGlobalDetailDialog({
          msg: this.$t('changeSegmentTips'),
          title: this.$t('kylinLang.common.tip'),
          dialogType: 'warning',
          showDetailBtn: false,
          isSubSubmit: true,
          wid: '600px',
          dangerouslyUseHTMLString: true,
          submitSubText: this.$t('kylinLang.common.save'),
          submitText: this.$t('saveAndLoad'),
          isHideSubmit: !this.isChangeToFullLoad
        })
        isOnlySave = res.isOnlySave
      } else if (this.isChangeModelLayout || this.originFilterCondition !== this.filterCondition) {
        let result = null
        this.importantChange = true
        if (this.exchangeJoinTableList.length > 0 && this.exchangeJoinTableList.filter(it => it.joinType === 'LEFT' && it.isNew).length === this.exchangeJoinTableList.length) {
          result = await this.callGlobalDetailDialog({
            msg: this.$t('onlyAddLeftJoinTip'),
            title: this.$t('kylinLang.common.tip'),
            dialogType: 'warning',
            showDetailBtn: false,
            isSubSubmit: true,
            wid: '600px',
            isHideSubmit: true,
            submitSubText: this.$t('kylinLang.common.save')
          })
        } else {
          result = await this.callGlobalDetailDialog({
            msg: this.$t('purgeSegmentDataTips', {storageSize: Vue.filter('dataSize')(this.modelInstance.storage)}),
            title: this.$t('kylinLang.common.tip'),
            dialogType: 'warning',
            showDetailBtn: false,
            isSubSubmit: true,
            wid: '600px',
            submitSubText: this.$t('kylinLang.common.save'),
            submitText: this.$t('saveAndLoad')
          })
        }
        isOnlySave = result.isOnlySave
      } else {
        this.importantChange = false
      }
      this.savePartition(isOnlySave)
    } else {
      this.savePartition(isOnlySave)
    }
  }

  savePartition (isOnlySave) {
    this.modelDesc.partition_desc = this.modelDesc.partition_desc || {}
    let hasSetDate = this.partitionMeta.table && this.partitionMeta.column && this.buildType === 'incremental'
    if (this.modelDesc && this.partitionMeta.table && this.partitionMeta.column && this.buildType === 'incremental') {
      this.modelDesc.partition_desc.partition_date_column = hasSetDate ? this.partitionMeta.table + '.' + this.partitionMeta.column : ''
    } else {
      this.modelDesc.partition_desc.partition_date_column = ''
    }
    if (this.partitionMeta.multiPartition && this.buildType === 'incremental') {
      this.modelDesc.multi_partition_desc = {
        ...this.modelInstance.multi_partition_desc || {},
        columns: [this.partitionMeta.table + '.' + this.partitionMeta.multiPartition]
      }
    } else {
      this.modelDesc.multi_partition_desc = null
    }
    this.modelDesc.partition_desc.partition_date_format = this.partitionMeta.format
    this.modelDesc.filter_condition = this.filterCondition
    this.modelDesc.project = this.currentSelectedProject
    if (this.modelBuildMeta.dataRangeVal[0] && this.modelBuildMeta.dataRangeVal[1]) {
      this.modelDesc.start = (+transToUTCMs(this.modelBuildMeta.dataRangeVal[0]))
      this.modelDesc.end = (+transToUTCMs(this.modelBuildMeta.dataRangeVal[1]))
    }
    this.modelDesc.other_columns = this.otherColumns.length ? this.otherColumns : this.getOtherColumns()
    if (this.mode === 'saveModel') {
      this.isLoadingSave = true
      const checkData = objectClone(this.modelDesc)
      // 如果未选择partition 把partition desc 设置为null
      if (!(checkData && checkData.partition_desc && checkData.partition_desc.partition_date_column) || this.buildType === 'fullLoad') {
        checkData.partition_desc = null
      }
      this.checkFilterConditon(checkData).then((res) => {
        handleSuccess(res, async (data) => {
          // TODO HA 模式时 post 等接口需要等待同步完去刷新列表
          if (!this.importantChange && 'rebuild_index' in data && data.rebuild_index) {
            try {
              const res = await this.callGlobalDetailDialog({
                msg: this.$t('editCCBuildTip'),
                title: this.$t('kylinLang.common.tip'),
                dialogType: 'warning',
                showDetailBtn: false,
                isSubSubmit: true,
                wid: '600px',
                submitSubText: this.$t('kylinLang.common.save'),
                submitText: this.$t('saveAndLoad'),
                needConcelReject: true
              })
              this.handleClose(true, res.isOnlySave)
              this.isLoadingSave = false
            } catch (e) {
              this.isLoadingSave = false
            }
          } else {
            this.handleClose(true, isOnlySave)
            this.isLoadingSave = false
          }
        })
      }, (errorRes) => {
        this.filterErrorMsg = errorRes.data.msg ?? errorRes.data.message
        this.isLoadingSave = false
        this.$nextTick(() => {
          this.$el.querySelector('.error-msg-box') && this.$el.querySelector('.error-msg-box').scrollIntoView()
        })
      })
    } else {
      this.handleClose(true, isOnlySave)
    }
  }
  handleClose (isSubmit, isOnlySave) {
    this.isLoadingFormat = false
    this.modelDesc.save_only = isOnlySave
    this.filterErrorMsg = ''
    // 不把这个信息记录下来的话，300 延迟后，modelDesc 就 undefined 了
    let temp = objectClone(this.modelDesc)
    setTimeout(() => {
      this.callback && this.callback({
        isSubmit: isSubmit,
        isPurgeSegment: this.isChangePartition,
        data: temp,
        base_index_type: this.source_types
      })
      this.hideModal()
      this.resetModalForm()
    }, 300)
  }
  showToolTip (value) {
    let len = 0
    value.split('').forEach((v) => {
      if (/[\u4e00-\u9fa5]/.test(v)) {
        len += 2
      } else {
        len += 1
      }
    })
    return len <= 15
  }
  getOtherColumns () {
    const { simplified_dimensions } = this.modelDesc
    const { tables } = this.modelInstance
    const selectDimensionIds = simplified_dimensions.map(it => it.column)
    let allColumns = []
    const others = []
    Object.values(tables).forEach(it => {
      it.columns && (allColumns = [...allColumns, ...it.columns.map(item => ({column: `${it.alias}.${item.name}`, name: item.name, datatype: item.datatype}))])
    })
    allColumns.filter(item => !selectDimensionIds.includes(item.column)).forEach((it, index, self) => {
      const names = self.map(it => it.name)
      const [table, column] = it.column.split('.')
      if (names.indexOf(it.name) !== names.lastIndexOf(it.name)) {
        others.push({...it, name: `${column}_${table}`})
      } else {
        others.push(it)
      }
    })
    return others
  }
  setAutoCompleteData () {
    const columnList = this.modelInstance.getTableColumns()
    let ad = columnList.map((col) => {
      return {
        meta: col.datatype,
        caption: col.full_colname,
        value: col.full_colname,
        id: col.id,
        scope: 1
      }
    })
    this.$refs.dataFilterCond.$emit('setAutoCompleteData', ad)
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.model-partition-dialog {
  .buildType-switch {
    .el-tabs__header {
      width: 100%;
      .el-tabs__nav {
        width: 97.8%;
        .el-tabs__item {
          width: 50%;
          .el-tag {
            position: relative;
            bottom: 1px;
          }
        }
      }
    }
  }
  .el-tabs__item.is-disabled {
    cursor: not-allowed;
  }
  .second-setting,
  .data-filter {
    .el-icon-ksd-what {
      position: relative;
      top: 1px;
    }
  }
  .error-format {
    color: @error-color-1;
    font-size: 12px;
    line-height: 16px;
  }
  .pre-format {
    color: @text-normal-color;
    font-size: 14px;
    margin-top: 4px;
    background-color: @ke-background-color-secondary;
    border: 1px solid @ke-border-divider-color;
    border-radius: 6px;
    height: 26px;
    line-height: 26px;
    border-radius: 4px;
    display: inline-block;
    padding: 0 4px;
  }
  .format {
    font-size: 12px;
    line-height: 16px;
    color: @text-disabled-color;
    margin-top: 4px;
    span {
      color: @base-color;
      cursor: pointer;
      .arrow {
        margin-left: 3px;
      }
    }
  }
  .detail-content {
    background-color: @ke-background-color-secondary;
    border: 1px solid @ke-border-divider-color;
    border-radius: 6px;
    padding: 8px 16px;
    box-sizing: border-box;
    font-size: 12px;
    color: @text-normal-color;
    line-height: 16px;
    margin-top: 4px;
    p {
      display: flex;
    }
  }
  .secStorage-desc {
    font-size: 12px;
    line-height: 16px;
    color: @text-normal-color;
  }
  .error-msg-box {
    border: 1px solid @line-border-color;
    max-height: 55px;
    overflow: auto;
    font-size: 12px;
    padding: 10px;
    .error-tag {
      color: @error-color-1;
    }
  }
  .base-index-block {
    .el-icon-ksd-what {
      position: relative;
      top: 1px;
    }
  }
  .divide-block {
    color: @text-placeholder-color;
    position: relative;
    text-align: center;
    margin-top: 16px;
    font-size: 12px;
    span {
      cursor: default;
      position: absolute;
      display: inline-block;
      top: -8px;
      background-color: #fff;
      padding: 0 10px;
      left: 44%;
    }
    .divider {
      margin: 10px 0;
      border-bottom: 1px solid @ke-border-secondary;
      position: relative;
    }
    &.advance-setting {
      color: @ke-color-primary;
      span {
        cursor: pointer;
      }
    }
  }
  .item-desc {
    font-size: 12px;
    line-height: 1;
  }
  .where-area {
    margin-top:20px;
  }
  .up-performance{
    i {
      color:@normal-color-1;
      margin-right: 7px;
    }
    span {
      color:@normal-color-1;
      margin-left: 7px;
    }
  }
  .down-performance{
    i {
      color:@error-color-1;
      margin-right: 7px;
    }
    span {
      color:@error-color-1;
      margin-left: 7px;
    }
  }
  .ksd-title-label {
    .icon {
      vertical-align: initial;
      color: @text-disabled-color;
    }
  }
}
.table-column-name {
  display: inline-block;
  width: 143px;
  overflow: hidden;
  text-overflow: ellipsis;
}

</style>
