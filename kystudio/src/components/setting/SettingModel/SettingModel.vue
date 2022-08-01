<template>
  <div class="setting-model">
    <div  class="ksd-mb-10 ksd-fright">
      <el-input :placeholder="$t('kylinLang.common.pleaseFilterByModelName')" style="width:200px" size="medium" prefix-icon="el-ksd-icon-search_22" v-model="filter.model_name" v-global-key-event.enter.debounce="searchModels" @clear="searchModels()">
      </el-input>
    </div>
    <el-table
      :data="modelList"
      class="model-setting-table"
      v-scroll-shadow
      :empty-text="emptyText"
      style="width: 100%">
      <el-table-column width="230px" show-overflow-tooltip prop="alias" :label="modelTableTitle"></el-table-column>
      <el-table-column prop="last_modified" show-overflow-tooltip width="218px" :label="$t('modifyTime')">
        <template slot-scope="scope">
          <span v-if="scope.row.config_last_modified>0">{{transToGmtTime(scope.row.config_last_modified)}}</span>
        </template>
      </el-table-column>
      <el-table-column prop="config_last_modifier" show-overflow-tooltip width="150" :label="$t('modifiedUser')"></el-table-column>
      <el-table-column min-width="400px" :label="$t('modelSetting')">
        <template slot-scope="scope">
          <div v-if="scope.row.auto_merge_time_ranges">
            <span class="model-setting-item">
              {{$t('segmentMerge')}}<span v-for="item in scope.row.auto_merge_time_ranges" :key="item">{{$t(item)}}</span>
            </span><common-tip :content="$t('kylinLang.common.edit')">
              <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editMergeItem(scope.row)"></i>
            </common-tip><common-tip :content="$t('kylinLang.common.delete')">
              <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'auto_merge_time_ranges')"></i>
            </common-tip>
          </div>
          <div v-if="scope.row.volatile_range">
            <span class="model-setting-item" @click="editVolatileItem(scope.row)">
              {{$t('volatileRange')}}<span>{{scope.row.volatile_range.volatile_range_number}} {{$t(scope.row.volatile_range.volatile_range_type.toLowerCase())}}</span>
            </span><common-tip :content="$t('kylinLang.common.edit')">
              <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editVolatileItem(scope.row)"></i>
            </common-tip><common-tip :content="$t('kylinLang.common.delete')">
              <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'volatile_range')"></i>
            </common-tip>
          </div>
          <div v-if="scope.row.retention_range">
            <span class="model-setting-item" @click="editRetentionItem(scope.row)">
              {{$t('retention')}}<span>{{scope.row.retention_range.retention_range_number}} {{$t(scope.row.retention_range.retention_range_type.toLowerCase())}}</span>
            </span><common-tip :content="$t('kylinLang.common.edit')">
              <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editRetentionItem(scope.row)"></i>
            </common-tip><common-tip :content="$t('kylinLang.common.delete')">
              <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'retention_range')"></i>
            </common-tip>
          </div>
          <div v-if="Object.keys(scope.row.override_props).length">
            <div v-for="(propValue, key) in scope.row.override_props" :key="key">
              <template v-if="key.includes('kylin.engine.spark-conf') && defaultConfigs.includes(key.replace(/^kylin.engine.spark-conf./, ''))">
                <span class="model-setting-item" @click="editSparkItem(scope.row, key)">
                  <!-- 去掉前缀kylin.engine.spark-conf. -->
                  {{key}}:<span>{{propValue}}</span>
                </span><common-tip :content="$t('kylinLang.common.edit')">
                  <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editSparkItem(scope.row, key)"></i>
                </common-tip><common-tip :content="$t('kylinLang.common.delete')">
                  <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, key)"></i>
                </common-tip>
              </template>
              <template v-else-if="key.split('.').includes('cube')">
                <span class="model-setting-item" @click="editCubeItem(scope.row, key)">
                  {{key.split('.').slice(-1).toString()}}:<span>{{propValue}}</span>
                </span><common-tip :content="$t('kylinLang.common.edit')">
                  <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editCubeItem(scope.row, key)"></i>
                </common-tip><common-tip :content="$t('kylinLang.common.delete')">
                  <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, key)"></i>
                </common-tip>
              </template>
              <template v-else>
                <span class="model-setting-item" @click="editCustomSettingItem(scope.row, key)">
                  {{key}}:<span>{{propValue}}</span>
                </span><common-tip :content="$t('kylinLang.common.edit')">
                  <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editCustomSettingItem(scope.row, key)"></i>
                </common-tip><common-tip :content="$t('kylinLang.common.delete')">
                  <i class="el-icon-ksd-symbol_type" @click="removeCustomSettingItem(scope.row, key)"></i>
                </common-tip>
              </template>
            </div>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        width="83px"
        :label="$t('kylinLang.common.action')">
          <template slot-scope="scope">
            <common-tip>
              <div slot="content">{{$t('addSettingItem')}}</div>
              <i class="el-ksd-icon-table_add_old" @click="addSettingItem(scope.row)"></i>
            </common-tip>
          </template>
      </el-table-column>
    </el-table>
    <kylin-pager :totalSize="modelListSize" :perPageSize="filter.page_size" :curPage="filter.page_offset+1" v-on:handleCurrentChange='currentChange' ref="modleConfigPager" :refTag="pageRefTags.modleConfigPager" class="ksd-mtb-10 ksd-center" ></kylin-pager>
    <el-dialog :title="modelSettingTitle" :visible.sync="editModelSetting" width="480px" class="model-setting-dialog" @closed="handleClosed" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form ref="form" label-position="top" size="medium" label-width="80px" :model="modelSettingForm" :rules="rules">
        <el-form-item :label="modelTableTitle">
          <el-input v-model.trim="modelSettingForm.name" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('settingItem')" v-if="step=='stepOne'" prop="settingItem">
          <el-select v-model="modelSettingForm.settingItem" size="medium" :placeholder="$t('kylinLang.common.pleaseSelect')" style="width:100%">
            <el-option
              v-for="item in settingOption"
              :key="item"
              :label="$t(settingMap[item])"
              :value="item">
            </el-option>
          </el-select>
          <p v-html="optionDesc[modelSettingForm.settingItem]"></p>
        </el-form-item>
        <el-form-item :label="$t('autoMerge')" class="ksd-mb-10" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Auto-merge'">
          <el-checkbox-group v-model="modelSettingForm.autoMerge" class="merge-groups">
            <div v-for="(item, index) in mergeGroups" :key="item"> <el-checkbox :label="item" v-if="index<3">{{$t(item)}}</el-checkbox></div>
            <div v-for="(item, index) in mergeGroups" :key="item"><el-checkbox :label="item" v-if="index>2">{{$t(item)}}</el-checkbox></div>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item :label="$t('volatileRangeItem')" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Volatile Range'">
          <el-input v-model="modelSettingForm.volatileRange.volatile_range_number" v-number="modelSettingForm.volatileRange.volatile_range_number" :placeholder="$t('kylinLang.common.pleaseInput')" class="retention-input"></el-input>
          <el-select v-model="modelSettingForm.volatileRange.volatile_range_type" class="ksd-ml-8" size="medium" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option
              v-for="item in units"
              :key="item.label"
              :label="$t(item.label)"
              :value="item.value">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('retentionThreshold')" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Retention Threshold'">
          <el-input v-model="modelSettingForm.retentionThreshold.retention_range_number" v-number="modelSettingForm.retentionThreshold.retention_range_number" :placeholder="$t('kylinLang.common.pleaseInput')" class="retention-input"></el-input>
          <!-- <span class="ksd-ml-10">{{$t(modelSettingForm.retentionThreshold.retention_range_type.toLowerCase())}}</span> -->
          <el-select
            class="ksd-ml-10"
            style="width: 100px;"
            v-model="modelSettingForm.retentionThreshold.retention_range_type"
            :placeholder="$t('kylinLang.common.pleaseChoose')">
            <el-option
              v-for="type in retentionTypes.filter(it => it !== 'WEEK')"
              :key="type"
              :label="$t(type.toLowerCase())"
              :value="type">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="settingMap[modelSettingForm.settingItem]" v-if="step=='stepTwo'&&modelSettingForm.settingItem.indexOf('spark.')!==-1">
          <el-input v-model="modelSettingForm[modelSettingForm.settingItem]" v-number="modelSettingForm[modelSettingForm.settingItem]" :placeholder="$t('kylinLang.common.pleaseInput')" class="retention-input"></el-input><span
          class="ksd-ml-5" v-if="modelSettingForm.settingItem==='spark.executor.memory'">G</span>
        </el-form-item>
        <el-form-item :label="settingMap[modelSettingForm.settingItem]" v-if="step=='stepTwo'&&modelSettingForm.settingItem === 'is-base-cuboid-always-valid'">
          <el-select v-model="modelSettingForm['is-base-cuboid-always-valid']">
            <el-option
              v-for="item in baseCuboidValid"
              :key="item.value"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t(settingMap[modelSettingForm.settingItem])" v-if="step=='stepTwo'&&modelSettingForm.settingItem === 'customSettings'">
          <div class="custom-settings" v-for="(item, index) in modelSettingForm[modelSettingForm.settingItem]" :key="index">
            <el-input :class="['custom-param-key', {'is-editting': isEdit}]" v-model.trim="modelSettingForm[modelSettingForm.settingItem][index][0]" :placeholder="$t('customSettingKeyPlaceholder')" />
            <el-input :class="['custom-param-value', {'is-editting': isEdit}]" v-model.trim="modelSettingForm[modelSettingForm.settingItem][index][1]" :placeholder="$t('customSettingValuePlaceholder')" />
            <span class="ky-no-br-space ksd-ml-2" v-if="!isEdit">
              <el-button type="primary" icon="el-ksd-icon-add_16" plain circle size="mini" @click="addCustomSetting"></el-button>
              <el-button icon="el-ksd-icon-minus_16" class="ksd-ml-2" plain circle size="mini" :disabled="modelSettingForm[modelSettingForm.settingItem].length === 1 && index === 0" @click="removeCustomSetting(index)"></el-button>
            </span>
          </div>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="editModelSetting = false" v-if="step=='stepOne' || (step=='stepTwo' && isEdit)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button @click="preStep" icon="el-ksd-icon-arrow_left_22" size="medium" v-if="step=='stepTwo' && !isEdit">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button type="primary" @click="nextStep" size="medium" iconr="el-ksd-icon-arrow_right_22" v-if="step=='stepOne'" :disabled="modelSettingForm.settingItem==''">{{$t('kylinLang.common.next')}}</el-button>
        <el-button
          type="primary"
          @click="submit"
          size="medium"
          v-if="step=='stepTwo'"
          :loading="isLoading"
          :disabled="isSubmit">
            {{$t('kylinLang.common.submit')}}
          </el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'

import locales from './locales'
import { pageCount, pageRefTags } from '../../../config'
import { handleSuccess, transToGmtTime, kylinConfirm } from '../../../util/business'
import { handleSuccessAsync, handleError, objectClone, ArrayFlat } from '../../../util/index'
import { retentionTypes } from '../handler'

const initialSettingForm = JSON.stringify({
  name: '',
  settingItem: '',
  autoMerge: [],
  volatileRange: {volatile_range_number: 0, volatile_range_type: '', volatile_range_enabled: true},
  retentionThreshold: {retention_range_number: 0, retention_range_type: '', retention_range_enabled: true},
  'spark.executor.cores': null,
  'spark.executor.instances': null,
  'spark.executor.memory': null,
  'spark.sql.shuffle.partitions': null,
  'is-base-cuboid-always-valid': 0,
  customSettings: [[]]
})

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      loadModelConfigList: 'LOAD_MODEL_CONFIG_LIST',
      updateModelConfig: 'UPDATE_MODEL_CONFIG'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales
})
export default class SettingStorage extends Vue {
  pageRefTags = pageRefTags
  retentionTypes = retentionTypes
  modelList = []
  modelListSize = 0
  filter = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.modleConfigPager) || pageCount,
    model_name: ''
  }
  editModelSetting = false
  isLoading = false
  isEdit = false
  step = 'stepOne'
  // settingOption = ['Auto-merge', 'Volatile Range', 'Retention Threshold', 'spark.executor.cores', 'spark.executor.instances', 'spark.executor.memory', 'spark.sql.shuffle.partitions']
  mergeGroups = ['HOUR', 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR']
  units = [{label: 'hour', value: 'HOUR'}, {label: 'day', value: 'DAY'}, {label: 'week', value: 'WEEK'}, {label: 'month', value: 'MONTH'}, {label: 'quarter', value: 'QUARTER'}, {label: 'year', value: 'YEAR'}]
  baseCuboidValid = [{label: 'true', value: 0}, {label: 'false', value: 1}]
  modelSettingForm = JSON.parse(initialSettingForm)
  activeRow = null
  settingMap = {
    'Auto-merge': 'Auto-merge',
    'Volatile Range': 'Volatile Range',
    'Retention Threshold': 'Retention Threshold',
    'spark.executor.cores': 'kylin.engine.spark-conf.spark.executor.cores',
    'spark.executor.instances': 'kylin.engine.spark-conf.spark.executor.instances',
    'spark.executor.memory': 'kylin.engine.spark-conf.spark.executor.memory',
    'spark.sql.shuffle.partitions': 'kylin.engine.spark-conf.spark.sql.shuffle.partitions',
    'is-base-cuboid-always-valid': 'is-base-cuboid-always-valid',
    'customSettings': 'customSettings'
  }
  defaultConfigs = ['spark.executor.cores', 'spark.executor.instances', 'spark.executor.memory', 'spark.sql.shuffle.partitions']

  get emptyText () {
    return this.filter.model_name ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get modelTableTitle () {
    return this.$t('kylinLang.model.modelNameGrid')
  }
  get settingOption () {
    return [
      'Auto-merge',
      'Volatile Range',
      'Retention Threshold',
      'spark.executor.cores',
      'spark.executor.instances',
      'spark.executor.memory',
      'spark.sql.shuffle.partitions',
      'is-base-cuboid-always-valid',
      'customSettings'
    ]
  }
  get availableRetentionRange () {
    // let largestRange = null
    // const modelAutoMergeRanges = this.activeRow && this.activeRow.auto_merge_time_ranges || []
    // const autoMergeRanges = this.isEdit ? this.modelSettingForm.autoMerge : modelAutoMergeRanges
    // this.units.forEach(unit => {
    //   if (autoMergeRanges.includes(unit.value)) {
    //     largestRange = unit.value
    //   }
    // })
    // return largestRange || ''
    return 'DAY'
  }
  validateSettingItem (rule, value, callback) {
    const autoMergeRanges = this.activeRow && this.activeRow.auto_merge_time_ranges || []
    if (this.step === 'stepOne' && value === 'Retention Threshold' && !autoMergeRanges.length) {
      callback(new Error(null))
    } else {
      callback()
    }
  }
  get optionDesc () {
    return {
      'Auto-merge': this.$t('autoMergeTip'),
      'Volatile Range': this.$t('volatileTip'),
      'Retention Threshold': this.$t('retentionThresholdDesc'),
      'kylin.engine.spark-conf.spark.executor.cores': this.$t('sparkCores'),
      'kylin.engine.spark-conf.spark.executor.instances': this.$t('sparkInstances'),
      'kylin.engine.spark-conf.spark.executor.memory': this.$t('sparkMemory'),
      'kylin.engine.spark-conf.spark.sql.shuffle.partitions': this.$t('sparkShuffle'),
      'is-base-cuboid-always-valid': this.$t('baseCuboidConfig'),
      'customSettings': this.$t('customOptions')
    }
  }
  get rules () {
    return {
      // settingItem: [{ validator: this.validateSettingItem, message: this.$t('pleaseSetAutoMerge') }]
    }
  }
  handleClosed () {
    this.isEdit = false
    this.activeRow = null
    this.$refs['form'].clearValidate()
    this.modelSettingForm = JSON.parse(initialSettingForm)
  }
  addSettingItem (row) {
    // if (row.auto_merge_time_ranges && row.volatile_range && row.retention_range && Object.keys(row.override_props).length === 4) {
    //   return
    // }
    this.modelSettingForm.name = row.alias
    this.activeRow = objectClone(row)
    this.step = 'stepOne'
    this.editModelSetting = true
  }
  get modelSettingTitle () {
    return this.isEdit ? this.$t('editSetting') : this.$t('newSetting')
  }
  get isSubmit () {
    if (this.modelSettingForm.settingItem === 'Auto-merge' && !this.modelSettingForm.autoMerge.length) {
      return true
    } else if (this.modelSettingForm.settingItem === 'Volatile Range' && !(this.modelSettingForm.volatileRange.volatile_range_number >= 0 && this.modelSettingForm.volatileRange.volatile_range_number !== '' && this.modelSettingForm.volatileRange.volatile_range_type)) {
      return true
    } else if (this.modelSettingForm.settingItem === 'Retention Threshold' && !(this.modelSettingForm.retentionThreshold.retention_range_number >= 0 && this.modelSettingForm.retentionThreshold.retention_range_number !== '' && this.modelSettingForm.retentionThreshold.retention_range_type)) {
      return true
    } else if (this.modelSettingForm.settingItem.indexOf('spark.') !== -1 && !this.modelSettingForm[this.modelSettingForm.settingItem]) {
      return true
    } else if (this.modelSettingForm.settingItem === 'is-base-cuboid-always-valid' && this.modelSettingForm[this.modelSettingForm.settingItem] === '') {
      return true
    } else if (this.modelSettingForm.settingItem === 'customSettings' && (!ArrayFlat(this.modelSettingForm[this.modelSettingForm.settingItem]).length || this.modelSettingForm[this.modelSettingForm.settingItem].some(it => it.length === 1))) {
      return true
    } else {
      return false
    }
  }
  removeAutoMerge (row, type) {
    kylinConfirm(this.$t('isDel_' + type), {
      type: 'warning',
      confirmButtonText: this.$t('kylinLang.common.delete')
    }).then(() => {
      const rowCopy = objectClone(row)
      rowCopy[type] = null
      rowCopy['auto_merge_enabled'] = type !== 'auto_merge_time_ranges' ? rowCopy['auto_merge_enabled'] : null
      rowCopy['retention_range'] = type !== 'auto_merge_time_ranges' ? rowCopy['retention_range'] : null
      if (type.indexOf('spark.') !== -1 || type.indexOf('cube.') !== -1) {
        delete rowCopy.override_props[type]
      }
      this.updateModelConfig(Object.assign({}, {project: this.currentSelectedProject}, rowCopy)).then((res) => {
        handleSuccess(res, () => {
          this.getConfigList()
        })
      }, (res) => {
        handleError(res)
      })
    })
  }
  // 移除自定义配置项
  removeCustomSettingItem (row, type) {
    kylinConfirm(this.$t('delCustomConfigTip', {name: type}), {
      type: 'warning',
      confirmButtonText: this.$t('kylinLang.common.delete')
    }).then(() => {
      const rowCopy = objectClone(row)
      if (type in rowCopy.override_props) {
        delete rowCopy.override_props[type]
      }
      this.updateModelConfig(Object.assign({}, {project: this.currentSelectedProject}, rowCopy)).then((res) => {
        handleSuccess(res, () => {
          this.getConfigList()
        })
      }, (res) => {
        handleError(res)
      })
    })
  }
  async nextStep () {
    try {
      await this.$refs['form'].validate()
      this.step = 'stepTwo'
      if (this.modelSettingForm.settingItem === 'Retention Threshold') {
        this.modelSettingForm.retentionThreshold.retention_range_type = this.availableRetentionRange
      }
    } catch (e) {
      handleError(e)
    }
  }
  preStep () {
    this.step = 'stepOne'
  }
  editMergeItem (row) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = 'Auto-merge'
    this.modelSettingForm.autoMerge = JSON.parse(JSON.stringify(row.auto_merge_time_ranges))
    this.modelSettingForm.retentionThreshold = JSON.parse(JSON.stringify(row.retention_range))
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  editVolatileItem (row) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = 'Volatile Range'
    this.modelSettingForm.volatileRange = JSON.parse(JSON.stringify(row.volatile_range))
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  editRetentionItem (row) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = 'Retention Threshold'
    this.modelSettingForm.autoMerge = JSON.parse(JSON.stringify(row.auto_merge_time_ranges))
    this.modelSettingForm.retentionThreshold = JSON.parse(JSON.stringify(row.retention_range))
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  editSparkItem (row, sparkItemKey) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = sparkItemKey.substring(24)
    if (this.modelSettingForm.settingItem === 'spark.executor.memory') {
      this.modelSettingForm[this.modelSettingForm.settingItem] = row.override_props[sparkItemKey].replace(/[a-zA-Z]+$/, '')
    } else {
      this.modelSettingForm[this.modelSettingForm.settingItem] = row.override_props[sparkItemKey]
    }
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  // 编辑base-cubiod相关配置
  editCubeItem (row, key) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = key.split('.').slice(-1).toString()
    this.modelSettingForm[this.modelSettingForm.settingItem] = row.override_props[key]
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  // 编辑自定义设置
  editCustomSettingItem (row, key) {
    let customList = []
    customList.push([key, row.override_props[key]])
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = 'customSettings'
    this.modelSettingForm[this.modelSettingForm.settingItem] = customList
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  submit () {
    if (this.modelSettingForm.settingItem === 'Auto-merge') {
      this.activeRow.auto_merge_time_ranges = this.modelSettingForm.autoMerge
      this.activeRow.auto_merge_enabled = true
      if (this.activeRow.retention_range) {
        this.modelSettingForm.retentionThreshold.retention_range_type = this.availableRetentionRange
        this.activeRow.retention_range = this.modelSettingForm.retentionThreshold
      }
    }
    if (this.modelSettingForm.settingItem === 'Volatile Range') {
      this.activeRow.volatile_range = this.modelSettingForm.volatileRange
    }
    if (this.modelSettingForm.settingItem === 'Retention Threshold') {
      this.activeRow.retention_range = this.modelSettingForm.retentionThreshold
    }
    if (this.modelSettingForm.settingItem.indexOf('spark.') !== -1) {
      this.activeRow.override_props['kylin.engine.spark-conf.' + this.modelSettingForm.settingItem] = this.modelSettingForm[this.modelSettingForm.settingItem]
    }
    if (this.modelSettingForm.settingItem === 'spark.executor.memory') {
      this.activeRow.override_props['kylin.engine.spark-conf.spark.executor.memory'] = this.activeRow.override_props['kylin.engine.spark-conf.spark.executor.memory'] + 'g'
    }
    if (this.modelSettingForm.settingItem === 'is-base-cuboid-always-valid') {
      this.activeRow.override_props['kylin.cube.aggrgroup.is-base-cuboid-always-valid'] = !this.modelSettingForm['is-base-cuboid-always-valid']
    }
    if (this.modelSettingForm.settingItem === 'customSettings') {
      let customData = {}
      this.modelSettingForm.customSettings.forEach(item => {
        customData[item[0]] = item[1]
      })
      this.activeRow.override_props = {...this.activeRow.override_props, ...customData}
    }
    this.isLoading = true
    this.updateModelConfig(Object.assign({}, {project: this.currentSelectedProject}, this.activeRow)).then((res) => {
      handleSuccess(res, () => {
        this.isLoading = false
        this.editModelSetting = false
        this.getConfigList()
      })
    }, (res) => {
      handleError(res)
      this.isLoading = false
    })
  }
  async getConfigList () {
    try {
      const res = await this.loadModelConfigList(Object.assign({}, {project: this.currentSelectedProject}, this.filter))
      const resData = await handleSuccessAsync(res)
      this.modelList = resData.value
      this.modelListSize = resData.total_size
    } catch (e) {
      handleError(e)
    }
  }
  currentChange (size, count) {
    this.filter.page_offset = size
    this.filter.page_size = count
    this.getConfigList()
  }
  searchModels () {
    this.filter.page_offset = 0
    this.getConfigList()
  }
  // 增加自定义配置项
  addCustomSetting () {
    this.modelSettingForm[this.modelSettingForm.settingItem].push([])
  }
  // 删除某个自定义配置项
  removeCustomSetting (index) {
    this.modelSettingForm[this.modelSettingForm.settingItem].splice(index, 1)
  }
  created () {
    this.getConfigList()
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.setting-model {
  .model-setting-item {
    background-color: @grey-4;
    line-height: 18px;
    > span {
      margin-left: 5px;
    }
  }
  .model-setting-table {
    .el-ksd-icon-table_add_old.disabled {
      color: @text-disabled-color;
      cursor: not-allowed;
    }
    .el-ksd-icon-table_add_old,
    .el-icon-ksd-table_edit,
    .el-icon-ksd-symbol_type {
      &:hover {
        color: @base-color;
      }
    }
  }
}
.model-setting-dialog {
  .el-dialog__body {
    max-height: 500px;
    overflow: auto;
  }
  .el-form-item__content p {
    font-size: 12px;
    line-height: 16px;
    color: @text-normal-color;
    margin-top: 5px;
  }
  .merge-groups {
    > div:nth-child(2) {
      margin-top: -15px;
    }
    .el-checkbox {
      line-height: 1;
      width: 60px;
      &+.el-checkbox {
        margin-left: 30px;
      }
      &:nth-child(4) {
        margin-left: 0;
      }
    }
  }
  .retention-input {
    display: inline-block;
    width: 100px;
  }
  .custom-settings {
    .custom-param-key {
      width: 250px;
      &.is-editting {
        width: 300px;
      }
    }
    .custom-param-value {
      width: 124px;
      &.is-editting {
        width: 135px;
      }
    }
  }
  .el-icon-ksd-alert {
    font-size: 14px;
    color: @warning-color-1;
    margin-right: 5px;
  }
}
</style>
