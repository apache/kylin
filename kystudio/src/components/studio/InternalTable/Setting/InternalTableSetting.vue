<template>
  <el-dialog
    :visible="open"
    :title="$t('dialogTitle')"
    :class="'internal-table-setting' + (selectTableAttributeKeys ? ' with-selector' : '')"
    :before-close="this.handleClose">
    <el-form :model="ruleForm" :rules="rules" ref="ruleForm">
      <div v-loading="loading">
        <div>{{$t('settingDesc')}}</div>

        <el-row :gutter="20" class="split-block">
          <el-col :span="16">
            <div class="sub-title">{{$t('partitionOptionsTitle')}}</div>
            <div>
              <el-select v-model="ruleForm.partitionColumn" :placeholder="$t('partitionDefaultOptionLabel')" clearable class='max-width'>
                <el-option
                  v-for="item in partitionOptions"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
                </el-option>
              </el-select>
            </div>
          </el-col>
          <el-col :span="8" v-show="ruleForm.partitionColumn">
            <div class="sub-title">{{$t('timePartitionFormatTitle')}}</div>
            <div>
              <el-form-item prop="timePartitionFormat">
                <el-select v-model="ruleForm.timePartitionFormat" clearable class='max-width'>
                  <el-option
                    v-for="item in timePartitionFormatOptions"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value">
                  </el-option>
                </el-select>
              </el-form-item>
            </div>
          </el-col>
        </el-row>

        <el-row :gutter="20" class="split-block">
          <el-col :span="16">
            <div class="sub-title">{{$t('bucketColumnTitle')}}</div>
            <div>
              <el-select v-model="ruleForm.bucketColumn" :placeholder="$t('bucketDefaultOptionLabel')" clearable class='max-width'>
                <el-option
                  v-for="item in bucketOptions"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
                </el-option>
              </el-select>
            </div>
          </el-col>
          <el-col :span="8" v-show="ruleForm.bucketColumn">
            <div class="sub-title">{{$t('bucketCountTitle')}}</div>
            <div>
              <el-form-item prop="bucketCount">
                <el-input v-model.number="ruleForm.bucketCount"></el-input>
              </el-form-item>
            </div>
          </el-col>
        </el-row>

        <div class="split-block">
          <span>{{$t('addNativeTableAttributeTitle')}}</span><el-tooltip effect="dark" :content="$t('attributeSettingTips')" placement="top" popper-class="internal-table-tips">
            <i class="el-ksd-icon-more_info_16 icon ksd-fs-16 query-inernal-table-tips"></i>
          </el-tooltip>
        </div>
        <el-table :data="ruleForm.selectedTableAttributeKeys" :show-empty-img="false">
          <el-table-column
            :label="$t('attributeSettingPrimaryTitle')"
            width="180">
            <template slot-scope="scope">
              {{scope.row.primaryKey ? scope.row.name: ''}}
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('attributeSettingSortByTitle')"
            width="180">
            <template slot-scope="scope">
              {{scope.row.sortByKey ? scope.row.name: ''}}
            </template>
          </el-table-column>
          <el-table-column
            :renderHeader="renderKeysHeader"
            width="52">
          </el-table-column>
        </el-table>

        <div class="split-block dialog-footer">
          <el-button size="small" @click="this.handleClose">{{$t('kylinLang.common.cancel')}}</el-button><el-button
          type="primary" size="small" @click="this.handleSave">{{isCreate ? $t('kylinLang.common.create') : $t('kylinLang.common.save')}}</el-button>
        </div>
      </div>
      <div class="table-attribute-selector" v-show="selectTableAttributeKeys" v-loading="loading">
        <el-table-draggable-wrapper handle=".draggable-td" @drop="_afterTableAttributesChanged">
          <el-table
            :data="tableAttributes"
            style="padding: 0 8px">
            <el-table-column width="45">
              <template slot-scope="scope">
                <i class="el-ksd-n-icon-grab-dots-outlined draggable-td" :key="scope.$index"></i>
              </template>
            </el-table-column>
            <el-table-column
              prop="name"
              :label="$t('attributeSettingColumnTitle')"
              width="180">
            </el-table-column>
            <el-table-column
              :label="$t('attributeSettingPrimaryTitle')"
              width="110">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.primaryKey" @change="v => primaryKeyChanged(v, scope)"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column
              :label="$t('attributeSettingSortByTitle')"
              width="110">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.sortByKey" :checked="scope.row.primaryKey" :disabled="scope.row.primaryKey"  @change="sortByKeyChanged"></el-checkbox>
              </template>
            </el-table-column>
          </el-table>
        </el-table-draggable-wrapper>
      </div>
    </el-form>
  </el-dialog>
</template>
<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
import * as types from 'src/store/types'
import { handleSuccessAsync, handleError } from 'src/util'
import locales from './locales'
import { timePartitionFormatOptions } from '../const'

@Component({
  locales,
  props: {
    open: {
      type: Boolean,
      default: false
    },
    isCreate: {
      type: Boolean,
      default: false
    },
    tableInfo: {
      type: Object
    },
    handleClose: {
      type: Function,
      default: () => {}
    }
  },
  computed: {
    partitionOptions () {
      return (this.tableInfo.columns ? this.tableInfo.columns.map(c => ({ label: c.name, value: c.name })) : [])
    },
    bucketOptions () {
      return (this.tableInfo.columns ? this.tableInfo.columns.filter(c => c.name !== this.ruleForm.partitionColumn).map(c => ({ label: c.name, value: c.name })) : [])
    },
    doesPartionColumnNeedDateFormatter () {
      // const column = this.tableInfo.columns && this.tableInfo.columns.find(c => c.name === this.ruleForm.partitionColumn)
      // return column && column.datatype === 'date'
      return true
    },
    rules () {
      return {
        // timePartitionFormat: { required: this.doesPartionColumnNeedDateFormatter, message: this.$t('validateErrorSelect'), trigger: 'change' },
        timePartitionFormat: { required: false, message: this.$t('validateErrorSelect'), trigger: 'change' },
        bucketCount: [
          { required: !!this.bucketColumn, trigger: 'blur' },
          { type: 'number', message: this.$t('validateErrorNumber'), min: 1, max: 5000, trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    ...mapActions({
      updateInternalTable: types.UPDATE_INTERNAL_TABLE,
      createInternalTable: types.CREATE_INTERNAL_TABLE
    })
  }
})
export default class InternalTableSetting extends Vue {
  ruleForm = {
    partitionColumn: '',
    timePartitionFormat: '',
    bucketColumn: '',
    bucketCount: 1,
    selectedTableAttributeKeys: []
  }

  selectTableAttributeKeys = false
  loading = false

  timePartitionFormatOptions = timePartitionFormatOptions

  tableAttributes = this.getTableAttributesFromTableInfo()

  getTableAttributesFromTableInfo () {
    if (this.tableInfo) {
      if (this.tableInfo.table_attributes) return this.tableInfo.table_attributes
      if (this.tableInfo.columns) return this.tableInfo.columns.map(c => ({ name: c.name, primaryKey: false, sortByKey: false }))
    }
    return []
  }

  @Watch('tableInfo')
  onTableInfoChanged () {
    if (this.tableInfo) {
      this.tableAttributes = this.getTableAttributesFromTableInfo()
      this.ruleForm.partitionColumn = this.tableInfo.time_partition_col || ''
      this.ruleForm.timePartitionFormat = this.tableInfo.date_partition_format || ''
      this.ruleForm.bucketColumn = this.tableInfo.bucket_column || ''
      this.ruleForm.bucketCount = this.tableInfo.bucket_num || 1
      this._sortTableAttributes()
      this.selectTableAttributeKeys = false
    }
  }

  @Watch('ruleForm.partitionColumn')
  onPartitionColumnChanged (newVal) {
    if (!this.doesPartionColumnNeedDateFormatter) this.ruleForm.timePartitionFormat = ''
    if (this.ruleForm.bucketColumn === newVal) {
      this.ruleForm.bucketColumn = ''
      this.ruleForm.bucketCount = 1
    }
    this.tableAttributes = this.getTableAttributesFromTableInfo().filter(a => a.name !== newVal)
    this._sortTableAttributes()
  }

  primaryKeyChanged (checked, scope) {
    scope.row.sortByKey = checked
    this._sortTableAttributes()
  }

  sortByKeyChanged () {
    this._sortTableAttributes()
  }

  _sortTableAttributes () {
    this.tableAttributes.sort((a, b) => {
      const firstCompare = b.primaryKey - a.primaryKey
      if (firstCompare === 0) {
        return b.sortByKey - a.sortByKey
      }
      return firstCompare
    })
    this._afterTableAttributesChanged()
  }

  _afterTableAttributesChanged () {
    this.ruleForm.selectedTableAttributeKeys = this.tableAttributes.filter(a => a.primaryKey || a.sortByKey)
  }

  renderKeysHeader (h, { column }) {
    return h('el-button', {
      props: {
        icon: this.selectTableAttributeKeys ? 'el-ksd-n-icon-arrow-left-duo-outlined' : 'el-ksd-n-icon-arrow-right-duo-outlined'
      },
      on: {
        click: () => {
          this.selectTableAttributeKeys = !this.selectTableAttributeKeys
        }
      }
    })
  }

  handleSave () {
    !this.loading && this.$refs.ruleForm.validate((valid) => {
      if (valid) {
        this.$confirm(this.isCreate ? this.$t('confirmCreatePrompt', {}) : this.$t('confirmPrompt', {}), this.$t('confirmTitle'), {
          confirmButtonText: this.$t('kylinLang.common.submit'),
          cancelButtonText: this.$t('kylinLang.common.cancel'),
          centerButton: true,
          type: 'warning'
        }).then(async () => {
          this.loading = true
          this._createOrUpdateSetting({
            database: this.tableInfo.database,
            table: this.tableInfo.name,
            project: this.tableInfo.project,

            partition_cols: this.ruleForm.partitionColumn ? [this.ruleForm.partitionColumn] : [],
            date_partition_format: this.doesPartionColumnNeedDateFormatter ? this.ruleForm.timePartitionFormat : null,
            tbl_properties: {
              primaryKey: this.ruleForm.selectedTableAttributeKeys.filter(k => k.primaryKey).map(k => k.name).join(',') || null,
              orderByKey: this.ruleForm.selectedTableAttributeKeys.filter(k => k.sortByKey).map(k => k.name).join(',') || null,
              bucketCol: this.ruleForm.bucketColumn || null,
              bucketNum: this.ruleForm.bucketCount
            }
          }).then(handleSuccessAsync).then(
            () => this.$message({ type: 'success', message: this.isCreate ? this.$t('createSuccess') : this.$t('saveSuccess') })
          ).then(
            () => this.handleClose()
          ).finally(() => {
            this.loading = false
          }).catch(handleError)
        })
      }
    })
  }

  _createOrUpdateSetting (params) {
    if (this.isCreate) {
      return this.createInternalTable(params)
    }
    return this.updateInternalTable(params)
  }
}
</script>
<style lang="less">
  @dialog-width: 476px;

  .internal-table-setting {
    overflow: visible;

    .el-dialog {
      width: @dialog-width;
    }

    .with-selector {
      padding-right: @dialog-width;
    }
    .el-dialog__body {
      scrollbar-gutter: stable;
    }
    .internal-table-container {
      margin: 32px 24px 24px;
    }
    .split-block {
      margin-top: 24px;
    }
    .sub-title {
      margin-bottom: 8px;
    }
    .table-attribute-selector {
      position: absolute;
      bottom: 0;
      right: -@dialog-width - 12px;
      width: @dialog-width;
      box-sizing: border-box;
      background-color: #fff;
      padding: 6px;
      border-radius: 12px;
      .el-table-draggable-wrapper {
        max-height: 560px;
        overflow: overlay;
      }
    }
    .dialog-footer {
      margin-bottom: 24px;
      text-align: right;
    }
    .query-inernal-table-tips {
      position: relative;
      top: -1px;
      left: 2px;
    }
    .el-table__empty-block {
      height: 0px;
    }
    .el-table {
      min-height: 148px;
    }
    .max-width {
      width: 100%;
    }
    .internal-table-tips {
      :first-child {
        max-height: 100% !important;
      }
    }
  }
</style>
