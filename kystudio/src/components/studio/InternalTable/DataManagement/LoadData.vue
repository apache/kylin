<template>
  <div class="internal-table-load-data"><el-form :model="ruleForm" :rules="rules" ref="ruleForm">
    <el-tabs v-model="activeTab" type="card">
      <el-tab-pane name="append" :disabled="!tableInfo.date_partition_format">
        <span slot="label">{{$t('loadModelAppend')}}</span>
        <div>
          <el-row :gutter="20">
            <el-col :span="16">
              <div class="sub-title">{{$t('timePartitionOptionsTitle')}}</div>
              <div>
                <el-form-item prop="timePartitionColumn">
                  <el-select v-model="tableInfo.time_partition_col" class='max-width' disabled>
                    <el-option
                      v-for="item in timePartitionOptions"
                      :key="item.value"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                  </el-select>
                </el-form-item>
              </div>
            </el-col>
            <el-col :span="8">
              <div class="sub-title">{{$t('timePartitionFormatTitle')}}</div>
              <div>
                <el-form-item prop="timePartitionFormat">
                  <el-select v-model="tableInfo.date_partition_format" class='max-width' disabled>
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
          <div class="split-block">
            <el-form-item prop="dateRange">
              <el-date-picker
                :append-to-body="false"
                v-model="ruleForm.dateRange"
                type="datetimerange"
                :range-separator="$t('dateRangeSeparator')"
                :start-placeholder="$t('dateRangeStartPlaceholder')"
                :end-placeholder="$t('dateRangeEndPlaceholder')">
              </el-date-picker>
            </el-form-item>
          </div>
        </div>
      </el-tab-pane>
      <el-tab-pane name="full">
        <span slot="label">{{$t('loadModelFull')}}</span>
        <div class="full-load-partition">
          <div class="sub-title"><span>{{$t('partitionOptionsTitle')}}</span></div>
          <div>
            <el-select v-model="tableInfo.time_partition_col" class='max-width' disabled>
              <el-option
                v-for="item in partitionOptions"
                :key="item.value"
                :label="item.label"
                :value="item.value">
              </el-option>
            </el-select>
          </div>
        </div>
      </el-tab-pane>
    </el-tabs>

    <div class="split-block dialog-footer">
      <el-button size="small" @click="this.handleClose">{{$t('kylinLang.common.cancel')}}</el-button><el-button
      type="primary" size="small" @click="this.handleSave">{{$t('kylinLang.common.load')}}</el-button>
    </div>
  </el-form></div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import * as types from 'src/store/types'
import { handleSuccessAsync } from 'src/util'
import { timePartitionFormatOptions } from '../const'
import { transToUTCMs } from 'util/business'

@Component({
  props: {
    tableInfo: {
      type: Object
    },
    handleClose: {
      type: Function,
      default: () => {}
    }
  },

  methods: {
    ...mapActions({
      loadSingleInternalTable: types.LOAD_SINGLE_INTERNAL_TABLE
    })
  },

  computed: {
    timePartitionOptions () {
      return (this.tableInfo.columns ? this.tableInfo.columns.filter(c => c.datatype === 'date').map(c => ({ label: c.name, value: c.name })) : [])
    },
    partitionOptions () {
      return (this.tableInfo.columns ? this.tableInfo.columns.map(c => ({ label: c.name, value: c.name })) : [])
    },
    rules () {
      return {
        // timePartitionColumn: { required: this.activeTab === 'append', message: this.$t('validateErrorTimeColumnSelect'), trigger: 'change' },
        // timePartitionFormat: { required: this.activeTab === 'append', message: this.$t('validateErrorTimeFormatSelect'), trigger: 'change' },
        dateRange: { required: this.activeTab === 'append', message: this.$t('validateErrorDateRangeSelect'), trigger: 'change' }
      }
    }
  },

  locales: {
    en: {
      timePartitionOptionsTitle: 'Time Partition Column',
      timePartitionFormatTitle: 'Time Format',
      validateErrorTimeFormatSelect: 'Please select time format',
      validateErrorTimeColumnSelect: 'Please select time partition column',
      validateErrorDateRangeSelect: 'Please select date range',
      confirmPrompt: 'Are you sure to load data as {model}?',
      confirmTitle: 'Load Data',
      partitionOptionsTitle: 'Partition Column',
      dateRangeSeparator: 'to'
    },
    'zh-cn': {
      timePartitionOptionsTitle: '时间分区列',
      timePartitionFormatTitle: '时间格式',
      loadModelAppend: '增量加载',
      loadModelFull: '全量加载',
      validateErrorTimeFormatSelect: '请选择时间格式',
      validateErrorTimeColumnSelect: '请选择时间分区列',
      validateErrorDateRangeSelect: '请选择时间范围',
      confirmPrompt: '确定要{model}数据？',
      confirmTitle: '加载数据',
      partitionOptionsTitle: '分区列',
      dateRangeSeparator: '至',
      dateRangeStartPlaceholder: '开始日期',
      dateRangeEndPlaceholder: '结束日期'
    }
  }
})
export default class LoadData extends Vue {
  activeTab = this.tableInfo.date_partition_format ? 'append' : 'full'

  @Watch('tableInfo')
  tableInfoChanged (newVal, oldVal) {
    this.activeTab = this.tableInfo.date_partition_format ? 'append' : 'full'
  }

  ruleForm = {
    dateRange: (() => {
      const currentDate = new Date()
      return [
        new Date(currentDate.getFullYear(), currentDate.getMonth() - 1, currentDate.getDate() - 1, 10, 10),
        new Date(currentDate.getFullYear(), currentDate.getMonth(), currentDate.getDate() - 1, 10, 10)
      ]
    })()
  }

  loading = false

  timePartitionFormatOptions = timePartitionFormatOptions

  handleSave () {
    !this.loading && this.$refs.ruleForm.validate((valid) => {
      if (valid) {
        this.$confirm(
          this.$t('confirmPrompt', { model: this.activeTab === 'append' ? this.$t('loadModelAppend') : this.$t('loadModelFull') }),
          this.$t('confirmTitle'),
          {
            confirmButtonText: this.$t('kylinLang.common.submit'),
            cancelButtonText: this.$t('kylinLang.common.cancel'),
            centerButton: true,
            type: 'warning'
          }
        ).then(async () => {
          this.loading = true
          return await this.loadSingleInternalTable({
            database: this.tableInfo.database,
            table: this.tableInfo.name,
            project: this.tableInfo.project,
            incremental: this.activeTab === 'append',
            refresh: false,
            start_date: this.activeTab === 'append' ? transToUTCMs(this.ruleForm.dateRange[0].getTime()) : null,
            end_date: this.activeTab === 'append' ? transToUTCMs(this.ruleForm.dateRange[1].getTime()) : null,
            yarn_queue: 'default'
          }).then(handleSuccessAsync).catch(() => {
            this.$message.error(this.$t('saveFailed'))
          }).finally(() => {
            this.loading = false
            this.handleClose()
          })
        })
      }
    })
  }
}
</script>
<style lang="less">
  .internal-table-load-data {
    .el-tabs__content {
      overflow: visible;
    }
    .full-load-partition {
      display: flex;
      align-items: center;

      .sub-title {
        padding-right: 8px;
      }
    }
    .dialog-footer {
      text-align: right;
    }
  }
</style>
