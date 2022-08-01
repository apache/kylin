<template>
  <div class="streaming-job">
    <div class="ksd-title-label-small streaming-header">
      <span>{{$t('streaming')}}</span>
      <div class="actions">
        <el-button @click="refreshJobInfo" type="primary" :loading="isRefreshJobInfoLoading" text>{{$t('refresh')}}</el-button>
        <el-button @click="stopStreaming" type="primary" :loading="isStopLoading" icon="el-icon-ksd-stop" :disabled="isDisabled" text v-if="statusOptions.streamingIngestion === 'RUNNING' || statusOptions.autoMerge === 'RUNNING'">{{$t('stop')}}</el-button><el-button @click="startStreaming" type="primary" :loading="isStartLoading" icon="el-icon-ksd-restart" text v-else :disabled="isDisabled">{{$t('restart')}}</el-button><el-dropdown trigger="click" class="ksd-ml-10" @command="handleCommand">
          <el-button type="primary" icon="el-icon-ksd-configurations" text>{{$t('setting')}}</el-button>
          <el-dropdown-menu slot="dropdown">
            <!-- <el-dropdown-item command="log">{{$t('logDetails')}}</el-dropdown-item> -->
            <el-dropdown-item command="configurations">{{$t('configurations')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
        <!-- <span class="item" @click="startStreaming"><i class="el-icon-ksd-table_resume ksd-mr-5"></i>{{$t('restart')}}</span>
        <span class="item" @click="stopStreaming"><i class="el-icon-ksd-login_intro ksd-mr-5"></i>{{$t('setting')}}</span> -->
      </div>
    </div>
    <el-row :gutter="15" class="ksd-mt-10">
      <el-col :span="5">
        <div class="job-info">
          <div class="job-sub-title ksd-mb-15">{{$t('status')}}</div>
          <p class="contain job-status" v-for="item in Object.keys(statusOptions)" :key="item">
            <span class="title">{{$t(item)}}：</span>
            <span class="status"><span :class="['flag', statusOptions[item].toLocaleLowerCase(), 'ksd-mr-5']"></span><span>{{$t(statusOptions[item])}}</span></span>
          </p>
        </div>
      </el-col>
      <el-col :span="8">
        <div class="job-info">
          <div class="job-sub-title ksd-mb-15">{{$t('metrics')}}</div>
          <p class="contain average-metrics" v-for="item in Object.keys(metricsOptions)" :key="item">
            <span class="title">{{$t(item)}}</span>
            <span>{{metricsOptions[item]}}</span>
          </p>
        </div>
      </el-col>
      <el-col :span="11">
        <div class="job-info">
          <div class="job-sub-title ksd-mb-15">{{$t('dataIngestion')}}</div>
          <p class="contain dataIngestion" v-for="item in Object.keys(dataIngestionOptions)" :key="item">
            <span class="title">{{$t(item)}}</span>
            <span>{{dataIngestionOptions[item]}}</span>
          </p>
        </div>
      </el-col>
    </el-row>
    <el-dialog
      :title="$t('configurations')"
      append-to-body
      class="configurations-dialog"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :visible.sync="showConfigurations"
      @close="handleClose"
      width="480px">
      <div>
        <el-tabs v-model="paramsTab" type="card" @tab-click="handleClickTab">
          <el-tab-pane :label="tab.label" :name="tab.name" v-for="tab in paramsConfigTabs" :key="tab.name">
            <el-row :gutter="10">
              <el-col :span="14"><span class="title">{{$t('key')}}</span></el-col>
              <el-col :span="6"><span class="title">{{$t('value')}}</span></el-col>
              <el-col :span="4"></el-col>
            </el-row>
            <el-row :gutter="10" class="ksd-mt-10" v-for="(item, index) in tab.config" :key="index">
              <el-col :span="14"><el-input v-model.trim="tab.config[index][0]" :disabled="tab.config[index] && !!tab.config[index][2]" :placeholder="$t('pleaseInputKey')" /></el-col>
              <el-col :span="6">
                <el-input v-number2="tab.config[index][1]" v-model.trim="tab.config[index][1]" :placeholder="$t('pleaseInputValue')" :class="{'is-empty': !tab.config[index][1]}" v-if="numberParams.indexOf(item[0]) !== -1"></el-input>
                <el-input v-else v-model.trim="tab.config[index][1]" :class="{'is-empty': !tab.config[index][1]}" :placeholder="$t('pleaseInputValue')" />
              </el-col>
              <!-- <el-col :span="4">
                <span class="action-btns ksd-ml-5">
                  <el-button type="primary" icon="el-icon-ksd-add_2" plain circle size="mini" @click="addParamsConfigs(tab.type)"></el-button>
                  <el-button icon="el-icon-minus" class="ksd-ml-5" plain circle size="mini" :disabled="tab.config.length === 1 && index === 0 || (tab.config[index] && !!tab.config[index][2])" @click="removeParamsConfigs(tab.type, index)"></el-button>
                </span>
              </el-col> -->
            </el-row>
          </el-tab-pane>
        </el-tabs>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="handleClose">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="loadingSetting" @click="saveSettings">{{$t('kylinLang.common.save')}}</el-button>
    </div>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { handleSuccessAsync, handleError } from 'util/index'
import { mapActions, mapGetters } from 'vuex'
import locales from './locales'
import { kylinConfirm, objectClone, transToGmtTime } from '../../../../../util'

@Component({
  name: 'ModelStreamingJob',
  props: ['model'],
  locales,
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      getStreamingJob: 'GET_STREAMING_JOB',
      changeStreamingJobStatus: 'CHANGE_STREAMING_JOB_STATUS',
      updateStreamingConfig: 'UPDATE_STREAMING_CONFIGURATIONS'
    })
  }
})
export default class ModelStreamingJob extends Vue {
  statusOptions = {
    streamingIngestion: '',
    autoMerge: ''
  }
  metricsOptions = {
    avg_consume_rate_in_5mins: '-',
    avg_consume_rate_in_15mins: '-',
    avg_consume_rate_in_30mins: '-',
    avg_consume_rate_in_All: '-'
  }
  dataIngestionOptions = {
    last_build_time: '-',
    last_update_time: '-',
    latency: '-'
  }
  loadingSetting = false
  showConfigurations = false
  isStartLoading = false
  isStopLoading = false
  isRefreshJobInfoLoading = false
  paramsTab = 'buildParams'
  buildParamsConfigs = [[]]
  buildParamsConfigsCache = [[]]
  mergeParamsConfigs = [[]]
  mergeParamsConfigsCache = [[]]
  commonParamsConfigs = [[]]
  buildDefaultParams = ['spark.master', 'spark.driver.memory', 'spark.executor.instances', 'spark.executor.cores', 'spark.executor.memory', 'spark.sql.shuffle.partitions', 'kylin.streaming.duration', 'kylin.streaming.job-retry-enabled', 'kylin.streaming.kafka-conf.maxOffsetsPerTrigger', 'kylin.streaming.watermark']
  mergeDefaultParams = ['spark.master', 'spark.driver.memory', 'spark.executor.instances', 'spark.executor.cores', 'spark.executor.memory', 'spark.sql.shuffle.partitions', 'kylin.streaming.segment-max-size', 'kylin.streaming.segment-merge-threshold', 'kylin.streaming.job-retry-enabled']
  numberParams = ['spark.executor.cores', 'kylin.streaming.duration', 'spark.executor.instances', 'kylin.streaming.segment-merge-threshold']
  get paramsConfigTabs () {
    return [
      {label: this.$t('streamingIngestion'), name: 'buildParams', config: this.buildParamsConfigs, type: 'build', actions: {add: this.addParamsConfigs, remove: this.removeParamsConfigs}},
      {label: this.$t('autoMerge'), name: 'mergeParams', config: this.mergeParamsConfigs, type: 'merge', actions: {add: this.addParamsConfigs, remove: this.removeParamsConfigs}}
      // {label: 'p3', name: 'commonParams', config: this.commonParamsConfigs, type: 'common', actions: {add: this.addParamsConfigs, remove: this.removeParamsConfigs}}
    ]
  }
  get isDisabled () {
    return this.statusOptions.streamingIngestion === 'STARTING' || this.statusOptions.streamingIngestion === 'STOPPING' || this.statusOptions.autoMerge === 'STARTING' || this.statusOptions.autoMerge === 'STOPPING'
  }
  refreshJobInfo () {
    this.initData()
  }
  initData () {
    this.getStreamingJob({project: this.currentSelectedProject, model_id: this.model}).then(async (res) => {
      const results = await handleSuccessAsync(res)
      const { build_job_meta, merge_job_meta, avg_consume_rate_in_5mins, avg_consume_rate_in_15mins, avg_consume_rate_in_30mins, avg_consume_rate_in_All, last_update_time, last_build_time, latency } = results
      this.metricsOptions = {
        avg_consume_rate_in_5mins: avg_consume_rate_in_5mins > 10000 ? (avg_consume_rate_in_5mins / 1000).toFixed(2) + ' kmsg/s' : avg_consume_rate_in_5mins.toFixed(2) + ' msg/s',
        avg_consume_rate_in_15mins: avg_consume_rate_in_15mins > 10000 ? (avg_consume_rate_in_15mins / 1000).toFixed(2) + ' kmsg/s' : avg_consume_rate_in_15mins.toFixed(2) + ' msg/s',
        avg_consume_rate_in_30mins: avg_consume_rate_in_30mins > 10000 ? (avg_consume_rate_in_30mins / 1000).toFixed(2) + ' kmsg/s' : avg_consume_rate_in_30mins.toFixed(2) + ' msg/s',
        avg_consume_rate_in_All: avg_consume_rate_in_All > 10000 ? (avg_consume_rate_in_All / 1000).toFixed(2) + ' kmsg/s' : avg_consume_rate_in_All.toFixed(2) + ' msg/s'
      }
      this.dataIngestionOptions = {
        last_build_time: last_build_time ? transToGmtTime(last_build_time) : '-',
        last_update_time: last_update_time ? transToGmtTime(last_update_time) : '-',
        latency: latency ? (latency / 60 / 1000).toFixed(2) + ' mins' : '-'
      }
      this.statusOptions.streamingIngestion = build_job_meta.job_status === 'NEW' ? 'STOPPED' : build_job_meta.job_status
      this.statusOptions.autoMerge = merge_job_meta.job_status === 'NEW' ? 'STOPPED' : merge_job_meta.job_status
      this.buildParamsConfigs = Object.entries(build_job_meta.params).map(it => ([...it, this.buildDefaultParams.indexOf(it[0]) !== -1]))
      this.buildParamsConfigsCache = objectClone(this.buildParamsConfigs)
      this.mergeParamsConfigs = Object.entries(merge_job_meta.params).map(it => ([...it, this.mergeDefaultParams.indexOf(it[0]) !== -1]))
      this.mergeParamsConfigsCache = objectClone(this.mergeParamsConfigs)
    }).catch((e) => {
      handleError(e)
    })
  }
  // 开启任务
  async startStreaming () {
    if (this.isStartLoading) return
    try {
      this.isStartLoading = true
      const res = await this.changeStreamingJobStatus({project: this.currentSelectedProject, model_id: this.model, action: 'START'})
      await handleSuccessAsync(res)
      this.isStartLoading = false
      this.initData()
    } catch (e) {
      this.isStartLoading = false
      handleError(e)
      this.initData()
    }
  }
  // 终止任务
  async stopStreaming () {
    if (this.isStopLoading) return
    await kylinConfirm(this.$t('cofirmStopStreaming'), {confirmButtonText: this.$t('stop')}, this.$t('stopStreamingTitle'))
    try {
      this.isStopLoading = true
      const res = await this.changeStreamingJobStatus({project: this.currentSelectedProject, model_id: this.model, action: 'STOP'})
      await handleSuccessAsync(res)
      this.isStopLoading = false
      this.initData()
    } catch (e) {
      this.isStopLoading = false
      handleError(e)
      this.initData()
    }
  }
  handleCommand (command) {
    if (command === 'configurations') {
      this.showConfigurations = true
      this.buildParamsConfigs = objectClone(this.buildParamsConfigsCache)
      this.mergeParamsConfigs = objectClone(this.mergeParamsConfigsCache)
    }
  }
  // 关闭配置弹窗
  handleClose () {
    this.showConfigurations = false
    this.initData()
  }
  // 更新配置
  saveSettings () {
    this.loadingSetting = true
    const buildParams = {}
    const mergeParams = {}
    this.buildParamsConfigs.forEach(item => {
      buildParams[item[0]] = item[1]
    })
    this.mergeParamsConfigs.forEach(item => {
      mergeParams[item[0]] = item[1]
    })
    this.updateStreamingConfig({project: this.currentSelectedProject, model_id: this.model, build_params: buildParams, merge_params: mergeParams}).then(() => {
      this.loadingSetting = false
      this.handleClose()
    }).catch((e) => {
      handleError(e)
      this.loadingSetting = false
    })
  }
  // 增加配置
  addParamsConfigs (type) {
    this[`${type}ParamsConfigs`].push([])
  }
  // 删减配置
  removeParamsConfigs (type, index) {
    this[`${type}ParamsConfigs`].splice(index, 1)
  }
  handleClickTab () {
  }
  created () {
    this.initData()
  }
}
</script>
<style lang="less">
  @import '../../../../../assets/styles/variables.less';
  .streaming-job {
    margin: 15px;
    padding: 15px;
    box-shadow: none;
    border: 1px solid @line-border-color4;
    background-color: @fff;
    .streaming-header {
      position: relative;
      .actions {
        position: absolute;
        top: 0;
        right: 0;
        .item {
          padding: 0 5px;
          cursor: pointer;
          &:hover {
            color: @color-primary;
          }
        }
      }
    }
    .job-info {
      padding: 15px;
      background-color: @regular-background-color;
      font-size: 12px;
      box-sizing: border-box;
      .job-sub-title {
        color: @color-text-secondary;
      }
      .contain {
        margin-bottom: 10px;
        &.average-metrics {
          display: inline-block;
          width: 50%;
          margin-bottom: 10px;
          &:nth-last-child(2) {
            margin-bottom: 0;
          }
        }
        &.dataIngestion {
          display: inline-block;
          margin-bottom: 10px;
          margin-right: 30px;
          &:nth-last-child(2) {
            margin-bottom: 0;
          }
        }
        .title {
          color: @color-text-primary;
          font-size: 12px;
          font-weight: bold;
        }
        &.job-status {
          .title {
            width: 120px;
            display: inline-block;
          }
        }
        .status {
          color: @color-text-primary;
          font-size: 12px;
          font-weight: bold;
          display: inline-flex;
          align-items: center;
          vertical-align: top;
        }
        &:last-child {
          margin-bottom: 0;
        }
        .flag {
          display: inline-block;
          width: 12px;
          height: 12px;
          border-radius: 100%;
          &.running {
            background: @color-success;
          }
          &.error {
            background: @error-color-1;
          }
          &.stopped {
            background: @color-text-disabled;
          }
          &.starting,
          &.stopping {
            background: @base-color;
          }
        }
      }
    }
  }
  .configurations-dialog {
    .action-btns {
      .el-button {
        // margin-top: 3px;
      }
    }
    .el-dialog__body {
      max-height: 350px;
      overflow: auto;
    }
  }
</style>
