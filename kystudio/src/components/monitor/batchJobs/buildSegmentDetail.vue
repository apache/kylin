<template>
  <div>
    <el-collapse>
      <el-collapse-item :name="item.name" v-for="item in segmentTesks" :key="item.name">
        <div class="segment-title" slot="title">
          <!-- <i :class="['segment-build-status', getStatusIcons(item)]"></i> -->
          <i class="segment-error-stop el-ksd-icon-wrong_fill_16" v-if="segmentStatus(item) === 'ERROR_STOP'"></i>
          <el-icon v-else :class="{'is-running': segmentStatus(item) === 'RUNNING'}" :name="jobStatus === 'STOPPED' ? 'el-ksd-icon-stopped_16' : getStatusIcons(item)" type="mult"></el-icon>
          <span>{{getSegmentTimeRange(item)}}</span>
          <el-tooltip placement="top" :content="getRunningStep(item)">
            <span class="running-step">{{getRunningStep(item)}}</span>
          </el-tooltip>
          <span>{{getProgress(item).toFixed(1) + '%'}}</span>
        </div>
        <el-progress
          class="ksd-mb-8"
          v-if="item.step_ratio"
          :percentage="getProgress(item)"
          :status="item.step_ratio === 1 ? 'success' : ''"
          :color="segmentStatus(item) === 'ERROR' ? '#CA1616' : segmentStatus(item) === 'ERROR_STOP' ? '#A5B2C5' : ''"
          :show-text="false"
        ></el-progress>
        <div class="segment-wait-time">
          <span class="jobActivityLabel">{{$t('waiting')}}: </span>
          <span v-if="item.wait_time">{{formatTime(item.wait_time)}}</span>
          <span v-else>{{segmentStatus(item) === 'PENDING' ? '-' : 0}}</span>
        </div>
        <div class="segment-duration-time">
          <span class="jobActivityLabel">{{$t('duration')}}: </span>
          <span v-if="item.exec_start_time && item.exec_end_time">{{formatTime(item.exec_end_time - item.exec_start_time)}}</span>
          <span v-else>{{segmentStatus(item) === 'PENDING' ? '-' : 0}}</span>
          <el-tooltip placement="bottom" triggle="hover">
            <div slot="content">
              <div><span>{{$t('durationStart')}}</span><span>{{item.exec_start_time !== 0 ? transToGmtTime(item.exec_start_time) : '-'}}</span></div>
              <div><span>{{$t('durationEnd')}}</span><span>{{item.exec_end_time !== 0 ? transToGmtTime(item.exec_end_time) : '-'}}</span></div>
            </div>
            <span class="duration-details" v-show="item.step_status !== 'PENDING'">{{$t('durationDetails')}}</span>
          </el-tooltip>
        </div>
        <div>
          <span class="active-nodes jobActivityLabel">{{$t('jobNodes')}}: </span>
          <span v-if="item.info">{{item.info.node_info || $t('unknow')}}</span>
          <br />
        </div>
        <ul class="sub-tasks" v-for="sub in item.stage" :key="sub.id">
          <li>
            <!-- 当 job 主步骤为暂停状态时，所有的未完成的子步骤都变更为 STOP 状态 -->
            <el-tooltip placement="bottom" :content="getStepStatusTips(jobStatus === 'STOPPED' && sub.step_status !== 'SKIP' ? 'STOPPED' : sub.step_status)">
              <span :class="[jobStatus === 'STOPPED' && sub.step_status !== 'FINISHED' && sub.step_status !== 'SKIP' ? 'sub-tasks-status is-stop' : getSubTaskStatus(sub)]"></span>
            </el-tooltip>
            <span class="sub-tasks-name">{{getTaskName(sub.name)}}</span>
            <span class="sub-tasks-layouts" v-if="sub.name === 'Build indexes by layer'"><span class="success-layout-count">{{sub.success_index_count}}</span>{{`/${sub.index_count}`}}</span>
          </li>
          <li><span class="list-details" v-if="sub.duration">{{$t('duration')}}: {{formatTime(sub.duration)}}</span><span class="list-details" v-else>{{$t('duration')}}: -</span></li>
        </ul>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import { getSubTasksName, getSubTaskStatus, formatTime, getStepStatusTips } from './handler'
import { transToGmtTime } from '../../../util/business'

@Component({
  props: {
    segmentTesks: {
      type: Object,
      default () {
        return {}
      }
    },
    jobStatus: {
      type: String,
      default: ''
    }
  },
  locales
})
export default class BuildSegmentDetail extends Vue {
  transToGmtTime = transToGmtTime
  getSubTasksName = getSubTasksName
  getSubTaskStatus = getSubTaskStatus
  formatTime = formatTime
  getStepStatusTips = (status) => getStepStatusTips(this, status)

  // 获取 segment 构建状态 icon
  getStatusIcons (step) {
    let status = this.segmentStatus(step)
    let iconMap = {
      RUNNING: 'el-ksd-icon-running_16',
      STOPPED: 'el-ksd-icon-stopped_16',
      ERROR: 'el-ksd-icon-error_16',
      FINISHED: 'el-ksd-icon-finished_16',
      PENDING: 'el-ksd-icon-pending_16',
      DISCARDED: 'el-ksd-icon-discarded_16'
    }
    return iconMap[status]
  }

  // segment 运行状态
  segmentStatus (step) {
    let status = 'PENDING'
    const { stage } = step

    stage.filter(item => item.step_status === 'RUNNING').length > 0 && (status = 'RUNNING')
    stage.filter(item => item.step_status === 'STOPPED').length > 0 && (status = 'STOPPED')
    stage.filter(item => item.step_status === 'ERROR').length > 0 && (status = 'ERROR')
    stage.filter(item => item.step_status === 'FINISHED').length === stage.filter(item => item.step_status !== 'SKIP').length && (status = 'FINISHED')
    stage.filter(item => item.step_status === 'PENDING').length === stage.filter(item => item.step_status !== 'SKIP').length && (status = 'PENDING')
    stage.filter(item => item.step_status === 'ERROR_STOP').length === stage.filter(item => item.step_status !== 'SKIP').length && (status = 'ERROR_STOP')
    stage.filter(item => item.step_status === 'DISCARDED').length === stage.filter(item => item.step_status !== 'SKIP').length && (status = 'DISCARDED')

    return status
  }

  getProgress (item) {
    return item.step_ratio > 1 ? 100 : (typeof item.step_ratio === 'number' && item.step_ratio >= 0 ? item.step_ratio * 100 : 0)
  }

  getTaskName (name) {
    return this.getSubTasksName(this, name)
  }

  getSegmentTimeRange (item) {
    const startTime = Vue.filter('toServerGMTDate')(item.start_time)
    const endTime = Vue.filter('toServerGMTDate')(item.end_time)
    return `${startTime} - ${endTime}`
  }

  // 获取运行状态的 segment 步骤
  getRunningStep (item) {
    const step = item.stage.filter(it => it.step_status === 'RUNNING')
    return `${step.length > 0 ? this.getSubTasksName(this, step[0].name) : ''}`
  }

  closeDialog () {
    this.$emit('close')
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.el-dialog__body {
  max-height: 500px;
  overflow: auto;
  .el-collapse-item__header {
    .segment-title {
      .segment-error-stop {
        font-size: 16px;
        margin-top: -3px;
        color: @text-placeholder-color;
      }
      .mutiple-color-icon {
        font-size: 16px;
        vertical-align: middle;
        margin-top: -3px;
        &.is-running {
          animation: rotating 2s linear infinite;
        }
      }
    }
    .running-step {
      color: @text-disabled-color;
      display: inline-block;
      max-width: 75px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      vertical-align: top;
    }
  }
  .el-collapse-item__arrow {
    float: left;
  }
  .el-collapse-item__content {
    padding-left: 36px;
    padding-right: 16px;
  }
  .jobActivityLabel {
    color: @text-normal-color;
  }
  .sub-tasks {
    &-name {
      color: @text-normal-color;
    }
    &-status {
      width: 6px;
      height: 6px;
      display: inline-block;
      border-radius: 100%;
      margin-right: 8px;
      &.is-finished {
        background: @ke-color-success;
      }
      &.is-pending {
        background: @ke-color-info-secondary;
        position: relative;
        &::after {
          content: '';
          width: 3px;
          height: 3px;
          border-radius: 100%;
          background: #fff;
          position: absolute;
          top: 50%;
          left: 50%;
          transform: translate(-50%, -50%);
        }
      }
      &.is-error {
        background: @ke-color-danger;
      }
      &.is-error-stop {
        background: @text-placeholder-color;
      }
      &.is-stop {
        background: @ke-color-primary;
      }
    }
    &-layouts {
      color: @text-normal-color;
      .success-layout-count {
        color: @ke-color-primary;
      }
    }
    .running {
      margin-right: 8px;
      font-size: 8px;
      margin-left: -1px;
    }
    .list-details {
      padding-left: 15px;
      color: @text-disabled-color;
    }
    .icons {
      transform: scale(0.6);
      margin-left: -2px;
      margin-right: 3px;
      color: #A5B2C5;
    }
  }
  .duration-details {
    color: @ke-color-primary;
    cursor: pointer;
  }
}

</style>
