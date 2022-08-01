<template>
  <el-dialog
    :visible="true"
    width="600px"
    status-icon="el-ksd-icon-error_24"
    @close="closeErrorDetail"
    :close-on-click-modal="false">
    <span slot="title">{{$t('errorDetail')}}</span>
    <div class="error-contain">
      <p class="error-title">{{getErrorReason}}</p>
      <el-button class="error-solution-btn ksd-mt-8" v-show="currentErrorJob.failed_resolve" @click="jumpToManual" nobg-text iconr="el-ksd-icon-spark_link_16">{{$t('resolveErrorBtn')}}</el-button>
      <div class="error-trace-msg ksd-mt-8">{{getErrorTrace}}</div>
      <el-button class="view-details-btn ksd-mt-8" v-if="showViewMore" @click="showMore = !showMore" nobg-text :iconr="showMore ? 'el-ksd-icon-arrow_up_16' : 'el-ksd-icon-arrow_down_16'">{{$t('viewMore')}}</el-button>
      <build-segment-detail v-if="showMore" :segmentTesks="currentErrorJob.segment_sub_stages" :jobStatus="currentErrorJob.step_status"/>
    </div>
    <span slot="footer" class="dialog-footer">
      <!-- <el-button type="primary" size="medium" @click="closeErrorDetail">{{$t('kylinLang.common.IKnow')}}</el-button> -->
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import buildSegmentDetail from './buildSegmentDetail.vue'
import locales from './locales'
import { getSubTasksName, getStepLineName } from './handler'

@Component({
  props: {
    currentErrorJob: {
      type: Object,
      default () {
        return {}
      }
    }
  },
  components: {
    buildSegmentDetail
  },
  locales
})
export default class jobErrorDetail extends Vue {
  getSubTasksName = (name) => getSubTasksName(this, name)
  getStepLineName = (name) => getStepLineName(this, name)
  showMore = false

  get getErrorTrace () {
    return this.currentErrorJob?.failed_stack ?? this.$t('noErrorMsg')
  }

  get showViewMore () {
    return this.currentErrorJob.segment_sub_stages && Object.keys(this.currentErrorJob.segment_sub_stages).length > 1
  }

  get getErrorReason () {
    return `${this.currentErrorJob.failed_code ?? ''}${this.currentErrorJob.failed_reason ?? (this.$t('errorStepTips', {name: this.currentErrorJob.failed_step_name ? (this.getSubTasksName(this.currentErrorJob.failed_step_name) || this.getStepLineName(this.currentErrorJob.name)) : this.getStepLineName(this.currentErrorJob.name)}))}`
  }

  // 跳转至手册
  jumpToManual () {
    const manualAddrs = this.currentErrorJob.failed_resolve
    if (manualAddrs) {
      const tag = document.createElement('a')
      tag.href = `https://docs.kyligence.io/books/v4.5/${this.$lang}${manualAddrs}`
      tag.target = '_blank'
      tag.click()
    }
  }

  closeErrorDetail () {
    this.$emit('close')
  }
}
</script>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  .error-contain {
    .error-title {
      font-weight: 500;
    }
    .error-solution-btn {
      i {
        font-size: 16px;
      }
    }
    .error-trace-msg {
      background: @ke-background-color-secondary;
      color: @text-normal-color;
      max-height: 372px;
      min-height: 200px;
      overflow: auto;
      padding: 8px;
      box-sizing: border-box;
      white-space: pre-wrap;
    }
  }
</style>
