<template>
  <div>
    <el-alert show-icon :closable="false" :show-background="false" type="info" class="download-tips">
      <span slot="title">{{$t('outputTips')}}<el-button size="mini" nobg-text @click="downloadLogs">{{$t('download')}}</el-button>{{$t('end')}}</span>
      <!-- 只在 KC 中使用 -->
    </el-alert>
    <!-- <el-alert show-icon :show-background="false" type="info" class="download-tips" v-else>
      <span slot="title">{{$t('outputTipsKC')}}</span>
    </el-alert> -->
    <el-input
    type="textarea"
    :rows="14"
    :readonly="true"
    v-model="stepDetail">
    </el-input>
    <form name="download" class="downloadLogs" :action="actionUrl" target="_blank" method="get">
      <input type="hidden" name="project" :value="downlodaProject"/>
    </form>
  </div>
</template>

<script>
import { apiUrl } from '../../config'
import { mapActions } from 'vuex'
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  props: ['stepDetail', 'stepId', 'jobId', 'targetProject', 'showOutputJob'],
  locales: {
    'en': {
      outputTips: 'The output log shows the first and last 100 lines. Click to ',
      download: 'download the full log',
      end: '.',
      outputTipsKC: 'The output log shows the first and last 100 lines by default. To view all the output, please download diagnosis package on Admin-Troubleshooting page.',
      gotoSystemLog: 'view all logs',
      or: ' or '
    }
  },
  computed: {
    downlodaProject () {
      if (this.showOutputJob) { // 下载流数据任务日志
        return this.showOutputJob.project
      } else {
        return this.targetProject
      }
    },
    actionUrl () {
      if (this.showOutputJob) { // 下载流数据任务日志
        return `${apiUrl}streaming_jobs/${this.showOutputJob.uuid}/download_log`
      } else {
        return apiUrl + 'jobs/' + this.jobId + '/steps/' + this.stepId + '/log'
      }
    }
  },
  methods: {
    ...mapActions({
      downloadLog: 'DOWNLOAD_LOGS'
    }),
    downloadLogs () {
      if (this.hasClickDownloadLogBtn) {
        return false
      }
      this.hasClickDownloadLogBtn = false
      this.$nextTick(() => {
        this.$el.querySelectorAll('.downloadLogs').length && this.$el.querySelectorAll('.downloadLogs')[0].submit()
      })
    }
  }
})
export default class JobDialog extends Vue {
  data () {
    return {
      hasClickDownloadLogBtn: false,
      cloudService: localStorage.getItem('cloud_service') ? localStorage.getItem('cloud_service') : 'AWSChinaCloud'
    }
  }
}
</script>
<style lang="less">
.download-tips {
  padding-top: 0 !important;
  .el-alert__title {
    font-size: 14px;
  }
  .el-button {
    padding: 0;
    font-size: 14px;
    span {
      vertical-align: bottom;
    }
  }
  .el-alert__closebtn{
    top:3px;
  }
}
</style>
