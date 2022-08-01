<template>
  <el-dialog
    custom-class="diagnostic-dialog"
    :visible.sync="isShow"
    width="600px"
    :close-on-click-modal="false"
    :before-close="() => handleClose('header')"
    :title="isQueryHistory?$t('queryDiagnostic'):isJobDiagnosis ? $t('jobDiagnosis') : $t('systemDiagnosis') "
  >
    <div class="body">
      <template v-if="!isJobDiagnosis && !isQueryHistory">
        <el-alert
          class="ksd-pt-0 ksd-mb-10"
          type="info"
          :show-background="false"
          :closable="false"
          show-icon>
          <span slot="title"><span v-html="$t('downloadSystemDiagPackage1')"></span><a href="javascript:void(0)" @click="goto('job',$route.name)">{{$t('jobPage')}}</a>{{$t('downloadSystemDiagPackage2')}}</span>
        </el-alert>
        <div class="time-range">{{$t('timeRange')}}<el-tooltip :content="$t('timeRangeTip')" effect="dark" placement="top"><i class="el-icon-ksd-what"></i></el-tooltip>：</div>
        <el-radio-group v-model="timeRangeValue" class="time-range-radio" @change="changeTimeRange" :disabled="isRunning">
          <el-radio :label="item.label" v-for="(item, index) in timeRange" :key="index">{{item.text}}</el-radio>
        </el-radio-group>
        <div class="datapicket-layout">
          <p class="custom-time-tip" v-if="timeRangeValue === 'custom'">{{$t('customTimeTip')}}</p>
          <el-date-picker
            :class="{'is-disabled': timeRangeValue !== 'custom', 'is-error': validDateTime && getDateTimeValid}"
            v-model="dateTime.prev"
            type="datetime"
            :placeholder="$t('selectDatePlaceholder')"
            align="right"
            :disabled="timeRangeValue !== 'custom' || isRunning"
            :clearable="timeRangeValue === 'custom'"
            @blur="onBlur"
          />
          <span>&#8211;</span>
          <el-date-picker
            :class="{'is-disabled': timeRangeValue !== 'custom', 'is-error': validDateTime && getDateTimeValid}"
            v-model="dateTime.next"
            type="datetime"
            :placeholder="$t('selectDatePlaceholder')"
            align="right"
            :disabled="timeRangeValue !== 'custom' || isRunning"
            :clearable="timeRangeValue === 'custom'"
          />
          <p class="error-text" v-if="validDateTime && getDateTimeValid">{{$t('timeErrorMsg')}}</p>
        </div>
      </template>
      <div :class="['server', !isJobDiagnosis && !isQueryHistory && 'ksd-mt-16']">
        <div
          v-if='isQueryHistory'
          class="ksd-pt-0 ksd-mb-16"
        >
          <template>
            <span v-if="isAdminRole" class='describe'>{{$t('downloadQueryDiagnostic')}}<a href="javascript:void(0)" @click="goto('job',$route.name)">{{$t("monitor")}}</a>{{$t("downloadQueryDiagnosticForKylinsubText1")}}<a href="javascript:void(0)" @click="goto('admin',$route.name)">{{$t("adminMode")}}</a>{{$t('downloadQueryDiagnosticForKylinsubText2')}}</span>
            <span v-else class='describe'>{{$t('downloadQueryDiagnostic')}}<a href="javascript:void(0)" @click="goto('job',$route.name)">{{$t("monitor")}}</a>{{$t('downloadQueryDiagnosticForKylinsubText3')}}</span>
          </template>
        </div>
        <div
          class="ksd-pt-0 ksd-mb-16"
          v-if="isJobDiagnosis"
        >
          <template>
            <span v-if="isAdminRole" class='describe'>{{$t('downloadJobDiagnostic')}}<a href="javascript:void(0)" @click="goto('query',$route.name)">{{$t("queryPage")}}</a>{{$t("downloadJobDiagnosticSubText1")}}<a href="javascript:void(0)" @click="goto('admin',$route.name)">{{$t("adminMode")}}</a>{{$t('downloadQueryDiagnosticForKylinsubText2')}}</span>
            <span v-else class='describe'>{{$t('downloadJobDiagnostic')}}<a href="javascript:void(0)" @click="goto('query',$route.name)">{{$t("queryPage")}}</a>{{$t('downloadJobDiagnosticSubText2')}}</span>
          </template>
        </div>
        <el-alert
          class="ksd-mb-16"
          type="warning"
          v-if="$route.name === 'Job' && this.jobStatus === 'RUNNING'"
          :closable="false"
          show-icon>
          <template>
            <span slot="title" v-html="$t('downloadJobDiagPackage3')"></span>
          </template>
        </el-alert>
        <p class="title">{{$t('server')}}</p>
        <el-select :class="{'no-selected': isServerChange && !servers.length}" v-model="servers" multiple :placeholder="$t('selectServerPlaceHolder')" :disabled="isRunning || isQueryHistory" @change="isServerChange = true">
          <el-option
            v-for="item in serverOptions"
            :key="item.value"
            :label="item.label"
            :value="item.value">
          </el-option>
        </el-select>
        <p class="error-text" v-if="isServerChange && !servers.length">{{$t('selectServerTip')}}</p>
      </div>
      <div class="download-layout" v-if="isShowDiagnosticProcess">
        <p class="download-layout-title">{{$t('createDiagnostic')}}</p>
        <div class="download-progress">
          <div class="progress-item clearfix" v-for="item in diagDumpIds" :key="item.id">
            <div class="download-details">
              <p class="title ksd-mb-8">{{ getTitle(item) }}
                <!-- <template v-if="item.tm"> ｜{{getPrevTimeValue({date: item.tm}).split(' ')[1]}}<span class="split-character default-color"></span></template> -->
                <template v-if="item.duration"> ｜{{item.duration | timeSize}}</template>
              </p>
              <el-progress class="progress" :percentage="Math.ceil(+item.progress * 100)" v-bind="setProgressColor(item)" :icon-class="item.stage==='PREPARE'?'el-ksd-icon-time_22':''"></el-progress>
              <template v-if="item.status === '001'">
                <!-- TODO: add kylin5 diag doc -->
                <p class="error-text">{{$t('requireOverTime1')}}<span class='retry-btn' @click="retryJob(item)">{{$t('retry')}}</span>{{$t('requireOverTime2')}}<a :href="$lang === 'en' ? 'https://docs.kyligence.io/books/v4.5/en/Operation-and-Maintenance-Guide/system-operation/diag.en.html' : 'https://docs.kyligence.io/books/v4.5/zh-cn/Operation-and-Maintenance-Guide/system-operation/diagnosis/diag.cn.html'" target="_blank">{{$t('manual')}}<i class="el-ksd-icon-export_22 export-icon"></i></a>{{$t('requireOverTime3')}}</p>
              </template>
              <template v-if="['002', '999'].includes(item.status)">
                <span class="error-text">{{item.status === '002' ? $t('noAuthorityTip') : $t('otherErrorMsg')}}</span><span class="detail-text" @click="item.showErrorDetail = !item.showErrorDetail">{{$t('details')}}<i :class="item.showErrorDetail ? 'el-icon-arrow-up' : 'el-icon-arrow-down'"></i></span>
                <div class="dialog-detail" v-if="item.showErrorDetail">
                  <el-input class="details-content" type="textarea" v-model.trim="item.error" :rows="4" :disabled="true"></el-input>
                  <el-tooltip
                    content="Copy"
                    placement="top"
                  >
                     <el-button class="copyBtn" size="mini" v-clipboard:copy="item.error" v-clipboard:success="onCopy" v-clipboard:error="onError" icon-button icon="el-ksd-icon-dup_22"></el-button>
                  </el-tooltip>
                </div>
              </template>
              <template v-if="item.status === '000' && item.stage === 'DONE'">
                <p class="manual-download">{{$t('manualDownloadTip')}}<el-button nobg-text size="small" @click="downloadEvent (item)">{{$t('manualDownload')}}</el-button></p>
              </template>
            </div>
          </div>
        </div>
      </div>
    </div>
    <template>
      <div slot="footer">
        <el-button size="medium" @click="handleClose">{{$t('cancel')}}</el-button>
        <el-button type="primary" size="medium" @click="generateDiagnostic" :loading="isRunning" :disabled="getDateTimeValid || !servers.length ">{{$t('generateBtn')}}</el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import locales from './locales'
import { getPrevTimeValue } from '../../../util/business'
import vuex from '../../../store'
import store, { types } from './store'
import { mapActions, mapState, mapMutations, mapGetters } from 'vuex'
vuex.registerModule(['diagnosticModel'], store)

@Component({
  props: {
    jobId: {
      type: String,
      default: ''
    },
    jobStatus: {
      type: String,
      default: ''
    },
    queryServer: {
      type: String,
      default: ''
    }
  },
  computed: {
    ...mapState('diagnosticModel', {
      host: state => state.host,
      diagDumpIds: state => state.diagDumpIds
    }),
    ...mapGetters([
      'isAdminRole',
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions('diagnosticModel', {
      getDumpRemote: types.GET_DUMP_REMOTE,
      getServers: types.GET_SERVERS,
      downloadDumps: types.DOWNLOAD_DUMP_DIAG,
      removeDiagnosticTask: types.REMOVE_DIAGNOSTIC_TASK,
      getQueryDiagnostic: types.GET_QUERY_DIAGNOSTIC
    }),
    ...mapMutations('diagnosticModel', {
      updateCheckType: types.UPDATE_CHECK_TYPE,
      delDumpid: types.DEL_DUMP_ID_LIST,
      resetDumpData: types.RESET_DUMP_DATA,
      stopInterfaceCall: types.STOP_INTERFACE_CALL
    }),
    getPrevTimeValue
  },
  locales
})

export default class Diagnostic extends Vue {
  isShow = true
  isRunning = false
  // isManualDownload = false
  checkAll = false
  indeterminate = false
  isShowDiagnosticProcess = false
  showError = false
  showPopoverTip = false
  popperClass = ''
  validDateTime = false
  isServerChange = false
  serverOptions = []
  timeRangeValue = 'lastDay'
  dateTime = {
    prev: '',
    next: ''
  }
  servers = []
  popoverCallback = null
  jumpPage = ''

  @Watch('diagDumpIds')
  onChangeDumpList (newVal, oldVal) {
    if (JSON.stringify(oldVal) === '{}' || JSON.stringify(newVal) === '{}') return
    let list = Object.keys(newVal).length && Object.keys(newVal).filter(it => newVal[it].running)
    if (!list.length) {
      this.isRunning = false
    }
  }
  get timeRange () {
    return [
      {text: this.$t('lastHour'), label: 'lastHour'},
      {text: this.$t('lastDay'), label: 'lastDay'},
      {text: this.$t('lastThreeDay'), label: 'lastThreeDay'},
      {text: this.$t('lastMonth'), label: 'lastMonth'},
      {text: this.$t('custom'), label: 'custom'}
    ]
  }
  openConfirm () {
    this.$confirm(this.$t('closeModelTip'), this.$t('closeModalTitle'), {
      confirmButtonText: this.$t('confrimBtn'),
      cancelButtonText: this.$t('cancelBtn'),
      type: 'warning',
      centerButton: true
    }).then(() => {
      this.closeDialog()
    })
  }
  goto (page, previousPage) {
    this.jumpPage = page
    if (this.isRunning) {
      this.popoverCallback = this.gotoEventCallback
      this.openConfirm()
    } else {
      this.gotoEventCallback(previousPage)
      this.$emit('close')
    }
  }
  gotoEventCallback (previousPage) {
    if (this.jumpPage === 'job') {
      if (this.$route.query.previousPage && this.$route.query.previousPage === 'StreamingJob') {
        this.$router.push('/monitor/streamingJob')
      } else {
        this.$router.push('/monitor/job')
      }
    } else if (this.jumpPage === 'query') {
      this.$router.push('/query/queryhistory')
    } else {
      this.$router.push(`/admin/project?previousPage=${previousPage}`)
    }
  }
  // 获取诊断包可下载数/总数
  get getDownloadNum () {
    const totalList = Object.keys(this.diagDumpIds)
    const checkList = totalList.filter(item => this.diagDumpIds[item] && this.diagDumpIds[item].isCheck)
    return `${checkList.length}/${totalList.length}`
  }
  // 日期在5分钟～1个月内
  get getDateTimeValid () {
    return !this.dateTime.prev || !this.dateTime.next || this.getTimes(this.dateTime.prev) > this.getTimes(this.dateTime.next) || (this.getTimes(this.dateTime.next) - this.getTimes(this.dateTime.prev)) < 300000 || (this.getTimes(getPrevTimeValue({ date: this.dateTime.next, m: 1 })) - this.getTimes(this.dateTime.prev)) > 0
  }
  // 是否展示手动下载提示
  // get showManualDownloadLayout () {
  //   return Object.keys(this.diagDumpIds).filter(it => this.diagDumpIds[it].stage === 'DONE').length > 0
  // }
  // 是否任务为Job诊断包
  get isJobDiagnosis () {
    return this.$route.name === 'Job' || this.$route.name === 'StreamingJob'
  }
  // 是否为查询诊断包
  get isQueryHistory () {
    return this.$route.name === 'QueryHistory'
  }
  // 进度条title
  getTitle (item) {
    let [{label}] = this.serverOptions.filter(it => it.value === item.host.replace(/http:\/\//, ''))
    return label
  }
  getTimes (date) {
    return date ? new Date(date).getTime() : new Date().getTime()
  }
  // 更改进度条颜色
  setProgressColor (item) {
    const { progress = +progress, status } = item
    if (status === '000') {
      let color = ''
      let type = null
      switch (true) {
        case progress >= 0 && progress < 0.3:
          color = '#0988DE' // 蓝色进度条
          break
        case progress >= 0.3 && progress < 1:
          color = '#0988DE'
          break
        case progress >= 1:
          type = 'success'
          break
      }
      return type ? {status: type} : {color}
    } else {
      return {status: 'exception'}
    }
  }
  created () {
    this.getServers(this).then((data) => {
      if (data) {
        if (this.isQueryHistory) {
          const hostItem = data.find((item) => item.host === this.queryServer)
          Object.prototype.toString.call(hostItem) === '[object Object]' && this.serverOptions.push({label: `${hostItem.host}(${hostItem.mode && hostItem.mode.toLocaleUpperCase()})`, value: hostItem.host})
          this.servers = this.serverOptions.length ? [this.serverOptions[0].value] : []
        } else {
          data.forEach(item => {
            Object.prototype.toString.call(item) === '[object Object]' && this.serverOptions.push({label: `${item.host}(${item.mode && item.mode.toLocaleUpperCase()})`, value: item.host})
          })
          this.servers = this.serverOptions.length ? [this.serverOptions[0].value] : []
        }
      }
    })
  }
  onBlur () {
    this.validDateTime = true
  }
  // 更改时间选择操作
  changeTimeRange (val) {
    this.validDateTime = false
    const date = new Date()
    let dt = date.getTime()
    let t = dt

    switch (val) {
      case 'lastHour':
        t = dt - 60 * 60 * 1000
        this.dateTime.prev = new Date(t)
        this.dateTime.next = date
        break
      case 'lastDay':
        t = dt - 24 * 60 * 60 * 1000
        this.dateTime.prev = new Date(t)
        this.dateTime.next = date
        break
      case 'lastThreeDay':
        t = dt - 3 * 24 * 60 * 60 * 1000
        this.dateTime.prev = new Date(t)
        this.dateTime.next = date
        break
      case 'lastMonth':
        this.dateTime.prev = new Date(getPrevTimeValue({ date, m: 1 }))
        this.dateTime.next = date
        break
      case 'custom':
        this.dateTime.prev = ''
        this.dateTime.next = new Date(date)
        break
      default:
        this.dateTime.prev = ''
        this.dateTime.next = ''
    }
  }
  // 有诊断包在生成中关闭弹窗时的popover提示
  handleClose (para) {
    if (this.isRunning) {
      this.openConfirm()
      return
    }
    this.closeDialog()
  }
  // 关闭弹窗
  closeDialog () {
    // 增加停止后台诊断包生成接口
    this.removeDiagnosticTask(this.$t('deleteDiagnosticSuccess'))
    this.resetDumpData(true)
    this.stopInterfaceCall(true)
    this.$emit('close')
    this.popoverCallback && this.popoverCallback()
  }
  // 生成诊断包
  generateDiagnostic () {
    if (this.getDateTimeValid || !this.servers.length) return
    if (this.isJobDiagnosis && !this.jobId) {
      return
    }
    this.resetDumpData(false)
    this.isRunning = true
    this.isShowDiagnosticProcess = true
    let apiErrorNum = 0
    let data = {}
    if (this.isJobDiagnosis) {
      data = {
        job_id: this.jobId
      }
    } else if (this.isQueryHistory) {
      data = {
        query_id: this.jobId,
        project: this.currentSelectedProject
      }
    } else {
      data = {
        start: new Date(this.dateTime.prev).getTime(),
        end: new Date(this.dateTime.next).getTime()
      }
    }
    this.servers.forEach(async (host) => {
      if (this.isQueryHistory) {
        await this.getQueryDiagnostic({
          host: `http://${host.trim()}`,
          ...data,
          tm: this.getTimes()
        }).then(() => {
        }).catch(() => {
          apiErrorNum += 1
          if (apiErrorNum === this.servers.length) {
            this.isShowDiagnosticProcess = false
            this.isRunning = false
          }
        })
      } else {
        await this.getDumpRemote({
          host: `http://${host.trim()}`,
          ...data,
          tm: this.getTimes()
        }).then(() => {
        }).catch(() => {
          apiErrorNum += 1
          if (apiErrorNum === this.servers.length) {
            this.isShowDiagnosticProcess = false
            this.isRunning = false
          }
        })
      }
    })
  }
  // 生成超时，重新生成
  retryJob (item) {
    const { host, start, end, id } = item
    this.delDumpid(id)
    this.getDumpRemote({ host, start, end, job_id: this.jobId || '', tm: this.getTimes() })
  }
  changeCheckAllType (val) {
    this.indeterminate = false
    this.updateCheckType(val)
  }
  // 取消手动下载
  cancelManualDownload () {
    this.checkAll = false
    this.indeterminate = false
    this.updateCheckType(false)
  }
  onCopy () {
    this.$message.success(this.$t('kylinLang.common.copySuccess'))
  }
  onError () {
    this.$message.error(this.$t('kylinLang.common.copyfail'))
  }
  changeCheckItems () {
    let checkList = Object.keys(this.diagDumpIds).filter(it => this.diagDumpIds[it].isCheck)
    this.indeterminate = checkList.length > 0 && checkList.length !== Object.keys(this.diagDumpIds).length
    this.checkAll = checkList.length === Object.keys(this.diagDumpIds).length
  }
  // 手动下在诊断包
  downloadEvent (item) {
    const {host, id} = item
    this.downloadDumps({host, id})
  }

  mounted () {
    this.changeTimeRange('lastDay')
    this.stopInterfaceCall(false)
  }
}
</script>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  .diagnostic-dialog {
    .el-dialog__body {
      max-height: 464px !important;
      overflow-y: overlay;
    }
    .body {
      color: @text-title-color;
      .time-range {
        margin-bottom: 10px;
        font-weight: bold;
        .el-icon-ksd-what {
          margin-left: 5px;
        }
      }
      .time-range-radio {
        .el-radio {
          height: 30px;
          padding: 0 8px;
          box-sizing: border-box;
          &.is-checked {
            background: @background-disabled-color;
          }
        }
      }
      .datapicket-layout {
        width: 100%;
        background: @background-disabled-color;
        padding: 10px 10px;
        box-sizing: border-box;
        .custom-time-tip {
          font-size: 12px;
          margin-bottom: 5px;
          color: @text-normal-color;
        }
        .el-date-editor.is-disabled {
          width: 175px;
          input {
            border: none;
            padding-right: 0;
            color: @text-normal-color;
            border-color: @line-border-color;
          }
        }
        .el-date-editor.is-error {
          input {
            border: 1px solid @error-color-1;
          }
        }
      }
      .server {
        .describe{
          font-size: 12px;
          color:@text-normal-color
        }
        .title{
          font-size: 12px;
          line-height: 16px;
          font-weight: 600
        }
        .el-select {
          margin-top: 10px;
          width: 100%;
        }
        .no-selected {
          input {
            border: 1px solid @error-color-1;
          }
        }
      }
      .error-text {
        font-size: 12px;
        color: @error-color-1;
        max-width: 650px;
        line-height: 16px;
        .export-icon{
          font-size:16px;
          vertical-align: top;
        }
      }
      .download-layout {
        margin-top: 15px;
        font-size: 14px;
        .download-layout-title{
          font-size: 12px;
          line-height: 16px;
          font-weight: 600;
        }
        .download-progress {
          margin-top: -16px;
          .progress-item {
            margin-bottom:16px;
            .el-checkbox {
              float: left;
              margin-top: 34px;
              margin-right: 10px;
              line-height: 1;
            }
            .download-details {
              display: inline-block;
              width:100%;
            }
            .title {
              font-size: 12px;
              font-weight: 400;
              text-align: right;
              color: @text-disabled-color;
            }
            .progress {
              width: 100%;
              .el-progress-bar{
                padding-right: 40px;
                margin-right: -40px;
               }
            }
            .retry-btn {
              color: @base-color;
              cursor: pointer;
              font-size: 12px;
            }
            .detail-text {
              font-size: 12px;
              color: @base-color;
              cursor: pointer;
            }
            .dialog-detail{
              // border:solid 1px @line-border-color;
              // background:@background-disabled-color;
              position: relative;
              margin-top: 10px;
              width: 100%;
              .details-content {
                width: 100%;
                border: solid 1px @line-border-color;
                border-radius: 2px;
                textarea {
                  height: 175px;
                  width: 100%;
                }
              }
              .copyBtn{
                position: absolute;
                right:5px;
                top:5px;
                width:40px;
                height:38px;
              }
            }
            .manual-download{
              line-height: 16px;
              font-size: 12px;
              font-weight: 400;
              color:@text-normal-color;
              button{
                vertical-align: top;
              }
            }
          }
        }
        .checkbox-group {
          height: 30px;
          background-color: @background-disabled-color;
          position: relative;
          font-size: 14px;
          margin-top: 15px;
          line-height: 30px;
          padding: 0 10px;
          .el-checkbox {
            line-height: 1;
            margin-top: 4px;
          }
          .download-msg {
            color: @base-color;
            position: absolute;
            right: 10px;
            top: 0;
            cursor: pointer;
            .cancel {
              color: @text-normal-color;
              margin-left: 10px;
              cursor: pointer;
            }
          }
        }
      }
    }
  }
  .el-popover.popover-running {
    top: 179px !important;
    left: 50% !important;
    .popper__arrow {
      display: none;
    }
  }
  .el-popover.popover-jump-link {
    top: 240px !important;
    left: 30% !important;
    .popper__arrow {
      display: none;
    }
  }
</style>
