<template>
  <div id="queryTab">
    <!-- for guide start -->
    <el-button @click="handleForGuide" style="position: absolute;" v-visible></el-button>
    <!-- for guide end -->
    <div class="query_panel_box ksd-mb-34">
      <kylin-editor ref="insightBox" :class="{'guide-WorkSpaceEditor':isWorkspace}" height="218" lang="sql" theme="chrome" @keydown.meta.enter.native="submitQuery(sourceSchema)" @keydown.ctrl.enter.native="submitQuery(sourceSchema)" v-model="sourceSchema" :readOnly="!isWorkspace">
      </kylin-editor>
      <div class="clearfix operatorBox">
        <p class="tips_box">
          <el-button type="primary" text size="small" @click.native="openSaveQueryDialog" :disabled="!sourceSchema">{{$t('kylinLang.common.save')}}</el-button><el-button
          size="small" type="primary" text @click.native="resetQuery" :disabled="!sourceSchema" v-if="isWorkspace" style="display:inline-block">{{$t('clear')}}</el-button>
        </p>
        <p class="operator" v-if="isWorkspace">
          <el-form :model="queryForm" :inline="true" ref="queryForm" @submit.native.prevent class="demo-form-inline">
            <el-form-item v-show="showHtrace">
              <el-checkbox class="ksd-mt-4" v-model="queryForm.isHtrace" @change="changeTrace">{{$t('trace')}}</el-checkbox>
            </el-form-item><el-form-item>
              <el-checkbox class="ksd-mt-4" v-model="queryForm.hasLimit" @change="changeLimit">Limit</el-checkbox>
            </el-form-item><el-form-item :rules="limitRule" prop="listRows" :show-message="false">
              <el-input placeholder="" size="small" style="width:90px;" @input="handleInputChange" v-model="queryForm.listRows" class="limit-input"></el-input>
            </el-form-item><el-form-item>
              <el-button type="primary" size="small" class="ksd-btn-minwidth" :disabled="!sourceSchema" :loading="isLoading && isStopping" @click="submitQuery(sourceSchema)">{{!isLoading ? $t('runQuery') : $t('stopQuery')}}</el-button>
            </el-form-item>
          </el-form>
        </p>
      </div>
      <div class="submit-tips" v-if="isWorkspace">
        <i class="el-icon-ksd-info ksd-fs-12" ></i>
        Control / Command + Enter = <span>{{!isLoading ? $t('runQuery') : $t('stopQuery')}}</span></div>
    </div>
    <div v-show="isLoading" class="ksd-center ksd-mt-10">
      <el-progress type="circle" :percentage="percent"></el-progress>
    </div>
    <div id="queryPanelBox" v-if="extraoptionObj&&errinfo">
      <div class="resultTipsLine">
        <div class="resultTips">
          <p class="resultText" v-if="extraoptionObj.queryId">
            <span class="label">{{$t('kylinLang.query.query_id')}}: </span>
            <span class="text" v-if="extraoptionObj.queryId">{{extraoptionObj.queryId}}</span>
            <span v-else class="ksd-ml-10">
              <common-tip :content="$t('linkToSpark')" v-if="extraoptionObj.appMasterURL">
                <a target="_blank" :href="extraoptionObj.appMasterURL"><i class="el-icon-ksd-go"></i></a>
              </common-tip>
            </span>
          </p>
          <!-- <p class="resultText"><span class="label">{{$t('kylinLang.query.status')}}</span>
          <span class="ky-error">{{$t('kylinLang.common.error')}}</span></p> -->
        </div>
        <pre class="error-block">{{errinfo}}</pre>
      </div>
    </div>
    <queryresult :extraoption="extraoptionObj" :isWorkspace="isWorkspace" :isStop="tabsItem.isStop" :tabsItem="tabsItem" v-if="extraoptionObj && !errinfo" :queryExportData="tabsItem.queryObj"></queryresult>
    <div class="no-result-block"  :class="{'is-global-alter': $store.state.system.isShowGlobalAlter}" v-if="!extraoptionObj && !isLoading">
      <kylin-nodata :content="$t('queryTips')"></kylin-nodata>
    </div>
    <save_query_dialog :show="saveQueryFormVisible" :sql="this.sourceSchema" :project="currentSelectedProject" v-on:closeModal="closeModal"></save_query_dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapState } from 'vuex'
import queryresult from './query_result'
import saveQueryDialog from './save_query_dialog'
import { kylinConfirm, handleSuccess, handleError } from '../../util/business'
@Component({
  props: ['tabsItem', 'completeData', 'tipsName'],
  methods: {
    ...mapActions({
      query: 'QUERY_BUILD_TABLES',
      stop: 'STOP_QUERY_BUILD'
    })
  },
  components: {
    queryresult,
    'save_query_dialog': saveQueryDialog
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    ...mapState({
      config: state => state.config
    })
  },
  locales: {
    'en': {
      trace: 'Trace',
      queryBox: 'Query Box',
      linkToSpark: 'Jump to Spark Web UI',
      resetTips: 'Are you sure you want to clear the SQL Editor?',
      queryTips: 'You can enter a SQL query in the SQL editor, and the results will be displayed here after the query runs',
      clear: 'Clear',
      runQuery: 'Run Query',
      stopQuery: 'Stop Query',
      overIntegerLength: 'Please enter a value no larger than 2,147,483,647.',
      viewLogs: 'View Logs',
      htraceTips: 'Please make sure Zipkin server is properly deployed according to the manual of performance diagnose package.',
      queryError: 'The query fails.',
      queryTimeOut: 'The request timeout, please check the network situation and Kylin service instance status. If the resource group is turned on, please make sure that the project is bound to the query resource group and there are available query instances',
      queryTimeOutInCloud: 'The request timeout, please check the network situation and Kyligence Engine service instance status.'
    }
  }
})
export default class QueryTab extends Vue {
  queryForm = {
    hasLimit: true,
    listRows: 500,
    isHtrace: false
  }
  sourceSchema = ''
  saveQueryFormVisible = false
  extraoptionObj = null
  isLoading = false
  percent = 0
  ST = null
  errinfo = ''
  isWorkspace = true
  activeQueryId = ''
  isStopping = false

  get limitRule () {
    return [
      { validator: this.checkLimitNum, trigger: 'blur' }
    ]
  }

  checkLimitNum (rule, value, callback) {
    if (+value > 2147483647) {
      this.$message({
        type: 'error',
        message: this.$t('overIntegerLength'),
        closeOtherMessages: true
      })
      callback(new Error())
    } else {
      callback()
    }
  }

  uniqueId () {
    return `query_${new Date().getTime().toString(32)}`
  }

  // 为了guide
  handleForGuide (obj) {
    let action = obj.action
    let data = obj.data
    if (action === 'intoEditor') {
      this.activeSubMenu = 'WorkSpace'
    } else if (action === 'inputSql') {
      this.sourceSchema = data
    } else if (action === 'requestSql') {
      const queryObj = {
        acceptPartial: true,
        limit: 500,
        offset: 0,
        project: this.currentSelectedProject,
        sql: data,
        backdoorToggles: {
          DEBUG_TOGGLE_HTRACE_ENABLED: false
        }
      }
      this.query(queryObj)
    }
  }
  changeLimit () {
    if (this.queryForm.hasLimit) {
      this.queryForm.listRows = 500
    } else {
      this.queryForm.listRows = 0
    }
  }
  changeTrace () {
    if (this.queryForm.isHtrace) {
      kylinConfirm(this.$t('htraceTips'), {centerButton: true})
    }
  }
  openSaveQueryDialog () {
    this.saveQueryFormVisible = true
  }
  closeModal (needRefreshData) {
    if (needRefreshData) {
      this.$emit('refreshSaveQueryCount')
    }
    this.saveQueryFormVisible = false
  }
  handleInputChange (value) {
    this.$nextTick(() => {
      this.queryForm.listRows = (isNaN(value) || value === '' || value < 0) ? 0 : Number(value)
    })
  }
  async resetQuery () {
    await kylinConfirm(this.$t('resetTips'), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('clear'), type: 'warning', centerButton: true})
    this.$emit('resetQuery')
    this.$nextTick(() => {
      this.sourceSchema = ''
      this.extraoptionObj = null
      this.errinfo = ''
      const editor = this.$refs.insightBox
      editor.$emit('setValue', '')
    })
  }
  async submitQuery (querySql) {
    try {
      const valid = await this.$refs.queryForm.validate()
      if (!valid) {
        return
      }
      if (!this.isLoading) {
        if (!this.isWorkspace || !querySql) {
          return
        }
        this.activeQueryId = this.uniqueId()
        const queryObj = {
          limit: this.queryForm.listRows,
          offset: 0,
          project: this.currentSelectedProject,
          sql: querySql,
          stopId: this.activeQueryId
        }
        this.$emit('addTab', 'query', queryObj)
      } else {
        this.isStopping = true
        this.stop({id: this.activeQueryId})
      }
    } catch (e) {
    }
  }

  resetResult () {
    this.extraoptionObj = null
    this.errinfo = ''
    this.isLoading = false
    this.isStopping = false
    this.queryLoading()
    this.$nextTick(() => {
      this.isLoading = true
    })
  }
  queryResult (queryObj) {
    this.resetResult()
    this.query(queryObj).then((res) => {
      clearInterval(this.ST)
      handleSuccess(res, (data) => {
        this.isLoading = false
        this.isStopping = false
        this.extraoptionObj = data
        if (data.isException) {
          this.errinfo = data.exceptionMessage || this.$t('queryError')
          this.$emit('changeView', this.tabsItem.index, data, this.errinfo)
        } else {
          this.errinfo = ''
          this.$emit('changeView', this.tabsItem.index, data)
        }
      })
    }, (res) => {
      this.isLoading = false
      this.isStopping = false
      handleError(res, (data, code, status, msg) => {
        this.errinfo = msg || this.$t('queryTimeOut')
        if (!status || status !== 200 || status < 0) {
          this.config.errorMsgBox.isShow = true
          this.config.errorMsgBox.msg = this.errinfo
          this.config.errorMsgBox.detail = data
        }
        this.$emit('changeView', this.tabsItem.index, data, this.errinfo, true) // 最后的true为系统弹框报错（job节点不支持查询）
      })
    })
  }
  queryLoading () {
    var _this = this
    this.percent = 0
    clearInterval(this.ST)
    this.ST = setInterval(() => {
      var randomPlus = Math.round(10 * Math.random())
      if (_this.percent + randomPlus < 99) {
        _this.percent += randomPlus
      } else {
        clearInterval(_this.ST)
      }
    }, 300)
  }
  get showHtrace () {
    return this.$store.state.system.showHtrace === 'true'
  }
  @Watch('completeData')
  onCompleteDataChange (val) {
    if (val) {
      this.$refs.insightBox.$emit('setAutoCompleteData', this.completeData)
    }
  }
  @Watch('tipsName')
  onTipsNameChange (val) {
    if (val && this.$parent.name === 'WorkSpace' && this.$parent.active) {
      const editor = this.$refs.insightBox
      editor.$emit('focus')
      editor.$emit('insert', val)
      this.sourceSchema = editor.getValue()
    }
  }
  @Watch('tabsItem.queryObj')
  onTabsItemChange (val) {
    if (val) {
      this.extraoptionObj = this.tabsItem.extraoption
      this.errinfo = this.tabsItem.queryErrorInfo
      this.sourceSchema = this.tabsItem.queryObj && this.tabsItem.queryObj.sql || ''
      this.isWorkspace = this.tabsItem.name === 'WorkSpace'
      // if (this.tabsItem.queryObj && this.tabsItem.index) {
      //   this.queryResult(this.tabsItem.queryObj)
      // } else {
      //   this.resetResult()
      // }
      this.resetResult()
    }
  }

  @Watch('tabsItem.extraoption')
  onTabsResultChange (val) {
    this.isLoading = false
    this.extraoptionObj = this.tabsItem.extraoption
    this.errinfo = this.tabsItem.queryErrorInfo
  }
  @Watch('tabsItem.queryErrorInfo')
  onQueryException (val) {
    this.isLoading = false
    this.errinfo = this.tabsItem.queryErrorInfo
  }
  @Watch('tabsItem.cancelQuery')
  onCancelQuery (val) {
    this.$nextTick(() => {
      if (val && this.isLoading) {
        this.isLoading = false
        this.$emit('changeView', 0, null, '')
      }
    })
  }
  destoryed () {
    clearInterval(this.ST)
  }

  created () {
    this.extraoptionObj = this.tabsItem.extraoption
    this.errinfo = this.tabsItem.queryErrorInfo
    this.isWorkspace = this.tabsItem.name === 'WorkSpace'
    this.tabsItem.queryObj && !this.tabsItem.queryObj.stopId && (this.tabsItem.queryObj.stopId = this.uniqueId())
    if (this.tabsItem.queryObj && !this.tabsItem.extraoption && this.tabsItem.index) {
      this.queryResult(this.tabsItem.queryObj)
    }
    // 查询语句量大的时候，会造成页面卡顿，所以延迟给编辑器赋值
    window.setTimeout(() => {
      // 如果快速被切走了，就不执行赋值
      if (this._isDestroyed) {
        return
      }
      this.sourceSchema = this.tabsItem.queryObj && this.tabsItem.queryObj.sql || ''
    }, 100)
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  #queryTab {
    .no-result-block {
      height: calc(~'100vh - 380px');
      position: relative;
      &.is-global-alter {
        height: calc(~'100vh - 433px');
      }
    }
    .btn-view-log {
      margin-left: 15px;
      position: relative;
      top: -2px;
    }
  }
  #queryPanelBox {
    .el-progress{
      margin-bottom: 10px;
    }
    .error-block {
      height: 200px;
      overflow-y: scroll;
      background-color: @fff;
      border: 1px solid @grey-2;
      padding: 10px;
      margin-top: 6px;
    }
  }
</style>
