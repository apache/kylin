<template>
  <el-dialog class="global-dialog-box"
    :class="{'hide-bottom-border-top': hideBottomLine}"
    :width="wid"
    :before-close="() => handleClose('close')"
    :close-on-click-modal="false"
    :append-to-body="true"
    :visible.sync="isShow"
    :status-icon="showIcon && dialogStatus[dialogType]"
    limited-area
    :custom-class="customClass"
    :close-on-press-escape="false">
    <span slot="title">
      <span class="ksd-title-label">{{title || $t('kylinLang.common.notice')}}</span>
      <span v-if="isBeta" class="beta-label">BETA</span>
    </span>
    <el-alert
      :type="dialogType"
      :show-background="false"
      :closable="false"
      :show-icon="false">
      <span class="confirm-msg" v-if="dangerouslyUseHTMLString" style="word-break: break-word;" v-html="msg"></span>
      <span class="confirm-msg" v-else style="word-break: break-word;" v-html="filterInjectScript(msg).replace(/\r\n/g, '<br/><br/>')"></span>
      <a href="javascript:;" @click="toggleDetail" v-if="showDetailBtn" class="show-detail">{{$t('kylinLang.common.seeDetail')}}
        <i class="el-icon-arrow-down" v-show="!showDetail"></i>
        <i class="el-icon-arrow-up" v-show="showDetail"></i>
      </a>
      <p v-if="showDetail && detailMsg" class="ksd-mt-15 detailMsg" :class="{'en-lang': $store.state.system.lang === 'en'}" v-html="filterInjectScript(detailMsg).replace(/\n/g, '<br/>')"></p>
      <div v-if="showDetailDirect || showDetail" style="padding-top:10px;">
        <template v-if="theme === 'plain-mult'">
          <div v-for="(item, index) in details" :key="index">
            <p class="mult-title">{{item.title}}</p>
            <div class="dialog-detail">
              <div v-scroll class="dialog-detail-scroll">
                <ul>
                  <li v-for="(p, index) in item.list" :key="index">{{p}}</li>
                </ul>
              </div>
              <transition name="fade">
                <p :class="['copy-text', {'error-text': !copySuccess}]" v-if="showCopyBtn && showCopyTextLeftBtn && showCopyText && copyBtnClickIndex === index"><i :class="[copySuccess ? 'el-icon-circle-check' : 'el-icon-circle-close', 'ksd-mr-5']"></i>{{$t(`${copySuccess ? 'copySuccess' : 'copyFail'}`)}}</p>
              </transition>
              <el-button class="copyBtn" v-if="showCopyBtn" size="mini" v-clipboard:copy="item.list.join('\r\n')"
          v-clipboard:success="() => {copyBtnClickIndex = index, onCopy()}" v-clipboard:error="() => {copyBtnClickIndex = index, onError()}">{{$t('kylinLang.common.copy')}}</el-button>
            </div>
          </div>
        </template>
        <template v-if="theme === 'plain'">
          <div class="dialog-detail">
            <div v-scroll class="dialog-detail-scroll">
              <ul>
                <li v-for="(p, index) in details" :key="index">{{p}}</li>
              </ul>
            </div>
            <transition name="fade">
              <p :class="['copy-text', {'error-text': !copySuccess}]" v-if="showCopyBtn && showCopyTextLeftBtn && showCopyText"><i :class="[copySuccess ? 'el-icon-circle-check' : 'el-icon-circle-close', 'ksd-mr-5']"></i>{{$t(`${copySuccess ? 'copySuccess' : 'copyFail'}`)}}</p>
            </transition>
            <el-button class="copyBtn" v-if="showCopyBtn" size="mini" v-clipboard:copy="details.join('\r\n')"
          v-clipboard:success="onCopy" v-clipboard:error="onError">{{$t('kylinLang.common.copy')}}</el-button>
          </div>
        </template>
        <template v-if="theme === 'sql'">
          <div v-scroll class="dialog-detail">
            <kylin-editor :readOnly="true" :isFormatter="true" class="list-editor" v-for="(s, index) in details" :key="index" height="96" lang="sql" theme="chrome" v-bind:value="s"></kylin-editor>
          </div>
        </template>
      </div>
    </el-alert>
    <div v-if="tableTitle">{{tableTitle}}</div>
    <el-table class="detail-table ksd-mt-10"
      nested
      size="small"
      max-height="420"
      ref="detailTable"
      v-if="detailTableData.length"
      @selection-change="handleSelectionChange"
      :row-class-name="tableRowClassName"
      :data="detailTableData">
      <el-table-column type="selection" align="center" width="44" v-if="isShowSelection"></el-table-column>
      <el-table-column
        v-for="(item, index) in detailColumns" :key="index"
        :prop="item.column"
        :label="item.label"
        :min-width="item.minWidth"
        show-overflow-tooltip>
      </el-table-column>
    </el-table>
    <div slot="footer" class="dialog-footer ky-no-br-space" :class="{'is-center-btn': isCenterBtn}">
      <template v-if="dialogType === 'error'">
        <el-button @click="handleClose">{{closeT}}</el-button>
      </template>
      <template v-else>
        <el-button v-if="needResolveCancel" :type="isSubSubmit? 'primary': ''" :text="isSubSubmit" @click="handleCloseAndResove">{{cancelT}}</el-button>
        <el-button :type="isSubSubmit? 'primary': ''" :text="isSubSubmit" v-else :class="[needResolveCancel && 'ksd-ml-12']" @click="handleClose">{{cancelT}}</el-button>
        <el-button v-if="isSubSubmit" :loading="loading" class="ksd-ml-12" @click="handleSubmit(false)">{{submitSubText}}</el-button>
        <el-button type="primary" v-if="!isHideSubmit" class="ksd-ml-12" :loading="loading" @click="handleSubmit(true)">{{submitT}}</el-button>
      </template>
    </div>
  </el-dialog>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations } from 'vuex'

import vuex from '../../../../store'
import store, { types } from './store'
import { filterInjectScript } from '../../../../util'
vuex.registerModule(['modals', 'DetailDialogModal'], store)

@Component({
  computed: {
    ...mapState('DetailDialogModal', {
      title: state => state.title,
      isBeta: state => state.isBeta,
      dangerouslyUseHTMLString: state => state.dangerouslyUseHTMLString,
      wid: state => state.wid,
      hideBottomLine: state => state.hideBottomLine,
      details: state => state.details,
      detailTableData: state => state.detailTableData,
      detailColumns: state => state.detailColumns,
      isShowSelection: state => state.isShowSelection,
      theme: state => state.theme,
      msg: state => state.msg,
      tableTitle: state => state.tableTitle,
      detailMsg: state => state.detailMsg, // 详情里其他的文案信息
      isShow: state => state.isShow,
      dialogType: state => state.dialogType,
      showIcon: state => state.showIcon,
      showDetailDirect: state => state.showDetailDirect, // 默认展示详情信息
      customClass: state => state.customClass,
      showCopyTextLeftBtn: state => state.showCopyTextLeftBtn,
      showDetailBtn: state => state.showDetailBtn, // 控制是否需要显示详情按钮，默认是显示的
      showCopyBtn: state => state.showCopyBtn,
      needCallbackWhenClose: state => state.needCallbackWhenClose, // 数据源处的特殊需求，关闭时执行回调
      callback: state => state.callback,
      cancelReject: state => state.cancelReject,
      needConcelReject: state => state.needConcelReject, // 模拟conform弹窗关闭时reject
      customCallback: state => state.customCallback,
      closeText: state => state.closeText,
      cancelText: state => state.cancelText,
      submitText: state => state.submitText,
      isCenterBtn: state => state.isCenterBtn,
      isSubSubmit: state => state.isSubSubmit,
      isHideSubmit: state => state.isHideSubmit,
      submitSubText: state => state.submitSubText,
      needResolveCancel: state => state.needResolveCancel,
      onlyCloseDialogReject: state => state.onlyCloseDialogReject
    })
  },
  methods: {
    ...mapMutations('DetailDialogModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      initForm: types.INIT_FORM,
      resetModal: types.RESET_MODAL
    })
  },
  locales: {
    'en': {
      copySuccess: 'Successed to copy.',
      copyFail: 'Can\'t copy.'
    }
  }
})
export default class DetailDialogModal extends Vue {
  filterInjectScript = filterInjectScript
  loading = false
  showDetail = false
  copySuccess = false
  showCopyText = false
  copyBtnClickIndex = 0
  multipleSelection = []
  dialogStatus = {
    success: 'el-ksd-icon-finished_24',
    warning: 'el-ksd-icon-warning_24',
    error: 'el-ksd-icon-error_24',
    info: 'el-ksd-icon-info_24'
  }
  get closeT () {
    return this.closeText || this.$t('kylinLang.common.close')
  }
  get cancelT () {
    return this.cancelText || this.$t('kylinLang.common.cancel')
  }
  get submitT () {
    return this.submitText || this.$t('kylinLang.common.submit')
  }
  // 纯关闭弹窗或者点取消按钮和X都resolve
  handleClose (source) {
    if (this.needCallbackWhenClose) {
      this.callback && this.callback()
    }
    if (this.needConcelReject || (this.onlyCloseDialogReject && source === 'close')) {
      this.cancelReject && this.cancelReject()
    }
    this.handleReset()
  }
  // 取消按钮是关闭且resolve，点X保留纯关闭弹窗
  handleCloseAndResove () {
    this.callback && this.callback()
    this.handleReset()
  }
  handleSubmit (isSubSubmit) {
    this.loading = true
    setTimeout(() => {
      if (this.isShowSelection && this.customCallback) {
        this.customCallback(this.multipleSelection)
      } else {
        this.callback && this.callback({
          isOnlySave: !isSubSubmit // 保存模型使用该属性，isSubSubmit为false时，仅保存模型，不加载数据
        })
      }
      this.handleReset()
    }, 200)
  }
  handleReset () {
    this.hideModal()
    this.resetModal()
    this.loading = false
    this.showDetail = false
    this.multipleSelection = []
  }
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  onCopy () {
    if (this.showCopyTextLeftBtn) {
      this.showCopyText = true
      this.copySuccess = true
      this.hideCopyText()
      return
    }
    this.$message({
      type: 'success',
      message: this.$t('kylinLang.common.copySuccess')
    })
  }
  onError () {
    if (this.showCopyTextLeftBtn) {
      this.showCopyText = true
      this.copySuccess = false
      this.hideCopyText()
      return
    }
    this.$message({
      type: 'error',
      message: this.$t('kylinLang.common.copyfail')
    })
  }
  hideCopyText () {
    let timer = setTimeout(() => {
      this.showCopyText = false
      clearTimeout(timer)
    }, 2000)
  }
  tableRowClassName ({row, rowIndex}) {
    if (row.highlight) {
      return 'highlight-row'
    }
    return ''
  }
  handleSelectionChange (val) {
    this.multipleSelection = val
  }
  @Watch('isShow')
  onModalShow () {
    this.$nextTick(() => {
      if (this.detailTableData.length && this.isShowSelection) {
        this.detailTableData.forEach((item) => {
          this.$refs.detailTable.toggleRowSelection(item)
        })
      }
    })
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.global-dialog-box {
  .beta-label {
    display: inline-block;
    height: 20px;
    line-height: 20px;
    background: #EFDBFF;
    color: #531DAB;
    font-size: 11px;
    font-family: Lato-Bold, Lato;
    font-weight: bold;
    border-radius: 2px;
    padding: 0 5px;
    box-sizing: border-box;
    position: relative;
    top: -2px;
  }
  .detail-table {
    .highlight-row {
      background: @warning-color-2;
    }
  }
  .confirm-msg {
    line-height: 22px;
    color: @text-normal-color;
  }
  .show-detail{
    display: inline-block;
  }
  .detailMsg{
    font-size: 12px;
    color: @text-normal-color;
    line-height: 1.5;
    margin-bottom: -5px;
    &.en-lang{
      line-height: 1.2;
    }
  }
  .mult-title{
    margin-bottom:5px;
    margin-top:5px;
  }
  .dialog-detail{
    border-radius: 2px;
    border:solid 1px @line-border-color;
    background:@background-disabled-color;
    margin-bottom:10px;
    position: relative;
    .dialog-detail-scroll{
      max-height:95px;
      ul {
        margin:10px;
        li {
          font-size: 12px;
        }
      }
    }
    .copyBtn{
      position: absolute;
      right:5px;
      top:5px;
    }
    .copy-text {
      position: absolute;
      right: 56px;
      top: 7px;
      font-size: 12px;
      color: @normal-color-1;
      &.error-text {
        color: @error-color-1;
      }
    }
  }
  .el-alert__content {
    width:100%;
  }
  .el-alert {
    padding: 0;
  }
  .list-editor {
    margin-bottom:10px;
    overflow: hidden;
    margin: 10px auto;
    width:calc(~'100% - 20px')!important;
  }
  &.hide-bottom-border-top .el-dialog__footer {
    border-top: none;
  }
  .dialog-footer.is-center-btn button {
    width: calc(~'50% - 6px');
  }
}
</style>
