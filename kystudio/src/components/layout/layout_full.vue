<template>
   <div>
    <router-view v-if="!forceLoadRoute"></router-view>
    <!-- 全局级别loading -->
    <kylin-loading v-show="$store.state.config.showLoadingBox"></kylin-loading>
    <!-- 全局级别错误提示框 -->
    <el-dialog class="errMsgBox"
      width="480px"
      limited-area
      status-icon="el-ksd-icon-error_24"
      :before-close="handleClose"
      :title="$t('kylinLang.common.notice')"
      :close-on-click-modal="false"
      :append-to-body="true"
      :visible.sync="showErrorMsgBox"
      :close-on-press-escape="false">
      <el-alert
        type="error"
        :show-background="false"
        :closable="false">
        <span class="error-title" v-html="filterInjectScript($store.state.config.errorMsgBox.msg).replace(/\r\n/g, '<br/><br/>')"></span>
        <a href="javascript:;" @click="toggleDetail" v-if="enableStackTrace === 'true'">{{$t('kylinLang.common.seeDetail')}}  
          <i class="el-icon-arrow-down" v-show="!showDetail"></i>
          <i class="el-icon-arrow-up" v-show="showDetail"></i>
        </a>
        <el-input :rows="4" ref="detailBox" readonly type="textarea" v-show="showDetail" class="ksd-mt-10" v-model="$store.state.config.errorMsgBox.detail"></el-input>
        <div class="ksd-left">
          <el-button size="small" v-clipboard:copy="$store.state.config.errorMsgBox.detail"
          v-clipboard:success="onCopy" v-clipboard:error="onError" plain v-show="showDetail" class="ksd-fleft ksd-mt-10">{{$t('kylinLang.common.copy')}}</el-button>
          <transition name="fade">
            <div class="copy-status-msg" v-show="showCopyStatus && showDetail" ><i class="el-icon-circle-check"></i> <span>{{$t('kylinLang.common.copySuccess')}}</span></div>
          </transition>
        </div>
      </el-alert>    
      <div slot="footer" class="dialog-footer">
        <el-button size="medium" @click="handleClose">{{$t('kylinLang.common.close')}}</el-button>
      </div>
    </el-dialog>
    <Modal />
   </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState } from 'vuex'
import Modal from '../common/Modal/Modal'
import { filterInjectScript } from 'util'
@Component({
  computed: {
    ...mapState({
      showErrorMsgBox: state => state.config.errorMsgBox.isShow,
      enableStackTrace: state => state.system.enableStackTrace
    })
  },
  provide () {
    return {
      forceUpdateRoute: this.forceUpdateRoute
    }
  },
  components: {
    Modal
  },
  locales: {
  }
})
export default class LayoutFull extends Vue {
  forceLoadRoute = false
  showDetail = false
  showCopyStatus = false
  filterInjectScript = filterInjectScript

  @Watch('showErrorMsgBox')
  changeShowType (newVal) {
    // 解决enter提交表单，input框仍可以操作并且成功之后错误弹窗不消失
    document.activeElement.blur()
  }
  // 强制刷新界面，更新数据
  forceUpdateRoute () {
    this.forceLoadRoute = true
    this.$nextTick(() => {
      this.forceLoadRoute = false
    })
  }
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  onCopy () {
    this.showCopyStatus = true
    setTimeout(() => {
      this.showCopyStatus = false
    }, 3000)
  }
  onError () {
    this.$message(this.$t('kylinLang.common.copyfail'))
  }
  handleClose () {
    this.showDetail = false
    this.$store.state.config.errorMsgBox.isShow = false
    this.$refs.detailBox.$el.firstChild.scrollTop = 0
  }
  destroyed () {
    this.showCopyStatus = false
  }
}
</script>

<style lang="less">
@import '../../assets/styles/variables.less';
*{
	margin: 0;
	padding: 0;
}
body{
	-webkit-font-smoothing: antialiased;
	-moz-osx-font-smoothing: grayscale;
}
.lincense-result-box {
  .el-icon-ksd-good_health {
    color: @normal-color-1;
    font-size: 16px;
  }
}
.errMsgBox {
  .el-dialog__body{
    .el-alert{
      padding:0;
    }
    .el-alert__content {
      width:100%;
      padding-right: 0;
      display: inline-block;
    }
    // border-top: 1px solid #2b2d3c;
    // box-shadow: 0 -1px 0 0 #424860;
    position: relative;
    .el-input{
      font-size: 12px;
    }
    .error-title {
      display: inline-block;
      word-break: break-word;
      word-wrap: break-word;
      white-space: -moz-pre-wrap;
      white-space: -hp-pre-wrap;
      white-space: -o-pre-wrap;
      white-space: -pre-wrap;
      white-space: pre;
      white-space: pre-wrap;
      white-space: pre-line;
      font-size: 14px;
      line-height: 22px;
    }
  }
  .copy-status-msg{
    display: inline-block;
    margin-top: 10px;
    margin-left: 10px;
    i{
      color:#28cd6b;
    }
    span{
      font-size: 12px;
    }
  }
  .el-textarea__inner {
    background-color: @table-stripe-color;
    font-size: 12px;
  }
}
</style>
