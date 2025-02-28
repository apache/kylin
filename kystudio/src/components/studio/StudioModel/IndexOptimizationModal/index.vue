<template>
   <el-dialog
    class="index-optimization-modal"
    append-to-body
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    :title="$t('indexOptimization')"
    width="600px"
    :visible="isShow"
    @close="closeModal(false)">
      <div>
        <p>{{$t('content1')}}<a href="javascript:;">{{$t('content2')}}</a>{{$t('content3')}}<a href="javascript:;" @click="downloadTemplate">{{$t('content4')}}</a>{{$t('content5')}}</p>
        <div class="aceEditorBox">
          <AceEditor
            v-if="isShow"
            :lang="'json'"
            :placeholder="'123'"
            class="text-input"
            ref="editorRef"
            :value="codeText"
            @input="handleInputText" />
        </div>
        <div class="error-msg" v-if="hasChecked && errMsg">{{errMsg}}</div>
        <form name="download" class="downloadTemplate" :action="actionUrl" target="_blank" method="get">
        </form>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button @click="closeModal(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="submit" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
</template>
<script>
import Vue from 'vue'
import AceEditor from 'vue2-ace-editor'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { handleError, kylinMessage } from 'util/business'
import { apiUrl } from '../../../../config'
vuex.registerModule(['modals', 'IndexOptimizationModal'], store)
@Component({
  computed: {
    ...mapGetters(['currentSelectedProject']),
    ...mapState('IndexOptimizationModal', {
      isShow: state => state.isShow,
      callback: state => state.callback,
      modelInstance: state => state.modelInstance
    })
  },
  methods: {
    ...mapActions({
      saveOptimizeLayoutData: 'SAVE_OPTIMIZE_LAYOUT_DATA',
      downloadOptimizeLayoutTemplate: 'DOWNLOAD_OPTIMIZE_LAYOUT_TEMPLATE'
    }),
    ...mapMutations('IndexOptimizationModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  components: {
    AceEditor
  },
  locales
})
export default class IndexOptimizationModal extends Vue {
  btnLoading = false
  codeText = ''
  jsonResut = {}
  hasChecked = false
  errMsg = ''

  get actionUrl () {
    return `${apiUrl}models/optimize_layout_data_template`
  }

  downloadTemplate () {
    if (this.$store.state.config.platform === 'iframe') { // 云上暂不支持流数据，暂不处理
      try {
        this.downloadOptimizeLayoutTemplate().then(res => {
          const file = (res && res.headers && res.headers.map && res.headers.map['content-disposition'] && res.headers.map['content-disposition'][0]) || ''
          const fileNameStrArr = file.split(';')
          const fileNameArr = fileNameStrArr[1] ? fileNameStrArr[1].split('=') : []
          if (res && res.status === 200 && res.body) {
            // 动态从header 里取文件名，如果没取到，就前端自己配
            const fileName = fileNameArr[1].trim().replace(/"/g, '') || 'optimize_layout_data_template.json'
            const data = JSON.stringify(res.body, null, 2)
            const blob = new Blob([data], { type: 'application/json;charset=utf-8' })
            if (window.navigator.msSaveOrOpenBlob) {
              navigator.msSaveBlob(blob, fileName)
            } else {
              const link = document.createElement('a')
              link.href = window.URL.createObjectURL(blob)
              link.download = fileName
              link.click()
              window.URL.revokeObjectURL(link.href)
            }
          }
        })
      } catch (e) {
        console.log(e)
      }
    } else {
      this.$nextTick(() => {
        this.$el.querySelectorAll('.downloadTemplate').length && this.$el.querySelectorAll('.downloadTemplate')[0].submit()
      })
    }
  }

  handleValidate (text) {
    if (!text || !text.trim()) {
      this.errMsg = this.$t('notEmpty')
    } else {
      try {
        this.jsonResut = JSON.parse(text)
        this.errMsg = ''
      } catch (e) {
        this.jsonResut = {}
        this.errMsg = this.$t('formatErrMsg')
        console.log(e)
      }
    }
  }

  handleInputText (text) {
    this.codeText = text
    this.handleValidate(text)
  }

  handleResetInfo () {
    this.codeText = ''
    this.btnLoading = false
    this.jsonResut = {}
    this.errMsg = ''
    this.hasChecked = false
  }

  closeModal (isSubmit) {
    this.handleResetInfo()
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback({ isSubmit })
      this.resetModalForm()
    }, 200)
  }

  async submit () {
    // 标记已经点过提交按钮了
    this.hasChecked = true
    this.handleValidate(this.codeText)
    if (this.errMsg) {
      return false
    }
    this.btnLoading = true
    try {
      // 接口参数中的数据来自文本输入框中的内容 json 化
      await this.saveOptimizeLayoutData({
        project: this.currentSelectedProject,
        model_id: this.modelInstance.uuid,
        ...this.jsonResut
      })
      this.btnLoading = false
      kylinMessage(this.$t('submitSuccess'))
      this.closeModal(true)
    } catch (res) {
      this.btnLoading = false
      res && handleError(res)
    }
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.index-optimization-modal{
  .aceEditorBox{
    height: 300px;
    margin-top:20px;
  }
  .text-input {
    background-color: @ke-background-color-secondary;
    .ace_gutter {
      background-color: @ke-background-color-secondary;
    }
    .ace_placeholder {
      font-weight: 400;
      font-size: 12px;
      line-height: 16px;
      color: @text-placeholder-color;
      padding: 0 6px;
      white-space: pre-wrap;
    }
  }
}

</style>
