<template>
   <el-dialog class="js_add-model" :title="$t('kylinLang.model.addModel')" limited-area width="480px" :visible="isShow" v-if="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="closeModal()">
      <el-form :model="createModelMeta" @submit.native.prevent :rules="rules" ref="addModelForm" label-width="130px" label-position="top">
        <el-form-item prop="newName">
          <span slot="label">{{$t('kylinLang.model.modelName')}}<common-tip :content="$t('kylinLang.model.modelNameTips')"><i class="el-ksd-icon-more_info_16 ksd-ml-5"></i></common-tip></span>
          <el-input name="modelName" :placeholder="$t('kylinLang.common.nameFormatValidTip')" @keyup.enter.native="submit" v-model="createModelMeta.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.model.modelDesc')" prop="modelDesc">
         <el-input
            type="textarea"
            :rows="2"
            :placeholder="$t('pleaseInputDesc')"
            v-model="createModelMeta.modelDesc"
            @keydown.native="handleKeyDown"
          >
          </el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="submit" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import vuex from '../../../../../store'
import { NamedRegex } from 'config'
import { handleError, handleSuccess } from 'util/business'
import locales from './locales'
import store, { types } from './store'
vuex.registerModule(['modals', 'ModelAddModal'], store)
@Component({
  computed: {
    ...mapGetters(['currentSelectedProject', 'isGuideMode']),
    ...mapState('ModelAddModal', {
      isShow: state => state.isShow,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapActions({
      modelNameValidate: 'NEW_MODEL_NAME_VALIDATE'
    }),
    ...mapMutations('ModelAddModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  locales
})
export default class ModelAddModal extends Vue {
  btnLoading = false;
  createModelMeta = {
    newName: '',
    modelDesc: ''
  }
  rules = {
    newName: [{ required: true, validator: this.checkName, trigger: 'blur' }]
  }
  checkName (rule, value, callback) {
    if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else if (value.length > 50) {
      callback(new Error(this.$t('kylinLang.common.overLengthTip')))
    } else {
      this.modelNameValidate({model_name: value, project: this.currentSelectedProject}).then((response) => {
        handleSuccess(response, (data) => {
          if (data) {
            callback(new Error(this.$t('kylinLang.model.sameModelName')))
          } else {
            callback()
          }
        })
      }, (res) => {
        handleError(res)
      })
    }
  }
  closeModal (isSubmit) {
    this.hideModal()
    this.createModelMeta.newName = ''
    this.$refs.addModelForm.resetFields()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
  async submit () {
    this.btnLoading = true
    this.$refs.addModelForm.validate(valid => {
      if (valid) {
        var modelName = this.createModelMeta.newName
        this.$router.push({name: 'ModelEdit', params: { modelName: modelName, action: 'add', modelDesc: this.createModelMeta.modelDesc }})
        this.closeModal(true)
      }
      this.btnLoading = false
    })
  }
  handleKeyDown (e) {
    const { ctrlKey, metaKey, keyCode } = e
    if (ctrlKey && keyCode === 13 || metaKey && keyCode === 13) {
      this.createModelMeta.modelDesc += '\n'
    } else if (keyCode === 13) {
      e.preventDefault()
      this.submit()
    }
  }
}
</script>
<style lang="less">
</style>
