<template>
  <!-- 模型重命名 -->
  <el-dialog :title="$t('modelClone')" width="480px" :visible="isShow" limited-area :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="modelClone" @submit.native.prevent :rules="rules" ref="cloneForm" label-width="100px" @keyup.enter.native="submit">
        <el-form-item :label="$t('modelName')" prop="newName">
          <el-input v-focus="isShow" v-model="modelClone.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="btnLoading" size="medium" @click="submit">{{$t('kylinLang.common.save')}}</el-button>
      </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../../store'
  import { NamedRegex } from 'config'
  import { handleError, kylinMessage } from 'util/business'
  import locales from './locales'
  import store, { types } from './store'

  vuex.registerModule(['modals', 'ModelCloneModal'], store)

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('ModelCloneModal', {
        isShow: state => state.isShow,
        modelDesc: state => state.form.modelDesc,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        cloneModel: 'CLONE_MODEL'
      }),
      ...mapMutations('ModelCloneModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class ModelCloneModal extends Vue {
    btnLoading = false
    modelClone = {
      newName: ''
    }
    rules = {
      newName: [
        {validator: this.checkName, trigger: 'blur'}
      ]
    }
    @Watch('modelDesc')
    initModelName () {
      this.modelClone.newName = this.modelDesc.alias + '_clone'
    }
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    }
    closeModal (isSubmit) {
      this.hideModal()
      this.modelClone.newName = ''
      this.$refs.cloneForm.resetFields()
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    async submit () {
      this.$refs.cloneForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        this.cloneModel({model: this.modelDesc.uuid, new_model_name: this.modelClone.newName, project: this.currentSelectedProject}).then(() => {
          this.btnLoading = false
          kylinMessage(this.$t('cloneSuccessful'))
          this.closeModal(true)
        }, (res) => {
          this.btnLoading = false
          res && handleError(res)
        })
      })
    }
  }
</script>
<style lang="less">
  
</style>
