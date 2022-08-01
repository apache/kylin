<template>
  <!-- 模型重命名 -->
  <el-dialog :title="$t('modelRename')" width="480px" :visible="isShow" limited-area :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="modelEdit" @submit.native.prevent :rules="rules" ref="renameForm" label-width="100px" @keyup.enter.native="submit">
        <el-form-item :label="$t('modelName')" prop="newName">
          <el-input v-focus="isShow" v-model="modelEdit.newName" auto-complete="off" size="medium"></el-input>
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

  vuex.registerModule(['modals', 'ModelRenameModal'], store)

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('ModelRenameModal', {
        isShow: state => state.isShow,
        modelDesc: state => state.form.modelDesc,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        renameModel: 'RENAME_MODEL'
      }),
      ...mapMutations('ModelRenameModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class ModelRenameModal extends Vue {
    btnLoading = false
    modelEdit = {
      newName: ''
    }
    rules = {
      newName: [
        {validator: this.checkName, trigger: 'blur'}
      ]
    }
    @Watch('modelDesc')
    initModelName () {
      this.modelEdit.newName = this.modelDesc.alias
    }
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else if (value.length > 50) {
        callback(new Error(this.$t('kylinLang.common.overLengthTip')))
      } else {
        callback()
      }
    }
    closeModal (isSubmit, name) {
      this.hideModal()
      this.$refs.renameForm.resetFields()
      setTimeout(() => {
        this.callback && this.callback({isSubmit, newName: name})
        this.modelEdit.newName = ''
        this.resetModalForm()
      }, 200)
    }
    submit () {
      this.$refs.renameForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        this.renameModel({model: this.modelDesc.uuid, new_model_name: this.modelEdit.newName, project: this.currentSelectedProject}).then(() => {
          this.btnLoading = false
          kylinMessage(this.$t('updateSuccessful'))
          this.closeModal(true, this.modelEdit.newName)
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
