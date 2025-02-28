<template>
   <el-dialog
    width="600px"
    :title="$t('dialogType')"
    class="storage_setting_dialog"
    :append-to-body="true"
    :visible="isShow"
    @close="closeModal(false)"
    :close-on-click-modal="false"
    :close-on-press-escape="false">
    <div>
      <el-form
        :model="form"
        @submit.native.prevent
        ref="storageSettingForm"
        @keyup.enter.native="submit">
        <el-form-item
          :label="$t('storageType')"
          prop="storage_type">
          <el-radio-group v-model="form.storage_type">
            <el-radio :label="1">
              {{$t('optionFixed')}}
              <el-tooltip :content="$t('optionFixedTips')">
                <i class="el-ksd-n-icon-info-circle-outlined"></i>
              </el-tooltip>
            </el-radio>
            <el-radio :label="3">
              {{$t('optionFlexiable')}}
              <el-tooltip :content="$t('optionFlexiableTips')">
                <i class="el-ksd-n-icon-info-circle-outlined"></i>
              </el-tooltip>
            </el-radio>
          </el-radio-group>
        </el-form-item>
      </el-form>
    </div>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button
        size="medium"
        @click="closeModal(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button
        :disabled="disableSubmitBtn"
        type="primary"
        size="medium"
        @click="submit"
        :loading="btnLoading">{{$t('kylinLang.common.save')}}</el-button>
    </div>
  </el-dialog>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { handleError, kylinMessage } from 'util/business'
vuex.registerModule(['modals', 'SettingStorageModal'], store)
@Component({
  computed: {
    ...mapGetters(['currentSelectedProject']),
    ...mapState('SettingStorageModal', {
      isShow: state => state.isShow,
      callback: state => state.callback,
      form: state => state.form
    })
  },
  methods: {
    ...mapActions({
      updateModelStorageType: 'UPDATE_STORAGE_TYPE'
    }),
    ...mapMutations('SettingStorageModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  components: {
  },
  locales
})
export default class SettingStorageModal extends Vue {
  btnLoading = false
  originType = ''

  @Watch('isShow')
  onModalShow (val) {
    if (val) {
      this.originType = this.form.storage_type
    } else {
      this.originType = ''
    }
  }

  // 值没改动的时候，不让提交，以防 type 值从 0 变成了 1
  get disableSubmitBtn () {
    return this.originType === this.form.storage_type
  }

  closeModal (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.btnLoading = false
      this.callback && this.callback({ isSubmit })
      this.resetModalForm()
    }, 200)
  }

  submit () {
    this.btnLoading = true
    this.updateModelStorageType({
      model_id: this.form.uuid,
      data: {
        project: this.currentSelectedProject,
        storage_type: this.form.storage_type
      }
    }).then(() => {
      this.btnLoading = false
      kylinMessage(this.$t('updateSuccessful'))
      this.closeModal(true)
    }, (res) => {
      this.btnLoading = false
      res && handleError(res)
    })
  }
}
</script>
<style lang="less">
</style>
