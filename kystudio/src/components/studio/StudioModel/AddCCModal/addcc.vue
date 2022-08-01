<template>
   <el-dialog append-to-body limited-area
    :close-on-press-escape="false"
    :close-on-click-modal="false" 
    :title="$t(`kylinLang.model.${editCC ? 'editCC' : 'addCC'}`)" width="480px" :visible="isShow" @close="closeModal()">
      <el-alert :title="$t('editCCTip')" type="warning" v-if="editCC" showIcon :closable="false" />
      <CCEditForm v-if="isShow" @saveSuccess="saveCC" @saveError="saveCCError" ref="ccForm" @resetSubmitLoading="resetLoading" :isPureForm="true" :currentCCForm="currentCCForm" :modelInstance="modelInstance" :isEdited="editCC"/>
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
import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import CCEditForm from '../ComputedColumnForm/ccform.vue'
vuex.registerModule(['modals', 'CCAddModal'], store)
@Component({
  computed: {
    ...mapGetters(['currentSelectedProject']),
    ...mapState('CCAddModal', {
      isShow: state => state.isShow,
      callback: state => state.callback,
      modelInstance: state => state.form.modelInstance,
      currentCCForm: state => state.form.currentCCForm,
      editCC: state => state.editCC
    })
  },
  methods: {
    ...mapActions({
      getModelByModelName: 'LOAD_MODEL_INFO'
    }),
    ...mapMutations('CCAddModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  components: {
    CCEditForm
  },
  locales
})
export default class CCAddModal extends Vue {
  btnLoading = false
  saveCC () {
    this.btnLoading = false
    this.closeModal(true)
  }
  saveCCError () {
    this.btnLoading = false
  }
  closeModal (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
  async submit () {
    this.btnLoading = true
    this.$refs.ccForm.$emit('addCC')
  }
  resetLoading () {
    this.btnLoading = false
  }
}
</script>
<style lang="less">
</style>
