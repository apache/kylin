<template>
  <!-- 模型数据检查 -->
  <el-dialog class="model-data-check" :title="$t('modelDataCheck')" limited-area width="480px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="checkModelMeta" ref="modelCheckDataForm" :rules="rules" label-width="130px" label-position="top">
        <div class="ky-list-title">数据检查项</div>
        <el-checkbox-group v-model="checkModelMeta.check_options">
          <ul class="ksd-mtb-20">
            <li class="ksd-mb-10"><el-checkbox :label="1">模型上主外键重复</el-checkbox></li>
            <li class="ksd-mb-10"><el-checkbox :label="2">数据倾斜（偏度过高）</el-checkbox></li>
            <li class="ksd-mb-10"><el-checkbox :label="4">字段中存在空值</el-checkbox></li>
          </ul>
        </el-checkbox-group>
        <div class="ky-line"></div>
        <div :class="{'disaable-fault-setting': !wantSetFault}">
          <div class="ky-list-title ksd-mt-20">数据容忍标准 
            <el-switch class="switch-fault"
              active-text="OFF"
              inactive-text="ON"
              v-model="wantSetFault">
            </el-switch>
          </div>
          <div class="ksd-mt-18">
            <el-form-item class="ksd-mb-20" prop="fault_threshold">
            有数据问题超过<el-input size="mini" :disabled="!wantSetFault" style="width:70px;" v-model="checkModelMeta.fault_threshold" class="ksd-mrl-4"></el-input>条时，采取以下方式：
            </el-form-item>
          </div>
          <div>
            <ul>
              <li class="ksd-mb-10"><el-radio :disabled="!wantSetFault" v-model="checkModelMeta.fault_actions" :label="1">构建报错并通知我</el-radio></li>
              <li class="ksd-mb-10"><el-radio :disabled="!wantSetFault" v-model="checkModelMeta.fault_actions" :label="2">继续构建，但通知我</el-radio></li>
            </ul>
          </div>
        </div>
      </el-form>
      <div slot="footer" class="dialog-footer">
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
  import { handleError, kylinMessage } from 'util/business'
  import locales from './locales'
  import store, { types } from './store'
  import { positiveNumberRegex } from '../../../../../config'

  vuex.registerModule(['modals', 'ModelCheckDataModal'], store)

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('ModelCheckDataModal', {
        isShow: state => state.isShow,
        modelDesc: state => state.form.modelDesc,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        checkModelData: 'MODEL_DATA_CHECK'
      }),
      ...mapMutations('ModelCheckDataModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class ModelCheckDataModal extends Vue {
    btnLoading = false
    wantSetFault = false
    checkModelMeta = {
      check_options: [],
      fault_threshold: 0,
      fault_actions: 0
    }
    rules = {
      fault_threshold: [{validator: this.validateFaultNumber, trigger: 'blur'}],
      fault_actions: [{validator: this.validateFaultActions, trigger: 'blur'}]
    }
    validateFaultNumber (rule, value, callback) {
      if (this.checkModelMeta.fault_actions !== '') {
        if (value === '') {
          callback(new Error('请输入'))
        } else if (!positiveNumberRegex.test(value)) {
          callback(new Error('请输入大于0的整数'))
        } else {
          callback()
        }
      } else {
        callback()
      }
    }
    @Watch('wantSetFault')
    initWantSetFault (val) {
      if (!val) {
        this.checkModelMeta.fault_threshold = ''
        this.checkModelMeta.fault_actions = ''
      }
    }
    @Watch('modelDesc')
    initModelName () {
      if (this.modelDesc.data_check_desc) {
        switch (this.modelDesc.data_check_desc.check_options) {
          case 1:
            this.checkModelMeta.check_options = [1]
            break
          case 2:
            this.checkModelMeta.check_options = [2]
            break
          case 3:
            this.checkModelMeta.check_options = [1, 2]
            break
          case 4:
            this.checkModelMeta.check_options = [4]
            break
          case 5:
            this.checkModelMeta.check_options = [1, 4]
            break
          case 6:
            this.checkModelMeta.check_options = [2, 4]
            break
          case 7:
            this.checkModelMeta.check_options = [1, 2, 4]
            break
        }
        this.checkModelMeta.fault_threshold = this.modelDesc.data_check_desc.fault_threshold
        this.checkModelMeta.fault_actions = this.modelDesc.data_check_desc.fault_actions
        this.wantSetFault = !!(this.checkModelMeta.fault_threshold || this.checkModelMeta.fault_actions)
      } else {
        this.checkModelMeta = {
          check_options: [],
          fault_threshold: 0,
          fault_actions: 0
        }
        this.wantSetFault = false
      }
    }
    get checkDataOptionsVal () {
      let x = 0
      this.checkModelMeta.check_options.forEach((o) => {
        x += o
      })
      return x
    }
    closeModal (isSubmit) {
      this.hideModal()
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    submit () {
      this.$refs.modelCheckDataForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        this.checkModelData({
          modelId: this.modelDesc.uuid,
          data: {
            project: this.currentSelectedProject,
            check_options: this.checkDataOptionsVal,
            fault_threshold: +this.checkModelMeta.fault_threshold,
            fault_actions: this.checkModelMeta.fault_actions
          }
        }).then(() => {
          this.btnLoading = false
          kylinMessage(this.$t('kylinLang.common.saveSuccess'))
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
@import '../../../../../assets/styles/variables.less';
.model-data-check {
  .disaable-fault-setting {
    color:@text-disabled-color;
  }
  .switch-fault {
    transform:scale(0.91);
  }
}
</style>
