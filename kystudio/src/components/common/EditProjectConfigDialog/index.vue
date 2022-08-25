<template>
  <el-dialog class="config-edit-modal" width="420px"
    :title="modalTitle"
    :visible="isShow"
    limited-area
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @close="handlerClose">
    <el-form :model="form" :rules="rules" ref="form">
      <el-form-item :label="$t('config')" prop="key">
        <el-input auto-complete="off" :disabled="editType === 'edit'" :placeholder="$t('keyPlaceholder')" @input="value => handlerInput('key', value.trim())" :value="form.key"></el-input>
      </el-form-item>
      <el-form-item :label="$t('value')" prop="value">
        <el-input auto-complete="off" :placeholder="$t('valuePlaceholder')" :type="form.key === 'kylin.source.jdbc.pass' ? 'password' : 'text'" @input="value => handlerInput('value', value)" :value="form.value.trim()"></el-input>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="handlerClose">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" :loading="loadingSubmit" @click="submit">{{$t('kylinLang.common.ok')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'

import vuex from '../../../store'
import store, { types } from './store'
import { handleError } from '../../../util'

vuex.registerModule(['modals', 'EditProjectConfigDialog'], store)

@Component({
  computed: {
    // Store数据注入
    ...mapState('EditProjectConfigDialog', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      originForm: state => state.originForm
    }),
    ...mapState({
      defaultConfigList: state => state.project.defaultConfigList
    }),
    ...mapGetters([
      'currentProjectData',
      'configList'
    ])
  },
  methods: {
    // Store方法注入
    ...mapMutations('EditProjectConfigDialog', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      getProjectConfigList: 'LOAD_CONFIG_BY_PROJEECT',
      updateProjectConfig: 'UPDATE_PROJECT_CONFIG'
    })
  },
  locales: {
    'zh-cn': {
      projectConfigTitle: '{type}自定义项目配置',
      config: '配置项',
      value: '参数值',
      add: '新增',
      edit: '编辑',
      requried: '{type}不能为空',
      hasExist: '该配置已存在',
      keyPlaceholder: '请输入配置项...',
      valuePlaceholder: '请输入参数值...',
      disableConfig: '该配置不支持新增'
    },
    'en': {
      projectConfigTitle: '{type} Custom Project Configuration',
      config: 'Configuration',
      value: 'Value',
      add: 'Add',
      edit: 'Edit',
      requried: '{type} is required',
      hasExist: 'The key is already exists',
      keyPlaceholder: 'Please enter the configuration...',
      valuePlaceholder: 'Please enter the value...',
      disableConfig: 'The configuration does not support add'
    }
  }
})
export default class EditProjectConfigDialog extends Vue {
  loadingSubmit = false
  // Data: el-form表单验证规则
  rules = {
    key: [
      { validator: this.validateConfigKey, trigger: 'blur', required: true }
    ],
    value: [
      { validator: this.validateConfigValue, trigger: 'blur', required: true }
    ]
  }

  // Modal标题
  get modalTitle () {
    return this.$t('projectConfigTitle', {type: this.$t(this.editType)})
  }
  validateConfigKey (rule, value, callback) {
    if (!value) {
      return callback(new Error(this.$t('requried', {type: this.$t('config')})))
    } else if (this.editType === 'add' && this.configList.findIndex(v => v.key === value) !== -1) {
      return callback(new Error(this.$t('hasExist')))
    } else if (this.defaultConfigList.findIndex(v => v === value) !== -1) {
      return callback(new Error(this.$t('disableConfig')))
    } else {
      callback()
    }
  }
  validateConfigValue (rule, value, callback) {
    if (!value) {
      return callback(new Error(this.$t('requried', {type: this.$t('value')})))
    } else {
      callback()
    }
  }
  handlerClose () {
    this.$refs.form.resetFields()
    this.resetModalForm()
    this.hideModal()
  }

  // Action: 修改Form函数
  handlerInput (key, value) {
    this.setModalForm({[key]: value})
  }

  async submit () {
    try {
      // 验证表单
      await this.$refs['form'].validate()
      if ((this.form.key === this.originForm.key) && (this.form.value === this.originForm.value)) {
        this.handlerClose()
        return
      }
      let params = {
        project: this.currentProjectData.name,
        data: {[this.form.key]: this.form.value}
      }
      this.loadingSubmit = true
      await this.updateProjectConfig(params)
      // 成功提示
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
      this.loadingSubmit = false
      this.handlerClose()
      this.getConfigList()
    } catch (e) {
      this.loadingSubmit = false
      // 异常处理
      e && handleError(e)
    }
  }

  async getConfigList () {
    // 获取项目列表
    try {
      let filterData = {
        page_offset: 0,
        page_size: 1,
        exact: true,
        project: this.currentProjectData.name,
        permission: 'ADMINISTRATION'
      }
      await this.getProjectConfigList(filterData)
    } catch (e) {
      handleError(e)
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.group-edit-modal {
  .el-transfer-panel {
    width: 250px;
  }
  .load-more-uers {
    color: @text-title-color;
    font-size: @text-assist-size;
    text-align: left;
    cursor: pointer;
    margin-left: 15px;
    &:hover {
      color: @base-color;
    }
  }
  .option-items {
    white-space: pre;
  }
}
</style>
