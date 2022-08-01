<template>
  <el-dialog class="project-edit-modal" :width="modalWidth" limited-area
    :title="$t(modalTitle)"
    :visible="isShow"
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @close="isShow && closeHandler(false)">
    
    <el-form :model="form" @submit.native.prevent label-position="top" :rules="rules" ref="form" v-if="isFormShow" label-width="110px">
      <!-- 去除了智能模式的，直接显示下面这个div 即可 begin -->
      <div>
        <div v-if="$store.state.system.resourceGroupEnabled==='true '">
          <el-alert
            :title="$t('resourceGroupTips')"
            type="warning"
            :closable="false">
          </el-alert>
          <el-alert
            :title="$t('aiProjectTips')"
            :show-background="false"
            :closable="false"
            class="ksd-mb-20">
          </el-alert>
        </div>
        <el-alert
          v-else
          type="info"
          :title="$t('aiProjectTips')"
          :show-background="false"
          :closable="false"
          show-icon
          class="ksd-mb-20">
        </el-alert>
      </div>
      <!-- 去除了智能模式的，直接显示下面这个div 即可 end -->
      <!-- 表单：项目名 -->
      <el-form-item :label="$t('projectName')" prop="name" v-if="isFieldShow('name')" class="js_projectname">
        <el-input v-guide.addProjectInput
          :disabled="editType !== 'new'"
          auto-complete="off"
          :value="form.name"
          :placeholder="$t('kylinLang.common.projectPlaceholder')"
          @input="value => inputHandler('name', value)">
        </el-input>
      </el-form-item>
      <!-- 表单：项目描述 -->
      <el-form-item :label="$t('description')" prop="description" v-if="isFieldShow('description')" class="js_project_desc">
        <el-input v-guide.addProjectDesc
          type="textarea"
          auto-complete="off"
          :value="form.description"
          :disabled="editType !== 'new'"
          :placeholder="$t('projectDescription')"
          @input="value => inputHandler('description', value)">
        </el-input>
      </el-form-item>
      <!-- 表单：项目配置 -->
      <div class="project-config" v-if="isFieldShow('configuration')">
        <label class="el-form-item__label">{{$t('projectConfig')}}</label>
        <div>
          <el-button
            plain
            class="add-property"
            size="small"
            type="primary"
            icon="el-icon-ksd-add_2"
            @click="addProperty">
            {{$t('property')}}
          </el-button>
        </div>
        <!-- 表单：配置项键 -->
        <el-row :gutter="20" v-for="(property, index) in form.properties" :key="index">
          <el-col :span="11">
            <el-form-item prop="properties.key">
              <el-input
                size="small"
                placeholder="Key"
                :value="property.key"
                :disabled="isPropertyDisabled(index)"
                @input="value => propertyHandler('input', 'key', index, value)"
                @blur="propertyHandler('blur', 'key', index)">
              </el-input>
            </el-form-item>
          </el-col>
          <!-- 表单：配置项值 -->
          <el-col :span="11">
            <el-form-item prop="properties.value">
              <el-input
                size="small"
                placeholder="Value"
                :value="property.value"
                :disabled="isPropertyDisabled(index)"
                @input="value => propertyHandler('input', 'value', index, value)"
                @blur="propertyHandler('blur', 'value', index)">
              </el-input>
            </el-form-item>
          </el-col>
          <!-- 表单：配置项删除按钮 -->
          <el-col :span="2">
            <el-button
              size="small"
              icon="el-icon-delete"
              @click.prevent="removeProperty(index)"
              v-if="!isPropertyDisabled(index)">
            </el-button>
          </el-col>
        </el-row>
      </div>
    </el-form>

    <div slot="footer" class="dialog-footer ky-no-br-space" v-if="isFormShow">
      <el-button size="medium" @click="closeHandler(false)">{{$t('cancel')}}</el-button>
      <el-button type="primary" size="medium" :loading="saveLoading" @click="submit" v-guide.saveProjectBtn class="js_addproject_submit">{{$t('kylinLang.common.ok')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { fieldVisiableMaps, titleMaps, getSubmitData, disabledProperties } from './handler'
import { validate, validateTypes, handleError, fromObjToArr, handleSuccessAsync } from '../../../util'

const { PROJECT_NAME } = validateTypes

vuex.registerModule(['modals', 'ProjectEditModal'], store)

@Component({
  computed: {
    ...mapState({
      defaultProperties: state => fromObjToArr(state.config.defaultConfig.project)
    }),
    // Store数据注入
    ...mapState('ProjectEditModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback
    }),
    ...mapGetters([
      'isSmartModeEnabled'
    ])
  },
  methods: {
    // Store方法注入
    ...mapMutations('ProjectEditModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      saveProject: 'SAVE_PROJECT',
      loadConfig: 'LOAD_DEFAULT_CONFIG'
    })
  },
  locales
})
export default class ProjectEditModal extends Vue {
  saveLoading = false
  // Data: 用来销毁el-form
  isFormShow = false
  // Data: el-form表单验证规则
  rules = {
    name: [{
      validator: this.validate(PROJECT_NAME), trigger: 'blur', required: true
    }]
  }
  // Computed: Modal宽度
  get modalWidth () {
    return '660px'
  }
  // Computed: Modal标题
  get modalTitle () {
    return titleMaps[this.editType]
  }
  // Computed Method: 计算每个Form的field是否显示
  isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }
  // Computed Method: 计算是否属性是被禁止修改
  isPropertyDisabled (propertyIdx) {
    const properties = JSON.parse(JSON.stringify(this.form.properties))
    const property = properties[propertyIdx]

    return !property.isNew && disabledProperties.includes(property.key)
  }
  // Watcher: 监视销毁上一次elForm
  @Watch('isShow')
  async onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 300)
    }
  }
  // Action: 模态框关闭函数
  closeHandler (isSubmit) {
    this.hideModal()
    this.saveLoading = false
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }
  // Action: 修改Form函数
  inputHandler (key, value) {
    this.setModalForm({[key]: value})
  }
  // Action: 修改Form中的properties
  propertyHandler (action, type, propertyIdx, value) {
    const properties = JSON.parse(JSON.stringify(this.form.properties))
    let shouldUpdate = false

    const property = properties[propertyIdx]

    if (action === 'input') {
      property[type] = value
      property.isNew = true
      shouldUpdate = true
    }
    if (action === 'blur' && property[type] !== property[type].trim()) {
      property[type] = property[type].trim()
      property.isNew = true
      shouldUpdate = true
    }
    shouldUpdate && this.setModalForm({ properties })
  }
  // Action: 新添加一个project property
  addProperty () {
    const properties = JSON.parse(JSON.stringify(this.form.properties))

    properties.push({key: '', value: '', isNew: true})
    this.setModalForm({ properties })
  }
  // Action: 删除一个project property
  removeProperty (propertyIdx) {
    const properties = JSON.parse(JSON.stringify(this.form.properties))

    properties.splice(propertyIdx, 1)
    this.setModalForm({ properties })
  }
  // Action: Form递交函数
  async submit () {
    try {
      const isInvaild = this.validateProperties()

      if (!isInvaild) {
        this.saveLoading = true
        let res
        // 获取Form格式化后的递交数据
        const data = getSubmitData(this)
        // 验证表单
        await this.$refs['form'].validate()
        // 针对不同的模式，发送不同的请求
        switch (this.editType) {
          case 'new':
            res = await handleSuccessAsync(await this.saveProject(data))
            break
          case 'edit':
            res = await handleSuccessAsync(await this.updateProject(data))
            break
        }
        // TODO HA 模式时 post 等接口需要等待同步完去刷新列表
        // await handleWaiting()
        this.$message({
          type: 'success',
          message: this.$t('saveSuccessful')
        })
        this.saveLoading = false
        this.closeHandler(res)
      } else {
        this.$message.error(isInvaild)
      }
    } catch (e) {
      this.saveLoading = false
      // 异常处理
      e && handleError(e)
    }
  }
  // Helper: 给el-form用的验证函数
  validate (type) {
    // TODO: 这里的this是vue的实例，而data却是class的实例
    return validate[type].bind(this)
  }
  // Helper: project属性验证
  validateProperties () {
    const duplicateProperty = this.form.properties.find(property => disabledProperties.includes(property.key) && property.isNew)
    const hasEmptyKeyProperty = this.form.properties.some(property => !property.key)
    const emptyValueProperty = this.form.properties.find(property => !property.value)

    if (duplicateProperty) {
      return this.$t('propertyCannotChange', { keyName: duplicateProperty.key })
    } else if (hasEmptyKeyProperty) {
      return this.$t('propertyEmptyKey')
    } else if (emptyValueProperty) {
      return this.$t('propertyEmptyValue')
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.project-edit-modal {
  // .el-form-item {
  //   margin-bottom: 20px;
  // }
  .project-config .add-property {
    margin: 3px 0 10px 0;
  }
  .project-config .el-input__inner {
    height: 32px;
    line-height: 32px;
  }
  .project-config .el-col {
    height: 32px;
    margin-bottom: 10px;
  }
  .project-type {
    width: 268px;
    height: 200px;
    * {
      transition: all .15s;
    }
    &.active {
      .project-type-button {
        border-color: @base-color;
        background: @base-color;
        box-shadow: 2px 2px 4px 0 @line-border-color;
        *,
        &:hover * {
          color: @fff;
        }
      }
      .project-type-title {
        color: @base-color;
      }
      .project-type-desc {
        color: @text-normal-color;
      }
    }
    .project-type-button {
      width: 90px;
      height: 90px;
      margin: 0 auto 10px auto;
      border: 1px solid @text-secondary-color;
      border-radius: 6px;
      background: @fff;
      cursor: pointer;
      * {
        color: @text-disabled-color;
      }
      &:hover {
        border-color: @base-color;
        * {
          color: @base-color;
        }
      }
    }
    .project-type-title {
      line-height: 1;
      text-align: center;
      white-space: nowrap;
      color: @text-normal-color;
      margin-bottom: 8px;
    }
    .project-type-icon {
      font-size: 55px;
      color: @base-color;
      line-height: 90px;
      text-align: center;
    }
    .project-type-desc {
      color: @text-disabled-color;
      word-break: break-word;
      hyphens: auto;
    }
  }
  .el-form-item__label {
    font-weight: @font-medium;
  }
}
</style>
