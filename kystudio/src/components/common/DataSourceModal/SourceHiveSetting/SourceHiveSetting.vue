<template>
  <el-form
    ref="form"
    class="source-hive-setting"
    label-position="top"
    size="medium"
    :model="form"
    :rules="rules">
    <el-row :gutter="10">
      <el-col :span="12" v-if="isFieldVisiable('type')">
        <el-form-item :label="$t('type')" prop="type">
          <el-input
            :value="form.type"
            :placeholder="$t('kylinLang.common.pleaseInput')"
            :disabled="!isEditable"
            @input="value => $emit('input', 'type', value)">
          </el-input>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row :gutter="10">
      <el-col :span="12" v-if="isFieldVisiable('name')">
        <el-form-item :label="$t('name')" prop="name">
          <el-input
            :value="form.name"
            :placeholder="$t('kylinLang.common.pleaseInput')"
            :disabled="!isEditable"
            @input="value => $emit('input', 'name', value)">
          </el-input>
        </el-form-item>
      </el-col>
      <el-col :span="12" v-if="isFieldVisiable('creator')">
        <el-form-item :label="$t('creator')">
          <el-input :value="form.creator" :disabled="true"></el-input>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row :gutter="10">
      <el-col :span="24" v-if="isFieldVisiable('description')">
        <el-form-item :label="$t('description')" prop="description">
          <el-input
            type="textarea"
            :rows="1"
            :value="form.description"
            :disabled="!isEditable"
            @input="value => $emit('input', 'description', value)">
          </el-input>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row :gutter="10" v-if="isFieldVisiable('host') || isFieldVisiable('port')">
      <el-col class="form-item-title" :span="24">{{$t('connection')}}</el-col>
      <el-col :span="12" v-if="isFieldVisiable('host')">
        <el-form-item :label="$t('hiveMetastoreHost')" :class="!isEditable ? 'mb-0' : ''" prop="host">
          <el-input
            :value="form.host"
            :placeholder="$t('kylinLang.common.pleaseInput')"
            :disabled="!isEditable"
            @input="value => $emit('input', 'host', value)">
          </el-input>
        </el-form-item>
      </el-col>
      <el-col :span="12" v-if="isFieldVisiable('port')">
        <el-form-item :label="$t('port')" :class="!isEditable ? 'mb-0' : ''" prop="port">
          <el-input
            :value="form.port"
            :placeholder="$t('kylinLang.common.pleaseInput')"
            :disabled="!isEditable"
            @input="value => $emit('input', 'port', value)">
          </el-input>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row :gutter="10">
      <el-col :span="24" v-if="isFieldVisiable('isAuthentication')">
        <el-form-item :label="$t('authentication')" prop="isAuthentication">
          <el-radio-group :value="form.isAuthentication" @input="value => $emit('input', 'isAuthentication', value)">
            <el-radio :label="false">{{$t('noAuthentication')}}</el-radio>
            <el-radio :label="true">{{$t('masterCredentials')}}</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row class="fieldset" :gutter="10" :class="{ disabled: !form.isAuthentication }" v-if="isFieldVisiable('username') || isFieldVisiable('password')">
      <el-col :span="12" v-if="isFieldVisiable('username')">
        <el-form-item class="mb-0" :label="$t('username')" prop="username">
          <el-input
            :value="form.username"
            :placeholder="$t('inputUserame')"
            :disabled="!form.isAuthentication"
            @input="value => $emit('input', 'username', value)">
          </el-input>
        </el-form-item>
      </el-col>
      <el-col :span="12" v-if="isFieldVisiable('password')">
        <el-form-item class="mb-0" :label="$t('password')" prop="password">
          <el-input
            :value="form.password"
            :placeholder="$t('inputPassword')"
            :disabled="!form.isAuthentication"
            @input="value => $emit('input', 'password', value)">
          </el-input>
        </el-form-item>
      </el-col>
    </el-row>
  </el-form>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { validate, fieldVisiableMaps } from './handler'

@Component({
  props: {
    isEditable: {
      type: Boolean,
      default: true
    },
    editType: {
      type: String
    },
    form: {
      type: Object,
      default: () => ({})
    }
  },
  locales
})
export default class SourceHiveSetting extends Vue {
  get rules () {
    return {
      name: [{ validator: validate['name'].bind(this), trigger: 'blur', required: this.isEditable }],
      host: [{ validator: validate['host'].bind(this), trigger: 'blur', required: this.isEditable }],
      port: [{ validator: validate['port'].bind(this), trigger: 'blur', required: this.isEditable }],
      username: [{ validator: validate['username'].bind(this), trigger: 'blur', required: this.form.isAuthentication }],
      password: [{ validator: validate['password'].bind(this), trigger: 'blur', required: this.form.isAuthentication }]
    }
  }
  isFieldVisiable (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.source-hive-setting {
  padding: 20px;
  .el-form-item {
    margin-bottom: 20px;
  }
  .el-form-item__label,
  .form-item-title {
    color: @text-title-color;
    font-weight: @font-medium;
  }
  .form-item-title {
    margin-bottom: 10px;
  }
  .fieldset {
    background: @aceditor-bg-color;
    padding: 10px;
  }
  .fieldset.disabled * {
    color: @text-disabled-color;
    border-color: @text-secondary-color;
  }
  .mb-0 {
    margin-bottom: 0;
  }
}
</style>
