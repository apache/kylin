<template>
  <el-form
    ref="form"
    class="source-gbase-setting"
    label-position="top"
    size="medium"
    :model="form"
    :rules="rules"
  >
    <el-form-item :label="$t('jdbcName')" prop="name">
      <el-input
        v-model="form.name"
        :placeholder="$t('kylinLang.common.pleaseInput')"
        :disabled="form.synced"
      >
      </el-input>
    </el-form-item>
    <el-form-item :label="$t('connectionString')" prop="connectionString">
      <el-input
        v-model="form.connectionString"
        :placeholder="$t('kylinLang.common.pleaseInput')"
        :disabled="form.synced"
      >
      </el-input>
    </el-form-item>
    <el-form-item :label="$t('username')" prop="username">
      <el-input
        name="name"
        v-model="form.username"
        :placeholder="$t('kylinLang.common.pleaseInput')"
        :disabled="form.synced"
        auto-complete="off"
      >
      </el-input>
    </el-form-item>
    <el-form-item :label="$t('password')" prop="password">
      <el-input
        name="pass"
        v-model="form.password"
        :placeholder="$t('kylinLang.common.pleaseInput')"
        :disabled="form.synced"
        type="password"
        auto-complete="new-password"
      >
      </el-input>
    </el-form-item>
    <el-form-item :label="$t('driver')" prop="driver">
      <el-input
        name="driver"
        v-model="form.driver"
        :placeholder="$t('kylinLang.common.pleaseInput')"
        :disabled="form.synced"
      >
      </el-input>
    </el-form-item>
  </el-form>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import locales from './locales'

@Component({
  name: 'SourceAuthorityForm',
  props: {
    sourceType: {
      type: Number
    },
    form: {
      type: Object
    }
  },
  locales
})
export default class SourceAuthorityForm extends Vue {
  get rules () {
    return {
      name: [{ message: this.$t('jdbcNameTips'), trigger: 'blur', required: true }],
      connectionString: [{ message: this.$t('connectionStringTips'), trigger: 'blur', required: true }],
      username: [{ message: this.$t('usernameEmptyTips'), trigger: 'blur', required: true }],
      password: [{ message: this.$t('passwordEmptyTips'), trigger: 'blur', required: true }],
      driver: [{ message: this.$t('driverEmptyTips'), trigger: 'blur', required: true }]
    }
  }
}
</script>
