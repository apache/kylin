import { editTypes } from '../handler'

export const fieldVisiableMaps = {
  [editTypes.CONFIG_SOURCE]: ['name', 'creator', 'description', 'host', 'port', 'isAuthentication', 'username', 'password'],
  [editTypes.VIEW_SOURCE]: ['type', 'name', 'host', 'port']
}

export const validate = {
  name (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!/^\w+$/.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  },
  host (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.test(value)) {
      callback(new Error(this.$t('hostValidTip')))
    } else {
      callback()
    }
  },
  port (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!/^[0-9]+$/.test(value)) {
      callback(new Error(this.$t('portValidTip')))
    } else {
      callback()
    }
  },
  username (rule, value, callback) {
    if (!this.form.isAuthentication) {
      callback()
    } else if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!/^\w+$/.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  },
  password (rule, value, callback) {
    if (!this.form.isAuthentication) {
      callback()
    } else if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else {
      callback()
    }
  }
}
