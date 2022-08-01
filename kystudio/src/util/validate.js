export const USERNAME = 'username'
export const PASSWORD = 'password'
export const CONFIRM_PASSWORD = 'confirm-password'
export const GROUP_NAME = 'group-name'
export const PROJECT_NAME = 'project-name'

// TODO: 在this中解构$t，会造成$t方法中的this为undefined
export default {
  [GROUP_NAME] (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.userGroupNameEmpty')))
    } else if (/^\./.test(value)) { // 不能以.开头
      callback(new Error(this.$t('kylinLang.user.noStartWithPeriod')))
    } else if (/^\s+|\s+$/.test(value)) { // 首尾不能有空字符
      callback(new Error(this.$t('kylinLang.user.noStartWithSpace')))
    } else if (/[\\\\/:*?><"\\|']/g.test(value)) { // 不能有\/:*?"<>|'等非法字符
      callback(new Error(this.$t('kylinLang.user.notOtherChars')))
    } else if (/[^\x00-\xff]/g.test(value)) { //  仅支持英文字符
      callback(new Error(this.$t('kylinLang.user.onlyEnglishChars')))
    } else {
      callback()
    }
  },

  [USERNAME] (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.usernameEmpty')))
    } else if (value.length > 180) {
      callback(new Error(this.$t('kylinLang.user.usernameLength')))
    } else if (/^\./.test(value)) { // 不能以.开头
      callback(new Error(this.$t('kylinLang.user.noStartWithPeriod')))
    } else if (/^\s+|\s+$/.test(value)) { // 首尾不能有空字符
      callback(new Error(this.$t('kylinLang.user.noStartWithSpace')))
    } else if (/[\\\\/:*?><"\\|']/g.test(value)) { // 不能有\/:*?"<>|'等非法字符
      callback(new Error(this.$t('kylinLang.user.notOtherChars')))
    } else if (/[^\x00-\xff]/g.test(value)) { // 仅支持英文字符
      callback(new Error(this.$t('kylinLang.user.onlyEnglishChars')))
    } else {
      callback()
    }
  },

  [PASSWORD] (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.passwordEmpty')))
    } else if (value.length < 8) {
      callback(new Error(this.$t('kylinLang.common.passwordLength')))
    } else if (!/^(?=.*\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:"<>?\[\];',.\/`]).{8,}$/.test(value)) { // eslint-disable-line
      callback(new Error(this.$t('kylinLang.user.tip_password_unsafe')))
    } else {
      callback()
    }
  },

  [CONFIRM_PASSWORD] (rule, value, callback) {
    const isPasswordInvalid = this.form.password && value !== this.form.password
    const isNewPasswordInvalid = this.form.newPassword && value !== this.form.newPassword

    if (!value) {
      callback(new Error(this.$t('kylinLang.common.passwordEmpty')))
    } else if (isPasswordInvalid || isNewPasswordInvalid) {
      callback(new Error(this.$t('kylinLang.common.passwordConfirm')))
    } else {
      callback()
    }
  },

  [PROJECT_NAME] (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.noProject')))
    } else if (!/^\w+$/.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else if (!/^(?![_])\w+$/.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip3')))
    } else if (value.length > 50) {
      callback(new Error(this.$t('kylinLang.common.overLengthTip')))
    } else {
      callback()
    }
  }
}
