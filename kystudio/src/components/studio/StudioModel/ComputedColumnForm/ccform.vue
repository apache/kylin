<template>
<div class="cc-form-box">
  <span class="ky-close ksd-fright ksd-mt-10 ksd-mr-10" @click="cancelCC" v-if="!isPureForm"><i class="el-icon-ksd-close"></i></span>
  <el-form :model="ccObject" :class="{'editCC': !isEdit, 'cc-block': !isPureForm, 'is-datatype-error': errorMsg}" label-position="top" :rules="ccRules" ref="ccForm">
    <el-form-item prop="columnName" class="ksd-mb-10 ksd-mt-10">
      <span slot="label">{{$t('columnName')}}: <el-tooltip :content="ccObject.columnName" effect="dark" placement="top"><span v-show="!isEdit" class="column-name">{{ccObject.columnName}}</span></el-tooltip></span>
      <el-input class="measures-width" size="medium" @keydown.enter.native.prevent v-model="ccObject.columnName" :disabled="isEdited" v-if="isEdit" :placeholder="$t('kylinLang.common.nameFormatValidTip')" @blur="upperCaseCCName"></el-input>
    </el-form-item>
    <el-form-item prop="datatype" class="ksd-mb-10" v-if="!isEdit">
      <span slot="label">{{$t('returnType')}}<span>: {{ccObject.datatype}}</span></span>
    </el-form-item>
    <el-form-item :label="$t('expression')" prop="expression" class="ksd-mb-10">
      <span slot="label">{{$t('kylinLang.dataSource.expression')}} <common-tip :content="$t('conditionExpress')" ><i class="el-icon-ksd-what"></i></common-tip></span>
      <div class="expression-tips">
        <p><span class="point">•</span><span class="text" v-html="$t('expressionTip1')"></span></p>
        <p><span class="point">•</span><span class="text">{{$t('expressionTip2')}}</span></p>
      </div>
      <kylin-editor ref="ccSql" height="100" lang="sql" theme="chrome" v-model="ccObject.expression" :read-only="source === 'createMeasure' ? isEditMeasureCC : !isEdit" @input="changeExpression">
      </kylin-editor>
    </el-form-item>
    <div class="ky-sql-check-msg" v-if="errorMsg">
      <div class="ky-error-title">{{$t('errorMsg')}}</div>
      <div class="ky-error-content">{{errorMsg}}</div>
    </div>
    <div class="btn-group clearfix ksd-mt-6">
      <el-button type="primary" size="small" @click="addCC" class="ksd-fright ksd-ml-10" v-if="isEdit && !isPureForm" :loading="checkBtnLoading">
        {{$t('kylinLang.common.save')}}
      </el-button>
      <el-button size="small" plain @click="cancelCC" class="ksd-fright" v-if="!isPureForm && !hideCancel">{{$t('kylinLang.common.cancel')}}</el-button>
    </div>
  </el-form>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleError, kylinMessage, handleSuccess } from 'util/business'
import { objectClone } from 'util/index'
import { modelErrorMsg } from '../ModelEdit/config'
import { NamedRegex } from 'config'
@Component({
  props: ['isShow', 'ccDesc', 'modelInstance', 'isPureForm', 'currentCCForm', 'isEdited', 'hideCancel', 'isEditMeasureCC', 'source'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    // 后台接口请求
    ...mapActions({
      getUsedCC: 'GET_COMPUTED_COLUMNS',
      checkCC: 'CHECK_COMPUTED_EXPRESSION'
    })
  },
  locales: {
    'en': {
      conditionExpress: 'Note that select one column should contain its table name(or alias table name)',
      numberNameFormatValidTip: 'Invalid computed column name',
      sameName: 'The name of computed column can\'t be duplicated within the same model',
      columnName: 'Column Name',
      name: 'Name',
      errorMsg: 'Error Message: ',
      expression: 'Expression',
      returnType: 'Return Type',
      paramValue: 'Param Value',
      nameReuse: 'The measure name is reused',
      requiredCCName: 'The column name is required',
      requiredReturnType: 'The return type is required',
      requiredExpress: 'The expression is required',
      onlyStartLetters: 'Only supports starting with a letter',
      deleteCCTip: 'After saving, as the expression was modified, measure(s) [{names}] would be unavailable and deleted. Do you want to continue?',
      expressionTip1: 'Within a project, the computed columns with same name must use <br/>identical expression',
      expressionTip2: 'When referencing a column, please follow the format as TABLE_ALIAS.COLUMN (TABLE_ALIAS is the table name defined in the model)',
      sameExpression: 'This expression has already been used by other computed columns in this model'
    }
  }
})
export default class CCForm extends Vue {
  checkBtnLoading = false
  ccRules = {
    columnName: [
      { required: true, message: this.$t('requiredCCName'), trigger: 'blur' },
      { validator: this.checkCCName, trigger: 'blur' }
    ],
    expression: [{ validator: this.checkExpression, trigger: 'change' }]
  }
  // required: true, message: this.$t('requiredExpress')
  ccMeta = JSON.stringify({
    columnName: '',
    datatype: '',
    expression: ''
  })
  ccObject = JSON.parse(this.ccMeta)
  activeCCIndex = null
  isEdit = true
  ccGroups = []
  integerType = ['bigint', 'int', 'integer', 'smallint', 'tinyint']
  floatType = ['decimal', 'double', 'float']
  otherType = ['binary', 'boolean', 'char', 'date', 'string', 'timestamp', 'varchar']

  checkSameExpression (value) {
    const { computed_columns } = this.modelInstance
    return computed_columns.filter(it => it.expression === value).length > 0
  }

  checkCCName (rule, value, callback) {
    if (!NamedRegex.test(value.toUpperCase())) {
      return callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    }
    if (/^\d|^_+/.test(value)) {
      return callback(new Error(this.$t('onlyStartLetters')))
    }
    if (!this.isEdited && !this.modelInstance.checkSameCCName(value.toUpperCase())) {
      return callback(new Error(this.$t('sameName')))
    }
    callback()
  }
  // 校验cc表达式是否存在相同或未填写情况
  checkExpression (rule, value, callback) {
    if (!value) {
      callback(this.$t('requiredExpress'))
    } else if (this.checkSameExpression(value)) {
      callback(this.$t('sameExpression'))
    } else {
      callback()
    }
  }
  upperCaseCCName () {
    this.ccObject.columnName = this.ccObject.columnName.toLocaleUpperCase()
  }
  newCC () {
    this.isEdit = true
    this.$refs['ccForm'].resetFields()
  }
  delCC () {
    this.modelInstance.delCC(this.ccObject).then(() => {
      this.$emit('delSuccess', this.ccObject)
      this.ccObject = JSON.parse(this.ccMeta)
    })
  }
  cancelCC () {
    this.$emit('delSuccess', this.ccObject)
  }
  errorMsg = ''
  checkRemoteCC (cb) {
    this.modelInstance.generateMetadata().then((data) => {
      // 组装带cc的模型功校验cc接口使用
      let resData = objectClone(data)
      if (!this.isEdited) {
        let ccMeta = this.modelInstance.generateCCMeta(this.ccObject)
        ccMeta.datatype = 'any' // 默认传给后台的数据类型
        resData.computed_columns.push(ccMeta)
      } else {
        const index = resData.computed_columns.findIndex(it => it.guid === this.ccObject.guid)
        resData.computed_columns[index] = {...resData.computed_columns[index], ...this.ccObject, datatype: 'any'}
      }
      this.checkBtnLoading = true
      this.checkCC({
        model_desc: resData,
        project: this.currentSelectedProject,
        cc_in_check: this.modelInstance.getFactTable().alias + '.' + this.ccObject.columnName,
        is_seeking_expr_advice: false
      }).then((res) => {
        this.checkBtnLoading = false
        this.errorMsg = ''
        handleSuccess(res, (data) => {
          const { computed_column, remove_measure } = data
          if (remove_measure && remove_measure.length) {
            // 判断更改cc表达式是否导致相关度量被删除
            this.$confirm(this.$t('deleteCCTip', {names: `${remove_measure.join(',')}`}), this.$t('kylinLang.common.tip'), {
              confirmButtonText: this.$t('kylinLang.common.ok'),
              cancelButtonText: this.$t('kylinLang.common.cancel'),
              type: 'warning'
            }).then(() => {
              remove_measure.forEach(name => {
                const index = this.modelInstance.all_measures.findIndex(item => item.name === name)
                const idx = this.modelInstance.simplified_measures.findIndex(it => it.name === name)
                if (index >= 0) {
                  this.modelInstance.all_measures.splice(index, 1)
                }
                if (idx >= 0) {
                  this.modelInstance.simplified_measures.splice(idx, 1)
                }
              })
              cb && cb(computed_column)
            }).catch(() => {
              this.$emit('resetSubmitLoading')
            })
          } else {
            cb && cb(computed_column)
          }
        })
      }, (res) => {
        this.$emit('saveError')
        this.checkBtnLoading = false
        handleError(res, (data, code, stasut, msg) => {
          this.errorMsg = msg
        })
      })
    }).catch((err) => {
      kylinMessage(this.$t(modelErrorMsg[err.errorKey], {aloneCount: err.aloneCount}), { type: 'warning' })
      this.$emit('saveError')
    })
  }
  addCC () {
    return new Promise((resolve, reject) => {
      this.$refs['ccForm'].validate((valid) => {
        if (valid) {
          let factTable = this.modelInstance.getFactTable()
          if (!factTable) {
            this.$emit('saveError')
            kylinMessage(this.$t(modelErrorMsg['noFact']), { type: 'warning' })
            reject()
            return
          }
          if (this.isEdited) {
            this.checkRemoteCC((data) => {
              this.ccObject.datatype = data.datatype
              const alias = factTable.alias
              this.modelInstance.editCC(this.ccObject).then(cc => {
                // 更改维度中引用该 cc 的 datatype
                for (let i = 0; i <= this.modelInstance.all_measures.length - 1; i++) {
                  const names = this.modelInstance.all_measures[i].parameter_value.map(it => it.value)
                  if (names.includes(`${alias}.${cc.columnName}`)) {
                    this.modelInstance.all_measures[i].return_type = cc.datatype
                    break
                  }
                }
                // 更改维度中引用该 cc 的 datatype
                const index = this.modelInstance._mount.dimensions.findIndex(it => it.name === cc.columnName)
                index >= 0 && this.modelInstance._mount.dimensions.splice(index, 1, {...this.modelInstance._mount.dimensions[index], datatype: cc.datatype})
                this.$emit('saveSuccess', cc)
                this.isEdit = false
                resolve()
              }, () => {
                this.$emit('saveError')
                reject()
              })
            })
          } else {
            this.checkRemoteCC((data) => {
              this.ccObject = data
              this.ccObject.table_guid = factTable.guid
              // 新增 measure 中创建 cc 暂不保存 cc
              if (this.source && this.source === 'createMeasure') {
                this.$emit('checkSuccess', this.ccObject)
                // this.isEdit = false
                resolve()
                return
              }
              if (this.ccObject.guid) {
                this.modelInstance.editCC(this.ccObject).then((cc) => {
                  this.$emit('saveSuccess', cc)
                  this.isEdit = false
                  resolve()
                }, () => {
                  this.$emit('saveError')
                  kylinMessage(this.$t('sameName'), { type: 'warning' })
                  reject()
                })
              } else {
                this.modelInstance.addCC(this.ccObject).then((cc) => {
                  this.$emit('saveSuccess', cc)
                  this.isEdit = false
                  resolve()
                }, () => {
                  this.$emit('saveError')
                  kylinMessage(this.$t('sameName'), { type: 'warning' })
                  reject()
                })
              }
            })
          }
        } else {
          this.$emit('saveError')
          reject()
        }
      })
    })
  }
  editCC () {
    this.isEdit = true
  }
  changeExpression () {
    this.errorMsg && (this.errorMsg = '')
    if (!this.isEditMeasureCC) {
      this.isEdit = true
    }
  }
  setAutoCompleteData (data) {
    let ad = data.map((col) => {
      return {
        meta: col.datatype,
        caption: col.full_colname,
        value: col.full_colname,
        id: col.id,
        scope: 1
      }
    })
    this.$refs.ccSql.$emit('setAutoCompleteData', ad)
  }
  resetCC () {
    this.$refs['ccForm'].resetFields()
    this.ccObject = JSON.parse(this.ccMeta)
  }
  @Watch('ccDesc')
  initCCDesc () {
    this.resetCC()
    if (this.ccDesc) {
      Object.assign(this.ccObject, this.ccDesc)
      this.isEdit = false
    } else {
      this.isEdit = true
    }
  }
  mounted () {
    this.$on('addCC', this.addCC)
    this.initCCDesc()
    let data = this.modelInstance.getTableColumns()
    this.setAutoCompleteData(data)
    if (this.currentCCForm) {
      this.ccObject = {...this.ccObject, ...this.currentCCForm}
    }
  }
}
</script>

<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .cc-form-box {
     .cc-block {
      border: 1px solid @line-border-color;
      padding: 15px;
      background-color: @table-stripe-color;
      margin-top: 8px;
    }
    .is-datatype-error {
      border-color: @ke-color-danger;
    }
    .column-name {
      display: inline-block;
      width: 360px;
      overflow: hidden;
      text-overflow: ellipsis;
      position: absolute;
      margin-left: 5px;
    }
    .expression-tips {
      background: @regular-background-color;
      font-size: 12px;
      padding: 15px 10px;
      box-sizing: border-box;
      margin-bottom: 10px;
      color: @color-text-secondary;
      p {
        line-height: 20px;
      }
      .point {
        margin-right: 10px;
        vertical-align: top;
      }
      .text {
        display: inline-block;
        width: calc(~'100% - 30px');
        word-break: break-all;
      }
    }
    &.error-tips {
      .cc-block {
        border-color: @ke-color-danger;
      }
    }
  }
</style>
