<template>
  <el-dialog
    append-to-body
    width="480px"
    limited-area
    :title="$t(measureTitle)"
    :visible="true"
    top="5%"
    class="add-measure-modal"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="handleHide(false)">
    <el-form :model="measure" class="add-measure" label-position="top" :rules="rules"  ref="measureForm">
      <el-form-item :label="$t('name')" prop="name">
        <div>
          <el-input class="measures-width measure-name-input" size="medium" v-model.trim="measure.name" :placeholder="$t('kylinLang.common.nameFormatValidTip2')"></el-input>
        </div>
      </el-form-item>
      <el-form-item :label="$t('expression')" prop="expression">
        <el-select :popper-append-to-body="false" class="measures-width measure-expression-select" popper-class="js_measure-expression" size="medium" v-model="measure.expression" @change="changeExpression">
          <el-option
            v-for="item in expressionsConfigs"
            :key="item.value"
            :label="item.label"
            :value="item.value">
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="measure.expression === 'TOP_N'|| measure.expression === 'PERCENTILE_APPROX' || measure.expression === 'COUNT_DISTINCT'" :label="$t('return_type')" >
        <el-select
          :popper-append-to-body="false"
          size="medium"
          v-model="measure.return_type"
          class="measures-width"
          @change="changeReturnType">
          <el-option
            v-for="(item, index) in getSelectDataType"
            :key="index"
            :label="item.name"
            :value="item.value">
          </el-option>
        </el-select>
      </el-form-item>
      <div class="ksd-fs-16 ksd-mb-6 value-label" v-if="measure.expression === 'TOP_N'">
        {{$t('paramValue')}}
      </div>
      <el-form-item :class="{'is-error': corrColumnError || (ccValidateError&&ccVisible), 'cc-item': ccVisible, 'corr-item': measure.expression ==='CORR'}" prop="parameterValue.value" key="parameterItem">
        <span slot="label" class="withIconLabel"><span>{{isOrderBy}}</span>
          <el-tooltip effect="dark" placement="top" v-if="measure.expression ==='CORR'"><span slot="content" v-html="$t('corrTips')"></span><i class="el-ksd-icon-more_info_22 icon ksd-ml-5"></i></el-tooltip>
        </span>
        <el-tag type="info" class="measures-width" v-if="measure.expression === 'SUM(constant)' || measure.expression === 'COUNT(constant)'">1</el-tag>
        <div class="measure-flex-row" v-else>
          <div class="flex-item">
            <el-select
            class="parameter-select"
            popper-class="js_parameter-select"
            :class="{
            'measures-addCC': measure.expression !== 'COUNT_DISTINCT' && measure.expression !== 'TOP_N',
            'measures-width': measure.expression === 'COUNT_DISTINCT' || measure.expression === 'TOP_N',
            'error-tip': showMutipleColumnsTip,
            'error-measure': handlerErrorTip(measure)}"
            size="medium" v-model="measure.parameterValue.value" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
            filterable @change="(v) => changeParamValue(v, measure)" :disabled="isEdit">
              <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!measure.parameterValue.value"></i>
              <el-option-group key="column" :label="$t('columns')">
                <el-option
                  v-for="(item, index) in getParameterValue"
                  :key="index"
                  :label="item.name"
                  :value="item.name">
                  <el-tooltip :content="item.name" effect="dark" placement="top"><span>{{item.name.split('.')[item.name.split('.').length - 1] | omit(30, '...')}}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{item.datatype.toLocaleLowerCase()}}</span>
                </el-option>
              </el-option-group>
              <el-option-group key="ccolumn" :label="$t('ccolumns')" v-if="getCCGroups().length || newCCList.length">
                <el-option
                  v-for="item in getCCGroups()"
                  :key="item.guid"
                  :label="item.tableAlias + '.' + item.columnName"
                  :value="item.tableAlias + '.' + item.columnName">
                  <el-tooltip :content="`${item.tableAlias}.${item.columnName}`" effect="dark" placement="top"><span>{{item.columnName | omit(30, '...')}}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{item.datatype.toLocaleLowerCase()}}</span>
                </el-option>
                <el-option
                  v-for="item in newCCList"
                  :key="item.table_guid"
                  :label="item.tableAlias + '.' + item.columnName"
                  :value="item.tableAlias + '.' + item.columnName">
                  <el-tooltip :content="`${item.tableAlias}.${item.columnName}`" effect="dark" placement="top"><span>{{item.columnName | omit(30, '...')}}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{item.datatype.toLocaleLowerCase()}}</span>
                </el-option>
              </el-option-group>
            </el-select>
            <common-tip :content="$t('addCCTip')"><el-button size="medium" v-if="measure.expression !== 'COUNT_DISTINCT' && measure.expression !== 'TOP_N'" icon="el-ksd-icon-auto_computed_column_old" type="primary" plain @click="newCC" class="ksd-ml-6" :disabled="(isEdit && ccVisible) || !!isHybridModel"></el-button></common-tip>
          </div>
          <el-button type="primary" size="mini" icon="el-icon-ksd-add_2" plain circle v-if="measure.expression === 'COUNT_DISTINCT'&&measure.return_type!=='bitmap'" class="ksd-ml-10" @click="addNewProperty"></el-button>
        </div>
        <p class="sync-comment-tip" v-if="showSync">{{$t('syncCommentTip1', {comment: columnComment(measure.parameterValue.value)})}}<span class="sync-content" @click="handleSyncComment">{{$t('syncContent')}}</span></p>
      </el-form-item>
      <CCEditForm ref="ccEditForm" class="ksd-mb-8" :class="{'error-tips': corrColumnError}" key="ccEditForm" v-if="ccVisible" @checkSuccess="saveCC" @delSuccess="delCC" :hideCancel="isEditMeasure" :isEditMeasureCC="!isEdit" source="createMeasure" :ccDesc="ccObject" :modelInstance="modelInstance" @resetSubmitLoading="resetSubmitType" @saveError="resetSubmitType"></CCEditForm>
      <div class="error-tips" v-if="corrColumnError">{{$t('corrColDatatypeError')}}</div>
      <el-form-item :label="isGroupBy" v-if="(measure.expression === 'COUNT_DISTINCT' || measure.expression === 'TOP_N')&&measure.convertedColumns.length>0" prop="convertedColumns[0].value" :rules="rules.convertedColValidate" key="topNItem" :class="{'measure-column-multiple': measure.expression === 'COUNT_DISTINCT'}">
        <div class="measure-flex-row" v-for="(column, index) in measure.convertedColumns" :key="index" :class="{'ksd-mt-10': !isGroupBy || (isGroupBy && index > 0)}">
          <div class="flex-item">
            <el-select :class="['measures-width', {'error-tip': showMutipleColumnsTip || column.sameIndex}]" size="medium" v-model="column.value" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" filterable @change="changeConColParamValue(column.value, index)">
              <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!column.value"></i>
              <el-option
                v-for="(item, index) in getParameterValue2"
                :key="index"
                :label="item.name"
                :value="item.name">
                <el-tooltip :content="item.name" effect="dark" placement="top"><span>{{item.name | omit(30, '...')}}</span></el-tooltip>
                <span class="ky-option-sub-info">{{item.datatype}}</span>
              </el-option>
              <el-option-group key="ccolumn" :label="$t('ccolumns')" v-if="measure.expression === 'TOP_N' && getCCGroups(isGroupBy).length">
                <el-option
                  v-for="item in getCCGroups(isGroupBy)"
                  :key="item.guid"
                  :label="item.tableAlias + '.' + item.columnName"
                  :value="item.tableAlias + '.' + item.columnName">
                  <el-tooltip :content="`${item.tableAlias}.${item.columnName}`" effect="dark" placement="top"><span>{{item.columnName | omit(30, '...')}}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{item.datatype.toLocaleLowerCase()}}</span>
                </el-option>
              </el-option-group>
            </el-select>
            <p class="error-text" v-if="column.sameIndex">{{$t('duplicateColumns')}}</p>
          </div>
          <el-button type="primary" plain icon="el-icon-ksd-add_2" size="mini" v-if="measure.expression === 'TOP_N' && index == 0" circle @click="addNewProperty" class="ksd-ml-10"></el-button><el-button
           type="primary" icon="el-icon-minus" size="mini" circle @click="deleteProperty(index)" class="del-pro" :class="[measure.expression === 'COUNT_DISTINCT' ? 'ksd-ml-10' : 'ksd-ml-5', {'del-margin-more': measure.expression === 'TOP_N' && index > 0}]" :disabled="measure.expression === 'TOP_N' && measure.convertedColumns.length == 1"></el-button>
        </div>
      </el-form-item>
      <el-form-item v-if="measure.expression ==='CORR'" :class="{'is-error': corrColumnError || (ccValidateError&&corrCCVisible), 'cc-item': corrCCVisible}" prop="convertedColumns[0].value" :rules="rules.convertedColValidate" key="corrItem">
        <div>
          <el-select class="measures-addCC" size="medium" v-model="measure.convertedColumns[0].value" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" filterable @change="(v) => changeCORRParamValue(v, measure)" :disabled="isCorrCCEdit">
            <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!measure.convertedColumns[0].value"></i>
            <el-option-group key="column" :label="$t('columns')">
              <el-option
                v-for="(item, index) in getParameterValue"
                :key="index"
                :label="item.name"
                :value="item.name">
                 <el-tooltip :content="item.name" effect="dark" placement="top"><span>{{item.name.split('.')[item.name.split('.').length - 1] | omit(30, '...')}}</span></el-tooltip>
                <span class="ky-option-sub-info">{{item.datatype.toLocaleLowerCase()}}</span>
              </el-option>
            </el-option-group>
            <el-option-group key="ccolumn" :label="$t('ccolumns')" v-if="getCCGroups().length || newCCList.length">
              <el-option
                v-for="item in getCCGroups()"
                :key="item.guid"
                :label="item.tableAlias + '.' + item.columnName"
                :value="item.tableAlias + '.' + item.columnName">
                <el-tooltip :content="`${item.tableAlias}.${item.columnName}`" effect="dark" placement="top"><span>{{item.columnName | omit(30, '...')}}</span></el-tooltip>
                <span class="ky-option-sub-info">{{item.datatype.toLocaleLowerCase()}}</span>
              </el-option>
              <el-option
                  v-for="item in newCCList"
                  :key="item.table_guid"
                  :label="item.tableAlias + '.' + item.columnName"
                  :value="item.tableAlias + '.' + item.columnName">
                  <el-tooltip :content="`${item.tableAlias}.${item.columnName}`" effect="dark" placement="top"><span>{{item.columnName | omit(30, '...')}}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{item.datatype.toLocaleLowerCase()}}</span>
                </el-option>
            </el-option-group>
          </el-select>
          <common-tip :content="$t('addCCTip')"><el-button size="medium" icon="el-ksd-icon-auto_computed_column_old" type="primary" plain class="ksd-ml-6" @click="newCorrCC" :disabled="(isCorrCCEdit && corrCCVisible) || !!isHybridModel"></el-button></common-tip>
        </div>
      </el-form-item>
      <CCEditForm class="ksd-mb-8" :class="{'error-tips': corrColumnError}" key="corrEditForm" ref="corrEditForm" v-if="corrCCVisible" @checkSuccess="saveCorrCC" @delSuccess="delCorrCC" :hideCancel="isEditMeasure" :isEditMeasureCC="!isCorrCCEdit" source="createMeasure" :ccDesc="corrCCObject" :modelInstance="modelInstance" @resetSubmitLoading="resetSubmitType" @saveError="resetSubmitType"></CCEditForm>
      <div class="error-tips" v-if="corrColumnError">{{$t('corrColDatatypeError')}}</div>
      <el-form-item :label="$t('comment')" prop="comment">
        <div>
          <el-input class="measures-width measure-comment-input" size="medium" v-model.trim="measure.comment" :placeholder="$t('kylinLang.common.pleaseInput')"></el-input>
        </div>
      </el-form-item>
    </el-form>
    <el-alert :title="measureError" v-if="measureError" class="ksd-mt-16" type="error" show-icon></el-alert>
    <span slot="footer" class="dialog-footer ky-no-br-space">
      <el-button plain size="medium" @click="handleHide(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" @click="checkMeasure" :disabled="sameGroupBy()" :loading="loadingSubmit">{{$t('kylinLang.common.submit')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { measuresDataType, measureSumAndTopNDataType, measurePercenDataType } from '../../../../config'
import { objectClone, sampleGuid, indexOfObjWithSomeKey, handleSuccessAsync } from '../../../../util/index'
import { measureNameRegex } from 'config'
import CCEditForm from '../ComputedColumnForm/ccform.vue'
import { mapGetters, mapActions, mapState } from 'vuex'
import $ from 'jquery'
@Component({
  props: {
    isEditMeasure: {
      type: Boolean,
      default: false
    },
    measureObj: {
      type: Object,
      default () {
        return {}
      }
    },
    modelInstance: {
      type: Object
    },
    isHybridModel: {
      type: [Boolean, String]
    }
  },
  computed: {
    ...mapGetters([
      'dimMeasNameMaxLength'
    ]),
    ...mapState({
      enableCheckName: state => state.system.enableCheckName
    })
  },
  methods: {
    // 后台接口请求
    ...mapActions({
      checkInternalMeasure: 'CHECK_INTERNAL_MEASURE'
    })
  },
  components: {
    CCEditForm
  },
  locales: {
    'en': {
      requiredName: 'The measure name is required.',
      name: 'Name',
      comment: 'Comment',
      expression: 'Function',
      return_type: 'Function Parameter',
      paramValue: 'Column',
      nameReuse: 'The name of measure can\'t be duplicated within the same model',
      requiredCCName: 'The column name is required.',
      requiredreturn_type: 'The function parameter is required.',
      requiredExpress: 'The function is required.',
      columns: 'Columns',
      ccolumns: 'Computed Columns',
      requiredParamValue: 'The column is Required.',
      addCCTip: 'Create Computed Column',
      editMeasureTitle: 'Edit Measure',
      addMeasureTitle: 'Add Measure',
      sameColumn: 'Column has been defined as a measure by the same function',
      selectMutipleColumnsTip: 'The {expression} function supports only one column when the function parameter is {params}.',
      createCCMeasureTips: 'This column’s type is {datatype}. It couldn’t be referenced by the selected function {expression}.',
      duplicateColumns: 'Same Statement',
      noProvideDecimalType: 'PERCENTILE function does not support columns with {datatype} data type.',
      syncCommentTip1: 'This column’s comment is "{comment}", ',
      syncContent: 'sync to the comment',
      corrTips: '* The supported column data types are: bigint, integer, tinyint, smallint, decimal, double and float.<br/>* If the data type of one of the columns is decimal, the other one\'s also needs to be decimal.',
      corrColDatatypeError: 'The data type of one of the columns is decimal, the other one\'s also needs to be decimal.',
      nameValid: 'Only supports Chinese or English characters, numbers, spaces and symbol（_ -()%?.）'
    }
  }
})
export default class AddMeasure extends Vue {
  measureVisible = false
  reuseColumn = ''
  measureTitle = ''
  measure = {
    name: '',
    expression: 'SUM(column)',
    parameterValue: {type: 'column', value: '', table_guid: null},
    convertedColumns: [],
    return_type: '',
    comment: '',
    nameBackUp: ''
  }
  syncComment = false
  ccObject = null
  corrCCObject = null
  isEdit = false
  isCorrCCEdit = false
  ccVisible = false
  corrCCVisible = false
  ccGroups = []
  newCCList = []
  allTableColumns = []
  // expressionsConf = [
  //   {label: 'SUM (column)', value: 'SUM(column)'},
  //   {label: 'SUM (constant)', value: 'SUM(constant)'},
  //   {label: 'MIN', value: 'MIN'},
  //   {label: 'MAX', value: 'MAX'},
  //   {label: 'TOP_N', value: 'TOP_N'},
  //   {label: 'COUNT (column)', value: 'COUNT(column)'},
  //   // {label: 'COUNT (constant)', value: 'COUNT(constant)'}, 去除 count(constant) 函数，默认添加 count_all 度量
  //   {label: 'COUNT_DISTINCT', value: 'COUNT_DISTINCT'},
  //   {label: 'CORR (column1, column2)', value: 'CORR'},
  //   {label: 'PERCENTILE_APPROX', value: 'PERCENTILE_APPROX'},
  //   {label: 'COLLECT_SET', value: 'COLLECT_SET'}
  // ]
  topNTypes = [
    {name: 'Top 10', value: 'topn(10)'},
    {name: 'Top 100', value: 'topn(100)'},
    {name: 'Top 1000', value: 'topn(1000)'}
  ]
  distinctDataTypes = [
    {name: 'Error Rate < 9.75%', value: 'hllc(10)'},
    {name: 'Error Rate < 4.88%', value: 'hllc(12)'},
    {name: 'Error Rate < 2.44%', value: 'hllc(14)'},
    {name: 'Error Rate < 1.72%', value: 'hllc(15)'},
    {name: 'Error Rate < 1.22%', value: 'hllc(16)'},
    {name: 'Precisely', value: 'bitmap'}
  ]
  percentileTypes = [
    {name: 'percentile(100)', value: 'percentile(100)'},
    {name: 'percentile(1000)', value: 'percentile(1000)'},
    {name: 'percentile(10000)', value: 'percentile(10000)'}
  ]
  integerType = ['bigint', 'int', 'integer', 'smallint', 'tinyint']
  floatType = ['decimal', 'double', 'float']
  otherType = ['binary', 'boolean', 'char', 'date', 'string', 'timestamp', 'varchar']
  showMutipleColumnsTip = false
  ccValidateError = false
  loadingSubmit = false
  corrColumnError = false
  measureError = ''

  get expressionsConfigs () {
    if (!this.modelInstance) return []
    const config = [
      {label: 'SUM (column)', value: 'SUM(column)'},
      {label: 'SUM (constant)', value: 'SUM(constant)'},
      {label: 'MIN', value: 'MIN'},
      {label: 'MAX', value: 'MAX'},
      {label: 'TOP_N', value: 'TOP_N'},
      {label: 'COUNT (column)', value: 'COUNT(column)'},
      // {label: 'COUNT (constant)', value: 'COUNT(constant)'}, 去除 count(constant) 函数，默认添加 count_all 度量
      {label: 'COUNT_DISTINCT', value: 'COUNT_DISTINCT'},
      {label: 'CORR (column1, column2)', value: 'CORR'},
      {label: 'PERCENTILE_APPROX', value: 'PERCENTILE_APPROX'},
      {label: 'COLLECT_SET', value: 'COLLECT_SET'}
    ]
    if (this.isHybridModel) {
      return config.filter(item => item.value !== 'CORR')
    } else {
      return config
    }
  }

  get rules () {
    return {
      name: [
        { required: true, message: this.$t('requiredName'), trigger: 'blur' },
        { validator: this.validateName, trigger: 'blur' }
      ],
      expression: [{ required: true, message: this.$t('requiredExpress'), trigger: 'change' }],
      'parameterValue.value': [
        { required: true, message: this.$t('requiredParamValue'), trigger: 'change' },
        { validator: this.checkColumn }
      ],
      convertedColValidate: [{ required: true, message: this.$t('requiredParamValue'), trigger: 'blur, change' }, { validator: this.checkColumn }]
    }
  }
  get ccRules () {
    return {
      name: [{ required: true, message: this.$t('requiredCCName'), trigger: 'blur' }],
      return_type: [{ required: true, message: this.$t('requiredreturn_type'), trigger: 'change' }],
      expression: [{ required: true, message: this.$t('requiredExpress'), trigger: 'change' }]
    }
  }
  getCCGroups (isGroupBy) {
    if (this.ccGroups.length) {
      if (this.measure.expression === 'SUM(column)' || this.measure.expression === 'CORR') {
        return this.ccGroups.filter(it => measureSumAndTopNDataType.includes(it.datatype.toLocaleLowerCase().match(/^(\w+)\(?/)[1]))
      } else if (this.measure.expression === 'TOP_N') {
        if (isGroupBy && isGroupBy === 'Group by') {
          return this.ccGroups.filter(it => measuresDataType.includes(it.datatype.toLocaleLowerCase().match(/^(\w+)\(?/)[1]))
        } else {
          return this.ccGroups.filter(it => measureSumAndTopNDataType.includes(it.datatype.toLocaleLowerCase().match(/^(\w+)\(?/)[1]))
        }
      } else if (this.measure.expression === 'PERCENTILE_APPROX') {
        return this.ccGroups.filter(item => measurePercenDataType.includes(item.datatype.toLocaleLowerCase().match(/^(\w+)\(?/)[1]))
      } else {
        return this.ccGroups
      }
    }
    return []
  }

  get flattenLookupTables () {
    return this.modelInstance.anti_flatten_lookups
  }

  get showSync () {
    if (!this.measure.parameterValue.value || this.ccVisible || ['SUM(constant)', 'COUNT_DISTINCT'].includes(this.measure.expression)) return false
    return this.columnComment(this.measure.parameterValue.value)
  }

  // 获取列的注释
  columnComment (value) {
    const alias = value.split('.')[0]
    const nTable = this.modelInstance.getTableByAlias(alias)
    const currentColumnInfo = nTable.columns.filter(item => item.full_colname === value)
    return currentColumnInfo.length ? currentColumnInfo[0].comment || '' : ''
  }

  sameGroupBy () {
    return this.measure.expression === 'TOP_N' && this.measure.convertedColumns.filter(it => it.sameIndex).length > 0
  }
  validateName (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('requiredName')))
    } else if (value.length > this.dimMeasNameMaxLength) {
      callback(new Error(this.$t('kylinLang.common.nameMaxLen', {len: this.dimMeasNameMaxLength})))
    } else {
      if (!measureNameRegex.test(value)) {
        callback(new Error(this.$t('nameValid')))
      }
      if (this.modelInstance.all_measures.length) {
        let isResuse = false
        for (let i = 0; i < this.modelInstance.all_measures.length; i++) {
          /* if (this.modelInstance.all_measures[i].name.toLocaleUpperCase() === this.measure.name.toLocaleUpperCase() && this.measure.guid !== this.modelInstance.all_measures[i].guid) {
            isResuse = true
            break
          } */
          // 大小写敏感
          if (this.modelInstance.all_measures[i].name === this.measure.name && this.measure.guid !== this.modelInstance.all_measures[i].guid) {
            isResuse = true
            break
          }
        }
        if (isResuse) {
          callback(new Error(this.$t('nameReuse')))
        } else {
          callback()
        }
      } else {
        callback()
      }
    }
  }

  checkColumn (rule, value, callback) {
    // TOP_N 度量无需判断列是否已经被使用过了
    // CORR 度量需要判断两个列都选择重复后报错
    if (this.measure.expression === 'TOP_N' || this.measure.expression === 'CORR' && (!this.measure.parameterValue.value || !this.measure.convertedColumns[0].value)) {
      callback()
      return
    }
    if (!this.modelInstance.checkSameEditMeasureColumn(this.measure)) {
      callback(new Error(this.$t('sameColumn')))
    } else {
      callback()
    }
  }
  // 取消之前的约定，不用再统一传大写
  /* upperCaseName () {
    this.measure.name = this.measure.name.toLocaleUpperCase()
  } */

  changeExpression () {
    this.measure.return_type = ''
    this.measure.parameterValue.value = ''
    this.measure.comment = ''
    this.corrCCVisible = false
    this.ccVisible = false
    this.corrCCVisible = false
    this.isEdit = false
    this.isCorrCCEdit = false
    this.showMutipleColumnsTip = false
    this.measureError = ''
    this.$refs['measureForm'].clearValidate('parameterValue.value')
    this.$refs['measureForm'].clearValidate('convertedColumns[0].value')
    this.corrColumnError = ''
    if (this.measure.expression === 'SUM(constant)' || this.measure.expression === 'COUNT(constant)') {
      this.measure.parameterValue.type = 'constant'
      this.measure.parameterValue.value = 1
    } else {
      this.measure.parameterValue.type = 'column'
      this.measure.parameterValue.value = ''
    }
    if (this.measure.expression === 'COUNT_DISTINCT') {
      this.measure.return_type = 'hllc(10)'
    }
    if (this.measure.expression === 'PERCENTILE_APPROX') {
      this.measure.return_type = 'percentile(100)'
    }
    if (this.measure.expression === 'TOP_N') {
      this.measure.return_type = 'topn(100)'
    }
    if (this.measure.expression === 'CORR' || this.measure.expression === 'TOP_N') {
      this.measure.convertedColumns = [{type: 'column', value: '', table_guid: null}]
    } else {
      this.measure.convertedColumns = []
    }
  }

  changeReturnType () {
    this.showMutipleColumnsTip = false
    if (this.measure.expression === 'COUNT_DISTINCT') {
      this.measure.convertedColumns = []
      this.measure.parameterValue.value = ''
    }
  }

  addNewProperty () {
    const GroupBy = {type: 'column', value: '', table_guid: null}
    this.measure.convertedColumns.push(GroupBy)
  }

  deleteProperty (index) {
    this.measure.convertedColumns.splice(index, 1)
    this.checkSameGroupByColumns()
  }

  getCCObj (value) {
    const measureNamed = value.split('.')
    const alias = measureNamed[0]
    const column = measureNamed[1]
    const ccObj = this.modelInstance.getCCObj(alias, column)
    return ccObj
  }

  changeParamValue (value, measure) {
    this.$refs['measureForm'] && measure.expression === 'CORR' && this.$refs['measureForm'].clearValidate('convertedColumns[0].value')
    const alias = value.split('.')[0]
    const nTable = this.modelInstance.getTableByAlias(alias)
    measure.parameterValue.table_guid = nTable && nTable.guid
    // const currentColumnInfo = nTable.columns.filter(item => item.full_colname === value)
    const ccObj = this.getCCObj(value)
    if (ccObj) {
      this.ccObject = ccObj
      this.ccVisible = true
      this.isEdit = false
    } else {
      this.ccVisible = false
      this.isEdit = false
    }
    // measure.comment = currentColumnInfo.length ? currentColumnInfo[0].comment || '' : ''
    measure.expression === 'SUM(column)' && (measure.return_type = '')
    this.syncComment = false
    this.corrColumnError = false
    this.measureError = ''
    this.$nextTick(() => {
      this.$refs['measureForm'] && measure.expression === 'CORR' && measure.convertedColumns[0].value && this.$refs['measureForm'].validateField('convertedColumns[0].value')
    })
  }

  changeConColParamValue (value, index) {
    const alias = value.split('.')[0]
    const nTable = this.modelInstance.getTableByAlias(alias)
    this.measure.convertedColumns[index].table_guid = nTable && nTable.guid
    this.checkSameGroupByColumns()
  }

  changeCORRParamValue (value, measure) {
    this.$refs['measureForm'] && this.$refs['measureForm'].clearValidate('parameterValue.value')
    const alias = value.split('.')[0]
    const nTable = this.modelInstance.getTableByAlias(alias)
    measure.convertedColumns[0].table_guid = nTable && nTable.guid
    const ccObj = this.getCCObj(value)
    if (ccObj) {
      this.corrCCObject = ccObj
      this.corrCCVisible = true
      this.isCorrCCEdit = false
    } else {
      this.corrCCVisible = false
      this.isCorrCCEdit = false
    }
    this.corrColumnError = false
    this.measureError = ''
    this.$nextTick(() => {
      this.$refs['measureForm'] && this.$refs['measureForm'].validateField('parameterValue.value')
    })
  }

  checkSameGroupByColumns () {
    if (this.measure.expression === 'TOP_N') {
      const columnList = this.measure.convertedColumns.map(it => it.value)
      this.measure.convertedColumns.forEach((item, index, self) => {
        item.sameIndex = columnList.indexOf(item.value) !== columnList.lastIndexOf(item.value)
      })
    }
  }

  resetCCVisble () {
    this.isEdit = false
    this.isCorrCCEdit = false
    this.ccVisible = false
    this.corrCCVisible = false
    this.ccObject = null
    this.corrCCObject = null
    this.ccValidateError = false
    this.corrColumnError = false
  }

  newCC () {
    this.resetCCVisble()
    this.measure.parameterValue.value = ''
    this.measure.return_type = ''
    this.isEdit = true
    this.ccVisible = true
  }
  newCorrCC () {
    this.resetCCVisble()
    this.measure.convertedColumns[0].value = ''
    this.isCorrCCEdit = true
    this.corrCCVisible = true
  }
  saveCC (cc) {
    this.ccObject = cc
    this.measure.parameterValue.value = cc.tableAlias + '.' + cc.columnName
    // this.isEdit = false
    this.checkNewCC(cc, 'ccEditForm')
  }
  saveCorrCC (cc) {
    this.measure.convertedColumns[0].value = cc.tableAlias + '.' + cc.columnName
    this.corrCCObject = cc
    // this.isCorrCCEdit = false
    this.checkNewCC(cc, 'corrEditForm')
  }
  checkNewCC (cc, ref) {
    this.$nextTick(() => {
      if (!measureSumAndTopNDataType.includes(cc.datatype.match(/^(\w+)\(?/)[1].toLocaleLowerCase()) && (['SUM(column)', 'CORR'].includes(this.measure.expression))) {
        this.$refs[ref] && (this.$refs[ref].isEdit = true)
        this.$refs[ref] && (this.$refs[ref].errorMsg = this.$t('createCCMeasureTips', {expression: this.measure.expression, datatype: cc.datatype}))
        this.ccValidateError = true
      } else if (!measurePercenDataType.includes(cc.datatype.toLocaleLowerCase()) && this.measure.expression === 'PERCENTILE_APPROX') {
        this.$refs[ref] && (this.$refs[ref].isEdit = true)
        this.$refs[ref] && (this.$refs[ref].errorMsg = this.$t('noProvideDecimalType', {datatype: cc.datatype.match(/^(\w+)\(?/)[1].toLocaleLowerCase()}))
        this.ccValidateError = true
      } else {
        this.$refs[ref] && (this.$refs[ref].isEdit = false)
        this.ccValidateError = false
      }
      if (!this.ccValidateError) {
        cc.name = cc.tableAlias + '.' + cc.columnName
        const index = indexOfObjWithSomeKey(this.newCCList, 'name', cc.name)
        index === -1 && this.newCCList.push(cc)
      }
    })
  }
  delCC (cc) {
    this.measure.parameterValue.value = ''
    this.ccVisible = false
    this.isEdit = false
    this.corrColumnError = false
  }
  delCorrCC (cc) {
    this.measure.convertedColumns[0].value = ''
    this.corrCCVisible = false
    this.isCorrCCEdit = false
    this.corrColumnError = false
  }

  get isOrderBy () {
    if (this.measure.expression === 'TOP_N') {
      return 'Order/ Sum by'
    } else {
      return this.$t('paramValue')
    }
  }

  get isGroupBy () {
    if (this.measure.expression === 'TOP_N') {
      return 'Group by'
    } else {
      return ''
    }
  }
  get getSelectDataType () {
    if (this.measure.expression === 'TOP_N') {
      return this.topNTypes
    }
    if (this.measure.expression === 'COUNT_DISTINCT') {
      return this.distinctDataTypes
    }
    if (this.measure.expression === 'PERCENTILE_APPROX') {
      return this.percentileTypes
    }
  }

  get getParameterValue () {
    let targetColumns = []
    let filterType = []
    if (this.allTableColumns) {
      if (['SUM(column)', 'TOP_N', 'CORR'].includes(this.measure.expression)) {
        filterType = measureSumAndTopNDataType
      } else if (this.measure.expression === 'PERCENTILE_APPROX') {
        filterType = measurePercenDataType
      } else {
        filterType = measuresDataType
      }
      $.each(this.allTableColumns, (index, column) => {
        const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
        const returnValue = returnRegex.exec(column.datatype)
        if (filterType.indexOf(returnValue[1]) >= 0 && !this.flattenLookupTables.includes(column.table_alias)) {
          const columnObj = {name: column.table_alias + '.' + column.name, datatype: column.datatype}
          targetColumns.push(columnObj)
        }
      })
    }
    return targetColumns
  }

  // 支持measure的任意类型
  get getParameterValue2 () {
    let targetColumns = []
    $.each(this.allTableColumns, (index, column) => {
      const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
      const returnValue = returnRegex.exec(column.datatype)
      if (measuresDataType.indexOf(returnValue[1]) >= 0 && !this.flattenLookupTables.includes(column.table_alias)) {
        const columnObj = {name: column.table_alias + '.' + column.name, datatype: column.datatype}
        targetColumns.push(columnObj)
      }
    })
    return targetColumns
  }

  handleHide (isSubmit, measure, isEdit, fromSearch) {
    this.measureVisible = false
    this.ccValidateError = false
    this.loadingSubmit = false
    this.newCCList = []
    this.$emit('closeAddMeasureDia', {
      isEdit: isEdit,
      fromSearch: fromSearch,
      data: measure,
      isSubmit: isSubmit
    })
    this.$refs['measureForm'].resetFields()
  }

  saveCCColumn () {
    this.newCCList.forEach(c => {
      this.modelInstance.addCC(c)
    })
  }

  async checkInterMea (measure, cclist) {
    this.measureError = ''
    return this.modelInstance.generateMetadata().then(async (data) => {
      try {
        // 新建measure的模型功校验measure接口使用
        let resData = objectClone(data)
        if (!this.isEditMeasure) {
          resData.simplified_measures.push(measure)
        } else {
          const name = this.measureObj.name
          const index = resData.simplified_measures.findIndex(it => it.name === name)
          index >= 0 && (resData.simplified_measures.splice(index, 1, measure))
        }
        cclist.length > 0 && (resData.computed_columns = [...resData.computed_columns, ...cclist])
        const res = await this.checkInternalMeasure(resData)
        const checkRespData = await handleSuccessAsync(res)
        return checkRespData
      } catch (res) {
        this.measureError = res.data.msg
        return false
      }
    })
  }

  async checkMeasure () {
    try {
      this.loadingSubmit = true
      // 创建measure 和 cc 整合，提交的时候统一检测 cc 是否符合规范，不再分看执行
      // if (this.ccVisible && this.isEdit) {
      //   // this.$refs.ccEditForm && await this.$refs.ccEditForm.addCC()
      //   if (this.ccValidateError) {
      //     this.loadingSubmit = false
      //     return
      //   }
      // }
      this.$refs.measureForm.validate(async (valid) => {
        if (valid) {
          // 老数据有多列但函数参数仅支持单列，此时提示错误信息
          let message = ''
          this.loadingSubmit = false
          if (this.measure.expression === 'COUNT_DISTINCT' && this.measure.return_type === 'bitmap' && this.measure.convertedColumns.length) {
            this.showMutipleColumnsTip = true
            message = this.$t('selectMutipleColumnsTip', {expression: this.measure.expression, params: 'Precisely'})
          }
          if (this.showMutipleColumnsTip) {
            this.$message({
              type: 'error',
              message
            })
            return
          }
          this.corrColumnError = false
          if (this.measure.expression === 'CORR') {
            const corr1Datatype = this.getDatatype(this.measure.parameterValue.value).toLocaleLowerCase().indexOf('decimal') >= 0
            const corr2Datatype = this.getDatatype(this.measure.convertedColumns[0].value).toLocaleLowerCase().indexOf('decimal') >= 0
            if ((corr1Datatype && !corr2Datatype) || (!corr1Datatype && corr2Datatype)) {
              this.corrColumnError = true
              return
            }
          }
          const measureClone = objectClone(this.measure)
          // 判断该操作是否属于搜索入口进来
          let isFromSearchAciton = measureClone.fromSearch
          if (measureClone.expression.indexOf('SUM') !== -1) {
            measureClone.expression = 'SUM'
          }
          if (measureClone.expression.indexOf('COUNT(constant)') !== -1 || measureClone.expression.indexOf('COUNT(column)') !== -1) {
            measureClone.expression = 'COUNT'
          }
          measureClone.convertedColumns.unshift(measureClone.parameterValue)
          measureClone.parameter_value = measureClone.convertedColumns
          delete measureClone.parameterValue
          delete measureClone.convertedColumns
          delete measureClone.fromSearch
          if (!this.isEditMeasure && measureClone.expression === 'SUM') {
            const name = measureClone.parameter_value.length ? measureClone.parameter_value[0].value : ''
            if (name) {
              const currentMeasure = [...this.allTableColumns, ...this.ccGroups].filter(it => it.full_colname === name)
              if (currentMeasure.length && !measureSumAndTopNDataType.includes(currentMeasure[0].datatype.match(/^(\w+)\(?/)[1].toLocaleLowerCase())) {
                this.$message({ type: 'error', closeOtherMessages: true, message: this.$t('createCCMeasureTips', {expression: measureClone.expression, datatype: currentMeasure[0].datatype}) })
                return
              }
            }
          }
          // await this.checkInterMea(measureClone, this.newCCList)
          if (this.measureError) {
            return
          }
          this.saveCCColumn()
          let action = this.isEditMeasure ? 'editMeasure' : 'addMeasure'
          this.modelInstance[action](measureClone).then(() => {
            // this.resetMeasure()
            this.handleHide(true, measureClone, this.isEditMeasure, isFromSearchAciton)
          })
        } else {
          this.loadingSubmit = false
        }
      })
    } catch (e) {
      this.loadingSubmit = false
    }
  }

  getDatatype (colName) {
    const columns = [...this.getParameterValue, ...this.getCCGroups(), ...this.newCCList]
    const index = indexOfObjWithSomeKey(columns, 'name', colName)
    if (index >= 0) {
      return columns[index].datatype
    }
  }

  resetSubmitType () {
    this.loadingSubmit = false
  }

  resetMeasure () {
    this.measure = {
      name: '',
      expression: 'SUM(column)',
      parameterValue: {type: 'column', value: '', table_guid: null},
      convertedColumns: [],
      return_type: '',
      comment: '',
      nameBackUp: ''
    }
    this.syncComment = false
    this.ccVisible = false
    this.isEdit = false
    this.corrCCVisible = false
    this.isCorrCCEdit = false
    this.showMutipleColumnsTip = false
    this.measureError = ''
  }

  initExpression () {
    let measureObj = objectClone(this.measureObj)
    if (measureObj.parameter_value && measureObj.parameter_value.length) {
      measureObj.parameterValue = measureObj.parameter_value[0]
      measureObj.convertedColumns = measureObj.parameter_value.length > 1 ? measureObj.parameter_value.splice(1, measureObj.parameter_value.length - 1) : []
      delete measureObj.parameter_value
      if (measureObj.parameterValue.type === 'column') {
        this.changeParamValue(measureObj.parameterValue.value, measureObj)
      }
      if (measureObj.expression === 'CORR' && measureObj.convertedColumns.length) {
        this.changeCORRParamValue(measureObj.convertedColumns[0].value, measureObj)
      }
    }
    if (measureObj.expression === 'SUM' || measureObj.expression === 'COUNT') {
      measureObj.expression = `${measureObj.expression}(${measureObj.parameterValue.type})`
    }
    this.measure = { ...measureObj }
    if (!this.isEditMeasure) {
      this.measure.guid = sampleGuid()
    }
  }

  created () {
    this.measureTitle = this.isEditMeasure ? 'editMeasureTitle' : 'addMeasureTitle'
    this.allTableColumns = this.modelInstance && this.modelInstance.getTableColumns()
    this.ccGroups = this.modelInstance.computed_columns.map(c => {
      c.name = c.tableAlias + '.' + c.columnName
      c.full_colname = c.tableAlias + '.' + c.columnName
      return c
    })
    this.initExpression()
  }

  // @Watch('isShow')
  // onShowChange (val) {
  //   this.measureVisible = val
  //   if (this.measureVisible) {
  //     this.measureTitle = this.isEditMeasure ? 'editMeasureTitle' : 'addMeasureTitle'
  //     this.allTableColumns = this.modelInstance && this.modelInstance.getTableColumns()
  //     this.ccGroups = this.modelInstance.computed_columns.map(c => {
  //       c.name = c.tableAlias + '.' + c.columnName
  //       return c
  //     })
  //     this.initExpression()
  //   } else {
  //     this.resetMeasure()
  //   }
  // }

  @Watch('measure.convertedColumns')
  onConvertedColumnsChange (val) {
    if (this.measure.expression === 'COUNT_DISTINCT' && this.measure.return_type === 'bitmap' && !val.length) {
      this.showMutipleColumnsTip = false
    }
  }

  // SUM 或 PERCENTILE_APPROX 度量不适用 varchar 列
  handlerErrorTip (m) {
    return (m.expression.indexOf('SUM') >= 0 || m.expression === 'PERCENTILE_APPROX') && m.return_type && m.return_type.indexOf('varchar') >= 0
  }

  handleSyncComment () {
    // if (!this.measure.parameterValue.table_guid) return
    // const expr = this.measure.expression.replace(/\(\w+\)$/, '')
    // if (!this.syncComment) {
    this.$set(this.measure, 'comment', this.columnComment(this.measure.parameterValue.value))
    // this.measure.comment = this.columnComment(this.measure.parameterValue.value)
    //   this.measure.nameBackUp = this.measure.name
    //   this.measure.name = `${this.measure.comment.slice(0, 100)}${expr.toLocaleUpperCase() === 'SUM' ? '' : '_' + expr}`
    // }
    // else {
    //   this.measure.name = this.measure.nameBackUp
    // }
    // this.syncComment = !this.syncComment
  }
}
</script>

<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .add-measure-modal {
    .el-dialog {
      width: 500px\0 !important;
    }
  }
  .add-measure {
    .el-form-item.cc-item,
    .el-form-item.is-error {
      margin-bottom: 8px;
    }
    .corr-item {
      margin-bottom: 16px;
    }
    .error-tips {
      color: @ke-color-danger;
      font-size: 12px;
      margin-bottom: 24px;
    }
    .measure-flex-row {
      display: flex;
      align-items: flex-start;
      .flex-item {
        flex-shrink: 1;
        width: 100%;
        .error-tip {
          input {
            border: 1px solid @error-color-1;
          }
        }
      }
      .error-text {
        font-size: 12px;
        color: @error-color-1;
        line-height: 1;
      }
      .el-button {
        margin-top: 5px;
      }
    }
    .tip_box {
      position: relative;
      bottom: 2px;
    }
    .sync-comment-tip {
      word-break: break-all;
    }
    .sync-content {
      color: @base-color;
      cursor: pointer;
    }
    .error-measure {
      .el-input__inner {
        border: 1px solid @error-color-1;
      }
    }
    .measures-width {
      width: 100%;
    }
    .measures-addCC {
      width: 379px;
    }
    .del-margin-more {
      margin-left: 37px !important;
    }
    .value-label {
      color: @text-title-color;
    }
    .el-button.is-disabled,
    .el-button--primary.is-plain.is-disabled {
      background-color: @grey-4;
      color: @line-border-color;
      .el-icon-minus {
        cursor: not-allowed;
      }
      :hover {
        background-color: @grey-4;
        color: @line-border-color;
      }
    }
    .measure-column-multiple {
      margin-top: -10px;
    }
  }
  .el-select-group__wrap {
    .el-select-dropdown__item {
      padding-left: 10px;
    }
    .el-select-group__title {
      padding-left: 10px;
    }
    &:not(:last-child) {
      &::after {
        left: 10px;
        right: 10px;
      }
    }
  }
</style>
