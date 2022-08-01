<template>
  <!-- 模型构建 -->
    <el-dialog class="model-build" :title="title" width="560px" :visible="isShow" v-if="isShow" :close-on-press-escape="false" :close-on-click-modal="false" :append-to-body="true" @close="isShow && closeModal(false)">
      <div>
        <p class="habird-tips ksd-mb-16" v-if="isHybridModel">{{$t('habirdModelBuildTips')}}</p>
        <el-alert
          :title="$t('secondStoragePartitionTips')"
          type="error"
          :closable="false"
          class="ksd-mb-10"
          v-if="secondStoragePartitionTips"
          show-icon>
        </el-alert>
        <el-alert
          :title="$t('changeBuildTypeTips')"
          type="warning"
          :closable="false"
          class="ksd-mb-10"
          v-if="isShowWarning"
          show-icon>
        </el-alert>
        <div class="ksd-title-label-small ksd-mb-10">{{$t('chooseBuildType')}}</div>
        <!-- <div>
          <el-radio-group v-model="buildOrComplete" class="ksd-mb-10">
            <el-radio label="build">{{$t('build')}}</el-radio>
            <common-tip :content="$t('unableComplete')" v-if="!modelDesc.empty_indexes_count">
              <el-radio :disabled="!modelDesc.empty_indexes_count" class="ksd-ml-10" label="complete">{{$t('complete')}}</el-radio>
            </common-tip>
            <el-radio v-else label="complete">{{$t('complete')}}</el-radio>
          </el-radio-group>
        </div> -->
        <el-select v-model="buildType" class="ksd-mb-5" @change="handChangeBuildType" v-if="buildOrComplete == 'build'" :disabled="!datasourceActions.includes('changeBuildType')">
          <el-option :label="$t('incremental')" value="incremental"></el-option>
          <el-option :label="$t('fullLoad')" v-if="!isStreamModel" value="fullLoad"></el-option>
        </el-select>
      </div>
      <div class="tips">{{buildTips}}</div>
      <div v-if="buildType === 'fullLoad'">
        <el-alert
          class="ksd-pt-0 ksd-mt-15"
          :title="$t('segmentTips')"
          type="warning"
          :show-background="false"
          :closable="false"
          show-icon>
        </el-alert>
        <span v-if="isHaveSegment">{{fullLoadBuildTips}}</span>
        <span v-else>{{$t('willAddSegmentTips')}}</span>
      </div>
      <div v-if="buildType === 'incremental' && buildOrComplete === 'build'">
        <el-form class="ksd-mb-20 ksd-mt-15" v-if="isExpand" :model="partitionMeta" ref="partitionForm" :rules="partitionRules"  label-width="85px" label-position="top">
          <el-form-item  :label="$t('partitionDateColumn')" class="clearfix">
            <el-row :gutter="5">
              <el-col :span="12">
                <el-tooltip effect="dark" :content="$t('disableChangePartitionTips')" :disabled="!isNotBatchModel" placement="bottom">
                  <el-select :disabled="isLoadingNewRange || !datasourceActions.includes('changePartition') || isNotBatchModel" v-model="partitionMeta.table" @change="partitionTableChange" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" style="width:100%">
                    <!-- <el-option :label="$t('noPartition')" value=""></el-option> -->
                    <el-option :label="t.alias" :value="t.alias" v-for="t in partitionTables" :key="t.alias">{{t.alias}}</el-option>
                  </el-select>
                </el-tooltip>
              </el-col>
              <el-col :span="12" v-if="partitionMeta.table">
                <el-form-item prop="column">
                  <el-tooltip effect="dark" :content="$t('disableChangePartitionTips')" :disabled="!isNotBatchModel" placement="bottom">
                    <el-select :disabled="isLoadingNewRange || !datasourceActions.includes('changePartition')||isNotBatchModel"
                    @change="partitionColumnChange" v-model="partitionMeta.column" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" filterable style="width:100%">
                    <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!partitionMeta.column.length"></i>
                      <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
                        <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                        <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                      </el-option>
                    </el-select>
                  </el-tooltip>
                </el-form-item>
              </el-col>
            </el-row>
          </el-form-item>
          <el-form-item  :label="$t('dateFormat')" :class="{'is-error': errorFormat}" v-if="partitionMeta.table">
            <el-row :gutter="5">
              <el-col :span="partitionMeta.column&&$store.state.project.projectPushdownConfig&&!isNotBatchModel ? 22 : 24">
                <el-tooltip effect="dark" :content="$t('disableChangePartitionTips')" :disabled="!isNotBatchModel" placement="bottom">
                  <el-select
                    :disabled="isLoadingNewRange || isLoadingFormat || !datasourceActions.includes('changePartition') || isNotBatchModel"
                    style="width:100%"
                    @change="val => partitionColumnFormatChange(val)"
                    v-model="partitionMeta.format"
                    filterable
                    allow-create
                    default-first-option
                    :placeholder="$t('pleaseInputColumn')">
                    <el-option-group>
                      <el-option v-if="prevPartitionMeta.format.indexOf(dateFormatsOptions) === -1&&prevPartitionMeta.format" :label="prevPartitionMeta.format" :value="prevPartitionMeta.format"></el-option>
                      <el-option :label="f.label" :value="f.value" v-for="f in dateFormatsOptions" :key="f.label"></el-option>
                      <!-- <el-option label="" value="" v-if="partitionMeta.column && timeDataType.indexOf(getColumnInfo(partitionMeta.column).datatype)===-1"></el-option> -->
                    </el-option-group>
                    <!-- <el-option-group>
                      <el-option :label="f.label" :value="f.value" v-for="f in dateTimestampFormats" :key="f.label"></el-option>
                    </el-option-group> -->
                  </el-select>
                </el-tooltip>
              </el-col>
              <el-col :span="2" v-if="partitionMeta.column&&$store.state.project.projectPushdownConfig&&!isNotBatchModel">
                <el-tooltip effect="dark" :content="$t('detectFormat')" placement="top">
                  <div style="display: inline-block;">
                    <el-button
                      size="medium"
                      class="auto-detect-btn"
                      :loading="isLoadingFormat"
                      :disabled="isLoadingNewRange || !datasourceActions.includes('changePartition')"
                      icon="el-ksd-icon-data_range_search_old"
                      @click="handleLoadFormat">
                    </el-button>
                  </div>
                </el-tooltip>
              </el-col>
            </el-row>
            <div class="error-format" v-if="errorFormat">{{errorFormat}}</div>
            <div class="pre-format" v-if="formatedDate">{{$t('previewFormat')}}{{formatedDate}}</div>
            <div class="format">{{$t('formatRule')}}
              <span v-if="isExpandFormatRule" @click="isExpandFormatRule = false">{{$t('viewDetail')}}<i class="el-icon-ksd-more_01-copy arrow"></i></span>
              <span v-else @click="isExpandFormatRule = true">{{$t('viewDetail')}}<i class="el-icon-ksd-more_02 arrow"></i></span>
            </div>
            <div class="detail-content" v-if="isExpandFormatRule">
              <p><span class="ksd-mr-2">1. </span><span>{{$t('rule1')}}</span></p>
              <p><span class="ksd-mr-2">2. </span><span>{{$t('rule2')}}</span></p>
              <p><span class="ksd-mr-2">3. </span><span>{{$t('rule3')}}</span></p>
            </div>
            <span style="position:absolute;width:1px; height:0" v-if="partitionMeta.format"></span>
          </el-form-item>
          <el-form-item v-if="((!modelDesc.multi_partition_desc && $store.state.project.multi_partition_enabled) || modelDesc.multi_partition_desc) && partitionMeta.table && !isNotBatchModel">
            <span slot="label">
              <span>{{$t('multilevelPartition')}}</span>
              <el-tooltip effect="dark" :content="$t('multilevelPartitionDesc')" placement="right">
                <i class="el-icon-ksd-what"></i>
              </el-tooltip>
            </span>
            <el-row>
              <el-col :span="11">
              <el-select
                :disabled="isLoadingNewRange || !datasourceActions.includes('changePartition')"
                v-model="partitionMeta.multiPartition"
                :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                filterable
                class="partition-multi-partition"
                popper-class="js_multi-partition"
                style="width:100%"
                @change="changePartitionSetting"
              >
                  <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!partitionMeta.multiPartition.length"></i>
                  <el-option :label="$t('noPartition')" value=""></el-option>
                  <el-option :label="t.name" :value="t.name" v-for="t in subPartitionColumnsOptions" :key="t.name">
                    <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                    <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                  </el-option>
                </el-select>
              </el-col>
            </el-row>
          </el-form-item>
        </el-form>
        <div v-if="partitionMeta.table && partitionMeta.column && partitionMeta.format">
          <div class="divide-block">
            <span v-if="isExpand" @click="toggleShowPartition">{{$t('showLess')}}</span>
            <span v-else @click="toggleShowPartition">{{$t('showMore')}}</span>
            <div class="divide-line"></div>
          </div>
          <div class="ksd-title-label-small ksd-mb-10">{{$t('addRangeTitle')}}</div>
          <el-form :model="modelBuildMeta" ref="buildForm" :rules="rules" label-position="top">
            <!-- <div class="ky-list-title ksd-mt-14">{{$t('buildRange')}}</div> -->
            <!-- <el-form-item prop="isLoadExisted" class="ksd-mt-10 ksd-mb-2">
              <el-radio class="font-medium" v-model="modelBuildMeta.isLoadExisted" :label="true">
                {{$t('loadExistingData')}}
              </el-radio>
              <div class="item-desc">{{$t('loadExistingDataDesc')}}</div>
            </el-form-item> -->
            <el-form-item :class="{'is-error': isShowErrorSegments}" :rule="modelBuildMeta.isLoadExisted ? [] : [{required: true, trigger: 'blur', message: this.$t('dataRangeValValid')}]">
              <!-- <el-radio class="font-medium" v-model="modelBuildMeta.isLoadExisted" :label="false">
                {{$t('customLoadRange')}}
              </el-radio>
              <br/> -->
              <p class="ksd-pt-0 ksd-fs-12 segment-tips"><i class="el-icon-ksd-alert ksd-mr-5 alert-icon"></i>{{$t('segmentTips')}}</p>
              <div class="ky-no-br-space" style="height:32px;">
                <el-date-picker
                  type="datetime"
                  style="width: 44%;"
                  :class="['ksd-mr-5', {'is-error': dateErrorMsg}]"
                  :key="`prevPicker_${new Date(modelBuildMeta.dataRangeVal[0]).getTime()}`"
                  ref="prevPicker"
                  v-model="modelBuildMeta.dataRangeVal[0]"
                  :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
                  @change="(v) => handleChangeDateTime(v, 'start')"
                  value-format="timestamp"
                  :is-auto-complete="true"
                  :format="partitionFormat"
                >
                </el-date-picker>
                <el-date-picker
                  type="datetime"
                  style="width: 44%;"
                  ref="nextPicker"
                  :class="{'is-error': dateErrorMsg}"
                  :key="`prevPicker_${new Date(modelBuildMeta.dataRangeVal[1]).getTime()}`"
                  v-model="modelBuildMeta.dataRangeVal[1]"
                  :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
                  value-format="timestamp"
                  @change="(v) => handleChangeDateTime(v, 'end')"
                  :is-auto-complete="true"
                  :format="partitionFormat"
                >
                </el-date-picker>
                <common-tip :content="noPartition ? $t('partitionFirst'):$t('detectAvailableRange')" placement="top">
                  <el-button
                    size="medium"
                    class="auto-detect-btn ksd-ml-10"
                    v-if="$store.state.project.projectPushdownConfig&&!isStreamModel"
                    :disabled="modelBuildMeta.isLoadExisted || noPartition"
                    :loading="isLoadingNewRange"
                    icon="el-ksd-icon-data_range_search_old"
                    @click="handleLoadNewestRange">
                  </el-button>
                </common-tip>
                <span style="position:absolute;width:1px; height:0" @click="handleLoadNewestRange"></span>
                <span style="position:absolute;width:1px; height:0" v-if="modelBuildMeta.dataRangeVal[0] && modelBuildMeta.dataRangeVal[1]"></span>
              </div>
              <p v-if="dateErrorMsg" class="error-date-range error-msg">{{dateErrorMsg}}</p>
              <div class="timestamp-format-tips" v-if="partitionMeta.format.indexOf('TIMESTAMP') !== -1 && (modelBuildMeta.dataRangeVal[0] || modelBuildMeta.dataRangeVal[1])">
                <span>{{partitionMeta.format}}: </span>
                <span v-if="modelBuildMeta.dataRangeVal[0]">{{partitionMeta.format === 'TIMESTAMP MILLISECOND' ? new Date(modelBuildMeta.dataRangeVal[0]).getTime() : Math.round(new Date(modelBuildMeta.dataRangeVal[0]).getTime() / 1000)}}</span> - 
                <span v-if="modelBuildMeta.dataRangeVal[1]">{{partitionMeta.format === 'TIMESTAMP MILLISECOND' ? new Date(modelBuildMeta.dataRangeVal[1]).getTime() : Math.round(new Date(modelBuildMeta.dataRangeVal[1]).getTime() / 1000)}}</span>
              </div>
            </el-form-item>
          </el-form>
          <el-alert
            v-show="!isShowRangeDateError && modelBuildMeta.dataRangeVal[1]"
            class="date-range-alert"
            :title="$t('segmentDateRangeTips')"
            type="warning"
            :closable="false"
          ></el-alert>
          <div class="error-msg" v-if="isShowRangeDateError">{{loadRangeDateError}}</div>
          <div v-if="isShowErrorSegments" class="error_segments">
            <el-alert type="error" :show-background="false" :closable="false" show-icon>
              <span class="overlaps-tips">{{$t('overlapsTips')}}</span>
              <a href="javascript:;" @click="toggleDetail">{{$t('kylinLang.common.seeDetail')}}
                <i class="el-icon-arrow-down" v-show="!showDetail"></i>
                <i class="el-icon-arrow-up" v-show="showDetail"></i>
              </a>
            </el-alert>
            <table class="ksd-table small-size" v-if="showDetail">
              <tr class="ksd-tr" v-for="(s, index) in errorSegments" :key="index">
                <td>{{s.start | toServerGMTDate}}</td>
                <td>{{s.end | toServerGMTDate}}</td>
              </tr>
            </table>
          </div>
          <div v-if="displaySubPartition">
            <div class="ksd-title-label-small ksd-mt-15">{{$t('multiPartitionValue')}}</div>
            <p class="sub-partition-alert"><i class="icon el-icon-ksd-alert ksd-mr-5"></i>{{$t('subPartitionAlert')}}</p>
            <arealabel
              ref="selectSubPartition"
              :class="['select-sub-partition', {'error-border': duplicateValueError}, 'ksd-mt-5']"
              :duplicateremove="false"
              splitChar=","
              :selectedlabels="selectMultiPartitionValues"
              :isNeedNotUpperCase="true"
              :allowcreate="true"
              :isSignSameValue="true"
              :remoteSearch="true"
              :labels="subPartitionOptions"
              :placeholder="$t('multiPartitionPlaceholder')"
              :remote-method="filterPartitions"
              :datamap="{label: 'label', value: 'value'}"
              :selectGroupOne="subPartitionGroupOne"
              @duplicateTags="checkDuplicateValue"
              @refreshData="refreshPartitionValues"
              @removeTag="removeSelectedMultiPartition">
            </arealabel>
            <p class="duplicate-tips" v-if="duplicateValueError"><span class="error-msg">{{$t('duplicatePartitionValueTip')}}</span><span class="clear-value-btn" @click="removeDuplicateValue"><i class="el-icon-ksd-clear ksd-mr-5"></i>{{$t('removeDuplicateValue')}}</span></p>
          </div>
        </div>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <!-- <div class="ksd-fleft" v-if="$store.state.project.multi_partition_enabled&&modelDesc.multi_partition_desc&&modelDesc.multi_partition_desc.columns.length">
          <el-checkbox v-model="isMultipleBuild">
            <span>{{$t('multipleBuild')}}</span>
            <common-tip placement="top" :content="$t('multipleBuildTip')">
              <span class='el-icon-ksd-what'></span>
            </common-tip>
          </el-checkbox>
        </div> -->
        <el-button @click="closeModal(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <template v-if="isAddSegment">
          <el-button type="primary" :loading="btnLoading" @click="setbuildModel(false, 'onlySave')" :disabled="incrementalDisabled || disableFullLoad" size="medium">{{$t('kylinLang.common.save')}}</el-button>
          <el-button type="primary" :loading="btnLoading" v-if="modelDesc.total_indexes && !multiPartitionEnabled" @click="setbuildModel(true)" :disabled="incrementalDisabled || disableFullLoad" size="medium">{{$t('saveAndBuild')}}</el-button>
          <el-button type="primary" :loading="btnLoading" v-else-if="!multiPartitionEnabled" @click="saveAndAddIndex" :disabled="incrementalDisabled || disableFullLoad" size="medium">{{$t('saveAndAddIndex')}}</el-button>
        </template>
        <template v-else>
          <el-button type="primary" :loading="btnLoading" @click="setbuildModel(true)" :disabled="incrementalDisabled || disableFullLoad || duplicateValueError" size="medium">{{$t(buildType)}}</el-button>
        </template>
      </div>
    </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from 'store'
  import { handleError, transToUTCMs, getGmtDateFromUtcLike, kylinMessage } from 'util/business'
  import { handleSuccessAsync, transToServerGmtTime, isDatePartitionType, isStreamingPartitionType, isSubPartitionType, kylinConfirm, split_array } from 'util/index'
  import locales from './locales'
  import store, { types } from './store'
  import NModel from '../../ModelEdit/model.js'
  import { BuildIndexStatus } from 'config/model'
  import { dateFormats, timestampFormats, dateTimestampFormats } from 'config'
  import arealabel from '../../../../common/area_label.vue'
  import moment from 'moment'

  vuex.registerModule(['modals', 'ModelBuildModal'], store)

  @Component({
    components: {
      arealabel
    },
    computed: {
      ...mapGetters([
        'currentSelectedProject',
        'datasourceActions'
      ]),
      ...mapState('ModelBuildModal', {
        isShow: state => state.isShow,
        title: state => state.title,
        source: state => state.source,
        type: state => state.type,
        isAddSegment: state => state.isAddSegment,
        buildOrComp: state => state.buildOrComp,
        isHaveSegment: state => state.isHaveSegment,
        disableFullLoad: state => state.disableFullLoad,
        modelDesc: state => state.form.modelDesc,
        modelInstance: state => state.form.modelInstance || state.form.modelDesc && new NModel(state.form.modelDesc) || null,
        callback: state => state.callback
      }),
      ...mapState({
        multiPartitionEnabled: state => state.project.multi_partition_enabled
      })
    },
    methods: {
      ...mapActions({
        buildModel: 'MODEL_BUILD',
        checkDataRange: 'CHECK_DATA_RANGE',
        buildFullLoadModel: 'MODEL_FULLLOAD_BUILD',
        buildIndex: 'BUILD_INDEX',
        fetchNewestModelRange: 'GET_MODEL_NEWEST_RANGE',
        fetchPartitionFormat: 'FETCH_PARTITION_FORMAT',
        updataModel: 'UPDATE_MODEL',
        autoFixSegmentHoles: 'AUTO_FIX_SEGMENT_HOLES',
        setModelPartition: 'MODEL_PARTITION_SET',
        fetchSubPartitionValues: 'FETCH_SUB_PARTITION_VALUES',
        validateDateFormat: 'VALIDATE_DATE_FORMAT'
      }),
      ...mapMutations('ModelBuildModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      }),
      ...mapActions('DetailDialogModal', {
        callGlobalDetailDialog: 'CALL_MODAL'
      })
    },
    locales
  })
  export default class ModelBuildModal extends Vue {
    moment = moment
    btnLoading = false
    isLoadingNewRange = false
    modelBuildMeta = {
      dataRangeVal: [],
      isLoadExisted: false
    }
    rules = {
      // dataRangeVal: [{
      //   validator: this.validateRange, trigger: 'blur'
      // }]
    }
    loadRangeDateError = ''
    isShowRangeDateError = false
    isShowErrorSegments = false
    showDetail = false
    errorSegments = []
    buildType = ''
    defaultBuildType = ''
    buildOrComplete = 'build'
    partitionMeta = {
      table: '',
      column: '',
      format: '',
      multiPartition: ''
    }
    prevPartitionMeta = {
      table: '',
      column: '',
      format: '',
      multiPartition: ''
    }
    partitionRules = {
      column: [{validator: this.validateBrokenColumn, trigger: 'change'}]
    }
    isLoadingFormat = false
    dateFormats = dateFormats
    timestampFormats = timestampFormats
    dateTimestampFormats = dateTimestampFormats
    isExpand = true
    isShowWarning = false
    isWillAddIndex = false
    inputMultiType = 'select'
    multiPartitionValues = []
    multiPartitionValueOptions = []
    selectMultiPartitionValues = []
    modelSubPartitionValues = []
    isMultipleBuild = false
    duplicateValueError = false
    subPartitionOptions = []
    subPartitionGroupOne = []
    dateErrorMsg = ''
    formatedDate = ''
    errorFormat = ''
    isExpandFormatRule = false
    timestamp = Date.now().toString(32)
    secondStoragePartitionTips = false

    @Watch('buildType')
    changeBuildType (newVal, oldVal) {
      newVal !== oldVal && (this.duplicateValueError = false)
    }

    toggleInputMultiPartition () {
      this.inputMultiType = this.inputMultiType === 'select' ? 'textarea' : 'select'
    }

    get modelId () {
      // batch 和 streaming 的都是取 uuid
      if (this.modelDesc.model_type !== 'HYBRID') {
        return this.modelDesc.uuid
      } else { // HYBRID 模式的，传批数据id
        return this.modelDesc.batch_id
      }
    }

    get toggleText () {
      return this.inputMultiType === 'select' ? this.$t('selectInput') : this.$t('batchInput')
    }

    get displaySubPartition () {
      return this.source !== 'addSegment' && this.$store.state.project.multi_partition_enabled && this.partitionMeta.multiPartition
    }

    get isStreamModel () {
      const factTable = this.modelInstance.getFactTable()
      return factTable.source_type === 1 || this.modelDesc.model_type === 'STREAMING'
    }
    get isNotBatchModel () {
      const factTable = this.modelInstance.getFactTable()
      return factTable.source_type === 1 || this.modelInstance.model_type !== 'BATCH'
    }
    get isHybridModel () {
      return this.modelDesc.model_type === 'HYBRID'
    }
    get dateFormatsOptions () {
      return this.isNotBatchModel ? timestampFormats : dateFormats
    }

    refreshPartitionValues (val) {
      this.multiPartitionValues = val
    }

    removeSelectedMultiPartition () {}

    toggleDetail () {
      this.showDetail = !this.showDetail
    }
    get fullLoadBuildTips () {
      return this.$t('fullLoadBuildTips', {storageSize: Vue.filter('dataSize')(this.modelDesc.storage)})
    }
    get noPartition () {
      return !(this.partitionMeta.table && this.partitionMeta.column && this.partitionMeta.format)
    }
    get incrementalDisabled () {
      return !(this.partitionMeta.table && this.partitionMeta.column && this.partitionMeta.format && this.modelBuildMeta.dataRangeVal.length) && this.buildType === 'incremental'
    }
    get partitionFormat () {
      if (this.partitionMeta.format === 'TIMESTAMP SECOND') {
        return 'yyyy-MM-dd HH:mm:ss'
      }
      if (this.partitionMeta.format === 'TIMESTAMP MILLISECOND') {
        return 'yyyy-MM-dd HH:mm:ss.SSS'
      }
      return this.partitionMeta.format
    }
    handChangeBuildType () {
      if (this.buildType === 'incremental' && !this.partitionMeta.table) {
        this.isExpand = true
        this.partitionMeta.table = this.partitionTables[0].alias
      }
      this.dateErrorMsg = ''
      this.secondStoragePartitionTips = false
      this.isShowWarning = this.isHaveSegment && (this.buildType !== this.defaultBuildType || JSON.stringify(this.prevPartitionMeta) !== JSON.stringify(this.partitionMeta))
    }
    handleChangeDateTime (val, pos) {
      // 仅在 IE 浏览器下做兼容处理
      if (val && navigator.userAgent.indexOf('Windows NT') >= 0) {
        const newDate = function (date) {
          if (typeof date === 'string' && /^\d{4}-\d{2}-\d{2}/.test(date)) {
            return new Date(date.replace(/-/g, '/'))
          } else {
            return new Date(date)
          }
        }
        let dateVal = this.modelBuildMeta.dataRangeVal
        pos === 'start' ? dateVal.splice(0, 1, newDate(val).getTime()) : dateVal.splice(1, 1, newDate(val).getTime())
        this.$set(this.modelBuildMeta, 'dataRangeVal', dateVal)
      }
      this.resetError()
    }
    validateBrokenColumn (rule, value, callback) {
      if (value) {
        if (this.checkIsBroken(this.brokenPartitionColumns, value)) {
          return callback(new Error(this.$t('noColumnFund')))
        }
      }
      if (!value && this.partitionMeta.table) {
        return callback(new Error(this.$t('pleaseInputColumn')))
      }
      callback()
    }

    checkIsBroken (brokenKeys, key) {
      if (key) {
        return ~brokenKeys.indexOf(key)
      }
      return false
    }
    // 获取破损的partition keys
    get brokenPartitionColumns () {
      if (this.partitionMeta.table) {
        let ntable = this.modelInstance.getTableByAlias(this.partitionMeta.table)
        return this.modelInstance.getBrokenModelLinksKeys(ntable.guid, [this.partitionMeta.column])
      }
      return []
    }

    get selectedTable () {
      if (this.partitionMeta.table) {
        for (let i = 0; i < this.partitionTables.length; i++) {
          if (this.partitionTables[i].alias === this.partitionMeta.table) {
            return this.partitionTables[i]
          }
        }
      }
    }

    async handleLoadFormat () {
      try {
        this.isLoadingFormat = true
        const response = await this.fetchPartitionFormat({ project: this.currentSelectedProject, table: this.selectedTable.name, partition_column: this.partitionMeta.column })
        this.partitionMeta.format = await handleSuccessAsync(response)
        this.partitionColumnFormatChange(this.partitionMeta.format)
        this.isLoadingFormat = false
      } catch (e) {
        this.isLoadingFormat = false
        handleError(e)
      }
    }

    get columns () {
      if (!this.isShow || this.partitionMeta.table === '') {
        return []
      }
      let result = []
      let factTable = this.modelInstance.getFactTable()
      if (factTable) {
        factTable.columns.forEach((x) => {
          if (this.isNotBatchModel && isStreamingPartitionType(x.datatype)) {
            result.push(x)
          } else if (!this.isNotBatchModel && isDatePartitionType(x.datatype)) {
            result.push(x)
          }
        })
      }
      // let ccColumns = this.modelInstance.getComputedColumns()
      // let cloneCCList = objectClone(ccColumns)
      // cloneCCList.forEach((x) => {
      //   let cc = {
      //     name: x.columnName,
      //     datatype: x.datatype
      //   }
      //   result.push(cc)
      // })
      return result
    }

    get subPartitionColumnsOptions () {
      if (!this.isShow || this.partitionMeta.table === '') {
        return []
      }
      let result = []
      let factTable = this.modelInstance.getFactTable()
      if (factTable) {
        factTable.columns.forEach((x) => {
          if (isSubPartitionType(x.datatype)) {
            result.push(x)
          }
        })
      }
      return result
    }

    // 分区设置改变
    changePartitionSetting () {
      if (JSON.stringify(this.prevPartitionMeta) !== JSON.stringify(this.partitionMeta) || this.buildType !== this.defaultBuildType) {
        if (this.isHaveSegment) {
          this.isShowWarning = true
        }
      } else {
        this.isShowWarning = false
      }
    }

    partitionTableChange () {
      this.partitionMeta.column = ''
      this.partitionMeta.format = ''
      this.$refs.partitionForm.validate()
      this.modelBuildMeta.dataRangeVal = []
      this.secondStoragePartitionTips = false
      this.changePartitionSetting()
    }

    partitionColumnChange () {
      // this.partitionMeta.format = 'yyyy-MM-dd'
      // this.$refs.partitionForm.validate()
      this.modelBuildMeta.dataRangeVal = []
      this.secondStoragePartitionTips = false
      this.changePartitionSetting()
    }

    async partitionColumnFormatChange (val) {
      this.formatedDate = ''
      this.errorFormat = ''
      if (val) {
        try {
          const res = await this.validateDateFormat({partition_date_column: this.partitionMeta.column, partition_date_format: this.partitionMeta.format})
          this.formatedDate = await handleSuccessAsync(res)
        } catch (e) {
          this.errorFormat = e.body.msg
          this.formatedDate = ''
        }
      }
      this.modelBuildMeta.dataRangeVal = []
      this.changePartitionSetting()
    }

    get partitionTables () {
      let result = []
      if (this.isShow && this.modelInstance) {
        Object.values(this.modelInstance.tables).forEach((nTable) => {
          if (nTable.kind === 'FACT') {
            result.push(nTable)
          }
        })
      }
      return result
    }

    toggleShowPartition () {
      this.isExpand = !this.isExpand
    }

    showToolTip (value) {
      let len = 0
      value.split('').forEach((v) => {
        if (/[\u4e00-\u9fa5]/.test(v)) {
          len += 2
        } else {
          len += 1
        }
      })
      return len <= 15
    }

    get buildTips () {
      if (this.buildType === 'incremental') {
        return this.$t('incrementalTips')
      } else if (this.buildType === 'fullLoad') {
        return this.$t('fullLoadTips', {storageSize: Vue.filter('dataSize')(this.modelDesc.storage)})
      }
    }
    get format () {
      return this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_format || 'yyyy-MM-dd'
    }
    formatPartition () {
      let format = ''
      switch (this.partitionMeta.format) {
        case 'yyyy-MM-dd':
        case 'yyyyMMdd':
        case 'yyyy/MM/dd':
          format = 'YYYY/MM/DD'
          break
        case 'yyyy-MM':
        case 'yyyyMM':
          format = 'YYYY/MM'
          break
        case 'yyyy-MM-dd HH:mm:ss':
          format = 'YYYY/MM/DD HH:mm:ss'
          break
        case 'yyyy-MM-dd HH:mm:ss.SSS':
          format = 'YYYY/MM/DD HH:mm:ss.SSS'
          break
      }
      return format
    }
    validateRange (value) {
      return new Promise((resolve, reject) => {
        const [ startValue, endValue ] = value
        let format = this.formatPartition()
        const formatTimestampStart = !format ? startValue : (startValue && new Date(moment(new Date(startValue)).format(format)).getTime())
        const formatTimestampEnd = !format ? endValue : (endValue && new Date(moment(new Date(endValue)).format(format)).getTime())
        const isLoadExisted = this.modelBuildMeta.isLoadExisted

        if ((!startValue || !endValue || transToUTCMs(formatTimestampStart) > transToUTCMs(formatTimestampEnd)) && !isLoadExisted) {
          // callback(new Error(this.$t('invaildDate')))
          this.dateErrorMsg = this.$t('invaildDate')
          reject()
        } else if (startValue && endValue && transToUTCMs(formatTimestampStart) === transToUTCMs(formatTimestampEnd) && !isLoadExisted) {
          // callback(new Error(this.$t('invaildDateNoEqual')))
          this.dateErrorMsg = this.$t('invaildDateNoEqual')
          reject()
        } else {
          // callback()
          this.dateErrorMsg = ''
          resolve()
        }
      })
    }
    @Watch('isShow')
    async initModelBuldRange () {
      if (this.isShow) {
        this.buildType = this.type
        this.defaultBuildType = this.type
        this.buildOrComplete = this.buildOrComp
        this.modelBuildMeta.dataRangeVal = []
        if (this.modelDesc.last_build_end && this.buildType === 'incremental') {
          let lastBuildDate = getGmtDateFromUtcLike(+this.modelDesc.last_build_end)
          if (lastBuildDate) {
            this.modelBuildMeta.dataRangeVal.push(lastBuildDate, lastBuildDate)
          }
        }
        this.$nextTick(() => {
          this.$refs.partitionForm && this.$refs.partitionForm.validate()
        })
        if (this.modelDesc && this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_column && this.buildType === 'incremental') {
          let named = this.modelDesc.partition_desc.partition_date_column.split('.')
          this.partitionMeta.table = this.prevPartitionMeta.table = named[0]
          this.partitionMeta.column = this.prevPartitionMeta.column = named[1]
          this.partitionMeta.format = this.prevPartitionMeta.format = this.modelDesc.partition_desc.partition_date_format
          this.partitionMeta.multiPartition = this.prevPartitionMeta.multiPartition = this.modelDesc.multi_partition_desc && this.modelDesc.multi_partition_desc.columns[0] && this.modelDesc.multi_partition_desc.columns[0].split('.')[1] || ''
          this.isExpand = false
        } else {
          this.isExpand = true
        }
        this.isShowWarning = false
        if (this.$store.state.project.multi_partition_enabled && this.modelDesc.multi_partition_desc && this.modelDesc.multi_partition_desc.columns.length) {
          try {
            const res = await this.fetchSubPartitionValues({ project: this.currentSelectedProject, model_id: this.modelId })
            const data = await handleSuccessAsync(res)
            this.modelSubPartitionValues = data.map((p) => {
              return p.partition_value[0]
            })
            this.modelSubPartitionValues.length && (this.subPartitionGroupOne = [{label: this.$t('selectAllSubPartitions'), value: `select_all_${this.timestamp}`}])
            this.subPartitionOptions = this.modelSubPartitionValues.slice(0, 50).map(it => ({label: it, value: it}))
          } catch (e) {
            handleError(e)
          }
        }
      } else {
        this.modelBuildMeta.dataRangeVal = []
        this.resetForm()
      }
    }
    resetForm () {
      this.partitionMeta = {
        table: '',
        column: '',
        format: '',
        multiPartition: ''
      }
      this.prevPartitionMeta = { table: '', column: '', format: '', multiPartition: '' }
      this.filterCondition = ''
      this.isLoadingSave = false
      this.isLoadingFormat = false
      this.secondStoragePartitionTips = false
    }
    async handleLoadNewestRange () {
      this.$refs.buildForm && this.$refs.buildForm.clearValidate()
      this.isLoadingNewRange = true
      this.resetError()
      const partition_desc = {
        partition_date_column: this.partitionMeta.table + '.' + this.partitionMeta.column,
        partition_date_format: this.partitionMeta.format
      }
      try {
        const submitData = {
          project: this.currentSelectedProject,
          model: this.modelId,
          partition_desc: partition_desc
        }
        const response = await this.fetchNewestModelRange(submitData)
        if (submitData.model !== this.modelId) { // 避免ajax耗时太长导致会覆盖新的model的load range数据
          return
        }
        if (response.body.code === '000') {
          const result = await handleSuccessAsync(response)
          const startTime = +result.start_time
          const endTime = +result.end_time
          this.modelBuildMeta.dataRangeVal = [ getGmtDateFromUtcLike(startTime), getGmtDateFromUtcLike(endTime) ]
        } else if (response.body.code === '999') {
          this.loadRangeDateError = response.body.msg
          this.isShowRangeDateError = true
        }
      } catch (e) {
        handleError(e)
      }
      this.isLoadingNewRange = false
    }
    resetError () {
      this.loadRangeDateError = ''
      this.dateErrorMsg = ''
      this.isShowRangeDateError = false
      this.isShowErrorSegments = false
      this.errorSegments = []
      this.showDetail = false
    }
    closeModal (isSubmit) {
      this.isLoadingNewRange = false
      this.btnLoading = false
      this.$refs.buildForm && this.$refs.buildForm.resetFields()
      this.isShowErrorSegments = false
      this.errorSegments = []
      this.showDetail = false
      this.isWillAddIndex = false
      this.multiPartitionValues = []
      this.subPartitionGroupOne = []
      this.dateErrorMsg = ''
      this.isExpandFormatRule = false
      this.resetError()
      this.hideModal()
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    _buildModel ({start, end, modelId, modelName, isBuild, partition_desc, multi_partition_desc, multi_partition_values, build_all_sub_partitions, segment_holes}) {
      const partitionValuesArr = split_array(multi_partition_values, 1)
      this.buildModel({
        model_id: modelId,
        data: {
          start: start,
          end: end,
          build_all_indexes: isBuild,
          partition_desc: partition_desc,
          sub_partition_values: partitionValuesArr,
          segment_holes: segment_holes || [],
          // parallel_build_by_segment: this.isMultipleBuild,
          multi_partition_desc,
          build_all_sub_partitions,
          project: this.currentSelectedProject
        }
      }).then(() => {
        this.btnLoading = false
        // this.$emit('refreshModelList')
        if (this.isWillAddIndex) {
          this.$emit('isWillAddIndex', modelName)
        } else {
          if (isBuild) {
            this.$message({
              dangerouslyUseHTMLString: true,
              type: 'success',
              customClass: 'build-full-load-success',
              duration: 10000,
              showClose: true,
              message: (
                <div>
                  <span>{this.$t('kylinLang.common.buildSuccess')}</span>
                  <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
                </div>
              )
            })
          } else {
            kylinMessage(this.$t('kylinLang.common.submitSuccess'))
          }
        }
        this.closeModal(true)
      }, (res) => {
        this.btnLoading = false
        res && handleError(res)
      })
    }
    saveAndAddIndex () {
      this.isWillAddIndex = true
      this.setbuildModel(false)
    }
    async setbuildModel (isBuild, type) {
      this.btnLoading = true
      try {
        if (this.buildType === 'incremental' && this.buildOrComplete === 'build') {
          await this.validateRange(this.modelBuildMeta.dataRangeVal)
          this.$refs.buildForm.validate(async (valid) => {
            if (!valid) {
              this.btnLoading = false
              return
            }
            await (this.$refs.rangeForm && this.$refs.rangeForm.validate()) || Promise.resolve()
            await (this.$refs.partitionForm && this.$refs.partitionForm.validate()) || Promise.resolve()
            const { column, table } = this.partitionMeta
            if (this.modelDesc.second_storage_enabled && !this.modelDesc.simplified_dimensions.map(it => it.column).includes(`${table}.${column}`)) {
              this.secondStoragePartitionTips = true
              this.btnLoading = false
              return
            }
            if (type && type === 'onlySave') {
              try {
                await this.$msgbox({
                  title: this.$t('kylinLang.common.tip'),
                  message: <div><p>{this.$t('onlySaveTip1')}</p><p>{this.$t('onlySaveTip2')}</p></div>,
                  showCancelButton: true,
                  confirmButtonText: this.$t('kylinLang.common.save')
                })
              } catch (e) {
                this.btnLoading = false
                return
              }
            }
            const partition_desc = {}
            if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
              if (this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format || this.prevPartitionMeta.multiPartition !== this.partitionMeta.multiPartition) {
                // await kylinConfirm(this.$t('changeSegmentTip1', {tableColumn: `${this.partitionMeta.table}.${this.partitionMeta.column}`, dateType: this.partitionMeta.format, modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
                try {
                  await kylinConfirm(this.$t('changeSegmentTips'), {confirmButtonText: this.$t('kylinLang.common.save'), type: 'warning', dangerouslyUseHTMLString: true}, this.$t('kylinLang.common.tip'))
                } catch (e) {
                  this.btnLoading = false
                  return false
                }
              }
            }
            partition_desc.partition_date_column = this.partitionMeta.table + '.' + this.partitionMeta.column
            partition_desc.partition_date_format = this.partitionMeta.format
            let start = null
            let end = null
            if (!this.modelBuildMeta.isLoadExisted) {
              start = transToUTCMs(this.modelBuildMeta.dataRangeVal[0])
              end = transToUTCMs(this.modelBuildMeta.dataRangeVal[1])
            }
            // 如果切换分区列或者构建方式，会清空segment，不用检测
            const isChangePatition = this.prevPartitionMeta.table && (this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format)
            const isChangeBuildType = !this.prevPartitionMeta.table && this.isHaveSegment
            const multi_partition_desc = this.partitionMeta.multiPartition ? {columns: [this.partitionMeta.table + '.' + this.partitionMeta.multiPartition]} : null
            let build_all_sub_partitions = false
            const partitionValues = JSON.parse(JSON.stringify(this.multiPartitionValues))
            if (this.multiPartitionValues.includes(`select_all_${this.timestamp}`)) {
              const index = this.multiPartitionValues.indexOf(`select_all_${this.timestamp}`)
              build_all_sub_partitions = true
              index >= 0 && partitionValues.splice(index, 1)
            }
            if (isChangePatition || isChangeBuildType) {
              this._buildModel({start: start, end: end, modelId: this.modelId, modelName: this.modelDesc.alias, isBuild: isBuild, partition_desc: partition_desc, multi_partition_desc, multi_partition_values: partitionValues, build_all_sub_partitions})
            } else {
              let res
              try {
                res = await this.checkDataRange({modelId: this.modelId, project: this.currentSelectedProject, start: start, end: end})
              } catch (e) {
                this.btnLoading = false
                handleError(e)
              }
              const data = await handleSuccessAsync(res)
              if (data) {
                if (data.overlap_segments.length) {
                  this.btnLoading = false
                  this.isShowErrorSegments = true
                  this.errorSegments = data.overlap_segments
                } else if (data.segment_holes.length) {
                  const tableData = []
                  let selectSegmentHoles = []
                  const segmentHoles = data.segment_holes
                  segmentHoles.forEach((seg) => {
                    const obj = {}
                    obj['start'] = transToServerGmtTime(seg.start)
                    obj['end'] = transToServerGmtTime(seg.end)
                    obj['date_range_start'] = seg.start
                    obj['date_range_end'] = seg.end
                    tableData.push(obj)
                  })
                  try {
                    await this.callGlobalDetailDialog({
                      msg: this.$t('segmentHoletips', {modelName: this.modelDesc.name}),
                      title: this.$t('fixSegmentTitle'),
                      detailTableData: tableData,
                      detailColumns: [
                        {column: 'start', label: this.$t('kylinLang.common.startTime')},
                        {column: 'end', label: this.$t('kylinLang.common.endTime')}
                      ],
                      isShowSelection: true,
                      dialogType: 'warning',
                      showDetailBtn: false,
                      needResolveCancel: true,
                      cancelText: this.$t('ignore'),
                      submitText: this.$t('fixAndBuild'),
                      onlyCloseDialogReject: true,
                      customCallback: async (segments) => {
                        selectSegmentHoles = segments.map((seg) => {
                          return {start: seg.date_range_start, end: seg.date_range_end}
                        })
                        try {
                          this._buildModel({start: start, end: end, modelId: this.modelId, modelName: this.modelDesc.alias, isBuild: isBuild, partition_desc: partition_desc, multi_partition_desc, multi_partition_values: partitionValues, build_all_sub_partitions, segment_holes: selectSegmentHoles})
                        } catch (e) {
                          handleError(e)
                        }
                      }
                    })
                    this._buildModel({start: start, end: end, modelId: this.modelId, modelName: this.modelDesc.alias, isBuild: isBuild, partition_desc: partition_desc, multi_partition_desc, multi_partition_values: partitionValues, build_all_sub_partitions})
                  } catch (e) {
                    this.btnLoading = false
                    handleError(e)
                  }
                } else {
                  this._buildModel({start: start, end: end, modelId: this.modelId, modelName: this.modelDesc.alias, isBuild: isBuild, partition_desc: partition_desc, multi_partition_desc, multi_partition_values: partitionValues, build_all_sub_partitions})
                }
              }
            }
          })
        } else if (this.buildType === 'fullLoad' && this.buildOrComplete === 'build') {
          if (this.modelDesc && this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_column) {
            // await kylinConfirm(this.$t('changeBuildTypeTipsConfirm', {modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
            try {
              await kylinConfirm(this.$t('changeSegmentTips'), {confirmButtonText: this.$t('kylinLang.common.save'), type: 'warning', dangerouslyUseHTMLString: true}, this.$t('kylinLang.common.tip'))
            } catch (e) {
              this.btnLoading = false
              return false
            }
            this.btnLoading = true
            await this.setModelPartition({modelId: this.modelId, project: this.currentSelectedProject, partition_desc: null})
          }
          this.buildFullLoadModel({
            model_id: this.modelId,
            start: null,
            end: null,
            build_all_indexes: isBuild,
            project: this.currentSelectedProject
          }).then(async () => {
            this.btnLoading = false
            // await this.$emit('refreshModelList')
            if (this.isWillAddIndex) {
              this.$emit('isWillAddIndex')
            } else {
              if (isBuild) {
                this.$message({
                  dangerouslyUseHTMLString: true,
                  type: 'success',
                  customClass: 'build-full-load-success',
                  duration: 10000,
                  showClose: true,
                  message: (
                    <div>
                      <span>{this.$t('kylinLang.common.buildSuccess')}</span>
                      <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
                    </div>
                  )
                })
              } else {
                kylinMessage(this.$t('kylinLang.common.submitSuccess'))
              }
            }
            this.closeModal(true)
          }, (res) => {
            this.btnLoading = false
            res && handleError(res)
          })
        }
      } catch (e) {
        this.btnLoading = false
        handleError(e)
      }
    }
    // 补全索引已拿掉
    async completeBuildModel () {
      if (this.modelDesc.segment_holes.length) {
        const segmentHoles = this.modelDesc.segment_holes
        try {
          const tableData = []
          let selectSegmentHoles = []
          segmentHoles.forEach((seg) => {
            const obj = {}
            obj['start'] = transToServerGmtTime(seg.date_range_start)
            obj['end'] = transToServerGmtTime(seg.date_range_end)
            obj['date_range_start'] = seg.date_range_start
            obj['date_range_end'] = seg.date_range_end
            tableData.push(obj)
          })
          await this.callGlobalDetailDialog({
            msg: this.$t('segmentHoletips', {modelName: this.modelDesc.name}),
            title: this.$t('fixSegmentTitle'),
            detailTableData: tableData,
            detailColumns: [
              {column: 'start', label: this.$t('kylinLang.common.startTime')},
              {column: 'end', label: this.$t('kylinLang.common.endTime')}
            ],
            isShowSelection: true,
            dialogType: 'warning',
            showDetailBtn: false,
            needResolveCancel: true,
            cancelText: this.$t('ignore'),
            submitText: this.$t('fixAndBuild'),
            customCallback: async (segments) => {
              selectSegmentHoles = segments.map((seg) => {
                return {start: seg.date_range_start, end: seg.date_range_end}
              })
              try {
                await this.autoFixSegmentHoles({project: this.currentSelectedProject, model_id: this.modelId, segment_holes: selectSegmentHoles})
              } catch (e) {
                handleError(e)
              }
              this.confirmBuild()
            }
          })
          this.confirmBuild()
        } catch (e) {
          e !== 'cancel' && handleError(e)
        }
      } else {
        this.confirmBuild()
      }
    }

    async confirmBuild () {
      try {
        this.btnLoading = true
        let res = await this.buildIndex({
          project: this.currentSelectedProject,
          model_id: this.modelId
        })
        let data = await handleSuccessAsync(res)
        this.handleBuildIndexTip(data)
        this.closeModal(true)
        // this.$emit('refreshModelList')
      } catch (e) {
        handleError(e)
      } finally {
        this.btnLoading = false
      }
    }
    handleBuildIndexTip (data) {
      let tipMsg = ''
      if (data.type === BuildIndexStatus.NORM_BUILD) {
        tipMsg = this.$t('kylinLang.model.buildIndexSuccess')
        this.$message({message: tipMsg, type: 'success'})
        return
      }
      if (data.type === BuildIndexStatus.NO_LAYOUT) {
        tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.index')})
      } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
        tipMsg += this.$t('kylinLang.model.buildIndexFail1', {modelName: this.modelDesc.name})
      }
      this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
    }
    // 检测是否输入了重复的子分区值
    checkDuplicateValue (type) {
      this.duplicateValueError = type
    }
    removeDuplicateValue () {
      this.$refs.selectSubPartition && this.$refs.selectSubPartition.clearDuplicateValue()
    }

    filterPartitions (query) {
      if (query) {
        this.subPartitionGroupOne = []
      } else if (this.modelSubPartitionValues.length) {
        this.subPartitionGroupOne = [{label: this.$t('selectAllSubPartitions'), value: `select_all_${this.timestamp}`}]
      }
      this.subPartitionOptions = this.modelSubPartitionValues.filter(item => item.indexOf(query) >= 0).slice(0, 50).map(it => ({label: it, value: it}))
    }

    jumpToJobs () {
      this.$router.push('/monitor/job')
    }
    created () {
      this.$on('buildModel', this._buildModel)
    }
  }
</script>
<style lang="less">
@import '../../../../../assets/styles/variables.less';
  .model-build {
    .error-format {
      color: @error-color-1;
      font-size: 12px;
      line-height: 16px;
    }
    .timestamp-format-tips,
    .pre-format {
      color: @text-normal-color;
      font-size: 14px;
      margin-top: 4px;
      background-color: @base-background-color;
      height: 26px;
      line-height: 26px;
      border-radius: 4px;
      display: inline-block;
      padding: 0 4px;
    }
    .format {
      font-size: 12px;
      line-height: 16px;
      color: @text-disabled-color;
      margin-top: 4px;
      span {
        color: @base-color;
        cursor: pointer;
        .arrow {
          transform: rotate( 90deg );
          margin-left: 3px;
        }
      }
    }
    .date-range-alert {
      padding: 10px 0;
      background-color: transparent;
    }
    .auto-detect-btn {
      line-height: 22px;
    }
    .detail-content {
      background-color: @base-background-color-1;
      padding: 8px 16px;
      box-sizing: border-box;
      font-size: 12px;
      color: @text-normal-color;
      line-height: 16px;
      margin-top: 5px;
      p {
        display: flex;
      }
    }
    .habird-tips {
      font-size: 14px;
    }
    .tips {
      font-size: 12px;
      color: @text-disabled-color;
    }
    .add-title {
      font-weight: bold;
      color: @text-title-color;
    }
    .item-desc {
      font-size: 12px;
      line-height: 1;
    }
    .el-date-editor.is-error {
      .el-input__inner {
        border: 1px solid @error-color-1;
      }
    }
    .error-date-range {
      padding: 0;
      margin: 0;
      line-height: 1;
    }
    .error-msg {
      color: @error-color-1;
      font-size: 12px;
      margin-top: 5px;
    }
    .error_segments {
      .el-alert__icon {
        padding-left: 2px;
      }
      .overlaps-tips {
        color: @error-color-1;
      }
    }
    .error_segments a:hover {
      text-decoration: none;
    }
    .divide-block {
      color: @base-color;
      font-size: 12px;
      text-align: center;
      cursor: pointer;
    }
    .divide-line {
      border-top: 1px solid @line-border-color;
      margin-top: 10px;
      margin-bottom: 20px;
    }
    .area-input {
      border: 1px solid @line-border-color3;
      height: 60px;
      overflow-y: auto;
      margin-top: 5px;
      .el-input__inner {
        border: none;
        padding: 0 26px 0 12px;
      }
      .el-select .el-input__suffix {
        display: none;
      }
    }
    .duplicate-tips {
      font-size: 12px;
      margin-top: 5px;
      .clear-value-btn {
        cursor: pointer;
        color: @text-normal-color;
        &:hover {
          color: @base-color;
        }
      }
    }
    .sub-partition-alert {
      font-size: 12px;
      color: @text-title-color;
      margin: 10px 0;
      .icon {
        color: @text-disabled-color;
      }
    }
    .select-sub-partition.error-border {
      .el-input__inner {
        border-color: @error-color-1;
      }
    }
    .select-sub-partition {
      // 针对 IE 游览器下中文输入法下输入文字后面会自带叉叉问题
      .el-select__input::-ms-clear {
        display: none;
      }
    }
    .el-icon-info {
      font-size: 14px;
      padding-left: 1px;
    }
    .segment-tips {
      line-height: 18px;
      margin-bottom: 10px;
    }
    .alert-icon {
      color: @text-disabled-color;
    }
  }
  .build-full-load-success {
    padding: 10px 30px 10px 10px;
    align-items: center;
  }
</style>
