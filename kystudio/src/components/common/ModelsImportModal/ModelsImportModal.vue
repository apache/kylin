<template>
  <el-dialog class="models-import-modal"
    :width="step !== 'second' ? '480px' : '900px'"
    :title="$t('importModelsMetadata')"
    :visible="true"
    v-if="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="handleClose"
    @closed="handleClosed">
    <p class="modal-import-tip" v-if="step === 'first'"><i class="el-icon-ksd-info ksd-mr-5"></i>{{$t('importModalTips')}}</p>
    <p v-if="step === 'second'"><span v-html="getParseImportModal"></span><span class="view-import-tips" @click="toggerDetailTips">{{$t('viewDetails')}}<i :class="['el-icon-arrow-up ksd-ml-2', {'reverse': showDetailTips}]"></i></span></p>
    <div class="import-details" v-show="showDetailTips && step === 'second'">
      <p>* <span v-html="$t('importTips1')" /></p>
      <p>* <span v-html="$t('importTips2')" /></p>
      <p>* <span v-html="$t('importTips3')" /></p>
    </div>
    <!-- 上传元数据zip包界面 -->
    <el-form ref="form" :model="form" :rules="rules" v-loading="isSubmiting" v-if="step === 'first'">
      <el-form-item class="file-upload" prop="file" :label="$t('selectFile')">
        <el-input :value="form.file && form.file.name" :placeholder="$t('placeholder')" :class="{'error-tip': showError && form.file}" />
        <input class="file-input" type="file" accept="application/zip" title="" @change="handleSelectFile" />
        <p class="error-msg" v-if="showError && form.file">{{validateErrorMsg}}</p>
      </el-form-item>
      <!-- <el-form-item class="checkbox-label">
        <el-checkbox class="table-mapping-checkbox" v-model="setTableMapping">{{$t('tableMapLabel')}}<el-tooltip :content="$t('tableMapLabelTip')" effect="dark" placement="top"><i class="el-icon-ksd-alert_1 ksd-ml-5"></i></el-tooltip></el-checkbox>
      </el-form-item> -->
      <el-input class="table-map-details" v-if="setTableMapping" type="textarea" :autosize="{ minRows: 4, maxRows: 6 }" :placeholder="$t('tableMapPlaceholder')" v-model.trim="tableMapContent"></el-input>
    </el-form>
    <!-- 解析zip元数据包界面 -->
    <div class="modal-parse-contain" v-loading="isSubmiting" v-else-if="step === 'second'">
      <div class="modal-tables">
        <el-table class="model-list" border :data="models" size="small" ref="parseModelList" :row-class-name="tableRowClassName" @row-click="activeModal">
          <el-table-column prop="target_name" class-name="model-name-item" :label="$t('modelName')">
            <template slot-scope="scope">
              <span class="import-models" v-if="showParseTable">
                <span :class="[importModelStatus(scope.row)]"></span>
                <span v-if="scope.row.action === 'new'">
                  <el-input :class="{'error-tip': scope.row.isNameError}" v-model="scope.row.target_name" size="mini" @change="handleRenameReset(scope.row)"/>
                  <div class="rename-error" v-if="scope.row.isNameError">{{$t(scope.row.nameErrorMsg)}}</div>
                </span>
                <div class="model-name-layout" v-else>
                  <span class="model-name-elip" v-custom-tooltip="{text: scope.row.original_name, w: 0, tableClassName: 'model-list'}">{{scope.row.original_name}}</span>
                </div>
              </span>
            </template>
          </el-table-column>
          <el-table-column :label="$t('actions')" width="150px">
            <template slot-scope="scope">
              <el-select size="mini" v-model="scope.row.action" :placeholder="$t('kylinLang.common.pleaseChoose')" @change="(type) => changeActions(scope.row, type)">
                <el-option
                  v-for="item in getActionOptions"
                  :disabled="getDefaultAction(scope.row).disabledValue.includes(item.value)"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
                </el-option>
              </el-select>
            </template>
          </el-table-column>
        </el-table>
      </div>
      <div :class="['single-modal-detail', {'no-conflict': !(activeModalObj && activeModalObj.differences > 0)}]">
        <div :class="['model-detail-tips', getCurrentModelDetails().key]" v-if="getCurrentModelDetails && activeModalObj && activeModalObj.differences > 0">
          <p class="title">{{$t(activeModalObj.has_same_name ? `${getCurrentModelDetails().title}SameName` : `${getCurrentModelDetails().title}DiffName`)}}<span v-if="!(getCurrentModelDetails().key === 'new' && !activeModalObj.has_same_name)" class="detail-btn" @click="showModelDetails = !showModelDetails">{{$t('detailBtn')}}<i :class="['el-icon-ksd-more_01', 'ksd-ml-5', {'open': showModelDetails}]"></i></span></p>
          <div class="detail-content" v-if="showModelDetails">
            <p>{{$t(activeModalObj.has_same_name ? `${getCurrentModelDetails().details}SameName` : `${getCurrentModelDetails().details}DiffName`)}}</p>
            <div class="ksd-pl-10" v-if="getCurrentDefaultStatus() === 'noImport'">
              <p v-if="noImportReason.filter(it => it === 'tablesDiff' || it === 'columnsDiff').length">* {{$t('noFoundItems', {items: noImportReason.filter(it => it === 'tablesDiff' || it === 'columnsDiff').map(item => $t(item)).join($t('or'))})}}</p>
              <p v-if="noImportReason.filter(it => it === 'columnsDataTypeDiff').length">* {{$t('columnsDataTypeConflict')}}</p>
              <p v-if="noImportReason.filter(it => it === 'computedColumnsDiff').length">* {{$t('computedColumnsDiff')}}</p>
            </div>
            <div class="ksd-pl-10" v-else-if="getCurrentDefaultStatus() === 'new'">
              <p v-for="(item, index) in noOverWriteReason" :key="index">* {{$t(item)}}</p>
            </div>
          </div>
          <p>{{$t(activeModalObj.has_same_name ? `${getCurrentModelDetails().msg}SameName` : `${getCurrentModelDetails().msg}DiffName`)}}</p>
        </div>
        <el-tabs v-model="activeTabName" @tab-click="handleClick" v-if="activeModalObj && activeModalObj.differences > 0">
          <el-tab-pane :name="it.name" v-for="(it, index) in tabPaneList" :key="index">
            <span slot="label"><i :class="it.icon"></i> {{it.label}}</span>
            <div class="tab-collapse-details" v-if="activeModalObj && displayNoDataContain(it) > 0">
              <template v-if="activeModalObj[activeTabName]">
                <el-collapse v-model="activeCollapse" v-if="activeTabName !== 'modified'">
                  <div v-for="(item, index) in Object.keys(activeModalObj[activeTabName])" :key="`${activeTabName}_${index}`">
                    <el-collapse-item v-if="activeTabLength(item)" :name="item">
                      <template slot="title">
                        <span v-if="it.name === 'nofound' && getCurrentDefaultStatus() === 'noImport'">
                          <el-tooltip :content="$t('notFoundTablesOrColumnsTip')" effect="dark" placement="top">
                            <i class="el-icon-ksd-error_01 is-error ksd-mr-5" v-show="item === 'tables' || item === 'columns'"></i>
                          </el-tooltip>
                          {{$t(item, {num: activeTabLength(item)})}}
                        </span>
                        <span v-else-if="it.name !== 'nofound' && getCurrentDefaultStatus() === 'new' && activeModalObj.has_same_name">
                          <el-tooltip :content="$t('cannotOverwriteTips')" effect="dark" placement="top">
                            <i class="el-icon-ksd-alert is-warning ksd-mr-5" v-show="['tables', 'modelFilter', 'modelJoin', 'partitionColumns'].includes(item)"></i>
                          </el-tooltip>
                          {{$t(item, {num: activeTabLength(item)})}}
                        </span>
                        <span v-else>{{$t(item, {num: activeTabLength(item)})}}</span>
                      </template>
                      <template v-if="item === 'partitionColumns'">
                        <div class="detail-text" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">
                          <template v-if="it.type === 'MODEL_PARTITION'">
                            <p>{{$t('timePartition')}} <span class="break-word">{{it.detail}}</span></p>
                          </template>
                           <template v-else-if="it.type === 'MODEL_MULTIPLE_PARTITION'">
                            <p class="detail-text break-word">{{$t('subPartition')}} <span class="ellipsis-layout">{{it.attributes.columns.join(',')}}</span></p>
                            <p class="detail-text break-word" v-if="ArrayFlat(it.attributes.partitions).length">{{$t('subPartitionValues')}} <span class="ellipsis-layout">{{ArrayFlat(it.attributes.partitions).join(',')}}</span></p>
                          </template>
                        </div>
                      </template>
                      <template v-else-if="item === 'computedColumns'">
                        <p class="detail-text" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">
                          <el-tooltip v-if="['SAME_CC_NAME_HAS_DIFFERENT_EXPR', 'DIFFERENT_CC_NAME_HAS_SAME_EXPR'].includes(it.reason)" :content="it.reason === 'SAME_CC_NAME_HAS_DIFFERENT_EXPR' ? $t('sameCCNameTips') : it.reason === 'DIFFERENT_CC_NAME_HAS_SAME_EXPR' ? $t('sameCCExpressionTips', {sameCCName: it.conflict_item || ''}) : ''" effect="dark" placement="top">
                            <i class="el-icon-ksd-error_01 is-error ksd-mr-5"></i>
                          </el-tooltip>
                          <span class="break-word">{{it.detail}}：{{it.attributes.expression}}</span>
                        </p>
                      </template>
                      <template v-else-if="item === 'modelJoin'">
                        <div class="detail-text break-word" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">
                          <p>{{`${it.attributes.foreign_table} ${it.attributes.join_type} JOIN ${it.attributes.primary_table} ON`}}</p>
                          <template v-if="!it.attributes.non_equal_join_condition"><p v-for="(v, index) in it.attributes.foreign_keys" :key="index">{{`${v} = ${it.attributes.primary_keys[index]}`}}</p></template>
                          <p>{{it.attributes.non_equal_join_condition}}</p>
                          <p>{{getJoinRaletionType(it.attributes.join_relation_type)}}</p>
                        </div>
                      </template>
                      <template v-else-if="item === 'indexes'">
                        <p class="detail-text index-item" v-if="activeModalObj[activeTabName][item].agg"><span>{{$t('aggIndex')}}</span>
                          <!-- <span class="ellipsis-layout">{{activeModalObj[activeTabName][item].agg}}</span> -->
                          ({{activeModalObj[activeTabName][item].agg.split(',').length}})
                        </p>
                        <p class="detail-text index-item" v-if="activeModalObj[activeTabName][item].table"><span>{{$t('tableIndex')}}</span>
                          <!-- <span class="ellipsis-layout">{{activeModalObj[activeTabName][item].table}}</span> -->
                          ({{activeModalObj[activeTabName][item].table.split(',').length}})
                        </p>
                      </template>
                      <template v-else-if="activeModalObj[activeTabName][item]">
                        <p class="detail-text" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">
                          <span class="break-word" v-if="item === 'modelFilter'">{{it.detail}}</span>
                          <span v-else v-custom-tooltip="{text: it.detail, w: 0}"><span>{{it.detail}}</span></span>
                        </p>
                      </template>
                      <!-- 分页 -->
                      <p class="loading-more" v-if="showLoadMoreBtn(item)" @click="loadMoreItemData(item)">{{$t('kylinLang.common.loadMore')}}</p>
                    </el-collapse-item>
                  </div>
                </el-collapse>
                <el-collapse v-model="activeCollapse" v-else>
                  <div v-for="(item, index) in Object.keys(activeModalObj[activeTabName])" :key="`${activeTabName}_${index}`">
                    <el-collapse-item v-if="activeTabLength(item)" :name="item">
                      <template slot="title">
                        <span>
                          <el-tooltip :content="$t('changeColumnsDataTypeTip')" effect="dark" placement="top">
                            <i v-if="item === 'columns' && getCurrentDefaultStatus() === 'noImport'" class="el-icon-ksd-error_01 is-error ksd-mr-5"></i>
                          </el-tooltip>
                          {{$t(item === 'columns' ? `${item}DataType` : item, {num: activeTabLength(item)})}}
                        </span>
                      </template>
                      <template v-if="item === 'columns'">
                        <p class="detail-text" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">{{it.first_detail}}：{{it.second_attributes.datatype}} <span class="modify-item">{{it.first_attributes.datatype}}</span></p>
                      </template>
                      <template v-else-if="item === 'measures'">
                        <div class="detail-text" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">
                          <p><span class="title">{{$t('measureName')}}</span><span v-custom-tooltip="{text: it.first_attributes.name, w: 110}">{{it.first_attributes.name}}</span></p>
                          <p v-if="it.first_attributes.expression !== it.second_attributes.expression" class="break-word"><span class="title">{{$t('measureFunction')}}</span>{{it.second_attributes.expression}} <span class="modify-item">{{it.first_attributes.expression}}</span></p>
                          <p v-if="it.first_attributes.returntype !== it.second_attributes.returntype" class="break-word"><span class="title">{{$t('measureFunctionParams')}}</span>{{it.second_attributes.returntype}} <span class="modify-item">{{it.first_attributes.returntype}}</span></p>
                          <div v-if="it.second_attributes.expression === 'TOP_N' && it.first_attributes.parameters[0].value !== it.second_attributes.parameters[0].value">
                            <span class="title">Order/SUM By：</span>
                            <div class="order-by-content break-word">
                              <p>{{it.second_attributes.parameters[0].value}}</p>
                              <p class="modify-item">{{it.first_attributes.parameters[0].value}}</p>
                            </div>
                          </div>
                          <div v-if="it.second_attributes.expression === 'TOP_N' && displayGroupByColumns(it)">
                            <span class="title">Group By：</span>
                            <div class="parameters">
                              <p><span v-custom-tooltip="{text: it.second_attributes.parameters.slice(1).map(it => it.value).join(', '), w: 0}">{{it.second_attributes.parameters.slice(1).map(it => it.value).join(', ')}}</span></p>
                              <p><span class="modify-item" v-custom-tooltip="{text: it.first_attributes.parameters.slice(1).map(it => it.value).join(', '), w: 0}">{{it.first_attributes.parameters.slice(1).map(it => it.value).join(', ')}}</span></p>
                            </div>
                          </div>
                          <p v-if="it.second_attributes.expression !== 'TOP_N' && it.first_attributes.parameters[0].value !== it.second_attributes.parameters[0].value" class="break-word"><span class="title">{{$t('params')}}</span>{{it.second_attributes.parameters[0].value}} <span class="modify-item">{{it.first_attributes.parameters[0].value}}</span></p>
                        </div>
                      </template>
                      <template v-else-if="item === 'modelFilter'">
                        <p class="detail-text break-word" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">{{it.second_attributes.condition}} <span class="modify-item">{{it.first_attributes.condition}}</span></p>
                      </template>
                      <template v-else-if="item === 'computedColumns'">
                        <p class="detail-text" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">
                          <el-tooltip v-if="['SAME_CC_NAME_HAS_DIFFERENT_EXPR', 'DIFFERENT_CC_NAME_HAS_SAME_EXPR'].includes(it.reason)" :content="it.reason === 'SAME_CC_NAME_HAS_DIFFERENT_EXPR' ? $t('sameCCNameTips') : it.reason === 'DIFFERENT_CC_NAME_HAS_SAME_EXPR' ? $t('sameCCExpressionTips') : ''" effect="dark" placement="top">
                            <i class="el-icon-ksd-error_01 is-error ksd-mr-5"></i>
                          </el-tooltip>
                          <span class="break-word">{{it.first_detail}}：{{it.second_attributes.expression}}</span> <span class="modify-item break-word">{{it.first_attributes.expression}}</span>
                        </p>
                      </template>
                      <template v-else-if="item === 'modelJoin'">
                        <div class="detail-text break-word" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">
                          <p>{{`${it.second_attributes.foreign_table} ${it.second_attributes.join_type}`}} <span class="modify-item" v-if="it.second_attributes.join_type !== it.first_attributes.join_type">{{`${it.first_attributes.join_type}`}}</span> JOIN {{`${it.second_attributes.primary_table}`}} ON</p>
                          <template v-if="!it.second_attributes.non_equal_join_condition"><p v-for="(v, index) in it.second_attributes.foreign_keys" :key="index">{{`${v} = ${it.second_attributes.primary_keys[index]}`}}</p></template>
                          <p>{{it.second_attributes.non_equal_join_condition}}</p>
                          <p v-if="it.second_attributes.join_relation_type !== it.first_attributes.join_relation_type">{{getJoinRaletionType(it.second_attributes.join_relation_type)}}</p>
                          <div class="modify-item">
                            <!-- <p>{{`${it.first_attributes.foreign_table} ${it.first_attributes.join_type} ${it.first_attributes.primary_table}`}}</p> -->
                            <template v-if="!it.first_attributes.non_equal_join_condition && isChangeEqualJoin(it)"><p v-for="(v, index) in it.first_attributes.foreign_keys" :key="index">{{`${v} = ${it.first_attributes.primary_keys[index]}`}}</p></template>
                            <p>{{`${it.first_attributes.non_equal_join_condition}`}}</p>
                            <p v-if="it.second_attributes.join_relation_type !== it.first_attributes.join_relation_type">{{getJoinRaletionType(it.first_attributes.join_relation_type)}}</p>
                          </div>
                        </div>
                      </template>
                      <template v-else-if="item === 'partitionColumns' && activeModalObj[activeTabName][item].list.length">
                        <div v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">
                          <template v-if="it.type === 'MODEL_PARTITION'">
                            <p class="detail-text break-word" v-if="it.second_attributes.column !== it.first_attributes.column">{{$t('timePartition')}} {{it.second_attributes.column}} <span class="modify-item">{{it.first_attributes.column}}</span></p>
                            <p class="detail-text break-word" v-if="it.second_attributes.format !== it.first_attributes.format">{{$t('timePartitionType')}} {{it.second_attributes.format}} <span class="modify-item">{{it.first_attributes.format}}</span></p>
                          </template>
                          <template v-else-if="it.type === 'MODEL_MULTIPLE_PARTITION'">
                            <div class="detail-text" v-if="it.second_attributes.columns.join(',') !== it.first_attributes.columns.join(',')">
                              <span>{{$t('subPartition')}}</span>
                              <div class="sub-partition-columns break-word">
                                <p>{{it.second_attributes.columns.join(',')}}</p>
                                <p class="modify-item">{{it.first_attributes.columns.join(',')}}</p>
                              </div>
                            </div>
                            <div class="detail-text" v-if="ArrayFlat(it.second_attributes.partitions).join(',') !== ArrayFlat(it.first_attributes.partitions).join(',')">
                              <span>{{$t('subPartitionValues')}}</span>
                              <div class="sub-partition-values break-word">
                                <p>{{ArrayFlat(it.second_attributes.partitions).join(',')}}</p>
                                <p class="modify-item">{{ArrayFlat(it.first_attributes.partitions).join(',')}}</p>
                              </div>
                            </div>
                          </template>
                        </div>
                      </template>
                      <template v-else-if="item === 'indexes'">
                        <p class="detail-text index-item" v-if="activeModalObj[activeTabName][item].agg"><span>{{$t('aggIndex')}}</span><span class="ellipsis-layout">{{activeModalObj[activeTabName][item].agg}}</span> ({{activeModalObj[activeTabName][item].agg.split(',').length}})</p>
                        <p class="detail-text index-item" v-if="activeModalObj[activeTabName][item].table"><span>{{$t('tableIndex')}}</span><span class="ellipsis-layout">{{activeModalObj[activeTabName][item].table}}</span> ({{activeModalObj[activeTabName][item].table.split(',').length}})</p>
                      </template>
                      <template v-else-if="activeModalObj[activeTabName][item]">
                        <p class="detail-text break-word" v-for="(it, index) in activeModalObj[activeTabName][item].list" :key="index">{{it.first_detail}}</p>
                      </template>
                    </el-collapse-item>
                  </div>
                </el-collapse>
              </template>
            </div>
            <div class="no-data" v-else>
              <template v-if="activeTabName === 'nofound'">
                <i class="nofound-icon el-icon-ksd-good_health"></i>
                <p>{{$t('noFoundDataTip')}}</p>
              </template>
              <template v-if="activeTabName === 'add'">
                <i class="el-icon-ksd-empty-box"></i>
                <p>{{$t('noAddDataTip')}}</p>
              </template>
              <template v-if="activeTabName === 'reduce'">
                <i class="el-icon-ksd-empty-box"></i>
                <p>{{$t('noDeleteDataTip')}}</p>
              </template>
              <template v-if="activeTabName === 'modified'">
                <i class="el-icon-ksd-empty-box"></i>
                <p>{{$t('noUpdateDataTip')}}</p>
              </template>
            </div>
          </el-tab-pane>
        </el-tabs>
        <div class="no-data" v-else>
          <i class="el-icon-ksd-empty-box"></i>
          <p>{{$t('noDifferences')}}</p>
        </div>
      </div>
      <!-- {{JSON.stringify(activeModalObj[activeTabName]['computedColumns'])}} -->
    </div>
    <!-- 确认导入界面 -->
    <div class="confirm-import-modal-layout" v-else-if="step === 'third'">
      <p class="title">{{$t('confirmImportTips')}}</p>
      <div class="contain" v-for="(item, index) in importModalView" :key="index">
        <p class="contain-item">{{$t(item.title, {num: item.value.length})}}</p>
        <template v-if="item.key !== 'replace'">
          <div class="confirm-box">
            <p v-for="(it, index) in item.value" :key="index">{{it.name}}</p>
          </div>
        </template>
        <template v-else>
          <el-alert class="ksd-mb-10" :title="$t('changeSubPartitionTip')" type="warning" show-icon :closable="false" v-if="item.value.filter(it => it.change_subpartition_value).length > 0" />
          <div class="confirm-box">
            <p v-for="(it, index) in item.value" :key="index"><i class="el-icon-ksd-info is-warning ksd-mr-5" v-show="it.change_subpartition_value"></i>{{it.name}}</p>
          </div>
        </template>
      </div>
    </div>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <!-- 确认/取消: 上传元数据zip包界面 -->
      <template v-if="step === 'first'">
        <el-button size="medium" :disabled="isSubmiting" @click="handleCancel">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" type="primary" :disabled="!form.file || setTableMapping && !tableMapContent || showError" :loading="isSubmiting" @click="handleUploadFile">{{$t('parseFile')}}</el-button>
      </template>
      <!-- 确认/取消: 解析zip元数据包界面 -->
      <template v-else-if="step === 'second'">
        <el-button size="medium" :disabled="isSubmiting" @click="handlePrev('first')">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button type="primary" size="medium" @click="nextConfirmImport" :loading="isCheckName">{{$t('kylinLang.common.next')}}</el-button>
      </template>
      <!-- 确认导入模型 -->
      <template v-else>
        <el-button size="medium" :disabled="isSubmiting" @click="handlePrev('second')">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button type="primary" size="medium" :loading="isSubmiting" @click="handleSubmit">{{getImportBtnText}}</el-button>
      </template>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'

import store, { getDefaultAction } from './store'
import locales from './locales'
import { NamedRegex } from 'config'
import { validator, diffOverWriteModel } from './handler'
import vuex, { actionTypes } from '../../../store'
import { handleSuccessAsync, handleError, ArrayFlat } from '../../../util'

vuex.registerModule(['modals', 'ModelsImportModal'], store)

@Component({
  components: {
  },
  computed: {
    ...mapState('ModelsImportModal', {
      project: state => state.project,
      models: state => state.models,
      conflicts: state => state.conflicts,
      form: state => state.form,
      isShow: state => state.isShow,
      callback: state => state.callback
    }),
    ...mapGetters('ModelsImportModal', [
      'conflictModels',
      'unConflictModels',
      'brokenConflictModels',
      'importableConflictModels'
    ]),
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapMutations('ModelsImportModal', {
      setModalForm: actionTypes.SET_MODAL_FORM,
      setModal: actionTypes.SET_MODAL,
      hideModal: actionTypes.HIDE_MODAL,
      initModal: actionTypes.INIT_MODAL,
      changeModalData: actionTypes.CHANGE_MODAL_DATA
    }),
    ...mapActions('ModelsImportModal', {
      uploadMetadataFile: actionTypes.UPLOAD_MODEL_METADATA_FILE,
      importModelsMetadata: actionTypes.IMPORT_MODEL_METADATA_FILE
    }),
    ...mapActions({
      getModelByModelName: 'LOAD_MODEL_INFO'
    })
  },
  locales
})
export default class ModelsImportModal extends Vue {
  isBodyShow = false
  isSubmiting = false
  step = 'first'
  setTableMapping = false
  showError = false
  tableMapContent = ''
  showDetailTips = false
  activeTabName = 'nofound'
  activeModalObj = null
  validateErrorMsg = ''
  modelNameError = ''
  isCheckName = false
  getDefaultAction = getDefaultAction
  ArrayFlat = ArrayFlat
  activeCollapse = ['tables', 'columns', 'partitionColumns', 'measures', 'dimensions', 'indexes', 'computedColumns', 'modelJoin', 'modelFilter']
  modelDetails = {
    noImport: {title: 'noImportTitle', details: 'noImportDetails', msg: 'noImportMsg', key: 'noImport'},
    new: {title: 'newModelTitle', details: 'newModelDetails', msg: 'newModelMsg', key: 'new'},
    replace: {title: 'replaceModelTitle', details: 'replaceModelDetails', msg: 'replaceModelMsg', key: 'replace'}
  }
  showModelDetails = false
  noImportReason = []
  noOverWriteReason = []
  showParseTable = true

  get rules () {
    return {
      file: [{ trigger: 'blur', required: true, validator: validator.file(this) }]
    }
  }

  get getParseImportModal () {
    const noImportNum = this.models.filter(item => !item.importable).length
    return `${this.$t('parseModalsTips', { modelNum: this.models.length })}${noImportNum ? this.$t('parseModalsTips1', { noImportNum }) : this.$t('kylinLang.common.dot')}`
  }

  get getActionOptions () {
    return [
      {label: this.$t('addNew'), value: 'new'},
      {label: this.$t('replace'), value: 'replace'},
      {label: this.$t('notImport'), value: 'noImport'}
    ]
  }

  get tabPaneList () {
    const { missing_items, new_items, reduce_items, update_items, has_same_name } = this.activeModalObj
    const defaultValue = this.getCurrentDefaultStatus(this.activeModalObj)
    if (defaultValue === 'noImport' && !has_same_name) {
      return [
        { label: this.$t('nofound', {num: missing_items.length}), name: 'nofound', icon: 'el-icon-ksd-error_01' },
        { label: this.$t('add', {num: new_items.length}), name: 'add', icon: 'el-icon-ksd-add_bg' },
        { label: this.$t('modified', {num: update_items.length}), name: 'modified', icon: 'el-icon-ksd-edit_bg' }
      ]
    } else {
      return [
        { label: this.$t('nofound', {num: missing_items.length}), name: 'nofound', icon: 'el-icon-ksd-error_01' },
        { label: this.$t('add', {num: new_items.length}), name: 'add', icon: 'el-icon-ksd-add_bg' },
        { label: this.$t('reduce', {num: reduce_items.length}), name: 'reduce', icon: 'el-icon-ksd-minus_bg' },
        { label: this.$t('modified', {num: update_items.length}), name: 'modified', icon: 'el-icon-ksd-edit_bg' }
      ]
    }
  }

  // 获取模型名称校验不通过 list
  get disabledNextBtnType () {
    return this.models.filter(it => it.isNameError && it.action === 'new')
  }

  activeTabLength (item) {
    return item === 'indexes' ? this.activeModalObj[this.activeTabName][item].length : this.activeModalObj[this.activeTabName][item].totalData.length
  }

  // 确认需要新建、覆盖及不导入的模型
  get importModalView () {
    const modalTypes = {
      new: [],
      replace: [],
      noImport: []
    }
    let changeSubPartitionValuelist = []
    this.models.forEach(element => {
      modalTypes[element.action].push(element.target_name)
      if (element.modified && element.modified.partitionColumns && element.modified.partitionColumns.list.length) {
        let list = element.modified.partitionColumns.list.filter(it => it.type === 'MODEL_MULTIPLE_PARTITION' && ArrayFlat(it.second_attributes.partitions).join(',') !== ArrayFlat(it.first_attributes.partitions).join(','))
        changeSubPartitionValuelist = list.map(it => it.model_alias)
      }
    })

    return Object.entries(modalTypes).map(([title, v]) => ({title: `${title}ConfirmTips`, key: title, value: v.map((item) => ({name: item, change_subpartition_value: changeSubPartitionValuelist.includes(item)}))})).filter(it => it.value.length)
  }

  get getImportBtnText () {
    const list = this.models.filter(it => it.action === 'noImport')
    return list.length === this.models.length ? this.$t('confirmNoImportBtn') : this.$t('confirmImportBtn')
  }

  get checkSameName () {
    return this.models.filter(it => it.isNameError && it.action === 'new').length > 0 || this.showError
  }

  // 获取不一致项
  getDiffs () {
    const {nofound, modified, add, has_same_name} = this.activeModalObj
    let defaultValue = this.getCurrentDefaultStatus(this.activeModalObj)
    if (defaultValue === 'noImport') {
      if (nofound) {
        this.noImportReason = this.noImportReason.concat(Object.keys(nofound).map(it => `${it}Diff`))
      }
      if (modified) {
        if (modified.columns && modified.columns.list.length) {
          this.noImportReason.push('columnsDataTypeDiff')
        }
        if (modified.computedColumns && this.judgeColumnByCC(modified.computedColumns.list).length) {
          this.noImportReason.push('computedColumnsDiff')
        }
      }
      if (add && add.computedColumns && this.judgeColumnByCC(add.computedColumns.list).length) {
        this.noImportReason.push('computedColumnsDiff')
      }
    } else if (defaultValue === 'new' && has_same_name) {
      this.noOverWriteReason = diffOverWriteModel(this.activeModalObj)
    }
  }

  // 判断cc是否同名或同表达式
  judgeColumnByCC (list) {
    return list.length ? list.filter(it => ['SAME_CC_NAME_HAS_DIFFERENT_EXPR', 'DIFFERENT_CC_NAME_HAS_SAME_EXPR'].includes(it.reason)) : []
  }

  getCurrentDefaultStatus (obj) {
    let content = obj || this.activeModalObj
    return this.getDefaultAction(content).defaultValue
  }

  getCurrentModelDetails () {
    return this.modelDetails[this.getCurrentDefaultStatus(this.activeModalObj) || '']
  }

  // 导入模型状态标志
  importModelStatus (row) {
    const defaultValue = this.getCurrentDefaultStatus(row)
    if (defaultValue === 'noImport') {
      return 'el-icon-ksd-error_01 is-error'
    } else if (defaultValue === 'new' && row.has_same_name) {
      return 'el-icon-ksd-alert is-warning'
    } else {
      return 'el-icon-ksd-good_health is-success'
    }
  }

  getJoinRaletionType (raletionType) {
    if (raletionType === 'MANY_TO_ONE' || raletionType === 'ONE_TO_ONE') {
      return 'One-to-One or Many-to-One'
    } else if (raletionType === 'ONE_TO_MANY' || raletionType === 'MANY_TO_MANY') {
      return 'One-to-Many or Many-to-Many'
    } else {
      return ''
    }
  }

  // diff equal 连接关系是否一致
  isChangeEqualJoin (item) {
    const targetItem = JSON.parse(JSON.stringify(item))
    const sortOldF = targetItem.first_attributes.foreign_keys.sort((a, b) => a - b).join(',')
    const sortNewF = targetItem.second_attributes.foreign_keys.sort((a, b) => a - b).join(',')
    const sortOldP = targetItem.first_attributes.primary_keys.sort((a, b) => a - b).join(',')
    const sortNewP = targetItem.second_attributes.primary_keys.sort((a, b) => a - b).join(',')
    return sortOldF !== sortNewF || sortOldP !== sortNewP
  }

  // 判断是否展示 group by 列
  displayGroupByColumns (item) {
    const oldGroupBy = item.first_attributes.parameters.slice(1).map(it => it.value).join(',')
    const newGroupBy = item.second_attributes.parameters.slice(1).map(it => it.value).join(',')
    return oldGroupBy !== newGroupBy
  }

  // 更改模型操作
  changeActions (row, type) {
    if (type !== 'new' && row.original_name !== row.target_name) {
      row.target_name = row.original_name
    }
    row.isNameError = false
    row.nameErrorMsg = ''
    this.showParseTable = false
    this.$nextTick(() => {
      this.showParseTable = true
    })
  }

  // 重置重名校验
  handleRenameReset (row) {
    row.isNameError = false
    row.nameErrorMsg = ''
  }

  // 新建模型时支持更改模型名称 - 以防重名
  async handleRename (row) {
    const value = row.target_name
    const allImportModalName = this.models.filter(it => it.action === 'new').map(item => item.target_name)
    if (!NamedRegex.test(value)) {
      row.isNameError = true
      row.nameErrorMsg = 'kylinLang.common.nameFormatValidTip'
    } else if (value.length > 50) {
      row.isNameError = true
      row.nameErrorMsg = 'kylinLang.common.overLengthTip'
    } else if (allImportModalName.filter(v => v === value).length > 1) {
      row.isNameError = true
      row.nameErrorMsg = 'kylinLang.model.sameModelName'
    } else {
      try {
        const res = await this.getModelByModelName({model_name: value, project: this.currentSelectedProject})
        const data = await handleSuccessAsync(res)
        if (data && data.value && data.value.length) {
          row.isNameError = true
          row.nameErrorMsg = 'kylinLang.model.sameModelName'
        } else {
          row.isNameError = false
        }
      } catch (e) {
        row.isNameError = false
        handleError(e)
        throw Error(e)
      }
    }
  }

  async nextConfirmImport () {
    this.isCheckName = true
    let errorApi = false
    for (let item of this.models) {
      if (item.action === 'new') {
        try {
          await this.handleRename(item)
        } catch (e) {
          errorApi = true
          break
        }
      }
    }
    this.isCheckName = false
    if (errorApi) return
    this.$nextTick(() => {
      if (this.disabledNextBtnType.length > 0) return
      this.step = 'third'
    })
  }

  showLoadMoreBtn (key) {
    let { pageOffset, pageSize, totalData } = this.activeModalObj[this.activeTabName][key]
    return pageOffset && totalData.length > pageOffset * pageSize
  }

  // 分页
  loadMoreItemData (key) {
    const _data = this.activeModalObj[this.activeTabName][key]
    let { pageSize, totalData } = _data
    _data.pageOffset += 1
    _data.list = totalData.slice(0, _data.pageOffset * pageSize)
  }

  toggerDetailTips () {
    this.showDetailTips = !this.showDetailTips
  }

  activeModal (obj) {
    let labels = [{key: 'missing_items', label: 'nofound'}, {key: 'new_items', label: 'add'}, {key: 'reduce_items', label: 'reduce'}, {key: 'update_items', label: 'modified'}]
    this.noImportReason = []
    this.showModelDetails = false
    this.activeModalObj = obj
    this.activeTabName = labels.filter(it => obj[it.key] && obj[it.key].length).length ? labels.filter(it => obj[it.key] && obj[it.key].length)[0].label : 'nofound'
    if (['new', 'noImport'].includes(this.getCurrentDefaultStatus(obj))) {
      this.getDiffs()
    }
  }

  tableRowClassName ({row, rowIndex}) {
    if (this.activeModalObj && row.original_name === this.activeModalObj.original_name) {
      return 'active-row'
    }
    return ''
  }

  // 展示 no-data 缺省页
  displayNoDataContain (pane) {
    if (this.activeTabName === 'nofound') {
      return this.activeModalObj.missing_items.length
    } else if (this.activeTabName === 'add') {
      return this.activeModalObj.new_items.length
    } else if (this.activeTabName === 'reduce') {
      return this.activeModalObj.reduce_items.length
    } else if (this.activeTabName === 'modified') {
      return this.activeModalObj.update_items.length
    } else {
      return false
    }
  }

  // 切换模型详情 tab
  handleClick (v) {
    this.activeTabName = v.name
  }

  resetState () {
    this.isBodyShow = false
    this.isSubmiting = false
    this.step === 'first'
  }

  async handleOpen () {
    this.isBodyShow = true
  }

  handleClose (isSubmit = false) {
    this.hideModal()
    this.resetState()
    this.showDetailTips = false
    this.step = 'first'
    this.activeTabName = 'nofound'
    this.showError = false
    this.showModelDetails = false
    this.noImportReason = []
    this.callback && this.callback(isSubmit)
  }

  handleClosed () {
    this.isBodyShow = false
  }

  handleSelectFile (event) {
    const [file] = event.target.files
    const ext = file.name.match(/\.\w+$/)
    this.setModalForm({ file })
    if (ext.length && ext[0] !== '.zip') {
      this.showError = true
      this.validateErrorMsg = this.$t('noZipFileErrorTip')
    } else {
      this.showError = false
    }
  }

  handleCancel () {
    this.handleClose()
  }

  handlePrev (step) {
    if (step === 'first') {
      this.setModalForm({ ids: [] })
      this.setModal({ models: [], conflicts: [], signature: '' })
    }
    this.step = step
  }

  handleUploadFile () {
    this.isSubmiting = true

    this.$refs.form.validate(async isValid => {
      if (isValid) {
        try {
          const { project, form } = this
          await this.uploadMetadataFile({ project, form })
          if (!this.isShow || !this.form.file) return
          this.step = 'second'
          this.showError = false
          this.activeModalObj = this.models[0];
          ['new', 'noImport'].includes(this.getCurrentDefaultStatus(this.models[0])) && this.getDiffs()
          this.showParseTable = false
          this.$nextTick(() => {
            this.showParseTable = true
            this.$refs.parseModelList.doLayout()
          })
        } catch (e) {
          if (!this.isShow || !this.form.file) return
          const { code } = e.body
          this.isSubmiting = false
          if (!code) {
            handleError(e)
            return
          }
          if (code === '999') {
            this.showError = true
            this.validateErrorMsg = this.$t('validateErrorTips')
          }
        }
      }

      this.isSubmiting = false
    })
  }

  async handleSubmit () {
    // if (this.form.ids.length < 0) {
    //   return this.$message.error('pleaseSelectModels')
    // }
    const actionMap = {
      new: 'NEW',
      replace: 'OVERWRITE',
      noImport: 'UN_IMPORT'
    }

    try {
      const models = this.models.map(it => ({original_name: it.original_name, target_name: it.target_name, import_type: actionMap[it.action]})).filter(item => item.import_type !== 'UN_IMPORT')
      if (models.length) {
        this.setModalForm({ request: { models } })
        const { project, form } = this
        this.isSubmiting = true

        await this.importModelsMetadata({ project, form })
        this.handleClose(true)
        this.$message.success(this.$t('submitSuccess'))
      } else {
        this.handleClose(true)
      }
    } catch (e) {}
    this.isSubmiting = false
  }

  mounted () {
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.models-import-modal {
  .modal-import-tip {
    color: @text-title-color;
    margin-bottom: 15px;
    i {
      color: @text-disabled-color;
    }
  }
  .view-import-tips {
    color: #0988DE;
    cursor: pointer;
    i {
      transform: rotate(180deg);
    }
    .reverse {
      transform: rotate(0deg);
    }
  }
  .import-details {
    background: #FAFAFA;
    padding: 10px;
    box-sizing: border-box;
    font-size: 12px;
    color: #5c5c5c;
    margin: 5px 0;
  }
  .modal-parse-contain {
    margin-top: 20px;
    display: flex;
    flex-direction: row;
    flex: auto;
    width: 100%;
    .modal-tables {
      width: 45%;
      height: 360px;
      overflow: auto;
      .active-row {
        background-color: @base-color-9;
      }
    }
    .is-error {
      color: @error-color-1;
    }
    .is-warning {
      color: @warning-color-1;
    }
    .is-success {
      color: @color-success;
    }
    .single-modal-detail {
      margin-left: 1.4%;
      box-sizing: border-box;
      width: 55%;
      height: 360px;
      position: relative;
      margin-bottom: 10px;
      overflow: auto;
      &.no-conflict {
        border: 1px solid @line-border-color3;
      }
      .el-tabs {
        border: 1px solid @line-border-color3;
        padding: 5px 20px;
        margin-top: 10px;
        box-sizing: border-box;
      }
      .model-detail-tips {
        font-size: 12px;
        padding: 10px;
        color: @text-title-color;
        .detail-btn {
          color: @base-color;
          cursor: pointer;
          i {
            font-size: 10px;
          }
          .open {
            transform: rotate(180deg);
          }
        }
        &.replace {
          background-color: @normal-color-2;
        }
        &.new {
          background-color: @warning-color-2;
        }
        &.noImport {
          background-color: @error-color-2;
        }
        .detail-content {
          color: @text-normal-color;
        }
      }
      .el-tabs__nav-wrap {
        .el-tabs__item {
          font-size: 12px;
          padding: 0 8px;
          i {
            color: @text-disabled-color;
          }
          &.is-active, &:hover {
            i {
              color: @base-color;
            }
          }
        }
      }
      .el-tabs--top {
        // height: 100%;
        .el-tabs__header {
          margin: 0;
        }
        .el-tabs__content {
          min-height: 250px;
          overflow: hidden;
        }
      }
      .el-collapse-item {
        .el-collapse-item__header {
          border-bottom: 0;
          font-size: 12px;
          height: 36px;
          line-height: 36px;
          .el-collapse-item__arrow {
            float: none;
          }
        }
        .el-collapse-item__wrap {
          border-bottom: 0;
        }
      }
      .tab-collapse-details {
        .el-collapse {
          border-top: none;
          border-bottom: none;
          .custom-tooltip-layout {
            line-height: 14px;
            vertical-align: middle;
          }
        }
        .detail-text {
          margin-top: 5px;
          color: @text-normal-color;
          &:first-child {
            margin-top: 0;
          }
          &.index-item {
            .ellipsis-layout {
              max-width: calc(~'100% - 100px');
              display: inline-block;
              text-overflow: ellipsis;
              overflow: hidden;
              vertical-align: top;
            }
          }
          .parameters {
            width: calc(~'100% - 80px');
            display: inline-block;
            vertical-align: top;
          }
          .order-by-content {
            width: calc(~'100% - 110px');
            display: inline-block;
            vertical-align: top;
          }
          .sub-partition-columns, .sub-partition-values {
            display: inline-block;
            vertical-align: top;
            width: calc(~'100% - 150px');
            p {
              width: 100%;
              text-overflow: ellipsis;
              overflow: hidden;
            }
          }
        }
        .modify-item {
          text-decoration: line-through;
          text-decoration-color: @error-color-1;
        }
        .loading-more {
          font-size: 12px;
          color: @text-disabled-color;
          cursor: pointer;
          &:hover {
            color: @base-color;
          }
        }
      }
      .no-data {
        color: @color-text-placeholder;
        text-align: center;
        position: absolute;
        top: 30%;
        left: 50%;
        transform: translate(-50%, -30%);
        font-size: 12px;
        i {
          font-size: 40px;
          color: @color-text-placeholder;
          cursor: default;
          margin-bottom: 5px;
        }
        .nofound-icon {
          font-size: 30px;
        }
      }
    }
  }
  .break-word {
    word-break: break-all;
  }
  .confirm-import-modal-layout {
    .title {
      margin-bottom: 5px;
    }
    .confirm-box {
      border: 1px solid @line-border-color3;
      padding: 10px;
      box-sizing: border-box;
      height: auto;
      overflow: auto;
      .is-warning {
        color: @warning-color-1;
      }
    }
    .contain {
      margin-top: 10px;
      &:first-child {
        margin-top: 0;
      }
      .contain-item {
        margin-bottom: 5px;
      }
    }
  }
  .table-mapping-checkbox {
    // margin-top: 15px;
    color: @text-title-color;
    i {
      color: @text-normal-color;
    }
    .el-checkbox__input.is-checked+.el-checkbox__label {
      color: @text-title-color;
    }
  }
  .el-form-item:not(.checkbox-label) {
    margin-bottom: 10px;
  }
  .el-form-item.checkbox-label {
    margin-bottom: 0;
  }
  .table-map-details {
    font-size: 12px;
    .el-textarea__inner {
      max-height: 400px;
      background: @base-background-color-1;
      border: 1px solid @line-border-color;
    }
  }
  .error-tip > .el-input__inner {
    border: 1px solid @error-color-1;
  }
  .error-msg {
    color: @error-color-1;
    font-size: 12px;
    line-height: 1;
  }
  .el-dialog__body {
    max-height: 463px;
    overflow: auto;
  }
  .file-upload {
    position: relative;
  }
  .file-input {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    opacity: 0;
    width: 100%;
    cursor: pointer;
  }
  .header {
    margin-bottom: 10px;
  }
  .model-type-list {
    border: none;
    & > .el-collapse-item {
      border: 1px solid @line-split-color;
      &:not(:last-child) {
        margin-bottom: 10px;
      }
    }
    & > .el-collapse-item > div > .el-collapse-item__header {
      height: 32px;
      line-height: 32px;
      border: none;
    }
    & > .el-collapse-item > div > .el-collapse-item__header > .el-collapse-item__arrow {
      line-height: 32px;
    }
    & .el-collapse-item__wrap {
      border: none;
    }
  }
  .el-collapse-item__content {
    padding: 0 0 0 20px;
  }
  .model-list {
    .import-models {
      // display: flex;
      // gap: 5px;
      // align-items: center;
    }
    .model-name-item {
      .cell > span {
        display: inline-block;
        width: 100%;
      }
      .el-input {
        width: calc(~'100% - 20px');
      }
      .tip_box {
        vertical-align: top;
        line-height: 20px;
      }
    }
    .model-name-layout {
      width: calc(~'100% - 35px');
      display: inline-block;
    //   flex: 1;
    //   height: 20px;
    //   display: inline-block;
    //   overflow: hidden;
    //   vertical-align: top;
    //   .tip_box {
    //     height: 100%;
    //   }
    }
    // .model-name-elip {
      // display: -webkit-box;
      // overflow: hidden;
      // text-overflow: ellipsis;
      // white-space: normal;
      // -webkit-line-clamp: 1;
      // /*! autoprefixer: off */
      // -webkit-box-orient: vertical;
      // /* autoprefixer: on */
      // white-space: nowrap\0 !important;
    // }
  }
  .model-type-header {
    padding: 0 10px;
    background-color: @background-disabled-color;
  }
  .model-list .model_item {
    display: block;
    padding: 0 10px;
    margin-left: 0;
    font-weight: bold;
    .el-checkbox__label {
      float: left;
      color: @text-normal-color;
      width: calc(~'100% - 25px');
      line-height: 26px;
      height: 26px;
    }
    .el-checkbox__input {
      float: left;
      line-height: 26px;
      height: 26px;
    }
    .el-checkbox__inner {
      transform: translateY(2px);
    }
    &:not(:last-child) {
      border-bottom: 1px solid @background-disabled-color;
    }
  }
  .model-name {
    font-weight: bold;
    display: inline-block;
    width: calc(~'100% - 25px');
    white-space: nowrap;
    i {
      float: left;
      font-size: 12px;
      height: 25px;
      line-height: 25px;
      margin-right: 5px;
    }
  }
  .el-collapse-item__header {
    font-size: 14px;
  }
  .conflict-models {
    .el-collapse {
      border: none;
      & > .el-collapse-item:not(:last-child):after {
        content: ' ';
        height: 1px;
        background: @background-disabled-color;
        display: block;
        margin: 0 10px;
      }
      & > .el-collapse-item .el-collapse-item__header {
        padding: 0 0 0 10px;
      }
      & > .el-collapse-item > div > .el-collapse-item__header {
        height: 25px;
        line-height: 25px;
        border: none;
      }
      & > .el-collapse-item > div > .el-collapse-item__header > .el-collapse-item__arrow {
        line-height: 25px;
      }
    }
  }
  .model-conflicts {
    font-size: 12px;
    padding: 0 10px;
    .message {
      margin: 0 0 6px 0;
      line-height: 12px;
    }
  }
  .conflict-item {
    margin-bottom: 10px;
  }
  .conflict-title {
    line-height: 14px;
    margin-bottom: 5px;
  }
  .empty-text {
    padding: 10px 0;
    text-align: center;
    color: @text-disabled-color;
  }
  .conflict-box textarea {
    background-color: @base-background-color-1;
  }
}
</style>
