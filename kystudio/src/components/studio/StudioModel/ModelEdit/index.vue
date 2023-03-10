<template>
  <div class="model-edit-layout">
    <div class="model-edit-header">
      <div class="model-title">
        <model-title-description :modelData="modelData" source="modelEdit" v-if="modelData" hideTimeTooltip />
      </div>
      <div class="model-search-layout">
        <el-input
          @input="searchModelEverything"
          clearable
          :placeholder="$t('kylinLang.common.search')"
          v-model="modelGlobalSearch"
          prefix-icon="el-ksd-n-icon-search-outlined"
        ></el-input>
        <transition name="bounceleft">
          <div class="search-board" v-if="modelGlobalSearch && showSearchResult">
            <el-row>
              <el-col :span="16" class="search-content-col">
                <div v-scroll.reactive class="search-result-box" v-keyborad-select="{scope:'.search-content', searchKey: modelGlobalSearch}" v-search-highlight="{scope:'.search-name', hightlight: modelGlobalSearch}">
                  <div>
                    <div class="search-group" v-for="(k,v) in searchResultData" :key="v">
                      <ul>
                        <li class="search-content" v-for="(x, i) in k" @click="(e) => {selectResult(e, x)}" :key="x.action + x.name + i"><span class="search-category">[{{$t(x.i18n)}}]</span> <span class="search-name">{{x.name}}</span><span v-html="x.extraInfo"></span></li>
                      </ul>
                      <div class="ky-line"></div>
                    </div>
                    <div v-show="Object.keys(searchResultData).length === 0" class="search-noresult">{{$t('kylinLang.common.noResults')}}</div>
                  </div>
                </div>
                </el-col>
              <el-col :span="8" class="search-history">
                <div class="search-action-list" v-if="modelSearchActionHistoryList && modelSearchActionHistoryList.length">
                  <div class="action-list-title">{{$t('searchHistory')}}</div>
                  <div class="action-content" v-for="(item, index) in modelSearchActionHistoryList" :key="index">
                    <div class="action-title">
                      <i :class="item.icon" class="ksd-mr-6 search-list-icon"></i>
                      <div class="action-desc" v-html="item.title"></div>
                    </div>
                    <div class="action-detail"></div>
                  </div>
                </div>
              </el-col>
            </el-row>
            <div class="search-footer"><span><el-tag type="beta" size="mini">Beta</el-tag>{{$t('betaSearchTips')}}<a class="feedback-btn" target="_blank" :href="$store.state.system.serverAboutKap['ke.license.isEvaluation'] ? supportUrl : $t('introductionUrl')">{{$t('feedback')}}</a></span></div>
          </div>
        </transition>
      </div>
      <div class="model-actions">
        <div class="btn-group ky-no-br-space">
          <el-button @click="goModelList" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button size="medium" type="primary" @click="saveModelEvent" :loading="saveBtnLoading">{{$t('kylinLang.common.save')}}</el-button>
        </div>
      </div>
    </div>
    <div class="model-edit-outer" @drop='dropTable($event)' @dragover='allowDrop($event)' v-drag="{sizeChangeCb:dragBox}" @dragleave="dragLeave" @click="removeTableFocus">
      <el-alert class="lose-fact-table-alert" v-if="modelRender && modelRender.tables && Object.keys(modelRender.tables).length && !showFactTableAlert" :title="$t('loseFactTableAlert')" type="warning" show-icon :closable="false"></el-alert>
      <div class="model-edit" :style="getModelEditStyles">
        <!-- 为了保证 gif 每次都从第一帧开始播放，所以每次都要重新加载 -->
        <kylin-empty-data class="gifEmptyData" :content="$t('noTableTip')" :image="require('../../../../assets/img/editmodel.gif') + '?v=' + new Date().getTime()" v-if="!Object.keys(modelRender.tables).length"></kylin-empty-data>
        <!-- table box -->
        <div
          :ref="`table_${t.guid}`"
          class="table-box"
          @click="(e) => activeTablePanel(e, t)"
          @mouseleave="removeCustomHoverStatus"
          :id="t.guid"
          v-event-stop
          :class="['js_' + t.alias.toLocaleLowerCase(), {'isLookup':t.kind==='LOOKUP'}]"
          v-for="t in modelRender && modelRender.tables || []"
          :key="`${t.guid}`"
          :style="tableBoxStyle(t.drawSize)"
        >
          <div :class="['table-title', {'table-spread-out': !t.spreadOut}]" :data-zoom="modelRender.zoom" v-drag:change.left.top="t.drawSize" @dblclick="handleDBClick(t)">
            <span class="table-sign">
              <el-tooltip :content="$t(t.kind)" placement="top">
                <i class="el-ksd-n-icon-symbol-f-filled kind" v-if="t.kind === 'FACT'"></i>
                <i v-else class="el-ksd-n-icon-dimention-table-filled kind"></i>
              </el-tooltip>
            </span>
            <span class="alias-span name">
              <span v-custom-tooltip="{text: t.alias, w: 10, effect: 'dark', 'popper-class': 'popper--small model-alias-tooltip', 'visible-arrow': false, position: 'bottom-start', observerId: t.guid}">{{t.alias}}</span>
            </span>
            <el-tooltip :content="`${t.columns.length}`" placement="top" :disabled="typeof getColumnNums(t) === 'number'">
              <span class="table-column-nums">{{getColumnNums(t)}}</span>
            </el-tooltip>
            <el-dropdown ref="tableActionsDropdown" class="table-dropdown" trigger="click" :disabled="isSchemaBrokenModel">
              <span class="setting-icon" @click.stop v-if="!isSchemaBrokenModel"><i class="el-ksd-n-icon-more-vertical-filled"></i></span>
              <el-dropdown-menu class="table-actions-dropdown" slot="dropdown">
                <el-dropdown-item>
                  <div v-if="t.kind === 'FACT' || modelInstance.checkTableCanSwitchFact(t.guid)">
                    <div class="action switch" :class="{'disabled': t.source_type === 1}" v-if="t.kind === 'FACT'" @click.stop="changeTableType(t)">
                      <span>{{$t('switchLookup')}}</span>
                    </div>
                    <div class="action switch" v-if="modelInstance.checkTableCanSwitchFact(t.guid)" @click.stop="changeTableType(t)">
                      <span >{{$t('switchFact')}}</span>
                    </div>
                  </div>
                </el-dropdown-item>
                <el-dropdown-item>
                  <div class="spread-or-expand-table" @click="handleDBClick(t)"><span>{{$t(t.spreadOut ? 'spreadTableColumns' : 'expandTableColumns')}}</span><span class="db">{{$t('doubleClick')}}</span></div>
                </el-dropdown-item>
                <el-dropdown-item v-if="t.kind !== 'FACT'">
                  <div @click.stop="openEditAliasForm(t)">{{$t('editTableAlias')}}</div>
                </el-dropdown-item>
                <el-dropdown-item>
                  <div class="action del" @click.stop="delTable(t)"> {{$t('kylinLang.common.delete')}}</div>
                </el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </div>
          <div class="column-search-box" v-show="t.spreadOut && !showOnlyConnectedColumn" @click.stop><el-input prefix-icon="el-ksd-icon-search_22" v-model="t.filterColumnChar" @input="t.filterColumns()" size="small" :placeholder="$t('kylinLang.common.search')"></el-input></div>
          <div class="column-list-box ksd-drag-box" :ref="`${t.guid}_column_box`" v-if="!t.drawSize.isOutOfView && t.showColumns.length" v-scroll.observe.reactive @scroll-bottom="handleScrollBottom(t)">
            <ul>
              <li class="column-search-results" v-if="t.filterColumnChar && t._cache_search_columns.length > 0"><span>{{$t('searchResults', {number: t._cache_search_columns.length})}}</span></li>
              <li
                :id="`${t.guid}_${col.column}`"
                @dragover="(e) => {dragColumnEnter(e, t, col)}"
                @dragleave="dragColumnLeave"
                @dragstart="(e) => {dragColumns(e, col, t)}"
                @mouseenter="(e) => handleMouseEnterColumn(e, t, col)"
                @mouseleave="(e) => handleMouseLeave(e, t, col)"
                draggable
                class="column-li"
                :class="{'column-li-cc': col.is_computed_column, 'is-link': col.isPK || col.isFK}"
                @drop.stop='(e) => {dropColumn(e, col, t)}'
                v-for="col in getCurrentColumns(t)"
                :key="col.name"
              >
                <span class="ksd-nobr-text">
                  <span class="col-type-icon">
                    <span class="is-pfk" v-show="col.isPK || col.isFK">{{`${col.isFK && col.isPK ? 'FK PK' : col.isFK ? 'FK' : 'PK'}`}}</span><i :class="columnTypeIconMap(col.datatype)"></i>
                  </span>
                  <span :class="['col-name']" v-custom-tooltip="{text: col.name, w: 30, effect: 'dark', 'popper-class': 'popper--small', 'visible-arrow': false, position: 'bottom-start', observerId: t.guid}">{{col.name}}</span>
                </span>
              </li>
              <!-- 渲染可计算列 -->
              <template v-if="t.kind=== 'FACT'">
                <li :class="['column-li', 'column-li-cc', {'is-link': col.isPK || col.isFK}]"
                  @drop='(e) => {dropColumn(e, {name: col.columnName }, t)}'
                  @dragover="(e) => {dragColumnEnter(e, t, col)}"
                  @dragleave="dragColumnLeave"
                  @dragstart="(e) => {dragColumns(e, col, t)}"
                  @mouseleave="(e) => handleMouseLeave(e, t, col)"
                  @mouseenter="(e) => handleMouseEnterColumn(e, t, col)"
                  draggable
                  v-for="col in getCurrentColumns(modelRender, 'cc')"
                  :key="col.name"
                >
                  <span class="ksd-nobr-text">
                    <span class="col-type-icon">
                      <span class="is-pfk" v-show="col.isPK || col.isFK">{{`${col.isFK && col.isPK ? 'FK PK' : col.isFK ? 'FK' : 'PK'}`}}</span><i :class="columnTypeIconMap(col.datatype)"></i>
                    </span>
                    <span :class="['col-name']" v-custom-tooltip="{text: col.columnName, w: 30, effect: 'dark', 'popper-class': 'popper--small', 'visible-arrow': false, position: 'bottom-start', observerId: t.guid}">{{col.columnName}}</span>
                  </span>
                </li>
              </template>
              <li class="li-load-more" v-if="t.hasMoreColumns && t.hasScrollEnd && !showOnlyConnectedColumn"><i class="el-ksd-icon-loading_16"></i></li>
            </ul>
          </div>
          <kylin-nodata v-show="t.showColumns.length === 0 && t.filterColumnChar" :content="$t('noResults')"></kylin-nodata>
          <!-- 拖动操纵 -->
          <DragBar :dragData="t.drawSize" :dragZoom="modelRender.zoom"/>
          <!-- 拖动操纵 -->
        </div>
        <!-- table box end -->
      </div>
      <!-- datasource面板  index 3-->
      <div class="tool-icon icon-ds" v-if="panelAppear.datasource.icon_display" :class="{active: panelAppear.datasource.display}" v-event-stop @click="toggleMenu('datasource')"><i class="el-icon-ksd-data_source"></i></div>
      <transition name="bounceleft">
        <div class="panel-box panel-datasource"  v-show="panelAppear.datasource.display" :style="panelStyle('datasource')" v-event-stop  @click.stop>
          <div class="panel-title" v-drag:change.left.top="panelAppear.datasource"><span class="title">{{$t('kylinLang.common.dataSource')}}</span><span class="close" @click="toggleMenu('datasource')"><i class="el-icon-ksd-close"></i></span></div>
          <DataSourceBar
            :ignore-node-types="['column']"
            class="tree-box"
            :class="{'iframeTreeBox': $store.state.config.platform === 'iframe'}"
            ref="datasourceTree"
            :project-name="currentSelectedProject"
            :is-show-load-source="true"
            :is-show-load-table="datasourceActions.includes('loadSource')"
            :is-show-load-table-inner-btn="datasourceActions.includes('loadSource')"
            :is-show-settings="false"
            :is-show-action-group="false"
            :is-expand-on-click-node="false"
            :expand-node-types="['datasource', 'database']"
            :draggable-node-types="['table']"
            :searchable-node-types="['table']"
            :is-model-have-fact="modelInstance && !!modelInstance.fact_table"
            :is-second-storage-enabled="modelInstance && modelInstance.second_storage_enabled"
            @drag="dragTable">
          </DataSourceBar>
          <!-- </div> -->
          <!-- 拖动操纵 -->
          <DragBar :dragData="panelAppear.datasource"/>
          <!-- 拖动操纵 -->
        </div>
      </transition>
      <!-- datasource面板  end-->
      <div class="tool-icon-group" v-event-stop>
        <div class="tool-icon broken-icon" v-if="panelAppear.brokenFocus.icon_display" @click="focusBrokenLinkedTable">
          <i class="el-icon-ksd-broken_disconnect"></i>
        </div>
        <div class="tool-icon" v-if="panelAppear.dimension.icon_display" :class="{active: panelAppear.dimension.display}" @click="toggleMenu('dimension')">D</div>
        <div class="tool-icon" v-if="panelAppear.measure.icon_display" :class="{active: panelAppear.measure.display}" @click="toggleMenu('measure')">M</div>
        <div class="tool-icon" v-if="panelAppear.cc.icon_display" :class="{active: panelAppear.cc.display}" @click="toggleMenu('cc')"><i class="el-icon-ksd-computed_column"></i></div>
        <!-- <div class="tool-icon" v-if="panelAppear.search.icon_display" :class="{active: panelAppear.search.display}" @click="toggleMenu('search')">
          <i class="el-icon-ksd-search"></i>
          <span class="new-icon">New</span>
        </div> -->
      </div>
      <ModelNavigationTools :zoom="modelRender.zoom" @command="handleActionsCommand" @addZoom="addZoom" @reduceZoom="reduceZoom" @autoLayout="autoLayout"/>
      <!-- 右侧面板组 -->
      <!-- dimension面板  index 0-->
      <transition name="bounceright">
        <div class="panel-box panel-dimension" @mousedown.stop="activePanel('dimension')" :style="panelStyle('dimension')" v-if="panelAppear.dimension.display">
          <div class="panel-title" @mousedown="activePanel('dimension')" v-drag:change.right.top="panelAppear.dimension">
            <span><i class="el-icon-ksd-dimension"></i></span>
            <span class="title">{{$t('kylinLang.common.dimension')}} <template v-if="(modelRender.dimensions || []).length">({{modelRender.dimensions.length}})</template></span>
            <span class="close" @click="toggleMenu('dimension')"><i class="el-icon-ksd-close"></i></span>
          </div>
          <div class="panel-sub-title">
            <div class="action_group" :class="{'is_active': !isShowCheckbox}">
              <!-- <span class="action_btn" @click="addCCDimension">
                <i class="el-icon-ksd-project_add"></i>
                <span>{{$t('add')}}</span>
              </span> -->
              <span class="action_btn" @click="batchSetDimension">
                <i class="el-icon-ksd-backup"></i>
                <span>{{$t('batchAdd')}}</span>
              </span>
              <span class="action_btn" :class="{'disabled': allDimension.length==0}" @click="toggleCheckbox">
                <i class="el-icon-ksd-batch_delete"></i>
                <span>{{$t('batchDel')}}</span>
              </span>
            </div>
            <div
            class="batch_group"
            :class="{'is_active': isShowCheckbox}"
            :style="{transform: isShowCheckbox ? 'translateX(0)' : 'translateX(100%)'}"
            >
              <span class="action_btn" :class="{'disabled': isDisableBatchCheck}" @click="toggleCheckAllDimension">
                <i class="el-icon-ksd-batch_uncheck" v-if="dimensionSelectedList.length==modelRender.dimensions.length || (modelInstance.second_storage_enabled||isHybridModel)&&dimensionSelectedList.length+1==modelRender.dimensions.length&&!isDisableBatchCheck"></i>
                <i class="el-icon-ksd-batch" v-else></i>
                <span v-if="dimensionSelectedList.length==modelRender.dimensions.length || (modelInstance.second_storage_enabled || isHybridModel) && dimensionSelectedList.length+1 == modelRender.dimensions.length && !isDisableBatchCheck">{{$t('unCheckAll')}}</span>
                <span v-else>{{$t('checkAll')}}</span>
              </span>
              <span class="action_btn" :class="{'disabled': dimensionSelectedList.length === 0}" @click="deleteDimenisons">
                <i class="el-icon-ksd-table_delete"></i>
                <span>{{$t('delete')}}</span>
              </span>
              <span class="action_btn" @click="toggleCheckbox">
                <i class="el-icon-ksd-back"></i>
                <span>{{$t('back')}}</span>
              </span>
            </div>
          </div>
          <div class="panel-main-content" @dragover='($event) => {allowDropColumnToPanle($event)}' @drop='(e) => {dropColumnToPanel(e, "dimension")}'>
            <div class="content-scroll-layout" v-if="allDimension.length" v-scroll.observe.reactive @scroll-bottom="boardScrollBottom('dimension')">
              <ul class="dimension-list">
                <li v-for="(d, i) in allDimension" :key="`${d.name}_${i}`" :class="{'is-checked':dimensionSelectedList.indexOf(d.name)>-1}">
                  <span :class="['ksd-nobr-text', {'checkbox-text-overflow': isShowCheckbox}]">
                    <el-checkbox v-model="dimensionSelectedList" v-if="isShowCheckbox" :disabled="(modelInstance.second_storage_enabled||isHybridModel)&&modelInstance.partition_desc&&modelInstance.partition_desc.partition_date_column===d.column" :label="d.name" class="text">{{d.name}}</el-checkbox>
                    <span v-else :title="d.name" class="text">{{d.name}}</span>
                    <span class="icon-group">
                      <el-tooltip :content="disableDelDimTips" placement="top-end" :disabled="!((modelInstance.second_storage_enabled||isHybridModel)&&modelInstance.partition_desc&&modelInstance.partition_desc.partition_date_column===d.column)">
                        <span class="icon-span" :class="{'is-disabled': (modelInstance.second_storage_enabled||isHybridModel)&&modelInstance.partition_desc&&modelInstance.partition_desc.partition_date_column===d.column}" @click="deleteDimenison(d)"><i class="el-icon-ksd-table_delete"></i></span>
                      </el-tooltip>
                      <span class="icon-span"><i class="el-icon-ksd-table_edit" @click="editDimension(d, i)"></i></span>
                      <span class="li-type ky-option-sub-info">{{d.datatype && d.datatype.toLocaleLowerCase()}}</span>
                    </span>
                  </span>
                </li>
              </ul>
            </div>
            <kylin-nodata v-else></kylin-nodata>
          </div>
          <!-- 拖动操纵 -->
          <DragBar :dragData="panelAppear.dimension"/>
          <!-- 拖动操纵 -->
        </div>
      </transition>
      <!-- measure面板  index 1-->
      <transition name="bounceright">
        <div class="panel-box panel-measure" @mousedown.stop="activePanel('measure')" :style="panelStyle('measure')"  v-if="panelAppear.measure.display">
          <div class="panel-title" @mousedown="activePanel('measure')" v-drag:change.right.top="panelAppear.measure">
            <span><i class="el-icon-ksd-measure"></i></span>
            <span class="title">{{$t('kylinLang.common.measure')}}<template v-if="modelRender.all_measures.length">({{modelRender.all_measures.length}})</template></span>
            <span class="close" @click="toggleMenu('measure')"><i class="el-icon-ksd-close"></i></span>
          </div>
          <div class="panel-sub-title">
            <div class="action_group" :class="{'is_active': !isShowMeaCheckbox}">
              <span class="action_btn" @click="addNewMeasure">
                <i class="el-icon-ksd-project_add"></i>
                <span>{{$t('add')}}</span>
              </span>
              <span class="action_btn" @click="batchSetMeasure">
                <i class="el-icon-ksd-backup"></i>
                <span>{{$t('batchAdd')}}</span>
              </span>
              <span class="action_btn" @click="toggleMeaCheckbox" :class="{'disabled': canDelMeasureAll}">
                <i class="el-icon-ksd-batch_delete"></i>
                <span>{{$t('batchDel')}}</span>
              </span>
            </div>
            <div
              class="batch_group"
              :class="{'is_active': isShowMeaCheckbox}"
              :style="{transform: isShowMeaCheckbox ? 'translateX(0)' : 'translateX(100%)'}"
            >
              <span class="action_btn" @click="toggleCheckAllMeasure">
                <i class="el-icon-ksd-batch" v-if="measureSelectedList.length > 0 && measureSelectedList.length === toggleMeasureStatus"></i>
                <i class="el-icon-ksd-batch_uncheck" v-else></i>
                <span v-if="measureSelectedList.length > 0 && measureSelectedList.length === toggleMeasureStatus">{{$t('unCheckAll')}}</span>
                <span v-else>{{$t('checkAll')}}</span>
              </span>
              <span class="action_btn" :class="{'disabled': measureSelectedList.length==0}" @click="deleteMeasures">
                <i class="el-icon-ksd-table_delete"></i>
                <span>{{$t('delete')}}</span>
              </span>
              <span class="action_btn" @click="toggleMeaCheckbox">
                <i class="el-icon-ksd-back"></i>
                <span>{{$t('back')}}</span>
              </span>
            </div>
          </div>
          <div class="panel-main-content" @dragover='($event) => {allowDropColumnToPanle($event)}' @drop='(e) => {dropColumnToPanel(e, "measure")}'>
            <div class="content-scroll-layout" v-if="allMeasure.length" v-scroll.observe.reactive @scroll-bottom="boardScrollBottom('measure')">
              <ul class="measure-list">
                <li v-for="m in allMeasure" :key="m.name" :class="{'is-checked':measureSelectedList.indexOf(m.name)>-1, 'error-measure': ['SUM', 'PERCENTILE_APPROX'].includes(m.expression) && m.return_type && m.return_type.indexOf('varchar') > -1}">
                  <span :class="['ksd-nobr-text', {'checkbox-text-overflow': isShowMeaCheckbox}]">
                    <el-tooltip class="count-all" :offset="isShowMeaCheckbox ? 50 : 60" :content="m.name ==='COUNT_ALL' ? $t('disabledConstantMeasureTip') : $t('measureRuleErrorTip', {type: m.expression})" effect="dark" placement="bottom" :disabled="!(['SUM', 'PERCENTILE_APPROX'].includes(m.expression) && m.return_type && m.return_type.indexOf('varchar') > -1) && m.name !== 'COUNT_ALL'">
                      <span>
                        <el-checkbox v-model="measureSelectedList" v-if="isShowMeaCheckbox" :disabled="m.name === 'COUNT_ALL'" :label="m.name" class="text">{{m.name}}</el-checkbox>
                        <span v-else class="text">{{m.name}}</span>
                      </span>
                    </el-tooltip>
                    <span class="icon-group">
                      <span class="icon-span" v-if="m.name !== 'COUNT_ALL'"><i class="el-icon-ksd-table_delete" @click="deleteMeasure(m.name)"></i></span>
                      <span class="icon-span" v-if="m.name !== 'COUNT_ALL'"><i class="el-icon-ksd-table_edit" @click="editMeasure(m)"></i></span>
                      <span class="li-type ky-option-sub-info">{{m.return_type && m.return_type.toLocaleLowerCase()}}</span>
                    </span>
                  </span>
                </li>
              </ul>
            </div>
            <kylin-nodata v-else></kylin-nodata>
          </div>
          <!-- 拖动操纵 -->
          <DragBar :dragData="panelAppear.measure"/>
          <!-- 拖动操纵 -->
        </div>
      </transition>
      <!-- 可计算列 -->
      <transition name="bounceright">
        <div class="panel-box panel-cc" @mousedown.stop="activePanel('cc')" :style="panelStyle('cc')"  v-if="panelAppear.cc.display">
          <div class="panel-title" @mousedown="activePanel('cc')" v-drag:change.right.top="panelAppear.cc">
            <span><i class="el-ksd-icon-auto_computed_column_old"></i></span>
            <span class="title">{{$t('kylinLang.model.computedColumn')}} <template v-if="modelRender.computed_columns.length">({{modelRender.computed_columns.length}})</template></span>
            <span class="close" @click="toggleMenu('cc')"><i class="el-icon-ksd-close"></i></span>
          </div>
          <div class="panel-sub-title">
            <div class="action_group" :class="{'is_active': !isShowCCCheckbox}">
              <el-tooltip :content="$t('forbidenCreateCCTip')" :disabled="!isHybridModel">
                <span :class="['action_btn', {'disabled': isHybridModel}]" @click="!isHybridModel && addCC()">
                  <i class="el-icon-ksd-project_add"></i>
                  <span>{{$t('add')}}</span>
                </span>
              </el-tooltip>
              <span class="action_btn" @click="toggleCCCheckbox" :class="{'active': isShowCCCheckbox}">
                <i class="el-icon-ksd-batch_delete"></i>
                <span>{{$t('batchDel')}}</span>
              </span>
            </div>
            <div
              class="batch_group"
              :class="{'is_active': isShowCCCheckbox}"
            >
              <span class="action_btn" @click="toggleCheckAllCC">
                <i class="el-icon-ksd-batch_uncheck" v-if="ccSelectedList.length==modelRender.computed_columns.length"></i>
                <i class="el-icon-ksd-batch" v-else></i>
                <span v-if="ccSelectedList.length==modelRender.computed_columns.length">{{$t('unCheckAll')}}</span>
                <span v-else>{{$t('checkAll')}}</span>
              </span>
              <span class="action_btn" :class="{'disabled': ccSelectedList.length==0}" @click="delCCs">
                <i class="el-icon-ksd-table_delete"></i>
                <span>{{$t('delete')}}</span>
              </span>
              <span class="action_btn" @click="toggleCCCheckbox">
                <i class="el-icon-ksd-back"></i>
                <span>{{$t('back')}}</span>
              </span>
            </div>
          </div>
          <div class="panel-main-content" v-scroll.obverse  v-if="modelRender.computed_columns.length">
            <ul class="cc-list">
              <li v-for="m in modelRender.computed_columns" :key="m.name" :class="{'is-checked':ccSelectedList.indexOf(m.columnName)>-1}">
                <span :class="['ksd-nobr-text', {'checkbox-text-overflow': isShowCCCheckbox}]">
                  <el-checkbox v-model="ccSelectedList" v-if="isShowCCCheckbox" :label="m.columnName" class="text">{{m.columnName}}</el-checkbox>
                  <span v-else class="text">{{m.columnName}}</span>
                  <span class="icon-group">
                    <span class="icon-span"><i class="el-icon-ksd-table_delete" @click="delCC(m.columnName)"></i></span>
                    <span class="icon-span"><i class="el-icon-ksd-table_edit" @click="editCC(m)"></i></span>
                    <span class="li-type ky-option-sub-info">{{m.datatype && m.datatype.toLocaleLowerCase()}}</span>
                  </span>
                </span>
              </li>
            </ul>
          </div>
          <kylin-nodata v-if="!modelRender.computed_columns.length"></kylin-nodata>
          <!-- 拖动操纵 -->
          <DragBar :dragData="panelAppear.cc"/>
          <!-- 拖动操纵 -->
        </div>
      </transition>
      <!-- 搜索面板 -->

      <ModelSaveConfig/>
      <DimensionModal/>
      <BatchMeasureModal @betchMeasures="updateBetchMeasure"/>
      <TableJoinModal/>
      <AddMeasure
        v-if="measureVisible"
        :isEditMeasure="isEditMeasure"
        :measureObj="measureObj"
        :modelInstance="modelInstance"
        :isHybridModel="isHybridModel"
        v-on:closeAddMeasureDia="closeAddMeasureDia">
      </AddMeasure>
      <SingleDimensionModal/>
      <AddCC/>
      <ShowCC/>
      <ActionUpdateGuide v-if="showModelGuide" @close-guide="showModelGuide = false" />

      <el-dialog
        :title="$t('kylinLang.common.tip')"
        :visible.sync="gotoIndexdialogVisible"
        width="30%"
        append-to-body
        limited-area
        class="add-index-confirm-dialog"
        :close-on-click-modal="false"
        :show-close="false">
        <i class="el-icon-success ksd-mr-10 ky-dialog-icon"></i>
        <div class="ksd-pl-26">
          <div>{{$t('saveSuccessTip')}}</div>
          <div>
            <span v-if="getBaseIndexCount(saveModelResponse).createBaseIndexNum > 0">{{$t('createAndBuildBaseIndexTips', {createBaseIndexNum: getBaseIndexCount(saveModelResponse).createBaseIndexNum})}}</span>
            <span v-if="getBaseIndexCount(saveModelResponse).createBaseIndexNum === 0">{{$t('addIndexTips')}}</span>
            <span v-else>{{$t('addIndexAndBaseIndex')}}</span>
          </div>
        </div>
        <span slot="footer" class="dialog-footer" v-if="gotoIndexdialogVisible">
          <el-button plain @click="ignoreAddIndex">{{getBaseIndexCount(saveModelResponse).createBaseIndexNum > 0 ? $t('kylinLang.common.cancel') : $t('ignoreaddIndexTip')}}</el-button>
          <el-button type="primary" @click="willAddIndex">{{getBaseIndexCount(saveModelResponse).createBaseIndexNum > 0 ? $t('viewIndexes') : $t('addIndex')}}</el-button>
        </span>
      </el-dialog>
    </div>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations, mapState } from 'vuex'
import locales from './locales'
import DataSourceBar from '../../../common/DataSourceBar'
import { handleSuccess, handleError, loadingBox, kylinMessage, kylinConfirm } from '../../../../util/business'
import { isIE, groupData, objectClone, filterObjectArray, handleSuccessAsync, indexOfObjWithSomeKey, debounceEvent } from '../../../../util'
import $ from 'jquery'
import DimensionModal from '../DimensionsModal/index.vue'
import BatchMeasureModal from '../BatchMeasureModal/index.vue'
import AddMeasure from '../AddMeasure/index.vue'
import TableJoinModal from '../TableJoinModal/index.vue'
import SingleDimensionModal from '../SingleDimensionModal/addDimension.vue'
import ModelSaveConfig from '../ModelList/ModelSaveConfig/index.vue'
import DragBar from './dragbar.vue'
import AddCC from '../AddCCModal/addcc.vue'
import ShowCC from '../ShowCC/showcc.vue'
import NModel from './model.js'
import ActionUpdateGuide from '../../../guide/modelEditPage/ActionUpdateGuide'
import ModelTitleDescription from '../ModelList/Components/ModelTitleDescription'
import ModelNavigationTools from '../../../common/ModelTools/ModelNavigationTools'
import { modelRenderConfig, modelErrorMsg } from './config'
import { NamedRegex, columnTypeIcon } from '../../../../config'
@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      vm.fromRoute = from
      vm.extraoption = {
        project: vm.currentSelectedProject,
        modelName: to.params.modelName,
        action: to.params.action
      }
      // 在添加模型页面刷新，跳转到列表页面
      if (to.name === 'ModelEdit' && to.params.action === 'add' && from.name === null) {
        vm.$router.replace({name: 'ModelList', params: { ignoreIntercept: true }})
        return
      }
      vm.initEditModel()
    })
  },
  beforeRouteLeave (to, from, next) {
    if (this.$store.state.config.platform === 'iframe') {
      next()
    } else {
      if (!to.params.ignoreIntercept) {
        next(false)
        setTimeout(() => {
          this.$confirm(this.$t('kylinLang.common.willGo'), this.$t('kylinLang.common.notice'), {
            confirmButtonText: this.$t('discardChange'),
            cancelButtonText: this.$t('continueEditing'),
            type: 'warning'
          }).then(() => {
            if (to.name === 'refresh') { // 刷新逻辑下要手动重定向
              next()
              this.$nextTick(() => {
                this.$router.replace({name: 'ModelList', params: { refresh: true }})
              })
              return
            }
            next()
          }).catch(() => {
            if (to.name === 'refresh') { // 取消刷新逻辑，所有上一个project相关的要撤回
              let preProject = cacheSessionStorage('preProjectName') // 恢复上一次的project
              this.setProject(preProject)
              this.getUserAccess({project: preProject})
            }
            next(false)
          })
        })
      } else {
        next()
      }
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isFullScreen',
      'datasourceActions',
      'supportUrl'
    ]),
    ...mapState('TableJoinModal', {
      tableJoinDialogShow: state => state.isShow
    }),
    ...mapState('SingleDimensionModal', {
      singleDimensionDialogShow: state => state.isShow
    }),
    ...mapState('DimensionsModal', {
      dimensionDialogShow: state => state.isShow
    }),
    ...mapState('BatchMeasureModal', {
      showBatchMeasureDialogShow: state => state.isShow
    })
  },
  methods: {
    ...mapMutations({
      clearDatasourceCache: 'CLEAR_DATASOURCE_CACHE',
      resetOtherColumns: 'RESET_OTHER_COLUMNS'
    }),
    ...mapActions({
      getModelByModelName: 'LOAD_MODEL_INFO',
      loadDataSourceByModel: 'LOAD_DATASOURCE_OF_MODEL',
      saveModel: 'SAVE_MODEL',
      updataModel: 'UPDATE_MODEL',
      invalidIndexes: 'INVALID_INDEXES'
    }),
    ...mapActions('DimensionsModal', {
      showDimensionDialog: 'CALL_MODAL'
    }),
    ...mapActions('BatchMeasureModal', {
      showBatchMeasureDialog: 'CALL_MODAL'
    }),
    ...mapActions('TableJoinModal', {
      showJoinDialog: 'CALL_MODAL'
    }),
    ...mapActions('SingleDimensionModal', {
      showSingleDimensionDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelSaveConfig', {
      showSaveConfigDialog: 'CALL_MODAL'
    }),
    ...mapMutations({
      toggleFullScreen: 'TOGGLE_SCREEN'
    }),
    ...mapActions('CCAddModal', {
      showAddCCDialog: 'CALL_MODAL'
    }),
    ...mapActions('ShowCCDialogModal', {
      showCCDetailDialog: 'CALL_MODAL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapActions('GuideModal', {
      callGuideModal: 'CALL_MODAL'
    }),
    ...mapActions('ConfirmSegment', {
      callConfirmSegmentModal: 'CALL_MODAL'
    })
  },
  components: {
    DataSourceBar,
    DragBar,
    AddMeasure,
    DimensionModal,
    BatchMeasureModal,
    TableJoinModal,
    SingleDimensionModal,
    ModelSaveConfig,
    AddCC,
    ShowCC,
    ActionUpdateGuide,
    ModelTitleDescription,
    ModelNavigationTools
  },
  locales
})
export default class ModelEdit extends Vue {
  extraoption = null
  fromRoute = null
  datasource = []
  modelRender = {tables: {}}
  dimensionSelectedList = []
  measureSelectedList = []
  gotoIndexdialogVisible = false // 保存成功弹窗
  saveModelResponse = {} // 保存模型返回数据
  ccSelectedList = []
  modelInstance = null // 模型实例对象
  currentDragTable = '' // 当前拖拽的表
  currentDragColumn = '' // 当前拖拽的列
  currentDropColumnData = {} // 当前释放到的列
  currentDragColumnData = {} // 当前拖拽列携带信息
  modelGlobalSearch = '' // model全局搜索信息
  showSearchResult = true
  modelGlobalSearchResult = []
  modelData = null
  columnTypeIconMap = columnTypeIcon
  modelSearchActionSuccessTip = ''
  modelSearchActionHistoryList = []
  globalLoading = loadingBox()
  renderBox = modelRenderConfig.drawBox
  measureVisible = false
  isEditMeasure = false
  allColumns = []
  // baseIndex = modelRenderConfig.baseIndex
  autoSetting = true
  stCycle = null
  measureObj = {
    name: '',
    expression: 'SUM(column)',
    parameterValue: {type: 'column', value: '', table_guid: null},
    convertedColumns: [],
    return_type: ''
  }
  aliasRules = {
    currentEditAlias: [
      { validator: this.validateName, trigger: 'blur' }
    ]
  }
  isIgnore = false
  isFullLoad = false
  saveModelType = ''
  isPurgeSegment = false
  panelAppear = modelRenderConfig.pannelsLayout()
  radio = 1
  isShowCheckbox = false
  isShowMeaCheckbox = false
  isShowCCCheckbox = false
  // 快捷编辑table操作 start
  showTableCoverDiv = false
  currentEditTable = null
  showEditAliasForm = false
  formTableAlias = {
    currentEditAlias: ''
  }
  delTipVisible = false
  // 记录 join 关系被更改的维表 guid
  exchangeJoinTableList = []
  boardPager = {
    dimension: {
      pageSize: 10,
      pageOffset: 1
    },
    measure: {
      pageSize: 10,
      pageOffset: 1
    }
  }
  dimensionBoardPager = 1
  listenerDragPoint = false
  selectedConnect = null
  dragPoint = { x: 0, y: 0 }
  currentPot = { x: 0, y: 0 }
  linkLineFocus = []
  showModelGuide = false
  saveBtnLoading = false
  showOnlyConnectedColumn = false
  debounceTimer = {
    tableType: null,
    expand: null
  }
  get disableDelDimTips () {
    if (this.isHybridModel) {
      return this.$t('streamTips')
    } else {
      return this.$t('disableDelDimTips')
    }
  }
  get isHybridModel () {
    return this.modelInstance.getFactTable() && this.modelInstance.getFactTable().batch_table_identity || this.modelInstance.model_type === 'HYBRID'
  }
  get allDimension () {
    return this.modelRender.dimensions.slice(0, this.boardPager.dimension.pageOffset * this.boardPager.dimension.pageSize) || []
  }
  get allMeasure () {
    return this.modelRender.all_measures.slice(0, this.boardPager.measure.pageOffset * this.boardPager.measure.pageSize) || []
  }
  get canDelMeasureAll () { // 控制批量删除按钮的 disable 状态
    let flag = true // 默认不可点
    if (this.modelRender.all_measures.length === 0) {
      flag = true
    } else {
      let temp = this.modelRender.all_measures.filter((item) => {
        return item.name === 'COUNT_ALL'
      })
      if (temp.length > 0) { // 如果有count all 这个度量，则批量删除按钮不可用
        flag = this.modelRender.all_measures.length === temp.length
      } else {
        flag = this.modelRender.all_measures.length === 0
      }
    }
    return flag
  }
  get toggleMeasureStatus () { // 控制批量删除度量的全选切换按钮的状态
    let temp = this.modelRender.all_measures.filter((item) => {
      return item.name === 'COUNT_ALL'
    })
    if (temp.length > 0) { // 如果有count all 全选文案的切换要去掉count all 后
      return this.modelRender.all_measures.length - 1
    } else {
      return this.modelRender.all_measures.length
    }
  }
  get isSchemaBrokenModel () {
    return this.modelRender.broken_reason === 'SCHEMA'
  }
  get isDisableBatchCheck () {
    if (this.allDimension.length === 1 && (this.modelInstance.second_storage_enabled || this.isHybridModel) && this.modelInstance.partition_desc && this.modelInstance.partition_desc.partition_date_column === this.allDimension[0].column) {
      return true
    } else {
      return false
    }
  }
  get tableBoxStyleNoZoom () {
    return (drawSize) => {
      if (drawSize) {
        // let zoom = this.modelRender.zoom / 10
        return {'z-index': drawSize.zIndex, width: drawSize.width + 'px', height: drawSize.height + 'px', left: drawSize.left + 'px', top: drawSize.top + 'px'}
      }
    }
  }
  get searchResultData () {
    return groupData(this.modelGlobalSearchResult, 'kind')
  }
  // 是否展示缺失事实表提醒
  get showFactTableAlert () {
    return Object.values(this.modelRender.tables).filter(table => table.kind === 'FACT').length > 0
  }
  // 画布位置调整
  get getModelEditStyles () {
    if (!this.modelRender || !this.modelRender.marginClient) return {marginLeft: 0, marginTop: 0}
    const { left, top } = this.modelRender.marginClient
    return {marginLeft: `${left}px`, marginTop: `${top}px`}
  }
  // 获取当前展示的列
  getCurrentColumns (t, type) {
    const columns = type === 'cc' ? t.computed_columns : t.showColumns
    return this.showOnlyConnectedColumn ? columns.filter(it => it.isPK || it.isFK) : columns
  }
  @Watch('modelGlobalSearch')
  watchSearch (v) {
    this.showSearchResult = v
  }
  // 当维度或度量高度改变时，需要增加页码以填充多余的空白区域
  changePageOfBround (obj) {
    switch (obj) {
      case 'dimension':
        const scrollContent = this.$el.querySelector('.panel-dimension .content-scroll-layout')
        const dimensionDom = this.$el.querySelector('.panel-dimension .dimension-list')
        if (!scrollContent || !dimensionDom) return
        if (dimensionDom.offsetHeight < scrollContent.offsetHeight) {
          this.boardScrollBottom('dimension')
        }
      case 'measure':
        const measureScrollContent = this.$el.querySelector('.panel-measure .content-scroll-layout')
        const measureDom = this.$el.querySelector('.panel-measure .measure-list')
        if (!measureScrollContent || !measureDom) return
        if (measureDom.offsetHeight < measureScrollContent.offsetHeight) {
          this.boardScrollBottom('measure')
        }
    }
  }
  // 获取列的数量
  getColumnNums (t) {
    return t.columns.length > 999 ? '999+' : t.columns.length
  }
  // 双击表头 - 展开或收起
  handleDBClick (t) {
    if (this.debounceTimer.expand) {
      clearTimeout(this.debounceTimer.expand)
    }
    this.debounceTimer.expand = setTimeout(() => {
      if (!t.spreadOut) {
        if (t.spreadHeight < 140) {
          this.$set(t, 'spreadHeight', 140)
        }
      } else {
        this.$set(t, 'spreadHeight', t.drawSize.height)
      }
      this.$set(t, 'spreadOut', !t.spreadOut)
      const boxH = !t.spreadOut ? this.$el.querySelector('.table-title').offsetHeight + 4 : t.spreadHeight
      this.$set(t.drawSize, 'height', boxH)
      // this.$refs[`table_${t.guid}`].length && (this.$refs[`table_${t.guid}`][0].style.cssText += `height: ${boxH}px;`)
      this.$nextTick(() => {
        this.modelInstance.plumbTool.refreshPlumbInstance()
      })
    }, 300)
  }
  // 滚动加载 columns
  handleScrollBottom (table) {
    if (table.hasMoreColumns) {
      table.hasScrollEnd = true
      setTimeout(() => {
        table.loadMoreColumns()
        table.hasScrollEnd = false
      }, 300)
    }
  }
  // 连线 selected 状态时监听键盘事件
  listenTableLink (connect) {
    this.selectedConnect = connect
    document.addEventListener('keydown', this.removeTableLinkEvent, true)
  }
  // 键盘 delete 按键删除连线
  removeTableLinkEvent (e) {
    e.returnValue = false
    if (!this.$el.querySelector('.jtk-connector.is-focus')) {
      document.removeEventListener('keydown', this.removeTableLinkEvent, true)
      return
    }
    if (e.keyCode !== 8) return
    kylinConfirm(this.$t('delConnTip'), null, this.$t('delConnTitle')).then(() => {
      this.modelInstance.removeRenderLink(this.selectedConnect)
      if (this.modelData.available_indexes_count > 0 && !this.isIgnore) {
        this.showChangeTips()
      }
    })
  }
  // 移除拖拽点状态
  removeColumnDragPots () {
    (Array.prototype.slice.call(this.$el.querySelectorAll('.column-li'))).forEach(item => item.classList.remove('is-hover'))
    const svgDom = this.$el.querySelector('.drag-svg')
    const customDragImage = document.getElementById('custom-drag-image')
    if (svgDom) {
      typeof svgDom.remove === 'function' ? svgDom.remove() : svgDom.parentNode.removeChild(svgDom)
    }
    if (customDragImage) {
      typeof customDragImage.remove === 'function' ? customDragImage.remove() : customDragImage.parentNode.removeChild(customDragImage)
    }
    this.$el.querySelectorAll('.column-point-dot').length && (Array.prototype.slice.call(this.$el.querySelectorAll('.column-point-dot'))).forEach(element => {
      if (typeof element.remove === 'function') {
        element.remove()
      } else {
        element.parentNode.removeChild(element)
      }
    })
  }
  // 移除自定义 linked column hover 属性
  removeCustomHoverStatus () {
    (Array.prototype.slice.call(this.$el.querySelectorAll('.table-box'))).forEach(item => item.classList.remove('is-hover'));
    (Array.prototype.slice.call(this.$el.querySelectorAll('.column-li'))).forEach(item => item.classList.remove('is-hover'))
  }
  // hover 一个列对应 linked column 也要展示为 hover 状态
  displayLinkColumn (t, col) {
    this.removeCustomHoverStatus()
    let { linkUsedColumns, tables } = this.modelRender
    tables = Object.values(tables)
    linkUsedColumns = Object.values(linkUsedColumns)
    const fullColumnName = `${t.alias}.${col.column || col.columnName}`
    const currentJoins = linkUsedColumns.filter(it => it.includes(fullColumnName))
    if (!currentJoins.length) return
    let links = []
    currentJoins.forEach(item => {
      const indexes = []
      item.forEach((v, index) => {
        v === fullColumnName && indexes.push(index)
      })
      indexes.forEach(num => {
        const middleNum = item.length / 2
        let [table, column] = item[num >= middleNum ? num - middleNum : num + middleNum].split('.')
        links.push({table, column})
      })
    })
    links.forEach(list => {
      const [{ guid }] = tables.filter(it => it.alias === list.table)
      const tDom = document.getElementById(`${guid}`)
      const cDom = document.getElementById(`${guid}_${list.column}`)
      const linkLines = document.getElementsByClassName(`${t.guid}&${guid}`).length > 0 ? document.getElementsByClassName(`${t.guid}&${guid}`) : document.getElementsByClassName(`${guid}&${t.guid}`)
      this.linkLineFocus.push(...linkLines)
      tDom && (tDom.className += ' is-hover')
      cDom && (cDom.className += ' is-hover');
      (Array.prototype.slice.call(linkLines)).forEach(item => {
        if (item.nodeName === 'svg') {
          const cls = item.getAttribute('class')
          item.setAttribute('class', `${cls} is-focus`)
        } else {
          item.className += ' is-focus'
        }
      })
    })
  }
  // table columns 上显示连接点
  handleMouseEnterColumn (_, table, column) {
    this.displayLinkColumn(table, column)
  }
  // 曲线连接线的 focus 状态
  cancelLinkLineFocus () {
    Array.prototype.slice.call(this.linkLineFocus).forEach(item => {
      if (item.nodeName === 'svg') {
        item.setAttribute('class', `${item.className.baseVal.replace(/is-focus/g, '')}`)
      } else {
        item.classList.remove('is-focus')
      }
    })
    this.linkLineFocus.splice(0, this.linkLineFocus.length)
    this.removeCustomHoverStatus()
  }
  handleMouseLeave (event, table, column) {
    this.cancelLinkLineFocus()
  }
  // 拖拽连接点绘制 svg
  createAndResetSvg (e) {
    e.preventDefault()
    const dragSvg = this.$el.querySelector('.drag-svg')
    const modelEdit = this.$el.querySelector('.model-edit')
    const { clientX, clientY } = e
    if (this.currentPot.x === clientX && this.currentPot.y === clientY) return
    this.currentPot.x = clientX
    this.currentPot.y = clientY
    const tableBox = this.$el.querySelector('.model-edit')
    const tableBoxBound = tableBox.getBoundingClientRect()
    let sW = Math.abs(clientX - this.dragPoint.x) / (this.modelRender.zoom / 10)
    let sH = Math.abs(clientY - this.dragPoint.y) / (this.modelRender.zoom / 10)
    sW = sW - (clientX > this.dragPoint.x && clientY > this.dragPoint.y ? -2 : 4) < 0 ? sW : sW - (clientX > this.dragPoint.x && clientY > this.dragPoint.y ? -2 : 4)
    sH = sH < 2 ? sH : sH - 2
    if (!dragSvg) {
      const sign = 'http://www.w3.org/2000/svg'
      const svg = document.createElementNS(sign, 'svg')
      const path = document.createElementNS(sign, 'path')
      svg.style.cssText = `position: absolute; top: ${(Math.min(this.dragPoint.y, clientY) - tableBoxBound.top) / (this.modelRender.zoom / 10)}px; left: ${(Math.min(this.dragPoint.x, clientX) - tableBoxBound.left + (clientX > this.dragPoint.x && clientY > this.dragPoint.y ? -3 : 2)) / (this.modelRender.zoom / 10)}px`
      svg.setAttribute('xmlns:xlink', 'http://www.w3.org/1999/xlink')
      svg.setAttribute('width', sW)
      svg.setAttribute('height', sH)
      svg.setAttribute('class', 'drag-svg')
      path.setAttribute('d', this.setSvgPath({x: clientX, y: clientY}, this.dragPoint, sW, sH))
      path.setAttribute('stroke', '#0875DA')
      path.setAttribute('stroke-width', 2)
      path.setAttribute('fill', 'none')
      // svg.appendChild(path)
      // svg.innerHTML = path
      svg.appendChild(path)
      modelEdit.appendChild(svg)
    } else {
      const dragPath = this.$el.querySelector('.drag-svg path')
      dragSvg.style.cssText = `position: absolute; top: ${(Math.min(this.dragPoint.y, clientY) - tableBoxBound.top) / (this.modelRender.zoom / 10)}px; left: ${(Math.min(this.dragPoint.x, clientX) - tableBoxBound.left + (clientX > this.dragPoint.x && clientY > this.dragPoint.y ? -3 : 2)) / (this.modelRender.zoom / 10)}px`
      dragSvg.setAttribute('width', sW)
      dragSvg.setAttribute('height', sH)
      dragPath.setAttribute('d', this.setSvgPath({x: clientX, y: clientY}, this.dragPoint, sW, sH))
    }
  }
  // 创建直角曲线 svg 轨迹
  setSvgPath (currentPot, dragPot, bW, bH) {
    const { x: currentX, y: currentY } = currentPot
    const { x: dragX, y: dragY } = dragPot
    let d = ''
    // 第三象限
    if (currentX < dragX && currentY > dragY) {
      if (bW > bH) { // 宽度要大于高度
        d = `M ${bW} 1 L ${bW - 1.5} 1 L ${bW / 2} 1 A 3 3 0 0,0 ${bW / 2 - 3} 3 L ${bW / 2 - 3} ${bH - 6} A 3 3 0 0,1 ${bW / 2 - 6} ${bH - 3} L 0.5 ${bH - 3} L 0 ${bH - 3}`
      } else {
        d = `M ${bW - 2} 1 L ${bW - 2} 1.5 L ${bW - 2} ${bH / 2} A 3 3 0 0,1 ${bW - 5} ${bH / 2 + 3} L 3 ${bH / 2 + 3} A 3 3 0 0,0 1 ${bH / 2 + 6} L 1 ${bH - 0.5} L 1 ${bH}`
      }
    // 第四象限
    } else if (currentX > dragX && currentY > dragY) {
      d = `M 1 0.5 L 1 2.5 L 1 ${bH / 2} A 3 3 0 0,0 3 ${bH / 2 + 3} L ${bW - 3} ${bH / 2 + 3} A 3 3 0 0,1 ${bW - 1} ${bH / 2 + 6} L ${bW - 1} ${bH - 0.5} L ${bW - 1} ${bH}`
    // 第二象限
    } else if (currentX < dragX && currentY < dragY) {
      d = `M ${bW} ${bH - 1} L ${bW} ${bH - 1} L ${bW / 2} ${bH - 1} A 3 3 0 0,1 ${bW / 2 - 4} ${bH - 3} L ${bW / 2 - 3} 3 A 3 3 0 0,0 ${bW / 2 - 6} 1 L 0.5 1 L 0 1`
    // 第一象限
    } else if (currentX > dragX && currentY < dragY) {
      d = `M 0 ${bH} L 0 ${bH - 1} L ${bW / 2} ${bH - 1} A 3 3 0 0,0 ${bW / 2 + 3} ${bH - 4} L ${bW / 2 + 3} 3 A 3 3 0 0,1 ${bW / 2 + 6} 1 L ${bW - 0.5} 1 L ${bW} 1`
    }
    return d
  }
  // 当表不在可视区域内隐藏自定义连接 label
  hideLinkLabel (connect, table) {
    if (connect.length === 0) return
    connect.forEach(item => {
      const { _jsPlumb } = item
      Object.values(_jsPlumb.overlays).forEach(label => {
        if (table.drawSize.isOutOfView) {
          label.canvas.className += ' is-hide'
        } else {
          label.canvas.className = label.canvas.className.replace(/is-hide/, '')
        }
      })
    })
  }
  validateName (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('requiredName')))
    } else {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    }
  }
  async showFistAddModelGuide () {
    await this.callGuideModal({ isShowDimAndMeasGuide: true })
    localStorage.setItem('isFirstAddModel', 'false')
  }
  async showUpdateGuide () {
    this.showModelGuide = true
    localStorage.setItem('isFirstUpdateModel', 'false')
  }
  initAllPanels () {
    if (!this.isSchemaBrokenModel) {
      this.panelAppear.dimension.display = true
      this.panelAppear.measure.display = true
      this.panelAppear.datasource.display = true
    }
  }
  hiddenAllPanels () {
    for (let i in this.panelAppear) {
      this.panelAppear[i].display = false
    }
  }
  hiddenAllPanelIconsInBroken () {
    for (let i in this.panelAppear) {
      this.panelAppear[i].icon_display = false
    }
    this.panelAppear.brokenFocus.icon_display = true
  }
  // 定位含有broken连线的table
  focusBrokenLinkedTable () {
    if (this.modelInstance) {
      let tables = this.modelInstance.getBrokenLinkedTable()
      if (tables) {
        this.modelInstance.setLinkInView(tables[0], tables[1])
        let ptable = this.modelInstance.getTableByGuid(tables[0])
        let ftable = this.modelInstance.getTableByGuid(tables[1])
        this.callJoinDialog({
          pid: ptable.guid,
          fid: ftable.guid,
          primaryTable: ptable,
          tables: this.modelRender.tables
        })
      } else {
        this.$message({
          message: this.$t('noBrokenLink'),
          type: 'warning'
        })
      }
    }
  }
  // 维度、度量滚动到底部
  boardScrollBottom (name) {
    switch (name) {
      case 'dimension':
        const { dimension } = this.boardPager
        const dimensionTotalSize = this.modelRender.dimensions.length
        if (dimension.pageOffset <= Math.ceil(dimensionTotalSize / dimension.pageSize)) {
          this.$set(dimension, 'pageOffset', dimension.pageOffset + 1)
        }
      case 'measure':
        const { measure } = this.boardPager
        const measureTotalSize = this.modelRender.all_measures.length
        if (measure.pageOffset <= Math.ceil(measureTotalSize / measure.pageSize)) {
          this.$set(measure, 'pageOffset', measure.pageOffset + 1)
        }
    }
  }
  toggleCheckbox () {
    if (this.allDimension.length === 0 && !this.isShowCheckbox) {
      return
    } else if (this.isShowCheckbox) {
      this.dimensionSelectedList = []
    }
    this.isShowCheckbox = !this.isShowCheckbox
  }
  toggleMeaCheckbox () {
    if (this.modelRender.all_measures.length === 1 && !this.isShowMeaCheckbox) {
      return
    } else if (this.isShowMeaCheckbox) {
      this.measureSelectedList = []
    }
    this.isShowMeaCheckbox = !this.isShowMeaCheckbox
  }
  toggleCCCheckbox () {
    if (this.modelRender.computed_columns.length === 0 && !this.isShowCCCheckbox) {
      return
    } else if (this.isShowCCCheckbox) {
      this.ccSelectedList = []
    }
    this.isShowCCCheckbox = !this.isShowCCCheckbox
  }
  async delTable (table) {
    this.blurTableActionDropdown()
    if (!this.modelInstance.checkTableCanDel(table.guid)) {
      await this.$msgbox({
        title: this.$t('kylinLang.common.delete'),
        message: this.$t('delTableTip'),
        type: 'warning',
        showCancelButton: true,
        confirmButtonText: this.$t('kylinLang.common.delete'),
        closeOnClickModal: false
      })
    }
    this.modelInstance.delTable(table.guid).then(() => {
      if (this.modelData.available_indexes_count > 0 && !this.isIgnore) {
        this.showChangeTips()
      }
    })
  }
  saveNewAlias (t, value) {
    this.modelInstance.setUniqueAlias(t, value)
    this.modelInstance.changeAlias()
  }
  // 快捷编辑table操作 end
  // 切换悬浮菜单
  toggleMenu (i) {
    this.panelAppear[i].display = !this.panelAppear[i].display
    if (this.panelAppear[i].display) {
      this.activePanel(i)
    }
    if (i === 'search') {
      this.modelSearchActionSuccessTip = ''
      this.modelGlobalSearch = ''
    }
  }
  activePanel (i) {
    var curPanel = this.panelAppear[i]
    this.modelInstance.setIndexTop(Object.values(this.panelAppear), curPanel, '')
  }
  activeTablePanel (e, t) {
    e.stopPropagation()
    this.modelInstance.setIndexTop(Object.values(this.modelRender.tables), t, 'drawSize')
    // this.$refs[`table_${t.guid}`]
    this.removeTableFocus()
    if (this.$refs[`table_${t.guid}`]) {
      this.$refs[`table_${t.guid}`][0].className += ' is-focus'
    }
  }
  // 取消 table focus 状态
  removeTableFocus () {
    const tableBoxes = this.$el.querySelectorAll('.table-box')
    const linkLabels = this.$el.querySelectorAll('.link-label')
    const svgLine = this.$el.querySelectorAll('.jtk-connector.is-focus')
    if (tableBoxes.length > 0) {
      Array.prototype.slice.call(tableBoxes).forEach(item => {
        item.classList.remove('is-focus')
        item.classList.remove('is-broken')
      })
    }
    if (linkLabels.length > 0) {
      Array.prototype.slice.call(linkLabels).forEach(item => {
        item.classList.remove('is-focus')
        item.classList.remove('is-broken')
      })
    }
    if (svgLine.length > 0) {
      Array.prototype.slice.call(svgLine).forEach(item => {
        item.setAttribute('class', `${item.className.baseVal.replace(/is-focus|is-broken/g, '')}`)
      })
    }
    this.blurTableActionDropdown()
    document.activeElement && document.activeElement.blur()
  }
  // 隐藏下拉框
  blurTableActionDropdown () {
    if (this.$refs.tableActionsDropdown) {
      this.$refs.tableActionsDropdown.forEach(it => {
        it.visible && it.hide()
      })
    }
  }
  closeAddMeasureDia ({isSubmit, data, isEdit, fromSearch}) {
    if (isSubmit) {
      if (fromSearch) {
        this.modelSearchActionSuccessTip = this.$t('searchActionSaveSuccess', {saveObj: this.$t('measure')})
        this._collectSearchActionRecords(data, isEdit ? 'editmeasure' : 'addmeasure')
      }
    }
    this.measureVisible = false
  }
  changeTableType (t) {
    if (this.debounceTimer.tableType) {
      clearTimeout(this.debounceTimer.tableType)
    }
    this.debounceTimer.tableType = setTimeout(() => {
      if (t.kind === 'FACT' && t.source_type === 1) {
        return
      }
      if (this._checkTableType(t)) {
        let joinT = Object.keys(this.modelInstance.linkUsedColumns).filter(it => it.indexOf(t.guid) === 0)
        if (joinT.length && joinT.some(it => this.modelInstance.linkUsedColumns[it].length)) {
          this.$message({
            message: this.$t('changeTableJoinCondition'),
            type: 'warning'
          })
          return
        }
        this.modelInstance.changeTableType(t)
        if (this.modelData.available_indexes_count > 0 && !this.isIgnore) {
          this.showChangeTips()
        }
        this.blurTableActionDropdown()
      }
    }, 300)
  }
  _checkTableType (t) {
    if (t.fact) {
      // 提示 增量构建的不能改成lookup
      return false
    }
    return true
  }
  // 放大视图
  addZoom (e) {
    this.modelInstance.addZoom()
  }
  // 缩小视图
  reduceZoom (e) {
    this.modelInstance.reduceZoom()
  }
  // 全屏
  fullScreen () {
    this.toggleFullScreen(!this.isFullScreen)
  }
  // 自动布局
  autoLayout () {
    this.modelInstance.renderPosition()
  }
  // 额外功能
  handleActionsCommand (command, showOnlyConnectedColumn) {
    this.showOnlyConnectedColumn = showOnlyConnectedColumn
    if (command === 'collapseAllTables') {
      const tableTitleHeight = document.querySelector('.table-title').offsetHeight
      for (let item in this.modelRender.tables) {
        this.$set(this.modelRender.tables[item], 'spreadOut', false)
        this.$set(this.modelRender.tables[item], 'spreadHeight', this.modelRender.tables[item].drawSize.height)
        this.$set(this.modelRender.tables[item].drawSize, 'height', tableTitleHeight + 4)
      }
    } else if (command === 'expandAllTables') {
      for (let item in this.modelRender.tables) {
        if (this.modelRender.tables[item].spreadHeight < 140) {
          this.$set(this.modelRender.tables[item], 'spreadOut', true)
          this.$set(this.modelRender.tables[item].drawSize, 'height', 140)
          this.$set(this.modelRender.tables[item], 'spreadHeight', 140)
        } else {
          this.$set(this.modelRender.tables[item], 'spreadOut', true)
          this.$set(this.modelRender.tables[item].drawSize, 'height', this.modelRender.tables[item].spreadHeight)
        }
      }
    } else if (command === 'showOnlyConnectedColumn') {
      for (let item in this.modelRender.tables) {
        const { columns } = this.modelRender.tables[item]
        const len = columns.filter(it => it.isFK || it.isPK).length
        const columnHeight = document.querySelector('.column-li').offsetHeight
        const tableTitleHeight = document.querySelector('.table-title').offsetHeight
        const sumHeight = columnHeight * len
        if (sumHeight !== 0) {
          this.$set(this.modelRender.tables[item], 'spreadOut', true)
          this.$set(this.modelRender.tables[item].drawSize, 'height', tableTitleHeight + sumHeight + 4)
        } else {
          this.$set(this.modelRender.tables[item], 'spreadOut', false)
          this.$set(this.modelRender.tables[item].drawSize, 'height', tableTitleHeight + 4)
        }
      }
    }
    this.$nextTick(() => {
      this.modelInstance.plumbTool.refreshPlumbInstance()
    })
  }
  async initModelDesc (cb) {
    if (this.extraoption.modelName && this.extraoption.action === 'edit') {
      this.getModelByModelName({model_name: this.extraoption.modelName, project: this.extraoption.project}).then((response) => {
        handleSuccess(response, (data) => {
          if (data && data.value && data.value.length) {
            this.modelData = data.value[0]
            this.modelData.project = this.currentSelectedProject
            this.modelData.anti_flatten_lookups = []
            this.resetOtherColumns()
            cb(this.modelData)
          } else {
            kylinMessage(this.$t('modelDataNullTip'), {type: 'warning'})
          }
          this.globalLoading.hide()
        })
      }, () => {
        this.globalLoading.hide()
      })
    } else if (this.extraoption.action === 'add') {
      this.modelData = {
        name: this.extraoption.modelName,
        description: this.extraoption.modelDesc,
        project: this.currentSelectedProject,
        anti_flatten_lookups: [],
        simplified_measures: [{
          expression: 'COUNT',
          name: 'COUNT_ALL',
          parameter_value: [{type: 'constant', value: 1, table_guid: null}],
          return_type: ''
        }]
      }
      cb(this.modelData)
      this.globalLoading.hide()
    }
  }
  batchSetDimension () {
    this.showDimensionDialog({
      modelDesc: this.modelRender,
      modelInstance: this.modelInstance
    })
  }
  addCCDimension () {
    this.allColumns = this.modelInstance.getTableColumns()
    this.showSingleDimensionDialog({
      modelInstance: this.modelInstance
    })
  }
  addNewMeasure () {
    this.measureVisible = true
    this.isEditMeasure = false
    this.measureObj = {
      name: '',
      expression: 'SUM(column)',
      parameterValue: {type: 'column', value: '', table_guid: null},
      convertedColumns: [],
      return_type: '',
      comment: ''
    }
  }
  batchSetMeasure () {
    this.showBatchMeasureDialog({
      modelDesc: this.modelRender
    })
  }
  editDimension (dimension, i) {
    dimension._id = i
    this.showSingleDimensionDialog({
      dimension: objectClone(dimension),
      modelInstance: this.modelInstance
    })
  }
  deleteDimenison (d) {
    if ((this.modelInstance.second_storage_enabled || this.isHybridModel) && this.modelInstance.partition_desc && this.modelInstance.partition_desc.partition_date_column === d.column) {
      return
    }
    this.modelInstance.delDimension(d.name)
  }
  toggleCheckAllDimension () {
    if (this.dimensionSelectedList.length === this.modelRender.dimensions.length || (this.modelInstance.second_storage_enabled || this.isHybridModel) && this.dimensionSelectedList.length + 1 === this.modelRender.dimensions.length) {
      this.dimensionSelectedList = []
    } else {
      this.dimensionSelectedList = this.modelRender.dimensions.filter(d => {
        return !((this.modelInstance.second_storage_enabled || this.isHybridModel) && this.modelInstance.partition_desc && this.modelInstance.partition_desc.partition_date_column === d.column)
      }).map((item, i) => {
        return item.name
      })
    }
  }
  // 批量删除
  deleteDimenisons () {
    this.dimensionSelectedList && this.dimensionSelectedList.forEach((name) => {
      this.modelInstance.delDimension(name)
    })
    this.dimensionSelectedList = []
    if (this.allDimension.length === 0) {
      this.toggleCheckbox()
    }
  }
  deleteMeasure (name) {
    this.modelInstance.delMeasure(name)
  }
  toggleCheckAllMeasure () {
    // 过滤出非count_all 的度量，未保存前，是没有count_all 这个度量的，这时候，选中的和给出的列表len是相同的，保存后编辑再进时，会多一个count_all 这时候len是减一后对比
    let temp = this.modelRender.all_measures.filter((item, i) => {
      return item.name !== 'COUNT_ALL'
    })
    if (this.measureSelectedList.length === temp.length) {
      this.measureSelectedList = []
    } else {
      // 全选时，count_all的，不勾
      this.measureSelectedList = temp.map((item, i) => {
        return item.name
      })
    }
  }
  deleteMeasures () {
    this.measureSelectedList && this.measureSelectedList.forEach((name) => {
      this.modelInstance.delMeasure(name)
    })
    this.measureSelectedList = []
    if (this.modelRender.all_measures.length === 1) {
      this.toggleMeaCheckbox()
    }
  }
  addCC () {
    this.showAddCCDialog({
      modelInstance: this.modelInstance
    })
  }
  // 单个删除CC
  delCC (name) {
    this.modelInstance.delCC(name)
  }
  editCC (cc) {
    this.showAddCCDialog({
      modelInstance: this.modelInstance,
      ccForm: cc,
      editCC: true
    })
  }
  toggleCheckAllCC () {
    if (this.ccSelectedList.length === this.modelRender.computed_columns.length) {
      this.ccSelectedList = []
    } else {
      this.ccSelectedList = this.modelRender.computed_columns.map((item) => {
        return item.columnName
      })
    }
  }
  // 批量删除CC
  delCCs () {
    this.ccSelectedList && this.ccSelectedList.forEach((i) => {
      this.delCC(i)
    })
    this.ccSelectedList = []
    if (this.modelRender.computed_columns.length === 0) {
      this.toggleCCCheckbox()
    }
  }
  editMeasure (m) {
    this.$nextTick(() => {
      this.measureObj = m
    })
    this.measureVisible = true
    this.isEditMeasure = true
  }
  cancelEditAlias () {
    this.showEditAliasForm = false
  }
  openEditAliasForm (table) {
    this.blurTableActionDropdown()
    this.$prompt(null, this.$t('rename'), {
      type: 'info',
      inputValue: table.alias,
      inputPattern: /^\w+$/,
      inputErrorMessage: this.$t('kylinLang.common.nameFormatValidTip'),
      inputPlaceholder: this.$t('kylinLang.common.pleaseInput'),
      confirmButtonText: this.$t('kylinLang.common.save'),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      closeOnClickModal: false
    }).then(({ value }) => {
      this.saveNewAlias(table, value)
    })
  }
  filterColumns (filterVal, columns, t) {
    let reg = new RegExp(filterVal, 'gi')
    columns.forEach((col) => {
      this.$set(col, 'isHidden', filterVal ? !reg.test(col.name) : false)
    })
  }
  getFilteredColumns (columns) {
    return filterObjectArray(columns, 'isHidden', false)
  }
  // 拖动画布
  dragBox (x, y, boxW, boxH, info, oDiv) {
    this.modelInstance.getSysInfo()
    this.modelInstance.moveModelPosition(x, y)
  }
  // 拖动tree-table
  dragTable (node) {
    this.currentDragTable = node.database + '.' + node.label
  }
  // 拖动列
  dragColumns (event, col, table, customDragImage) {
    event.stopPropagation()
    this.currentDragColumn = customDragImage || (event.srcElement ? event.srcElement : event.target)
    event.dataTransfer && (event.dataTransfer.effectAllowed = 'move')
    if (!isIE()) {
      event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('text', '')
    }
    if (event.dataTransfer.setDragImage) {
      customDragImage ? event.dataTransfer.setDragImage(this.currentDragColumn, 15, 27) : event.dataTransfer.setDragImage(this.currentDragColumn, 0, 0)
    }
    this.currentDragColumnData = {
      guid: table.guid,
      columnName: col.name,
      btype: col.btype
    }
    return true
  }
  dragColumnEnter (event, t, col) {
    event.preventDefault()
    if (t.guid === this.currentDragColumnData.guid) {
      return
    }
    var target = event.currentTarget
    $(target).addClass('drag-column-li-in')
  }
  dragColumnLeave (event) {
    var target = event.currentTarget
    $(target).removeClass('drag-column-li-in')
  }
  // 释放table
  dropTable (e) {
    e.preventDefault && e.preventDefault()
    e.stopPropagation && e.stopPropagation()
    var target = e.srcElement ? e.srcElement : e.target
    if (!this.currentDragTable) {
      return
    }
    if (this.modelData.available_indexes_count > 0 && !this.isIgnore) {
      this.showChangeTips()
    }
    // 优化缩放时候的table位置算法 （鼠标相对文档位置 - drawbox相对于文档位置）* (10-缩放的倍数）保证缩放的时候拖入table位置精准
    let drawBoxDom = document.querySelector(modelRenderConfig.drawBox)
    let domPos = drawBoxDom && drawBoxDom.getBoundingClientRect() || {}
    if (target.className.indexOf(modelRenderConfig.drawBox.substring(1)) >= 0) {
      this.modelInstance.addTable({
        table: this.currentDragTable,
        alias: this.currentDragTable.split('.')[1],
        isSecStorageEnabled: this.modelInstance.second_storage_enabled,
        drawSize: {
          left: (e.clientX - domPos.left) * (10 / this.modelRender.zoom),
          top: (e.clientY - domPos.top) * (10 / this.modelRender.zoom)
        }
      })
    }
    // 拖拽首张表时，如果其它模型用了该表做为了事实表则默认为事实表，否者默认展开编辑菜单
    const databaseArray = this.$refs.datasourceTree ? this.$refs.datasourceTree.databaseArray : []
    const [database, table] = this.currentDragTable.split('.')
    const tables = databaseArray.filter(it => it.label === database).length ? databaseArray.filter(it => it.label === database)[0].children : []
    let tableIsFact = tables.filter(item => item.label === table && item.__data.root_fact).length > 0
    const currentTablesIncludeFact = Object.values(this.modelInstance.tables).filter(it => it.kind === 'FACT')
    if (!currentTablesIncludeFact.length && tableIsFact) {
      this.modelInstance.changeTableType(this.modelInstance.getTableByGuid(Object.keys(this.modelInstance.tables).pop()))
    }
    this.currentDragTableData = {}
    this.currentDragTable = null
    this.removeDragInClass()
    this.$nextTick(() => {
      if (Object.keys(this.modelInstance.tables).length === 1 && !(!currentTablesIncludeFact.length && tableIsFact)) {
        const guid = Object.keys(this.modelInstance.tables)[0]
        this.$refs[`table_${guid}`]?.[0].querySelector('.table-dropdown .setting-icon')?.click()
      }
    })
  }
  showChangeTips () {
    this.$message({
      message: this.$t('modelChangeTips'),
      type: 'warning',
      showClose: true,
      duration: 10000,
      closeOtherMessages: true
    })
    this.isIgnore = true
  }
  // 释放列
  dropColumn (event, col, table) {
    // 火狐默认行为会打开drop对应列的url，所以需要阻止默认行为
    event.preventDefault && event.preventDefault()
    this.removeDragInClass()
    this.removeColumnDragPots()
    let fromTable = this.modelInstance.getTableByGuid(this.currentDragColumnData.guid)
    // 判断是否是自己连自己
    if (this.currentDragColumnData.guid === table.guid) {
      return
    }
    // scd2 join关系从事实表开始连接
    if (table.kind === 'FACT') {
      this.$message({
        message: this.$t('noStarOrSnowflakeSchema'),
        dangerouslyUseHTMLString: true,
        type: 'warning'
      })
      return
    }
    // 判断两个表是否已经在一个方向上连接过(scd2 有连接阻止返方向连接)
    if (fromTable.getJoinInfoByFGuid(table.guid)) {
      this.$message({
        message: this.$t('tableHasOppositeLinks'),
        type: 'warning'
      })
      return
      // this.modelInstance.changeLinkDirect(this.currentDragColumnData.guid, null, table.guid)
    }
    if (this.modelInstance.checkLinkCircle(this.currentDragColumnData.guid, table.guid)) {
      this.$message({
        message: this.$t('noStarOrSnowflakeSchema'),
        dangerouslyUseHTMLString: true,
        type: 'warning'
      })
      return
    }
    // 一张维度表不能被指向两次
    if (table.joinInfo) {
      let isPTable = false
      for (let l in table.joinInfo) {
        if (table.joinInfo[l].table.guid === table.guid) {
          isPTable = true
          this.$message({
            message: this.$t('noStarOrSnowflakeSchema'),
            dangerouslyUseHTMLString: true,
            type: 'warning'
          })
          break
        }
      }
      if (isPTable) {
        return
      }
    }
    if (this.currentDragColumnData.guid) {
      this.currentDropColumnData = {
        guid: table.guid,
        columnName: col && col.name || ''
      }
    }
    let fTable = this.modelRender.tables[this.currentDragColumnData.guid]
    let joinInfo = table.joinInfo[`${this.currentDragColumnData.guid}$${table.guid}`]
    let joinDialogOption = {
      fid: this.currentDragColumnData.guid,
      pid: table.guid,
      fColumnName: fTable.alias + '.' + this.currentDragColumnData.columnName,
      selectTableRelation: joinInfo && joinInfo.join_relation_type,
      tables: this.modelRender.tables
    }
    if (col) {
      joinDialogOption.pColumnName = table.alias + '.' + col.name
    }
    this.callJoinDialog(joinDialogOption)
  }
  // 释放列
  dropColumnToPanel (event, type) {
    // 火狐默认行为会打开drop对应列的url，所以需要阻止默认行为
    event.preventDefault && event.preventDefault()
    this.removeDragInClass()
    let guid = this.currentDragColumnData.guid
    let table = this.modelInstance.getTableByGuid(guid)
    if (!table) {
      return
    }
    let alias = table.alias
    let fullName = alias + '.' + this.currentDragColumnData.columnName
    // 当维度表关联关系没有被预计算时无法作为维度、度量或CC
    if (this.modelRender.anti_flatten_lookups.includes(alias)) {
      this.$message({
        message: this.$t('flattenLookupTableTips'),
        type: 'warning',
        showClose: true,
        closeOtherMessages: true,
        duration: 10000
      })
      return
    }
    if (type === 'dimension') {
      this.showSingleDimensionDialog({
        dimension: {
          column: fullName,
          name: this.currentDragColumnData.columnName
        },
        modelInstance: this.modelInstance
      })
    } else if (type === 'measure') {
      this.measureObj = {
        name: this.currentDragColumnData.columnName, // 默认列名
        expression: 'SUM(column)',
        parameterValue: {type: 'column', value: fullName, table_guid: null},
        convertedColumns: [],
        return_type: ''
      }
      this.measureVisible = true
      this.isEditMeasure = false
    }
  }
  callJoinDialog (data) {
    data.modelInstance = this.modelInstance
    return new Promise((resolve, reject) => {
      // 弹出框弹出
      this.showJoinDialog(data).then(({isSubmit, data, isChange}) => {
        // 保存的回调
        if (isSubmit) {
          resolve(data)
          this.saveLinkData(data)
          if (isChange && this.exchangeJoinTableList.filter(item => item.guid === data.selectF).length === 0) {
            this.exchangeJoinTableList.push({guid: data.selectF, joinType: data.joinType, isNew: data.isNew ?? false})
          } else if (isChange && this.exchangeJoinTableList.filter(item => item.guid === data.selectF).length > 0) {
            const idx = this.exchangeJoinTableList.findIndex(it => it.guid === data.selectF)
            this.exchangeJoinTableList[idx] = {...this.exchangeJoinTableList[idx], ...{joinType: data.joinType, isNew: data.isNew ?? false}}
          } else if (!isChange) {
            const index = this.exchangeJoinTableList.findIndex(it => it.guid === data.selectF)
            index >= 0 && this.exchangeJoinTableList.splice(index, 1)
          }
          !this.exchangeJoinTableList.length && (this.isIgnore = false)
          if (this.modelData.available_indexes_count > 0 && this.exchangeJoinTableList.length > 0) {
            this.showChangeTips()
          }
        }
      })
    })
  }
  saveLinkData (data) {
    var pGuid = data.selectP
    var fGuid = data.selectF
    var joinData = data.joinData
    var selectTableRelation = data.selectTableRelation
    var joinType = data.joinType
    // 2020/06/24 add 过滤出条件是等于的
    var fcols = joinData.foreign_key
    var pcols = joinData.primary_key
    var fTable = this.modelInstance.tables[fGuid]
    var pTable = this.modelInstance.tables[pGuid]
    // 2020/06/24 add 多了 scd2 模式后，joinData 中的字段格式需要重新拼接
    // 给table添加连接数据
    pTable.addLinkData(fTable, fcols, pcols, joinType, joinData.op, data.isPrecompute, selectTableRelation)
    this.currentDragColumnData = {}
    this.currentDropColumnData = {}
    this.currentDragColumn = null
    // 渲染连线
    // 当 a 要连 b  判断 b 有没有连过 a
    // 有反向连接，合并连接数据，切换连线方向
    if (fTable.getJoinInfoByFGuid(pTable.guid)) {
      this.modelInstance.changeLinkDirect(fGuid, null, pGuid)
    } else {
    // 正常连线
      this.modelInstance.renderLink(pGuid, fGuid)
      this.$nextTick(() => {
        // const cloneTables = objectClone(this.modelRender.tables)
        Object.values(this.modelRender.tables).forEach(t => {
          const pfkLinkColumns = t.columns.filter(it => it.isPK && it.isFK).sort((a, b) => a - b)
          const pkLinkColumns = t.columns.filter(it => it.isPK && !it.isFK).sort((a, b) => a - b)
          const fkLinkColumns = t.columns.filter(it => it.isFK && !it.isPK).sort((a, b) => a - b)
          const unlinkColumns = t.columns.filter(it => !it.isPK && !it.isFK)
          t.columns = [...pfkLinkColumns, ...pkLinkColumns, ...fkLinkColumns, ...unlinkColumns]
          t._cache_search_columns = t.columns
          t.showColumns = [...pfkLinkColumns, ...pkLinkColumns, ...fkLinkColumns, ...unlinkColumns].slice(0, t.columnPerSize)
        })
        this.$set(this.modelInstance, 'tables', this.modelRender.tables)
        console.log(this.modelInstance.allConnInfo)
        const [currentConnector] = Object.values(this.modelInstance.allConnInfo).slice(-1)
      }, 500)
    }
    // 同步因为预计算被禁用的表
    this.modelRender.anti_flatten_lookups = this.modelInstance.anti_flatten_lookups = Object.values(this.modelInstance.tables).filter(it => data.anti_flatten_lookups.includes(it.name)).map(it => it.alias)
    this.modelRender.anti_flatten_cc = this.modelInstance.anti_flatten_cc = data.anti_flatten_cc
  }
  removeDragInClass () {
    $(this.$el).find('.drag-column-li-in').removeClass('drag-column-li-in')
    $(this.$el).find('.drag-column-in').removeClass('drag-column-in')
    $(this.$el).removeClass('drag-in').find('.drag-in').removeClass('drag-in')
  }
  _checkTableDropOver (className) {
    return className && className.indexOf(modelRenderConfig.drawBox.substring(1)) >= 0 || false
  }
  allowDrop (e, guid) {
    e.preventDefault()
    var target = e.srcElement ? e.srcElement : e.target
    if (this.currentDragTable && this._checkTableDropOver(target.className)) {
      $(this.$el).addClass('drag-in')
    }
  }
  allowDropColumn (e, guid) {
    e.preventDefault()
    var target = e.srcElement ? e.srcElement : e.target
    if (this.currentDragColumn) {
      if (this.currentDragColumnData.guid === guid) {
        return
      }
    }
  }
  allowDropColumnToPanle (e) {
    e.preventDefault()
    var target = e.srcElement ? e.srcElement : e.target
    if (this.currentDragColumn) {
      $(target).parents('.panel-box').find('.panel-main-content').addClass('drag-in')
    }
  }
  dragLeave (e) {
    e.preventDefault()
    this.removeDragInClass()
  }
  _collectSearchActionRecords (data, type) {
    let record = {}
    let actionData = objectClone(data)
    if (type === 'tableaddjoin' || type === 'addjoin') {
      record.icon = 'el-icon-ksd-joint_condition'
      let fTable = this.modelInstance.getTableByGuid(actionData.selectF)
      let pTable = this.modelInstance.getTableByGuid(actionData.selectP)
      record.data = objectClone(actionData.joinData)
      record.title = `${this.$t('addTableJoinCondition')} (<i>${fTable.alias} ${data.joinType} ${pTable.alias}</i>)`
    } else if (type === 'tableeditjoin' || type === 'editjoin') {
      record.icon = 'el-icon-ksd-joint_condition'
      let fTable = this.modelInstance.getTableByGuid(actionData.selectF)
      let pTable = this.modelInstance.getTableByGuid(actionData.selectP)
      record.data = objectClone(actionData.joinData)
      record.title = `${this.$t('editTableJoinCondition')} (<i>${fTable.alias} ${data.joinType} ${pTable.alias}</i>)`
    } else if (type === 'addmeasure') {
      record.icon = 'el-icon-ksd-measure'
      record.title = `${this.$t('addMeasure')} (<i>${actionData.name}</i>), expression (<i>${data.expression}</i>)`
    } else if (type === 'editmeasure') {
      record.icon = 'el-icon-ksd-measure'
      record.title = `${this.$t('editMeasure')} (<i>${actionData.name}</i>), expression (<i>${data.expression}</i>)`
    } else if (type === 'adddimension') {
      record.icon = 'el-icon-ksd-dimension'
      record.title = `${this.$t('addDimension')} (<i>${actionData.name}</i>)`
    } else if (type === 'editdimension') {
      record.icon = 'el-icon-ksd-dimension'
      record.title = `${this.$t('editDimension')} (<i>${actionData.name}</i>)`
    } else if (type === 'showtable') {
      record.icon = 'el-icon-ksd-sample'
      record.title = `${this.$t('searchTable')} (<i>${actionData.alias}</i>)`
    }
    this.modelSearchActionHistoryList.unshift(record)
    if (this.modelSearchActionHistoryList.length > 10) {
      this.modelSearchActionHistoryList.pop()
    }
  }
  searchHandleStart = false // 标识业务弹窗是不是通过搜索弹出的
  selectResult (e, select) {
    this.modelSearchActionSuccessTip = ''
    this.searchHandleStart = true
    this.modelGlobalSearch = ''
    var moreInfo = select.more
    if (select.action === 'showtable') {
      if (select.more) {
        let nTable = select.more
        this.modelInstance.setTableInView(nTable.guid)
        this._collectSearchActionRecords(nTable, select.action)
      }
    }
    if (select.action === 'tableeditjoin') {
      let pguid = moreInfo.guid
      let joinInfo = moreInfo.getJoinInfo()
      let fguid = joinInfo.foreignTable.guid
      this.callJoinDialog({
        pid: pguid,
        fid: fguid,
        selectTableRelation: joinInfo.join_relation_type,
        tables: this.modelRender.tables,
        flattenable: joinInfo.flattenable
      }).then((data) => {
        this._collectSearchActionRecords(data, select.action)
        this.modelSearchActionSuccessTip = this.$t('searchActionSaveSuccess', {saveObj: this.$t('tableJoin')})
      })
    }
    if (select.action === 'tableaddjoin') {
      let pguid = moreInfo.guid
      this.callJoinDialog({
        fid: '',
        pid: pguid,
        tables: this.modelRender.tables
      }).then((data) => {
        this._collectSearchActionRecords(data, select.action)
        this.modelSearchActionSuccessTip = this.$t('searchActionSaveSuccess', {saveObj: this.$t('tableJoin')})
      })
    }
    if (select.action === 'adddimension') {
      if (this.modelRender.anti_flatten_lookups.includes(select.more.table_alias)) {
        this.$message({
          message: this.$t('flattenLookupTableTips'),
          type: 'warning',
          showClose: true,
          closeOtherMessages: true,
          duration: 10000
        })
        return
      }
      this.showSingleDimensionDialog({
        dimension: {
          column: moreInfo.full_colname
        },
        modelInstance: this.modelInstance
      }).then((res) => {
        if (res && res.isSubmit) {
          this._collectSearchActionRecords(res.data.dimension, select.action)
          this.modelSearchActionSuccessTip = this.$t('searchActionSaveSuccess', {saveObj: this.$t('dimension')})
        }
      })
    }
    if (select.action === 'editdimension') {
      this.showSingleDimensionDialog({
        dimension: moreInfo,
        modelInstance: this.modelInstance
      }).then((res) => {
        if (res && res.isSubmit) {
          this._collectSearchActionRecords(res.data.dimension, select.action)
          this.modelSearchActionSuccessTip = this.$t('searchActionSaveSuccess', {saveObj: this.$t('dimension')})
        }
      })
    }
    if (select.action === 'editjoin') {
      let pguid = moreInfo.guid
      let joinInfo = moreInfo.getJoinInfo()
      let fguid = joinInfo.foreignTable.guid
      this.callJoinDialog({
        pid: pguid,
        fid: fguid,
        selectTableRelation: joinInfo.join_relation_type,
        tables: this.modelRender.tables
      }).then((data) => {
        this._collectSearchActionRecords(data, select.action)
        this.modelSearchActionSuccessTip = this.$t('searchActionSaveSuccess', {saveObj: this.$t('tableJoin')})
      })
    }
    if (select.action === 'addmeasure') {
      if (this.modelRender.anti_flatten_lookups.includes(select.more.table_alias)) {
        this.$message({
          message: this.$t('flattenLookupTableTips'),
          type: 'warning',
          showClose: true,
          closeOtherMessages: true,
          duration: 10000
        })
        return
      }
      this.measureObj = {
        name: '',
        expression: 'SUM(column)',
        parameterValue: {type: 'column', value: moreInfo.full_colname, table_guid: null},
        convertedColumns: [],
        return_type: '',
        fromSearch: true
      }
      this.measureVisible = true
      this.isEditMeasure = false
    }
    if (select.action === 'editmeasure') {
      this.measureObj = moreInfo
      moreInfo.fromSearch = true
      this.measureVisible = true
      this.isEditMeasure = true
    }
    if (select.action === 'addjoin') {
      let pguid = moreInfo.table_guid
      this.callJoinDialog({
        pid: pguid,
        fid: '',
        tables: this.modelRender.tables,
        pColumnName: moreInfo.name
      }).then((data) => {
        this._collectSearchActionRecords(data, select.action)
        this.modelSearchActionSuccessTip = this.$t('searchActionSaveSuccess', {saveObj: this.$t('tableJoin')})
      })
    }
    this.panelAppear.search.display = false
  }
  searchModelEverything (val) {
    this.modelGlobalSearchResult = this.modelInstance.search(val)
    console.log(this.modelSearchActionHistoryList, 2222)
  }
  getColumnType (tableName, column) {
    var ntable = this.modelInstance.getTable('alias', tableName)
    return ntable && ntable.getColumnType(column)
  }
  panelStyle (k) {
    // return (k) => {
    if (['dimension', 'measure'].includes(k)) {
      this.changePageOfBround(k)
    }
    var styleObj = {'z-index': this.panelAppear[k].zIndex, width: this.panelAppear[k].width + 'px', height: this.panelAppear[k].height + 'px', right: this.panelAppear[k].right + 'px', top: this.panelAppear[k].top + 'px'}
    if (this.panelAppear[k].left) {
      styleObj.left = this.panelAppear[k].left + 'px'
    }
    if (this.panelAppear[k].right) {
      styleObj.right = this.panelAppear[k].right + 'px'
    }
    return styleObj
    // }
  }
  tableBoxStyle (drawSize) {
    return {'z-index': drawSize.zIndex, width: drawSize.width + 'px', height: drawSize.height + 'px', left: drawSize.left + 'px', top: drawSize.top + 'px'}
  }
  // 判断是否添加分区列方法
  addPartitionFunc (data) {
    data.available_indexes_count = this.modelData.available_indexes_count
    if (this.modelRender.management_type !== 'TABLE_ORIENTED') {
      this.showSaveConfigDialog({
        modelDesc: data,
        modelInstance: this.modelInstance,
        mode: 'saveModel',
        allDimension: this.modelRender.dimensions || [],
        isChangeModelLayout: this.isIgnore, // isIgnore为true说明至少改动过一次模型join关系等重大变化
        exchangeJoinTableList: this.exchangeJoinTableList // 变更的 join table
      }).then((res) => {
        if (data.uuid && res.isPurgeSegment) {
          this.isPurgeSegment = res.isPurgeSegment // 修改分区列会清空所有segment，该字段引导用户去添加segment
        }
        if (res.isSubmit) {
          this.handleSaveModel({data, modelSaveConfigData: res.data, createBaseIndex: this.modelInstance.has_base_table_index && this.modelInstance.has_base_agg_index ? false : res.with_base_index})
        } else {
          this.saveBtnLoading = false
        }
      })
    } else {
      this.handleSaveModel({data})
    }
  }
  // 解析校验保存模型数据
  generateModelData (ignoreAloneTableCheck) {
    this.modelInstance.generateMetadata(ignoreAloneTableCheck).then(async (data) => {
      if (!(data.simplified_dimensions && data.simplified_dimensions.length)) {
        this._tipNoDimension(data).then(async () => {
          await this.checkMeasureWithCC(data)
          this.addPartitionFunc(data)
        }).catch(() => {
          this.saveBtnLoading = false
        })
      } else {
        await this.checkMeasureWithCC(data)
        this.addPartitionFunc(data)
      }
    }, (err, t) => {
      if (err.errorKey === 'hasAloneTable') {
        this._tipHasAloneTable(err.aloneCount).then(() => {
          this.generateModelData(true)
        }).catch(() => {
          this.saveBtnLoading = false
        })
      } else {
        kylinMessage(this.$t(modelErrorMsg[err.errorKey], {tableName: err.tableName}), {type: 'warning'})
      }
      this.saveBtnLoading = false
    }).catch(() => {
      this.saveBtnLoading = false
    })
  }
  // 对于老数据检测 SUM 或 PERCENTILE_APPROX 度量中是否使用 varchar cc 列
  checkMeasureWithCC (data) {
    return new Promise((resolve, reject) => {
      let list = data.simplified_measures.filter(it => (it.expression === 'SUM' || it.expression === 'PERCENTILE_APPROX') && it.return_type && it.return_type.indexOf('varchar') > -1)
      if (list.length) {
        return this.$msgbox({
          title: this.$t('kylinLang.common.tip'),
          closeOnPressEscape: false,
          closeOnClickModal: false,
          showClose: false,
          confirmButtonText: this.$t('iKnow'),
          message: <div>
            <p class='ksd-mb-5'><i class='error-font el-icon-ksd-error_01 ksd-mr-5'></i>{this.$t('varcharSumMeasureTip')}</p>
            <ul>{
              list.map(item => <li>• {item.name}</li>)
            }</ul>
            <p class='ksd-mt-5'>{this.$t('pleaseModify')}</p>
          </div>,
          callback: () => {
            reject()
          }
        })
      } else {
        resolve()
      }
    })
  }
  // 保存模型
  saveModelEvent () {
    this.saveBtnLoading = true
    this.generateModelData()
  }
  // 返回上级页面
  goModelList () {
    this.toggleFullScreen(false)
    if (this.fromRoute && this.fromRoute.name) {
      this.$router.push({name: this.fromRoute.name, params: {modelName: this.currentModel}})
      return
    }
    this.$router.push({name: 'ModelList'})
  }
  async initEditModel () {
    this.globalLoading.show()
    this.$el.onselectstart = function (e) {
      return false
    }
    // 注册保存事件
    // this.$on('saveModel', () => {
    //   this.generateModelData()
    // })
    this.clearDatasourceCache(this.currentSelectedProject) // 清空 当前project下的 datasource缓存
    // 如果是 edit 需要获取模型使用的 table 信息
    try {
      if (this.extraoption.modelName && this.extraoption.action === 'edit') {
        // 初始化project数据
        const result = await this.loadDataSourceByModel({ project: this.currentSelectedProject, model_name: this.extraoption.modelName })
        this.datasource = await handleSuccessAsync(result)
        this.datasource.forEach((item) => {
          item.table = item.database + '.' + item.name
        })
      }
    } catch (err) {
      handleError(err)
      this.globalLoading.hide()
      return false
    }
    await this.initModelDesc(async (data) => { // 初始化模型数据
      if ('visible' in this.modelData && !this.modelData.visible) {
        this.showNoAuthorityContent(this.modelData)
        return
      }
      try {
        let modelInfo = objectClone(data) // 全量模型用到的数据库表
        modelInfo.global_datasource = {}
        modelInfo.global_datasource[this.currentSelectedProject] = this.datasource
        this.modelInstance = new NModel(Object.assign(modelInfo, {
          project: this.currentSelectedProject,
          renderDom: this.renderBox
        }), this.modelRender, this)
        this.updateTableInfo()
        if (this.isSchemaBrokenModel) {
          kylinConfirm(this.$t('brokenEditTip'), {
            showCancelButton: false,
            type: 'warning'
          })
          this.hiddenAllPanels()
          this.hiddenAllPanelIconsInBroken()
        } else {
          this.initAllPanels()
        }
        this.modelInstance.bindConnClickEvent((ptable, ftable) => {
          // 设置连接弹出框数据
          this.callJoinDialog({
            pid: ptable.guid,
            fid: ftable.guid,
            primaryTable: ptable,
            selectTableRelation: ptable.joinInfo[`${ftable.guid}$${ptable.guid}`].join_relation_type,
            tables: this.modelRender.tables
          })
        })
        this.$nextTick(() => {
          this.checkInvalidIndex()
          this.exchangeModelTable()
        })
      } catch (e) {
        this.globalLoading.hide()
        kylinConfirm(this.$t('canNotRepairBrokenTip'), {
          type: 'warning',
          showCancelButton: false,
          confirmButtonText: this.$t('kylinLang.common.exit')
        }).then(() => {
          this.$router.replace({name: 'ModelList', params: { ignoreIntercept: true }})
        })
      }
    })
    if (localStorage.getItem('isFirstAddModel') === 'true') {
      await this.showFistAddModelGuide()
    }
    const keVersion = this.$store.state.system.serverAboutKap['ke.version']?.match(/Kyligence Enterprise (\d+.\d+.\d+.\d+)-\w+/)[1]
    if ((localStorage.getItem('isFirstUpdateModel') === 'true' || !localStorage.getItem('isFirstUpdateModel')) && (keVersion && +keVersion.split('.').join('') >= 45160) && this.extraoption.action === 'edit') {
      await this.showUpdateGuide()
    }
  }
  // table drawSize 数据更新
  exchangeModelTable () {
    if (!Object.values(this.modelRender.tables).length) return
    const modelTableTitle = this.$el.querySelector('.table-title')
    const modelTableBoxBorder = +window.getComputedStyle(modelTableTitle)['borderWidth'].replace(/px/, '')
    for (let item in this.modelRender.tables) {
      const { drawSize } = this.modelRender.tables[item]
      if (drawSize.height === modelTableTitle.offsetHeight + modelTableBoxBorder * 2 + 4) {
        this.modelRender.tables[item].spreadOut = false
        this.modelRender.tables[item].spreadHeight = modelRenderConfig.tableBoxHeight
      } else if (drawSize.height < 140) {
        this.$set(this.modelRender.tables[item].drawSize, 'height', 140)
        this.modelRender.tables[item].spreadHeight = 140
      } else {
        this.modelRender.tables[item].spreadHeight = this.modelRender.tables[item].drawSize.height
      }
    }
  }
  // 更新 model.js 里的 tables 数据
  updateTableInfo () {
    for (let key in this.modelInstance.tables) {
      const { name } = this.modelInstance.tables[key]
      const index = indexOfObjWithSomeKey(this.datasource, 'table', name)
      index >= 0 && (this.modelInstance.tables[key].source_type = this.datasource[index].source_type)
    }
  }
  async checkInvalidIndex () {
    if (this.extraoption.action === 'edit') {
      const res = await this.modelInstance.generateMetadata(true)
      // const _data = {
      //   project: this.currentSelectedProject,
      //   model_id: res.uuid,
      //   join_tables: res.join_tables
      // }
      const response = await this.invalidIndexes(res)
      const result = await handleSuccessAsync(response)
      const { computed_columns, anti_flatten_lookups } = result
      this.modelInstance.anti_flatten_lookups = this.modelRender.anti_flatten_lookups = Object.values(this.modelInstance.tables).filter(it => anti_flatten_lookups.includes(it.name)).map(it => it.alias)
      this.modelInstance.anti_flatten_cc = computed_columns
    }
  }
  // 展示model无权限的相关table和columns信息
  async showNoAuthorityContent (row) {
    const { unauthorized_tables, unauthorized_columns } = row
    let details = []
    if (unauthorized_tables && unauthorized_tables.length) {
      details.push({title: `Table (${unauthorized_tables.length})`, list: unauthorized_tables})
    }
    if (unauthorized_columns && unauthorized_columns.length) {
      details.push({title: `Columns (${unauthorized_columns.length})`, list: unauthorized_columns})
    }
    await this.callGlobalDetailDialog({
      theme: 'plain-mult',
      title: this.$t('kylinLang.model.authorityDetail'),
      msg: this.$t('kylinLang.model.authorityMsg', {modelName: row.name}),
      showCopyBtn: true,
      showIcon: false,
      showDetailDirect: true,
      details,
      showDetailBtn: false,
      dialogType: 'error',
      customClass: 'no-acl-model',
      showCopyTextLeftBtn: true,
      needCallbackWhenClose: true
    })
    this.$router.replace({name: 'ModelList', params: { ignoreIntercept: true }})
  }
  _tipNoDimension (data) {
    // 判断是 无dimension 和 measure 都无的情况 还是 只是没有dimension的情况
    let tipContent = this.$t('noDimensionAndMeasureTipContent')
    let tipTitle = this.$t('noDimensionAndMeasureTipTitle')
    if (data && data.simplified_measures && data.simplified_measures.length) {
      tipContent = this.$t('noDimensionTipContent')
      tipTitle = this.$t('noDimensionTipTitle')
    }
    let saveBtnWord = this.$t('noDimensionGoOnSave')
    let cancelBtnWord = this.$t('noDimensionBackEdit')
    return this.$confirm(tipContent, tipTitle, {
      confirmButtonText: saveBtnWord,
      cancelButtonText: cancelBtnWord,
      type: 'warning'
    })
  }
  // 当有脱离树的节点存在的时候的提示框
  _tipHasAloneTable (data) {
    // 判断是 无dimension 和 measure 都无的情况 还是 只是没有dimension的情况
    let tipContent = this.$t('kylinLang.model.aloneTableTip', {aloneCount: data})
    let tipTitle = this.$t('kylinLang.model.aloneTableTipTitle')
    let saveBtnWord = this.$t('kylinLang.common.save')
    let cancelBtnWord = this.$t('kylinLang.common.cancel')
    return this.$confirm(tipContent, tipTitle, {
      confirmButtonText: saveBtnWord,
      cancelButtonText: cancelBtnWord,
      type: 'warning'
    })
  }
  ignoreAddIndex () {
    this.$router.replace({name: 'ModelList', params: { ignoreIntercept: true }})
    this.gotoIndexdialogVisible = false
  }
  willAddIndex () {
    if (localStorage.getItem('isFirstAddModel') === 'false' && !localStorage.getItem('isFirstSaveModel')) {
      localStorage.setItem('isFirstSaveModel', 'true')
    }
    this.$router.replace({name: 'ModelDetails', params: { modelName: this.modelInstance.alias, tabTypes: 'second', ignoreIntercept: true }})
    this.gotoIndexdialogVisible = false
  }
  willAddSegment () {
    this.$router.replace({name: 'ModelDetails', params: { modelName: this.modelInstance.alias, tabTypes: 'first', ignoreIntercept: true }})
    this.gotoIndexdialogVisible = false
  }
  getBaseIndexCount (result) {
    const { base_agg_index, base_table_index } = result
    const createBaseIndexNum = base_agg_index ? [...[{...base_agg_index}].filter(it => it.operate_type === 'CREATE'), ...[{...base_table_index}].filter(it => it.operate_type === 'CREATE')].length : 0
    const updateBaseIndexNum = base_table_index ? [...[{...base_agg_index}].filter(it => it.operate_type === 'UPDATE'), ...[{...base_table_index}].filter(it => it.operate_type === 'UPDATE')].length : 0
    return {createBaseIndexNum, updateBaseIndexNum}
  }
  handleSaveModel ({data, modelSaveConfigData, createBaseIndex}) {
    this.saveModelType = 'saveModel'
    let para = {...data, with_base_index: createBaseIndex}
    if (data.uuid) {
      this.saveModelType = 'updataModel'
    }
    // 如果未选择partition 把partition desc 设置为null
    if (!(data && data.partition_desc && data.partition_desc.partition_date_column)) {
      para.partition_desc = null
      this.isFullLoad = true
    }
    this[this.saveModelType](para).then((res) => {
      handleSuccess(res, async (result) => {
        // TODO HA 模式时 post 等接口需要等待同步完去刷新列表
        // await handleWaiting()
        // kylinMessage(this.$t('kylinLang.common.saveSuccess'))
        // if (!(data.simplified_dimensions && data.simplified_dimensions.length)) {
        //   this.$router.replace({name: 'ModelList', params: { ignoreIntercept: true }})
        //   return
        // }
        this.saveModelResponse = result
        const { createBaseIndexNum, updateBaseIndexNum } = this.getBaseIndexCount(result)
        setTimeout(() => {
          // 有可用索引数据时，保存前二次确认是否清空segment，不再弹框引导是否打开添加索引tab
          if (typeof this.modelData.available_indexes_count === 'number' && this.modelData.available_indexes_count > 0) {
            this.ignoreAddIndex()
            // 需要根据是点击的是保存，还是保存并加载来出提示，必须放在路由跳转之后，否则message会被吞掉
            if (modelSaveConfigData) {
              if (modelSaveConfigData.save_only) {
                this.$message({
                  type: 'success',
                  duration: 10000,
                  showClose: true,
                  message: <p>{this.$t('saveSuccessTip')}<br/>{createBaseIndexNum > 0 ? this.$t('createBaseIndexTips', {createBaseNum: createBaseIndexNum}) : updateBaseIndexNum > 0 ? this.$t('updateBaseIndexTips', {updateBaseNum: updateBaseIndexNum}) : ''}{(createBaseIndexNum > 0 || updateBaseIndexNum > 0) && this.modelData.model_type !== 'STREAMING' ? <a href="javascript:void();" onClick={() => this.buildBaseIndexEvent(result)}>{this.$t('buildIndex')}</a> : ''}</p>
                })
              } else {
                this.$message({
                  dangerouslyUseHTMLString: true,
                  type: 'success',
                  duration: 10000,
                  showClose: true,
                  message: (
                    <div>
                      <span>{this.$t('kylinLang.common.buildSuccess')}</span>
                      <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
                    </div>
                  )
                })
              }
            }
          } else {
            this.gotoIndexdialogVisible = true
          }
          this.saveBtnLoading = false
        }, 1000)
      })
    }).catch((res) => {
      this.saveBtnLoading = false
      handleError(res)
    })
  }
  updateBetchMeasure (val) {
    this.modelInstance.all_measures = val
  }
  // 构建基础索引
  buildBaseIndexEvent (result) {
    const { base_agg_index, base_table_index } = result
    const layoutIds = []
    base_agg_index && layoutIds.push(base_agg_index.layout_id)
    base_table_index && layoutIds.push(base_table_index.layout_id)
    this.callConfirmSegmentModal({
      title: this.$t('buildIndex'),
      subTitle: this.$t('batchBuildSubTitle'),
      indexes: layoutIds,
      submitText: this.$t('buildIndex'),
      model: this.modelInstance
    })
  }
  // 跳转至job页面
  jumpToJobs () {
    this.$router.push('/monitor/job')
  }
  beforeDestroy () {
    this.toggleFullScreen(false)
  }
  destoryed () {
    $(document).unbind('selectstart')
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
@fact-title-color: @base-color;
@lookup-title-color: @base-color-12;
@fact-shadow: 0 0 4px 0 @base-color;
@fact-hover-shadow: 0 0 8px 0 @base-color;
@lookup-shadow:0 0 4px 0 @base-color-12;
@lookup-hover-shadow: 0 0 8px 0 @base-color-12;
@--index-normal: 1;
@broken-line-color: @color-danger;
@broken-line-lable-close-hover-color: #F178A2;
.gifEmptyData{
  &.empty-data.empty-data-normal{
    img{
      height: 140px;
      opacity: 0.8;
    }
    .center:first-child{
      margin-bottom:16px;
    }
  }
}
.drag-in {
  box-shadow: inset 0 0 4px 0 @base-color;
}
.fast-action-popper {
  z-index:100001 !important;
}
.jtk-connector.is-focus {
  path:not(#use) {
    stroke: @ke-color-primary !important;
    stroke-width: 2;
  }
}
.jtk-connector.is-broken.is-focus {
  path:not(#use) {
    stroke: @ke-color-danger !important;
    stroke-width: 2;
  }
}
.jtk-overlay {
  z-index: 9;
  cursor: pointer;
  border-radius: 10px;
  text-align: center;
  background: @ke-background-color-secondary;
  padding: 0 10px;
  &.is-hide {
    font-size: 0;
    .join-type-hide {
      display: inline-block;
      vertical-align: middle;
    }
    .join-type {
      display: none;
    }
  }
  &.is-hide.jtk-hover {
    .join-type {
      display: inline-block;
    }
    .join-type-hide {
      display: none;
    }
  }
  &.link-label-broken {
    .join-type {
      font-size: 24px;
      color: @ke-color-danger;
    }
    .join-type-hide {
      background: @ke-color-danger;
    }
    &:hover {
      .join-type {
        color: @ke-color-danger-hover;
      }
    }
  }
  &.link-label.is-focus {
    .join-type {
      color: @ke-color-primary;
    }
    .join-type-hide {
      background: @ke-color-primary;
    }
  }
  &.link-label.is-broken.is-focus {
    .join-type {
      color: @ke-color-danger;
    }
    .join-type-hide {
      background: @ke-color-danger;
    }
  }
  &.jtk-hover:not(.link-label-broken):not(.is-focus) {
    .join-type {
      color: @base-color-6;
      &:hover {
        color: @ke-color-primary;
      }
    }
    
  }
  .join-type-hide {
    display: none;
    width: 6px;
    height: 6px;
    border-radius: 100%;
    background: @text-placeholder-color;
  }
  .line-label-bar {
    &:hover {
      .close-icon {
        display: inline-block;
      }
    }
  }
  .join-type {
    color: @text-placeholder-color;
    font-size: 30px;
    &:hover {
      color: @ke-color-primary;
    }
  }
  .close-icon {
    display: none;
    position: absolute;
    top: 50%;
    transform: translate(-50%, -50%);
    right: -13px;
    font-size: 16px;
  }
}
.drag-column-li-in {
  background-color: @base-color-8;
  border: 2px solid @line-border-drag !important;
}
.box-css() {
  position:relative;
  background-color:@grey-3;
}
@-moz-document url-prefix() {
  .column-point-dot {
    .add-point {
      top: -1.5px !important;
    }
  }
}
.search-position() {
  position:relative;
}
.model-edit-layout {
  width: 100%;
  height: 100%;
  .model-edit-header {
    width: 100%;
    height: 72px;
    display: flex;
    align-items: center;
    flex-direction: row;
    border-bottom: 1px solid @ke-border-secondary;
    box-sizing: border-box;
    .model-title {
      // height: 100%;
      padding: 0 16px;
      line-height: 72px;
      box-sizing: border-box;
      max-width: 290px;
      .model-alias-title {
        max-width: 90%;
      }
      .model-alias-label {
        .filter-status {
          top: -4px;
          margin-right: 2px;
        }
        .last-modified-tooltip {
          margin-top: 0;
        }
      }
    }
    .model-search-layout {
      flex: 1;
      position: relative;
      .el-input {
        width: 100%;
        .el-input__inner {
          border: 0;
          outline: none;
          &:focus {
            border: 1px solid #0875DA;
            box-shadow: 0px 0px 0px 1px #0875DA;
          }
        }
      }
      .search-board {
        width: 100%;
        position: absolute;
        // box-shadow: 0 0px 2px 0 @color-text-placeholder;
        background-color: rgba(255, 255, 255, 1);
        box-shadow: 0px 2px 8px rgba(50, 73, 107, 0.24);
        border-radius: 6px;
        height:calc(~'100vh - 464px')!important;
        min-height: 250px;
        z-index: 101;
        box-sizing: border-box;
        display: flex;
        flex-direction: column;
        .search-footer {
          background-color: @ke-border-divider-color;
          height: 36px;
          line-height: 36px;
          text-align: right;
          padding: 0 8px;
          box-sizing: border-box;
          > span {
            font-size: 12px;
          }
          .feedback-btn {
            color: @ke-color-primary;
          }
        }
      }
      .el-row {
        width: 100%;
        height: 100%;
        overflow: hidden;
        padding: 12px 10px 0 10px;
        box-sizing: border-box;
        .search-content-col {
          padding-right: 8px;
          height: 100%;
        }
        .search-history {
          border-left: 1px solid @ke-border-divider-color;
          padding-left: 16px;
          height: 100%;
        }
        .search-action-list {
          font-size:12px;
          .action-list-title {
            height:30px;
            line-height:30px;
            font-weight: @font-medium;
            border-bottom: solid 1px @line-split-color;
          }
          .search-list-icon {
            width:16px;
            height:16px;
            position:absolute;
            .ky-square-box(16px, 16px);
            border-radius: 50%;
          }
          .action-content {
            border-bottom: dashed 1px @line-split-color;
            .action-title {
              padding:10px 0;
            }
            .action-desc {
              margin-left:26px;
              word-break: break-all;
              i {
                font-weight: @font-medium;
                font-style: normal;
              }
            }
          }
        }
      }
      .search-result-box {
        width: 100%;
        height: 100%;
        .search-noresult {
          font-size:20px;
          text-align: center;
          margin-top:100px;
          color:@text-placeholder-color;
        }
        .search-group {
          padding-top: 5px;
          padding-bottom: 5px;
        }
        .search-content {
          cursor:pointer;
          height: 32px;
          line-height: 32px;
          padding-left: 8px;
          &.active,&:hover{
            background-color:@base-color-9;
          }
          .search-category {
            font-size:12px;
            color:@text-normal-color;
          }
          .search-name {
            font-size:14px;
            color:@text-title-color;
            i {
              color:@base-color;
              font-style: normal;
            }
          }
        }
      }
    }
    .model-actions {
      height: 100%;
      // width: 300px;
      display: flex;
      align-items: center;
      justify-content: right;
      padding: 0 16px;
      box-sizing: border-box;
    }
  }
}
.model-edit-outer {
  border-top: @text-placeholder-color;
  user-select: none;
  overflow: hidden;
  .box-css();
  height: calc(~'100% - 72px');
  background-color: @ke-background-color-secondary;
  .lose-fact-table-alert {
    position: absolute;
    z-index: 1;
  }
  .slide-fade-enter-active {
    transition: all .3s;
  }
  .slide-fade-leave-active {
    transition: all 0s cubic-bezier(1.0, 0.5, 0.8, 1.0);
  }
  .slide-fade-enter, .slide-fade-leave-to{
    transform: translateX(-10px);
    opacity: 0;
  }
  .full-screen-cover {
    position: fixed;
    top:0;
    left:0;
    right:0;
    bottom:0;
    // background-color: #000;
    z-index: 99999;
    background-color: rgba(24, 32, 36, 0.7);
  }
  .shortcuts-group {
    position: absolute;
    right: 20px;
    bottom: 20px;
  }
  
  .panel-box{
    box-shadow: 0 2px 4px 0 @color-text-placeholder;
    position:relative;
    width:250px;
    background: @fff;
    position:absolute;
      .panel-title {
        background:@text-normal-color;
        height:28px;
        color: @fff;
        font-size:14px;
        line-height:28px;
        padding-left: 10px;
        cursor: move;
        .title{
          margin-left:4px;
          font-weight: @font-medium;
        }
        .close{
          float: right;
          margin-right:10px;
          font-size:14px;
          transform: scale(0.8);
        }
      }
      .panel-main-content{
        overflow:hidden;
        position:absolute;
        bottom: 16px;
        top:62px;
        right:0;
        left:0;
        .ksd-nobr-text {
          width: calc(~'100% - 80px');
          color: @text-title-color;
          &.checkbox-text-overflow {
            width: calc(~'100% - 90px');
            height: 100%;
            .el-checkbox {
              width: 100%;
              display: inline-block;
              .el-checkbox__label {
                width: calc(~'100% - 20px');
                display: inline-block;
                position: relative;
                top: -1px;
                text-overflow: ellipsis;
                overflow: hidden;
              }
              .el-checkbox__input {
                top: 2px;
              }
            }
            .count-all {
              display: inline-grid;
              height: 100%;
            }
          }
          .text {
            white-space: pre;
          }
        }
        .content-scroll-layout {
          height: 100%;
        }
        ul {
          list-style: circle;
          margin-top:15px;
          margin-bottom:17px;
        }
        .dimension-list , .measure-list, .cc-list{
          cursor:default;
          margin-top:0;
          li {
            line-height:28px;
            height:28px;
            padding: 0 7px 0px 10px;
            border-bottom: 1px solid @line-border-color;
            box-sizing: border-box;
            .li-type{
              position:absolute;
              right:4px;
              font-size:12px;
              color:@text-disabled-color;
            }
            .col-name {
              color:@text-title-color;
            }
            .icon-group {
              position: absolute;
              right: 7px;
              color: @text-title-color;
            }
            .icon-span {
              display:none;
              margin-left:5px;
              float:right;
              font-size:14px;
            }
            &.is-checked {
              background-color:@base-color-9;
            }
            &.error-measure {
              .ksd-nobr-text, .li-type {
                color: @error-color-1;
              }
            }
            &:hover {
              .li-type{
                display:none;
              }
              background-color:@base-color-9;
              .icon-span{
                .ky-square-box(22px,22px);
                display:inline-block;
                margin-top: 3px;
                border-radius: 2px;
                &:hover {
                  background-color: @background-color-regular;
                  color: @base-color;
                }
                &.is-disabled {
                  i {
                    cursor: not-allowed;
                    color: @text-disabled-color;
                  }
                  &:hover {
                    background-color: transparent;
                    color: @text-title-color;
                  }
                }
              }
            }
          }
        }
        .cc-list {
          li {
            background-color:@warning-color-2;
            &:hover,
            &.is-checked {
              background-color:@warning-color-3;
            }
          }
        }
      }
      .panel-sub-title {
        height:35px;
        background:@fff;
        line-height:34px;
        border-bottom: 1px solid @text-placeholder-color;
        padding: 1px;
        box-sizing: border-box;
        position: relative;
        overflow: hidden;
        .action_group,
        .batch_group {
          font-size: 0;
          display: flex;
          position: absolute;
          width: 100%;
          top: 1px;
          z-index: @--index-normal - 1;
          // transform: translateX(0px);
          transition: transform .4s ease-in-out;
          &.is_active {
            transition: transform .4s ease-in-out;
            z-index: @--index-normal + 1;
          }
          .action_btn {
            height: 32px;
            flex: 1 0 33.1%;
            font-size: 14px;
            display: inline-block;
            border-right: 1px solid @fff;
            background-color: @base-color-9;
            color:  @base-color;
            text-align: center;
            cursor: pointer;
            i {
              display: inline;
            }
            span {
              display: none;
              font-size: 12px;
            }
            &:last-child {
              border-right: 0;
            }
            &:hover {
              background-color: @base-color;
              color: @fff;
              i {
                display: none;
              }
              span {
                display: inline;
              }
            }
            &.disabled,
            &.disabled:hover {
              background-color: @background-disabled-color;
              color: @text-disabled-color;
              cursor: not-allowed;
              i {
                cursor: not-allowed;
              }
            }
          }
        }
        .batch_group .action_btn {
          background-color: @base-color;
          color: @fff;
          &:hover {
            background-color: @base-color-11;
          }
        }
      }
  }
  .panel-datasource {
    .tree-box {
      width:100%;
      &.iframeTreeBox {
        .el-tree > .el-tree-node > .el-tree-node__content{
          display: none;
        }
        .el-tree > .el-tree-node > .el-tree-node__children > .el-tree-node{
          border-top: none;
        }
      }
      .body{
        width:100%;
        padding:10px;
      }
      .empty-data {
        top:120%;
      }
    }
  }
  .panel-setting {
    height:316px;
    .panel-main-content{
      color:@text-disabled-color;
      .el-radio {
        color:@text-disabled-color;
      }
      overflow:hidden;
      padding: 10px;
      .active{
        color:@text-normal-color;
        .el-radio{
          color:@text-normal-color;
        }
      }
      ul {
        margin-left:40px;
        li{
          list-style:disc;
        }
      }
    }
  }
  .panel-search-box {
    cursor: default;
    .search-action-list {
      font-size:12px;
      margin-top:140px;
      padding-right:30px;
      .action-list-title {
        height:30px;
        line-height:30px;
        font-weight: @font-medium;
        border-bottom: solid 1px @line-split-color;
      }
      .search-list-icon {
        width:16px;
        height:16px;
        position:absolute;
        .ky-square-box(16px, 16px);
        border-radius: 50%;
      }
      .action-content {
        border-bottom: dashed 1px @line-split-color;
        .action-title {
          padding:10px 0;
        }
        .action-desc {
          margin-left:26px;
          word-break: break-all;
          i {
            font-weight: @font-medium;
            font-style: normal;
          }
        }
      }
    }
    &.full-screen {
      top:51px!important;
    }
    width:100%!important;
    height:100%!important;
    position:fixed;
    top:102px!important;
    bottom:0!important;
    left:0!important;
    right:0!important;
    background:rgba(255,255,255,.93);
    z-index: 120!important;
    .close {
      position: absolute;
      right:10px;
      top:10px;
      width:72px;
      height:72px;
      font-size:18px;
      text-align:center;
      border-radius: 50%;
      cursor:pointer;
      i {
        font-size:24px;
      }
      &:hover {
        background: @grey-2;
      }
    }
    
    .search-action-result {
      width:620px;
      // margin: 0 auto;
      top: 90px;
      position: absolute;
      left: 50%;
      margin-left: -310px;
    }
    .search-input {
      .search-position();
      height:72px;
      margin-top: 140px;
      .el-input__inner {
        height:72px;
        font-size:24px;
        color:@text-normal-color;
        padding-left: 50px;
      }
      .el-input__prefix {
        font-size:24px;
        margin-left: 14px;
      }
      .el-input__suffix {
        font-size:24px;
      }
    }
  }
  .tool-icon{
    position:absolute;
    width: 32px;
    height:32px;
    text-align: center;
    line-height: 32px;
    border-radius: 50%;
    cursor: pointer;
    .new-icon {
      background-color:@error-color-1;
      border-radius:2px;
      font-size:12px;
      text-align: center;
      position:absolute;
      width:30px;
      height:18px;
      line-height:18px;
      left: 16px;
      top:-6px;
      transform:scale(0.6);
    }
  }
  .tool-icon-group {
    position:absolute;
    width: 32px;
    top: 42px;
    right: 10px;
    .tool-icon {
      box-shadow: @box-shadow;
      background:@text-normal-color;
      color:#fff;
      position:relative;
      margin-bottom: 10px;
      font-weight: @font-medium;
      font-size: 16px;
      &.active{
        background:@base-color;
      }
    }
  }
  .icon-ds {
    top: 40px;
    left: 10px;
    background: @text-normal-color;
    color: @fff;
    box-shadow: @box-shadow;
    &.active{
      background: @base-color;
    }
    &:hover{
      background: @base-color;
    }
  }
  .icon-lock-status {
    top:10px;
    right:10px;
    i {
        display: block;
        line-height: 32px;
    }
    border:solid 1px @text-normal-color;
    &:hover{
      color: @fff;
      background-color: @normal-color-1;
      border:solid 1px @normal-color-1;
    }
  }
  .unlock-icon {
    &:hover{
      color: @fff;
      background-color: @base-color;
      border:solid 1px @base-color;
    }
  }
  .model-edit {
    height: 100%;
    position: relative;
    .drag-svg {
      z-index: 50;
    }
    svg.jtk-connector {
      cursor: pointer;
      &.jtk-hover:not(.is-broken):not(.is-focus) {
        > path {
          stroke: @base-color-6;
          stroke-width: 2;
        }
      }
      &.jtk-hover.is-broken {
        > path {
          stroke: @ke-color-danger;
          stroke-width: 2;
        }
      }
    }
  }
  .edit-table-layout {
    width: 100%;
    height: 100%;
    position: absolute;
    top: 0;
    left: 0;
    z-index: 100000;
  }
  .table-box {
    background-color: @fff;
    position: absolute;
    // box-shadow: @fact-shadow;
    border-radius: 6px;
    border: 2px solid @ke-border-secondary;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    &.is-focus {
      border: 2px solid @line-border-focus;
      .column-list-box {
        .column-li.drag-column-li-in {
          border: 0 !important;
          border-top: 2px solid @line-border-drag !important;
          border-bottom: 2px solid @line-border-drag !important;
        }
      }
    }
    &.is-broken.is-focus {
      border: 2px solid @ke-color-danger;
    }
    &.link-hover:not(.is-focus) {
      border: 2px solid @base-color-6;
    }
    &:hover:not(.is-focus) {
      // box-shadow: @fact-hover-shadow;
      border: 2px solid @base-color-6;
      .scrollbar-track-y{
        opacity: 1;
      }
    }
    &.is-hover:not(.is-focus) {
      border: 2px solid @base-color-6;
    }
    &:focus {
      border: 2px solid @line-border-focus;
    }
    &.isLookup {
      // box-shadow:@lookup-shadow;
      // &:hover {
      //   box-shadow:@lookup-hover-shadow;
      // }
      .table-title {
        background-color: @background-gray;
        color: @fff;
        .setting-icon {
          i:hover {
            background-color: rgba(0, 0, 0, 0.2);
          }
        }
      }
    }
    &:not(.is-focus) .column-list-box {
      &.ksd-drag-box *[draggable="true"].is-link:hover {
        border: 0 !important;
        border-top: 2px solid @ke-color-primary !important;
        border-bottom: 2px solid @ke-color-primary !important;
      }
      .column-li {
        &.is-link {
          &.is-hover {
            background-color: @base-color-8;
            border-top: 2px solid @line-border-drag;
            border-bottom: 2px solid @line-border-drag;
          }
        }
      }
    }
    &.is-focus .column-list-box {
      &.ksd-drag-box *[draggable="true"].is-link:hover {
        border: 0 !important;
        border-top: 2px solid @ke-color-primary !important;
        border-bottom: 2px solid @ke-color-primary !important;
      }
      .column-li {
        &.is-link {
          &.is-hover {
            background-color: @base-color-8;
            border-top: 2px solid @line-border-drag;
            border-bottom: 2px solid @line-border-drag;
          }
        }
      }
    }
    .no-data {
      position: initial;
      margin-top: 32px;
    }
    // overflow: hidden;
    .table-title {
      background-color: @fact-title-color;
      color:@fff;
      line-height: 38px;
      border-radius: 4px 4px 0 0;
      height: 38px;
      padding: 0 8px 0 12px;
      display: flex;
      align-items: center;
      cursor: move;
      &.table-spread-out {
        border-radius: 4px;
      }
      .table-sign {
        display: inline-block;
        font-size: 0;
      }
      .table-column-nums {
        font-size: 12px;
        // cursor: pointer;
      }
      .setting-icon {
        font-size: 0;
        text-align: center;
        margin-left: 4px;
        cursor: pointer;
        i {
          font-size: 22px;
          margin: auto;
          color: @fff;
          cursor: pointer;
          border-radius: 4px;
          &:hover {
            background-color: rgba(47, 55, 76, 0.2);
          }
        }
      }
      .table-dropdown {
        font-size: 0;
      }
      .custom-tooltip-layout {
        height: 100%;
        cursor: default;
      }
      .name {
        line-height: 29px\0;
        width: calc(~"100% - 50px");
        height: 100%;
        display: inline-block;
        margin-left: 4px;
        font-weight: bold;
        .tip_box {
          cursor: move;
          .alias-span {
            font-size: 14px;
            font-weight: @font-medium;
          }
        }
      }
      span {
        line-height: 30px\0;
      }
      .kind {
        font-size: 14px;
        margin: 0;
      }
      .kind:hover {
        color:@grey-3;
      }
      i {
        color:@fff;
        margin: auto 6px 8px;
      }
    }
    .column-search-box {
      height: 44px;
      line-height: 44px;
      padding: 0 8px;
      border-bottom: solid 1px @line-border-color;
      .el-input__inner {
        border: none;
        &:focus {
          box-shadow: none;
        }
      }
    }
    .column-list-box {
      overflow: auto;
      overflow-x: hidden;
      flex: 1;
      border-radius: 0 0 5px 5px;
      &.ksd-drag-box *[draggable="true"]:not(.is-link):hover {
        border: 0 !important;
        border-top: 2px solid @base-color-6 !important;
        border-bottom: 2px solid @base-color-6 !important;
      }
      .column-li {
        cursor: default;
      }
      ul {
        li {       
          padding-left: 5px;
          cursor: move;
          border-top: solid 2px transparent;
          border-bottom: 2px solid transparent;
          height: 32px;
          line-height: 32px;
          font-size: 14px;
          position: relative;
          &.is-hover {
            background-color: @ke-background-color-secondary;
            border-top: 2px solid @base-color-6;
            border-bottom: 2px solid @base-color-6;
          }
          &.is-focus {
            background-color: @base-color-8;
            border-top: 2px solid @line-border-drag;
            border-bottom: 2px solid @line-border-drag;
          }
          &:hover{
            border-top: 2px solid @base-color-6;
            border-bottom: 2px solid @base-color-6;
            background-color: @ke-color-info-bg;
          }
          &.is-link {
            background: @ke-color-info-bg;
            .col-name {
              color: @ke-color-primary;
              font-weight: @font-medium;
            }
            &.is-focus {
              background: @base-color-8;
            }
            &:hover {
              background: @base-color-8;
            }
          }
          .ksd-nobr-text {
            width: calc(~'100% - 25px');
            display: flex;
            align-items: center;
            .tip_box {
              height: 32px;
              line-height: 30px;
            }
          }
          .col-type-icon {
            color: @text-disabled-color;
            font-size: 12px;
            margin-right: 5px;
            .is-pfk{
              color: @text-placeholder-color;
              position: absolute;
              right: 8px;
            }
            i {
              cursor: default;
            }
          }
          .col-name {
            color: @text-title-color;
            font-size: 12px;
          }
          &.column-li-cc {
            background-color:@warning-color-2;
            &:hover {
              background-color:@warning-color-3;
            }
          }
          &.li-load-more {
            text-align:center;
            cursor:pointer;
            font-size: 12px;
            color: @text-disabled-color;
            border-bottom:none;
          }
        }
        .column-search-results {
          font-size: 12px;
          color: @text-placeholder-color;
          text-align: center;
          cursor: default;
          padding-left: 0;
          &:hover{
            background-color: transparent;
            border: 2px solid transparent;
          }
        }
      }
      .scrollbar-track-y {
        width: 4px;
        margin-right: 2px;
        .scrollbar-thumb-y {
          width: 4px;
          background: @ke-background-color-hover;
        }
      }
    }
  }
  .column-point-dot {
    z-index: 100;
    border: 2px solid @base-color-6;
    border-radius: 100%;
    height: 8px !important;
    width: 8px !important;
    background: #fff !important;
    transform: translate(-30%, -25%);
    &.jtk-hover {
      width: 16px !important;
      height: 16px !important;
      transform: translate(-30%, -37%) !important;
    }
    .add-point {
      position: absolute;
      color: @base-color-6;
      z-index: 20;
      cursor: pointer;
      width: 100%;
      display: inline-block;
      top: -3px;
      padding: 0 2px;
      box-sizing: border-box;
      font-size: 16px;
    }
    &.is-focus {
      border: 3px solid @ke-color-primary;
      .add-point {
        color: @ke-color-primary;
      }
    }
  }
  .line-end {
    background: transparent;
    z-index: 100;
    .line-end-pointer {
      font-size: 20px;
      color: @base-color-6;
      &:hover {
        color: @ke-color-primary;
      }
    }
  }
}
.error-font {
  color: @error-color-1;
}
.table-actions-dropdown {
  transform: translate(106%, 0);
  margin-top: -38px !important;
  width: 200px;
  .spread-or-expand-table {
    display: flex;
    align-items: center;
    justify-content: space-between;
    .db {
      color: @text-placeholder-color;
      font-size: 12px;
    }
  }
}
.model-alias-tooltip {
  margin-top: -5px !important;
}
#custom-drag-image {
  color: @ke-color-primary;
  font-size: 18px;
  position: absolute;
  background: transparent;
  opacity: 0;
}
#broken-use-group:hover{
  > path:not(#use) {
    stroke: @ke-color-danger;
    stroke-width: 2;
  }
}
#use-group:hover {
  > path:not(#use) {
    stroke: @base-color-6;
    stroke-width: 2;
  }
}
.model-action-tools {
  .el-dropdown-menu__item {
    i {
      margin-top: -2px;
    }
    &.is-active {
      color: @ke-color-primary;
    }
  }
}
</style>