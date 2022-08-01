<template>
  <div class="model-edit-outer" @drop='dropTable($event)' @dragover='allowDrop($event)' v-drag="{sizeChangeCb:dragBox}" @dragleave="dragLeave">
    <div class="model-edit">
      <el-button v-visible @click="guideActions"></el-button>
      <!-- 为了保证 gif 每次都从第一帧开始播放，所以每次都要重新加载 -->
      <kylin-empty-data class="gifEmptyData" :content="$t('noTableTip')" :image="require('../../../../assets/img/editmodel.gif') + '?v=' + new Date().getTime()" v-if="!Object.keys(modelRender.tables).length"></kylin-empty-data>
      <!-- table box -->
      <div class="table-box" @click="activeTablePanel(t)" v-visible="!currentEditTable || currentEditTable.guid !== t.guid" :id="t.guid" v-event-stop :class="['js_' + t.alias.toLocaleLowerCase(), {'isLookup':t.kind==='LOOKUP'}]" v-for="t in modelRender && modelRender.tables || []" :key="t.guid" :style="tableBoxStyle(t.drawSize)">
        <div class="table-title" :data-zoom="modelRender.zoom"  v-drag:change.left.top="t.drawSize">
          <common-tip class="name" v-show="!t.aliasIsEdit">
            <span slot="content">{{t.alias}}</span>
            <span class="alias-span ksd-ml-4">{{t.alias}}</span>
          </common-tip>
          <span class="setting-icon" v-if="!isSchemaBrokenModel" @click="editTable(t.guid)"><i class="el-icon-ksd-table_setting"></i></span>
        </div>
        <div class="column-search-box"><el-input prefix-icon="el-ksd-icon-search_22" v-model="t.filterColumnChar" @input="t.filterColumns()" size="small" :placeholder="$t('searchColumn')"></el-input></div>
        <div class="column-list-box ksd-drag-box" v-if="!t.drawSize.isOutOfView&&t.showColumns.length" @dragover='($event) => {allowDropColumn($event, t.guid)}' @drop='(e) => {dropColumn(e, null, t)}' v-scroll.reactive>
          <ul>
            <li v-on:dragover="(e) => {dragColumnEnter(e, t)}" v-on:dragleave="dragColumnLeave" class="column-li" :class="{'column-li-cc': col.is_computed_column}" @drop.stop='(e) => {dropColumn(e, col, t)}' @dragstart="(e) => {dragColumns(e, col, t)}"  draggable v-for="col in t.showColumns" :key="col.name">
              <span class="ksd-nobr-text">
                <span class="col-type-icon">
                  <i class="el-icon-ksd-fkpk_big is-pfk" v-show="col.isPFK"></i><i :class="columnTypeIconMap(col.datatype)"></i>
                </span>
                <span class="col-name">{{col.name}}</span>
              </span>
            </li>
            <!-- 渲染可计算列 -->
            <template v-if="t.kind=== 'FACT'">
              <li class="column-li column-li-cc" @drop='(e) => {dropColumn(e, {name: col.columnName }, t)}' @dragstart="(e) => {dragColumns(e, {name: col.columnName}, t)}"  draggable v-for="col in modelRender.computed_columns" :key="col.name">
                <span class="ksd-nobr-text">
                  <span class="col-type-icon">
                    <i class="el-icon-ksd-fkpk_big is-pfk" v-show="col.isPFK"></i><i :class="columnTypeIconMap(col.datatype)"></i>
                  </span>
                  <span class="col-name">{{col.columnName}}</span>
                </span>
              </li>
            </template>
            <li class="li-load-more" v-if="t.hasMoreColumns" @click="t.loadMoreColumns()">{{$t('kylinLang.common.loadMore')}}</li>
          </ul>
        </div>
        <kylin-nodata v-else :content="$t('kylinLang.common.noResults')"></kylin-nodata>
        <!-- 拖动操纵 -->
        <DragBar :dragData="t.drawSize" :dragZoom="modelRender.zoom"/>
        <!-- 拖动操纵 -->
      </div>
      <!-- table box end -->
    </div>
    <!-- datasource面板  index 3-->
    <div class="tool-icon icon-ds" v-if="panelAppear.datasource.icon_display" :class="{active: panelAppear.datasource.display}" v-event-stop @click="toggleMenu('datasource')"><i class="el-icon-ksd-data_source"></i></div>
      <transition name="bounceleft">
        <div class="panel-box panel-datasource"  v-show="panelAppear.datasource.display" :style="panelStyle('datasource')" v-event-stop>
          <div class="panel-title" v-drag:change.left.top="panelAppear.datasource"><span class="title">{{$t('kylinLang.common.dataSource')}}</span><span class="close" @click="toggleMenu('datasource')"><i class="el-icon-ksd-close"></i></span></div>
          <!-- <div v-scroll style="height:calc(100% - 79px)"> -->
          <DataSourceBar
            :ignore-node-types="['column']"
            class="tree-box"
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
        <div class="tool-icon" v-if="panelAppear.search.icon_display" :class="{active: panelAppear.search.display}" @click="toggleMenu('search')">
          <i class="el-icon-ksd-search"></i>
          <span class="new-icon">New</span>
        </div>
      </div>
      <!-- 快捷操作 -->
      <div class="sub-tool-icon-group" v-event-stop>
        <div class="tool-icon" @click="reduceZoom"><i class="el-icon-ksd-shrink" ></i></div>
        <div class="tool-icon" @click="addZoom"><i class="el-icon-ksd-enlarge"></i></div>
        <!-- <div class="tool-icon" v-event-stop>{{modelRender.zoom}}0%</div> -->
        <div class="tool-icon tool-full-screen" @click="fullScreen"><i class="el-icon-ksd-full_screen_2" v-if="!isFullScreen"></i><i class="el-icon-ksd-collapse_2" v-if="isFullScreen"></i></div>
        <div class="tool-icon" @click="autoLayout"><i class="el-icon-ksd-auto"></i></div>
      </div>
      <!-- 右侧面板组 -->
      <!-- <div class="panel-group"> -->
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
                  <span v-if="dimensionSelectedList.length==modelRender.dimensions.length || (modelInstance.second_storage_enabled||isHybridModel)&&dimensionSelectedList.length+1==modelRender.dimensions.length&&!isDisableBatchCheck">{{$t('unCheckAll')}}</span>
                  <span v-else>{{$t('checkAll')}}</span>
                </span>
                <span class="action_btn" :class="{'disabled': dimensionSelectedList.length==0}" @click="deleteDimenisons">
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
                  <li v-for="(d, i) in allDimension" :key="d.name" :class="{'is-checked':dimensionSelectedList.indexOf(d.name)>-1}">
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
                  <i class="el-icon-ksd-batch" v-if="measureSelectedList.length > 0 && measureSelectedList.length==toggleMeasureStatus"></i>
                  <i class="el-icon-ksd-batch_uncheck" v-else></i>
                  <span v-if="measureSelectedList.length > 0 && measureSelectedList.length==toggleMeasureStatus">{{$t('unCheckAll')}}</span>
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
    <transition name="bouncecenter">
      <div class="panel-search-box panel-box" :class="{'full-screen': isFullScreen}"  v-event-stop :style="panelStyle('search')" v-if="panelAppear.search.display">
        <el-row :gutter="20">
          <el-col :span="14" :offset="5">
            <el-alert class="search-action-result" v-if="modelSearchActionSuccessTip" v-timer-hide:2
              :title="modelSearchActionSuccessTip"
              type="success"
              :closable="false"
              show-icon>
            </el-alert>
            <el-input @input="searchModelEverything"  clearable class="search-input" :placeholder="$t('searchInputPlaceHolder')" v-model="modelGlobalSearch" prefix-icon="el-ksd-icon-search_22"></el-input>
            <transition name="bounceleft">
              <div v-scroll.reactive class="search-result-box" v-keyborad-select="{scope:'.search-content', searchKey: modelGlobalSearch}" v-if="modelGlobalSearch && showSearchResult" v-search-highlight="{scope:'.search-name', hightlight: modelGlobalSearch}">
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
            </transition>
          </el-col>
          <el-col :span="5">
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
        <div class="close" @click="toggleMenu('search')" v-global-key-event.esc="() => {toggleMenu('search')}">
          <i class="el-icon-ksd-close ksd-mt-12"></i><br/>
          <span>ESC</span>
        </div>
      </div>
    </transition>


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

    <!-- 编辑模型table遮罩 -->
    <div class="full-screen-cover" v-event-stop @click="cancelTableEdit" v-if="showTableCoverDiv"></div>
    <div class="edit-table-layout" v-if="showTableCoverDiv" v-event-stop @click.self="cancelTableEdit">
      <!-- 被编辑table clone dom -->
      <div class="table-box fast-action-temp-table" :id="currentEditTable.guid + 'temp'" v-event-stop :class="{isLookup:currentEditTable.kind==='LOOKUP'}" :style="tableBoxStyleNoZoom(currentEditTable.drawSize)">
        <transition name="slide-fade">
          <!-- 编辑table 快捷按钮 -->
          <div class="fast-action-box" v-event-stop @click="cancelTableEdit" :class="{'edge-right': currentEditTable.drawSize.isInRightEdge}" v-if="currentEditTable">
            <div v-if="currentEditTable.kind === 'FACT' || modelInstance.checkTableCanSwitchFact(currentEditTable.guid)">
              <div class="action switch" :class="{'disabled': currentEditTable.source_type === 1}" v-if="currentEditTable.kind === 'FACT'" @click.stop="changeTableType(currentEditTable)"><i class="el-icon-ksd-switch"></i>
                <span>{{$t('switchLookup')}}</span>
              </div>
              <div class="action switch" v-if="modelInstance.checkTableCanSwitchFact(currentEditTable.guid)" @click.stop="changeTableType(currentEditTable)"><i class="el-icon-ksd-switch"></i>
                <span >{{$t('switchFact')}}</span>
              </div>
            </div>
            <div v-show="showEditAliasForm">
              <div class="alias-form" v-event-stop:click>
                <el-form :model="formTableAlias" :rules="aliasRules" ref="aliasForm" @submit.native="()=> {return false}">
                  <el-form-item prop="currentEditAlias">
                  <el-input v-model="formTableAlias.currentEditAlias" size="mini" @click.stop @keyup.enter.native="saveEditTableAlias"></el-input>
                  <input type="text" style="display:none" />
                  <el-button type="primary" size="mini" icon="el-icon-check" @click.stop="saveEditTableAlias"></el-button><el-button size="mini" @click.stop="cancelEditAlias" icon="el-icon-close" plain></el-button>
                  </el-form-item>
                </el-form>
              </div>
            </div>
            <div v-show="!showEditAliasForm && currentEditTable.kind!=='FACT'">
              <div class="action">
                <div @click.stop="openEditAliasForm"><i class="el-icon-ksd-table_edit"></i> {{$t('editTableAlias')}}</div>
              </div>
            </div>
            <template v-if="!showEditAliasForm">
              <el-popover
                popper-class="fast-action-popper"
                style="z-index:100001"
                ref="popover5"
                placement="top"
                width="160"
                v-model="delTipVisible">
                <p>{{$t('delTableTip')}}</p>
                <div style="text-align: right; margin: 0">
                  <el-button size="mini" type="info" text @click="delTipVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
                  <el-button type="primary" size="mini" @click.enter="delTable">{{$t('kylinLang.common.ok')}}</el-button>
                </div>
              </el-popover>
              <div class="action del" v-if="!modelInstance.checkTableCanDel(currentEditTable.guid)" @click.stop="showDelTableTip"  v-popover:popover5><i class="el-icon-ksd-table_delete"></i> {{$t('deleteTable')}}</div>
              <div class="action del" v-else @click.stop="delTable"><i class="el-icon-ksd-table_delete"></i> {{$t('deleteTable')}}</div>
            </template>
          </div>
      </transition>
        <div class="table-title" :data-zoom="modelRender.zoom"  v-drag:change.left.top="currentEditTable.drawSize">
          <span @click.stop="changeTableType(currentEditTable)">
            <i class="el-icon-ksd-fact_table kind" v-if="currentEditTable.kind==='FACT'"></i>
            <i v-else class="el-icon-ksd-lookup_table kind"></i>
          </span>
          <span class="alias-span name">{{currentEditTable.alias}}</span>
          <span class="setting-icon guide-setting" @click="cancelTableEdit"><i class="el-icon-ksd-table_setting"></i></span>
        </div>
        <div class="column-search-box"><el-input prefix-icon="el-ksd-icon-search_22" v-model="currentEditTable.filterColumnChar" @input="currentEditTable.filterColumns()" size="small"></el-input></div>
        <div class="column-list-box" v-scroll v-if="currentEditTable.showColumns.length">
          <ul >
            <li class="column-li" :class="{'column-li-cc': col.is_computed_column}"  v-for="col in currentEditTable.showColumns" :key="col.name">
              <span class="ksd-nobr-text">
                <span class="col-type-icon"> <i class="el-icon-ksd-fkpk_big is-pfk" v-show="col.isPFK"></i><i :class="columnTypeIconMap(col.datatype)"></i></span>
                <span class="col-name">{{col.name}}</span>
              </span>
              <!-- <span class="li-type ky-option-sub-info">{{col.datatype}}</span> -->
            </li>
            <template v-if="currentEditTable.kind=== 'FACT'">
              <li class="column-li column-li-cc"  v-for="col in modelRender.computed_columns" :key="col.name">
                <span class="ksd-nobr-text">
                  <span class="col-type-icon"><i class="el-icon-ksd-fkpk_big is-pfk" v-show="col.isPFK"></i><i :class="columnTypeIconMap(col.datatype)"></i></span>
                  <span class="col-name">{{col.columnName}}</span>
                </span>
                <!-- <span class="li-type ky-option-sub-info">{{col.datatype}}</span> -->
              </li>
            </template>
            <li class="li-load-more" v-if="currentEditTable.hasMoreColumns" @click="currentEditTable.loadMoreColumns()">{{$t('kylinLang.common.loadMore')}}</li>
          </ul>
        </div>
        <kylin-nodata v-else :content="$t('kylinLang.common.noResults')"></kylin-nodata>
        <!-- 拖动操纵 -->
        <DragBar :dragData="currentEditTable.drawSize"/>
        <!-- 拖动操纵 -->
      </div>
    </div>
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
      <!-- <div class="ksd-pl-26">
        <div>{{$t('saveSuccessTip')}}</div>
        <div v-if="saveModelType==='saveModel'&&!isFullLoad || saveModelType==='updataModel'&&modelData.segments&&!modelData.segments.length">{{$t('addSegmentTips')}}</div>
        <div v-if="saveModelType==='saveModel'&&isFullLoad || saveModelType==='updataModel'&&modelData.segments&&modelData.segments.length">{{$t('addIndexTips')}}</div>
      </div>
      <span slot="footer" class="dialog-footer" v-if="gotoIndexdialogVisible">
        <el-button plain @click="ignoreAddIndex">{{$t('ignoreaddIndexTip')}}</el-button>
        <el-button type="primary" v-if="saveModelType==='saveModel'&&!isFullLoad || saveModelType==='updataModel'&&modelData.segments&&(!modelData.segments.length || isPurgeSegment)" @click="willAddSegment">{{$t('addSegment')}}</el-button>
        <el-button type="primary" v-if="saveModelType==='saveModel'&&isFullLoad || saveModelType==='updataModel'&&modelData.segments&&modelData.segments.length&&!isPurgeSegment" @click="willAddIndex">{{$t('addIndex')}}</el-button>
      </span> -->
    </el-dialog>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations, mapState } from 'vuex'
import locales from './locales'
import DataSourceBar from '../../../common/DataSourceBar'
import { handleSuccess, handleError, loadingBox, kylinMessage, kylinConfirm } from '../../../../util/business'
import { isIE, groupData, objectClone, filterObjectArray, handleSuccessAsync, indexOfObjWithSomeKey } from '../../../../util'
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
import { modelRenderConfig, modelErrorMsg } from './config'
import { NamedRegex, columnTypeIcon } from '../../../../config'
@Component({
  props: ['extraoption'],
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isFullScreen',
      'datasourceActions'
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
    ShowCC
  },
  locales
})
export default class ModelEdit extends Vue {
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
  modelData = {}
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

  async showGuide () {
    await this.callGuideModal({ isShowDimAndMeasGuide: true })
    localStorage.setItem('isFirstAddModel', 'false')
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
  query (className) {
    return $(this.$el.querySelector(className))
  }
  get isSchemaBrokenModel () {
    return this.modelRender.broken_reason === 'SCHEMA'
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
  guideActions (obj) {
    let data = obj.data
    if (obj.action === 'addTable') {
      let { left, top } = this.modelInstance.renderDom.getBoundingClientRect()
      this.modelInstance.addTable({
        table: data.tableName,
        alias: data.tableName.split('.')[1],
        guid: data.guid,
        isSecStorageEnabled: this.modelInstance.second_storage_enabled,
        drawSize: {
          left: data.x - left - this.modelRender.zoomXSpace,
          top: data.y - top - this.modelRender.zoomYSpace
        }
      })
    } else if (obj.action === 'link') {
      let fTable = this.modelInstance.getTableByGuid(data.fguid)
      let pTable = this.modelInstance.getTableByGuid(data.pguid)
      let joinDialogOption = {
        fid: data.fguid,
        pid: data.pguid,
        joinType: data.joinType,
        fColumnName: fTable.alias + '.' + data.fColumnName,
        pColumnName: pTable.alias + '.' + data.pColumnName,
        tables: this.modelRender.tables
      }
      this.callJoinDialog(joinDialogOption)
    }
  }
  // 取消table编辑
  cancelTableEdit () {
    this.showTableCoverDiv = false
    this.currentEditTable = null
    this.showEditAliasForm = false
    this.formTableAlias.currentEditAlias = ''
    this.delTipVisible = false
  }
  showDelTableTip () {
    this.delTipVisible = true
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

  toggleCheckbox () {
    if (this.allDimension.length === 0 && !this.isShowCheckbox) {
      return
    } else if (this.isShowCheckbox) {
      this.dimensionSelectedList = []
    }
    this.isShowCheckbox = !this.isShowCheckbox
  }
  get translate () {
    if (this.isShowCheckbox) {
      return 0 - this.panelAppear.dimension.width
    } else {
      return 0
    }
  }
  toggleMeaCheckbox () {
    if (this.modelRender.all_measures.length === 1 && !this.isShowMeaCheckbox) {
      return
    } else if (this.isShowMeaCheckbox) {
      this.measureSelectedList = []
    }
    this.isShowMeaCheckbox = !this.isShowMeaCheckbox
  }
  get translateMea () {
    if (this.isShowMeaCheckbox) {
      return 0 - this.panelAppear.measure.width
    } else {
      return 0
    }
  }
  toggleCCCheckbox () {
    if (this.modelRender.computed_columns.length === 0 && !this.isShowCCCheckbox) {
      return
    } else if (this.isShowCCCheckbox) {
      this.ccSelectedList = []
    }
    this.isShowCCCheckbox = !this.isShowCCCheckbox
  }
  get translateCC () {
    if (this.isShowCCCheckbox) {
      return 0 - this.panelAppear.cc.width
    } else {
      return 0
    }
  }
  delTable () {
    this.modelInstance.delTable(this.currentEditTable.guid).then(() => {
      this.cancelTableEdit()
      if (this.modelData.available_indexes_count > 0 && !this.isIgnore) {
        this.showChangeTips()
      }
    }, () => {
      this.delTipVisible = false
      // kylinMessage(this.$t('delTableTip'), {type: 'warning'})
    })
  }
  // 编辑table
  editTable (guid) {
    this._hisZoom = this.modelRender.zoom
    this.currentEditTable = this.modelInstance.getTableByGuid(guid)
    this.formTableAlias.currentEditAlias = this.currentEditTable.alias
    this.showTableCoverDiv = true
    this.showEditAliasForm = false
    this.delTipVisible = false
    this.$nextTick(() => {
      const dom = this.$el.querySelector('.edit-table-layout')
      const modelEdit = this.$el.querySelector('.model-edit')
      dom && (dom.style.cssText = modelEdit?.style.cssText ?? '')
    })
  }
  // 保存table的别名
  saveEditTableAlias () {
    this.$refs.aliasForm.validate((valid) => {
      if (valid) {
        this.currentEditTable.alias = this.formTableAlias.currentEditAlias
        this.saveNewAlias(this.currentEditTable)
        this.showEditAliasForm = false
      }
    })
  }
  saveNewAlias (t) {
    this.modelInstance.setUniqueAlias(t)
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
  activeTablePanel (t) {
    this.modelInstance.setIndexTop(Object.values(this.modelRender.tables), t, 'drawSize')
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
    if (t.kind === 'FACT' && t.source_type === 1) {
      return
    }
    if (this._checkTableType(t)) {
      let joinT = Object.keys(this.modelInstance.linkUsedColumns).filter(it => it.indexOf(t.guid) === 0)
      if (joinT.length && joinT.some(it => this.modelInstance.linkUsedColumns[it].length)) {
        this.cancelTableEdit()
        this.$message({
          message: this.$t('changeTableJoinCondition'),
          type: 'warning'
        })
        return
      }
      this.modelInstance.changeTableType(t)
      this.cancelTableEdit()
      if (this.modelData.available_indexes_count > 0 && !this.isIgnore) {
        this.showChangeTips()
      }
    }
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
  get isDisableBatchCheck () {
    if (this.allDimension.length === 1 && (this.modelInstance.second_storage_enabled || this.isHybridModel) && this.modelInstance.partition_desc && this.modelInstance.partition_desc.partition_date_column === this.allDimension[0].column) {
      return true
    } else {
      return false
    }
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
    // this.showCCDetailDialog({
    //   ccDetail: cc
    // })
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
  openEditAliasForm () {
    this.showEditAliasForm = true
    this.formTableAlias.currentEditAlias = this.currentEditTable.alias
  }
  filterColumns (filterVal, columns, t) {
    let reg = new RegExp(filterVal, 'gi')
    columns.forEach((col) => {
      this.$set(col, 'isHidden', filterVal ? !reg.test(col.name) : false)
    })
    // t.columns = filterObjectArray(columns, 'isfiltered', true)
  }
  getFilteredColumns (columns) {
    return filterObjectArray(columns, 'isHidden', false)
  }
  // 拖动画布
  dragBox (x, y) {
    this.$nextTick(() => {
      this.modelInstance.getSysInfo()
      this.modelInstance.moveModelPosition(x, y)
    })
  }
  // 拖动tree-table
  dragTable (node) {
    this.currentDragTable = node.database + '.' + node.label
  }
  // 拖动列
  dragColumns (event, col, table) {
    event.stopPropagation()
    this.currentDragColumn = event.srcElement ? event.srcElement : event.target
    event.dataTransfer && (event.dataTransfer.effectAllowed = 'move')
    if (!isIE()) {
      event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('text', '')
    }
    event.dataTransfer.setDragImage && event.dataTransfer.setDragImage(this.currentDragColumn, 0, 0)
    this.currentDragColumnData = {
      guid: table.guid,
      columnName: col.name,
      btype: col.btype
    }
    return true
  }
  dragColumnEnter (event, t) {
    if (t.guid === this.currentDragColumnData.guid) {
      return
    }
    var target = event.currentTarget
    $(target).addClass('drag-column-in')
  }
  dragColumnLeave (event) {
    var target = event.currentTarget
    $(target).removeClass('drag-column-in')
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
    } else if (Object.keys(this.modelInstance.tables).length === 1) {
      this.editTable(Object.keys(this.modelInstance.tables)[0])
    }
    this.currentDragTableData = {}
    this.currentDragTable = null
    this.removeDragInClass()
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
    }
    // 同步因为预计算被禁用的表
    this.modelRender.anti_flatten_lookups = this.modelInstance.anti_flatten_lookups = Object.values(this.modelInstance.tables).filter(it => data.anti_flatten_lookups.includes(it.name)).map(it => it.alias)
    this.modelRender.anti_flatten_cc = this.modelInstance.anti_flatten_cc = data.anti_flatten_cc
  }
  removeDragInClass () {
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
      $(target).parents('.column-list-box').addClass('drag-in')
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
  @Watch('dimensionDialogShow')
  @Watch('singleDimensionDialogShow')
  @Watch('tableJoinDialogShow')
  @Watch('measureVisible')
  tableJoinDialogClose (val) {
    if (!val) {
      if (this.searchHandleStart) {
        this.searchHandleStart = false
        this.panelAppear.search.display = true
      }
    }
  }
  searchModelEverything (val) {
    this.modelGlobalSearchResult = this.modelInstance.search(val)
  }
  getColumnType (tableName, column) {
    var ntable = this.modelInstance.getTable('alias', tableName)
    return ntable && ntable.getColumnType(column)
  }
  @Watch('modelGlobalSearch')
  watchSearch (v) {
    this.showSearchResult = v
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
  get tableBoxStyle () {
    return (drawSize) => {
      if (drawSize) {
        return {'z-index': drawSize.zIndex, width: drawSize.width + 'px', height: drawSize.height + 'px', left: drawSize.left + 'px', top: drawSize.top + 'px'}
      }
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
  // get tableBoxToolStyleNoZoom () {
  //   return (drawSize) => {
  //     if (drawSize) {
  //       let zoom = this.modelRender.zoom / 10
  //       if (drawSize.isInRightEdge) {
  //         return {left: drawSize.left - 230 + 'px', top: drawSize.top + 'px'}
  //       }
  //       return {left: this.currentEditTable.drawSize.width + drawSize.left + 'px', top: drawSize.top + 'px'}
  //     }
  //   }
  // }
  get searchResultData () {
    return groupData(this.modelGlobalSearchResult, 'kind')
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
          this.$emit('saveRequestEnd')
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
          this.$emit('saveRequestEnd')
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
          this.$emit('saveRequestEnd')
        })
      } else {
        kylinMessage(this.$t(modelErrorMsg[err.errorKey], {tableName: err.tableName}), {type: 'warning'})
      }
      this.$emit('saveRequestEnd')
    }).catch(() => {
      this.$emit('saveRequestEnd')
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

  setModelBoundStyle () {
    const { left, top } = this.modelRender.marginClient ?? {left: 0, top: 0}
    const dom = this.$el.querySelector('.model-edit')
    if (!dom) return
    dom.style.cssText += `margin-left: ${left}px; margin-top: ${top}px`
  }

  async mounted () {
    this.globalLoading.show()
    this.$el.onselectstart = function (e) {
      return false
    }
    // 注册保存事件
    this.$on('saveModel', () => {
      this.generateModelData()
    })
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
    this.setModelBoundStyle()
    if (localStorage.getItem('isFirstAddModel') === 'true') {
      await this.showGuide()
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
          // kylinConfirm(this.$t('saveSuccessTip'), {
          //   confirmButtonText: this.$t('addIndexTip'),
          //   cancelButtonText: this.$t('ignoreaddIndexTip'),
          //   type: 'success',
          //   confirmButtonClass: 'guide-gotoindex-btn'
          // }, this.$t('addIndexTip')).then(() => {
          //   this.$router.replace({name: 'ModelList', params: { ignoreIntercept: true, addIndex: true }})
          // }).catch(() => {
          //   this.$router.replace({name: 'ModelList', params: { ignoreIntercept: true }})
          // })
          this.$emit('saveRequestEnd')
        }, 1000)
      })
    }).catch((res) => {
      this.$emit('saveRequestEnd')
      handleError(res)
    })
  }
  // autoFilter () {
  //   clearTimeout(this.stCycle)
  //   this.stCycle = setTimeout(() => {
  //     this.getAboutKap().then((res) => {
  //       handleSuccess(res, (data) => {
  //         if (this._isDestroyed) {
  //           return
  //         }
  //         this.autoFilter()
  //       })
  //     }, (res) => {
  //       handleError(res)
  //     })
  //   }, 1500000)
  // }
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

  // created () {
  //   // 心跳请求，保持用户 active
  //   // this.autoFilter()
  // }
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
  z-index:100001!important;
}
.jtk-overlay {
  &.link-label-broken {
    background-color: @broken-line-color;
    &:hover {
      background-color: @broken-line-color;
      .close {
        &:hover {
          color:@fff;
          background: @broken-line-lable-close-hover-color;
        }
      }
    }
  }
  background-color: @base-color;
  font-size: 12px;
  z-index: 21;
  cursor: pointer;
  min-width: 40px;
  height: 20px;
  border-radius: 10px;
  text-align: center;
  line-height: 20px;
  padding: 0 4px;
  color:@fff;
  transition: width 0.5s;
  .close {
    display: none;
    .ky-square-box(14px, 14px);
    line-height: 14px;
    font-size:12px;
    float:right;
    border-radius: 7px;
    margin-left:8px;
    // vertical-align: text-bottom;
    margin-top:3px;
  }
  &:hover {
    .close {
      display: block;
      color:#ccc;
      // .ky-square-box(14px, 14px);
      // border-radius: 50%;
      // display: inline-block;
      &:hover {
        color:#fff;
        background: #4da9e7;
      }
    }
    // height: 20px;
    // line-height: 20px;
    // font-size:13px;
    color:@fff;
    background-color: @base-color-11;
    // border: 2px solid @base-color-11;
  }
}
.drag-column-in {
 background-color: @base-color-10;
 box-shadow: 2px 2px 4px 0 @text-secondary-color;
}
.box-css() {
  position:relative;
  background-color:@grey-3;
}
.search-position() {
  // width:620px;
  // left:50%;
  // margin-left:-310px;
  position:relative;
}
.model-edit-outer {
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
  .fast-action-box {
    width: 210px;
    left: 230px;
    color: @fff;
    position: absolute;
    z-index: 100001;
    margin-left:10px;
    .el-form-item__content {
      line-height: 0;
    }
    &.edge-right {
      text-align: right;
      left: -230px;
      .el-form-item__error {
        text-align: left;
      }
    }
    div {
      margin-bottom:5px;
    }
    div.alias-form{
      .el-input {
        width:140px;
      }
      .el-button+.el-button {
        margin-left:5px;
        color: @fff;
      }
    }
    div.action {
      display: inline-block;
      border-radius: 2px;
      background:black;
      color:@fff;
      height:24px;
      padding-left:5px;
      padding-right:6px;
      font-size:12px;
      line-height:25px;
      cursor:pointer;
      margin-left:0;
      transform: margin-left ease;
      &:hover {
        margin-left: 4px;
      }
      &.disabled {
        opacity: 0.375;
        cursor: not-allowed;
      }
    }
  }
  .fast-action-temp-table {
    z-index:100000!important;
  }
  border-top:@text-placeholder-color;
  user-select:none;
  overflow:hidden;
  .box-css();
  height: 100%;
  .panel-box{
      box-shadow: 0 2px 4px 0 @color-text-placeholder;
      position:relative;
      width:250px;
      .panel-title {
        background:@text-normal-color;
        height:28px;
        color:#fff;
        font-size:14px;
        line-height:28px;
        padding-left: 10px;
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
          color:@text-title-color;
          &.checkbox-text-overflow {
            width: calc(~'100% - 90px');
            height: 100%;
            .el-checkbox {
              width: 100%;
              overflow: hidden;
              text-overflow: ellipsis;
              display: inline-block;
              .el-checkbox__label {
                display: initial;
                position: relative;
                top: -1px;
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
      background:#fff;
      position:absolute;
    }
    .panel-datasource {
      .tree-box {
        width:100%;
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
      .search-result-box {
        box-shadow: 0 0px 2px 0 @color-text-placeholder;
        background-color: rgba(255, 255, 255, 1);
        height:calc(~'100vh - 464px')!important;
        min-height:250px;
        // overflow:auto;
        .search-position();
        // box-shadow:@box-shadow;
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
          &.active,&:hover{
            background-color:@base-color-9;
          }
          cursor:pointer;
          height:32px;
          line-height:32px;
          padding-left: 20px;
          .search-category {
            font-size:12px;
            color:@text-normal-color;
          }
          .search-name {
            i {
              color:@base-color;
              font-style: normal;
            }
            font-size:14px;
            color:@text-title-color;
          }
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
      width:32px;
      top:12px;
      right:10px;
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
    .sub-tool-icon-group {
      position:absolute;
      right:10px;
      top:258px;
      width:32px;
      .tool-icon{
        &.broken-location i{
          color:@error-color-1;
        }
        position:relative;
        height:30px;
        line-height:30px;
        i {
          color:@text-normal-color;
          font-size:18px;
          &:hover{
            color:@base-color;
          }
        }
      }
    }
    .icon-ds {
      top:10px;
      left:10px;
      background:@text-normal-color;
      color:#fff;
      box-shadow: @box-shadow;
      &.active{
        background:@base-color;
      }
      &:hover{
        background:@base-color;
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
        color: #fff;
        background-color:@normal-color-1;
        border:solid 1px @normal-color-1;
      }
    }
    .unlock-icon {
      &:hover{
        color: #fff;
        background-color:@base-color;
        border:solid 1px @base-color;
      }
    }
  }
  .model-edit-outer{
    .model-edit {
      height: 100%;
      position:relative;
    }
    .edit-table-layout {
      width: 100%;
      height: 100%;
      position: absolute;
      top: 0;
      left: 0;
      z-index: 100000;
    }
    .box-css();
    .table-box {
      &:hover{
        .scrollbar-track-y{
          opacity: 1;
        }
      }
      &.isLookup {
        box-shadow:@lookup-shadow;
        &:hover {
          box-shadow:@lookup-hover-shadow;
        }
        .table-title {
          background-color: @lookup-title-color;
          color:@fff;
          .setting-icon {
            &:hover{
              background-color:@base-color-14;
            }
          }
        }
      }
      background-color:#fff;
      position:absolute;
      box-shadow:@fact-shadow;
      &:hover {
        box-shadow:@fact-hover-shadow;
      }
      // overflow: hidden;
      .table-title {
        .setting-icon {
          float:right;
          font-size:14px;
          width:20px;
          height:20px;
          line-height:20px;
          text-align: center;
          margin-top: 6px;
          margin-right: 3px;
          &:hover {
            background-color:@base-color-11;
          }
          i {
            margin: auto;
            color:@fff;
          }
        }
        .name {
          &.tip_box {
            .alias-span {
              font-size: 14px;
              font-weight: @font-medium;
            }
          }
          text-overflow: ellipsis;
          overflow: hidden;
          line-height: 29px\0;
          width:calc(~"100% - 50px");
        }
        span {
          width:24px;
          height:24px;
          float:left;
          line-height: 30px\0;
        }
        .kind {
          cursor:move;
        }
        .kind:hover {
          // background-color:@base-color;
          color:@grey-3;
        }
        height:32px;
        background-color: @fact-title-color;
        color:#fff;
        line-height:32px;
        i {
          color:@fff;
          margin: auto 6px 8px;
        }
      }
      .column-search-box {
        height: 30px;
        line-height: 30px;
        padding: 0 5px;
      }
      .column-list-box {
        overflow:auto;
        position:absolute;
        border-top: solid 1px @line-border-color;
        top:62px;
        bottom:5px;
        right:0;
        left:0;
        overflow-x:hidden;
        ul {
          li {
            &:hover{
              background-color:@base-color-9;
            }
            padding-left:5px;
            cursor:move;
            border-bottom:solid 1px @line-border-color;
            height:28px;
            line-height:28px;
            font-size:14px;
            .col-type-icon {
              color:@text-disabled-color;
              font-size:12px;
              .is-pfk{
                color: #f7ba2a;
              }
            }
            .col-name {
              color:@text-title-color;
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
              &:hover{
                color:@base-color;
              }
            }
          }
        }
      }
    }
  }
  .error-font {
    color: @error-color-1;
  }

</style>
