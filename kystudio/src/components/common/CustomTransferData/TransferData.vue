<template>
    <div class="sec-index-container">
      <el-alert v-for="(it, index) in alertItems" :key="index" :title="it.text" :type="it.type" show-icon class="ksd-mb-8" :closable="false"></el-alert>
      <div class="transfer-container">
        <div class="left_layout" :style="leftStyle">
          <div class="search_layout">
            <el-input size="small" :placeholder="$t('kylinLang.common.search')" v-model.trim="searchVar" prefix-icon="el-ksd-n-icon-search-outlined"></el-input>
          </div>
          <div class="sec-index-content">
            <div class="dropdowns">
              <template v-if="filterModelColumns.length">
                <el-checkbox class="select-all" v-if="isAllSelect"  :value="isSelectAll" :indeterminate="filterTypeOptionSelectedColumns.length !== 0 && filterModelColumns.length > filterTypeOptionSelectedColumns.length" @change="selectAll">
                  <span>{{$t('kylinLang.common.selectAll')}}</span>
                </el-checkbox>
                <span v-if="searchVar" class="disable-block">
                  <el-tooltip placement="top" class="sort-colum-dropdown" :content="$t('disableSortFilterTips')">
                    <el-tag is-light :type="!columnSort ? 'info' : ''"><i class="el-ksd-n-icon-table-rank-filled ksd-mr-2"></i><span>{{$t('column')}}{{columnSort ? `: ${$t(columnSort)}` : ''}}</span></el-tag>
                  </el-tooltip>
                  <el-tooltip placement="top" class="column-type-dropdown" :content="$t('disableSortFilterTips')">
                    <el-tag is-light :type="!typeOptions.length ? 'info' : ''"><i class="el-ksd-n-icon-filter-outlined ksd-mr-2"></i><span>{{$t('type')}}{{typeOptions.length ? `: ${typeOptions.length}` : ''}}</span></el-tag>
                  </el-tooltip>
                </span>
                <span v-else>
                  <el-dropdown class="sort-colum-dropdown" placement="bottom-start" @command="handleSortCommand" trigger="click">
                    <el-tag is-light :type="!columnSort ? 'info' : ''"><i class="el-ksd-n-icon-table-rank-filled ksd-mr-2"></i><span>{{$t('column')}}{{columnSort ? `: ${$t(columnSort)}` : ''}}</span></el-tag>
                    <el-dropdown-menu slot="dropdown" :append-to-body="false">
                      <el-dropdown-item :class="{'is-active': columnSort === 'ascending'}" command="ascending">{{$t('ascending')}}</el-dropdown-item>
                      <el-dropdown-item :class="{'is-active': columnSort === 'descending'}" command="descending">{{$t('descending')}}</el-dropdown-item>
                    </el-dropdown-menu>
                  </el-dropdown>
                  <el-dropdown class="column-type-dropdown" placement="bottom-start" :hide-on-click="false" trigger="click">
                    <el-tag is-light :type="!typeOptions.length ? 'info' : ''"><i class="el-ksd-n-icon-filter-outlined ksd-mr-2"></i><span>{{$t('type')}}{{typeOptions.length ? `: ${typeOptions.length}` : ''}}</span></el-tag>
                    <el-dropdown-menu slot="dropdown" :append-to-body="false">
                      <el-checkbox-group v-model="typeOptions">
                        <el-dropdown-item v-for="(item, index) in columnTypes" :key="index"><el-checkbox :label="item" size="small">{{item}}</el-checkbox></el-dropdown-item>
                      </el-checkbox-group>
                    </el-dropdown-menu>
                  </el-dropdown>
                </span>
                <template v-if="isTextRecognition">
                  <span class="dive ksd-ml-4 ksd-mr-6">|</span>
                  <el-tooltip placement="top" :content="$t('textRecognitionTips')">
                    <el-button size="small" nobg-text icon="el-ksd-n-icon-view-outlined" @click="handleTableIndexRecognize">{{ $t('textRecognition') }}</el-button>
                  </el-tooltip>
                </template>
              </template>
              
            </div>
            <div class="result-content" :class="{'is-no-footer': !hasLeftFooter}">
              <div v-for="item in pagedModelColumns" :key="item.id" class="result-content_item">
                <div class="checkbox-list">
                  <el-checkbox v-model="item.selected" :disabled="item.disabled" @change="handleIndexColumnChange(item)">
                    <el-tooltip :content="$t('excludedTableIconTip')" effect="dark" placement="top"><i class="excluded_table-icon el-icon-ksd-exclude" v-if="item.excluded"></i></el-tooltip>
                    <span :style="{'max-width': dragData.width - 140 + 'px', width: 'auto'}" :title="item.label" class="ksd-nobr-text">{{item.label}}</span>
                  </el-checkbox>
                </div>
                <div class="item-type">{{item.type}}</div>
                <div class="hover-icons">
                  <el-tooltip placement="top">
                    <div slot="content" style="word-break: break-all">
                      <div v-if="item.name">{{ $t('kylinLang.dataSource.dimensionName') }}: {{ item.name }}</div>
                      <div>{{ $t('kylinLang.model.columnName') }}: {{ item.label }}</div>
                      <div v-if="item.cardinality">{{ $t('cardinality') }}: {{ item.cardinality }}</div>
                      <div v-if="item.comment">{{ $t('kylinLang.dataSource.comment') }}: {{ item.comment }}</div>
                    </div>
                    <i class="el-ksd-n-icon-info-circle-outlined"></i>
                  </el-tooltip>
                </div>
              </div>
              <!-- <div class="load-more-layout" v-if="showLoadMore('left')" @click="addPagination('left')">{{$t('kylinLang.common.loadMore')}}</div> -->
            </div>
            <empty-data size="small" :image="require('../../../assets/img/empty/empty_state_not_found.svg')" :content="$t('kylinLang.common.noData')" v-if="!pagedModelColumns.length" />
            <div class="transfer-footer" v-if="hasLeftFooter">
              <slot name="left-footer"></slot>
            </div>
          </div>
        </div>
        <div class="drag-line" unselectable="on" v-drag:change.width="dragData">
          <div class="ky-drag-layout-line"></div>
        </div>
        <div class="right_layout">
          <div class="title-layout">
            <p class="title">{{rightTitle}}</p>
            <slot name="help"></slot>
          </div>
          <div class="search_layout" v-if="showRightSearch">
            <el-input size="small" :placeholder="$t('kylinLang.common.search')" v-model.trim="searchUsedContent" prefix-icon="el-ksd-n-icon-search-outlined"></el-input>
          </div>
          <div v-show="searchVar&&pagedSelectedColumns.length" class="search-num">{{ filterSelectedColumns.length }}{{ $t('searchResultes') }}</div>
          <div :class="['selected-results', {'over-limit': showOverLimit && selectedColumns.length > limitLen}]">
            <div class="dropdowns" v-show="pagedSelectedColumns.length && isSortAble">
              <span v-if="searchVar || !selectedData.length || !isAnyCardinalityCol" class="disable-block">
                <el-tooltip placement="top" :disabled="!selectedData.length" class="sort-colum-dropdown">
                  <div slot="content" v-if="searchVar">{{ $t('disableSortFilterTips') }}</div>
                  <div slot="content" v-else>{{ $t('disabledSortTips') }} <a class="ky-a-like" @click="goToDataSource">{{ $t('goToDataSource') }}</a></div>
                  <el-tag is-light :type="!selectedColumnSortByCardinality ? 'info' : ''">
                    <i class="el-ksd-n-icon-table-rank-filled ksd-mr-2"></i><span>{{$t('cardinality')}}{{selectedColumnSortByCardinality ? `: ${$t(selectedColumnSortByCardinality)}` : ''}}</span>
                    <el-tooltip placement="top" v-if="isPartCardinalityCol" :content="$t('isPartCardinalityCol')">
                      <i class="el-ksd-n-icon-warning-filled ksd-fs-12"></i>
                    </el-tooltip>
                  </el-tag>
                </el-tooltip>
              </span>
              <span v-else>
                <el-dropdown class="sort-colum-dropdown" placement="bottom-start" @command="handleSortCardinality" trigger="click">
                  <el-tag is-light :type="!selectedColumnSortByCardinality ? 'info' : ''">
                    <i class="el-ksd-n-icon-table-rank-filled ksd-mr-2"></i><span>{{$t('cardinality')}}{{selectedColumnSortByCardinality ? `: ${$t(selectedColumnSortByCardinality)}` : ''}}</span>
                    <el-tooltip placement="top" v-if="isPartCardinalityCol" :content="$t('isPartCardinalityCol')">
                      <i class="el-ksd-n-icon-warning-filled ksd-fs-12"></i>
                    </el-tooltip>
                  </el-tag>
                  <el-dropdown-menu slot="dropdown" :append-to-body="false">
                    <el-dropdown-item :class="{'is-active': selectedColumnSortByCardinality === 'ascending'}" command="ascending">{{$t('ascending')}}</el-dropdown-item>
                    <el-dropdown-item :class="{'is-active': selectedColumnSortByCardinality === 'descending'}" command="descending">{{$t('descending')}}</el-dropdown-item>
                  </el-dropdown-menu>
                </el-dropdown>
              </span>
              <span v-if="searchVar || !selectedData.length" class="disable-block">
                <el-tooltip placement="top" class="sort-colum-dropdown" :disabled="!selectedData.length" :content="$t('disableSortFilterTips')">
                  <el-tag is-light :type="!selectedColumnSort ? 'info' : ''"><i class="el-ksd-n-icon-table-rank-filled ksd-mr-2"></i><span>{{$t('column')}}{{selectedColumnSort ? `: ${$t(selectedColumnSort)}` : ''}}</span></el-tag>
                </el-tooltip>
              </span>
              <span v-else>
                <el-dropdown class="sort-colum-dropdown" placement="bottom-start"  @command="handleSortSelectedCommand" trigger="click">
                  <el-tag is-light :type="!selectedColumnSort ? 'info' : ''"><i class="el-ksd-n-icon-table-rank-filled ksd-mr-2"></i><span>{{$t('column')}}{{selectedColumnSort ? `: ${$t(selectedColumnSort)}` : ''}}</span></el-tag>
                  <el-dropdown-menu slot="dropdown" :append-to-body="false">
                    <el-dropdown-item :class="{'is-active': selectedColumnSort === 'ascending'}" command="ascending">{{$t('ascending')}}</el-dropdown-item>
                    <el-dropdown-item :class="{'is-active': selectedColumnSort === 'descending'}" command="descending">{{$t('descending')}}</el-dropdown-item>
                  </el-dropdown-menu>
                </el-dropdown>
              </span>
            </div>
            <div class="selected-results_layout" :class="{'is-no-footer': !(showOverLimit && selectedColumns.length > limitLen) && !(draggable && searchVar), 'is-search-mode': searchVar}">
              <div :class="['selected-results_item', {'drag-item': draggable&&!searchVar, 'is-shared-col': it.isShared}]" v-for="it in pagedSelectedColumns" :key="it.key" :id="it.key">
                <p class="label">
                  <span class="num" :class="{'is-alive': isShowNum, 'is-disable-drag': isShowNum&&draggable&&searchVar}" :style="{'scale': ((getIndex(it.key) + 1) + '').length > 2 ? 1 - 0.2 * (((getIndex(it.key) + 1) + '').length - 2) : 1}" v-if="isShowNum">{{ getIndex(it.key) + 1 }}</span>
                  <el-tooltip placement="top" :content="$t('dragToMove')" v-show="draggable&&!searchVar">
                    <i class="el-ksd-n-icon-grab-dots-outlined" :class="{'is-alive': draggable&&!searchVar}"></i>
                  </el-tooltip><el-tooltip :content="$t('excludedTableIconTip')" effect="dark" placement="top"><i class="excluded_table-icon el-icon-ksd-exclude" v-if="it.excluded"></i></el-tooltip>
                  <el-button type="primary" size="mini" class="shardby is-shardby" v-if="it.isShared" text icon="el-ksd-n-icon-symbol-s-circle-filled"></el-button>
                  <span class="top-icon ignore-drag" v-if="topColAble"><el-button type="primary" size="mini" v-if="getIndex(it.key) !== 0" text @click.native="topColumn($event, it.key)" icon="el-ksd-n-icon-top-filled"></el-button></span>
                  <span class="ksd-nobr-text">{{it.label}}</span>
                </p>
                <div class="right-actions">
                  <el-tooltip placement="top" :content="$t('cardinality')">
                    <span class="cardinality ksd-nobr-text" @mouseenter.stop>{{it.cardinality}}</span>
                  </el-tooltip>
                </div>
                <div class="hover-icons ignore-drag">
                  <el-tooltip placement="top">
                    <div slot="content" style="word-break: break-all">
                      <div v-if="it.name">{{ $t('kylinLang.dataSource.dimensionName') }}: {{ it.name }}</div>
                      <div>{{ $t('kylinLang.model.columnName') }}: {{ it.label }}</div>
                      <div v-if="it.cardinality">{{ $t('cardinality') }}: {{ it.cardinality }}</div>
                      <div v-if="it.comment">{{ $t('kylinLang.dataSource.comment') }}: {{ it.comment }}</div>
                    </div>
                    <i class="el-ksd-n-icon-info-circle-outlined ksd-ml-5"></i>
                  </el-tooltip>
                  <el-tooltip placement="top" v-if="!it.isShared&sharedByAble" :content="shardByIndex !== -1 ? $t('replaceShardBy') : $t('setShardBy')">
                    <el-button type="primary" size="mini" class="shardby"  @click.stop="setShardBy(it.key)" text icon="el-ksd-n-icon-symbol-s-circle-filled"></el-button>
                  </el-tooltip><el-tooltip placement="top" v-if="it.isShared&sharedByAble"  :content="$t('removeShardBy')">
                    <el-button type="primary" size="mini" class="shardby is-shardby" text @click.stop="unSetSharyBy(it.key)" icon="el-ksd-n-icon-symbol-s-circle-filled"></el-button>
                  </el-tooltip><el-tooltip placement="top" :content="$t('remove')">
                    <i class="el-ksd-n-icon-close-outlined close-btn" @click.stop="removeColumn(it)"></i>
                  </el-tooltip>
                </div>
              </div>
              <!-- <div class="load-more-layout" v-if="showLoadMore('right')" @click="addPagination('right')">{{$t('kylinLang.common.loadMore')}}</div> -->
            </div>
            <empty-data size="small" :image="require('../../../assets/img/empty/empty_state_not_found.svg')" :content="searchVar ? $t('kylinLang.common.noData') : rightTitleTip" v-if="!pagedSelectedColumns.length" />
          </div>
          <div class="over-limit-tips" v-if="showOverLimit && selectedColumns.length > limitLen"><el-tooltip placement="top" :content="$t('limitTooltip')"><span class="text"><el-icon class="ksd-mr-4" name="el-ksd-n-icon-warning-color-filled" type="mult"></el-icon>{{$t('overLimitTips')}}</span></el-tooltip><div class="line"></div></div>
          <div class="over-limit-tips" v-if="draggable && searchVar"><span class="text"><el-icon class="ksd-mr-4" name="el-ksd-n-icon-warning-color-filled" type="mult"></el-icon>{{$t('disableDragtips')}}</span><div class="line"></div></div>
        </div>
      </div>
      <div class="search-results_layout">
        <p class="search-results_item">{{$t('selectedNum', {num: selectedData.length})}}</p>
        <span class="dive ksd-ml-12 ksd-mr-12">|</span>
        <p class="search-results_item">{{$t('allResultNum', {num: unSelectedData.length})}}</p>
      </div>
    </div>
  </template>
  <script>
  import { Component, Vue } from 'vue-property-decorator'
  import { objectClone, indexOfObjWithSomeKey, kylinConfirm } from 'util'
  import Sortable, { AutoScroll } from 'sortablejs/modular/sortable.core.esm.js'
  import EmptyData from '../EmptyData/EmptyData'
  
  Sortable.mount(new AutoScroll())
  
  @Component({
    props: {
      allModelColumns: {
        type: Array,
        default () {
          return []
        }
      },
      isShowNum: {
        type: Boolean,
        default: false
      },
      draggable: {
        type: Boolean,
        default: false
      },
      isSortAble: {
        type: Boolean,
        default: false
      },
      sharedByAble: {
        type: Boolean,
        default: false
      },
      topColAble: {
        type: Boolean,
        default: false
      },
      selectedColumns: {
        type: Array,
        default () {
          return []
        }
      },
      rightTitle: {
        type: String,
        default: ''
      },
      rightTitleTip: {
        type: String,
        default: ''
      },
      showOverLimit: {
        type: Boolean,
        default: false
      },
      pageSize: {
        type: Array,
        default () {
          return [20, 20]
        }
      },
      showRightSearch: {
        type: Boolean,
        default: false
      },
      limitLen: {
        type: Number,
        default: 3
      },
      alertTips: {
        type: Array,
        default () {
          return []
        }
      },
      isTextRecognition: {
        type: Boolean,
        default: false
      },
      isEdit: {
        type: Boolean,
        default: false
      },
      isAllSelect: {
        type: Boolean,
        default: false
      }
    },
    locales: {
      en: {
        ascending: 'ascending',
        descending: 'descending',
        column: 'Column',
        type: 'Type',
        allResultNum: 'Total {num}',
        selectedNum: 'Selected {num}',
        searchResultNum: '{num} Results',
        overLimitTips: 'Reached the recommended limit',
        limitTooltip: 'Please limit the number of indexes to 3 or less. Add index will increase the cost of building and will not effectively improve query performance.',
        cardinality: 'Cardinality',
        cardinalityValue: 'Cardinality: {value}',
        dragToMove: 'Drag to move',
        textRecognition: 'Text Recognition',
        setShardBy: 'Set as Shardby',
        replaceShardBy: 'Replace Shardby',
        removeShardBy: 'Remove Shardby',
        moveToTop: 'Move to Top',
        remove: 'Remove',
        shardbyTips: 'The shardby column is used to store data in slices, which can avoid uneven dispersion of table data, improve parallelism and query efficiency.',
        textRecognitionTips: 'Batch selection of columns by automatic recognition of pasted text',
        disabledSortTips: 'The current data has not been sampled. Please sample first and try again.',
        goToDataSource: 'Go to sampling',
        excludedTableIconTip: 'Excluded column',
        topColSuccess: 'Column moved to the top',
        setShardBySuccess: 'Shardby seted up',
        unSetShardBySuccess: 'Shardby removed',
        replaceShardBySuccess: 'Shardby replaced',
        isPartCardinalityCol: 'Some of the current data is not sampled, so sorting will only affect some of the results',
        searchResultes: ' search results',
        disableDragtips: 'Cannot drag and drop index in search mode',
        disableSortFilterTips: 'Cannot filter or sort index in search mode',
        cofirmRemoveSort: 'Are you sure you want to remove the current sort?'
      }
    },
    components: {
      EmptyData
    }
  })
  export default class TransferData extends Vue {
    columnSort = ''
    selectedColumnSort = ''
    selectedColumnSortByCardinality = this.isEdit ? '' : 'descending'
    typeOptions = []
    searchVar = ''
    searchUsedContent = ''
    isSelectAll = false
    currentDrag = {
      sourceId: '',
      targetId: '',
      sourceElement: null,
      startClientY: null,
      directive: ''
    }
    pagination = {
      left: {
        pageOffset: 0,
        pageSize: this.pageSize[0] || 20,
        totalSize: 0
      },
      right: {
        pageOffset: 0,
        pageSize: this.pageSize[1] || 20,
        totalSize: 0
      }
    }
    dragData = {
      width: 411.5,
      limit: {
        width: [190, 669]
      }
    }
    hasDragSuccess = false
  
    get leftStyle () {
      return {
        width: this.dragData.width + 'px'
      }
    }
  
    get hasLeftFooter () {
      return !!this.$slots['left-footer'] && this.$slots['left-footer'].length
    }
  
    get alertItems () {
      let items = []
      if (this.alertTips && this.alertTips.length > 0) {
        this.alertTips.forEach(item => {
          if (Object.prototype.toString.call(item) !== '[object Object]') {
            items.push({ text: item, type: 'warning' })
          } else {
            if ((item.checkFn && item.checkFn()) || !item.checkFn) {
              items.push(item)
            }
          }
        })
      }
      return items
    }
  
    get columnTypes () {
      return [...new Set(this.allModelColumns.filter(it => it.type).map(it => it.type))]
    }
  
    get pagedModelColumns () {
      return this.filterModelColumns.slice(0, (this.pagination.left.pageOffset + 1) * this.pagination.left.pageSize)
    }

    get filterTypeOptionSelectedColumns () {
      return this.typeOptions.length ? this.filterSelectedColumns.filter(it => this.typeOptions.includes(it.type)) : this.filterSelectedColumns
    }
  
    getIndex (key) {
      return indexOfObjWithSomeKey(this.selectedData, 'key', key)
    }
  
    get pagedSelectedColumns () {
      return this.filterSelectedColumns.slice(0, (this.pagination.right.pageOffset + 1) * this.pagination.right.pageSize)
    }
  
    get filterSelectedColumns () {
      let selectedColumns = objectClone(this.selectedData)
      if (this.searchVar) {
        this.$set(this.pagination.right, 'pageOffset', 0)
        selectedColumns = this.selectedData.filter(it => it.label.toLocaleLowerCase().indexOf(this.searchVar.toLocaleLowerCase()) > -1)
      }
      return selectedColumns
    }
  
    get filterModelColumns () {
      // let allModelColumns = this.allModelColumns.filter(it => !this.selectedColumns.includes(it.key)).map(item => ({...item, selected: false}))
      let allModelColumns = this.allModelColumns.map(item => ({...item, selected: this.selectedColumns.includes(item.key)}))
      let resultDatas = []
      if (this.searchVar) {
        this.$set(this.pagination.left, 'pageOffset', 0)
        allModelColumns = allModelColumns.filter(it => it.label.toLocaleLowerCase().indexOf(this.searchVar.toLocaleLowerCase()) > -1)
      }
      if (!this.typeOptions.length) {
        if (!this.columnSort) {
          resultDatas = allModelColumns
        } else {
          resultDatas = this.columnSort === 'ascending' ? allModelColumns.sort((a, b) => a.label.localeCompare(b.label)) : allModelColumns.sort((a, b) => b.label.localeCompare(a.label))
        }
      } else {
        this.$set(this.pagination.left, 'pageOffset', 0)
  
        // const selectedColumns = allModelColumns.filter(it => this.selectedColumns.includes(it.key))
        const results = [...allModelColumns.filter(it => this.typeOptions.includes(it.type))]
        if (!this.columnSort) {
          resultDatas = results
        } else {
          resultDatas = this.columnSort === 'ascending' ? results.sort((a, b) => a.label.localeCompare(b.label)) : results.sort((a, b) => b.label.localeCompare(a.label))
        }
      }
      this.$set(this.pagination.left, 'totalSize', resultDatas.length)
      return resultDatas
    }
  
    get isAnyCardinalityCol () {
      return this.selectedData.length && this.selectedData.filter((c) => !!c.cardinality).length > 0
    }
  
    get isPartCardinalityCol () {
      const cardinalityNum = this.selectedData.filter((c) => !!c.cardinality).length
      return this.selectedData.length && cardinalityNum > 0 && cardinalityNum < this.selectedData.length
    }
  
    get unSelectedData () {
      return this.allModelColumns
    }
  
    get selectedData () {
      let sData = []
      if (this.selectedColumns.length) {
        this.selectedColumns.forEach(key => {
          const [obj] = this.allModelColumns.filter(item => item.key === key)
          obj && sData.push(obj)
        })
        if (this.isSortAble) {
          if (this.searchUsedContent) {
            this.$set(this.pagination.right, 'pageOffset', 0)
            sData = sData.filter(it => it.label.toLocaleLowerCase().indexOf(this.searchUsedContent.toLocaleLowerCase()) >= 0)
          }
          if (this.selectedColumnSort) {
            sData = this.selectedColumnSort === 'ascending' ? sData.sort((a, b) => a.label.localeCompare(b.label)) : sData.sort((a, b) => b.label.localeCompare(a.label))
          } else if (this.selectedColumnSortByCardinality) {
            sData = this.selectedColumnSortByCardinality === 'ascending' ? sData.sort((a, b) => a.cardinality - b.cardinality) : sData.sort((a, b) => b.cardinality - a.cardinality)
          }
        }
      }
      this.$set(this.pagination.right, 'totalSize', sData.length)
      return sData
    }
  
    get shardByIndex () {
      return indexOfObjWithSomeKey(this.selectedData, 'isShared', true)
    }
  
    created () {
      this.pagination.left.totalSize = this.filterModelColumns.length
      this.pagination.right.totalSize = this.selectedData.length
    }
  
    mounted () {
      const $unSelectPage = this.$el.querySelector('.result-content')
      const $selectedPage = this.$el.querySelector('.selected-results_layout')
      $unSelectPage && $unSelectPage.addEventListener('scroll', this.debounce(this.handleScroll, '.result-content', 'left', 200))
      $selectedPage && $selectedPage.addEventListener('scroll', this.debounce(this.handleScroll, '.selected-results_layout', 'right', 200))
      if (this.draggable) {
        const rightPanel = this.$el.querySelector('.selected-results')
        const rightEl = $selectedPage
        Sortable.create(rightEl, {
          scroll: true,
          handle: '.drag-item',
          filter: '.ignore-drag',
          animation: 150,
          scrollSensitivity: 50,
          scrollSpeed: 10,
          bubbleScroll: true,
          forceFallback: true,
          revert: true,
          scrollFn: () => {
            return 'continue'
          },
          ghostClass: 'sortable-ghost',
          dragClass: 'sortable-drag',
          onEnd: async (e) => {
            if (this.selectedColumnSortByCardinality || this.selectedColumnSort) {
              try {
                await kylinConfirm(' ', {confirmButtonText: this.$t('remove'), type: 'warning'}, this.$t('cofirmRemoveSort'))
              } catch (res) {
                const items = this.$el.querySelectorAll('.drag-item')
                this.$nextTick(() => {
                  e.from.insertBefore(e.item, items[e.oldIndex + (e.oldIndex > e.newIndex)])
                })
                return false
              }
            }
            const { oldIndex, newIndex } = e
            let selectedList = this.selectedData.map(d => d.key)
            const temp = selectedList[oldIndex]
            if (temp === undefined) {
              return
            }
            const target = selectedList.splice(oldIndex, 1)[0]
            selectedList.splice(newIndex, 0, target)
            this.selectedColumnSortByCardinality = ''
            this.selectedColumnSort = ''
            this.hasDragSuccess = true
            this.handleEmitEvent('setSelectedColumns', selectedList)
          }
        })
        rightPanel.ondragover = (e) => {
          e.preventDefault()
        }
        rightPanel.ondrop = (e) => {
          e.preventDefault()
        }
      }
    }
  
    debounce (fn, dom, scrollType, delay) {
      let timer = null
      return function () {
        if (timer) {
          clearTimeout(timer)
        }
        timer = setTimeout(() => {
          fn(dom, scrollType)
        }, delay)
      }
    }
  
    destroyed () {
      const $unSelectPage = this.$el.querySelector('.result-content')
      const $selectedPage = this.$el.querySelector('.selected-results_layout')
      $unSelectPage && $unSelectPage.removeEventListener('scroll', this.handleScroll)
      $selectedPage && $selectedPage.removeEventListener('scroll', this.handleScroll)
    }
  
    handleScroll (dom, scrollType) {
      const scrollBoxDom = this.$el.querySelector(dom)
      const scrollT = scrollBoxDom.scrollTop
      const scrollH = scrollBoxDom.scrollHeight
      const clientH = scrollBoxDom.clientHeight
      if (scrollT + scrollH >= clientH) {
        this.scrollLoad(scrollType)
      }
    }
  
    handleTableIndexRecognize () {
      this.$emit('handleTableIndexRecognize')
    }
  
    selectAll () {
      let selectedList = objectClone(this.selectedColumns)
      // 除去disabled的列，
      this.isSelectAll = this.filterModelColumns.filter(c => !c.disabled).length > this.filterSelectedColumns.length
      this.filterModelColumns.forEach((c) => {
        !c.disabled && (c.selected = this.isSelectAll)
        if (this.isSelectAll) {
          const index = selectedList.indexOf(c.key)
          index === -1 && !c.disabled && selectedList.push(c.key)
        } else {
          const index = selectedList.indexOf(c.key)
          index !== -1 && !c.disabled && selectedList.splice(index, 1)
        }
      })
      this.handleEmitEvent('setSelectedColumns', selectedList)
      this.setSelectedColumns()
    }
  
    showLoadMore (type) {
      const { left, right } = this.pagination
      if (type === 'left') {
        return (left.pageOffset + 1) * left.pageSize < left.totalSize
      } else {
        return (right.pageOffset + 1) * right.pageSize < right.totalSize
      }
    }
  
    scrollLoad (type) {
      if (this.showLoadMore(type)) {
        this.addPagination(type)
      }
    }
  
    addPagination (type) {
      const page = this.pagination[type].pageOffset + 1
      this.$set(this.pagination[type], 'pageOffset', page)
    }
  
    handleEmitEvent (name, value) {
      this.$emit(name, value)
    }
  
    handleSortCommand (sort) {
      this.columnSort = sort
    }
  
    setSelectedColumns () {
      this.$nextTick(() => {
        const selectedList = this.selectedData.map(d => d.key)
        this.handleEmitEvent('setSelectedColumns', selectedList)
      })
    }
  
    async handleSortSelectedCommand (sort) {
      if (this.hasDragSuccess) {
        await kylinConfirm(' ', {confirmButtonText: this.$t('remove'), type: 'warning'}, this.$t('cofirmRemoveSort'))
      }
      this.selectedColumnSortByCardinality = ''
      this.selectedColumnSort = sort
      this.setSelectedColumns()
      this.hasDragSuccess = false
    }
  
    async handleSortCardinality (sort) {
      if (this.hasDragSuccess) {
        await kylinConfirm(' ', {confirmButtonText: this.$t('remove'), type: 'warning'}, this.$t('cofirmRemoveSort'))
      }
      this.selectedColumnSort = ''
      this.selectedColumnSortByCardinality = sort
      this.setSelectedColumns()
      this.hasDragSuccess = false
    }
  
    goToDataSource () {
      this.$router.push('/studio/source')
    }
  
    handleIndexColumnChange (v) {
      const selectedList = objectClone(this.selectedColumns)
      if (v.selected) {
        selectedList.push(v.key)
      } else {
        const index = selectedList.indexOf(v.key)
        index > -1 && selectedList.splice(index, 1)
      }
      this.handleEmitEvent('setSelectedColumns', selectedList)
      this.setSelectedColumns()
    }
  
    removeColumn (v) {
      const selectedList = objectClone(this.selectedColumns)
      const index = selectedList.indexOf(v.key)
      index > -1 && selectedList.splice(index, 1)
      this.handleEmitEvent('setSelectedColumns', selectedList)
    }
  
    async topColumn (e, key) {
      e.stopPropagation()
      e.preventDefault()
      if (this.selectedColumnSortByCardinality || this.selectedColumnSort) {
        try {
          await kylinConfirm(' ', {confirmButtonText: this.$t('remove'), type: 'warning'}, this.$t('cofirmRemoveSort'))
        } catch (res) {
          return
        }
      }
      const selectedList = this.selectedData.map(d => d.key)
      const index = selectedList.indexOf(key)
      selectedList.splice(0, 0, selectedList[index])
      selectedList.splice(index + 1, 1)
      this.selectedColumnSortByCardinality = ''
      this.selectedColumnSort = ''
      this.$nextTick(() => {
        this.handleEmitEvent('setSelectedColumns', selectedList)
      })
      this.hasDragSuccess = true
      this.$message({ type: 'success', message: this.$t('topColSuccess') })
    }
  
    setShardBy (key) {
      let message = ''
      if (this.shardByIndex !== -1) {
        this.selectedData[this.shardByIndex].isShared = false
        message = this.$t('replaceShardBySuccess')
      }
      const index = indexOfObjWithSomeKey(this.selectedData, 'key', key)
      this.selectedData[index].isShared = true
      this.handleEmitEvent('setShardbyCol', this.selectedData[index].label)
      this.$message({ type: 'success', message: message || this.$t('setShardBySuccess') })
    }
  
    unSetSharyBy (key) {
      const index = indexOfObjWithSomeKey(this.selectedData, 'key', key)
      this.selectedData[index].isShared = false
      this.handleEmitEvent('setShardbyCol', '')
      this.$message({ type: 'success', message: this.$t('unSetShardBySuccess') })
    }
  }
  </script>
  <style lang="less">
  @import '../../../assets/styles/variables.less';
  .sec-index-container {
    width: 100%;
    height: 450px;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    .hover-icons {
      display: none;
      position: absolute;
      right: 4px;
      right: 0px;
      z-index: 999;
      background-color: @ke-color-info-secondary-bg;
      padding: 0 4px;
      .close-btn {
        color: @text-placeholder-color;
        cursor: pointer;
      }
      .el-ksd-n-icon-info-circle-outlined {
        font-size: 14px;
        color: @text-placeholder-color;
        cursor: pointer;
      }
      .shardby.is-sharcby {
        background-color: @ke-color-success-hover;
      }
      .el-button {
        position: relative;
        top: -1px;
      }
    }
    .disable-block {
      .el-tag {
        margin: 2px 4px;
        cursor: not-allowed;
        opacity: 0.3;
        background-color: @ke-background-color-secondary;
        &:hover {
          background-color: @ke-background-color-drag;
        }
      }
    }
    .search-results_layout {
      box-sizing: border-box;
      position: absolute;
      bottom: 32px;
      .search-results_item {
        display: inline-block;
        color: @text-disabled-color;
        font-size: 14px;
        font-weight: 400;
      }
    }
    .drag-line {
      padding: 0 4px;
      .ky-drag-layout-line {
        border-left: 1px solid @ke-border-secondary;
        height: 100%;
      }
      &:hover {
        cursor: col-resize;
        .ky-drag-layout-line {
          border-left: 1px solid @base-color;
        }
      }
    }
    .dive {
      color: @ke-border-secondary;
    }
    .transfer-container {
      width: 100%;
      height: 0;
      flex: 1;
      display: flex;
      border-radius: 6px;
      border: 1px solid @ke-border-divider-color;
      .el-transfer {
        .el-transfer-panel__functions {
          font-size: 0;
        }
      }
      .column-type-dropdown {
        .el-checkbox__inner {
          vertical-align: middle;
        }
        .el-dropdown-menu {
          max-height: 358px;
          overflow: auto;
        }
      }
    }
    .left_layout {
      // width: 50%;
      height: 100%;
      // border-right: 1px solid @ke-border-divider-color;
      display: inline-flex;
      flex-direction: column;
      .search_layout {
        padding: 8px;
        margin-right: -4px;
        box-sizing: border-box;
        border-bottom: 1px solid @ke-border-divider-color;
        .el-input__inner {
          border: 0;
          outline: none;
          box-shadow: none;
        }
      }
      .sec-index-content {
        height: 0;
        display: flex;
        flex-direction: column;
        flex: 1;
        position: relative;
      }
      .transfer-footer {
        height: 28px;
        padding: 8px 0;
        border-top: 1px solid @ke-border-divider-color;
      }
    }
    .right_layout {
      // width: 50%;
      flex: 1;
      height: 100%;
      display: inline-flex;
      flex-direction: column;
      vertical-align: top;
      position: relative;
      .search-num {
        margin: 0 auto;
        text-align: center;
        color: @text-placeholder-color;
        font-size: 12px;
        line-height: 18px;
        margin-bottom: 16px;
      }
      .empty-data {
        width: 100%;
        .center {
          padding: 0 16px;
        }
      }
      .search_layout {
        padding: 8px 4px;
        box-sizing: border-box;
        .el-input__inner {
          border: 0;
          outline: none;
          box-shadow: none;
        }
      }
      .title-layout {
        display: flex;
        justify-content: space-between;
        padding: 17px 8px;
        box-sizing: border-box;
        .el-ksd-n-icon-help-circle-outlined {
          color: @text-placeholder-color;
          cursor: pointer;
        }
      }
      .selected-results {
        // height: calc(~'100% - 52px');
        flex: 1;
        position: relative;
        height: 0;
        display: flex;
        flex-direction: column;
        .el-ksd-n-icon-warning-filled {
          color: @ke-color-warning;
        }
        &.over-limit {
          padding-bottom: 40px;
          box-sizing: border-box;
        }
        .right-actions {
          max-width: 100px;
          display: inherit;
        }
        .dropdowns {
          padding: 0;
          height: 30px;
        }
        .selected-results_layout {
          overflow: auto;
          height: 320px;
          margin-left: -4px;
          &.is-search-mode {
            height: 286px;
          }
          &.is-no-footer {
            height: 360px;
            &.is-search-mode {
              height: 326px;
            }
          }
        }
        .selected-results_item {
          line-height: 34px;
          font-size: 14px;
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 0 4px 0 4px;
          box-sizing: border-box;
          position: relative;
          color: @text-title-color;
          cursor: default;
          &.drag-item {
            cursor: grab;
          }
          .excluded_table-icon {
            color: @text-placeholder-color;
            margin-right: 2px;
          }
          .label {
            // user-select: none;
            flex: 1;
            width: 0;
            display: inline-flex;
            align-items: center;
            .num.is-alive {
              display: inline-block;
              color: @text-placeholder-color;
              width: 20px;
              text-align: center;
            }
            .el-ksd-n-icon-grab-dots-outlined {
              display: none;
            }
          }
          &:hover {
            .num:not(.is-disable-drag) {
              display: none !important;
            }
            .el-ksd-n-icon-grab-dots-outlined.is-alive {
              display: inline-block !important;
              width: 20px;
              text-align: center;
            }
          }
          .el-button--mini {
            padding: 3px 4px;
          }
          .shardby {
            .el-ksd-n-icon-symbol-s-circle-filled {
              color: @text-placeholder-color;
            }
            &:hover {
              .el-ksd-n-icon-symbol-s-circle-filled {
                color: @text-disabled-color;
              }
            }
            &.is-shardby {
              .el-ksd-n-icon-symbol-s-circle-filled{
                color: @ke-color-success-hover;
              }
              &:hover {
                .el-ksd-n-icon-symbol-s-circle-filled{
                  color: @ke-color-success-active;
                }
              }
            }
          }
          .el-ksd-n-icon-delete-outlined {
            position: relative;
            top: -1px;
            color: @ke-color-info-secondary;
            font-size: 16px;
            margin-right: 2px;
            &.active {
              color: @ke-color-success-hover;
            }
          }
          .top-icon {
            display: none;
            position: absolute;
            left: 16px;
            z-index: 999;
            background-color: @ke-color-info-secondary-bg;
            .el-button {
              position: relative;
              top: -1px;
            }
            .el-ksd-n-icon-top-filled {
              color: @text-placeholder-color;
              &:hover {
                color: @text-disabled-color;
              }
            }
          }
          &:hover {
            background: @ke-color-info-secondary-bg;
            .hover-icons,
            .top-icon {
              display: initial;
            }
          }
          &:active {
            background-color: @ke-background-color-drag;
            .hover-icons,
            .top-icon {
              background-color: @ke-background-color-drag;
              .el-button {
                background-color: @ke-background-color-drag;
              }
            }
          }
          &.is-shared-col {
            background-color: @ke-color-success-bg;
            color: @ke-color-success-active;
            font-weight: @font-medium;
            .hover-icons,
            .top-icon {
              background-color: @ke-color-success-bg;
              .el-button {
                background-color: #D9F1D0;
              }
            }
          }
          .tip_box {
            height: 32px;
            vertical-align: middle;
          }
          &.sortable-ghost {
            opacity: 0.1 !important;
            background-color: @ke-border-drag;
            border-width: 2px 3px;
            border-style: dashed;
            border-color: @ke-border-drag-target;
          }
          &.sortable-drag {
            background-color: @ke-background-color-drag;
            box-shadow: @ke-drag-box-shadow;
            border: 1px solid @ke-border-drag;
            opacity: 0.8;
            width: 300px !important;
            cursor: grabbing !important;
            .cardinality {
              display: none;
            }
          }
        }
        .cardinality {
          color: @text-placeholder-color;
          user-select: none;
        }
        .empty-data-normal {
          img {
            height: 64px;
          }
        }
      }
      .over-limit-tips {
        width: 100%;
        height: 32px;
        text-align: center;
        font-size: 12px;
        color: @ke-color-warning-hover;
        position: absolute;
        bottom: 0;
        left: 0;
        .line {
          width: 100%;
          height: 1px;
          background: #FFDFC2;
          position: absolute;
          left: 0;
          top: 50%;
          z-index: 1;
        }
        .text {
          display: inline-block;
          background: @fff;
          padding: 0 10px;
          z-index: 2;
          position: absolute;
          top: 0;
          left: 50%;
          transform: translate(-50%, 0);
          line-height: 32px;
          cursor: default;
          white-space: pre;
        }
      }
    }
    .load-more-layout {
      text-align: center;
      font-size: 12px;
      cursor: pointer;
      line-height: 32px;
      &:hover {
        color: #1268FB;
      }
    }
    .sec-index-content {
      padding: 10px 0 0 0;
      box-sizing: border-box;
      .result-content {
        height: 320px;
        overflow: auto;
        margin-right: -4px;
        &.is-no-footer {
          height: 360px;
        }
        .result-content_item {
          display: flex;
          align-items: center;
          justify-content: space-between;
          overflow-x: hidden;
          padding: 0 4px 0 8px;
          box-sizing: border-box;
          position: relative;
          &:hover {
            background: @ke-background-color-secondary;
            .hover-icons {
              display: initial !important;
            }
          }
          .excluded_table-icon {
            position: relative;
            top: -7px;
            color: @text-placeholder-color;
          }
          .checkbox-list {
            flex: 1;
            width: 0;
          }
          .el-checkbox {
            width: 100%;
            height: 34px;
            .el-checkbox__label {
              position: relative;
              top: 3px;
            }
            .el-ksd-n-icon-info-circle-outlined {
              position: relative;
              top: -5px;
              font-size: 14px;
              color: @text-placeholder-color;
            }
          }
          .item-type {
            color: @text-placeholder-color;
          }
        }
      }
    }
    .flip-list-move {
      transition: transform .5s;
    }
    .dropdowns {
      padding: 0 8px;
      display: flex;
      flex-wrap: wrap;
      font-size: 0;
      height: 30px;
      margin-right: -4px;
      .select-all {
        flex-grow: 3;
      }
      .el-dropdown {
        cursor: pointer;
        margin: 2px 4px;
        .el-dropdown-menu__item {
          &.is-active {
            color: @ke-color-primary;
          }
          &.is-disabled {
            color: @text-placeholder-color;
            pointer-events: initial;
          }
        }
      }
      .dive {
        font-size: 18px;
      }
      .el-tag {
        &.is-light {
          background-color: #f1f6fe;
          border-color: #f1f6fe;
          &:hover {
            background-color: #d5e4fe;
            border-color: #d5e4fe;
          }
        }
      }
    }
  }
  </style>
