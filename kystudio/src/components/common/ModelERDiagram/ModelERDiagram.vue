<template>
  <div class="model-er-diagram" v-drag="{sizeChangeCb:dragBox}" v-loading="loadingER">
    <div class="er-layout" ref="el-draw-layout" v-if="currentModel" :style="{'transform': `scale(${currentModel.canvas.zoom / 10})`}">
      <div :class="['table-box', {'is-lookup': t.type !== 'FACT'}]" :id="t.guid" v-for="t in currentModel.tables" :key="t.guid" :style="getTableStyles(t)">
        <div :class="['table-title', {'table-spread-out': !t.spreadOut}]" @dblclick="handleDBClick(t)">
          <span class="table-sign">
            <el-tooltip :content="$t(t.type)" placement="top">
              <i class="el-ksd-n-icon-symbol-f-filled kind" v-if="t.type === 'FACT'"></i>
              <i v-else class="el-ksd-n-icon-dimention-table-filled kind"></i>
            </el-tooltip>
          </span>
          <el-tooltip :content="t.alias" :visible-arrow="false" popper-class="popper--small model-alias-tooltip" effect="dark">
            <span class="table-alias">{{t.alias}}</span>
          </el-tooltip>
          <!-- <el-tooltip :content="$t('tableColumnNum', {'all': getColumnNums(t), 'len': t.spreadOut ? getColumnNumInView(t) : 0})" placement="top">
            <span class="table-column-nums"><i class="el-ksd-n-icon-eye-open-outlined ksd-mr-4 ksd-fs-16"></i>{{getColumnNumInView(t)}}</span>
          </el-tooltip> -->
          <el-tooltip :content="`${t.columns.length}`" placement="top" :disabled="typeof getColumnNums(t) === 'number'">
            <span class="table-column-nums">{{getColumnNums(t)}}</span>
          </el-tooltip>
        </div>
        <div class="column-list-box">
          <ul>
            <li
              v-for="col in t.columns"
              :key="col.id"
              :id="`${t.guid}_${col.name}`"
              :class="{'is-pfk': isPFK(col.name, t).isPK || isPFK(col.name, t).isFK}"
              @mouseleave="(e) => handleMouseLeave(e, t, col)"
              @mouseenter="(e) => handleMouseEnterColumn(e, t, col)"
            >
              <span class="ksd-nobr-text">
                <span class="col-type-icon">
                  <span class="pfk-sign">{{isPFK(col.name, t).isPK ? 'PK' : isPFK(col.name, t).isFK ? 'FK' : ''}}</span><i :class="columnTypeIconMap(col.datatype)"></i>
                </span>
                <span :class="['col-name', {'is-link': col.isPFK}]">{{col.name}}</span>
              </span>
            </li>
          </ul>
        </div>
      </div>
    </div>
    <el-button-group class="diagram-actions">
      <el-button plain icon="el-ksd-n-icon-zoom-in-outlined" @click.stop="handleZoom('up')" />
      <el-button plain icon="el-ksd-n-icon-zoom-out-outlined" @click.stop="handleZoom('down')" />
      <!-- <el-button plain icon="el-ksd-icon-zoom_to_default_old" @click="handleReset" /> -->
    </el-button-group>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { handleSuccessAsync } from 'util'
import { columnTypeIcon } from '../../../config'
import { mapActions, mapGetters, mapState } from 'vuex'
import { initPlumb, drawLines, customCanvasPosition } from './handler'
import locales from './locales'
@Component({
  props: {
    model: {
      type: Object,
      default () {
        return {}
      }
    }
  },
  computed: {
    ...mapState({
      modelList: state => state.model.modelsList
    }),
    ...mapGetters([
      'currentProjectData'
    ])
  },
  methods: {
    ...mapActions({
      loadColumnOfModel: 'LOAD_DATASOURCE_OF_MODEL'
    })
  },
  locales
})
export default class ModelERDiagram extends Vue {
  currentModel = null
  columnTypeIconMap = columnTypeIcon
  plumbTool = null
  plumbInstance = null
  loadingER = false
  defaultZoom = 9
  foreignKeys = []
  primaryKeys = []
  linkFocus = []
  // 判断是否为主外键
  isPFK (column, table) {
    return {
      isPK: this.primaryKeys.includes(`${table.alias}.${column}`),
      isFK: this.foreignKeys.includes(`${table.alias}.${column}`)
    }
  }
  // 获取列的数量
  getColumnNums (t) {
    return t.columns.length > 999 ? '999+' : t.columns.length
  }
  // 获取在可视区域内列的个数
  getColumnNumInView (table) {
    if (!this.currentModel.canvas) return ''
    const { canvas } = this.currentModel
    const { height } = canvas.coordinate[`${table.alias}`]
    const num = Math.floor((height - 38) / 34)
    return table.spreadOut ? table.columns.length > num ? `${num}` :`${table.columns.length}` : ''
  }
  async created () {
    await this.getTableColumns()
    this.$nextTick(() => {
      const { plumbInstance, plumbTool } = initPlumb(this.$el.querySelector('.er-layout'), this.currentModel.canvas ?? this.defaultZoom)
      this.plumbInstance = plumbInstance
      this.plumbTool = plumbTool
      drawLines(this, this.plumbTool, this.currentModel.joints)
      this.handleSortTables()
      if (!this.currentModel.canvas) {
        const cv = customCanvasPosition(this.$el.querySelector('.er-layout'), this.currentModel, this.defaultZoom)
        this.$set(this.currentModel, 'canvas', cv)
        this.$nextTick(() => {
          this.plumbTool.refreshPlumbInstance()
        })
      }
    })
  }
  // 获取 table 位置信息
  getTableStyles (table) {
    const { canvas } = this.currentModel
    if (!canvas) return {}
    const { height, width, x, y } = canvas.coordinate[`${table.alias}`]
    return { top: `${y}px`, left: `${x}px`, height: `${height}px`, width: `${width}px` }
  }
  // 双击表头 - 展开或收起
  handleDBClick (t) {
    this.$set(t, 'spreadOut', !t.spreadOut)
    !t.spreadOut && this.$set(t, 'spreadHeight', this.currentModel.canvas.coordinate[`${t.alias}`].height)
    const boxH = !t.spreadOut ? this.$el.querySelector('.table-title').offsetHeight + 4 : t.spreadHeight
    this.$set(this.currentModel.canvas.coordinate[`${t.alias}`], 'height', boxH)
    this.$nextTick(() => {
      this.plumbTool.refreshPlumbInstance()
    })
  }
  getTableColumns () {
    this.loadingER = true
    return new Promise((resolve, reject) => {
      const { name } = this.currentProjectData
      const [{ canvas }] = this.modelList.filter(item => item.alias === this.model.name)
      this.loadColumnOfModel({project: name, model_name: this.model.name}).then(async (result) => {
        const values = await handleSuccessAsync(result)
        this.loadingER = false
        this.currentModel = {
          ...this.model,
          tables: this.model.tables.map(table => {
            const [{ columns }] = values.filter(v => table.name === `${v.database}.${v.name}`)
            return {
              ...table,
              columns
            }
          }),
          canvas: canvas
        }
        resolve()
      }).catch(e => {
        this.loadingER = false
      })
    })
  }
  handleSortTables () {
    const tables = this.currentModel.tables
    tables.forEach(t => {
      const pfkLinkColumns = t.columns.filter(it => this.isPFK(it.name, t).isPK && this.isPFK(it.name, t).isFK).sort((a, b) => a - b)
      const pkLinkColumns = t.columns.filter(it => this.isPFK(it.name, t).isPK && !this.isPFK(it.name, t).isFK).sort((a, b) => a - b)
      const fkLinkColumns = t.columns.filter(it => this.isPFK(it.name, t).isFK && !this.isPFK(it.name, t).isPK).sort((a, b) => a - b)
      const unlinkColumns = t.columns.filter(it => !this.isPFK(it.name, t).isPK && !this.isPFK(it.name, t).isFK)
      t.columns = [...pfkLinkColumns, ...pkLinkColumns, ...fkLinkColumns, ...unlinkColumns]
    })
  }
  // 拖动画布
  dragBox (x, y) {
    const drawBoard = this.$el.querySelector('.er-layout')
    const mL = drawBoard.offsetLeft ?? 0
    const mT = drawBoard.offsetTop ?? 0
    drawBoard.style.cssText += `margin-left: ${mL + x}px; margin-top: ${mT + y}px`
  }
  // 放大缩小
  handleZoom (type) {
    let {zoom} = this.currentModel.canvas
    if (type === 'up') {
      this.$set(this.currentModel.canvas, 'zoom', zoom + zoom * 0.2)
    } else {
      this.$set(this.currentModel.canvas, 'zoom', zoom + zoom * -0.2)
    }
  }
  handleMouseEnterColumn (e, t, col) {
    const linkList  = []
    this.currentModel.joints.forEach(item => {
      if (item.guid === t.guid) {
        linkList.push(...item.joins.filter(it => it.primaryKey === `${t.alias}.${col.name}`).map(it => ({table_guid: it.guid, linked_column: it.foreignKey})))
      } else if (item.joins.filter(it => it.guid === t.guid).length > 0) {
        const [linkJoin] = item.joins.filter(it => it.guid === t.guid && it.foreignKey === `${t.alias}.${col.name}`)
        if (!linkJoin) return
        linkList.push({table_guid: item.guid, linked_column: linkJoin.primaryKey})
      }
    })
    linkList.forEach(lk => {
      const tDom = document.getElementById(`${lk.table_guid}`)
      const linkColumn = document.getElementById(`${lk.table_guid}_${lk.linked_column.split('.')[1]}`)
      const linkLine = document.getElementsByClassName(`${t.guid}&${lk.table_guid}`).length > 0 ? document.getElementsByClassName(`${t.guid}&${lk.table_guid}`) : document.getElementsByClassName(`${lk.table_guid}&${t.guid}`)
      linkColumn && (linkColumn.className += ' is-hover')
      linkLine[0].nextElementSibling && (linkLine[0].nextElementSibling.className += ' is-focus')
      tDom && (tDom.className += ' is-hover');
      (Array.prototype.slice.call(linkLine)).forEach(item => {
        if (item.nodeName === 'svg') {
          const cls = item.getAttribute('class')
          item.setAttribute('class', `${cls} is-focus`)
        } else {
          item.className += ' is-focus'
        }
      })
    })
  }
  // 移除 linked column focus 状态
  handleMouseLeave () {
    const svgLine = this.$el.querySelectorAll('.jtk-connector.is-focus')
    const linkColumn = this.$el.querySelectorAll('.is-pfk.is-hover')
    const linkLabels = this.$el.querySelectorAll('.jtk-overlay.is-focus')
    if (linkLabels.length > 0) {
      Array.prototype.slice.call(linkLabels).forEach(item => {
        item.classList.remove('is-focus')
      })
    }
    if (linkColumn.length > 0) {
      Array.prototype.slice.call(linkColumn).forEach(item => {
        item.classList.remove('is-hover')
      })
    }
    if (svgLine.length > 0) {
      Array.prototype.slice.call(svgLine).forEach(item => {
        item.setAttribute('class', `${item.className.baseVal.replace(/is-focus|is-broken/g, '')}`)
      })
    }
  }
}
</script>
<style lang="less">
@import '../../../assets/styles/variables.less';
.model-er-diagram {
  width: 100%;
  height: 100%;
  .er-layout {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    user-select: none;
    .jtk-connector.jtk-hover {
      path {
        stroke: #A5B2C5;
        stroke-width: 1;
      }
    }
    .jtk-connector.is-focus {
      path {
        stroke: @ke-color-primary;
        stroke-width: 2;
      }
    }
    .jtk-overlay {
      background: @fff;
      .join-type {
        color: #A5B2C5;
        font-size: 30px;
        cursor: move;
        // &:hover {
        //   color: #0875DA;
        // }
      }
      .close-icon {
        display: none;
      }
      &.is-focus {
        .join-type {
          color: @ke-color-primary;
        }
      }
    }
  }
  .table-box {
    width: 200px;
    height: 230px;
    background-color: @fff;
    position: absolute;
    border-radius: 6px;
    border: 2px solid #E6EBF4;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    z-index: 10;
    &.is-focus {
      border: 2px solid #0867BF;
    }
    &.is-broken.is-focus {
      border: 2px solid @ke-color-danger;
    }
    &.link-hover {
      border: 2px solid #9DCEFB;
    }
    &:hover {
      // box-shadow: @fact-hover-shadow;
      border: 2px solid #9DCEFB;
      .scrollbar-track-y{
        opacity: 1;
      }
    }
    &:focus {
      border: 2px solid @line-border-focus;
    }
    &.is-lookup {
      .table-title {
        background-color: @background-gray;
        color: @fff;
      }
    }
    .no-data {
      position: initial;
      margin-top: 32px;
    }
    // overflow: hidden;
    .table-title {
      background-color: @base-color;
      color:#fff;
      line-height: 38px;
      border-radius: 5px 5px 0 0;
      height: 38px; // 高度改变需要更改 getColumnNumInView 方法里相应值
      padding: 0 8px 0 12px;
      display: flex;
      align-items: center;
      &.table-spread-out {
        border-radius: 5px;
      }
      .table-sign {
        display: inline-block;
        font-size: 0;
        cursor: pointer;
      }
      .table-column-nums {
        font-size: 12px;
        cursor: pointer;
        i {
          margin-top: -2px;
        }
      }
      .table-alias {
        text-overflow: ellipsis;
        overflow: hidden;
        line-height: 29px\0;
        width: calc(~"100% - 50px");
        display: inline-block;
        margin-left: 4px;
        font-weight: bold;
        font-size: 14px;
      }
      .kind {
        font-size: 16px;
        margin: 0;
      }
      .kind:hover {
        color:@grey-3;
      }
      i {
        color:@fff;
        // margin: auto 6px 8px;
      }
    }
    .column-list-box {
      overflow: hidden;
      border-top: solid 1px @line-border-color;
      flex: 1;
      ul {
        li {       
          padding-left: 5px;
          border-top: solid 2px transparent;
          border-bottom: 2px solid transparent;
          height: 32px;
          line-height: 32px;
          font-size: 14px;
          position: relative;
          &.is-hover {
            background-color: #CEE6FD;
            border: 2px solid #0875DA;
          }
          &.is-pfk {
            .col-name {
              color: #0875DA;
            }
            background-color: #F0F8FE;
            &:hover {
              background: #CEE6FD;
              border: 2px solid #0875DA
            }
          }
          // &:hover{
          //   border-top: 2px solid #9DCEFB;
          //   border-bottom: 2px solid #9DCEFB;
          //   background-color: #CEE6FD;
          // }
          .ksd-nobr-text {
            width: calc(~'100% - 25px');
          }
          .col-type-icon {
            color: @text-disabled-color;
            font-size: 12px;
            .pfk-sign{
              color: @text-placeholder-color;
              position: absolute;
              right: 8px;
            }
          }
          .col-name {
            color: @text-title-color;
            &.is-link {
              color: #0875DA;
              font-weight: @font-medium;
            }
          }
          &.column-li-cc {
            background-color: @warning-color-2;
            &:hover {
              background-color: @warning-color-3;
            }
          }
        }
      }
    }
  }
  .diagram-actions {
    position: absolute;
    right: 20px;
    z-index: 50;
  }
}
.model-alias-tooltip {
  margin-top: -10px !important;
}
</style>