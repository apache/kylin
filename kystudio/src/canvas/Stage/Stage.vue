<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import CanvasTooltip from '../CanvasTooltip/CanvasTooltip'

@Component({
  props: {
    // Canvas渲染定位X
    stageX: {
      type: Number,
      default: 0
    },
    // Canvas渲染定位Y
    stageY: {
      type: Number,
      default: 0
    },
    // Canvas是否可以拖拽
    draggable: {
      type: Boolean,
      default: false
    },
    // Canvas是否全局拖拽层
    isDragLayer: {
      type: Boolean,
      default: false
    },
    // Canvas渲染缩放
    zoom: {
      type: Number,
      default: 1
    }
  },
  components: {
    CanvasTooltip
  }
})
export default class Stage extends Vue {
  stageWidth = 0
  stageHeight = 0

  debounce = null

  mounted () {
    this.addEventListeners()
    this.handleResize()
  }

  beforeDestroy () {
    this.removeEventListeners()
  }

  get stageStyle () {
    const { stageX: x, stageY: y, stageWidth: width, stageHeight: height, draggable } = this
    return { x, y, width, height, draggable }
  }

  get contentStyle () {
    const { zoom } = this
    const scale = { x: zoom, y: zoom }
    return { scale }
  }

  get dragLayerStyle () {
    const { stageX, stageY, stageWidth: width, stageHeight: height } = this
    return { width, height, x: -stageX, y: -stageY }
  }

  addEventListeners () {
    window.addEventListener('resize', this.handleDebounceResize)

    if (this.isDragLayer) {
      this.addCanvasPointerStyle()
    } else {
      this.addBackgroundPointerStyle()
    }
  }

  removeEventListeners () {
    window.removeEventListener('resize', this.handleDebounceResize)

    if (this.isDragLayer) {
      this.removeCanvasPointerStyle()
    } else {
      this.removeBackgroundPointerStyle()
    }
  }

  addBackgroundPointerStyle () {
    const $dragLayer = this.$refs['$drag-layer'].getNode()
    $dragLayer.on('mouseenter mouseup', this.setPointGrab)
    $dragLayer.on('mouseout', this.setPointDefault)
    $dragLayer.on('mousedown', this.setPointGrabbing)
  }

  removeBackgroundPointerStyle () {
    const $dragLayer = this.$refs['$drag-layer'].getNode()
    $dragLayer.off('mouseenter mouseup', this.setPointGrab)
    $dragLayer.off('mouseout', this.setPointDefault)
    $dragLayer.off('mousedown', this.setPointGrabbing)
  }

  addCanvasPointerStyle () {
    const $canvas = this.$el.querySelector('canvas')
    $canvas.addEventListener('mouseenter', this.setPointGrab)
    $canvas.addEventListener('mouseup', this.setPointGrab)
    $canvas.addEventListener('mousedown', this.setPointGrabbing)
  }

  removeCanvasPointerStyle () {
    const $canvas = this.$el.querySelector('canvas')
    $canvas.removeEventListener('mouseenter', this.setPointGrab)
    $canvas.removeEventListener('mouseup', this.setPointGrab)
    $canvas.removeEventListener('mousedown', this.setPointGrabbing)
  }

  handleDebounceResize () {
    clearTimeout(this.debounce)
    this.debounce = setTimeout(this.handleResize, 500)
  }

  handleResize () {
    const { $container } = this.$refs
    this.stageWidth = $container.clientWidth
    this.stageHeight = $container.clientHeight
  }

  handleMouseWheel (event) {
    if (event.evt.altKey) {
      event.evt.preventDefault()
      this.$emit('zoom', event.evt.deltaY / 100)
    }
  }

  handleDragStage () {
    const position = this.$refs.$stage.getNode().position()
    this.$emit('dragmove', position)
  }

  setPointGrabbing () {
    this.$refs.$stage.getNode().container().style.cursor = 'move'
    this.$refs.$stage.getNode().container().style.cursor = 'grabbing'
  }

  setPointDefault () {
    this.$refs.$stage.getNode().container().style.cursor = ''
  }

  setPointGrab () {
    this.$refs.$stage.getNode().container().style.cursor = 'move'
    this.$refs.$stage.getNode().container().style.cursor = 'grab'
  }

  render (h) {
    const { stageStyle, contentStyle, dragLayerStyle } = this
    return (
      <div ref="$container" class="stage">
        <v-stage ref="$stage" config={stageStyle} onWheel={this.handleMouseWheel} onDragmove={this.handleDragStage}>
          {/* 插槽：背景层canvas，此处适合放入v-layer */}
          {this.$slots.background}
          {/* 渲染：主体层canvas */}
          <v-layer ref="$content" config={contentStyle}>
            {/* 组件：手形拖拽底层 */}
            <v-rect ref="$drag-layer" config={dragLayerStyle} />
            {/* 插槽：canvas图形渲染组件 */}
            {this.$slots.default}
            {/* 组件：tooltip */}
            <CanvasTooltip />
          </v-layer>
        </v-stage>
      </div>
    )
  }
}
</script>

<style lang="less">
.stage {
  overflow: hidden;
  height: 100%;
}
</style>
