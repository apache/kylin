<template>
  <div class="drag-bar-group">
    <div class="drag-bar drag-bar-xy-br" v-drag:change.width.height="dragData" :data-zoom="dragZoomVal"></div>
    <div class="drag-bar drag-bar-xy-tr" v-drag:change.width.height.reverseH="dragData" :data-zoom="dragZoomVal"></div>
    <div class="drag-bar drag-bar-xy-tl" v-drag:change.width.height.reverse="dragData" :data-zoom="dragZoomVal"></div>
    <div class="drag-bar drag-bar-xy-bl" v-drag:change.width.height.reverseW="dragData" :data-zoom="dragZoomVal"></div>
    <div class="drag-bar drag-bar-y-r" v-drag:change.width="dragData" :data-zoom="dragZoomVal"></div>
    <div class="drag-bar drag-bar-x-b" v-drag:change.height="dragData" :data-zoom="dragZoomVal"></div>
    <div class="drag-bar drag-bar-x-t" v-drag:change.height.reverse="dragData" :data-zoom="dragZoomVal"></div>
    <div class="drag-bar drag-bar-y-l" v-drag:change.width.reverse="dragData" :data-zoom="dragZoomVal"></div>
  </div>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  @Component({
    props: ['dragData', 'dragZoom']

  })
  export default class DragBar extends Vue {
    dragZoomVal = this.dragZoom || null
    @Watch('dragZoom')
    changeDragZoom () {
      this.dragZoomVal = this.dragZoom
    }
  }
</script>
<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .drag-bar-group{
  // 拖动操作样式
    .drag-bar {
      position:absolute;
      z-index: 1;
    }
    .drag-bar-x() {
      left:10px;
      right:10px;
      height:10px;
      cursor:ns-resize;
    }
    .drag-bar-x-b {
      bottom: -5px;
      .drag-bar-x()
    }
    .drag-bar-x-t {
      top: -5px;
      .drag-bar-x()
    }
    .drag-bar-y() {
      width: 10px;
      top:10px;
      bottom:10px;
      cursor:ew-resize;
    }
    .drag-bar-y-r {
      .drag-bar-y();
      right: -5px;
    }
    .drag-bar-y-l {
      .drag-bar-y();
      left: -5px;
    }
    .drag-bar-corner (@x,@y,@cursor) {
      height: 10px;
      width: 10px;
      @{x}:-5px;
      @{y}:-5px;
      cursor: @cursor;
    }
    .drag-bar-xy-br {
      .drag-bar-corner(right, bottom, nwse-resize);
    }
    .drag-bar-xy-tl {
      .drag-bar-corner(left, top, nwse-resize);
    }
    .drag-bar-xy-tr {
      .drag-bar-corner(right, top, nesw-resize);
    }
    .drag-bar-xy-bl {
      .drag-bar-corner(left, bottom, nesw-resize);
    }
    // 拖动操作样式 
  }
</style>
