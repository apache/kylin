<template>  
  <div class="kylin_progress" :class="progressClass">
     <el-progress :percentage="percent" :icon-class="icon"></el-progress>
     <!-- <i v-if="icon" :class="icon"></i> -->
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  props: ['percent', 'status'],
  methods: {
  },
  computed: {
    icon () {
      if (this.status === 'PENDING') {
        return ''
      } else if (this.status === 'RUNNING') {
        return ''
      } else if (this.status === 'FINISHED') {
        return 'el-icon-ksd-good_health'
      } else if (this.status === 'ERROR') {
        return 'el-icon-ksd-error_01'
      } else if (this.status === 'DISCARDED') {
        return 'el-icon-ksd-error_01'
      } else if (this.status === 'STOPPED') {
        return 'el-icon-ksd-pause_02'
      }
    },
    progressClass () {
      if (this.status === 'PENDING') {
        return 'pending'
      } else if (this.status === 'RUNNING') {
        return 'running'
      } else if (this.status === 'FINISHED') {
        return 'success'
      } else if (this.status === 'ERROR') {
        return 'error'
      } else if (this.status === 'DISCARDED') {
        return 'lose'
      } else if (this.status === 'STOPPED') {
        return 'pause'
      }
    }
  }
})
export default class Total extends Vue {
  created () {
  }
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
  .kylin_progress{
    height: 20px;
    .el-progress-bar {
      padding-right: 55px;
    }
    i {
      cursor: default !important;
    }
    &.success {
      .el-progress-bar__inner {
        background-color: @ke-color-success-hover;
      }
      i {
        color: @ke-color-success-hover;
      }
    }
    &.error {
      .el-progress-bar__inner {
        background-color: @ke-color-danger-hover;
      }
      i {
        color: @ke-color-danger-hover;
      }
    }
    &.lose {
      .el-progress-bar__inner {
        background-color: @text-placeholder-color;
      }
      i {
        color: @text-placeholder-color;
      }
    }
    &.pause {
      i {
        color: @color-primary;
      }
    }
  }
</style>
